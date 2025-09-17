package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

type openDB func() (*sqlx.DB, string, error)

type warmState struct {
	p50s      []time.Duration
	currIdx   int
	tolerance float64
	try       int
}

func newWarmState(recordSize int, tolerance float64) *warmState {
	return &warmState{
		p50s:      make([]time.Duration, recordSize),
		tolerance: tolerance,
	}
}

func (ws *warmState) updateP50s(p50 time.Duration) {
	ws.currIdx %= len(ws.p50s)
	ws.p50s[ws.currIdx] = p50
	ws.currIdx++
	ws.try++
}

func (ws *warmState) isStable() bool {
	if len(ws.p50s) == 0 {
		return false
	}
	if ws.try < len(ws.p50s) {
		return false
	}
	lastP50 := ws.p50s[len(ws.p50s)-1]
	return lo.EveryBy(ws.p50s, func(d time.Duration) bool {
		diff := math.Abs(float64(lastP50-d)) / float64(lastP50)
		return diff <= ws.tolerance
	})
}

func calcP50(samples []time.Duration) (time.Duration, error) {
	if len(samples) == 0 {
		return time.Duration(0), errors.New("no sample")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 := samples[len(samples)/2]
	if len(samples)%2 == 0 {
		p50 = (samples[len(samples)/2] + samples[len(samples)/2+1]) / 2
	}
	return p50, nil
}

func calcP99(samples []time.Duration) (time.Duration, error) {
	if len(samples) == 0 {
		return time.Duration(0), errors.New("no sample")
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	n := len(samples)
	pos := (float64(99) / 100) * float64(n-1)
	i := int(math.Floor(pos))
	frac := pos - float64(i)
	if i+1 < n {
		v1 := float64(samples[i])
		v2 := float64(samples[i+1])
		return time.Duration(v1*(1-frac) + v2*frac), nil
	}
	return samples[i], nil
}

func calcAvg(samples []time.Duration) (time.Duration, error) {
	if len(samples) == 0 {
		return time.Duration(0), errors.New("no sample")
	}
	avg := int64(0)
	for _, d := range samples {
		avg += int64(d)
	}
	avg /= int64(len(samples))
	return time.Duration(avg), nil
}

func benchOtelSQL(tb *testing.B, f openDB) {
	windowSize := 200
	tolerance := 0.05
	recordSize := 3
	db, name, err := f()
	require.NoError(tb, err)
	defer db.Close()

	fmt.Printf("===%s===\n", name)
	log.Println("warmup")
	warmState := newWarmState(recordSize, tolerance)
	for !warmState.isStable() {
		samples := make([]time.Duration, windowSize)

		runBench := func() {
			tb.StartTimer()
			tx, err := db.BeginTxx(tb.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
			require.NoError(tb, err)
			rows, err := tx.QueryContext(tb.Context(), "SELECT id, content FROM messages LIMIT ?", 65536)
			require.NoError(tb, err)
			defer rows.Close()

			for rows.Next() {
				var id int
				var content string
				err = rows.Scan(&id, &content)
				require.NoError(tb, err)
			}
			require.NoError(tb, tx.Commit())
			elapsed := tb.Elapsed()
			samples = append(samples, elapsed)
			tb.ResetTimer()
		}

		for i := 0; i < windowSize; i++ {
			runBench()
		}
		p50, err := calcP50(samples)
		require.NoError(tb, err)
		warmState.updateP50s(p50)
	}

	log.Println("start bench")
	samples := make([]time.Duration, windowSize)
	runBench := func() {
		tb.StartTimer()
		tx, err := db.BeginTxx(tb.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		require.NoError(tb, err)
		rows, err := tx.QueryContext(tb.Context(), "SELECT id, content FROM messages LIMIT ?", 65536)
		require.NoError(tb, err)
		defer rows.Close()

		for rows.Next() {
			var id int
			var content string
			err = rows.Scan(&id, &content)
			require.NoError(tb, err)
		}
		require.NoError(tb, tx.Commit())
		elapsed := tb.Elapsed()
		samples = append(samples, elapsed)
		tb.ResetTimer()
	}
	for i := 0; i < windowSize; i++ {
		runBench()
	}

	p50, err := calcP50(samples)
	require.NoError(tb, err)
	avg, err := calcAvg(samples)
	require.NoError(tb, err)
	p99, err := calcP99(samples)
	require.NoError(tb, err)
	fmt.Printf("p(50): %v\np(99): %v\navg: %v\n", p50, p99, avg)
	fmt.Println("======")
}

func BenchmarkOtelSQL(tb *testing.B) {
	rdbClients := []openDB{
		openDBXSAM,
		openDBUptrace,
		openDBNhat,
	}

	for _, f := range rdbClients {
		benchOtelSQL(tb, f)
	}
}
