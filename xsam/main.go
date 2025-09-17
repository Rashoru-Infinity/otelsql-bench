package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func initOtelTracer(ctx context.Context, resAttr ...attribute.KeyValue) *trace.TracerProvider {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}
	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithContainer(),
		resource.WithAttributes(resAttr...),
	)
	if err != nil {
		log.Fatal(err)
	}
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp
}

func main() {
	dsn := "root:root@unix(../sql/mysql-sock/mysqld.sock)/hello_db"

	driverName, err := otelsql.Register(
		"mysql",
		otelsql.WithAttributes(append(otelsql.AttributesFromDSN(dsn), semconv.DBSystemNameMySQL)...),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			// List all options
			Ping:                 true,
			RowsNext:             true,
			DisableErrSkip:       false,
			DisableQuery:         false,
			OmitConnResetSession: false,
			OmitConnPrepare:      false,
			OmitConnQuery:        false,
			OmitRows:             false,
			OmitConnectorConnect: false,
			// SpanFilter
			// RecordError
		}),
		otelsql.WithAttributesGetter(func(_ context.Context, _ otelsql.Method, _ string, args []driver.NamedValue) []attribute.KeyValue {
			kvs := []attribute.KeyValue{
				attribute.Int("db.sql.args.count", len(args)),
			}
			for i, a := range args {
				valueAttrKey := fmt.Sprintf("db.sql.args.%d.value", i+1)
				switch v := a.Value.(type) {
				case string:
					kvs = append(kvs, attribute.String(valueAttrKey, v))
				case int:
					kvs = append(kvs, attribute.Int(valueAttrKey, v))
				case int32:
					kvs = append(kvs, attribute.Int64(valueAttrKey, int64(v)))
				case int64:
					kvs = append(kvs, attribute.Int64(valueAttrKey, v))
				case float32:
					kvs = append(kvs, attribute.Float64(valueAttrKey, float64(v)))
				case float64:
					kvs = append(kvs, attribute.Float64(valueAttrKey, v))
				case bool:
					kvs = append(kvs, attribute.Bool(valueAttrKey, v))
				case []byte:
					kvs = append(kvs, attribute.String(valueAttrKey, hex.EncodeToString(v)))
				case time.Time:
					kvs = append(kvs, attribute.String(valueAttrKey, v.Format(time.RFC3339Nano)))
				default:
					kvs = append(kvs, attribute.String(valueAttrKey, fmt.Sprintf("%v", v)))
				}
			}
			return kvs
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	db, err := sqlx.Open(driverName, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	tp := initOtelTracer(ctx)
	defer tp.Shutdown(ctx)

	tx, err := db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal("failed to commit transaction: %w", err)
		}
	}()

	rows, err := tx.QueryContext(ctx, "SELECT id, content FROM messages LIMIT ?", 65536)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var content string
		if err := rows.Scan(&id, &content); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d: %s\n", id, content)
	}
}
