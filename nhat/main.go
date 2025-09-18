package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"

	"oteltest/redactkey"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.nhat.io/otelsql"
	xattr "go.nhat.io/otelsql/attribute"
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

type TraceOpt int

const (
	TRACE = iota
	MASK
)

func main() {
	dsn := "root:root@unix(../sql/mysql-sock/mysqld.sock)/hello_db"

	driverName, err := otelsql.Register(
		"mysql",
		otelsql.TracePing(),
		otelsql.TraceRowsNext(),
		otelsql.TraceRowsClose(),
		otelsql.TraceRowsAffected(),
		otelsql.TraceLastInsertID(),
		otelsql.DisableErrSkip(),
		otelsql.AllowRoot(), // for testing, allow to create root span
		otelsql.TraceQuery(func(ctx context.Context, sql string, args []driver.NamedValue) []attribute.KeyValue {
			attrs := make([]attribute.KeyValue, 0, 1+len(args))
			attrs = append(attrs, semconv.DBQueryText(sql))

			var redactedArgs []driver.NamedValue

			for _, arg := range args {
				if m, ok := ctx.Value(redactkey.GetRedactHintKey()).(map[int]TraceOpt); ok {
					if traceHint, ok := m[arg.Ordinal]; ok {
						switch traceHint {
						case TRACE:
							redactedArgs = append(redactedArgs, arg)
						case MASK:
							redactedArgs = append(redactedArgs, driver.NamedValue{Value: "*****", Ordinal: arg.Ordinal})
						}
					}
				}
			}
			for _, a := range redactedArgs {
				attrs = append(attrs, xattr.FromNamedValue(a))
			}
			return attrs
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sqlxdb := sqlx.NewDb(db, "mysql")

	ctx := context.Background()

	tp := initOtelTracer(ctx)
	defer tp.Shutdown(ctx)

	tx, err := sqlxdb.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
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

	ctxWithTraceHint := context.WithValue(ctx, redactkey.GetRedactHintKey(), map[int]TraceOpt{
		1: MASK,
		3: TRACE,
	})
	rows, err := tx.QueryContext(ctxWithTraceHint, "SELECT id, content FROM messages WHERE id = ? LIKE ? LIMIT ?", 2, "wk1X%", 65536)
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
