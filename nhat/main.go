package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.nhat.io/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
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
		otelsql.TraceAll(),
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
