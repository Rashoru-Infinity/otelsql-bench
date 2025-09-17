package main

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func openDBXSAM() (*sqlx.DB, string, error) {
	name := "XSAM"
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
		return nil, "", err
	}
	db, err := sqlx.Open(driverName, dsn)
	if err != nil {
		return nil, "", err
	}
	return db, name, nil
}
