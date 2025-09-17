package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
)

func openDBUptrace() (*sqlx.DB, string, error) {
	name := "uptrace"
	dsn := "root:root@unix(../sql/mysql-sock/mysqld.sock)/hello_db"
	db, err := otelsqlx.Open("mysql", dsn)
	if err != nil {
		return nil, "", err
	}
	return db, name, nil
}
