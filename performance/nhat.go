package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.nhat.io/otelsql"
)

func openDBNhat() (*sqlx.DB, string, error) {
	name := "nhat"
	dsn := "root:root@unix(../sql/mysql-sock/mysqld.sock)/hello_db"

	driverName, err := otelsql.Register(
		"mysql",
		otelsql.TraceAll(),
	)
	if err != nil {
		return nil, "", err
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, "", err
	}

	sqlxdb := sqlx.NewDb(db, "mysql")
	return sqlxdb, name, nil
}
