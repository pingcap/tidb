// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
)

var (
	// check sql.DB and sql.Conn implement QueryExecutor and DBExecutor
	_ DBExecutor = &sql.DB{}
	_ DBExecutor = &sql.Conn{}
)

// QueryExecutor is a interface for exec query
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// StmtExecutor define both query and exec methods
type StmtExecutor interface {
	QueryExecutor
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// DBExecutor is a interface for statements and txn
type DBExecutor interface {
	StmtExecutor
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
