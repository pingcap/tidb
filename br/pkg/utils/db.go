// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
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

// IsLogBackupEnabled checks if LogBackup is enabled in cluster.
// this mainly used for three places.
// 1. Resolve locks to scan more locks after safepoint.
// 2. Add index to skip using lightning.
// 3. Telemetry of log backup feature usage statistics.
func IsLogBackupEnabled(ctx sessionctx.Context) bool {
	var valStr = "show config where name = 'log-backup.enable'"
	rows, fields, errSQL := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(context.Background(), nil, valStr)
	if errSQL != nil {
		// if failed by any reason. we can simply return true this time.
		// for GC worker it will scan more locks in one tick.
		// for Add index it will skip using lightning this time.
		// for Telemetry it will get a false positive usage.
		return true
	}
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		d := row.GetDatum(3, &fields[3].Column.FieldType)
		value, errField := d.ToString()
		if errField != nil {
			return true
		}
		if strings.ToLower(value) == "false" {
			return false
		}
	}
	return true
}
