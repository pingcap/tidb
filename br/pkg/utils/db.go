// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/tidb/kv"
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

// CheckLogBackupEnabled checks if LogBackup is enabled in cluster.
// this mainly used in three places.
// 1. GC worker resolve locks to scan more locks after safepoint. (every minute)
// 2. Add index skipping use lightning.(every add index ddl)
// 3. Telemetry of log backup feature usage (every 6 hours).
// NOTE: this result shouldn't be cached by caller. because it may change every time in one cluster.
func CheckLogBackupEnabled(ctx sessionctx.Context) bool {
	enabled, err := IsLogBackupEnabled(ctx.(sqlexec.RestrictedSQLExecutor))
	if err != nil {
		// if it failed by any reason. we can simply return true this time.
		// for GC worker it will scan more locks in one tick.
		// for Add index it will skip using lightning this time.
		// for Telemetry it will get a false positive usage count.
		return true
	}
	return enabled
}

// IsLogBackupEnabled is used for br to check whether tikv has enabled log backup.
// we use `sqlexec.RestrictedSQLExecutor` as parameter because it's easy to mock.
// it should return error.
func IsLogBackupEnabled(ctx sqlexec.RestrictedSQLExecutor) (bool, error) {
	valStr := "show config where name = 'log-backup.enable'"
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR)
	rows, fields, errSQL := ctx.ExecRestrictedSQL(internalCtx, nil, valStr)
	if errSQL != nil {
		return false, errSQL
	}
	if len(rows) == 0 {
		// no rows mean not support log backup.
		return false, nil
	}
	for _, row := range rows {
		d := row.GetDatum(3, &fields[3].Column.FieldType)
		value, errField := d.ToString()
		if errField != nil {
			return false, errField
		}
		if strings.ToLower(value) == "false" {
			return false, nil
		}
	}
	return true, nil
}
