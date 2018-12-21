package testutil

import (
	"context"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
)

// SessionExecInGoroutine export for testing.
func SessionExecInGoroutine(c *check.C, s kv.Storage, sql string, done chan error) {
	ExecMultiSQLInGoroutine(c, s, "test_db", []string{sql}, done)
}

// ExecMultiSQLInGoroutine exports for testing.
func ExecMultiSQLInGoroutine(c *check.C, s kv.Storage, dbName string, multiSQL []string, done chan error) {
	go func() {
		se, err := session.CreateSession4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, sql := range multiSQL {
			rs, err := se.Execute(context.Background(), sql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty.")
				return
			}
			done <- nil
		}
	}()
}
