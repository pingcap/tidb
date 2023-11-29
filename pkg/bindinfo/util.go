package bindinfo

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
)

// exec is a helper function to execute sql and return RecordSet.
func exec(sctx sessionctx.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	sqlExec, ok := sctx.(sqlexec.SQLExecutor)
	if !ok {
		return nil, errors.Errorf("invalid sql executor")
	}
	// TODO: use RestrictedSQLExecutor + ExecOptionUseCurSession instead of SQLExecutor
	return sqlExec.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), sql, args...)
}

// execRows is a helper function to execute sql and return rows and fields.
func execRows(sctx sessionctx.Context, sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	if intest.InTest {
		if v := sctx.Value(mock.MockRestrictedSQLExecutorKey{}); v != nil {
			return v.(*mock.MockRestrictedSQLExecutor).ExecRestrictedSQL(
				kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), nil, sql, args...)
		}
	}

	sqlExec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, nil, errors.Errorf("invalid sql executor")
	}
	return sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), nil, sql, args...)
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(sctx sessionctx.Context, err error) error {
	if err == nil {
		_, _, err = execRows(sctx, "COMMIT")
	} else {
		_, _, err1 := execRows(sctx, "ROLLBACK")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}
