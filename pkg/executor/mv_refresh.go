package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func mvQuoteFullName(schema, table ast.CIStr) string {
	escape := func(s string) string {
		return strings.ReplaceAll(s, "`", "``")
	}
	if schema.L == "" {
		return fmt.Sprintf("`%s`", escape(table.O))
	}
	return fmt.Sprintf("`%s`.`%s`", escape(schema.O), escape(table.O))
}

func mvQuoteIdent(name ast.CIStr) string {
	escape := strings.ReplaceAll(name.O, "`", "``")
	return fmt.Sprintf("`%s`", escape)
}

func mvExecInternal(ctx context.Context, sctx sessionctx.Context, sql string, args ...any) error {
	execProvider, ok := sctx.(interface{ GetSQLExecutor() sqlexec.SQLExecutor })
	if !ok {
		return errors.New("session does not support internal SQL execution")
	}
	_, err := sqlexec.ExecSQL(ctx, execProvider.GetSQLExecutor(), sql, args...)
	return err
}

func mvQueryInternal(ctx context.Context, sctx sessionctx.Context, sql string, args ...any) ([]chunk.Row, error) {
	execProvider, ok := sctx.(interface{ GetSQLExecutor() sqlexec.SQLExecutor })
	if !ok {
		return nil, errors.New("session does not support internal SQL execution")
	}
	return sqlexec.ExecSQL(ctx, execProvider.GetSQLExecutor(), sql, args...)
}

func mvBeginPessimistic(ctx context.Context, sctx sessionctx.Context) error {
	return mvExecInternal(ctx, sctx, "BEGIN PESSIMISTIC")
}

func mvCommit(ctx context.Context, sctx sessionctx.Context) error {
	return mvExecInternal(ctx, sctx, "COMMIT")
}

func mvRollback(ctx context.Context, sctx sessionctx.Context) error {
	return mvExecInternal(ctx, sctx, "ROLLBACK")
}

// mvCompleteRefresh performs a COMPLETE refresh for a materialized view:
// - delete all MV rows
// - insert from MV definition SELECT
// - update mv_refresh_info (last_refresh_tso = txn.start_ts) atomically in the same txn
func mvCompleteRefresh(
	ctx context.Context,
	sctx sessionctx.Context,
	internalSourceType string,
	mvSchema, mvName ast.CIStr,
	mvID int64,
	definitionSQL string,
) (retErr error) {
	internalCtx := kv.WithInternalSourceType(ctx, internalSourceType)
	if err := mvBeginPessimistic(internalCtx, sctx); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = mvRollback(internalCtx, sctx)
		}
	}()

	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	readTS := txn.StartTS()

	// Refresh mutex: lock the mv_refresh_info row.
	rows, err := mvQueryInternal(internalCtx, sctx, "SELECT mv_id FROM mysql.mv_refresh_info WHERE mv_id = %? FOR UPDATE", mvID)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return errors.Errorf("mv_refresh_info row not found for mv_id=%d", mvID)
	}

	mvFullName := mvQuoteFullName(mvSchema, mvName)

	if err := mvExecInternal(internalCtx, sctx, "DELETE FROM "+mvFullName); err != nil {
		return err
	}
	if err := mvExecInternal(internalCtx, sctx, "INSERT INTO "+mvFullName+" "+definitionSQL); err != nil {
		return err
	}
	if err := mvExecInternal(internalCtx, sctx,
		`UPDATE mysql.mv_refresh_info
		   SET last_refresh_tso = %?,
		       last_refresh_type = 'COMPLETE',
		       last_refresh_result = 'SUCCESS',
		       last_refresh_time = NOW(),
		       next_run_time = DATE_ADD(NOW(), INTERVAL refresh_interval_seconds SECOND),
		       last_error = NULL
		 WHERE mv_id = %?`,
		readTS, mvID,
	); err != nil {
		return err
	}
	return mvCommit(internalCtx, sctx)
}
