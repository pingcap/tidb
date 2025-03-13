package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

// UpdatePipelinedDMLProgress update the progress of resolve lock
// If done is true, delete the progress record
func UpdatePipelinedDMLProgress(
	sctx sessionctx.Context,
	startTS uint64,
	status transaction.PipelinedDMLStatus,
	resolvedRegions int64,
	done bool,
) error {
	// Get a new session from system session pool
	pool := domain.GetDomain(sctx).SysSessionPool()
	tmp, err := pool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	var returnErr error
	se := tmp.(sessionctx.Context)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)

	defer func() {
		if returnErr != nil {
			pool.Destroy(tmp)
		} else {
			pool.Put(tmp)
		}
	}()

	if done {
		// Failpoint to skip deleting the record when done=true
		skip := false
		failpoint.Inject("skipDeletePipelinedDMLProgress", func() {
			skip = true
		})
		if skip {
			logutil.Logger(ctx).Info("skipDeletePipelinedDMLProgress failpoint triggered, skipping delete",
				zap.Uint64("startTS", startTS))
			return nil
		}

		// When done, delete the record
		sql := "DELETE FROM mysql.pipelined_dml_progress WHERE start_ts = %?"
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql, startTS)
		returnErr = errors.Trace(err)
		return returnErr
	}

	switch status {
	case transaction.PipelinedDMLExecuting:
		var tableNames string
		if tables := sctx.GetSessionVars().StmtCtx.Tables; len(tables) > 0 {
			names := make([]string, len(tables))
			for i, t := range tables {
				names[i] = t.Table
			}
			tableNames = strings.Join(names, ",")
		}
		// truncate at 256
		if len(tableNames) > 256 {
			tableNames = tableNames[:256]
		}

		// The initial state only executes INSERT once
		sql := `INSERT INTO mysql.pipelined_dml_progress
				(start_ts, involved_tables, resolved_regions, status, create_time, update_time)
				VALUES (%?, %?, 0, %?, NOW(), NOW())`
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql, startTS, tableNames, status.String())
		returnErr = errors.Trace(err)
		return returnErr

	case transaction.PipelinedDMLResolvingLocks, transaction.PipelinedDMLRollingBack:
		// Failpoint to pause when updating to ResolvingLocks status
		if status == transaction.PipelinedDMLResolvingLocks {
			failpoint.Inject("pauseResolvingLocksPipelinedDML", func() {
				logutil.Logger(ctx).Info("pauseResolvingLocksPipelinedDML failpoint triggered, pausing",
					zap.Uint64("startTS", startTS),
					zap.Int64("resolvedRegions", resolvedRegions))
				// Sleep for a short while to simulate a pause, but keep tests fast
				time.Sleep(100 * time.Millisecond)
			})
		}

		// The intermediate state only executes UPDATE
		sql := `UPDATE mysql.pipelined_dml_progress
				SET resolved_regions = %?, status = %?, update_time = NOW()
				WHERE start_ts = %?`
		_, err := se.GetSQLExecutor().ExecuteInternal(ctx, sql, resolvedRegions, status.String(), startTS)
		returnErr = errors.Trace(err)
		return returnErr

	default:
		returnErr = errors.Errorf("unknown status: %s", status.String())
		return returnErr
	}
}

// CleanupPipelinedDMLProgressBySafePoint cleans the expired records by GC safepoint.
func CleanupPipelinedDMLProgressBySafePoint(sctx sessionctx.Context, safePoint uint64) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx,
		"DELETE FROM mysql.pipelined_dml_progress WHERE start_ts < %?",
		safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	if rs == nil {
		return nil
	}
	defer rs.Close()

	// get the number of deleted records
	rows, err := sqlexec.DrainRecordSet(ctx, rs, -1)
	if err != nil {
		return errors.Trace(err)
	}
	logutil.Logger(ctx).Info("cleanup async pipelined-dml resolve lock progress records by GC safepoint",
		zap.Uint64("safePoint", safePoint),
		zap.Int("cleaned", len(rows)))
	return nil
}
