// Copyright 2023-2023 PingCAP, Inc.

package gcworker

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func enableLogHistory() bool {
	return variable.EnableLoginHistory.Load()
}

func loginHistoryRetainDuration() time.Duration {
	return variable.LoginHistoryRetainDuration.Load()
}

// TickGCSysTable ticks the gc system table, like mysql.login_history.
func (w *GCWorker) TickGCSysTable(ctx context.Context) error {
	if !enableLogHistory() {
		return nil
	}

	cur := time.Now()
	if cur.After(w.lastGCSysTable.Add(w.intervalGcSystable)) {
		// do the GC mysql.login_history
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
		sess := createSession(w.store)
		defer sess.Close()

		retainDur := loginHistoryRetainDuration()
		sql := `DELETE FROM mysql.login_history WHERE Time < NOW() - INTERVAL %? second LIMIT 50000`
		_, err := sess.ExecuteInternal(ctx, sql, uint64(retainDur.Seconds()))
		if err != nil {
			logutil.Logger(ctx).Warn("faild to execute sql", zap.String("sql", sql),
				zap.Duration("retain", retainDur), zap.Error(err))
			return err
		}
		// update lastGCSysTable finally.
		w.UpdateLastGCSystem(cur)
	}
	return nil
}

// UpdateLastGCSystem updates the timestamp of last GC system table.
func (w *GCWorker) UpdateLastGCSystem(update time.Time) {
	w.lastGCSysTable = update
}
