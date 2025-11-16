// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	distproto "github.com/pingcap/tidb/pkg/disttask/framework/proto"
	diststorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"go.uber.org/zap"
)

// Default intervals/thresholds for orphan backfill detection.
var (
	defaultOrphanCheckInterval = 10 * time.Minute
	defaultOrphanAge           = 24 * time.Hour
)

// orphanBackfillCleanupLoop periodically detects and cancels orphaned backfill tasks
// that no longer have a parent DDL job (neither in active jobs nor in history).
func (d *ddl) orphanBackfillCleanupLoop() {
	logger := logutil.DDLLogger().With(zap.String("component", "orphanBackfillCleanup"))
	logger.Info("orphan backfill cleanup loop start")

	interval := defaultOrphanCheckInterval
	failpoint.Inject("orphanBackfillCheckInterval", func(v failpoint.Value) {
		if sec, ok := v.(int); ok && sec > 0 {
			interval = time.Duration(sec) * time.Second
		}
	})
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Do an immediate pass on startup (only by owner) so we don't have to wait for the first tick.
	// In tests we may bypass the owner check via failpoint.
	shouldRun := d.ownerManager.IsOwner()
	failpoint.Inject("orphanBackfillIgnoreOwner", func(_ failpoint.Value) {
		shouldRun = true
	})
	if shouldRun {
		if err := d.orphanBackfillReconcileOnce(d.ctx); err != nil {
			logger.Warn("orphan backfill cleanup encountered error", zap.Error(err))
		}
	}

	for {
		select {
		case <-d.ctx.Done():
			logger.Info("orphan backfill cleanup loop exit")
			return
		case <-ticker.C:
			// Only DDL owner performs cleanup to avoid redundant work.
			shouldRun := d.ownerManager.IsOwner()
			failpoint.Inject("orphanBackfillIgnoreOwner", func(_ failpoint.Value) {
				shouldRun = true
			})
			if !shouldRun {
				continue
			}
			if err := d.orphanBackfillReconcileOnce(d.ctx); err != nil {
				logger.Warn("orphan backfill cleanup encountered error", zap.Error(err))
			}
		}
	}
}

// orphanBackfillReconcileOnce performs a single pass of orphan detection and cancellation.
func (d *ddl) orphanBackfillReconcileOnce(ctx context.Context) error {
	taskMgr, err := diststorage.GetTaskManager()
	if err != nil {
		return err
	}
	// Consider unfinished states (consistent with GetTopUnfinishedTasks).
	tasks, err := taskMgr.GetTasksInStates(ctx,
		distproto.TaskStatePending,
		distproto.TaskStateRunning,
		distproto.TaskStateReverting,
		distproto.TaskStateCancelling,
		distproto.TaskStatePausing,
		distproto.TaskStateResuming,
		distproto.TaskStateModifying,
	)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		logutil.DDLLogger().Debug("orphan backfill reconcile: no unfinished tasks found")
		return nil
	}
	logutil.DDLLogger().Debug("orphan backfill reconcile: fetched unfinished tasks", zap.Int("count", len(tasks)))
	ageThreshold := defaultOrphanAge
	failpoint.Inject("orphanBackfillAgeSeconds", func(v failpoint.Value) {
		if sec, ok := v.(int); ok && sec >= 0 {
			ageThreshold = time.Duration(sec) * time.Second
		}
	})
	now := time.Now()
	cancelled := 0

	for _, t := range tasks {
		// Only handle backfill tasks (by type or key pattern, to be robust).
		if t.Type != distproto.Backfill && !strings.Contains(t.Key, "/backfill/") {
			continue
		}
		// log candidate task briefly
		logutil.DDLLogger().Debug("orphan backfill candidate",
			zap.Int64("taskID", t.ID), zap.String("taskKey", t.Key), zap.Stringer("state", t.State))
		// Guard against very new tasks; give normal flows a chance first.
		if ageThreshold > 0 && now.Sub(t.CreateTime) < ageThreshold {
			continue
		}
		jobID, ok := parseDDLJobIDFromTaskKey(t.Key)
		if !ok || jobID <= 0 {
			// Unknown pattern; skip.
			continue
		}
		// If job exists in active table, skip.
		if _, err := d.sysTblMgr.GetJobByID(ctx, jobID); err == nil {
			continue
		}
		// If job exists in history, skip. Use DDL session pool to avoid import cycle.
		if seCtx, err := d.sessPool.Get(); err == nil {
			histJob, hErr := GetHistoryJobByID(seCtx, jobID)
			d.sessPool.Put(seCtx)
			if hErr == nil && histJob != nil {
				continue
			}
		}
		// Otherwise, cancel the task to trigger scheduler revert->reverted, then normal cleanup moves it to history.
		logutil.DDLLogger().Info("orphan backfill detected, cancelling task",
			zap.Int64("taskID", t.ID), zap.String("taskKey", t.Key))
		if err := taskMgr.CancelTask(ctx, t.ID); err != nil {
			// Log and continue to next.
			logutil.DDLLogger().Warn("failed to cancel orphan backfill task",
				zap.Int64("taskID", t.ID), zap.String("taskKey", t.Key), zap.Error(err))
			continue
		}
		cancelled++
	}
	if cancelled > 0 {
		logutil.DDLLogger().Info("orphan backfill cleanup cancelled tasks",
			zap.Int("count", cancelled))
	}
	return nil
}

// parseDDLJobIDFromTaskKey extracts the DDL job ID from a backfill task key.
// Expected format: "ddl/backfill/<jobID>"
func parseDDLJobIDFromTaskKey(key string) (int64, bool) {
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return 0, false
	}
	// .../backfill/<jobID>
	if parts[len(parts)-2] != "backfill" {
		return 0, false
	}
	idStr := parts[len(parts)-1]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// ForTest_OrphanBackfillReconcileOnceFromInterface triggers one orphan reconciliation
// immediately on the provided DDL instance. Test-only; avoids owner/ticker timing.
func ForTest_OrphanBackfillReconcileOnceFromInterface(d DDL) error {
	impl, ok := d.(*ddl)
	if !ok {
		return nil
	}
	return impl.orphanBackfillReconcileOnce(impl.ctx)
}


