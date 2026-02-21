// Copyright 2023 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

func (m *taskManager) checkFinishedTask(se session.Session, now time.Time) {
	if len(m.runningTasks) == 0 {
		return
	}
	stillRunningTasks := make([]*runningScanTask, 0, len(m.runningTasks))
	for _, task := range m.runningTasks {
		if reason, resign := shouldRunningTaskResignOwner(task); resign {
			if !m.tryResignTaskOwner(se, task, reason, now) {
				stillRunningTasks = append(stillRunningTasks, task)
			}
			continue
		}

		if !task.finished(logutil.Logger(m.ctx)) {
			stillRunningTasks = append(stillRunningTasks, task)
			continue
		}
		// we should cancel task to release inner context and avoid memory leak
		task.cancel()
		err := m.reportTaskFinished(se, now, task)
		if err != nil {
			task.taskLogger(logutil.Logger(m.ctx)).Error("fail to report finished task", zap.Error(err))
			stillRunningTasks = append(stillRunningTasks, task)
			continue
		}
	}

	m.runningTasks = stillRunningTasks
}

func (m *taskManager) reportTaskFinished(se session.Session, now time.Time, task *runningScanTask) error {
	state := task.dumpNewTaskState()

	intest.Assert(se.GetSessionVars().Location().String() == now.Location().String())
	sql, args, err := setTTLTaskFinishedSQL(task.JobID, task.ScanID, state, now, m.id)
	if err != nil {
		return err
	}
	task.Status = cache.TaskStatusFinished

	timeoutCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
	_, err = se.ExecuteSQL(timeoutCtx, sql, args...)
	cancel()
	if err != nil {
		return err
	}
	if se.GetSessionVars().StmtCtx.AffectedRows() != 1 {
		return errors.Errorf("fail to update task status, maybe the owner is not myself (%s) or task is not running, affected rows: %d",
			m.id, se.GetSessionVars().StmtCtx.AffectedRows())
	}

	task.taskLogger(logutil.Logger(m.ctx)).Info(
		"TTL task finished",
		zap.Uint64("finalTotalRows", state.TotalRows),
		zap.Uint64("finalSuccessRows", state.SuccessRows),
		zap.Uint64("finalErrorRows", state.ErrorRows),
	)

	return nil
}

// checkInvalidTask removes the task whose owner is not myself or which has disappeared
func (m *taskManager) checkInvalidTask(se session.Session) {
	if len(m.runningTasks) == 0 {
		return
	}
	// TODO: optimize this function through cache or something else
	ownRunningTask := make([]*runningScanTask, 0, len(m.runningTasks))

	for _, task := range m.runningTasks {
		sql, args := cache.SelectFromTTLTaskWithID(task.JobID, task.ScanID)
		l := logutil.Logger(m.ctx)
		timeoutCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
		rows, err := se.ExecuteSQL(timeoutCtx, sql, args...)
		cancel()
		if err != nil {
			task.taskLogger(l).Warn("fail to execute sql", zap.String("sql", sql), zap.Any("args", args), zap.Error(err))
			task.cancel()
			continue
		}
		if len(rows) == 0 {
			task.taskLogger(l).Warn("didn't find task")
			task.cancel()
			continue
		}
		t, err := cache.RowToTTLTask(se.GetSessionVars().Location(), rows[0])
		if err != nil {
			task.taskLogger(l).Warn("fail to get task", zap.Error(err))
			task.cancel()
			continue
		}

		if t.OwnerID != m.id {
			task.taskLogger(l).Warn("task owner changed", zap.String("myOwnerID", m.id), zap.String("taskOwnerID", t.OwnerID))
			task.cancel()
			continue
		}

		ownRunningTask = append(ownRunningTask, task)
	}

	m.runningTasks = ownRunningTask
}

func (m *taskManager) reportMetrics() {
	scanningTaskCnt := 0
	deletingTaskCnt := 0
	for _, task := range m.runningTasks {
		if task.result != nil {
			scanningTaskCnt += 1
		} else {
			deletingTaskCnt += 1
		}
	}
	metrics.ScanningTaskCnt.Set(float64(scanningTaskCnt))
	metrics.DeletingTaskCnt.Set(float64(deletingTaskCnt))
}

func (m *taskManager) meetTTLRunningTask(count int, taskStatus cache.TaskStatus) bool {
	if taskStatus == cache.TaskStatusRunning {
		// always return true for already running task because it is already included in count
		return true
	}
	return getMaxRunningTasksLimit(m.store) > count
}

func getMaxRunningTasksLimit(store kv.Storage) int {
	ttlRunningTask := vardef.TTLRunningTasks.Load()
	if ttlRunningTask != -1 {
		return int(ttlRunningTask)
	}

	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return vardef.MaxConfigurableConcurrency
	}

	regionCache := tikvStore.GetRegionCache()
	if regionCache == nil {
		return vardef.MaxConfigurableConcurrency
	}

	limit := min(len(regionCache.GetStoresByType(tikvrpc.TiKV)), vardef.MaxConfigurableConcurrency)

	return limit
}

type runningScanTask struct {
	*ttlScanTask
	cancel func()
	result *ttlScanTaskExecResult
}

// Context returns context for the task and is only used by test now
func (t *runningScanTask) Context() context.Context {
	return t.ctx
}

// dumpNewTaskState dumps a new TTLTaskState which is used to update the task meta in the storage
func (t *runningScanTask) dumpNewTaskState() *cache.TTLTaskState {
	state := &cache.TTLTaskState{
		TotalRows:   t.statistics.TotalRows.Load(),
		SuccessRows: t.statistics.SuccessRows.Load(),
		ErrorRows:   t.statistics.ErrorRows.Load(),
	}

	if prevState := t.TTLTask.State; prevState != nil {
		// If a task was timeout and taken over by the current instance,
		// adding the previous state to the current state to make the statistics more accurate.
		state.TotalRows += prevState.SuccessRows + prevState.ErrorRows
		state.SuccessRows += prevState.SuccessRows
		state.ErrorRows += prevState.ErrorRows
	}

	if r := t.result; r != nil && r.err != nil {
		state.ScanTaskErr = r.err.Error()
	}

	return state
}

func (t *runningScanTask) finished(logger *zap.Logger) bool {
	if t.result == nil {
		// Scan task isn't finished
		return false
	}

	totalRows := t.statistics.TotalRows.Load()
	errRows := t.statistics.ErrorRows.Load()
	successRows := t.statistics.SuccessRows.Load()
	processedRows := successRows + errRows
	if processedRows == totalRows {
		// All rows are processed.
		t.taskLogger(logger).Info(
			"will mark TTL task finished because all scanned rows are processed",
			zap.Uint64("totalRows", totalRows),
			zap.Uint64("successRows", successRows),
			zap.Uint64("errorRows", errRows),
		)
		return true
	}

	if processedRows > totalRows {
		// All rows are processed but processed rows are more than total rows.
		// We still think it is finished.
		t.taskLogger(logger).Warn(
			"will mark TTL task finished but processed rows are more than total rows",
			zap.Uint64("totalRows", totalRows),
			zap.Uint64("successRows", successRows),
			zap.Uint64("errorRows", errRows),
		)
		return true
	}

	if time.Since(t.result.time) > waitTaskProcessRowsTimeout {
		// If the scan task is finished and not all rows are processed, we should wait a certain time to report the task.
		// After a certain time, if the rows are still not processed, we need to mark the task finished anyway to make
		// sure the TTL job does not hang.
		t.taskLogger(logger).Info(
			"will mark TTL task finished because timeout for waiting all scanned rows processed after scan task done",
			zap.Uint64("totalRows", totalRows),
			zap.Uint64("successRows", successRows),
			zap.Uint64("errorRows", errRows),
		)
		return true
	}

	return false
}
