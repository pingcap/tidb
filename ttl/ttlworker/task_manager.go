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
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/metrics"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const setTTLTaskOwnerTemplate = `UPDATE mysql.tidb_ttl_task
	SET owner_id = %?,
		owner_hb_time = %?,
		status = 'running',
		status_update_time = %?
	WHERE job_id = %? AND scan_id = %?`

func setTTLTaskOwnerSQL(jobID string, scanID int64, id string, now time.Time) (string, []interface{}) {
	return setTTLTaskOwnerTemplate, []interface{}{id, now.Format(timeFormat), now.Format(timeFormat), jobID, scanID}
}

const setTTLTaskFinishedTemplate = `UPDATE mysql.tidb_ttl_task
	SET status = 'finished',
		status_update_time = %?,
		state = %?
	WHERE job_id = %? AND scan_id = %?`

func setTTLTaskFinishedSQL(jobID string, scanID int64, state *cache.TTLTaskState, now time.Time) (string, []interface{}, error) {
	stateStr, err := json.Marshal(state)
	if err != nil {
		return "", nil, err
	}
	return setTTLTaskFinishedTemplate, []interface{}{now.Format(timeFormat), string(stateStr), jobID, scanID}, nil
}

const updateTTLTaskHeartBeatTempalte = `UPDATE mysql.tidb_ttl_task
    SET state = %?,
		owner_hb_time = %?
    WHERE job_id = %? AND scan_id = %?`

func updateTTLTaskHeartBeatSQL(jobID string, scanID int64, now time.Time, state *cache.TTLTaskState) (string, []interface{}, error) {
	stateStr, err := json.Marshal(state)
	if err != nil {
		return "", nil, err
	}
	return updateTTLTaskHeartBeatTempalte, []interface{}{string(stateStr), now.Format(timeFormat), jobID, scanID}, nil
}

// taskManager schedules and manages the ttl tasks on this instance
type taskManager struct {
	ctx      context.Context
	sessPool sessionPool

	id string

	scanWorkers []worker
	delWorkers  []worker

	infoSchemaCache *cache.InfoSchemaCache
	runningTasks    []*runningScanTask

	delCh         chan *ttlDeleteTask
	notifyStateCh chan interface{}
}

func newTaskManager(ctx context.Context, sessPool sessionPool, infoSchemaCache *cache.InfoSchemaCache, id string) *taskManager {
	return &taskManager{
		ctx:      logutil.WithKeyValue(ctx, "ttl-worker", "task-manager"),
		sessPool: sessPool,

		id: id,

		scanWorkers: []worker{},
		delWorkers:  []worker{},

		infoSchemaCache: infoSchemaCache,
		runningTasks:    []*runningScanTask{},

		delCh:         make(chan *ttlDeleteTask),
		notifyStateCh: make(chan interface{}, 1),
	}
}

func (m *taskManager) resizeWorkersWithSysVar() {
	err := m.resizeScanWorkers(int(variable.TTLScanWorkerCount.Load()))
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to resize scan workers", zap.Error(err))
	}
	err = m.resizeDelWorkers(int(variable.TTLDeleteWorkerCount.Load()))
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to resize delete workers", zap.Error(err))
	}
}

func (m *taskManager) resizeScanWorkers(count int) error {
	var err error
	var canceledWorkers []worker
	m.scanWorkers, canceledWorkers, err = m.resizeWorkers(m.scanWorkers, count, func() worker {
		return newScanWorker(m.delCh, m.notifyStateCh, m.sessPool)
	})
	for _, w := range canceledWorkers {
		s := w.(scanWorker)

		var jobID string
		var scanID int64
		var scanErr error
		result := s.PollTaskResult()
		if result != nil {
			jobID = result.task.JobID
			scanID = result.task.ScanID

			scanErr = result.err
		} else {
			// if the scan worker failed to poll the task, it's possible that the `WaitStopped` has timeout
			// we still consider the scan task as finished
			curTask := s.CurrentTask()
			if curTask == nil {
				continue
			}
			jobID = curTask.JobID
			scanID = curTask.ScanID
			scanErr = errors.New("timeout to cancel scan task")
		}

		task := findTaskWithID(m.runningTasks, jobID, scanID)
		if task == nil {
			logutil.Logger(m.ctx).Warn("task state changed but job not found", zap.String("jobID", jobID), zap.Int64("scanID", scanID))
			continue
		}
		logutil.Logger(m.ctx).Debug("scan task finished", zap.String("jobID", task.JobID), zap.Int64("taskID", task.ScanID), zap.Error(scanErr))

		task.result = result
	}
	return err
}

func findTaskWithID(tasks []*runningScanTask, jobID string, scanID int64) *runningScanTask {
	for _, t := range tasks {
		if t.ScanID == scanID && t.JobID == jobID {
			return t
		}
	}

	return nil
}

func (m *taskManager) resizeDelWorkers(count int) error {
	var err error
	m.delWorkers, _, err = m.resizeWorkers(m.delWorkers, count, func() worker {
		return newDeleteWorker(m.delCh, m.sessPool)
	})
	return err
}

// resizeWorkers scales the worker, and returns the full set of workers as the first return value. If there are workers
// stopped, return the stopped worker in the second return value
func (m *taskManager) resizeWorkers(workers []worker, count int, factory func() worker) ([]worker, []worker, error) {
	if count < len(workers) {
		logutil.Logger(m.ctx).Info("shrink ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for _, w := range workers[count:] {
			w.Stop()
		}

		var errs error
		// don't use `m.ctx` here, because when shutdown the server, `m.ctx` has already been cancelled
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for _, w := range workers[count:] {
			err := w.WaitStopped(ctx, 30*time.Second)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to stop ttl worker", zap.Error(err))
				errs = multierr.Append(errs, err)
			}
		}
		cancel()

		// remove the existing workers, and keep the left workers
		return workers[:count], workers[count:], errs
	}

	if count > len(workers) {
		logutil.Logger(m.ctx).Info("scale ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for i := len(workers); i < count; i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
		return workers, nil, nil
	}

	return workers, nil, nil
}

// handleScanFinishedTask polls the result from scan worker and returns whether there are result polled
func (m *taskManager) handleScanFinishedTask() bool {
	results := m.pollScanWorkerResults()
	for _, result := range results {
		logger := logutil.Logger(m.ctx).With(zap.Int64("tableID", result.task.tbl.ID), zap.String("jobID", result.task.JobID), zap.Int64("scanID", result.task.ScanID))
		if result.err != nil {
			logger = logger.With(zap.Error(result.err))
		}

		task := findTaskWithID(m.runningTasks, result.task.JobID, result.task.ScanID)
		if task == nil {
			logger.Warn("task state changed but task not found")
			continue
		}
		logger.Info("task scans finished")
		task.result = result
	}

	return len(results) > 0
}

func (m *taskManager) pollScanWorkerResults() []*ttlScanTaskExecResult {
	results := make([]*ttlScanTaskExecResult, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		worker := w.(scanWorker)
		result := worker.PollTaskResult()
		if result != nil {
			results = append(results, result)
		}
	}

	return results
}

func (m *taskManager) idleScanWorkers() []scanWorker {
	workers := make([]scanWorker, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		if w.(scanWorker).CouldSchedule() {
			workers = append(workers, w.(scanWorker))
		}
	}
	return workers
}

func (m *taskManager) rescheduleTasks(se session.Session, now time.Time) {
	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) == 0 {
		return
	}

	tasks, err := m.peekWaitingScanTasks(se, now)
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to peek scan task", zap.Error(err))
		return
	}

	for _, t := range tasks {
		logger := logutil.Logger(m.ctx).With(zap.String("jobID", t.JobID), zap.Int64("scanID", t.ScanID))

		task, err := m.lockScanTask(se, t, now)
		if err != nil {
			// If other nodes lock the task, it will return an error. It's expected
			// so the log level is only `info`
			logutil.Logger(m.ctx).Info("fail to lock scan task", zap.Error(err))
			continue
		}

		idleWorker := idleScanWorkers[0]
		idleScanWorkers = idleScanWorkers[1:]

		err = idleWorker.Schedule(task.ttlScanTask)
		if err != nil {
			logger.Warn("fail to schedule task", zap.Error(err))
			continue
		}

		logger.Info("scheduled ttl task")
		m.runningTasks = append(m.runningTasks, task)

		if len(idleScanWorkers) == 0 {
			return
		}
	}
}

func (m *taskManager) peekWaitingScanTasks(se session.Session, now time.Time) ([]*cache.TTLTask, error) {
	sql, args := cache.PeekWaitingTTLTask(now.Add(-2 * ttlTaskHeartBeatTickerInterval))
	rows, err := se.ExecuteSQL(m.ctx, sql, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute sql: %s", sql)
	}

	tasks := make([]*cache.TTLTask, 0, len(rows))
	for _, r := range rows {
		task, err := cache.RowToTTLTask(se, r)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (m *taskManager) lockScanTask(se session.Session, task *cache.TTLTask, now time.Time) (*runningScanTask, error) {
	ctx := m.ctx

	table, ok := m.infoSchemaCache.Tables[task.TableID]
	if !ok {
		return nil, errors.Errorf("didn't find table with id: %d", task.TableID)
	}

	err := se.RunInTxn(ctx, func() error {
		var err error

		task, err = m.syncTaskFromTable(se, task.JobID, task.ScanID, true)
		if err != nil {
			return err
		}
		if task.OwnerID != "" && !task.OwnerHBTime.Add(2*jobManagerLoopTickerInterval).Before(now) {
			return errors.New("task is already scheduled")
		}

		sql, args := setTTLTaskOwnerSQL(task.JobID, task.ScanID, m.id, now)
		_, err = se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		return nil
	}, session.TxnModePessimistic)
	if err != nil {
		return nil, err
	}

	// update the task after setting status and owner
	task, err = m.syncTaskFromTable(se, task.JobID, task.ScanID, false)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(m.ctx)
	scanTask := &ttlScanTask{
		ctx: ctx,

		TTLTask: task,

		tbl:        table,
		statistics: &ttlStatistics{},
	}
	return &runningScanTask{
		scanTask,
		cancel,
		nil,
	}, nil
}

func (m *taskManager) syncTaskFromTable(se session.Session, jobID string, scanID int64, detectLock bool) (*cache.TTLTask, error) {
	ctx := m.ctx

	sql, args := cache.SelectFromTTLTaskWithID(jobID, scanID)
	if detectLock {
		sql += " FOR UPDATE NOWAIT"
	}
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute sql: %s", sql)
	}
	if len(rows) == 0 {
		return nil, errors.Errorf("didn't find task with jobID: %s, scanID: %d", jobID, scanID)
	}
	task, err := cache.RowToTTLTask(se, rows[0])
	if err != nil {
		return nil, err
	}

	return task, nil
}

// updateHeartBeat updates the heartbeat for all tasks with current instance as owner
func (m *taskManager) updateHeartBeat(ctx context.Context, se session.Session, now time.Time) error {
	for _, task := range m.runningTasks {
		state := &cache.TTLTaskState{
			TotalRows:   task.statistics.TotalRows.Load(),
			SuccessRows: task.statistics.SuccessRows.Load(),
			ErrorRows:   task.statistics.ErrorRows.Load(),
		}
		if task.result != nil && task.result.err != nil {
			state.ScanTaskErr = task.result.err.Error()
		}

		sql, args, err := updateTTLTaskHeartBeatSQL(task.JobID, task.ScanID, now, state)
		if err != nil {
			return err
		}
		_, err = se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}
	}
	return nil
}

func (m *taskManager) checkFinishedTask(se session.Session, now time.Time) {
	stillRunningTasks := make([]*runningScanTask, 0, len(m.runningTasks))
	for _, task := range m.runningTasks {
		if !task.finished() {
			stillRunningTasks = append(stillRunningTasks, task)
			continue
		}
		err := m.reportTaskFinished(se, now, task)
		if err != nil {
			logutil.Logger(m.ctx).Error("fail to report finished task", zap.Error(err))
			stillRunningTasks = append(stillRunningTasks, task)
			continue
		}
	}

	m.runningTasks = stillRunningTasks
}

func (m *taskManager) reportTaskFinished(se session.Session, now time.Time, task *runningScanTask) error {
	state := &cache.TTLTaskState{
		TotalRows:   task.statistics.TotalRows.Load(),
		SuccessRows: task.statistics.SuccessRows.Load(),
		ErrorRows:   task.statistics.ErrorRows.Load(),
	}
	if task.result.err != nil {
		state.ScanTaskErr = task.result.err.Error()
	}

	sql, args, err := setTTLTaskFinishedSQL(task.JobID, task.ScanID, state, now)
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

	return nil
}

// checkInvalidTask removes the task whose owner is not myself or which has disappeared
func (m *taskManager) checkInvalidTask(se session.Session) {
	// TODO: optimize this function through cache or something else
	ownRunningTask := make([]*runningScanTask, 0, len(m.runningTasks))

	for _, task := range m.runningTasks {
		sql, args := cache.SelectFromTTLTaskWithID(task.JobID, task.ScanID)

		timeoutCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
		rows, err := se.ExecuteSQL(timeoutCtx, sql, args...)
		cancel()
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to execute sql", zap.String("sql", sql), zap.Any("args", args), zap.Error(err))
			task.cancel()
			continue
		}
		if len(rows) == 0 {
			logutil.Logger(m.ctx).Warn("didn't find task", zap.String("jobID", task.JobID), zap.Int64("scanID", task.ScanID))
			task.cancel()
			continue
		}
		t, err := cache.RowToTTLTask(se, rows[0])
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to get task", zap.Error(err))
			task.cancel()
			continue
		}

		if t.OwnerID == m.id {
			ownRunningTask = append(ownRunningTask, task)
		}
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

type runningScanTask struct {
	*ttlScanTask
	cancel func()
	result *ttlScanTaskExecResult
}

func (t *runningScanTask) finished() bool {
	return t.result != nil && t.statistics.TotalRows.Load() == t.statistics.ErrorRows.Load()+t.statistics.SuccessRows.Load()
}
