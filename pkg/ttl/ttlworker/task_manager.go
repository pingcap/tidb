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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const setTTLTaskOwnerTemplate = `UPDATE mysql.tidb_ttl_task
	SET owner_id = %?,
		owner_hb_time = %?,
		status = 'running',
		status_update_time = %?
	WHERE job_id = %? AND scan_id = %?`

func setTTLTaskOwnerSQL(jobID string, scanID int64, id string, now time.Time) (string, []any) {
	return setTTLTaskOwnerTemplate, []any{id, now.Format(timeFormat), now.Format(timeFormat), jobID, scanID}
}

const setTTLTaskFinishedTemplate = `UPDATE mysql.tidb_ttl_task
	SET status = 'finished',
		status_update_time = %?,
		state = %?
	WHERE job_id = %? AND scan_id = %? AND status = 'running' AND owner_id = %?`

func setTTLTaskFinishedSQL(jobID string, scanID int64, state *cache.TTLTaskState, now time.Time, ownerID string) (string, []any, error) {
	stateStr, err := json.Marshal(state)
	if err != nil {
		return "", nil, err
	}
	return setTTLTaskFinishedTemplate, []any{now.Format(timeFormat), string(stateStr), jobID, scanID, ownerID}, nil
}

const updateTTLTaskHeartBeatTemplate = `UPDATE mysql.tidb_ttl_task
    SET state = %?,
		owner_hb_time = %?
    WHERE job_id = %? AND scan_id = %? AND owner_id = %?`

func updateTTLTaskHeartBeatSQL(jobID string, scanID int64, now time.Time, state *cache.TTLTaskState, ownerID string) (string, []any, error) {
	stateStr, err := json.Marshal(state)
	if err != nil {
		return "", nil, err
	}
	return updateTTLTaskHeartBeatTemplate, []any{string(stateStr), now.Format(timeFormat), jobID, scanID, ownerID}, nil
}

const resignOwnerSQLTemplate = `UPDATE mysql.tidb_ttl_task
    SET state = %?,
        status = 'waiting',
        owner_id = NULL,
        status_update_time = %?,
		owner_hb_time = NULL
    WHERE job_id = %? AND scan_id = %? AND owner_id = %?`

func resignOwnerSQL(jobID string, scanID int64, now time.Time, state *cache.TTLTaskState, ownerID string) (string, []any, error) {
	stateStr, err := json.Marshal(state)
	if err != nil {
		return "", nil, err
	}
	return resignOwnerSQLTemplate, []any{string(stateStr), now.Format(timeFormat), jobID, scanID, ownerID}, nil
}

const countRunningTasks = "SELECT count(1) FROM mysql.tidb_ttl_task WHERE status = 'running'"

// waitTaskProcessRowTimeout is the timeout for waiting the task to process all rows after a scan task finished.
// If not all rows are processed after this timeout, the task will still be marked as finished.
const waitTaskProcessRowsTimeout = 5 * time.Minute

var errAlreadyScheduled = errors.New("task is already scheduled")
var errTooManyRunningTasks = errors.New("there are too many running tasks")

// taskManager schedules and manages the ttl tasks on this instance
type taskManager struct {
	ctx      context.Context
	sessPool syssession.Pool

	id string

	store kv.Storage

	scanWorkers []worker
	delWorkers  []worker

	infoSchemaCache *cache.InfoSchemaCache
	runningTasks    []*runningScanTask

	delCh         chan *ttlDeleteTask
	notifyStateCh chan any
}

func newTaskManager(ctx context.Context, sessPool syssession.Pool, infoSchemaCache *cache.InfoSchemaCache, id string, store kv.Storage) *taskManager {
	ctx = logutil.WithKeyValue(ctx, "ttl-worker", "task-manager")
	if intest.InTest {
		// in test environment, in the same log there will be multiple ttl managers, so we need to distinguish them
		ctx = logutil.WithKeyValue(ctx, "ttl-worker", id)
	}
	return &taskManager{
		ctx:      ctx,
		sessPool: sessPool,

		id: id,

		store: store,

		scanWorkers: []worker{},
		delWorkers:  []worker{},

		infoSchemaCache: infoSchemaCache,
		runningTasks:    []*runningScanTask{},

		delCh:         make(chan *ttlDeleteTask),
		notifyStateCh: make(chan any, 1),
	}
}

func (m *taskManager) resizeWorkersWithSysVar() {
	err := m.resizeScanWorkers(int(vardef.TTLScanWorkerCount.Load()))
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to resize scan workers", zap.Error(err))
	}
	err = m.resizeDelWorkers(int(vardef.TTLDeleteWorkerCount.Load()))
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

			result = curTask.result(scanErr)
			result.reason = ReasonWorkerStop
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

var waitWorkerStopTimeout = 30 * time.Second

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
		ctx, cancel := context.WithTimeout(context.Background(), waitWorkerStopTimeout)
		for _, w := range workers[count:] {
			err := w.WaitStopped(ctx, waitWorkerStopTimeout)
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
		logger := result.task.taskLogger(logutil.Logger(m.ctx))
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

	if len(tasks) == 0 {
		return
	}

	err = m.infoSchemaCache.Update(se)
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to update infoSchemaCache", zap.Error(err))
		return
	}
loop:
	for _, t := range tasks {
		logger := logutil.Logger(m.ctx).With(
			zap.String("jobID", t.JobID),
			zap.Int64("scanID", t.ScanID),
			zap.Int64("tableID", t.TableID),
		)

		task, err := m.lockScanTask(se, t, now)
		if err != nil {
			switch errors.Cause(err) {
			case errAlreadyScheduled:
				continue
			case errTooManyRunningTasks:
				break loop
			case storeerr.ErrLockWaitTimeout:
				// don't step into the next step to avoid exceeding the limit
				break loop
			}
			logger.Warn("fail to lock scan task", zap.Error(err))
			continue
		}

		idleWorker := idleScanWorkers[0]
		idleScanWorkers = idleScanWorkers[1:]

		err = idleWorker.Schedule(task.ttlScanTask)
		if err != nil {
			logger.Warn("fail to schedule task", zap.Error(err))
			task.cancel()
			continue
		}

		var prevTotalRows, prevSuccessRows, prevErrorRows uint64
		if state := task.TTLTask.State; state != nil {
			prevTotalRows = state.TotalRows
			prevSuccessRows = state.SuccessRows
			prevErrorRows = state.ErrorRows
		}

		logger.Info(
			"scheduled ttl task",
			zap.Uint64("prevTotalRows", prevTotalRows),
			zap.Uint64("prevSuccessRows", prevSuccessRows),
			zap.Uint64("prevErrorRows", prevErrorRows),
		)
		m.runningTasks = append(m.runningTasks, task)

		if len(idleScanWorkers) == 0 {
			return
		}
	}
}

func (m *taskManager) peekWaitingScanTasks(se session.Session, now time.Time) ([]*cache.TTLTask, error) {
	intest.Assert(se.GetSessionVars().Location().String() == now.Location().String())
	sql, args := cache.PeekWaitingTTLTask(now.Add(-2 * getTaskManagerHeartBeatInterval()))
	rows, err := se.ExecuteSQL(m.ctx, sql, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute sql: %s", sql)
	}

	tasks := make([]*cache.TTLTask, 0, len(rows))
	for _, r := range rows {
		task, err := cache.RowToTTLTask(se.GetSessionVars().Location(), r)
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

		// The `for update` here ensures two things:
		// 1. The instance which will update this row has committed, and we can get the newest information.
		// 2. There is only one successful transaction concurrently.
		//    If it's a `for update nowait`, we cannot avoid the situation that multiple transactions fetched the tasks
		//    at the same time, and the task limitation doesn't work.
		task, err = m.syncTaskFromTable(se, task.JobID, task.ScanID, true)
		if err != nil {
			return err
		}
		if task.OwnerID != "" && !task.OwnerHBTime.Add(2*getTaskManagerHeartBeatInterval()).Before(now) {
			return errors.WithStack(errAlreadyScheduled)
		}

		// check the count of running task is smaller or equal than the limit
		rows, err := se.ExecuteSQL(ctx, countRunningTasks)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", countRunningTasks)
		}
		if !m.meetTTLRunningTask(int(rows[0].GetInt64(0)), task.Status) {
			return errors.WithStack(errTooManyRunningTasks)
		}

		if task.OwnerID != "" {
			logutil.Logger(m.ctx).Info(
				"try to lock a heartbeat timeout task",
				zap.String("jobID", task.JobID),
				zap.Int64("scanID", task.ScanID),
				zap.String("prevOwner", task.OwnerID),
				zap.Time("lastHeartbeat", task.OwnerHBTime),
			)
		} else if task.State != nil && task.State.PreviousOwner != "" {
			logutil.Logger(m.ctx).Info(
				"try to lock a task resigned from another instance",
				zap.String("jobID", task.JobID),
				zap.Int64("scanID", task.ScanID),
				zap.String("prevOwner", task.State.PreviousOwner),
			)
		}

		intest.Assert(se.GetSessionVars().Location().String() == now.Location().String())
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

func (m *taskManager) syncTaskFromTable(se session.Session, jobID string, scanID int64, waitLock bool) (*cache.TTLTask, error) {
	ctx := m.ctx

	sql, args := cache.SelectFromTTLTaskWithID(jobID, scanID)
	if waitLock {
		sql += " FOR UPDATE"
	}
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute sql: %s", sql)
	}
	if len(rows) == 0 {
		return nil, errors.Errorf("didn't find task with jobID: %s, scanID: %d", jobID, scanID)
	}
	task, err := cache.RowToTTLTask(se.GetSessionVars().Location(), rows[0])
	if err != nil {
		return nil, err
	}

	return task, nil
}

// updateHeartBeat updates the heartbeat for all tasks with current instance as owner
func (m *taskManager) updateHeartBeat(ctx context.Context, se session.Session, now time.Time) {
	for _, task := range m.runningTasks {
		err := m.taskHeartbeatOrResignOwner(ctx, se, now, task, false)
		if err != nil {
			task.taskLogger(logutil.Logger(m.ctx)).Warn("fail to update task heart beat", zap.Error(err))
		}
	}
}

func (m *taskManager) taskHeartbeatOrResignOwner(ctx context.Context, se session.Session, now time.Time, task *runningScanTask, isResignOwner bool) error {
	state := task.dumpNewTaskState()

	intest.Assert(se.GetSessionVars().Location().String() == now.Location().String())
	buildSQLFunc := updateTTLTaskHeartBeatSQL
	if isResignOwner {
		state.PreviousOwner = m.id
		buildSQLFunc = resignOwnerSQL
	}

	sql, args, err := buildSQLFunc(task.JobID, task.ScanID, now, state, m.id)
	if err != nil {
		return err
	}
	_, err = se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return errors.Wrapf(err, "execute sql: %s", sql)
	}

	if se.GetSessionVars().StmtCtx.AffectedRows() != 1 {
		return errors.Errorf("fail to update task status, maybe the owner is not myself (%s), affected rows: %d",
			m.id, se.GetSessionVars().StmtCtx.AffectedRows())
	}

	return nil
}

func shouldRunningTaskResignOwner(task *runningScanTask) (string, bool) {
	if result := task.result; result != nil {
		switch result.reason {
		case ReasonWorkerStop:
			return string(result.reason), true
		}
	}
	return "", false
}

func (m *taskManager) tryResignTaskOwner(se session.Session, task *runningScanTask, reason string, now time.Time) bool {
	var totalRows, successRows, errRows, processedRows uint64
	if stats := task.statistics; stats != nil {
		totalRows = stats.TotalRows.Load()
		successRows = stats.SuccessRows.Load()
		errRows = stats.ErrorRows.Load()
		processedRows = successRows + errRows
	}

	var taskEndTime time.Time
	if r := task.result; r != nil {
		taskEndTime = r.time
	}

	logger := task.taskLogger(logutil.Logger(m.ctx)).With(
		zap.Time("taskEndTime", taskEndTime),
		zap.String("reason", reason),
		zap.Uint64("totalRows", totalRows),
		zap.Uint64("processedRows", processedRows),
		zap.Uint64("successRows", successRows),
		zap.Uint64("errRows", errRows),
	)

	sinceTaskEnd := now.Sub(taskEndTime)
	if sinceTaskEnd < 0 {
		logger.Warn("task end time is after current time, something may goes wrong")
	}

	if totalRows > processedRows && sinceTaskEnd < 10*time.Second && sinceTaskEnd > -10*time.Second {
		logger.Info("wait all rows processed before resign the owner of a TTL task")
		return false
	}

	if totalRows > processedRows {
		logger.Info("wait all rows processed timeout, force to resign the owner of a TTL task")
	} else {
		logger.Info("resign the owner of a TTL task")
	}

	task.cancel()
	// Update the task state with heartbeatTask for the last time.
	// The task will be taken over by another instance after timeout.
	if err := m.taskHeartbeatOrResignOwner(m.ctx, se, now, task, true); err != nil {
		logger.Warn("fail to update the state before resign the task owner", zap.Error(err))
	}

	return true
}

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

	limit := len(regionCache.GetStoresByType(tikvrpc.TiKV))
	if limit > vardef.MaxConfigurableConcurrency {
		limit = vardef.MaxConfigurableConcurrency
	}

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
