// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const insertNewTableIntoStatusTemplate = "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (%d, %d)"
const setTableStatusOwnerTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_id = UUID(), current_job_owner_id = '%s',current_job_start_time = '%s',current_job_status = 'waiting',current_job_status_update_time = '%s',current_job_ttl_expire = '%s',current_job_owner_hb_time = '%s' WHERE (current_job_owner_id IS NULL OR current_job_owner_hb_time < '%s') AND table_id = %d"
const updateHeartBeatTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = '%s' WHERE table_id = %d AND current_job_owner_id = '%s'"

const timeFormat = "2006-01-02 15:04:05"

func insertNewTableIntoStatusSQL(tableID int64, parentTableID int64) string {
	return fmt.Sprintf(insertNewTableIntoStatusTemplate, tableID, parentTableID)
}

func setTableStatusOwnerSQL(tableID int64, now time.Time, currentJobTTLExpire time.Time, maxHBTime time.Time, id string) string {
	return fmt.Sprintf(setTableStatusOwnerTemplate, id, now.Format(timeFormat), now.Format(timeFormat), currentJobTTLExpire.Format(timeFormat), now.Format(timeFormat), maxHBTime.Format(timeFormat), tableID)
}

func updateHeartBeatSQL(tableID int64, now time.Time, id string) string {
	return fmt.Sprintf(updateHeartBeatTemplate, now.Format(timeFormat), tableID, id)
}

// JobManager schedules and manages the ttl jobs on this instance
type JobManager struct {
	// the `runningJobs`, `scanWorkers` and `delWorkers` should be protected by mutex:
	// `runningJobs` will be shared in the state loop and schedule loop
	// `scanWorkers` and `delWorkers` can be modified by setting variables at any time
	baseWorker

	sessPool sessionPool

	// id is the ddl id of this instance
	id string

	store kv.Storage

	// the workers are shared between the loop goroutine and other sessions (e.g. manually resize workers through
	// setting variables)
	scanWorkers []worker
	delWorkers  []worker

	// infoSchemaCache and tableStatusCache are a cache stores the information from info schema and the tidb_ttl_table_status
	// table. They don't need to be protected by mutex, because they are only used in job loop goroutine.
	infoSchemaCache  *cache.InfoSchemaCache
	tableStatusCache *cache.TableStatusCache

	// runningJobs record all ttlJob waiting in local
	// when a job for a table is created, it could spawn several scan tasks. If there are too many scan tasks, and they cannot
	// be fully consumed by local scan workers, their states should be recorded in the runningJobs, so that we could continue
	// to poll scan tasks from the job in the future when there are scan workers in idle.
	runningJobs []*ttlJob

	delCh         chan *ttlDeleteTask
	notifyStateCh chan interface{}
}

// NewJobManager creates a new ttl job manager
func NewJobManager(id string, sessPool sessionPool, store kv.Storage) (manager *JobManager) {
	manager = &JobManager{}
	manager.id = id
	manager.store = store
	manager.sessPool = sessPool
	manager.delCh = make(chan *ttlDeleteTask)

	manager.init(manager.jobLoop)
	manager.ctx = logutil.WithKeyValue(manager.ctx, "ttl-worker", "manager")

	manager.infoSchemaCache = cache.NewInfoSchemaCache(updateInfoSchemaCacheInterval)
	manager.tableStatusCache = cache.NewTableStatusCache(updateTTLTableStatusCacheInterval)

	return
}

func (m *JobManager) jobLoop() error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.resizeScanWorkers(0), m.resizeDelWorkers(0)))
		se.Close()
	}()

	scheduleTicker := time.Tick(jobManagerLoopTickerInterval)
	updateHeartBeatTicker := time.Tick(jobManagerLoopTickerInterval)
	jobCheckTicker := time.Tick(jobManagerLoopTickerInterval)
	updateScanTaskStateTicker := time.Tick(jobManagerLoopTickerInterval)
	infoSchemaCacheUpdateTicker := time.Tick(m.infoSchemaCache.GetInterval())
	tableStatusCacheUpdateTicker := time.Tick(m.tableStatusCache.GetInterval())
	resizeWorkersTicker := time.Tick(resizeWorkersInterval)
	for {
		now := se.Now()

		select {
		case <-m.ctx.Done():
			return nil
		case <-infoSchemaCacheUpdateTicker:
			err := m.updateInfoSchemaCache(se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update info schema cache", zap.Error(err))
			}
		case <-tableStatusCacheUpdateTicker:
			err := m.updateTableStatusCache(se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update table status cache", zap.Error(err))
			}
		case <-updateHeartBeatTicker:
			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
			err = m.updateHeartBeat(updateHeartBeatCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update heart beat", zap.Error(err))
			}
			cancel()
		case <-updateScanTaskStateTicker:
			m.updateTaskState()
		case <-m.notifyStateCh:
			m.updateTaskState()
		case <-jobCheckTicker:
			m.checkFinishedJob(se, now)
			m.checkNotOwnJob()
		case <-resizeWorkersTicker:
			err := m.resizeScanWorkers(int(ScanWorkersCount.Load()))
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to resize scan workers", zap.Error(err))
			}
			err = m.resizeDelWorkers(int(DeleteWorkerCount.Load()))
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to resize delete workers", zap.Error(err))
			}
		case <-scheduleTicker:
			m.rescheduleJobs(se, now)
		}
	}
}

func (m *JobManager) resizeScanWorkers(count int) error {
	var err error
	m.scanWorkers, err = m.resizeWorkers(m.scanWorkers, count, func() worker {
		return newScanWorker(m.delCh, m.notifyStateCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeDelWorkers(count int) error {
	var err error
	m.delWorkers, err = m.resizeWorkers(m.delWorkers, count, func() worker {
		return newDeleteWorker(m.delCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeWorkers(workers []worker, count int, factory func() worker) ([]worker, error) {
	if count < len(workers) {
		logutil.Logger(m.ctx).Info("shrink ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for _, w := range workers[count:] {
			w.Stop()
		}
		var errs error
		for _, w := range workers[count:] {
			err := w.WaitStopped(m.ctx, 30*time.Second)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to stop ttl worker", zap.Error(err))
				errs = multierr.Append(errs, err)
			}
		}

		// remove the existing workers, and keep the left workers
		workers = workers[:count]
		return workers, errs
	}

	if count > len(workers) {
		logutil.Logger(m.ctx).Info("scale ttl worker", zap.Int("originalCount", len(workers)), zap.Int("newCount", count))

		for i := len(workers); i < count; i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
		return workers, nil
	}

	return workers, nil
}

func (m *JobManager) updateTaskState() {
	results := m.pollScanWorkerResults()
	for _, result := range results {
		job := findJobWithTableID(m.runningJobs, result.task.tbl.ID)
		if job != nil {
			logutil.Logger(m.ctx).Debug("scan task state changed", zap.String("jobID", job.id))

			job.finishedScanTaskCounter += 1
			job.scanTaskErr = multierr.Append(job.scanTaskErr, result.err)
		}
	}
}

func (m *JobManager) pollScanWorkerResults() []*ttlScanTaskExecResult {
	results := make([]*ttlScanTaskExecResult, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		worker := w.(*ttlScanWorker)
		result := worker.PollTaskResult()
		if result != nil {
			results = append(results, result)
		}
	}

	return results
}

// checkNotOwnJob removes the job whose current job owner is not yourself
func (m *JobManager) checkNotOwnJob() {
	for _, job := range m.runningJobs {
		tableStatus := m.tableStatusCache.Tables[job.tbl.ID]
		if tableStatus == nil || tableStatus.CurrentJobOwnerID != m.id {
			logutil.Logger(m.ctx).Info("job has been taken over by another node", zap.String("jobID", job.id), zap.String("statistics", job.statistics.String()))
			m.removeJob(job)
			job.cancel()
		}
	}
}

func (m *JobManager) checkFinishedJob(se session.Session, now time.Time) {
	for _, job := range m.runningJobs {
		timeoutJobCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
		if job.Finished() {
			logutil.Logger(m.ctx).Info("job has finished", zap.String("jobID", job.id), zap.String("statistics", job.statistics.String()))
			m.removeJob(job)
			job.finish(se, se.Now())
		} else if job.Timeout(timeoutJobCtx, se, now) {
			logutil.Logger(m.ctx).Info("job is timeout", zap.String("jobID", job.id), zap.String("statistics", job.statistics.String()))
			m.removeJob(job)
			err := job.Cancel(timeoutJobCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to cancel job", zap.Error(err))
			}
			job.finish(se, se.Now())
		}
		cancel()
	}
}

func (m *JobManager) rescheduleJobs(se session.Session, now time.Time) {
	if !timeutil.WithinDayTimePeriod(ttlJobScheduleWindowStartTime, ttlJobScheduleWindowEndTime, now) {
		// Local jobs will also not run, but as the server is still sending heartbeat,
		// and keep the job in memory, it could start the left task in the next window.
		return
	}

	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) == 0 {
		return
	}

	localJobs := m.localJobs()
	newJobTables := m.readyForNewJobTables(now)
	// TODO: also consider to resume tables, but it's fine to left them there, as other nodes will take this job
	// when the heart beat is not sent
	for len(idleScanWorkers) > 0 && (len(newJobTables) > 0 || len(localJobs) > 0) {
		var job *ttlJob
		var err error

		switch {
		case len(localJobs) > 0:
			job = localJobs[0]
			localJobs = localJobs[1:]
		case len(newJobTables) > 0:
			table := newJobTables[0]
			newJobTables = newJobTables[1:]
			logutil.Logger(m.ctx).Debug("try lock new job", zap.Int64("tableID", table.ID))
			job, err = m.lockNewJob(m.ctx, se, table, now)
			if job != nil {
				m.appendJob(job)
			}
		}
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to create new job", zap.Error(err))
		}
		if job == nil {
			continue
		}

		for !job.AllSpawned() {
			task, err := job.peekScanTask()
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to generate scan task", zap.Error(err))
				break
			}

			for len(idleScanWorkers) > 0 {
				idleWorker := idleScanWorkers[0]
				idleScanWorkers = idleScanWorkers[1:]

				err := idleWorker.Schedule(task)
				if err != nil {
					logutil.Logger(m.ctx).Info("fail to schedule task", zap.Error(err))
					continue
				}

				ctx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
				err = job.changeStatus(ctx, se, cache.JobStatusRunning)
				if err != nil {
					// not a big problem, current logic doesn't depend on the job status to promote
					// the routine, so we could just print a log here
					logutil.Logger(m.ctx).Error("change ttl job status", zap.Error(err), zap.String("id", job.id))
				}
				cancel()

				logArgs := []zap.Field{zap.String("table", task.tbl.TableInfo.Name.L)}
				if task.tbl.PartitionDef != nil {
					logArgs = append(logArgs, zap.String("partition", task.tbl.PartitionDef.Name.L))
				}
				logutil.Logger(m.ctx).Debug("schedule ttl task",
					logArgs...)

				job.nextScanTask()
				break
			}

			if len(idleScanWorkers) == 0 {
				break
			}
		}
	}
}

func (m *JobManager) idleScanWorkers() []scanWorker {
	workers := make([]scanWorker, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		if w.(scanWorker).Idle() {
			workers = append(workers, w.(scanWorker))
		}
	}
	return workers
}

func (m *JobManager) localJobs() []*ttlJob {
	for _, job := range m.runningJobs {
		status := m.tableStatusCache.Tables[job.tbl.ID]
		if status == nil || status.CurrentJobOwnerID != m.id {
			m.removeJob(job)
			continue
		}
	}
	return m.runningJobs
}

// readyForNewJobTables returns all tables which should spawn a TTL job according to cache
func (m *JobManager) readyForNewJobTables(now time.Time) []*cache.PhysicalTable {
	tables := make([]*cache.PhysicalTable, 0, len(m.infoSchemaCache.Tables))
	for _, table := range m.infoSchemaCache.Tables {
		status := m.tableStatusCache.Tables[table.ID]
		ok := m.couldTrySchedule(status, now)
		if ok {
			tables = append(tables, table)
		}
	}

	return tables
}

// couldTrySchedule returns whether a table should be tried to run TTL
func (m *JobManager) couldTrySchedule(table *cache.TableStatus, now time.Time) bool {
	if table == nil {
		// if the table status hasn't been created, return true
		return true
	}
	if table.CurrentJobOwnerID != "" {
		// see whether it's heart beat time is expired
		hbTime := table.CurrentJobOwnerHBTime
		// a more concrete value is `2 * max(updateTTLTableStatusCacheInterval, jobManagerLoopTickerInterval)`, but the
		// `updateTTLTableStatusCacheInterval` is greater than `jobManagerLoopTickerInterval` in most cases.
		if hbTime.Add(2 * updateTTLTableStatusCacheInterval).Before(now) {
			logutil.Logger(m.ctx).Info("task heartbeat has stopped", zap.Int64("tableID", table.TableID), zap.Time("hbTime", hbTime), zap.Time("now", now))
			return true
		}
		return false
	}

	if table.LastJobFinishTime.IsZero() {
		return true
	}

	finishTime := table.LastJobFinishTime

	return finishTime.Add(ttlJobInterval).Before(now)
}

// occupyNewJob tries to occupy a new job in the ttl_table_status table. If it locks successfully, it will create a new
// localJob and return it.
// It could be nil, nil, if the table query doesn't return error but the job has been locked by other instances.
func (m *JobManager) lockNewJob(ctx context.Context, se session.Session, table *cache.PhysicalTable, now time.Time) (*ttlJob, error) {
	maxHBTime := now.Add(-2 * jobManagerLoopTickerInterval)
	var expireTime time.Time

	err := se.RunInTxn(ctx, func() error {
		rows, err := se.ExecuteSQL(ctx, cache.SelectFromTTLTableStatusWithID(table.TableInfo.ID))
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			// cannot find the row, insert the status row
			_, err = se.ExecuteSQL(ctx, insertNewTableIntoStatusSQL(table.ID, table.TableInfo.ID))
			if err != nil {
				return err
			}
			rows, err = se.ExecuteSQL(ctx, cache.SelectFromTTLTableStatusWithID(table.TableInfo.ID))
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return errors.New("table status row still doesn't exist after insertion")
			}
		}
		tableStatus, err := cache.RowToTableStatus(se, rows[0])
		if err != nil {
			return err
		}
		if !m.couldTrySchedule(tableStatus, now) {
			return errors.New("couldn't schedule ttl job")
		}

		expireTime, err = table.EvalExpireTime(m.ctx, se, now)
		if err != nil {
			return err
		}

		_, err = se.ExecuteSQL(ctx, setTableStatusOwnerSQL(table.ID, now, expireTime, maxHBTime, m.id))

		return err
	})
	if err != nil {
		return nil, err
	}

	// successfully update the table status, will need to refresh the cache.
	err = m.updateInfoSchemaCache(se)
	if err != nil {
		return nil, err
	}
	err = m.updateTableStatusCache(se)
	if err != nil {
		return nil, err
	}
	return m.createNewJob(expireTime, now, table)
}

func (m *JobManager) createNewJob(expireTime time.Time, now time.Time, table *cache.PhysicalTable) (*ttlJob, error) {
	id := m.tableStatusCache.Tables[table.ID].CurrentJobID

	statistics := &ttlStatistics{}

	ranges, err := table.SplitScanRanges(m.ctx, m.store, splitScanCount)
	if err != nil {
		return nil, err
	}

	jobCtx, cancel := context.WithCancel(m.ctx)

	scanTasks := make([]*ttlScanTask, 0, len(ranges))
	for _, r := range ranges {
		scanTasks = append(scanTasks, &ttlScanTask{
			ctx:        jobCtx,
			tbl:        table,
			expire:     expireTime,
			scanRange:  r,
			statistics: statistics,
		})
	}

	return &ttlJob{
		id:      id,
		ownerID: m.id,

		ctx:    jobCtx,
		cancel: cancel,

		createTime: now,
		// at least, the info schema cache and table status cache are consistent in table id, so it's safe to get table
		// information from schema cache directly
		tbl:   table,
		tasks: scanTasks,

		status:     cache.JobStatusWaiting,
		statistics: statistics,
	}, nil
}

// updateHeartBeat updates the heartbeat for all task with current instance as owner
func (m *JobManager) updateHeartBeat(ctx context.Context, se session.Session) error {
	now := se.Now()
	for _, job := range m.localJobs() {
		_, err := se.ExecuteSQL(ctx, updateHeartBeatSQL(job.tbl.ID, now, m.id))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// updateInfoSchemaCache updates the cache of information schema
func (m *JobManager) updateInfoSchemaCache(se session.Session) error {
	return m.infoSchemaCache.Update(se)
}

// updateTableStatusCache updates the cache of table status
func (m *JobManager) updateTableStatusCache(se session.Session) error {
	cacheUpdateCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
	defer cancel()
	return m.tableStatusCache.Update(cacheUpdateCtx, se)
}

func (m *JobManager) removeJob(finishedJob *ttlJob) {
	for idx, job := range m.runningJobs {
		if job.id == finishedJob.id {
			if idx+1 < len(m.runningJobs) {
				m.runningJobs = append(m.runningJobs[0:idx], m.runningJobs[idx+1:]...)
			} else {
				m.runningJobs = m.runningJobs[0:idx]
			}
			return
		}
	}
}

func (m *JobManager) appendJob(job *ttlJob) {
	m.runningJobs = append(m.runningJobs, job)
}

// CancelJob cancels a job
// TODO: the delete task is not controlled by the context now (but controlled by the worker context), so cancel
// doesn't work for delete tasks.
func (m *JobManager) CancelJob(ctx context.Context, jobID string) error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	for _, job := range m.runningJobs {
		if job.id == jobID {
			return job.Cancel(ctx, se)
		}
	}

	return errors.Errorf("cannot find the job with id: %s", jobID)
}
