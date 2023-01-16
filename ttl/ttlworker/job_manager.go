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
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/duration"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/client"
	"github.com/pingcap/tidb/ttl/metrics"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const insertNewTableIntoStatusTemplate = "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (%?, %?)"
const setTableStatusOwnerTemplate = `UPDATE mysql.tidb_ttl_table_status
	SET current_job_id = %?,
		current_job_owner_id = %?,
		current_job_start_time = %?,
		current_job_status = 'waiting',
		current_job_status_update_time = %?,
		current_job_ttl_expire = %?,
		current_job_owner_hb_time = %?
	WHERE table_id = %?`
const updateHeartBeatTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = %? WHERE table_id = %? AND current_job_owner_id = %?"

const timeFormat = "2006-01-02 15:04:05"

func insertNewTableIntoStatusSQL(tableID int64, parentTableID int64) (string, []interface{}) {
	return insertNewTableIntoStatusTemplate, []interface{}{tableID, parentTableID}
}

func setTableStatusOwnerSQL(uuid string, tableID int64, now time.Time, currentJobTTLExpire time.Time, id string) (string, []interface{}) {
	return setTableStatusOwnerTemplate, []interface{}{uuid, id, now.Format(timeFormat), now.Format(timeFormat), currentJobTTLExpire.Format(timeFormat), now.Format(timeFormat), tableID}
}

func updateHeartBeatSQL(tableID int64, now time.Time, id string) (string, []interface{}) {
	return updateHeartBeatTemplate, []interface{}{now.Format(timeFormat), tableID, id}
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

	store  kv.Storage
	cmdCli client.CommandClient

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
func NewJobManager(id string, sessPool sessionPool, store kv.Storage, etcdCli *clientv3.Client) (manager *JobManager) {
	manager = &JobManager{}
	manager.id = id
	manager.store = store
	manager.sessPool = sessPool
	manager.delCh = make(chan *ttlDeleteTask)
	manager.notifyStateCh = make(chan interface{}, 1)

	manager.init(manager.jobLoop)
	manager.ctx = logutil.WithKeyValue(manager.ctx, "ttl-worker", "manager")

	manager.infoSchemaCache = cache.NewInfoSchemaCache(getUpdateInfoSchemaCacheInterval())
	manager.tableStatusCache = cache.NewTableStatusCache(getUpdateTTLTableStatusCacheInterval())

	if etcdCli != nil {
		manager.cmdCli = client.NewEtcdCommandClient(etcdCli)
	} else {
		manager.cmdCli = client.NewMockCommandClient()
	}

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
		logutil.Logger(m.ctx).Info("ttlJobManager loop exited.")
	}()

	scheduleTicker := time.Tick(jobManagerLoopTickerInterval)
	updateHeartBeatTicker := time.Tick(jobManagerLoopTickerInterval)
	jobCheckTicker := time.Tick(jobManagerLoopTickerInterval)
	updateScanTaskStateTicker := time.Tick(jobManagerLoopTickerInterval)
	infoSchemaCacheUpdateTicker := time.Tick(m.infoSchemaCache.GetInterval())
	tableStatusCacheUpdateTicker := time.Tick(m.tableStatusCache.GetInterval())
	resizeWorkersTicker := time.Tick(getResizeWorkersInterval())
	cmdWatcher := m.cmdCli.WatchCommand(m.ctx)
	m.resizeWorkersWithSysVar()
	for {
		m.reportMetrics()
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
			if m.updateTaskState() {
				m.checkFinishedJob(se, now)
				m.rescheduleJobs(se, now)
			}
		case <-m.notifyStateCh:
			if m.updateTaskState() {
				m.checkFinishedJob(se, now)
				m.rescheduleJobs(se, now)
			}
		case <-jobCheckTicker:
			m.checkFinishedJob(se, now)
			m.checkNotOwnJob()
		case <-resizeWorkersTicker:
			m.resizeWorkersWithSysVar()
		case <-scheduleTicker:
			m.rescheduleJobs(se, now)
		case cmd, ok := <-cmdWatcher:
			if !ok {
				if m.ctx.Err() != nil {
					return nil
				}

				logutil.BgLogger().Warn("The TTL cmd watcher is closed unexpectedly, re-watch it again")
				cmdWatcher = m.cmdCli.WatchCommand(m.ctx)
				continue
			}

			if triggerJobCmd, ok := cmd.GetTriggerTTLJobRequest(); ok {
				m.triggerTTLJob(cmd.RequestID, triggerJobCmd, se)
				m.rescheduleJobs(se, now)
			}
		}
	}
}

func (m *JobManager) resizeWorkersWithSysVar() {
	err := m.resizeScanWorkers(int(variable.TTLScanWorkerCount.Load()))
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to resize scan workers", zap.Error(err))
	}
	err = m.resizeDelWorkers(int(variable.TTLDeleteWorkerCount.Load()))
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to resize delete workers", zap.Error(err))
	}
}

func (m *JobManager) triggerTTLJob(requestID string, cmd *client.TriggerNewTTLJobRequest, se session.Session) {
	if len(m.runningJobs) > 0 {
		// sleep 2 seconds to make sure the TiDB without any job running in it to have a higher priority to take a new job.
		time.Sleep(2 * time.Second)
	}

	ok, err := m.cmdCli.TakeCommand(m.ctx, requestID)
	if err != nil {
		logutil.BgLogger().Error("failed to take TTL trigger job command",
			zap.String("requestID", requestID),
			zap.String("database", cmd.DBName),
			zap.String("table", cmd.TableName))
		return
	}

	if !ok {
		return
	}

	logutil.BgLogger().Info("Get a command to trigger a new TTL job",
		zap.String("requestID", requestID),
		zap.String("database", cmd.DBName),
		zap.String("table", cmd.TableName))

	responseErr := func(err error) {
		terror.Log(m.cmdCli.ResponseCommand(m.ctx, requestID, err))
	}

	if err = m.infoSchemaCache.Update(se); err != nil {
		responseErr(err)
		return
	}

	if err = m.tableStatusCache.Update(m.ctx, se); err != nil {
		responseErr(err)
		return
	}

	var tables []*cache.PhysicalTable
	for _, tbl := range m.infoSchemaCache.Tables {
		if tbl.Schema.L == strings.ToLower(cmd.DBName) && tbl.Name.L == strings.ToLower(cmd.TableName) {
			tables = append(tables, tbl)
		}
	}

	if len(tables) == 0 {
		responseErr(errors.Errorf("table %s.%s not exists", cmd.DBName, cmd.TableName))
		return
	}

	now := time.Now()
	tableResults := make([]*client.TriggerNewTTLJobTableResult, 0, len(tables))
	allError := true
	var firstError error
	for _, ttlTbl := range tables {
		tblResult := &client.TriggerNewTTLJobTableResult{
			TableID:       ttlTbl.ID,
			DBName:        cmd.DBName,
			TableName:     cmd.TableName,
			PartitionName: ttlTbl.Partition.O,
		}

		job, err := m.lockNewJob(m.ctx, se, ttlTbl, now, true)
		if err != nil {
			firstError = err
			tblResult.ErrorMessage = err.Error()
			tableResults = append(tableResults, tblResult)
			continue
		}

		allError = false
		if job != nil {
			m.appendJob(job)
			tblResult.JobID = job.id
			tableResults = append(tableResults, tblResult)
		}
	}

	if allError {
		responseErr(firstError)
		return
	}

	terror.Log(m.cmdCli.ResponseCommand(m.ctx, requestID, &client.TriggerNewTTLJobResponse{
		TableResult: tableResults,
	}))

	tableResultsJSON, _ := json.Marshal(tableResults)
	logutil.BgLogger().Info("Done to trigger a new TTL job",
		zap.String("requestID", requestID),
		zap.String("database", cmd.DBName),
		zap.String("table", cmd.TableName),
		zap.ByteString("tableResults", tableResultsJSON),
	)
}

func (m *JobManager) reportMetrics() {
	var runningJobs, cancellingJobs float64
	for _, job := range m.runningJobs {
		switch job.status {
		case cache.JobStatusRunning:
			runningJobs++
		case cache.JobStatusCancelling:
			cancellingJobs++
		}
	}
	metrics.RunningJobsCnt.Set(runningJobs)
	metrics.CancellingJobsCnt.Set(cancellingJobs)
}

func (m *JobManager) resizeScanWorkers(count int) error {
	var err error
	var canceledWorkers []worker
	m.scanWorkers, canceledWorkers, err = m.resizeWorkers(m.scanWorkers, count, func() worker {
		return newScanWorker(m.delCh, m.notifyStateCh, m.sessPool)
	})
	for _, w := range canceledWorkers {
		s := w.(scanWorker)

		var tableID int64
		var scanErr error
		result := s.PollTaskResult()
		if result != nil {
			tableID = result.task.tbl.ID
			scanErr = result.err
		} else {
			// if the scan worker failed to poll the task, it's possible that the `WaitStopped` has timeout
			// we still consider the scan task as finished
			curTask := s.CurrentTask()
			if curTask == nil {
				continue
			}
			tableID = curTask.tbl.ID
			scanErr = errors.New("timeout to cancel scan task")
		}

		job := findJobWithTableID(m.runningJobs, tableID)
		if job == nil {
			logutil.Logger(m.ctx).Warn("task state changed but job not found", zap.Int64("tableID", tableID))
			continue
		}
		logutil.Logger(m.ctx).Debug("scan task finished", zap.String("jobID", job.id))
		job.finishedScanTaskCounter += 1
		job.scanTaskErr = multierr.Append(job.scanTaskErr, scanErr)
	}
	return err
}

func (m *JobManager) resizeDelWorkers(count int) error {
	var err error
	m.delWorkers, _, err = m.resizeWorkers(m.delWorkers, count, func() worker {
		return newDeleteWorker(m.delCh, m.sessPool)
	})
	return err
}

// resizeWorkers scales the worker, and returns the full set of workers as the first return value. If there are workers
// stopped, return the stopped worker in the second return value
func (m *JobManager) resizeWorkers(workers []worker, count int, factory func() worker) ([]worker, []worker, error) {
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

// updateTaskState polls the result from scan worker and returns whether there are result polled
func (m *JobManager) updateTaskState() bool {
	results := m.pollScanWorkerResults()
	for _, result := range results {
		logger := logutil.Logger(m.ctx).With(zap.Int64("tableID", result.task.tbl.ID))
		if result.err != nil {
			logger = logger.With(zap.Error(result.err))
		}

		job := findJobWithTableID(m.runningJobs, result.task.tbl.ID)
		if job == nil {
			logger.Warn("task state changed but job not found", zap.Int64("tableID", result.task.tbl.ID))
			continue
		}
		logger.Info("scan task finished", zap.String("jobID", job.id))

		job.finishedScanTaskCounter += 1
		job.scanTaskErr = multierr.Append(job.scanTaskErr, result.err)
	}

	return len(results) > 0
}

func (m *JobManager) pollScanWorkerResults() []*ttlScanTaskExecResult {
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

// checkNotOwnJob removes the job whose current job owner is not yourself
func (m *JobManager) checkNotOwnJob() {
	for _, job := range m.runningJobs {
		tableStatus := m.tableStatusCache.Tables[job.tbl.ID]
		if tableStatus == nil || tableStatus.CurrentJobOwnerID != m.id {
			logger := logutil.Logger(m.ctx).With(zap.String("jobID", job.id), zap.String("statistics", job.statistics.String()))
			if tableStatus != nil {
				logger.With(zap.String("newJobOwnerID", tableStatus.CurrentJobOwnerID))
			}
			logger.Info("job has been taken over by another node")
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
	if !timeutil.WithinDayTimePeriod(variable.TTLJobScheduleWindowStartTime.Load(), variable.TTLJobScheduleWindowEndTime.Load(), now) {
		// Local jobs will also not run, but as the server is still sending heartbeat,
		// and keep the job in memory, it could start the left task in the next window.
		return
	}
	if !variable.EnableTTLJob.Load() {
		if len(m.runningJobs) > 0 {
			ctx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)

			for _, job := range m.runningJobs {
				logutil.Logger(m.ctx).Info("cancel job because tidb_ttl_job_enable turned off", zap.String("jobID", job.id), zap.String("statistics", job.statistics.String()))
				m.removeJob(job)
				err := job.Cancel(ctx, se)
				if err != nil {
					logutil.Logger(m.ctx).Warn("fail to cancel job", zap.Error(err))
				}
				job.finish(se, se.Now())
			}

			cancel()
		}
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
			logutil.Logger(m.ctx).Info("try lock new job", zap.Int64("tableID", table.ID))
			job, err = m.lockNewJob(m.ctx, se, table, now, false)
			if job != nil {
				logutil.Logger(m.ctx).Info("append new running job", zap.String("jobID", job.id), zap.Int64("tableID", job.tbl.ID))
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
			task := job.peekScanTask()
			logger := logutil.Logger(m.ctx).With(zap.String("jobID", job.id), zap.String("table", task.tbl.TableInfo.Name.L))
			if task.tbl.PartitionDef != nil {
				logger = logger.With(zap.String("partition", task.tbl.PartitionDef.Name.L))
			}

			for len(idleScanWorkers) > 0 {
				idleWorker := idleScanWorkers[0]
				idleScanWorkers = idleScanWorkers[1:]

				err := idleWorker.Schedule(task)
				if err != nil {
					logger.Info("fail to schedule task", zap.Error(err))
					continue
				}

				ctx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
				err = job.changeStatus(ctx, se, cache.JobStatusRunning)
				if err != nil {
					// not a big problem, current logic doesn't depend on the job status to promote
					// the routine, so we could just print a log here
					logger.Error("change ttl job status", zap.Error(err), zap.String("id", job.id))
				}
				cancel()

				logger.Info("scheduled ttl task")

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
	jobs := make([]*ttlJob, 0, len(m.runningJobs))
	for _, job := range m.runningJobs {
		status := m.tableStatusCache.Tables[job.tbl.ID]
		if status == nil || status.CurrentJobOwnerID != m.id {
			// these jobs will be removed in `checkNotOwnJob`
			continue
		}

		jobs = append(jobs, job)
	}
	return jobs
}

// readyForNewJobTables returns all tables which should spawn a TTL job according to cache
func (m *JobManager) readyForNewJobTables(now time.Time) []*cache.PhysicalTable {
	tables := make([]*cache.PhysicalTable, 0, len(m.infoSchemaCache.Tables))

tblLoop:
	for _, table := range m.infoSchemaCache.Tables {
		// If this node already has a job for this table, just ignore.
		// Actually, the logic should ensure this condition never meet, we still add the check here to keep safety
		// (especially when the content of the status table is incorrect)
		for _, job := range m.runningJobs {
			if job.tbl.ID == table.ID {
				continue tblLoop
			}
		}

		status := m.tableStatusCache.Tables[table.ID]
		ok := m.couldTrySchedule(status, table, now, false)
		if ok {
			tables = append(tables, table)
		}
	}

	return tables
}

// couldTrySchedule returns whether a table should be tried to run TTL
func (m *JobManager) couldTrySchedule(tableStatus *cache.TableStatus, table *cache.PhysicalTable, now time.Time, ignoreScheduleInterval bool) bool {
	if tableStatus == nil {
		// if the table status hasn't been created, return true
		return true
	}
	if table == nil {
		// if the table is not recorded in info schema, return false
		return false
	}
	if tableStatus.CurrentJobOwnerID != "" {
		// see whether it's heart beat time is expired
		hbTime := tableStatus.CurrentJobOwnerHBTime
		// a more concrete value is `2 * max(updateTTLTableStatusCacheInterval, jobManagerLoopTickerInterval)`, but the
		// `updateTTLTableStatusCacheInterval` is greater than `jobManagerLoopTickerInterval` in most cases.
		if hbTime.Add(2 * getUpdateTTLTableStatusCacheInterval()).Before(now) {
			logutil.Logger(m.ctx).Info("task heartbeat has stopped", zap.Int64("tableID", table.ID), zap.Time("hbTime", hbTime), zap.Time("now", now))
			return true
		}
		return false
	}

	if ignoreScheduleInterval || tableStatus.LastJobStartTime.IsZero() {
		return true
	}

	startTime := tableStatus.LastJobStartTime

	interval := table.TTLInfo.JobInterval
	d, err := duration.ParseDuration(interval)
	if err != nil {
		logutil.Logger(m.ctx).Warn("illegal job interval", zap.String("interval", interval))
		return false
	}
	return startTime.Add(d).Before(now)
}

// occupyNewJob tries to occupy a new job in the ttl_table_status table. If it locks successfully, it will create a new
// localJob and return it.
// It could be nil, nil, if the table query doesn't return error but the job has been locked by other instances.
func (m *JobManager) lockNewJob(ctx context.Context, se session.Session, table *cache.PhysicalTable, now time.Time, ignoreScheduleInterval bool) (*ttlJob, error) {
	var expireTime time.Time

	err := se.RunInTxn(ctx, func() error {
		sql, args := cache.SelectFromTTLTableStatusWithID(table.ID)
		rows, err := se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}
		if len(rows) == 0 {
			// cannot find the row, insert the status row
			sql, args := insertNewTableIntoStatusSQL(table.ID, table.TableInfo.ID)
			_, err = se.ExecuteSQL(ctx, sql, args...)
			if err != nil {
				return errors.Wrapf(err, "execute sql: %s", sql)
			}
			sql, args = cache.SelectFromTTLTableStatusWithID(table.ID)
			rows, err = se.ExecuteSQL(ctx, sql, args...)
			if err != nil {
				return errors.Wrapf(err, "execute sql: %s", sql)
			}
			if len(rows) == 0 {
				return errors.New("table status row still doesn't exist after insertion")
			}
		}
		tableStatus, err := cache.RowToTableStatus(se, rows[0])
		if err != nil {
			return err
		}
		if !m.couldTrySchedule(tableStatus, m.infoSchemaCache.Tables[tableStatus.TableID], now, ignoreScheduleInterval) {
			return errors.New("couldn't schedule ttl job")
		}

		expireTime, err = table.EvalExpireTime(m.ctx, se, now)
		if err != nil {
			return err
		}

		jobID := uuid.New().String()
		failpoint.Inject("set-job-uuid", func(val failpoint.Value) {
			jobID = val.(string)
		})

		sql, args = setTableStatusOwnerSQL(jobID, table.ID, now, expireTime, m.id)
		_, err = se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		ranges, err := table.SplitScanRanges(ctx, m.store, splitScanCount)
		if err != nil {
			return errors.Wrap(err, "split scan ranges")
		}
		for scanID, r := range ranges {
			sql, args, err = cache.InsertIntoTTLTask(se, jobID, table.ID, scanID, r.Start, r.End, expireTime, now)
			if err != nil {
				return errors.Wrap(err, "encode scan task")
			}
			_, err = se.ExecuteSQL(ctx, sql, args...)
			if err != nil {
				return errors.Wrapf(err, "execute sql: %s", sql)
			}
		}

		return nil
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
		sql, args := updateHeartBeatSQL(job.tbl.ID, now, m.id)
		_, err := se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}
		// also updates some internal state for this job
		err = job.updateState(ctx, se)
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to update state of the job", zap.String("jobID", job.id))
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

// GetCommandCli returns the command client
func (m *JobManager) GetCommandCli() client.CommandClient {
	return m.cmdCli
}
