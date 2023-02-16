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

const scanTaskNotificationType string = "scan"

const insertNewTableIntoStatusTemplate = "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (%?, %?)"
const setTableStatusOwnerTemplate = `UPDATE mysql.tidb_ttl_table_status
	SET current_job_id = %?,
		current_job_owner_id = %?,
		current_job_start_time = %?,
		current_job_status = 'running',
		current_job_status_update_time = %?,
		current_job_ttl_expire = %?,
		current_job_owner_hb_time = %?
	WHERE table_id = %?`
const updateHeartBeatTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = %? WHERE table_id = %? AND current_job_owner_id = %?"
const taskGCTemplate = `DELETE task FROM
		mysql.tidb_ttl_task task
	left join
		mysql.tidb_ttl_table_status job
	ON task.job_id = job.current_job_id
	WHERE job.table_id IS NULL`

const ttlJobHistoryGCTemplate = `DELETE FROM mysql.tidb_ttl_job_history WHERE create_time < CURDATE() - INTERVAL 90 DAY`

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

	store           kv.Storage
	cmdCli          client.CommandClient
	notificationCli client.NotificationClient

	// infoSchemaCache and tableStatusCache are a cache stores the information from info schema and the tidb_ttl_table_status
	// table. They don't need to be protected by mutex, because they are only used in job loop goroutine.
	infoSchemaCache  *cache.InfoSchemaCache
	tableStatusCache *cache.TableStatusCache

	// runningJobs record all ttlJob waiting in local
	// when a job for a table is created, it could spawn several scan tasks. If there are too many scan tasks, and they cannot
	// be fully consumed by local scan workers, their states should be recorded in the runningJobs, so that we could continue
	// to poll scan tasks from the job in the future when there are scan workers in idle.
	runningJobs []*ttlJob

	taskManager *taskManager
}

// NewJobManager creates a new ttl job manager
func NewJobManager(id string, sessPool sessionPool, store kv.Storage, etcdCli *clientv3.Client) (manager *JobManager) {
	manager = &JobManager{}
	manager.id = id
	manager.store = store
	manager.sessPool = sessPool

	manager.init(manager.jobLoop)
	manager.ctx = logutil.WithKeyValue(manager.ctx, "ttl-worker", "job-manager")

	manager.infoSchemaCache = cache.NewInfoSchemaCache(getUpdateInfoSchemaCacheInterval())
	manager.tableStatusCache = cache.NewTableStatusCache(getUpdateTTLTableStatusCacheInterval())

	if etcdCli != nil {
		manager.cmdCli = client.NewCommandClient(etcdCli)
		manager.notificationCli = client.NewNotificationClient(etcdCli)
	} else {
		manager.cmdCli = client.NewMockCommandClient()
		manager.notificationCli = client.NewMockNotificationClient()
	}

	manager.taskManager = newTaskManager(manager.ctx, sessPool, manager.infoSchemaCache, id)

	return
}

func (m *JobManager) jobLoop() error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.taskManager.resizeScanWorkers(0), m.taskManager.resizeDelWorkers(0)))
		se.Close()
		logutil.Logger(m.ctx).Info("ttlJobManager loop exited.")
	}()

	infoSchemaCacheUpdateTicker := time.Tick(m.infoSchemaCache.GetInterval())
	tableStatusCacheUpdateTicker := time.Tick(m.tableStatusCache.GetInterval())
	resizeWorkersTicker := time.Tick(getResizeWorkersInterval())
	gcTicker := time.Tick(ttlGCInterval)

	scheduleJobTicker := time.Tick(jobManagerLoopTickerInterval)
	jobCheckTicker := time.Tick(jobManagerLoopTickerInterval)
	updateJobHeartBeatTicker := time.Tick(jobManagerLoopTickerInterval)

	scheduleTaskTicker := time.Tick(getTaskManagerLoopTickerInterval())
	updateTaskHeartBeatTicker := time.Tick(ttlTaskHeartBeatTickerInterval)
	taskCheckTicker := time.Tick(time.Second * 5)
	checkScanTaskFinishedTicker := time.Tick(getTaskManagerLoopTickerInterval())

	cmdWatcher := m.cmdCli.WatchCommand(m.ctx)
	scanTaskNotificationWatcher := m.notificationCli.WatchNotification(m.ctx, scanTaskNotificationType)
	m.taskManager.resizeWorkersWithSysVar()
	for {
		m.reportMetrics()
		m.taskManager.reportMetrics()
		now := se.Now()

		select {
		// misc
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
		case <-gcTicker:
			gcCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
			DoGC(gcCtx, se)
			cancel()
		// Job Schedule loop:
		case <-updateJobHeartBeatTicker:
			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
			err = m.updateHeartBeat(updateHeartBeatCtx, se, now)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update job heart beat", zap.Error(err))
			}
			cancel()
		case <-jobCheckTicker:
			m.checkFinishedJob(se)
			m.checkNotOwnJob()
		case <-scheduleJobTicker:
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

		// Task Manager Loop
		case <-scheduleTaskTicker:
			m.taskManager.rescheduleTasks(se, now)
		case _, ok := <-scanTaskNotificationWatcher:
			if !ok {
				if m.ctx.Err() != nil {
					return nil
				}

				logutil.BgLogger().Warn("The TTL scan task notification watcher is closed unexpectedly, re-watch it again")
				scanTaskNotificationWatcher = m.notificationCli.WatchNotification(m.ctx, scanTaskNotificationType)
				continue
			}
			m.taskManager.rescheduleTasks(se, now)
		case <-taskCheckTicker:
			m.taskManager.checkInvalidTask(se)
			m.taskManager.checkFinishedTask(se, now)
		case <-resizeWorkersTicker:
			m.taskManager.resizeWorkersWithSysVar()
		case <-updateTaskHeartBeatTicker:
			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)
			err = m.taskManager.updateHeartBeat(updateHeartBeatCtx, se, now)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update task heart beat", zap.Error(err))
			}
			cancel()
		case <-checkScanTaskFinishedTicker:
			if m.taskManager.handleScanFinishedTask() {
				m.taskManager.rescheduleTasks(se, now)
			}
		case <-m.taskManager.notifyStateCh:
			if m.taskManager.handleScanFinishedTask() {
				m.taskManager.rescheduleTasks(se, now)
			}
		}
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

// checkNotOwnJob removes the job whose current job owner is not yourself
func (m *JobManager) checkNotOwnJob() {
	for _, job := range m.runningJobs {
		tableStatus := m.tableStatusCache.Tables[job.tbl.ID]
		if tableStatus == nil || tableStatus.CurrentJobOwnerID != m.id {
			logger := logutil.Logger(m.ctx).With(zap.String("jobID", job.id))
			if tableStatus != nil {
				logger.With(zap.String("newJobOwnerID", tableStatus.CurrentJobOwnerID))
			}
			logger.Info("job has been taken over by another node")
			m.removeJob(job)
		}
	}
}

func (m *JobManager) checkFinishedJob(se session.Session) {
j:
	for _, job := range m.runningJobs {
		timeoutJobCtx, cancel := context.WithTimeout(m.ctx, ttlInternalSQLTimeout)

		sql, args := cache.SelectFromTTLTaskWithJobID(job.id)
		rows, err := se.ExecuteSQL(timeoutJobCtx, sql, args...)
		cancel()
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to execute sql", zap.String("sql", sql), zap.Any("args", args), zap.Error(err))
			continue
		}

		allFinished := true
		allTasks := make([]*cache.TTLTask, 0, len(rows))
		for _, r := range rows {
			task, err := cache.RowToTTLTask(se, r)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to read task", zap.Error(err))
				continue j
			}
			allTasks = append(allTasks, task)

			if task.Status != "finished" {
				allFinished = false
			}
		}

		if allFinished {
			logutil.Logger(m.ctx).Info("job has finished", zap.String("jobID", job.id))
			summary, err := summarizeTaskResult(allTasks)
			if err != nil {
				logutil.Logger(m.ctx).Info("fail to summarize job", zap.Error(err))
			}
			m.removeJob(job)
			job.finish(se, se.Now(), summary)
		}
		cancel()
	}
}

func (m *JobManager) rescheduleJobs(se session.Session, now time.Time) {
	if !variable.EnableTTLJob.Load() || !timeutil.WithinDayTimePeriod(variable.TTLJobScheduleWindowStartTime.Load(), variable.TTLJobScheduleWindowEndTime.Load(), now) {
		if len(m.runningJobs) > 0 {
			for _, job := range m.runningJobs {
				logutil.Logger(m.ctx).Info("cancel job because tidb_ttl_job_enable turned off", zap.String("jobID", job.id))

				summary, err := summarizeErr(errors.New("ttl job is disabled"))
				if err != nil {
					logutil.Logger(m.ctx).Info("fail to summarize job", zap.Error(err))
				}
				m.removeJob(job)
				job.finish(se, now, summary)
			}
		}
		return
	}

	// don't lock job if there's no free scan workers in local
	// it's a mechanism to avoid too many scan tasks waiting in the ttl_tasks table.
	if len(m.taskManager.idleScanWorkers()) == 0 {
		return
	}

	newJobTables := m.readyForNewJobTables(now)
	// TODO: also consider to resume tables, but it's fine to left them there, as other nodes will take this job
	// when the heart beat is not sent
	for _, table := range newJobTables {
		logutil.Logger(m.ctx).Info("try lock new job", zap.Int64("tableID", table.ID))
		job, err := m.lockNewJob(m.ctx, se, table, now, false)
		if job != nil {
			logutil.Logger(m.ctx).Info("append new running job", zap.String("jobID", job.id), zap.Int64("tableID", job.tbl.ID))
			m.appendJob(job)
		}
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to create new job", zap.Error(err))
		}
	}
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
	var jobID string

	err := se.RunInTxn(ctx, func() error {
		sql, args := cache.SelectFromTTLTableStatusWithID(table.ID)
		// use ` FOR UPDATE NOWAIT`, then if the new job has been locked by other nodes, it will return:
		// [tikv:3572]Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.
		// Then this tidb node will not waste resource in calculating the ranges.
		rows, err := se.ExecuteSQL(ctx, sql+" FOR UPDATE NOWAIT", args...)
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
			rows, err = se.ExecuteSQL(ctx, sql+" FOR UPDATE NOWAIT", args...)
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

		jobID = uuid.New().String()
		jobExist := false
		if len(tableStatus.CurrentJobID) > 0 {
			// don't create new job if there is already one running
			// so the running tasks don't need to be cancelled
			jobID = tableStatus.CurrentJobID
			expireTime = tableStatus.CurrentJobTTLExpire
			jobExist = true
		}
		failpoint.Inject("set-job-uuid", func(val failpoint.Value) {
			jobID = val.(string)
		})

		sql, args = setTableStatusOwnerSQL(jobID, table.ID, now, expireTime, m.id)
		_, err = se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}

		// if the job already exist, don't need to submit scan tasks
		if jobExist {
			return nil
		}

		sql, args = createJobHistorySQL(jobID, table, expireTime, now)
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
	}, session.TxnModePessimistic)
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

	job := m.createNewJob(jobID, expireTime, now, table)

	// job is created, notify every scan managers to fetch new tasks
	err = m.notificationCli.Notify(m.ctx, scanTaskNotificationType, job.id)
	if err != nil {
		logutil.Logger(m.ctx).Warn("fail to trigger scan tasks", zap.Error(err))
	}
	return job, nil
}

func (m *JobManager) createNewJob(id string, expireTime time.Time, now time.Time, table *cache.PhysicalTable) *ttlJob {
	return &ttlJob{
		id:      id,
		ownerID: m.id,

		createTime:    now,
		ttlExpireTime: expireTime,
		// at least, the info schema cache and table status cache are consistent in table id, so it's safe to get table
		// information from schema cache directly
		tbl: table,

		status: cache.JobStatusRunning,
	}
}

// updateHeartBeat updates the heartbeat for all task with current instance as owner
func (m *JobManager) updateHeartBeat(ctx context.Context, se session.Session, now time.Time) error {
	for _, job := range m.localJobs() {
		if job.createTime.Add(ttlJobTimeout).Before(now) {
			logutil.Logger(m.ctx).Info("job is timeout", zap.String("jobID", job.id))
			summary, err := summarizeErr(errors.New("job is timeout"))
			if err != nil {
				logutil.Logger(m.ctx).Info("fail to summarize job", zap.Error(err))
			}
			m.removeJob(job)
			job.finish(se, now, summary)
			continue
		}

		sql, args := updateHeartBeatSQL(job.tbl.ID, now, m.id)
		_, err := se.ExecuteSQL(ctx, sql, args...)
		if err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
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

// GetCommandCli returns the command client
func (m *JobManager) GetCommandCli() client.CommandClient {
	return m.cmdCli
}

// GetNotificationCli returns the notification client
func (m *JobManager) GetNotificationCli() client.NotificationClient {
	return m.notificationCli
}

// TTLSummary is the summary for TTL job
type TTLSummary struct {
	TotalRows   uint64 `json:"total_rows"`
	SuccessRows uint64 `json:"success_rows"`
	ErrorRows   uint64 `json:"error_rows"`

	TotalScanTask     int `json:"total_scan_task"`
	ScheduledScanTask int `json:"scheduled_scan_task"`
	FinishedScanTask  int `json:"finished_scan_task"`

	ScanTaskErr string `json:"scan_task_err,omitempty"`
	SummaryText string `json:"-"`
}

func summarizeErr(err error) (*TTLSummary, error) {
	summary := &TTLSummary{
		ScanTaskErr: err.Error(),
	}

	buf, err := json.Marshal(summary)
	if err != nil {
		return nil, err
	}
	summary.SummaryText = string(buf)
	return summary, nil
}

func summarizeTaskResult(tasks []*cache.TTLTask) (*TTLSummary, error) {
	summary := &TTLSummary{}
	var allErr error
	for _, t := range tasks {
		if t.State != nil {
			summary.TotalRows += t.State.TotalRows
			summary.SuccessRows += t.State.SuccessRows
			summary.ErrorRows += t.State.ErrorRows
			if len(t.State.ScanTaskErr) > 0 {
				allErr = multierr.Append(allErr, errors.New(t.State.ScanTaskErr))
			}
		}

		summary.TotalScanTask += 1
		if t.Status != cache.TaskStatusWaiting {
			summary.ScheduledScanTask += 1
		}
		if t.Status == cache.TaskStatusFinished {
			summary.FinishedScanTask += 1
		}
	}
	if allErr != nil {
		summary.ScanTaskErr = allErr.Error()
	}

	buf, err := json.Marshal(summary)
	if err != nil {
		return nil, err
	}
	summary.SummaryText = string(buf)
	return summary, nil
}

// DoGC deletes some old TTL job histories and redundant scan tasks
func DoGC(ctx context.Context, se session.Session) {
	if _, err := se.ExecuteSQL(ctx, taskGCTemplate); err != nil {
		logutil.Logger(ctx).Warn("fail to gc redundant scan task", zap.Error(err))
	}

	if _, err := se.ExecuteSQL(ctx, ttlJobHistoryGCTemplate); err != nil {
		logutil.Logger(ctx).Warn("fail to gc ttl job history", zap.Error(err))
	}
}
