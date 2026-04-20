//go:build intest

// Copyright 2026 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
)

// TTLJob exports ttlJob for external intest packages.
type TTLJob = ttlJob

// TTLDeleteTask exports ttlDeleteTask for external intest packages.
type TTLDeleteTask = ttlDeleteTask

// Worker exports worker for external intest packages.
type Worker = worker

// NewTaskManager exports newTaskManager for external intest packages.
var NewTaskManager = newTaskManager

// NewTTLScanTask creates a new TTL scan task for external intest packages.
func NewTTLScanTask(ctx context.Context, tbl *cache.PhysicalTable, ttlTask *cache.TTLTask) *ttlScanTask {
	return &ttlScanTask{
		ctx:        ctx,
		tbl:        tbl,
		TTLTask:    ttlTask,
		statistics: &ttlStatistics{},
	}
}

// DoScan exports doScan for external intest packages.
func (t *ttlScanTask) DoScan(ctx context.Context, delCh chan<- *TTLDeleteTask, sessPool syssession.Pool) *ttlScanTaskExecResult {
	return t.doScan(ctx, delCh, sessPool)
}

// LockJob exports lockNewJob/lockHBTimeoutJob for external intest packages.
func (m *JobManager) LockJob(
	ctx context.Context,
	se session.Session,
	table *cache.PhysicalTable,
	now time.Time,
	createJobID string,
	checkInterval bool,
) (*TTLJob, error) {
	if createJobID == "" {
		return m.lockHBTimeoutJob(ctx, se, table.ID, table.TableInfo.ID, now)
	}
	return m.lockNewJob(ctx, se, table, now, createJobID, checkInterval)
}

// RunningJobs returns running jobs for external intest packages.
func (m *JobManager) RunningJobs() []*TTLJob {
	return m.runningJobs
}

// InfoSchemaCache returns the info schema cache for external intest packages.
func (m *JobManager) InfoSchemaCache() *cache.InfoSchemaCache {
	return m.infoSchemaCache
}

// TableStatusCache returns the table status cache for external intest packages.
func (m *JobManager) TableStatusCache() *cache.TableStatusCache {
	return m.tableStatusCache
}

// RescheduleJobs exports rescheduleJobs for external intest packages.
func (m *JobManager) RescheduleJobs(se session.Session, now time.Time) {
	m.rescheduleJobs(se, now)
}

// SubmitJob exports handleSubmitJobRequest for external intest packages.
func (m *JobManager) SubmitJob(se session.Session, tableID, physicalID int64, requestID string) error {
	ch := make(chan error, 1)
	m.handleSubmitJobRequest(se, &SubmitTTLManagerJobRequest{
		TableID:    tableID,
		PhysicalID: physicalID,
		RequestID:  requestID,
		RespCh:     ch,
	})
	return <-ch
}

// TaskManager returns the task manager for external intest packages.
func (m *JobManager) TaskManager() *taskManager {
	return m.taskManager
}

// UpdateHeartBeat exports updateHeartBeat for external intest packages.
func (m *JobManager) UpdateHeartBeat(ctx context.Context, se session.Session, now time.Time) {
	m.updateHeartBeat(ctx, se, now)
}

// UpdateHeartBeatForJob exports updateHeartBeatForJob for external intest packages.
func (m *JobManager) UpdateHeartBeatForJob(ctx context.Context, se session.Session, now time.Time, job *TTLJob) error {
	return m.updateHeartBeatForJob(ctx, se, now, job)
}

// SetLastReportDelayMetricsTime sets the last report time for external intest packages.
func (m *JobManager) SetLastReportDelayMetricsTime(t time.Time) {
	m.lastReportDelayMetricsTime = t
}

// GetLastReportDelayMetricsTime gets the last report time for external intest packages.
func (m *JobManager) GetLastReportDelayMetricsTime() time.Time {
	return m.lastReportDelayMetricsTime
}

// ReportMetrics exports reportMetrics for external intest packages.
func (m *JobManager) ReportMetrics(se session.Session) {
	m.reportMetrics(se)
}

// CheckNotOwnJob exports checkNotOwnJob for external intest packages.
func (m *JobManager) CheckNotOwnJob() {
	m.checkNotOwnJob()
}

// CheckFinishedJob exports checkFinishedJob for external intest packages.
func (m *JobManager) CheckFinishedJob(se session.Session) {
	m.checkFinishedJob(se)
}

// ID returns the job manager id for external intest packages.
func (m *JobManager) ID() string {
	return m.id
}

// Finish exports finish for external intest packages.
func (j *ttlJob) Finish(se session.Session, now time.Time, summary *TTLSummary) error {
	return j.finish(se, now, summary)
}

// ID returns the ttl job id for external intest packages.
func (j *ttlJob) ID() string {
	return j.id
}

// SetTimeFormat overrides the SQL time format used by intest packages.
func SetTimeFormat(format string) {
	timeFormat = format
}

// SetScanWorkers4Test replaces scan workers for external intest packages.
func (m *taskManager) SetScanWorkers4Test(workers []Worker) {
	m.scanWorkers = workers
}

// LockScanTask exports lockScanTask for external intest packages.
func (m *taskManager) LockScanTask(se session.Session, task *cache.TTLTask, now time.Time) (*runningScanTask, error) {
	return m.lockScanTask(se, task, now)
}

// ResizeWorkersWithSysVar exports resizeWorkersWithSysVar for external intest packages.
func (m *taskManager) ResizeWorkersWithSysVar() {
	m.resizeWorkersWithSysVar()
}

// ResizeWorkersToZero shrinks all workers for external intest packages.
func (m *taskManager) ResizeWorkersToZero(t *testing.T) {
	t.Helper()
	if err := m.resizeScanWorkers(0); err != nil {
		t.Fatal(err)
	}
	if err := m.resizeDelWorkers(0); err != nil {
		t.Fatal(err)
	}
}

// RescheduleTasks exports rescheduleTasks for external intest packages.
func (m *taskManager) RescheduleTasks(se session.Session, now time.Time) {
	m.rescheduleTasks(se, now)
}

// ResizeScanWorkers exports resizeScanWorkers for external intest packages.
func (m *taskManager) ResizeScanWorkers(count int) error {
	return m.resizeScanWorkers(count)
}

// ResizeDelWorkers exports resizeDelWorkers for external intest packages.
func (m *taskManager) ResizeDelWorkers(count int) error {
	return m.resizeDelWorkers(count)
}

// ReportMetrics exports reportMetrics for external intest packages.
func (m *taskManager) ReportMetrics() {
	m.reportMetrics()
}

// CheckFinishedTask exports checkFinishedTask for external intest packages.
func (m *taskManager) CheckFinishedTask(se session.Session, now time.Time) {
	m.checkFinishedTask(se, now)
}

// GetRunningTasks returns current running tasks for external intest packages.
func (m *taskManager) GetRunningTasks() []*runningScanTask {
	return m.runningTasks
}

// MeetTTLRunningTasks exports meetTTLRunningTask for external intest packages.
func (m *taskManager) MeetTTLRunningTasks(count int, taskStatus cache.TaskStatus) bool {
	return m.meetTTLRunningTask(count, taskStatus)
}

// ReportTaskFinished exports reportTaskFinished for external intest packages.
func (m *taskManager) ReportTaskFinished(se session.Session, now time.Time, task *runningScanTask) error {
	return m.reportTaskFinished(se, now, task)
}

// GetScanWorkers returns scan workers for external intest packages.
func (m *taskManager) GetScanWorkers() []Worker {
	return m.scanWorkers
}

// SetResult sets a task result for external intest packages.
func (t *runningScanTask) SetResult(err error) {
	t.result = t.ttlScanTask.result(err)
}

// UpdateHeartBeat exports updateHeartBeat for external intest packages.
func (m *taskManager) UpdateHeartBeat(ctx context.Context, se session.Session, now time.Time) {
	m.updateHeartBeat(ctx, se, now)
}

// UpdateHeartBeatForTask exports taskHeartbeatOrResignOwner for external intest packages.
func (m *taskManager) UpdateHeartBeatForTask(ctx context.Context, se session.Session, now time.Time, task *runningScanTask) error {
	return m.taskHeartbeatOrResignOwner(ctx, se, now, task, false)
}

// SetWaitWorkerStopTimeoutForTest overrides waitWorkerStopTimeout for external intest packages.
func SetWaitWorkerStopTimeoutForTest(timeout time.Duration) func() {
	original := waitWorkerStopTimeout
	waitWorkerStopTimeout = timeout
	return func() {
		waitWorkerStopTimeout = original
	}
}

// GetTerminateInfo returns terminate info for external intest packages.
func (t *runningScanTask) GetTerminateInfo() (bool, TaskTerminateReason, time.Time) {
	if t.result == nil {
		return false, "", time.Time{}
	}
	return true, t.result.reason, t.result.time
}

// GetStatistics returns statistics for external intest packages.
func (t *runningScanTask) GetStatistics() *ttlStatistics {
	return t.statistics
}

// ResetEndTimeForTest resets the task end time for external intest packages.
func (t *runningScanTask) ResetEndTimeForTest(tb *testing.T, tm time.Time) {
	tb.Helper()
	if t.result == nil {
		tb.Fatal("task result is nil")
	}
	t.result.time = tm
}

// SetCancel replaces the cancel function for external intest packages.
func (t *runningScanTask) SetCancel(cancel func()) {
	t.cancel = cancel
}

// CheckInvalidTask exports checkInvalidTask for external intest packages.
func (m *taskManager) CheckInvalidTask(se session.Session) {
	m.checkInvalidTask(se)
}
