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

package autoanalyze

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

// analyzeJobRecorder implements statstypes.AnalyzeJobRecorder in the sessions of a pool.
type analyzeJobRecorder struct {
	pool statsutil.SessionPool
}

// newAnalyzeJobRecorder creates a statstypes.AnalyzeJobRecorder that records the jobs in the
// sessions of pool. It needs no stats cache.
func newAnalyzeJobRecorder(pool statsutil.SessionPool) *analyzeJobRecorder {
	return &analyzeJobRecorder{pool: pool}
}

// Analyzer analyzes the outdated tables in the background, and manages the analyze jobs.
type Analyzer struct {
	*analyzeJobRecorder

	pool statsutil.SessionPool
	// refresher is used to refresh the analyze job queue and analyze the highest priority tables.
	refresher *refresher.Refresher
}

// NewAnalyzer creates an Analyzer that analyzes the outdated tables in the sessions
// of pool. It finds them from the stats in statsCache and tableStats.
func NewAnalyzer(
	ctx context.Context,
	pool statsutil.SessionPool,
	procIDs statsutil.AutoAnalyzeProcIDGenerator,
	tables statsutil.TableInfoGetter,
	statsCache priorityqueue.StatsCache,
	tableStats statstypes.NonPseudoPhysicalTableStatsGetter,
	sysProcTracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
) *Analyzer {
	r := refresher.NewRefresher(ctx, pool, procIDs, tables, statsCache, tableStats, sysProcTracker, ddlNotifier)
	return &Analyzer{
		analyzeJobRecorder: newAnalyzeJobRecorder(pool),
		pool:               pool,
		refresher:          r,
	}
}

// InsertAnalyzeJob inserts the analyze job to the storage.
func (r *analyzeJobRecorder) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return statsutil.CallWithSCtx(r.pool.SPool(), func(sctx sessionctx.Context) error {
		return insertAnalyzeJob(sctx, job, instance, procID)
	})
}

func (r *analyzeJobRecorder) StartAnalyzeJob(job *statistics.AnalyzeJob) {
	err := statsutil.CallWithSCtx(r.pool.SPool(), func(sctx sessionctx.Context) error {
		startAnalyzeJob(sctx, job)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to start analyze job", zap.Error(err))
	}
}

func (r *analyzeJobRecorder) UpdateAnalyzeJobProgress(job *statistics.AnalyzeJob, rowCount int64) {
	err := statsutil.CallWithSCtx(r.pool.SPool(), func(sctx sessionctx.Context) error {
		updateAnalyzeJobProgress(sctx, job, rowCount)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job progress", zap.Error(err))
	}
}

func (r *analyzeJobRecorder) FinishAnalyzeJob(job *statistics.AnalyzeJob, failReason error, analyzeType statistics.JobType) {
	err := statsutil.CallWithSCtx(r.pool.SPool(), func(sctx sessionctx.Context) error {
		finishAnalyzeJob(sctx, job, failReason, analyzeType)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to finish analyze job", zap.Error(err))
	}
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (sa *Analyzer) DeleteAnalyzeJobs(updateTime time.Time) error {
	return statsutil.CallWithSCtx(sa.pool.SPool(), func(sctx sessionctx.Context) error {
		_, _, err := statsutil.ExecRows(sctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
		return err
	})
}

// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the potentially corrupted analyze job.
// It only cleans up the jobs that are associated with the current instance.
// A corrupted analyze job is one that is in a 'pending' or 'running' state,
// but is associated with a TiDB instance that is either not currently running or has been restarted.
// We use current running analyze jobs to check whether the analyze job is corrupted.
func (sa *Analyzer) CleanupCorruptedAnalyzeJobsOnCurrentInstance(currentRunningProcessIDs map[uint64]struct{}) error {
	return statsutil.CallWithSCtx(sa.pool.SPool(), func(sctx sessionctx.Context) error {
		return CleanupCorruptedAnalyzeJobsOnCurrentInstance(sctx, currentRunningProcessIDs)
	}, statsutil.FlagWrapTxn)
}

// CleanupCorruptedAnalyzeJobsOnDeadInstances removes analyze jobs that may have been corrupted.
// Specifically, it removes jobs associated with instances that no longer exist in the cluster.
func (sa *Analyzer) CleanupCorruptedAnalyzeJobsOnDeadInstances() error {
	return statsutil.CallWithSCtx(sa.pool.SPool(), func(sctx sessionctx.Context) error {
		return CleanupCorruptedAnalyzeJobsOnDeadInstances(sctx)
	}, statsutil.FlagWrapTxn)
}

// SelectAnalyzeJobsOnCurrentInstanceSQL is the SQL to select the analyze jobs whose
// state is `pending` or `running` and the update time is more than 10 minutes ago
// and the instance is current instance.
const SelectAnalyzeJobsOnCurrentInstanceSQL = `SELECT id, process_id
		FROM mysql.analyze_jobs
		WHERE instance = %?
		AND state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)`

// SelectAnalyzeJobsSQL is the SQL to select the analyze jobs whose
// state is `pending` or `running` and the update time is more than 10 minutes ago.
const SelectAnalyzeJobsSQL = `SELECT id, instance
		FROM mysql.analyze_jobs
		WHERE state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)`

// BatchUpdateAnalyzeJobSQL is the SQL to update the analyze jobs to `failed` state.
const BatchUpdateAnalyzeJobSQL = `UPDATE mysql.analyze_jobs
            SET state = 'failed',
            fail_reason = 'The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution',
            process_id = NULL
            WHERE id IN (%?)`

func tenMinutesAgo() string {
	return time.Now().Add(-10 * time.Minute).UTC().Format(types.TimeFormat)
}

// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the potentially corrupted analyze job from current instance.
// Exported for testing.
func CleanupCorruptedAnalyzeJobsOnCurrentInstance(
	sctx sessionctx.Context,
	currentRunningProcessIDs map[uint64]struct{},
) error {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return errors.Trace(err)
	}
	instance := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	// Get all the analyze jobs whose state is `pending` or `running` and the update time is more than 10 minutes ago
	// and the instance is current instance.
	rows, _, err := statsutil.ExecRows(
		sctx,
		SelectAnalyzeJobsOnCurrentInstanceSQL,
		instance,
		tenMinutesAgo(),
	)
	if err != nil {
		return errors.Trace(err)
	}

	jobIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		// The process ID is typically non-null for running or pending jobs.
		// However, in rare cases(I don't which case), it may be null. Therefore, it's necessary to check its value.
		if !row.IsNull(1) {
			processID := row.GetUint64(1)
			// If the process id is not in currentRunningProcessIDs, we need to clean up the job.
			// They don't belong to current instance any more.
			if _, ok := currentRunningProcessIDs[processID]; !ok {
				jobID := row.GetUint64(0)
				jobIDs = append(jobIDs, strconv.FormatUint(jobID, 10))
			}
		}
	}

	// Do a batch update to clean up the jobs.
	if len(jobIDs) > 0 {
		_, _, err = statsutil.ExecRows(
			sctx,
			BatchUpdateAnalyzeJobSQL,
			jobIDs,
		)
		if err != nil {
			return errors.Trace(err)
		}
		statslogutil.StatsLogger().Info(
			"clean up the potentially corrupted analyze jobs from current instance",
			zap.Strings("jobIDs", jobIDs),
		)
	}

	return nil
}

// CleanupCorruptedAnalyzeJobsOnDeadInstances cleans up the potentially corrupted analyze job from dead instances.
func CleanupCorruptedAnalyzeJobsOnDeadInstances(
	sctx sessionctx.Context,
) error {
	rows, _, err := statsutil.ExecRows(
		sctx,
		SelectAnalyzeJobsSQL,
		tenMinutesAgo(),
	)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil
	}

	// Get all the instances from etcd.
	serverInfo, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	instances := make(map[string]struct{}, len(serverInfo))
	for _, info := range serverInfo {
		instance := net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port)))
		instances[instance] = struct{}{}
	}

	jobIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		// If the instance is not in instances, we need to clean up the job.
		// It means the instance is down or the instance is not in the cluster any more.
		instance := row.GetString(1)
		if _, ok := instances[instance]; !ok {
			jobID := row.GetUint64(0)
			jobIDs = append(jobIDs, strconv.FormatUint(jobID, 10))
		}
	}

	// Do a batch update to clean up the jobs.
	if len(jobIDs) > 0 {
		_, _, err = statsutil.ExecRows(
			sctx,
			BatchUpdateAnalyzeJobSQL,
			jobIDs,
		)
		if err != nil {
			return errors.Trace(err)
		}
		statslogutil.StatsLogger().Info(
			"clean up the potentially corrupted analyze jobs from dead instances",
			zap.Strings("jobIDs", jobIDs),
		)
	}

	return nil
}

// HandleAutoAnalyze analyzes the outdated tables. (The change percent of the table exceeds the threshold)
// It also analyzes newly created tables and newly added indexes.
func (sa *Analyzer) HandleAutoAnalyze() (analyzed bool) {
	if err := statsutil.CallWithSCtx(sa.pool.SPool(), func(sctx sessionctx.Context) error {
		analyzed = sa.handleAutoAnalyze(sctx)
		return nil
	}); err != nil {
		statslogutil.StatsErrVerboseSampleLogger().Error("Failed to handle auto analyze", zap.Error(err))
	}
	return
}

// GetPriorityQueueSnapshot returns the stats priority queue snapshot.
func (sa *Analyzer) GetPriorityQueueSnapshot() (statstypes.PriorityQueueSnapshot, error) {
	return sa.refresher.GetPriorityQueueSnapshot()
}

// ClosePriorityQueue closes the stats priority queue if initialized.
// NOTE: This does NOT stop the analyze worker. Only the priority queue is closed.
func (sa *Analyzer) ClosePriorityQueue() {
	sa.refresher.ClosePriorityQueue()
}

func (sa *Analyzer) handleAutoAnalyze(sctx sessionctx.Context) bool {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error(
				"HandleAutoAnalyze panicked",
				zap.Any("recover", r),
				zap.Stack("stack"),
			)
		}
	}()
	// During the test, we need to fetch all DML changes before analyzing the highest priority tables.
	if intest.InTest {
		sa.refresher.ProcessDMLChangesForTest()
		sa.refresher.RequeueMustRetryJobsForTest()
	}
	analyzed := sa.refresher.AnalyzeHighestPriorityTables(sctx)
	// During the test, we need to wait for the auto analyze job to be finished.
	if intest.InTest {
		sa.refresher.WaitAutoAnalyzeFinishedForTest()
	}
	return analyzed
}

// Close closes the auto-analyze worker.
func (sa *Analyzer) Close() {
	sa.refresher.Close()
}

// CheckAutoAnalyzeWindow determine the time window for auto-analysis and verify if the current time falls within this range.
func CheckAutoAnalyzeWindow(sctx sessionctx.Context) (startStr, endStr string, ok bool) {
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[vardef.TiDBAutoAnalyzeStartTime],
		parameters[vardef.TiDBAutoAnalyzeEndTime],
	)
	startStr = start.Format("15:04")
	endStr = end.Format("15:04")
	if err != nil {
		statslogutil.StatsLogger().Error(
			"parse auto analyze period failed",
			zap.Error(err),
		)
		return startStr, endStr, false
	}
	return startStr, endStr, timeutil.WithinDayTimePeriod(start, end, time.Now())
}

// insertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
func insertAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob, instance string, procID uint64) (err error) {
	jobInfo := job.JobInfo
	const textMaxLength = 65535
	if len(jobInfo) > textMaxLength {
		jobInfo = jobInfo[:textMaxLength]
	}
	const insertJob = "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)"
	_, _, err = statsutil.ExecRows(sctx, insertJob, job.DBName, job.TableName, job.PartitionName, jobInfo, statistics.AnalyzePending, instance, procID)
	if err != nil {
		return err
	}
	const getJobID = "SELECT LAST_INSERT_ID()"
	rows, _, err := statsutil.ExecRows(sctx, getJobID)
	if err != nil {
		return err
	}
	job.ID = new(uint64)
	*job.ID = rows[0].GetUint64(0)
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("InsertAnalyzeJob",
				zap.String("table_schema", job.DBName),
				zap.String("table_name", job.TableName),
				zap.String("partition_name", job.PartitionName),
				zap.String("job_info", jobInfo),
				zap.Uint64("job_id", *job.ID),
			)
		}
	})
	return nil
}

// startAnalyzeJob marks the state of the analyze job as running and sets the start time.
func startAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob) {
	if job == nil || job.ID == nil {
		return
	}
	job.StartTime = time.Now()
	job.Progress.SetLastDumpTime(job.StartTime)
	const sql = "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?"
	_, _, err := statsutil.ExecRows(sctx, sql, job.StartTime.UTC().Format(types.TimeFormat), statistics.AnalyzeRunning, *job.ID)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzePending, statistics.AnalyzeRunning)), zap.Error(err))
	}
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("StartAnalyzeJob",
				zap.Time("start_time", job.StartTime),
				zap.Uint64("job id", *job.ID),
			)
		}
	})
}

// updateAnalyzeJobProgress updates count of the processed rows when increment reaches a threshold.
func updateAnalyzeJobProgress(sctx sessionctx.Context, job *statistics.AnalyzeJob, rowCount int64) {
	if job == nil || job.ID == nil {
		return
	}
	delta := job.Progress.Update(rowCount)
	if delta == 0 {
		return
	}
	const sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %? WHERE id = %?"
	_, _, err := statsutil.ExecRows(sctx, sql, delta, *job.ID)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("process %v rows", delta)), zap.Error(err))
	}
	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logutil.BgLogger().Info("UpdateAnalyzeJobProgress",
				zap.Int64("increase processed_rows", delta),
				zap.Uint64("job id", *job.ID),
			)
		}
	})
}

// finishAnalyzeJob finishes an analyze or merge job
func finishAnalyzeJob(sctx sessionctx.Context, job *statistics.AnalyzeJob, analyzeErr error, analyzeType statistics.JobType) {
	if job == nil || job.ID == nil {
		return
	}

	job.EndTime = time.Now()
	var sql string
	var args []any

	// process_id is used to see which process is running the analyze job and kill the analyze job. After the analyze job
	// is finished(or failed), process_id is useless and we set it to NULL to avoid `kill tidb process_id` wrongly.
	if analyzeErr != nil {
		failReason := analyzeErr.Error()
		const textMaxLength = 65535
		if len(failReason) > textMaxLength {
			failReason = failReason[:textMaxLength]
		}

		if analyzeType == statistics.TableAnalysisJob {
			sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?"
			args = []any{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFailed, failReason, *job.ID}
		} else {
			sql = "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?"
			args = []any{job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFailed, failReason, *job.ID}
		}
	} else {
		if analyzeType == statistics.TableAnalysisJob {
			sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?"
			args = []any{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFinished, *job.ID}
		} else {
			sql = "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?"
			args = []any{job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFinished, *job.ID}
		}
	}

	_, _, err := statsutil.ExecRows(sctx, sql, args...)
	if err != nil {
		state := statistics.AnalyzeFinished
		if analyzeErr != nil {
			state = statistics.AnalyzeFailed
		}
		logutil.BgLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzeRunning, state)), zap.Error(err))
	}

	failpoint.Inject("DebugAnalyzeJobOperations", func(val failpoint.Value) {
		if val.(bool) {
			logger := logutil.BgLogger().With(
				zap.Time("end_time", job.EndTime),
				zap.Uint64("job id", *job.ID),
			)
			if analyzeType == statistics.TableAnalysisJob {
				logger = logger.With(zap.Int64("increase processed_rows", job.Progress.GetDeltaCount()))
			}
			if analyzeErr != nil {
				logger = logger.With(zap.Error(analyzeErr))
			}
			logger.Info("FinishAnalyzeJob")
		}
	})
}
