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
	"net"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/refresher"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

// statsAnalyze implements util.StatsAnalyze.
// statsAnalyze is used to handle auto-analyze and manage analyze jobs.
type statsAnalyze struct {
	statsHandle statstypes.StatsHandle
	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sysproctrack.Tracker
	// refresher is used to refresh the analyze job queue and analyze the highest priority tables.
	// It is only used when auto-analyze priority queue is enabled.
	refresher *refresher.Refresher
}

// NewStatsAnalyze creates a new StatsAnalyze.
func NewStatsAnalyze(
	ctx context.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
) statstypes.StatsAnalyze {
	// Usually, we should only create the refresher when auto-analyze priority queue is enabled.
	// But to allow users to enable auto-analyze priority queue on the fly, we need to create the refresher here.
	r := refresher.NewRefresher(ctx, statsHandle, sysProcTracker, ddlNotifier)
	return &statsAnalyze{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		refresher:      r,
	}
}

// InsertAnalyzeJob inserts the analyze job to the storage.
func (sa *statsAnalyze) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return insertAnalyzeJob(sctx, job, instance, procID)
	})
}

func (sa *statsAnalyze) StartAnalyzeJob(job *statistics.AnalyzeJob) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		startAnalyzeJob(sctx, job)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to start analyze job", zap.Error(err))
	}
}

func (sa *statsAnalyze) UpdateAnalyzeJobProgress(job *statistics.AnalyzeJob, rowCount int64) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		updateAnalyzeJobProgress(sctx, job, rowCount)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to update analyze job progress", zap.Error(err))
	}
}

func (sa *statsAnalyze) FinishAnalyzeJob(job *statistics.AnalyzeJob, failReason error, analyzeType statistics.JobType) {
	err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		finishAnalyzeJob(sctx, job, failReason, analyzeType)
		return nil
	})
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to finish analyze job", zap.Error(err))
	}
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (sa *statsAnalyze) DeleteAnalyzeJobs(updateTime time.Time) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		_, _, err := statsutil.ExecRows(sctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
		return err
	})
}

// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the potentially corrupted analyze job.
// It only cleans up the jobs that are associated with the current instance.
func (sa *statsAnalyze) CleanupCorruptedAnalyzeJobsOnCurrentInstance(currentRunningProcessIDs map[uint64]struct{}) error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return CleanupCorruptedAnalyzeJobsOnCurrentInstance(sctx, currentRunningProcessIDs)
	}, statsutil.FlagWrapTxn)
}

// CleanupCorruptedAnalyzeJobsOnDeadInstances removes analyze jobs that may have been corrupted.
// Specifically, it removes jobs associated with instances that no longer exist in the cluster.
func (sa *statsAnalyze) CleanupCorruptedAnalyzeJobsOnDeadInstances() error {
	return statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
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
func (sa *statsAnalyze) HandleAutoAnalyze() (analyzed bool) {
	if err := statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		analyzed = sa.handleAutoAnalyze(sctx)
		return nil
	}); err != nil {
		statslogutil.StatsErrVerboseSampleLogger().Error("Failed to handle auto analyze", zap.Error(err))
	}
	return
}

// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
func (sa *statsAnalyze) CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool {
	// We simply choose one physical id to get its stats.
	var tbl *statistics.Table
	for _, pid := range physicalIDs {
		tbl = sa.statsHandle.GetPhysicalTableStats(pid, tblInfo)
		if !tbl.Pseudo {
			break
		}
	}
	if tbl == nil || tbl.Pseudo {
		return true
	}
	return statistics.CheckAnalyzeVerOnTable(tbl, version)
}

// GetPriorityQueueSnapshot returns the stats priority queue snapshot.
func (sa *statsAnalyze) GetPriorityQueueSnapshot() (statstypes.PriorityQueueSnapshot, error) {
	return sa.refresher.GetPriorityQueueSnapshot()
}

// ClosePriorityQueue closes the stats priority queue if initialized.
// NOTE: This does NOT stop the analyze worker. Only the priority queue is closed.
func (sa *statsAnalyze) ClosePriorityQueue() {
	sa.refresher.ClosePriorityQueue()
}

func (sa *statsAnalyze) handleAutoAnalyze(sctx sessionctx.Context) bool {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error(
				"HandleAutoAnalyze panicked",
				zap.Any("recover", r),
				zap.Stack("stack"),
			)
		}
	}()
	if vardef.EnableAutoAnalyzePriorityQueue.Load() {
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

	parameters := exec.GetAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[vardef.TiDBAutoAnalyzeRatio])
	start, end, ok := checkAutoAnalyzeWindow(parameters)
	if !ok {
		return false
	}

	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())

	return RandomPickOneTableAndTryAutoAnalyze(
		sctx,
		sa.statsHandle,
		sa.sysProcTracker,
		autoAnalyzeRatio,
		pruneMode,
		start,
		end,
	)
}

// Close closes the auto-analyze worker.
func (sa *statsAnalyze) Close() {
	sa.refresher.Close()
}

// CheckAutoAnalyzeWindow determine the time window for auto-analysis and verify if the current time falls within this range.
func CheckAutoAnalyzeWindow(sctx sessionctx.Context) (startStr, endStr string, ok bool) {
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	start, end, ok := checkAutoAnalyzeWindow(parameters)
	startStr = start.Format("15:04")
	endStr = end.Format("15:04")
	return
}

func checkAutoAnalyzeWindow(parameters map[string]string) (_, _ time.Time, _ bool) {
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[vardef.TiDBAutoAnalyzeStartTime],
		parameters[vardef.TiDBAutoAnalyzeEndTime],
	)
	if err != nil {
		statslogutil.StatsLogger().Error(
			"parse auto analyze period failed",
			zap.Error(err),
		)
		return start, end, false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return start, end, false
	}
	return start, end, true
}

// RandomPickOneTableAndTryAutoAnalyze randomly picks one table and tries to analyze it.
// 1. If the table is not analyzed, analyze it.
// 2. If the table is analyzed, analyze it when "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio".
// 3. If the table is analyzed, analyze its indices when the index is not analyzed.
// 4. If the table is locked, skip it.
// Exposed solely for testing.
