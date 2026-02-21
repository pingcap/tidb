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
	"fmt"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)
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
