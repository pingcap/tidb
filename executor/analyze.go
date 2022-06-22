// Copyright 2017 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	baseExecutor
	tasks      []*analyzeTask
	wg         util.WaitGroupWrapper
	opts       map[ast.AnalyzeOptionType]uint64
	OptionsMap map[int64]core.V2AnalyzeOptions
	snapshot   uint64
}

var (
	// RandSeed is the seed for randing package.
	// It's public for test.
	RandSeed = int64(1)
)

const (
	maxRegionSampleSize = 1000
	maxSketchSize       = 10000
)

type taskType int

const (
	colTask taskType = iota
	idxTask
	fastTask
	pkIncrementalTask
	idxIncrementalTask
)

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return err
	}
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultsCh := make(chan *statistics.AnalyzeResults, len(e.tasks))
	if len(e.tasks) < concurrency {
		concurrency = len(e.tasks)
	}
	for i := 0; i < concurrency; i++ {
		e.wg.Run(func() { e.analyzeWorker(taskCh, resultsCh) })
	}
	for _, task := range e.tasks {
		prepareV2AnalyzeJobInfo(task.colExec, false)
		AddNewAnalyzeJob(e.ctx, task.job)
	}
	failpoint.Inject("mockKillPendingAnalyzeJob", func() {
		dom := domain.GetDomain(e.ctx)
		dom.SysProcTracker().KillSysProcess(util.GetAutoAnalyzeProcID(dom.ServerID))
	})
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	e.wg.Wait()
	close(resultsCh)
	pruneMode := variable.PartitionPruneMode(e.ctx.GetSessionVars().PartitionPruneMode.Load())
	// needGlobalStats used to indicate whether we should merge the partition-level stats to global-level stats.
	needGlobalStats := pruneMode == variable.Dynamic
	globalStatsMap := make(map[globalStatsKey]globalStatsInfo)
	err = e.handleResultsError(ctx, concurrency, needGlobalStats, globalStatsMap, resultsCh)
	for _, task := range e.tasks {
		if task.colExec != nil && task.colExec.memTracker != nil {
			task.colExec.memTracker.Detach()
		}
	}
	if err != nil {
		return err
	}
	failpoint.Inject("mockKillFinishedAnalyzeJob", func() {
		dom := domain.GetDomain(e.ctx)
		dom.SysProcTracker().KillSysProcess(util.GetAutoAnalyzeProcID(dom.ServerID))
	})

	// If we enabled dynamic prune mode, then we need to generate global stats here for partition tables.
	err = e.handleGlobalStats(ctx, needGlobalStats, globalStatsMap)
	if err != nil {
		return err
	}
	err = e.saveV2AnalyzeOpts()
	if err != nil {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
	}
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	if e.ctx.GetSessionVars().InRestrictedSQL {
		return statsHandle.Update(e.ctx.GetInfoSchema().(infoschema.InfoSchema))
	}
	return statsHandle.Update(e.ctx.GetInfoSchema().(infoschema.InfoSchema), handle.WithTableStatsByQuery())
}

func (e *AnalyzeExec) saveV2AnalyzeOpts() error {
	if !variable.PersistAnalyzeOptions.Load() || len(e.OptionsMap) == 0 {
		return nil
	}
	// only to save table options if dynamic prune mode
	dynamicPrune := variable.PartitionPruneMode(e.ctx.GetSessionVars().PartitionPruneMode.Load()) == variable.Dynamic
	toSaveMap := make(map[int64]core.V2AnalyzeOptions)
	for id, opts := range e.OptionsMap {
		if !opts.IsPartition || !dynamicPrune {
			toSaveMap[id] = opts
		}
	}
	sql := new(strings.Builder)
	sqlexec.MustFormatSQL(sql, "REPLACE INTO mysql.analyze_options (table_id,sample_num,sample_rate,buckets,topn,column_choice,column_ids) VALUES ")
	idx := 0
	for _, opts := range toSaveMap {
		sampleNum := opts.RawOpts[ast.AnalyzeOptNumSamples]
		sampleRate := float64(0)
		if val, ok := opts.RawOpts[ast.AnalyzeOptSampleRate]; ok {
			sampleRate = math.Float64frombits(val)
		}
		buckets := opts.RawOpts[ast.AnalyzeOptNumBuckets]
		topn := int64(-1)
		if val, ok := opts.RawOpts[ast.AnalyzeOptNumTopN]; ok {
			topn = int64(val)
		}
		colChoice := opts.ColChoice.String()
		colIDs := make([]string, len(opts.ColumnList))
		for i, colInfo := range opts.ColumnList {
			colIDs[i] = strconv.FormatInt(colInfo.ID, 10)
		}
		colIDStrs := strings.Join(colIDs, ",")
		sqlexec.MustFormatSQL(sql, "(%?,%?,%?,%?,%?,%?,%?)", opts.PhyTableID, sampleNum, sampleRate, buckets, topn, colChoice, colIDStrs)
		if idx < len(toSaveMap)-1 {
			sqlexec.MustFormatSQL(sql, ",")
		}
		idx += 1
	}
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	_, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, sql.String())
	if err != nil {
		return err
	}
	return nil
}

func (e *AnalyzeExec) recordHistoricalStats(tableID int64) error {
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	historicalStatsEnabled, err := statsHandle.CheckHistoricalStatsEnable()
	if err != nil {
		return errors.Errorf("check tidb_enable_historical_stats failed: %v", err)
	}
	if !historicalStatsEnabled {
		return nil
	}

	is := domain.GetDomain(e.ctx).InfoSchema()
	tbl, existed := is.TableByID(tableID)
	if !existed {
		return errors.Errorf("cannot get table by id %d", tableID)
	}
	tblInfo := tbl.Meta()
	dbInfo, existed := is.SchemaByTable(tblInfo)
	if !existed {
		return errors.Errorf("cannot get DBInfo by TableID %d", tableID)
	}
	if _, err := statsHandle.RecordHistoricalStatsToStorage(dbInfo.Name.O, tblInfo); err != nil {
		return errors.Errorf("record table %s.%s's historical stats failed", dbInfo.Name.O, tblInfo.Name.O)
	}
	return nil
}

// handleResultsError will handle the error fetch from resultsCh and record it in log
func (e *AnalyzeExec) handleResultsError(ctx context.Context, concurrency int, needGlobalStats bool,
	globalStatsMap globalStatsMap, resultsCh <-chan *statistics.AnalyzeResults) error {
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	panicCnt := 0
	var err error
	for panicCnt < concurrency {
		results, ok := <-resultsCh
		if !ok {
			break
		}
		if results.Err != nil {
			err = results.Err
			if isAnalyzeWorkerPanic(err) {
				panicCnt++
			} else {
				logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
			}
			finishJobWithLog(e.ctx, results.Job, err)
			continue
		}
		if results.TableID.IsPartitionTable() && needGlobalStats {
			for _, result := range results.Ars {
				if result.IsIndex == 0 {
					// If it does not belong to the statistics of index, we need to set it to -1 to distinguish.
					globalStatsID := globalStatsKey{tableID: results.TableID.TableID, indexID: int64(-1)}
					histIDs := make([]int64, 0, len(result.Hist))
					for _, hg := range result.Hist {
						// It's normal virtual column, skip.
						if hg == nil {
							continue
						}
						histIDs = append(histIDs, hg.ID)
					}
					globalStatsMap[globalStatsID] = globalStatsInfo{isIndex: result.IsIndex, histIDs: histIDs, statsVersion: results.StatsVer}
				} else {
					for _, hg := range result.Hist {
						globalStatsID := globalStatsKey{tableID: results.TableID.TableID, indexID: hg.ID}
						globalStatsMap[globalStatsID] = globalStatsInfo{isIndex: result.IsIndex, histIDs: []int64{hg.ID}, statsVersion: results.StatsVer}
					}
				}
			}
		}
		if err1 := statsHandle.SaveTableStatsToStorage(results, results.TableID.IsPartitionTable(), e.snapshot); err1 != nil {
			err = err1
			logutil.Logger(ctx).Error("save table stats to storage failed", zap.Error(err))
			finishJobWithLog(e.ctx, results.Job, err)
		} else {
			finishJobWithLog(e.ctx, results.Job, nil)
			// Dump stats to historical storage.
			if err := e.recordHistoricalStats(results.TableID.TableID); err != nil {
				logutil.BgLogger().Error("record historical stats failed", zap.Error(err))
			}
		}
	}
	return err
}

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultsCh chan<- *statistics.AnalyzeResults) {
	var task *analyzeTask
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultsCh <- &statistics.AnalyzeResults{
				Err: getAnalyzePanicErr(r),
				Job: task.job,
			}
		}
	}()
	for {
		var ok bool
		task, ok = <-taskCh
		if !ok {
			break
		}
		StartAnalyzeJob(e.ctx, task.job)
		switch task.taskType {
		case colTask:
			resultsCh <- analyzeColumnsPushDownEntry(task.colExec)
		case idxTask:
			resultsCh <- analyzeIndexPushdown(task.idxExec)
		case fastTask:
			resultsCh <- analyzeFastExec(task.fastExec)
		case pkIncrementalTask:
			resultsCh <- analyzePKIncremental(task.colIncrementalExec)
		case idxIncrementalTask:
			resultsCh <- analyzeIndexIncremental(task.idxIncrementalExec)
		}
	}
}

type analyzeTask struct {
	taskType           taskType
	idxExec            *AnalyzeIndexExec
	colExec            *AnalyzeColumnsExec
	fastExec           *AnalyzeFastExec
	idxIncrementalExec *analyzeIndexIncrementalExec
	colIncrementalExec *analyzePKIncrementalExec
	job                *statistics.AnalyzeJob
}

type baseAnalyzeExec struct {
	ctx         sessionctx.Context
	tableID     statistics.AnalyzeTableID
	concurrency int
	analyzePB   *tipb.AnalyzeReq
	opts        map[ast.AnalyzeOptionType]uint64
	job         *statistics.AnalyzeJob
}

// AddNewAnalyzeJob records the new analyze job.
func AddNewAnalyzeJob(ctx sessionctx.Context, job *statistics.AnalyzeJob) {
	if job == nil {
		return
	}
	var instance string
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Error("failed to get server info", zap.Error(err))
		instance = "unknown"
	} else {
		instance = fmt.Sprintf("%s:%d", serverInfo.IP, serverInfo.Port)
	}
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	err = statsHandle.InsertAnalyzeJob(job, instance, ctx.GetSessionVars().ConnectionID)
	if err != nil {
		logutil.BgLogger().Error("failed to insert analyze job", zap.Error(err))
	}
}

// StartAnalyzeJob marks the state of the analyze job as running and sets the start time.
func StartAnalyzeJob(ctx sessionctx.Context, job *statistics.AnalyzeJob) {
	if job == nil || job.ID == nil {
		return
	}
	job.StartTime = time.Now()
	job.Progress.SetLastDumpTime(job.StartTime)
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	const sql = "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?"
	_, _, err := exec.ExecRestrictedSQL(context.TODO(), []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseSessionPool}, sql, job.StartTime.UTC().Format(types.TimeFormat), statistics.AnalyzeRunning, *job.ID)
	if err != nil {
		logutil.BgLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzePending, statistics.AnalyzeRunning)), zap.Error(err))
	}
}

// UpdateAnalyzeJob updates count of the processed rows when increment reaches a threshold.
func UpdateAnalyzeJob(ctx sessionctx.Context, job *statistics.AnalyzeJob, rowCount int64) {
	if job == nil || job.ID == nil {
		return
	}
	delta := job.Progress.Update(rowCount)
	if delta == 0 {
		return
	}
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	const sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %? WHERE id = %?"
	_, _, err := exec.ExecRestrictedSQL(context.TODO(), []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseSessionPool}, sql, delta, *job.ID)
	if err != nil {
		logutil.BgLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("process %v rows", delta)), zap.Error(err))
	}
}

// FinishAnalyzeJob updates the state of the analyze job to finished/failed according to `meetError` and sets the end time.
func FinishAnalyzeJob(ctx sessionctx.Context, job *statistics.AnalyzeJob, analyzeErr error) {
	if job == nil || job.ID == nil {
		return
	}
	job.EndTime = time.Now()
	var sql string
	var args []interface{}
	// process_id is used to see which process is running the analyze job and kill the analyze job. After the analyze job
	// is finished(or failed), process_id is useless and we set it to NULL to avoid `kill tidb process_id` wrongly.
	if analyzeErr != nil {
		failReason := analyzeErr.Error()
		const textMaxLength = 65535
		if len(failReason) > textMaxLength {
			failReason = failReason[:textMaxLength]
		}
		sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?"
		args = []interface{}{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFailed, failReason, *job.ID}
	} else {
		sql = "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?"
		args = []interface{}{job.Progress.GetDeltaCount(), job.EndTime.UTC().Format(types.TimeFormat), statistics.AnalyzeFinished, *job.ID}
	}
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	_, _, err := exec.ExecRestrictedSQL(context.TODO(), []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseSessionPool}, sql, args...)
	if err != nil {
		var state string
		if analyzeErr != nil {
			state = statistics.AnalyzeFailed
		} else {
			state = statistics.AnalyzeFinished
		}
		logutil.BgLogger().Warn("failed to update analyze job", zap.String("update", fmt.Sprintf("%s->%s", statistics.AnalyzeRunning, state)), zap.Error(err))
	}
}

func finishJobWithLog(sctx sessionctx.Context, job *statistics.AnalyzeJob, analyzeErr error) {
	FinishAnalyzeJob(sctx, job, analyzeErr)
	if job != nil {
		var state string
		if analyzeErr != nil {
			state = statistics.AnalyzeFailed
		} else {
			state = statistics.AnalyzeFinished
		}
		logutil.BgLogger().Info(fmt.Sprintf("analyze table `%s`.`%s` has %s", job.DBName, job.TableName, state),
			zap.String("partition", job.PartitionName),
			zap.String("job info", job.JobInfo),
			zap.Time("start time", job.StartTime),
			zap.Time("end time", job.EndTime),
			zap.String("cost", job.EndTime.Sub(job.StartTime).String()))
	}
}
