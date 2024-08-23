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
	stderrors "errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tiancaiamao/gp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ exec.Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	exec.BaseExecutor
	tasks      []*analyzeTask
	wg         *util.WaitGroupPool
	opts       map[ast.AnalyzeOptionType]uint64
	OptionsMap map[int64]core.V2AnalyzeOptions
	gp         *gp.Pool
	// errExitCh is used to notice the worker that the whole analyze task is finished when to meet error.
	errExitCh chan struct{}
}

var (
	// RandSeed is the seed for randing package.
	// It's public for test.
	RandSeed = int64(1)

	// MaxRegionSampleSize is the max sample size for one region when analyze v1 collects samples from table.
	// It's public for test.
	MaxRegionSampleSize = int64(1000)
)

type taskType int

const (
	colTask taskType = iota
	idxTask
)

// Next implements the Executor Next interface.
// It will collect all the sample task and run them concurrently.
func (e *AnalyzeExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	statsHandle := domain.GetDomain(e.Ctx()).StatsHandle()
	infoSchema := sessiontxn.GetTxnManager(e.Ctx()).GetTxnInfoSchema()
	sessionVars := e.Ctx().GetSessionVars()

	// Filter the locked tables.
	tasks, needAnalyzeTableCnt, skippedTables, err := filterAndCollectTasks(e.tasks, statsHandle, infoSchema)
	if err != nil {
		return err
	}
	warnLockedTableMsg(sessionVars, needAnalyzeTableCnt, skippedTables)

	if len(tasks) == 0 {
		return nil
	}

	// Get the min number of goroutines for parallel execution.
	buildStatsConcurrency, err := getBuildStatsConcurrency(e.Ctx())
	if err != nil {
		return err
	}
	buildStatsConcurrency = min(len(tasks), buildStatsConcurrency)

	// Start workers with channel to collect results.
	taskCh := make(chan *analyzeTask, buildStatsConcurrency)
	resultsCh := make(chan *statistics.AnalyzeResults, 1)
	for i := 0; i < buildStatsConcurrency; i++ {
		e.wg.Run(func() { e.analyzeWorker(taskCh, resultsCh) })
	}
	pruneMode := variable.PartitionPruneMode(sessionVars.PartitionPruneMode.Load())
	// needGlobalStats used to indicate whether we should merge the partition-level stats to global-level stats.
	needGlobalStats := pruneMode == variable.Dynamic
	globalStatsMap := make(map[globalStatsKey]statstypes.GlobalStatsInfo)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return e.handleResultsError(buildStatsConcurrency, needGlobalStats, globalStatsMap, resultsCh, len(tasks))
	})
	for _, task := range tasks {
		prepareV2AnalyzeJobInfo(task.colExec)
		AddNewAnalyzeJob(e.Ctx(), task.job)
	}
	failpoint.Inject("mockKillPendingAnalyzeJob", func() {
		dom := domain.GetDomain(e.Ctx())
		for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
			dom.SysProcTracker().KillSysProcess(id)
		}
	})
TASKLOOP:
	for _, task := range tasks {
		select {
		case taskCh <- task:
		case <-e.errExitCh:
			break TASKLOOP
		case <-gctx.Done():
			break TASKLOOP
		}
	}
	close(taskCh)
	defer func() {
		for _, task := range tasks {
			if task.colExec != nil && task.colExec.memTracker != nil {
				task.colExec.memTracker.Detach()
			}
		}
	}()

	err = e.waitFinish(ctx, g, resultsCh)
	if err != nil {
		return err
	}

	failpoint.Inject("mockKillFinishedAnalyzeJob", func() {
		dom := domain.GetDomain(e.Ctx())
		for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
			dom.SysProcTracker().KillSysProcess(id)
		}
	})
	// If we enabled dynamic prune mode, then we need to generate global stats here for partition tables.
	if needGlobalStats {
		err = e.handleGlobalStats(statsHandle, globalStatsMap)
		if err != nil {
			return err
		}
	}

	// Update analyze options to mysql.analyze_options for auto analyze.
	err = e.saveV2AnalyzeOpts()
	if err != nil {
		sessionVars.StmtCtx.AppendWarning(err)
	}
	return statsHandle.Update(ctx, infoSchema)
}

func (e *AnalyzeExec) waitFinish(ctx context.Context, g *errgroup.Group, resultsCh chan *statistics.AnalyzeResults) error {
	checkwg, _ := errgroup.WithContext(ctx)
	checkwg.Go(func() error {
		// It is to wait for the completion of the result handler. if the result handler meets error, we should cancel
		// the analyze process by closing the errExitCh.
		err := g.Wait()
		if err != nil {
			close(e.errExitCh)
			return err
		}
		return nil
	})
	checkwg.Go(func() error {
		// Wait all workers done and close the results channel.
		e.wg.Wait()
		close(resultsCh)
		return nil
	})
	return checkwg.Wait()
}

// filterAndCollectTasks filters the tasks that are not locked and collects the table IDs.
func filterAndCollectTasks(tasks []*analyzeTask, statsHandle *handle.Handle, is infoschema.InfoSchema) ([]*analyzeTask, uint, []string, error) {
	var (
		filteredTasks       []*analyzeTask
		skippedTables       []string
		needAnalyzeTableCnt uint
		// tidMap is used to deduplicate table IDs.
		// In stats v1, analyze for each index is a single task, and they have the same table id.
		tidAndPidsMap = make(map[int64]struct{}, len(tasks))
	)

	lockedTableAndPartitionIDs, err := getLockedTableAndPartitionIDs(statsHandle, tasks)
	if err != nil {
		return nil, 0, nil, err
	}

	for _, task := range tasks {
		// Check if the table or partition is locked.
		tableID := getTableIDFromTask(task)
		_, isLocked := lockedTableAndPartitionIDs[tableID.TableID]
		// If the whole table is not locked, we should check whether the partition is locked.
		if !isLocked && tableID.IsPartitionTable() {
			_, isLocked = lockedTableAndPartitionIDs[tableID.PartitionID]
		}

		// Only analyze the table that is not locked.
		if !isLocked {
			filteredTasks = append(filteredTasks, task)
		}

		// Get the physical table ID.
		physicalTableID := tableID.TableID
		if tableID.IsPartitionTable() {
			physicalTableID = tableID.PartitionID
		}
		if _, ok := tidAndPidsMap[physicalTableID]; !ok {
			if isLocked {
				if tableID.IsPartitionTable() {
					tbl, _, def := is.FindTableByPartitionID(tableID.PartitionID)
					if def == nil {
						logutil.BgLogger().Warn("Unknown partition ID in analyze task", zap.Int64("pid", tableID.PartitionID))
					} else {
						schema, _ := infoschema.SchemaByTable(is, tbl.Meta())
						skippedTables = append(skippedTables, fmt.Sprintf("%s.%s partition (%s)", schema.Name, tbl.Meta().Name.O, def.Name.O))
					}
				} else {
					tbl, ok := is.TableByID(context.Background(), physicalTableID)
					if !ok {
						logutil.BgLogger().Warn("Unknown table ID in analyze task", zap.Int64("tid", physicalTableID))
					} else {
						schema, _ := infoschema.SchemaByTable(is, tbl.Meta())
						skippedTables = append(skippedTables, fmt.Sprintf("%s.%s", schema.Name, tbl.Meta().Name.O))
					}
				}
			} else {
				needAnalyzeTableCnt++
			}
			tidAndPidsMap[physicalTableID] = struct{}{}
		}
	}

	return filteredTasks, needAnalyzeTableCnt, skippedTables, nil
}

// getLockedTableAndPartitionIDs queries the locked tables and partitions.
func getLockedTableAndPartitionIDs(statsHandle *handle.Handle, tasks []*analyzeTask) (map[int64]struct{}, error) {
	tidAndPids := make([]int64, 0, len(tasks))
	// Check the locked tables in one transaction.
	// We need to check all tables and its partitions.
	// Because if the whole table is locked, we should skip all partitions.
	for _, task := range tasks {
		tableID := getTableIDFromTask(task)
		tidAndPids = append(tidAndPids, tableID.TableID)
		if tableID.IsPartitionTable() {
			tidAndPids = append(tidAndPids, tableID.PartitionID)
		}
	}
	return statsHandle.GetLockedTables(tidAndPids...)
}

// warnLockedTableMsg warns the locked table IDs.
func warnLockedTableMsg(sessionVars *variable.SessionVars, needAnalyzeTableCnt uint, skippedTables []string) {
	if len(skippedTables) > 0 {
		tables := strings.Join(skippedTables, ", ")
		var msg string
		if len(skippedTables) > 1 {
			msg = "skip analyze locked tables: %s"
			if needAnalyzeTableCnt > 0 {
				msg = "skip analyze locked tables: %s, other tables will be analyzed"
			}
		} else {
			msg = "skip analyze locked table: %s"
		}
		sessionVars.StmtCtx.AppendWarning(errors.NewNoStackErrorf(msg, tables))
	}
}

func getTableIDFromTask(task *analyzeTask) statistics.AnalyzeTableID {
	switch task.taskType {
	case colTask:
		return task.colExec.tableID
	case idxTask:
		return task.idxExec.tableID
	}

	panic("unreachable")
}

func (e *AnalyzeExec) saveV2AnalyzeOpts() error {
	if !variable.PersistAnalyzeOptions.Load() || len(e.OptionsMap) == 0 {
		return nil
	}
	// only to save table options if dynamic prune mode
	dynamicPrune := variable.PartitionPruneMode(e.Ctx().GetSessionVars().PartitionPruneMode.Load()) == variable.Dynamic
	toSaveMap := make(map[int64]core.V2AnalyzeOptions)
	for id, opts := range e.OptionsMap {
		if !opts.IsPartition || !dynamicPrune {
			toSaveMap[id] = opts
		}
	}
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "REPLACE INTO mysql.analyze_options (table_id,sample_num,sample_rate,buckets,topn,column_choice,column_ids) VALUES ")
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
		colIDs := make([]string, 0, len(opts.ColumnList))
		for _, colInfo := range opts.ColumnList {
			colIDs = append(colIDs, strconv.FormatInt(colInfo.ID, 10))
		}
		colIDStrs := strings.Join(colIDs, ",")
		sqlescape.MustFormatSQL(sql, "(%?,%?,%?,%?,%?,%?,%?)", opts.PhyTableID, sampleNum, sampleRate, buckets, topn, colChoice, colIDStrs)
		if idx < len(toSaveMap)-1 {
			sqlescape.MustFormatSQL(sql, ",")
		}
		idx++
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	exec := e.Ctx().GetRestrictedSQLExecutor()
	_, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		return err
	}
	return nil
}

func recordHistoricalStats(sctx sessionctx.Context, tableID int64) error {
	statsHandle := domain.GetDomain(sctx).StatsHandle()
	historicalStatsEnabled, err := statsHandle.CheckHistoricalStatsEnable()
	if err != nil {
		return errors.Errorf("check tidb_enable_historical_stats failed: %v", err)
	}
	if !historicalStatsEnabled {
		return nil
	}
	historicalStatsWorker := domain.GetDomain(sctx).GetHistoricalStatsWorker()
	historicalStatsWorker.SendTblToDumpHistoricalStats(tableID)
	return nil
}

// handleResultsError will handle the error fetch from resultsCh and record it in log
func (e *AnalyzeExec) handleResultsError(
	buildStatsConcurrency int,
	needGlobalStats bool,
	globalStatsMap globalStatsMap,
	resultsCh <-chan *statistics.AnalyzeResults,
	taskNum int,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze save stats panic", zap.Any("recover", r), zap.Stack("stack"))
			if err != nil {
				err = stderrors.Join(err, getAnalyzePanicErr(r))
			} else {
				err = getAnalyzePanicErr(r)
			}
		}
	}()
	saveStatsConcurrency := e.Ctx().GetSessionVars().AnalyzePartitionConcurrency
	// The buildStatsConcurrency of saving partition-level stats should not exceed the total number of tasks.
	saveStatsConcurrency = min(taskNum, saveStatsConcurrency)
	if saveStatsConcurrency > 1 {
		logutil.BgLogger().Info("save analyze results concurrently",
			zap.Int("buildStatsConcurrency", buildStatsConcurrency),
			zap.Int("saveStatsConcurrency", saveStatsConcurrency),
		)
		return e.handleResultsErrorWithConcurrency(buildStatsConcurrency, saveStatsConcurrency, needGlobalStats, globalStatsMap, resultsCh)
	}
	logutil.BgLogger().Info("save analyze results in single-thread",
		zap.Int("buildStatsConcurrency", buildStatsConcurrency),
		zap.Int("saveStatsConcurrency", saveStatsConcurrency),
	)
	failpoint.Inject("handleResultsErrorSingleThreadPanic", nil)
	return e.handleResultsErrorWithConcurrency(buildStatsConcurrency, saveStatsConcurrency, needGlobalStats, globalStatsMap, resultsCh)
}

func (e *AnalyzeExec) handleResultsErrorWithConcurrency(
	buildStatsConcurrency int,
	saveStatsConcurrency int,
	needGlobalStats bool,
	globalStatsMap globalStatsMap,
	resultsCh <-chan *statistics.AnalyzeResults,
) error {
	statsHandle := domain.GetDomain(e.Ctx()).StatsHandle()
	wg := util.NewWaitGroupPool(e.gp)
	saveResultsCh := make(chan *statistics.AnalyzeResults, saveStatsConcurrency)
	errCh := make(chan error, saveStatsConcurrency)
	for i := 0; i < saveStatsConcurrency; i++ {
		worker := newAnalyzeSaveStatsWorker(saveResultsCh, errCh, &e.Ctx().GetSessionVars().SQLKiller)
		ctx1 := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
		wg.Run(func() {
			worker.run(ctx1, statsHandle, e.Ctx().GetSessionVars().EnableAnalyzeSnapshot)
		})
	}
	tableIDs := map[int64]struct{}{}
	panicCnt := 0
	var err error
	// Only if all the analyze workers exit can we close the saveResultsCh.
	for panicCnt < buildStatsConcurrency {
		if err := e.Ctx().GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			close(saveResultsCh)
			return err
		}
		results, ok := <-resultsCh
		if !ok {
			break
		}
		if results.Err != nil {
			err = results.Err
			if isAnalyzeWorkerPanic(err) {
				panicCnt++
			} else {
				logutil.BgLogger().Error("receive error when saving analyze results", zap.Error(err))
			}
			finishJobWithLog(statsHandle, results.Job, err)
			continue
		}
		handleGlobalStats(needGlobalStats, globalStatsMap, results)
		tableIDs[results.TableID.GetStatisticsID()] = struct{}{}
		saveResultsCh <- results
	}
	close(saveResultsCh)
	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		errMsg := make([]string, 0)
		for err1 := range errCh {
			errMsg = append(errMsg, err1.Error())
		}
		err = errors.New(strings.Join(errMsg, ","))
	}
	for tableID := range tableIDs {
		// Dump stats to historical storage.
		if err := recordHistoricalStats(e.Ctx(), tableID); err != nil {
			logutil.BgLogger().Error("record historical stats failed", zap.Error(err))
		}
	}
	return err
}

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultsCh chan<- *statistics.AnalyzeResults) {
	var task *analyzeTask
	statsHandle := domain.GetDomain(e.Ctx()).StatsHandle()
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			// If errExitCh is closed, it means the whole analyze task is aborted. So we do not need to send the result to resultsCh.
			err := getAnalyzePanicErr(r)
			select {
			case resultsCh <- &statistics.AnalyzeResults{
				Err: err,
				Job: task.job,
			}:
			case <-e.errExitCh:
				logutil.BgLogger().Error("analyze worker exits because the whole analyze task is aborted", zap.Error(err))
			}
		}
	}()
	for {
		var ok bool
		task, ok = <-taskCh
		if !ok {
			break
		}
		failpoint.Inject("handleAnalyzeWorkerPanic", nil)
		statsHandle.StartAnalyzeJob(task.job)
		switch task.taskType {
		case colTask:
			select {
			case <-e.errExitCh:
				return
			case resultsCh <- analyzeColumnsPushDownEntry(e.gp, task.colExec):
			}
		case idxTask:
			select {
			case <-e.errExitCh:
				return
			case resultsCh <- analyzeIndexPushdown(task.idxExec):
			}
		}
	}
}

type analyzeTask struct {
	taskType taskType
	idxExec  *AnalyzeIndexExec
	colExec  *AnalyzeColumnsExec
	job      *statistics.AnalyzeJob
}

type baseAnalyzeExec struct {
	ctx         sessionctx.Context
	tableID     statistics.AnalyzeTableID
	concurrency int
	analyzePB   *tipb.AnalyzeReq
	opts        map[ast.AnalyzeOptionType]uint64
	job         *statistics.AnalyzeJob
	snapshot    uint64
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
		instance = net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	}
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	err = statsHandle.InsertAnalyzeJob(job, instance, ctx.GetSessionVars().ConnectionID)
	if err != nil {
		logutil.BgLogger().Error("failed to insert analyze job", zap.Error(err))
	}
}

func finishJobWithLog(statsHandle *handle.Handle, job *statistics.AnalyzeJob, analyzeErr error) {
	statsHandle.FinishAnalyzeJob(job, analyzeErr, statistics.TableAnalysisJob)
	if job != nil {
		var state string
		if analyzeErr != nil {
			state = statistics.AnalyzeFailed
			logutil.BgLogger().Warn(fmt.Sprintf("analyze table `%s`.`%s` has %s", job.DBName, job.TableName, state),
				zap.String("partition", job.PartitionName),
				zap.String("job info", job.JobInfo),
				zap.Time("start time", job.StartTime),
				zap.Time("end time", job.EndTime),
				zap.String("cost", job.EndTime.Sub(job.StartTime).String()),
				zap.String("sample rate reason", job.SampleRateReason),
				zap.Error(analyzeErr))
		} else {
			state = statistics.AnalyzeFinished
			logutil.BgLogger().Info(fmt.Sprintf("analyze table `%s`.`%s` has %s", job.DBName, job.TableName, state),
				zap.String("partition", job.PartitionName),
				zap.String("job info", job.JobInfo),
				zap.Time("start time", job.StartTime),
				zap.Time("end time", job.EndTime),
				zap.String("cost", job.EndTime.Sub(job.StartTime).String()),
				zap.String("sample rate reason", job.SampleRateReason))
		}
	}
}

func handleGlobalStats(needGlobalStats bool, globalStatsMap globalStatsMap, results *statistics.AnalyzeResults) {
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
				globalStatsMap[globalStatsID] = statstypes.GlobalStatsInfo{IsIndex: result.IsIndex, HistIDs: histIDs, StatsVersion: results.StatsVer}
			} else {
				for _, hg := range result.Hist {
					globalStatsID := globalStatsKey{tableID: results.TableID.TableID, indexID: hg.ID}
					globalStatsMap[globalStatsID] = statstypes.GlobalStatsInfo{IsIndex: result.IsIndex, HistIDs: []int64{hg.ID}, StatsVersion: results.StatsVer}
				}
			}
		}
	}
}
