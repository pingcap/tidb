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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	baseExecutor
	tasks []*analyzeTask
	wg    *sync.WaitGroup
	opts  map[ast.AnalyzeOptionType]uint64
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

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return err
	}
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan analyzeResult, len(e.tasks))
	e.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.analyzeWorker(taskCh, resultCh, i == 0)
	}
	for _, task := range e.tasks {
		statistics.AddNewAnalyzeJob(task.job)
	}
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	panicCnt := 0

	pruneMode := variable.PartitionPruneMode(e.ctx.GetSessionVars().PartitionPruneMode.Load())
	// needGlobalStats used to indicate whether we should merge the partition-level stats to global-level stats.
	needGlobalStats := pruneMode == variable.Dynamic
	type globalStatsKey struct {
		tableID int64
		indexID int64
	}
	type globalStatsInfo struct {
		isIndex int
		// When the `isIndex == 0`, the idxID will be the column ID.
		// Otherwise, the idxID will be the index ID.
		idxID        int64
		statsVersion int
	}
	// globalStatsMap is a map used to store which partition tables and the corresponding indexes need global-level stats.
	// The meaning of key in map is the structure that used to store the tableID and indexID.
	// The meaning of value in map is some additional information needed to build global-level stats.
	globalStatsMap := make(map[globalStatsKey]globalStatsInfo)
	finishJobWithLogFn := func(ctx context.Context, job *statistics.AnalyzeJob, meetError bool) {
		job.Finish(meetError)
		if job != nil {
			logutil.Logger(ctx).Info(fmt.Sprintf("analyze table `%s`.`%s` has %s", job.DBName, job.TableName, job.State),
				zap.String("partition", job.PartitionName),
				zap.String("job info", job.JobInfo),
				zap.Time("start time", job.StartTime),
				zap.Time("end time", job.EndTime),
				zap.String("cost", job.EndTime.Sub(job.StartTime).String()))
		}
	}
	for panicCnt < concurrency {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			err = result.Err
			if err == errAnalyzeWorkerPanic {
				panicCnt++
			} else {
				logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
			}
			finishJobWithLogFn(ctx, result.job, true)
			continue
		}
		statisticsID := result.TableID.GetStatisticsID()
		for i, hg := range result.Hist {
			// It's normal virtual column, skip.
			if hg == nil {
				continue
			}
			if result.TableID.IsPartitionTable() && needGlobalStats {
				// If it does not belong to the statistics of index, we need to set it to -1 to distinguish.
				idxID := int64(-1)
				if result.IsIndex != 0 {
					idxID = hg.ID
				}
				globalStatsID := globalStatsKey{result.TableID.TableID, idxID}
				if _, ok := globalStatsMap[globalStatsID]; !ok {
					globalStatsMap[globalStatsID] = globalStatsInfo{result.IsIndex, hg.ID, result.StatsVer}
				}
			}
			var err1 error
			if result.StatsVer == statistics.Version2 {
				err1 = statsHandle.SaveStatsToStorage(statisticsID, result.Count, result.IsIndex, hg, nil, result.TopNs[i], result.Fms[i], result.StatsVer, 1, result.TableID.IsPartitionTable() && needGlobalStats)
			} else {
				err1 = statsHandle.SaveStatsToStorage(statisticsID, result.Count, result.IsIndex, hg, result.Cms[i], result.TopNs[i], result.Fms[i], result.StatsVer, 1, result.TableID.IsPartitionTable() && needGlobalStats)
			}
			if err1 != nil {
				err = err1
				logutil.Logger(ctx).Error("save stats to storage failed", zap.Error(err))
				finishJobWithLogFn(ctx, result.job, true)
				continue
			}
		}
		if err1 := statsHandle.SaveExtendedStatsToStorage(statisticsID, result.ExtStats, false); err1 != nil {
			err = err1
			logutil.Logger(ctx).Error("save extended stats to storage failed", zap.Error(err))
			finishJobWithLogFn(ctx, result.job, true)
		} else {
			finishJobWithLogFn(ctx, result.job, false)
		}
	}
	for _, task := range e.tasks {
		statistics.MoveToHistory(task.job)
	}
	if err != nil {
		return err
	}
	if needGlobalStats {
		for globalStatsID, info := range globalStatsMap {
			globalStats, err := statsHandle.MergePartitionStats2GlobalStatsByTableID(e.ctx, e.opts, e.ctx.GetInfoSchema().(infoschema.InfoSchema), globalStatsID.tableID, info.isIndex, info.idxID)
			if err != nil {
				if types.ErrPartitionStatsMissing.Equal(err) {
					// When we find some partition-level stats are missing, we need to report warning.
					e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
					continue
				}
				return err
			}
			for i := 0; i < globalStats.Num; i++ {
				hg, cms, topN, fms := globalStats.Hg[i], globalStats.Cms[i], globalStats.TopN[i], globalStats.Fms[i]
				// fms for global stats doesn't need to dump to kv.
				err = statsHandle.SaveStatsToStorage(globalStatsID.tableID, globalStats.Count, info.isIndex, hg, cms, topN, fms, info.statsVersion, 1, false)
				if err != nil {
					logutil.Logger(ctx).Error("save global-level stats to storage failed", zap.Error(err))
				}
			}
		}
	}
	return statsHandle.Update(e.ctx.GetInfoSchema().(infoschema.InfoSchema))
}

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionOrGlobalSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

type taskType int

const (
	colTask taskType = iota
	idxTask
	fastTask
	pkIncrementalTask
	idxIncrementalTask
)

type analyzeTask struct {
	taskType           taskType
	idxExec            *AnalyzeIndexExec
	colExec            *AnalyzeColumnsExec
	fastExec           *AnalyzeFastExec
	idxIncrementalExec *analyzeIndexIncrementalExec
	colIncrementalExec *analyzePKIncrementalExec
	job                *statistics.AnalyzeJob
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- analyzeResult, isCloseChanThread bool) {
	var task *analyzeTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- analyzeResult{
				Err: errAnalyzeWorkerPanic,
				job: task.job,
			}
		}
		e.wg.Done()
		if isCloseChanThread {
			e.wg.Wait()
			close(resultCh)
		}
	}()
	for {
		var ok bool
		task, ok = <-taskCh
		if !ok {
			break
		}
		task.job.Start()
		switch task.taskType {
		case colTask:
			task.colExec.job = task.job
			for _, result := range analyzeColumnsPushdown(task.colExec) {
				resultCh <- result
			}
		case idxTask:
			task.idxExec.job = task.job
			resultCh <- analyzeIndexPushdown(task.idxExec)
		case fastTask:
			task.fastExec.job = task.job
			task.job.Start()
			for _, result := range analyzeFastExec(task.fastExec) {
				resultCh <- result
			}
		case pkIncrementalTask:
			task.colIncrementalExec.job = task.job
			resultCh <- analyzePKIncremental(task.colIncrementalExec)
		case idxIncrementalTask:
			task.idxIncrementalExec.job = task.job
			resultCh <- analyzeIndexIncremental(task.idxIncrementalExec)
		}
	}
}

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) analyzeResult {
	ranges := ranger.FullRange()
	// For single-column index, we do not load null rows from TiKV, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-column index is always 0 then.
	if len(idxExec.idxInfo.Columns) == 1 {
		ranges = ranger.FullNotNullRange()
	}
	hist, cms, fms, topN, err := idxExec.buildStats(ranges, true)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	var statsVer = statistics.Version1
	if idxExec.analyzePB.IdxReq.Version != nil {
		statsVer = int(*idxExec.analyzePB.IdxReq.Version)
	}
	result := analyzeResult{
		TableID:  idxExec.tableID,
		Hist:     []*statistics.Histogram{hist},
		Cms:      []*statistics.CMSketch{cms},
		TopNs:    []*statistics.TopN{topN},
		Fms:      []*statistics.FMSketch{fms},
		IsIndex:  1,
		job:      idxExec.job,
		StatsVer: statsVer,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	if topN.TotalCount() > 0 {
		result.Count += int64(topN.TotalCount())
	}
	return result
}

func analyzeIndexNDVPushDown(idxExec *AnalyzeIndexExec) analyzeResult {
	ranges := ranger.FullRange()
	// For single-column index, we do not load null rows from TiKV, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-column index is always 0 then.
	if len(idxExec.idxInfo.Columns) == 1 {
		ranges = ranger.FullNotNullRange()
	}
	fms, nullHist, err := idxExec.buildSimpleStats(ranges, len(idxExec.idxInfo.Columns) == 1)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	result := analyzeResult{
		TableID: idxExec.tableID,
		Fms:     []*statistics.FMSketch{fms},
		// We use histogram to get the Index's ID.
		Hist:    []*statistics.Histogram{statistics.NewHistogram(idxExec.idxInfo.ID, 0, 0, statistics.Version1, types.NewFieldType(mysql.TypeBlob), 0, 0)},
		IsIndex: 1,
		job:     idxExec.job,
		// TODO: avoid reusing Version1.
		StatsVer: statistics.Version1,
	}
	if nullHist != nil && nullHist.Len() > 0 {
		result.Count = nullHist.Buckets[nullHist.Len()-1].Count
	}
	return result
}

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	ctx            sessionctx.Context
	tableID        core.AnalyzeTableID
	idxInfo        *model.IndexInfo
	isCommonHandle bool
	concurrency    int
	analyzePB      *tipb.AnalyzeReq
	result         distsql.SelectResult
	countNullRes   distsql.SelectResult
	opts           map[ast.AnalyzeOptionType]uint64
	job            *statistics.AnalyzeJob
}

// fetchAnalyzeResult builds and dispatches the `kv.Request` from given ranges, and stores the `SelectResult`
// in corresponding fields based on the input `isNullRange` argument, which indicates if the range is the
// special null range for single-column index to get the null count.
func (e *AnalyzeIndexExec) fetchAnalyzeResult(ranges []*ranger.Range, isNullRange bool) error {
	var builder distsql.RequestBuilder
	var kvReqBuilder *distsql.RequestBuilder
	if e.isCommonHandle && e.idxInfo.Primary {
		kvReqBuilder = builder.SetHandleRangesForTables(e.ctx.GetSessionVars().StmtCtx, []int64{e.tableID.GetStatisticsID()}, true, ranges, nil)
	} else {
		kvReqBuilder = builder.SetIndexRangesForTables(e.ctx.GetSessionVars().StmtCtx, []int64{e.tableID.GetStatisticsID()}, e.idxInfo.ID, ranges)
	}
	kvReqBuilder.SetResourceGroupTag(e.ctx.GetSessionVars().StmtCtx)
	kvReq, err := kvReqBuilder.
		SetAnalyzeRequest(e.analyzePB).
		SetStartTS(math.MaxUint64).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetSessionVars().StmtCtx.MemTracker)
	if err != nil {
		return err
	}
	if isNullRange {
		e.countNullRes = result
	} else {
		e.result = result
	}
	return nil
}

func (e *AnalyzeIndexExec) open(ranges []*ranger.Range, considerNull bool) error {
	err := e.fetchAnalyzeResult(ranges, false)
	if err != nil {
		return err
	}
	if considerNull && len(e.idxInfo.Columns) == 1 {
		ranges = ranger.NullRange()
		err = e.fetchAnalyzeResult(ranges, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateIndexResult(
	ctx *stmtctx.StatementContext,
	resp *tipb.AnalyzeIndexResp,
	job *statistics.AnalyzeJob,
	hist *statistics.Histogram,
	cms *statistics.CMSketch,
	fms *statistics.FMSketch,
	topn *statistics.TopN,
	idxInfo *model.IndexInfo,
	numBuckets int,
	numTopN int,
	statsVer int,
) (
	*statistics.Histogram,
	*statistics.CMSketch,
	*statistics.FMSketch,
	*statistics.TopN,
	error,
) {
	var err error
	needCMS := cms != nil
	respHist := statistics.HistogramFromProto(resp.Hist)
	if job != nil {
		job.Update(int64(respHist.TotalRowCount()))
	}
	hist, err = statistics.MergeHistograms(ctx, hist, respHist, numBuckets, statsVer)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if needCMS {
		if resp.Cms == nil {
			logutil.Logger(context.TODO()).Warn("nil CMS in response", zap.String("table", idxInfo.Table.O), zap.String("index", idxInfo.Name.O))
		} else {
			cm, tmpTopN := statistics.CMSketchAndTopNFromProto(resp.Cms)
			if err := cms.MergeCMSketch(cm); err != nil {
				return nil, nil, nil, nil, err
			}
			statistics.MergeTopNAndUpdateCMSketch(topn, tmpTopN, cms, uint32(numTopN))
		}
	}
	if fms != nil && resp.Collector != nil && resp.Collector.FmSketch != nil {
		fms.MergeFMSketch(statistics.FMSketchFromProto(resp.Collector.FmSketch))
	}
	return hist, cms, fms, topn, nil
}

func (e *AnalyzeIndexExec) buildStatsFromResult(result distsql.SelectResult, needCMS bool) (*statistics.Histogram, *statistics.CMSketch, *statistics.FMSketch, *statistics.TopN, error) {
	failpoint.Inject("buildStatsFromResult", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, nil, nil, nil, errors.New("mock buildStatsFromResult error"))
		}
	})
	hist := &statistics.Histogram{}
	var cms *statistics.CMSketch
	var topn *statistics.TopN
	if needCMS {
		cms = statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]))
		topn = statistics.NewTopN(int(e.opts[ast.AnalyzeOptNumTopN]))
	}
	fms := statistics.NewFMSketch(maxSketchSize)
	statsVer := statistics.Version1
	if e.analyzePB.IdxReq.Version != nil {
		statsVer = int(*e.analyzePB.IdxReq.Version)
	}
	for {
		data, err := result.NextRaw(context.TODO())
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		hist, cms, fms, topn, err = updateIndexResult(e.ctx.GetSessionVars().StmtCtx, resp, e.job, hist, cms, fms, topn,
			e.idxInfo, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), statsVer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	if needCMS && topn.TotalCount() > 0 {
		hist.RemoveVals(topn.TopN)
	}
	if needCMS && cms != nil {
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	return hist, cms, fms, topn, nil
}

func (e *AnalyzeIndexExec) buildStats(ranges []*ranger.Range, considerNull bool) (hist *statistics.Histogram, cms *statistics.CMSketch, fms *statistics.FMSketch, topN *statistics.TopN, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	hist, cms, fms, topN, err = e.buildStatsFromResult(e.result, true)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if e.countNullRes != nil {
		nullHist, _, _, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			hist.NullCount = nullHist.Buckets[l-1].Count
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, cms, fms, topN, nil
}

func (e *AnalyzeIndexExec) buildSimpleStats(ranges []*ranger.Range, considerNull bool) (fms *statistics.FMSketch, nullHist *statistics.Histogram, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	_, _, fms, _, err = e.buildStatsFromResult(e.result, false)
	if e.countNullRes != nil {
		nullHist, _, _, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			return fms, nullHist, nil
		}
	}
	return fms, nil, nil
}

func analyzeColumnsPushdown(colExec *AnalyzeColumnsExec) []analyzeResult {
	var ranges []*ranger.Range
	if hc := colExec.handleCols; hc != nil {
		if hc.IsInt() {
			ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(hc.GetCol(0).RetType.Flag))
		} else {
			ranges = ranger.FullNotNullRange()
		}
	} else {
		ranges = ranger.FullIntRange(false)
	}
	collExtStats := colExec.ctx.GetSessionVars().EnableExtendedStats
	if colExec.StatsVersion == statistics.Version2 {
		specialIndexes := make([]*model.IndexInfo, 0, len(colExec.indexes))
		specialIndexesOffsets := make([]int, 0, len(colExec.indexes))
		for i, idx := range colExec.indexes {
			isSpecial := false
			for _, col := range idx.Columns {
				colInfo := colExec.colsInfo[col.Offset]
				isVirtualCol := colInfo.IsGenerated() && !colInfo.GeneratedStored
				isPrefixCol := col.Length != types.UnspecifiedLength
				if isVirtualCol || isPrefixCol {
					isSpecial = true
					break
				}
			}
			if isSpecial {
				specialIndexesOffsets = append(specialIndexesOffsets, i)
				specialIndexes = append(specialIndexes, idx)
			}
		}
		idxNDVPushDownCh := make(chan analyzeIndexNDVTotalResult, 1)
		go colExec.handleNDVForSpecialIndexes(specialIndexes, idxNDVPushDownCh)
		count, hists, topns, fmSketches, extStats, err := colExec.buildSamplingStats(ranges, collExtStats, specialIndexesOffsets, idxNDVPushDownCh)
		if err != nil {
			return []analyzeResult{{Err: err, job: colExec.job}}
		}
		cLen := len(colExec.analyzePB.ColReq.ColumnsInfo)
		colGroupResult := analyzeResult{
			TableID:  colExec.TableID,
			Hist:     hists[cLen:],
			TopNs:    topns[cLen:],
			Fms:      fmSketches[cLen:],
			job:      colExec.job,
			StatsVer: colExec.StatsVersion,
			Count:    count,
			IsIndex:  1,
		}
		// Discard stats of _tidb_rowid.
		// Because the process of analyzing will keep the order of results be the same as the colsInfo in the analyze task,
		// and in `buildAnalyzeFullSamplingTask` we always place the _tidb_rowid at the last of colsInfo, so if there are
		// stats for _tidb_rowid, it must be at the end of the column stats.
		// Virtual column has no histogram yet. So we check nil here.
		if hists[cLen-1] != nil && hists[cLen-1].ID == -1 {
			cLen -= 1
		}
		colResult := analyzeResult{
			TableID:  colExec.TableID,
			Hist:     hists[:cLen],
			TopNs:    topns[:cLen],
			Fms:      fmSketches[:cLen],
			ExtStats: extStats,
			job:      colExec.job,
			StatsVer: colExec.StatsVersion,
			Count:    count,
		}

		return []analyzeResult{colResult, colGroupResult}
	}
	hists, cms, topNs, fms, extStats, err := colExec.buildStats(ranges, collExtStats)
	if err != nil {
		return []analyzeResult{{Err: err, job: colExec.job}}
	}

	if hasPkHist(colExec.handleCols) {
		PKresult := analyzeResult{
			TableID:  colExec.TableID,
			Hist:     hists[:1],
			Cms:      cms[:1],
			TopNs:    topNs[:1],
			Fms:      fms[:1],
			ExtStats: nil,
			job:      nil,
			StatsVer: statistics.Version1,
		}
		PKresult.Count = int64(PKresult.Hist[0].TotalRowCount())
		restResult := analyzeResult{
			TableID:  colExec.TableID,
			Hist:     hists[1:],
			Cms:      cms[1:],
			TopNs:    topNs[1:],
			Fms:      fms[1:],
			ExtStats: extStats,
			job:      colExec.job,
			StatsVer: colExec.StatsVersion,
		}
		restResult.Count = PKresult.Count
		return []analyzeResult{PKresult, restResult}
	}
	var result []analyzeResult
	if colExec.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
		result = append(result, analyzeResult{
			TableID:  colExec.TableID,
			Hist:     []*statistics.Histogram{hists[0]},
			Cms:      []*statistics.CMSketch{cms[0]},
			TopNs:    []*statistics.TopN{topNs[0]},
			Fms:      []*statistics.FMSketch{nil},
			IsIndex:  1,
			job:      colExec.job,
			StatsVer: colExec.StatsVersion,
		})
		hists = hists[1:]
		cms = cms[1:]
		topNs = topNs[1:]
	}
	colResult := analyzeResult{
		TableID:  colExec.TableID,
		Hist:     hists,
		Cms:      cms,
		TopNs:    topNs,
		Fms:      fms,
		ExtStats: extStats,
		job:      colExec.job,
		StatsVer: colExec.StatsVersion,
	}
	colResult.Count = int64(colResult.Hist[0].TotalRowCount())
	if colResult.StatsVer >= statistics.Version2 {
		colResult.Count += int64(topNs[0].TotalCount())
	}
	return append(result, colResult)
}

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	ctx           sessionctx.Context
	tableInfo     *model.TableInfo
	colsInfo      []*model.ColumnInfo
	handleCols    core.HandleCols
	concurrency   int
	analyzePB     *tipb.AnalyzeReq
	commonHandle  *model.IndexInfo
	resultHandler *tableResultHandler
	opts          map[ast.AnalyzeOptionType]uint64
	job           *statistics.AnalyzeJob
	indexes       []*model.IndexInfo
	core.AnalyzeInfo

	subIndexWorkerWg  *sync.WaitGroup
	samplingBuilderWg *sync.WaitGroup
	samplingMergeWg   *sync.WaitGroup

	schemaForVirtualColEval *expression.Schema
}

func (e *AnalyzeColumnsExec) open(ranges []*ranger.Range) error {
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(ranges, true, false, !hasPkHist(e.handleCols))
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeColumnsExec) buildResp(ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	reqBuilder := builder.SetHandleRangesForTables(e.ctx.GetSessionVars().StmtCtx, []int64{e.TableID.GetStatisticsID()}, e.handleCols != nil && !e.handleCols.IsInt(), ranges, nil)
	builder.SetResourceGroupTag(e.ctx.GetSessionVars().StmtCtx)
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of columns.
	kvReq, err := reqBuilder.
		SetAnalyzeRequest(e.analyzePB).
		SetStartTS(math.MaxUint64).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetSessionVars().StmtCtx.MemTracker)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// decodeSampleDataWithVirtualColumn constructs the virtual column by evaluating from the deocded normal columns.
// If it failed, it would return false to trigger normal decoding way without the virtual column.
func (e AnalyzeColumnsExec) decodeSampleDataWithVirtualColumn(
	collector *statistics.RowSampleCollector,
	fieldTps []*types.FieldType,
	virtualColIdx []int,
	schema *expression.Schema,
) error {
	totFts := make([]*types.FieldType, 0, e.schemaForVirtualColEval.Len())
	for _, col := range e.schemaForVirtualColEval.Columns {
		totFts = append(totFts, col.RetType)
	}
	chk := chunk.NewChunkWithCapacity(totFts, len(collector.Samples))
	decoder := codec.NewDecoder(chk, e.ctx.GetSessionVars().TimeZone)
	for _, sample := range collector.Samples {
		for i := range sample.Columns {
			if schema.Columns[i].VirtualExpr != nil {
				continue
			}
			_, err := decoder.DecodeOne(sample.Columns[i].GetBytes(), i, e.schemaForVirtualColEval.Columns[i].RetType)
			if err != nil {
				return err
			}
		}
	}
	err := FillVirtualColumnValue(fieldTps, virtualColIdx, schema, e.colsInfo, e.ctx, chk)
	if err != nil {
		return err
	}
	iter := chunk.NewIterator4Chunk(chk)
	for row, i := iter.Begin(), 0; row != iter.End(); row, i = iter.Next(), i+1 {
		datums := row.GetDatumRow(totFts)
		collector.Samples[i].Columns = datums
	}
	return nil
}

func (e *AnalyzeColumnsExec) buildSamplingStats(
	ranges []*ranger.Range,
	needExtStats bool,
	indexesWithVirtualColOffsets []int,
	idxNDVPushDownCh chan analyzeIndexNDVTotalResult,
) (
	count int64,
	hists []*statistics.Histogram,
	topns []*statistics.TopN,
	fmSketches []*statistics.FMSketch,
	extStats *statistics.ExtendedStatsColl,
	err error,
) {
	if err = e.open(ranges); err != nil {
		return 0, nil, nil, nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			err = err1
		}
	}()

	l := len(e.analyzePB.ColReq.ColumnsInfo) + len(e.analyzePB.ColReq.ColumnGroups)
	rootRowCollector := &statistics.RowSampleCollector{
		NullCount:     make([]int64, l),
		FMSketches:    make([]*statistics.FMSketch, 0, l),
		TotalSizes:    make([]int64, l),
		Samples:       make(statistics.WeightedRowSampleHeap, 0, e.analyzePB.ColReq.SampleSize),
		MaxSampleSize: int(e.analyzePB.ColReq.SampleSize),
	}
	for i := 0; i < l; i++ {
		rootRowCollector.FMSketches = append(rootRowCollector.FMSketches, statistics.NewFMSketch(maxSketchSize))
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	statsConcurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return 0, nil, nil, nil, nil, err
	}
	mergeResultCh := make(chan *samplingMergeResult, statsConcurrency)
	mergeTaskCh := make(chan []byte, statsConcurrency)
	e.samplingMergeWg = &sync.WaitGroup{}
	e.samplingMergeWg.Add(statsConcurrency)
	for i := 0; i < statsConcurrency; i++ {
		go e.subMergeWorker(mergeResultCh, mergeTaskCh, l, i == 0)
	}
	for {
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return 0, nil, nil, nil, nil, err1
		}
		if data == nil {
			break
		}
		mergeTaskCh <- data
	}
	close(mergeTaskCh)
	mergeWorkerPanicCnt := 0
	for mergeWorkerPanicCnt < statsConcurrency {
		mergeResult, ok := <-mergeResultCh
		if !ok {
			break
		}
		if mergeResult.err != nil {
			err = mergeResult.err
			if mergeResult.err == errAnalyzeWorkerPanic {
				mergeWorkerPanicCnt++
			}
			continue
		}
		rootRowCollector.MergeCollector(mergeResult.collector)
	}
	if err != nil {
		return 0, nil, nil, nil, nil, err
	}

	// handling virtual columns
	virtualColIdx := buildVirtualColumnIndex(e.schemaForVirtualColEval, e.colsInfo)
	if len(virtualColIdx) > 0 {
		fieldTps := make([]*types.FieldType, 0, len(virtualColIdx))
		for _, colOffset := range virtualColIdx {
			fieldTps = append(fieldTps, e.schemaForVirtualColEval.Columns[colOffset].RetType)
		}
		err = e.decodeSampleDataWithVirtualColumn(rootRowCollector, fieldTps, virtualColIdx, e.schemaForVirtualColEval)
		if err != nil {
			return 0, nil, nil, nil, nil, err
		}
	} else {
		// If there's no virtual column or we meet error during eval virtual column, we fallback to normal decode otherwise.
		for _, sample := range rootRowCollector.Samples {
			for i := range sample.Columns {
				sample.Columns[i], err = tablecodec.DecodeColumnValue(sample.Columns[i].GetBytes(), &e.colsInfo[i].FieldType, sc.TimeZone)
				if err != nil {
					return 0, nil, nil, nil, nil, err
				}
			}
		}
	}

	for _, sample := range rootRowCollector.Samples {
		for i := range sample.Columns {
			if sample.Columns[i].Kind() == types.KindBytes {
				sample.Columns[i].SetBytes(sample.Columns[i].GetBytes())
			}
		}
		// Calculate handle from the row data for each row. It will be used to sort the samples.
		sample.Handle, err = e.handleCols.BuildHandleByDatums(sample.Columns)
		if err != nil {
			return 0, nil, nil, nil, nil, err
		}
	}

	colLen := len(e.colsInfo)

	// The order of the samples are broken when merging samples from sub-collectors.
	// So now we need to sort the samples according to the handle in order to calculate correlation.
	sort.Slice(rootRowCollector.Samples, func(i, j int) bool {
		return rootRowCollector.Samples[i].Handle.Compare(rootRowCollector.Samples[j].Handle) < 0
	})

	totalLen := len(e.colsInfo) + len(e.indexes)
	hists = make([]*statistics.Histogram, totalLen)
	topns = make([]*statistics.TopN, totalLen)
	fmSketches = make([]*statistics.FMSketch, 0, totalLen)
	buildResultChan := make(chan error, totalLen)
	buildTaskChan := make(chan *samplingBuildTask, totalLen)
	e.samplingBuilderWg = &sync.WaitGroup{}
	e.samplingBuilderWg.Add(statsConcurrency)
	sampleCollectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := 0; i < statsConcurrency; i++ {
		go e.subBuildWorker(buildResultChan, buildTaskChan, hists, topns, sampleCollectors, i == 0)
	}

	for i, col := range e.colsInfo {
		buildTaskChan <- &samplingBuildTask{
			id:               col.ID,
			rootRowCollector: rootRowCollector,
			tp:               &col.FieldType,
			isColumn:         true,
			slicePos:         i,
		}
		fmSketches = append(fmSketches, rootRowCollector.FMSketches[i])
	}

	indexPushedDownResult := <-idxNDVPushDownCh
	if indexPushedDownResult.err != nil {
		return 0, nil, nil, nil, nil, indexPushedDownResult.err
	}
	for _, offset := range indexesWithVirtualColOffsets {
		ret := indexPushedDownResult.results[e.indexes[offset].ID]
		rootRowCollector.NullCount[colLen+offset] = ret.Count
		rootRowCollector.FMSketches[colLen+offset] = ret.Fms[0]
	}

	// build index stats
	for i, idx := range e.indexes {
		buildTaskChan <- &samplingBuildTask{
			id:               idx.ID,
			rootRowCollector: rootRowCollector,
			tp:               types.NewFieldType(mysql.TypeBlob),
			isColumn:         false,
			slicePos:         colLen + i,
		}
		fmSketches = append(fmSketches, rootRowCollector.FMSketches[colLen+i])
	}
	close(buildTaskChan)
	panicCnt := 0
	for panicCnt < statsConcurrency {
		err1, ok := <-buildResultChan
		if !ok {
			break
		}
		if err1 != nil {
			err = err1
			if err1 == errAnalyzeWorkerPanic {
				panicCnt++
			}
			continue
		}
	}
	if err != nil {
		return 0, nil, nil, nil, nil, err
	}
	count = rootRowCollector.Count
	if needExtStats {
		statsHandle := domain.GetDomain(e.ctx).StatsHandle()
		extStats, err = statsHandle.BuildExtendedStats(e.TableID.GetStatisticsID(), e.colsInfo, sampleCollectors)
		if err != nil {
			return 0, nil, nil, nil, nil, err
		}
	}
	return
}

type analyzeIndexNDVTotalResult struct {
	results map[int64]analyzeResult
	err     error
}

// handleNDVForSpecialIndexes deals with the logic to analyze the index containing the virtual column when the mode is full sampling.
func (e *AnalyzeColumnsExec) handleNDVForSpecialIndexes(indexInfos []*model.IndexInfo, totalResultCh chan analyzeIndexNDVTotalResult) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze ndv for special index panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			totalResultCh <- analyzeIndexNDVTotalResult{
				err: errAnalyzeWorkerPanic,
			}
		}
	}()
	tasks := e.buildSubIndexJobForSpecialIndex(indexInfos)
	statsConcurrncy, err := getBuildStatsConcurrency(e.ctx)
	taskCh := make(chan *analyzeTask, len(tasks))
	for _, task := range tasks {
		statistics.AddNewAnalyzeJob(task.job)
	}
	resultCh := make(chan analyzeResult, len(tasks))
	e.subIndexWorkerWg = &sync.WaitGroup{}
	e.subIndexWorkerWg.Add(statsConcurrncy)
	for i := 0; i < statsConcurrncy; i++ {
		go e.subIndexWorkerForNDV(taskCh, resultCh, i == 0)
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	panicCnt := 0
	totalResult := analyzeIndexNDVTotalResult{
		results: make(map[int64]analyzeResult, len(indexInfos)),
	}
	for panicCnt < statsConcurrncy {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			result.job.Finish(true)
			err = result.Err
			if err == errAnalyzeWorkerPanic {
				panicCnt++
			}
			continue
		}
		result.job.Finish(false)
		statistics.MoveToHistory(result.job)
		totalResult.results[result.Hist[0].ID] = result
	}
	if err != nil {
		totalResult.err = err
	}
	totalResultCh <- totalResult
}

// subIndexWorker receive the task for each index and return the result for them.
func (e *AnalyzeColumnsExec) subIndexWorkerForNDV(taskCh chan *analyzeTask, resultCh chan analyzeResult, isFirstToCloseCh bool) {
	var task *analyzeTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- analyzeResult{
				Err: errAnalyzeWorkerPanic,
				job: task.job,
			}
		}
		e.subIndexWorkerWg.Done()
		if isFirstToCloseCh {
			e.subIndexWorkerWg.Wait()
			close(resultCh)
		}
	}()
	for {
		var ok bool
		task, ok = <-taskCh
		if !ok {
			break
		}
		task.job.Start()
		if task.taskType != idxTask {
			resultCh <- analyzeResult{
				Err: errors.Errorf("incorrect analyze type"),
				job: task.job,
			}
			continue
		}
		task.idxExec.job = task.job
		resultCh <- analyzeIndexNDVPushDown(task.idxExec)
	}
}

// buildSubIndexJobForSpecialIndex builds sub index pushed down task to calculate the NDV information for indexes containing virtual column.
// This is because we cannot push the calculation of the virtual column down to the tikv side.
func (e *AnalyzeColumnsExec) buildSubIndexJobForSpecialIndex(indexInfos []*model.IndexInfo) []*analyzeTask {
	_, offset := timeutil.Zone(e.ctx.GetSessionVars().Location())
	tasks := make([]*analyzeTask, 0, len(indexInfos))
	sc := e.ctx.GetSessionVars().StmtCtx
	for _, indexInfo := range indexInfos {
		idxExec := &AnalyzeIndexExec{
			ctx:            e.ctx,
			tableID:        e.TableID,
			isCommonHandle: e.tableInfo.IsCommonHandle,
			idxInfo:        indexInfo,
			concurrency:    e.ctx.GetSessionVars().IndexSerialScanConcurrency(),
			analyzePB: &tipb.AnalyzeReq{
				Tp:             tipb.AnalyzeType_TypeIndex,
				Flags:          sc.PushDownFlags(),
				TimeZoneOffset: offset,
			},
		}
		idxExec.opts = make(map[ast.AnalyzeOptionType]uint64, len(ast.AnalyzeOptionString))
		idxExec.opts[ast.AnalyzeOptNumTopN] = 0
		idxExec.opts[ast.AnalyzeOptCMSketchDepth] = 0
		idxExec.opts[ast.AnalyzeOptCMSketchWidth] = 0
		idxExec.opts[ast.AnalyzeOptNumSamples] = 0
		idxExec.opts[ast.AnalyzeOptNumBuckets] = 1
		statsVersion := new(int32)
		*statsVersion = statistics.Version1
		// No Top-N
		topnSize := int32(0)
		idxExec.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
			// One bucket to store the null for null histogram.
			BucketSize: 1,
			NumColumns: int32(len(indexInfo.Columns)),
			TopNSize:   &topnSize,
			Version:    statsVersion,
			SketchSize: maxSketchSize,
		}
		if idxExec.isCommonHandle && indexInfo.Primary {
			idxExec.analyzePB.Tp = tipb.AnalyzeType_TypeCommonHandle
		}
		// No CM-Sketch.
		depth := int32(0)
		width := int32(0)
		idxExec.analyzePB.IdxReq.CmsketchDepth = &depth
		idxExec.analyzePB.IdxReq.CmsketchWidth = &width
		autoAnalyze := ""
		if e.ctx.GetSessionVars().InRestrictedSQL {
			autoAnalyze = "auto "
		}
		job := &statistics.AnalyzeJob{DBName: e.job.DBName, TableName: e.job.TableName, PartitionName: e.job.PartitionName, JobInfo: autoAnalyze + "analyze ndv for index " + indexInfo.Name.O}
		tasks = append(tasks, &analyzeTask{
			taskType: idxTask,
			idxExec:  idxExec,
			job:      job,
		})
	}
	return tasks
}

type samplingMergeResult struct {
	collector *statistics.RowSampleCollector
	err       error
}

func (e *AnalyzeColumnsExec) subMergeWorker(resultCh chan<- *samplingMergeResult, taskCh <-chan []byte, l int, isClosedChanThread bool) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- &samplingMergeResult{err: errAnalyzeWorkerPanic}
		}
		// Consume the remaining things.
		for {
			_, ok := <-taskCh
			if !ok {
				break
			}
		}
		e.samplingMergeWg.Done()
		if isClosedChanThread {
			e.samplingMergeWg.Wait()
			close(resultCh)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingMergeWorkerPanic", func() {
		panic("failpoint triggered")
	})
	retCollector := &statistics.RowSampleCollector{
		NullCount:     make([]int64, l),
		FMSketches:    make([]*statistics.FMSketch, 0, l),
		TotalSizes:    make([]int64, l),
		Samples:       make(statistics.WeightedRowSampleHeap, 0, e.analyzePB.ColReq.SampleSize),
		MaxSampleSize: int(e.analyzePB.ColReq.SampleSize),
	}
	for i := 0; i < l; i++ {
		retCollector.FMSketches = append(retCollector.FMSketches, statistics.NewFMSketch(maxSketchSize))
	}
	for {
		data, ok := <-taskCh
		if !ok {
			break
		}
		colResp := &tipb.AnalyzeColumnsResp{}
		err := colResp.Unmarshal(data)
		if err != nil {
			resultCh <- &samplingMergeResult{err: err}
			return
		}
		subCollector := &statistics.RowSampleCollector{
			MaxSampleSize: int(e.analyzePB.ColReq.SampleSize),
		}
		subCollector.FromProto(colResp.RowCollector)
		e.job.Update(subCollector.Count)
		retCollector.MergeCollector(subCollector)
	}
	resultCh <- &samplingMergeResult{collector: retCollector}
}

type samplingBuildTask struct {
	id               int64
	rootRowCollector *statistics.RowSampleCollector
	tp               *types.FieldType
	isColumn         bool
	slicePos         int
}

func (e *AnalyzeColumnsExec) subBuildWorker(resultCh chan error, taskCh chan *samplingBuildTask, hists []*statistics.Histogram, topns []*statistics.TopN, collectors []*statistics.SampleCollector, isClosedChanThread bool) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- errAnalyzeWorkerPanic
		}
		e.samplingBuilderWg.Done()
		if isClosedChanThread {
			e.samplingBuilderWg.Wait()
			close(resultCh)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingBuildWorkerPanic", func() {
		panic("failpoint triggered")
	})
	colLen := len(e.colsInfo)
workLoop:
	for {
		task, ok := <-taskCh
		if !ok {
			break
		}
		var collector *statistics.SampleCollector
		if task.isColumn {
			if e.colsInfo[task.slicePos].IsGenerated() && !e.colsInfo[task.slicePos].GeneratedStored {
				hists[task.slicePos] = nil
				topns[task.slicePos] = nil
				continue
			}
			sampleItems := make([]*statistics.SampleItem, 0, task.rootRowCollector.MaxSampleSize)
			for j, row := range task.rootRowCollector.Samples {
				if row.Columns[task.slicePos].IsNull() {
					continue
				}
				sampleItems = append(sampleItems, &statistics.SampleItem{
					Value:   row.Columns[task.slicePos],
					Ordinal: j,
				})
			}
			collector = &statistics.SampleCollector{
				Samples:   sampleItems,
				NullCount: task.rootRowCollector.NullCount[task.slicePos],
				Count:     task.rootRowCollector.Count - task.rootRowCollector.NullCount[task.slicePos],
				FMSketch:  task.rootRowCollector.FMSketches[task.slicePos],
				TotalSize: task.rootRowCollector.TotalSizes[task.slicePos],
			}
		} else {
			var tmpDatum types.Datum
			var err error
			idx := e.indexes[task.slicePos-colLen]
			sampleItems := make([]*statistics.SampleItem, 0, task.rootRowCollector.MaxSampleSize)
			for _, row := range task.rootRowCollector.Samples {
				if len(idx.Columns) == 1 && row.Columns[idx.Columns[0].Offset].IsNull() {
					continue
				}

				b := make([]byte, 0, 8)
				for _, col := range idx.Columns {
					if col.Length != types.UnspecifiedLength {
						row.Columns[col.Offset].Copy(&tmpDatum)
						ranger.CutDatumByPrefixLen(&tmpDatum, col.Length, &e.colsInfo[col.Offset].FieldType)
						b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, b, tmpDatum)
						if err != nil {
							resultCh <- err
							continue workLoop
						}
						continue
					}
					b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, b, row.Columns[col.Offset])
					if err != nil {
						resultCh <- err
						continue workLoop
					}
				}
				sampleItems = append(sampleItems, &statistics.SampleItem{
					Value: types.NewBytesDatum(b),
				})
			}
			collector = &statistics.SampleCollector{
				Samples:   sampleItems,
				NullCount: task.rootRowCollector.NullCount[task.slicePos],
				Count:     task.rootRowCollector.Count - task.rootRowCollector.NullCount[task.slicePos],
				FMSketch:  task.rootRowCollector.FMSketches[task.slicePos],
				TotalSize: task.rootRowCollector.TotalSizes[task.slicePos],
			}
		}
		if task.isColumn {
			collectors[task.slicePos] = collector
		}
		hist, topn, err := statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), task.id, collector, task.tp, task.isColumn)
		if err != nil {
			resultCh <- err
			continue
		}
		hists[task.slicePos] = hist
		topns[task.slicePos] = topn
		resultCh <- nil
	}
}

func (e *AnalyzeColumnsExec) buildStats(ranges []*ranger.Range, needExtStats bool) (hists []*statistics.Histogram, cms []*statistics.CMSketch, topNs []*statistics.TopN, fms []*statistics.FMSketch, extStats *statistics.ExtendedStatsColl, err error) {
	if err = e.open(ranges); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			hists = nil
			cms = nil
			extStats = nil
			err = err1
		}
	}()
	var handleHist *statistics.Histogram
	var handleCms *statistics.CMSketch
	var handleFms *statistics.FMSketch
	var handleTopn *statistics.TopN
	statsVer := statistics.Version1
	if e.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
		handleHist = &statistics.Histogram{}
		handleCms = statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]))
		handleTopn = statistics.NewTopN(int(e.opts[ast.AnalyzeOptNumTopN]))
		handleFms = statistics.NewFMSketch(maxSketchSize)
		if e.analyzePB.IdxReq.Version != nil {
			statsVer = int(*e.analyzePB.IdxReq.Version)
		}
	}
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			CMSketch:      statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth])),
		}
	}
	for {
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return nil, nil, nil, nil, nil, err1
		}
		if data == nil {
			break
		}
		sc := e.ctx.GetSessionVars().StmtCtx
		var colResp *tipb.AnalyzeColumnsResp
		if e.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
			resp := &tipb.AnalyzeMixedResp{}
			err = resp.Unmarshal(data)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			colResp = resp.ColumnsResp
			handleHist, handleCms, handleFms, handleTopn, err = updateIndexResult(sc, resp.IndexResp, nil, handleHist,
				handleCms, handleFms, handleTopn, e.commonHandle, int(e.opts[ast.AnalyzeOptNumBuckets]),
				int(e.opts[ast.AnalyzeOptNumTopN]), statsVer)

			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
		} else {
			colResp = &tipb.AnalyzeColumnsResp{}
			err = colResp.Unmarshal(data)
		}
		rowCount := int64(0)
		if hasPkHist(e.handleCols) {
			respHist := statistics.HistogramFromProto(colResp.PkHist)
			rowCount = int64(respHist.TotalRowCount())
			pkHist, err = statistics.MergeHistograms(sc, pkHist, respHist, int(e.opts[ast.AnalyzeOptNumBuckets]), statistics.Version1)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
		}
		for i, rc := range colResp.Collectors {
			respSample := statistics.SampleCollectorFromProto(rc)
			rowCount = respSample.Count + respSample.NullCount
			collectors[i].MergeSampleCollector(sc, respSample)
		}
		e.job.Update(rowCount)
	}
	timeZone := e.ctx.GetSessionVars().Location()
	if hasPkHist(e.handleCols) {
		pkInfo := e.handleCols.GetCol(0)
		pkHist.ID = pkInfo.ID
		err = pkHist.DecodeTo(pkInfo.RetType, timeZone)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		hists = append(hists, pkHist)
		cms = append(cms, nil)
		topNs = append(topNs, nil)
		fms = append(fms, nil)
	}
	for i, col := range e.colsInfo {
		if e.StatsVersion < 2 {
			// In analyze version 2, we don't collect TopN this way. We will collect TopN from samples in `BuildColumnHistAndTopN()` below.
			err := collectors[i].ExtractTopN(uint32(e.opts[ast.AnalyzeOptNumTopN]), e.ctx.GetSessionVars().StmtCtx, &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			topNs = append(topNs, collectors[i].TopN)
		}
		for j, s := range collectors[i].Samples {
			collectors[i].Samples[j].Ordinal = j
			collectors[i].Samples[j].Value, err = tablecodec.DecodeColumnValue(s.Value.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			// When collation is enabled, we store the Key representation of the sampling data. So we set it to kind `Bytes` here
			// to avoid to convert it to its Key representation once more.
			if collectors[i].Samples[j].Value.Kind() == types.KindString {
				collectors[i].Samples[j].Value.SetBytes(collectors[i].Samples[j].Value.GetBytes())
			}
		}
		var hg *statistics.Histogram
		var err error
		var topn *statistics.TopN
		if e.StatsVersion < 2 {
			hg, err = statistics.BuildColumn(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), col.ID, collectors[i], &col.FieldType)
		} else {
			hg, topn, err = statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), col.ID, collectors[i], &col.FieldType, true)
			topNs = append(topNs, topn)
		}
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		hists = append(hists, hg)
		collectors[i].CMSketch.CalcDefaultValForAnalyze(uint64(hg.NDV))
		cms = append(cms, collectors[i].CMSketch)
		fms = append(fms, collectors[i].FMSketch)
	}
	if needExtStats {
		statsHandle := domain.GetDomain(e.ctx).StatsHandle()
		extStats, err = statsHandle.BuildExtendedStats(e.TableID.GetStatisticsID(), e.colsInfo, collectors)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}
	if handleHist != nil {
		handleHist.ID = e.commonHandle.ID
		if handleTopn != nil && handleTopn.TotalCount() > 0 {
			handleHist.RemoveVals(handleTopn.TopN)
		}
		if handleCms != nil {
			handleCms.CalcDefaultValForAnalyze(uint64(handleHist.NDV))
		}
		hists = append([]*statistics.Histogram{handleHist}, hists...)
		cms = append([]*statistics.CMSketch{handleCms}, cms...)
		fms = append([]*statistics.FMSketch{handleFms}, fms...)
		topNs = append([]*statistics.TopN{handleTopn}, topNs...)
	}
	return hists, cms, topNs, fms, extStats, nil
}

func hasPkHist(handleCols core.HandleCols) bool {
	return handleCols != nil && handleCols.IsInt()
}

func pkColsCount(handleCols core.HandleCols) int {
	if handleCols == nil {
		return 0
	}
	return handleCols.NumCols()
}

var (
	fastAnalyzeHistogramSample        = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "sample")
	fastAnalyzeHistogramAccessRegions = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "access_regions")
	fastAnalyzeHistogramScanKeys      = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "scan_keys")
)

func analyzeFastExec(exec *AnalyzeFastExec) []analyzeResult {
	hists, cms, topNs, fms, err := exec.buildStats()
	if err != nil {
		return []analyzeResult{{Err: err, job: exec.job}}
	}
	var results []analyzeResult
	pkColCount := pkColsCount(exec.handleCols)
	if len(exec.idxsInfo) > 0 {
		for i := pkColCount + len(exec.colsInfo); i < len(hists); i++ {
			idxResult := analyzeResult{
				TableID:  exec.tableID,
				Hist:     []*statistics.Histogram{hists[i]},
				Cms:      []*statistics.CMSketch{cms[i]},
				TopNs:    []*statistics.TopN{topNs[i]},
				Fms:      []*statistics.FMSketch{nil},
				IsIndex:  1,
				Count:    hists[i].NullCount,
				job:      exec.job,
				StatsVer: statistics.Version1,
			}
			if hists[i].Len() > 0 {
				idxResult.Count += hists[i].Buckets[hists[i].Len()-1].Count
			}
			if exec.rowCount != 0 {
				idxResult.Count = exec.rowCount
			}
			results = append(results, idxResult)
		}
	}
	hist := hists[0]
	colResult := analyzeResult{
		TableID:  exec.tableID,
		Hist:     hists[:pkColCount+len(exec.colsInfo)],
		Cms:      cms[:pkColCount+len(exec.colsInfo)],
		TopNs:    topNs[:pkColCount+len(exec.colsInfo)],
		Fms:      fms[:pkColCount+len(exec.colsInfo)],
		Count:    hist.NullCount,
		job:      exec.job,
		StatsVer: statistics.Version1,
	}
	if hist.Len() > 0 {
		colResult.Count += hist.Buckets[hist.Len()-1].Count
	}
	if exec.rowCount != 0 {
		colResult.Count = exec.rowCount
	}
	results = append(results, colResult)
	return results
}

// AnalyzeFastExec represents Fast Analyze executor.
type AnalyzeFastExec struct {
	ctx         sessionctx.Context
	tableID     core.AnalyzeTableID
	handleCols  core.HandleCols
	colsInfo    []*model.ColumnInfo
	idxsInfo    []*model.IndexInfo
	concurrency int
	opts        map[ast.AnalyzeOptionType]uint64
	tblInfo     *model.TableInfo
	cache       *tikv.RegionCache
	wg          *sync.WaitGroup
	rowCount    int64
	sampCursor  int32
	sampTasks   []*tikv.KeyLocation
	scanTasks   []*tikv.KeyLocation
	collectors  []*statistics.SampleCollector
	randSeed    int64
	job         *statistics.AnalyzeJob
	estSampStep uint32
}

func (e *AnalyzeFastExec) calculateEstimateSampleStep() (err error) {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	var stmt ast.StmtNode
	stmt, err = exec.ParseWithParams(context.TODO(), "select flag from mysql.stats_histograms where table_id = %?", e.tableID.GetStatisticsID())
	if err != nil {
		return
	}
	var rows []chunk.Row
	rows, _, err = exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return
	}
	var historyRowCount uint64
	hasBeenAnalyzed := len(rows) != 0 && rows[0].GetInt64(0) == statistics.AnalyzeFlag
	if hasBeenAnalyzed {
		historyRowCount = uint64(domain.GetDomain(e.ctx).StatsHandle().GetPartitionStats(e.tblInfo, e.tableID.GetStatisticsID()).Count)
	} else {
		dbInfo, ok := domain.GetDomain(e.ctx).InfoSchema().SchemaByTable(e.tblInfo)
		if !ok {
			err = errors.Errorf("database not found for table '%s'", e.tblInfo.Name)
			return
		}
		var rollbackFn func() error
		rollbackFn, err = e.activateTxnForRowCount()
		if err != nil {
			return
		}
		defer func() {
			if rollbackFn != nil {
				err = rollbackFn()
			}
		}()
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "select count(*) from %n.%n", dbInfo.Name.L, e.tblInfo.Name.L)

		if e.tblInfo.ID != e.tableID.GetStatisticsID() {
			for _, definition := range e.tblInfo.Partition.Definitions {
				if definition.ID == e.tableID.GetStatisticsID() {
					sqlexec.MustFormatSQL(sql, " partition(%n)", definition.Name.L)
					break
				}
			}
		}
		var rs sqlexec.RecordSet
		rs, err = e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql.String())
		if err != nil {
			return
		}
		if rs == nil {
			err = errors.Trace(errors.Errorf("empty record set"))
			return
		}
		defer terror.Call(rs.Close)
		chk := rs.NewChunk()
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return
		}
		e.rowCount = chk.GetRow(0).GetInt64(0)
		historyRowCount = uint64(e.rowCount)
	}
	totalSampSize := e.opts[ast.AnalyzeOptNumSamples]
	e.estSampStep = uint32(historyRowCount / totalSampSize)
	return
}

func (e *AnalyzeFastExec) activateTxnForRowCount() (rollbackFn func() error, err error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		if kv.ErrInvalidTxn.Equal(err) {
			_, err := e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "begin")
			if err != nil {
				return nil, errors.Trace(err)
			}
			rollbackFn = func() error {
				_, err := e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "rollback")
				return err
			}
		} else {
			return nil, errors.Trace(err)
		}
	}
	txn.SetOption(kv.Priority, kv.PriorityLow)
	txn.SetOption(kv.IsolationLevel, kv.RC)
	txn.SetOption(kv.NotFillCache, true)
	return rollbackFn, nil
}

// buildSampTask build sample tasks.
func (e *AnalyzeFastExec) buildSampTask() (err error) {
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	accessRegionsCounter := 0
	pid := e.tableID.GetStatisticsID()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(pid)
	targetKey := startKey
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return err
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		accessRegionsCounter++

		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampTasks = append(e.sampTasks, loc)
			continue
		}

		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			loc.StartKey = startKey
		}
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			loc.EndKey = endKey
			break
		}
	}
	fastAnalyzeHistogramAccessRegions.Observe(float64(accessRegionsCounter))

	return nil
}

func (e *AnalyzeFastExec) decodeValues(handle kv.Handle, sValue []byte, wantCols map[int64]*types.FieldType) (values map[int64]types.Datum, err error) {
	loc := e.ctx.GetSessionVars().Location()
	values, err = tablecodec.DecodeRowToDatumMap(sValue, wantCols, loc)
	if err != nil || e.handleCols == nil {
		return values, err
	}
	wantCols = make(map[int64]*types.FieldType, e.handleCols.NumCols())
	handleColIDs := make([]int64, e.handleCols.NumCols())
	for i := 0; i < e.handleCols.NumCols(); i++ {
		c := e.handleCols.GetCol(i)
		handleColIDs[i] = c.ID
		wantCols[c.ID] = c.RetType
	}
	return tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, wantCols, loc, values)
}

func (e *AnalyzeFastExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	val, ok := values[colInfo.ID]
	if !ok {
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	return val, nil
}

func (e *AnalyzeFastExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32) (err error) {
	var handle kv.Handle
	handle, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		return err
	}

	// Decode cols for analyze table
	wantCols := make(map[int64]*types.FieldType, len(e.colsInfo))
	for _, col := range e.colsInfo {
		wantCols[col.ID] = &col.FieldType
	}

	// Pre-build index->cols relationship and refill wantCols if not exists(analyze index)
	index2Cols := make([][]*model.ColumnInfo, len(e.idxsInfo))
	for i, idxInfo := range e.idxsInfo {
		for _, idxCol := range idxInfo.Columns {
			colInfo := e.tblInfo.Columns[idxCol.Offset]
			index2Cols[i] = append(index2Cols[i], colInfo)
			wantCols[colInfo.ID] = &colInfo.FieldType
		}
	}

	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(handle, sValue, wantCols)
	if err != nil {
		return err
	}
	// Update the primary key collector.
	pkColsCount := pkColsCount(e.handleCols)
	for i := 0; i < pkColsCount; i++ {
		col := e.handleCols.GetCol(i)
		v, ok := values[col.ID]
		if !ok {
			return errors.Trace(errors.Errorf("Primary key column not found"))
		}
		if e.collectors[i].Samples[samplePos] == nil {
			e.collectors[i].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[i].Samples[samplePos].Handle = handle
		e.collectors[i].Samples[samplePos].Value = v
	}

	// Update the columns' collectors.
	for j, colInfo := range e.colsInfo {
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			return err
		}
		if e.collectors[pkColsCount+j].Samples[samplePos] == nil {
			e.collectors[pkColsCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[pkColsCount+j].Samples[samplePos].Handle = handle
		e.collectors[pkColsCount+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		cols := index2Cols[j]
		for _, colInfo := range cols {
			v, err := e.getValueByInfo(colInfo, values)
			if err != nil {
				return err
			}
			idxVals = append(idxVals, v)
		}
		var keyBytes []byte
		keyBytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, keyBytes, idxVals...)
		if err != nil {
			return err
		}
		if e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos] == nil {
			e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos].Handle = handle
		e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos].Value = types.NewBytesDatum(keyBytes)
	}
	return nil
}

func (e *AnalyzeFastExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		exceedNeededSampleCounts := uint64(samplePos) >= e.opts[ast.AnalyzeOptNumSamples]
		if exceedNeededSampleCounts {
			atomic.StoreInt32(&e.sampCursor, int32(e.opts[ast.AnalyzeOptNumSamples]))
			break
		}
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeFastExec) handleScanIter(iter kv.Iterator) (scanKeysSize int, err error) {
	rander := rand.New(rand.NewSource(e.randSeed))
	sampleSize := int64(e.opts[ast.AnalyzeOptNumSamples])
	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		scanKeysSize++
		randNum := rander.Int63n(int64(e.sampCursor) + int64(scanKeysSize))
		if randNum > sampleSize && e.sampCursor == int32(sampleSize) {
			continue
		}

		p := rander.Int31n(int32(sampleSize))
		if e.sampCursor < int32(sampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p)
		if err != nil {
			return
		}
	}
	return
}

func (e *AnalyzeFastExec) handleScanTasks(bo *tikv.Backoffer) (keysSize int, err error) {
	snapshot := e.ctx.GetStore().GetSnapshot(kv.MaxVersion)
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	setResourceGroupTagForTxn(e.ctx.GetSessionVars().StmtCtx, snapshot)
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(kv.Key(t.StartKey), kv.Key(t.EndKey))
		if err != nil {
			return keysSize, err
		}
		size, err := e.handleScanIter(iter)
		keysSize += size
		if err != nil {
			return keysSize, err
		}
	}
	return keysSize, nil
}

func (e *AnalyzeFastExec) handleSampTasks(workID int, step uint32, err *error) {
	defer e.wg.Done()
	snapshot := e.ctx.GetStore().GetSnapshot(kv.MaxVersion)
	snapshot.SetOption(kv.NotFillCache, true)
	snapshot.SetOption(kv.IsolationLevel, kv.RC)
	snapshot.SetOption(kv.Priority, kv.PriorityLow)
	setResourceGroupTagForTxn(e.ctx.GetSessionVars().StmtCtx, snapshot)
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}

	rander := rand.New(rand.NewSource(e.randSeed))
	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		// randomize the estimate step in range [step - 2 * sqrt(step), step]
		if step > 4 { // 2*sqrt(x) < x
			lower, upper := step-uint32(2*math.Sqrt(float64(step))), step
			step = uint32(rander.Intn(int(upper-lower))) + lower
		}
		snapshot.SetOption(kv.SampleStep, step)
		kvMap := make(map[string][]byte)
		var iter kv.Iterator
		iter, *err = snapshot.Iter(kv.Key(task.StartKey), kv.Key(task.EndKey))
		if *err != nil {
			return
		}
		for iter.Valid() {
			kvMap[string(iter.Key())] = iter.Value()
			*err = iter.Next()
			if *err != nil {
				return
			}
		}
		fastAnalyzeHistogramSample.Observe(float64(len(kvMap)))

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeFastExec) buildColumnStats(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, *statistics.TopN, *statistics.FMSketch, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	data := make([][]byte, 0, len(collector.Samples))
	fmSketch := statistics.NewFMSketch(maxSketchSize)
	notNullSamples := make([]*statistics.SampleItem, 0, len(collector.Samples))
	for i, sample := range collector.Samples {
		sample.Ordinal = i
		if sample.Value.IsNull() {
			collector.NullCount++
			continue
		}
		notNullSamples = append(notNullSamples, sample)
		err := fmSketch.InsertValue(sc, sample.Value)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		valBytes, err := tablecodec.EncodeValue(sc, nil, sample.Value)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		data = append(data, valBytes)
	}
	// Build CMSketch.
	cmSketch, topN, ndv, scaleRatio := statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data, uint32(e.opts[ast.AnalyzeOptNumTopN]), uint64(rowCount))
	// Build Histogram.
	collector.Samples = notNullSamples
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), ID, collector, tp, rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, topN, fmSketch, err
}

func (e *AnalyzeFastExec) buildIndexStats(idxInfo *model.IndexInfo, collector *statistics.SampleCollector, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, *statistics.TopN, error) {
	data := make([][][]byte, len(idxInfo.Columns))
	for _, sample := range collector.Samples {
		var preLen int
		remained := sample.Value.GetBytes()
		// We need to insert each prefix values into CM Sketch.
		for i := 0; i < len(idxInfo.Columns); i++ {
			var err error
			var value []byte
			value, remained, err = codec.CutOne(remained)
			if err != nil {
				return nil, nil, nil, err
			}
			preLen += len(value)
			data[i] = append(data[i], sample.Value.GetBytes()[:preLen])
		}
	}
	numTop := uint32(e.opts[ast.AnalyzeOptNumTopN])
	cmSketch, topN, ndv, scaleRatio := statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[0], numTop, uint64(rowCount))
	// Build CM Sketch for each prefix and merge them into one.
	for i := 1; i < len(idxInfo.Columns); i++ {
		var curCMSketch *statistics.CMSketch
		var curTopN *statistics.TopN
		// `ndv` should be the ndv of full index, so just rewrite it here.
		curCMSketch, curTopN, ndv, scaleRatio = statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[i], numTop, uint64(rowCount))
		err := cmSketch.MergeCMSketch(curCMSketch)
		if err != nil {
			return nil, nil, nil, err
		}
		statistics.MergeTopNAndUpdateCMSketch(topN, curTopN, cmSketch, numTop)
	}
	// Build Histogram.
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), idxInfo.ID, collector, types.NewFieldType(mysql.TypeBlob), rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, topN, err
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, []*statistics.TopN, []*statistics.FMSketch, error) {
	errs := make([]error, e.concurrency)
	pkColCount := pkColsCount(e.handleCols)
	// collect column samples and primary key samples and index samples.
	length := len(e.colsInfo) + pkColCount + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			Samples:       make([]*statistics.SampleItem, e.opts[ast.AnalyzeOptNumSamples]),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(i, e.estSampStep, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	scanKeysSize, err := e.handleScanTasks(bo)
	fastAnalyzeHistogramScanKeys.Observe(float64(scanKeysSize))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stats := domain.GetDomain(e.ctx).StatsHandle()
	var rowCount int64 = 0
	if stats.Lease() > 0 {
		if t := stats.GetPartitionStats(e.tblInfo, e.tableID.GetStatisticsID()); !t.Pseudo {
			rowCount = t.Count
		}
	}
	hists, cms, topNs, fms := make([]*statistics.Histogram, length), make([]*statistics.CMSketch, length), make([]*statistics.TopN, length), make([]*statistics.FMSketch, length)
	for i := 0; i < length; i++ {
		// Build collector properties.
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool {
			return collector.Samples[i].Handle.Compare(collector.Samples[j].Handle) < 0
		})
		collector.CalcTotalSize()
		// Adjust the row count in case the count of `tblStats` is not accurate and too small.
		rowCount = mathutil.MaxInt64(rowCount, int64(len(collector.Samples)))
		// Scale the total column size.
		if len(collector.Samples) > 0 {
			collector.TotalSize *= rowCount / int64(len(collector.Samples))
		}
		if i < pkColCount {
			pkCol := e.handleCols.GetCol(i)
			hists[i], cms[i], topNs[i], fms[i], err = e.buildColumnStats(pkCol.ID, e.collectors[i], pkCol.RetType, rowCount)
		} else if i < pkColCount+len(e.colsInfo) {
			hists[i], cms[i], topNs[i], fms[i], err = e.buildColumnStats(e.colsInfo[i-pkColCount].ID, e.collectors[i], &e.colsInfo[i-pkColCount].FieldType, rowCount)
		} else {
			hists[i], cms[i], topNs[i], err = e.buildIndexStats(e.idxsInfo[i-pkColCount-len(e.colsInfo)], e.collectors[i], rowCount)
		}
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	return hists, cms, topNs, fms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, topNs []*statistics.TopN, fms []*statistics.FMSketch, err error) {
	// To set rand seed, it's for unit test.
	// To ensure that random sequences are different in non-test environments, RandSeed must be set time.Now().
	if RandSeed == 1 {
		atomic.StoreInt64(&e.randSeed, time.Now().UnixNano())
	} else {
		atomic.StoreInt64(&e.randSeed, RandSeed)
	}

	err = e.buildSampTask()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return e.runTasks()
}

// AnalyzeTestFastExec is for fast sample in unit test.
type AnalyzeTestFastExec struct {
	AnalyzeFastExec
	Ctx         sessionctx.Context
	TableID     core.AnalyzeTableID
	HandleCols  core.HandleCols
	ColsInfo    []*model.ColumnInfo
	IdxsInfo    []*model.IndexInfo
	Concurrency int
	Collectors  []*statistics.SampleCollector
	TblInfo     *model.TableInfo
	Opts        map[ast.AnalyzeOptionType]uint64
}

// TestFastSample only test the fast sample in unit test.
func (e *AnalyzeTestFastExec) TestFastSample() error {
	e.ctx = e.Ctx
	e.handleCols = e.HandleCols
	e.colsInfo = e.ColsInfo
	e.idxsInfo = e.IdxsInfo
	e.concurrency = e.Concurrency
	e.tableID = e.TableID
	e.wg = &sync.WaitGroup{}
	e.job = &statistics.AnalyzeJob{}
	e.tblInfo = e.TblInfo
	e.opts = e.Opts
	_, _, _, _, err := e.buildStats()
	e.Collectors = e.collectors
	return err
}

type analyzeIndexIncrementalExec struct {
	AnalyzeIndexExec
	oldHist *statistics.Histogram
	oldCMS  *statistics.CMSketch
	oldTopN *statistics.TopN
}

func analyzeIndexIncremental(idxExec *analyzeIndexIncrementalExec) analyzeResult {
	var statsVer = statistics.Version1
	if idxExec.analyzePB.IdxReq.Version != nil {
		statsVer = int(*idxExec.analyzePB.IdxReq.Version)
	}
	pruneMode := variable.PartitionPruneMode(idxExec.ctx.GetSessionVars().PartitionPruneMode.Load())
	if idxExec.tableID.IsPartitionTable() && pruneMode == variable.Dynamic {
		err := errors.Errorf("[stats]: global statistics for partitioned tables unavailable in ANALYZE INCREMENTAL")
		return analyzeResult{Err: err, job: idxExec.job}
	}
	startPos := idxExec.oldHist.GetUpper(idxExec.oldHist.Len() - 1)
	values, _, err := codec.DecodeRange(startPos.GetBytes(), len(idxExec.idxInfo.Columns), nil, nil)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	ran := ranger.Range{LowVal: values, HighVal: []types.Datum{types.MaxValueDatum()}}
	hist, cms, fms, topN, err := idxExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	hist, err = statistics.MergeHistograms(idxExec.ctx.GetSessionVars().StmtCtx, idxExec.oldHist, hist, int(idxExec.opts[ast.AnalyzeOptNumBuckets]), statsVer)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	if idxExec.oldCMS != nil && cms != nil {
		err = cms.MergeCMSketch4IncrementalAnalyze(idxExec.oldCMS, uint32(idxExec.opts[ast.AnalyzeOptNumTopN]))
		if err != nil {
			return analyzeResult{Err: err, job: idxExec.job}
		}
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	if statsVer >= statistics.Version2 {
		poped := statistics.MergeTopNAndUpdateCMSketch(topN, idxExec.oldTopN, cms, uint32(idxExec.opts[ast.AnalyzeOptNumTopN]))
		hist.AddIdxVals(poped)
	}
	result := analyzeResult{
		TableID:  idxExec.tableID,
		Hist:     []*statistics.Histogram{hist},
		Cms:      []*statistics.CMSketch{cms},
		TopNs:    []*statistics.TopN{topN},
		Fms:      []*statistics.FMSketch{fms},
		IsIndex:  1,
		job:      idxExec.job,
		StatsVer: statsVer,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

type analyzePKIncrementalExec struct {
	AnalyzeColumnsExec
	oldHist *statistics.Histogram
}

func analyzePKIncremental(colExec *analyzePKIncrementalExec) analyzeResult {
	var maxVal types.Datum
	pkInfo := colExec.handleCols.GetCol(0)
	if mysql.HasUnsignedFlag(pkInfo.RetType.Flag) {
		maxVal = types.NewUintDatum(math.MaxUint64)
	} else {
		maxVal = types.NewIntDatum(math.MaxInt64)
	}
	startPos := *colExec.oldHist.GetUpper(colExec.oldHist.Len() - 1)
	ran := ranger.Range{LowVal: []types.Datum{startPos}, LowExclude: true, HighVal: []types.Datum{maxVal}}
	hists, _, _, _, _, err := colExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return analyzeResult{Err: err, job: colExec.job}
	}
	hist := hists[0]
	hist, err = statistics.MergeHistograms(colExec.ctx.GetSessionVars().StmtCtx, colExec.oldHist, hist, int(colExec.opts[ast.AnalyzeOptNumBuckets]), statistics.Version1)
	if err != nil {
		return analyzeResult{Err: err, job: colExec.job}
	}
	result := analyzeResult{
		TableID:  colExec.TableID,
		Hist:     []*statistics.Histogram{hist},
		Cms:      []*statistics.CMSketch{nil},
		TopNs:    []*statistics.TopN{nil},
		Fms:      []*statistics.FMSketch{nil},
		job:      colExec.job,
		StatsVer: statistics.Version1,
	}
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// analyzeResult is used to represent analyze result.
type analyzeResult struct {
	TableID  core.AnalyzeTableID
	Hist     []*statistics.Histogram
	Cms      []*statistics.CMSketch
	TopNs    []*statistics.TopN
	Fms      []*statistics.FMSketch
	ExtStats *statistics.ExtendedStatsColl
	Count    int64
	IsIndex  int
	Err      error
	job      *statistics.AnalyzeJob
	StatsVer int
}
