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
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	baseExecutor
	tasks []*analyzeTask
	wg    *sync.WaitGroup
}

var (
	// MaxSampleSize is the size of samples for once analyze.
	// It's public for test.
	MaxSampleSize = int64(10000)
	// RandSeed is the seed for randing package.
	// It's public for test.
	RandSeed = int64(1)
)

const (
	maxRegionSampleSize  = 1000
	maxSketchSize        = 10000
	defaultCMSketchDepth = 5
	defaultCMSketchWidth = 2048
	defaultNumTopN       = uint32(20)
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
			result.job.Finish(true)
			continue
		}
		for i, hg := range result.Hist {
			err1 := statsHandle.SaveStatsToStorage(result.PhysicalTableID, result.Count, result.IsIndex, hg, result.Cms[i], 1)
			if err1 != nil {
				err = err1
				logutil.Logger(ctx).Error("save stats to storage failed", zap.Error(err))
				result.job.Finish(true)
				continue
			}
		}
		result.job.Finish(false)
	}
	for _, task := range e.tasks {
		statistics.MoveToHistory(task.job)
	}
	if err != nil {
		return err
	}
	return statsHandle.Update(GetInfoSchema(e.ctx))
}

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
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
			logutil.Logger(context.Background()).Error("analyze worker panicked", zap.String("stack", string(buf)))
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
			resultCh <- analyzeColumnsPushdown(task.colExec)
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
	hist, cms, err := idxExec.buildStats(ranges, true)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	result := analyzeResult{
		PhysicalTableID: idxExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{cms},
		IsIndex:         1,
		job:             idxExec.job,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	idxInfo         *model.IndexInfo
	concurrency     int
	priority        int
	analyzePB       *tipb.AnalyzeReq
	result          distsql.SelectResult
	countNullRes    distsql.SelectResult
	maxNumBuckets   uint64
	job             *statistics.AnalyzeJob
}

// fetchAnalyzeResult builds and dispatches the `kv.Request` from given ranges, and stores the `SelectResult`
// in corresponding fields based on the input `isNullRange` argument, which indicates if the range is the
// special null range for single-column index to get the null count.
func (e *AnalyzeIndexExec) fetchAnalyzeResult(ranges []*ranger.Range, isNullRange bool) error {
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.idxInfo.ID, ranges).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		return err
	}
	result.Fetch(ctx)
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

func (e *AnalyzeIndexExec) buildStatsFromResult(result distsql.SelectResult, needCMS bool) (*statistics.Histogram, *statistics.CMSketch, error) {
	failpoint.Inject("buildStatsFromResult", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, nil, errors.New("mock buildStatsFromResult error"))
		}
	})
	hist := &statistics.Histogram{}
	var cms *statistics.CMSketch
	if needCMS {
		cms = statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth)
	}
	for {
		data, err := result.NextRaw(context.TODO())
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, err
		}
		respHist := statistics.HistogramFromProto(resp.Hist)
		e.job.Update(int64(respHist.TotalRowCount()))
		hist, err = statistics.MergeHistograms(e.ctx.GetSessionVars().StmtCtx, hist, respHist, int(e.maxNumBuckets))
		if err != nil {
			return nil, nil, err
		}
		if needCMS {
			if resp.Cms == nil {
				logutil.Logger(context.TODO()).Warn("nil CMS in response", zap.String("table", e.idxInfo.Table.O), zap.String("index", e.idxInfo.Name.O))
			} else if err := cms.MergeCMSketch(statistics.CMSketchFromProto(resp.Cms), 0); err != nil {
				return nil, nil, err
			}
		}
	}
	err := hist.ExtractTopN(cms, len(e.idxInfo.Columns), defaultNumTopN)
	if needCMS && cms != nil {
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	return hist, cms, err
}

func (e *AnalyzeIndexExec) buildStats(ranges []*ranger.Range, considerNull bool) (hist *statistics.Histogram, cms *statistics.CMSketch, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	hist, cms, err = e.buildStatsFromResult(e.result, true)
	if err != nil {
		return nil, nil, err
	}
	if e.countNullRes != nil {
		nullHist, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			hist.NullCount = nullHist.Buckets[l-1].Count
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, cms, nil
}

func analyzeColumnsPushdown(colExec *AnalyzeColumnsExec) analyzeResult {
	var ranges []*ranger.Range
	if colExec.pkInfo != nil {
		ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(colExec.pkInfo.Flag))
	} else {
		ranges = ranger.FullIntRange(false)
	}
	hists, cms, err := colExec.buildStats(ranges)
	if err != nil {
		return analyzeResult{Err: err, job: colExec.job}
	}
	result := analyzeResult{
		PhysicalTableID: colExec.physicalTableID,
		Hist:            hists,
		Cms:             cms,
		job:             colExec.job,
	}
	hist := hists[0]
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	colsInfo        []*model.ColumnInfo
	pkInfo          *model.ColumnInfo
	concurrency     int
	priority        int
	analyzePB       *tipb.AnalyzeReq
	resultHandler   *tableResultHandler
	maxNumBuckets   uint64
	job             *statistics.AnalyzeJob
}

func (e *AnalyzeColumnsExec) open(ranges []*ranger.Range) error {
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(ranges, true, false)
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
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of columns.
	kvReq, err := builder.SetTableRanges(e.physicalTableID, ranges, nil).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

func (e *AnalyzeColumnsExec) buildStats(ranges []*ranger.Range) (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	if err = e.open(ranges); err != nil {
		return nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			hists = nil
			cms = nil
			err = err1
		}
	}()
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: atomic.LoadInt64(&MaxSampleSize),
			CMSketch:      statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth),
		}
	}
	for {
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return nil, nil, err1
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeColumnsResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, err
		}
		sc := e.ctx.GetSessionVars().StmtCtx
		rowCount := int64(0)
		if e.pkInfo != nil {
			respHist := statistics.HistogramFromProto(resp.PkHist)
			rowCount = int64(respHist.TotalRowCount())
			pkHist, err = statistics.MergeHistograms(sc, pkHist, respHist, int(e.maxNumBuckets))
			if err != nil {
				return nil, nil, err
			}
		}
		for i, rc := range resp.Collectors {
			respSample := statistics.SampleCollectorFromProto(rc)
			rowCount = respSample.Count + respSample.NullCount
			collectors[i].MergeSampleCollector(sc, respSample)
		}
		e.job.Update(rowCount)
	}
	timeZone := e.ctx.GetSessionVars().Location()
	if e.pkInfo != nil {
		pkHist.ID = e.pkInfo.ID
		err = pkHist.DecodeTo(&e.pkInfo.FieldType, timeZone)
		if err != nil {
			return nil, nil, err
		}
		hists = append(hists, pkHist)
		cms = append(cms, nil)
	}
	for i, col := range e.colsInfo {
		collectors[i].ExtractTopN(defaultNumTopN)
		for j, s := range collectors[i].Samples {
			collectors[i].Samples[j].Ordinal = j
			collectors[i].Samples[j].Value, err = tablecodec.DecodeColumnValue(s.Value.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, err
			}
		}
		hg, err := statistics.BuildColumn(e.ctx, int64(e.maxNumBuckets), col.ID, collectors[i], &col.FieldType)
		if err != nil {
			return nil, nil, err
		}
		hists = append(hists, hg)
		collectors[i].CMSketch.CalcDefaultValForAnalyze(uint64(hg.NDV))
		cms = append(cms, collectors[i].CMSketch)
	}
	return hists, cms, nil
}

func analyzeFastExec(exec *AnalyzeFastExec) []analyzeResult {
	hists, cms, err := exec.buildStats()
	if err != nil {
		return []analyzeResult{{Err: err, job: exec.job}}
	}
	var results []analyzeResult
	hasPKInfo := 0
	if exec.pkInfo != nil {
		hasPKInfo = 1
	}
	if len(exec.idxsInfo) > 0 {
		for i := hasPKInfo + len(exec.colsInfo); i < len(hists); i++ {
			idxResult := analyzeResult{
				PhysicalTableID: exec.physicalTableID,
				Hist:            []*statistics.Histogram{hists[i]},
				Cms:             []*statistics.CMSketch{cms[i]},
				IsIndex:         1,
				Count:           hists[i].NullCount,
				job:             exec.job,
			}
			if hists[i].Len() > 0 {
				idxResult.Count += hists[i].Buckets[hists[i].Len()-1].Count
			}
			results = append(results, idxResult)
		}
	}
	hist := hists[0]
	colResult := analyzeResult{
		PhysicalTableID: exec.physicalTableID,
		Hist:            hists[:hasPKInfo+len(exec.colsInfo)],
		Cms:             cms[:hasPKInfo+len(exec.colsInfo)],
		Count:           hist.NullCount,
		job:             exec.job,
	}
	if hist.Len() > 0 {
		colResult.Count += hist.Buckets[hist.Len()-1].Count
	}
	results = append(results, colResult)
	return results
}

// AnalyzeFastTask is the task for build stats.
type AnalyzeFastTask struct {
	Location    *tikv.KeyLocation
	SampSize    uint64
	BeginOffset uint64
	EndOffset   uint64
}

// AnalyzeFastExec represents Fast Analyze executor.
type AnalyzeFastExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	pkInfo          *model.ColumnInfo
	colsInfo        []*model.ColumnInfo
	idxsInfo        []*model.IndexInfo
	concurrency     int
	maxNumBuckets   uint64
	tblInfo         *model.TableInfo
	cache           *tikv.RegionCache
	wg              *sync.WaitGroup
	sampLocs        chan *tikv.KeyLocation
	rowCount        uint64
	sampCursor      int32
	sampTasks       []*AnalyzeFastTask
	scanTasks       []*tikv.KeyLocation
	collectors      []*statistics.SampleCollector
	randSeed        int64
	job             *statistics.AnalyzeJob
}

func (e *AnalyzeFastExec) getSampRegionsRowCount(bo *tikv.Backoffer, needRebuild *bool, err *error, sampTasks *[]*AnalyzeFastTask) {
	defer func() {
		if *needRebuild == true {
			for ok := true; ok; _, ok = <-e.sampLocs {
				// Do nothing, just clear the channel.
			}
		}
		e.wg.Done()
	}()
	client := e.ctx.GetStore().(tikv.Storage).GetTiKVClient()
	for {
		loc, ok := <-e.sampLocs
		if !ok {
			return
		}
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDebugGetRegionProperties,
			DebugGetRegionProperties: &debugpb.GetRegionPropertiesRequest{
				RegionId: loc.Region.GetID(),
			},
		}
		var resp *tikvrpc.Response
		var rpcCtx *tikv.RPCContext
		rpcCtx, *err = e.cache.GetRPCContext(bo, loc.Region)
		if *err != nil {
			return
		}
		ctx := context.Background()
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			return
		}
		if resp.DebugGetRegionProperties == nil || len(resp.DebugGetRegionProperties.Props) == 0 {
			*needRebuild = true
			return
		}
		for _, prop := range resp.DebugGetRegionProperties.Props {
			if prop.Name == "mvcc.num_rows" {
				var cnt uint64
				cnt, *err = strconv.ParseUint(prop.Value, 10, 64)
				if *err != nil {
					return
				}
				newCount := atomic.AddUint64(&e.rowCount, cnt)
				task := &AnalyzeFastTask{
					Location:    loc,
					BeginOffset: newCount - cnt,
					EndOffset:   newCount,
				}
				*sampTasks = append(*sampTasks, task)
				break
			}
		}
	}
}

// getNextSampleKey gets the next sample key after last failed request. It only retries the needed region.
// Different from other requests, each request range must be the whole region because the region row count
// is only for a whole region. So we need to first find the longest successive prefix ranges of previous request,
// then the next sample key should be the last range that could align with the region bound.
func (e *AnalyzeFastExec) getNextSampleKey(bo *tikv.Backoffer, startKey kv.Key) (kv.Key, error) {
	if len(e.sampTasks) == 0 {
		e.scanTasks = e.scanTasks[:0]
		return startKey, nil
	}
	sort.Slice(e.sampTasks, func(i, j int) bool {
		return bytes.Compare(e.sampTasks[i].Location.StartKey, e.sampTasks[j].Location.StartKey) < 0
	})
	// The sample task should be consecutive with scan task.
	if len(e.scanTasks) > 0 && bytes.Equal(e.scanTasks[0].StartKey, startKey) && !bytes.Equal(e.scanTasks[0].EndKey, e.sampTasks[0].Location.StartKey) {
		e.scanTasks = e.scanTasks[:0]
		e.sampTasks = e.sampTasks[:0]
		return startKey, nil
	}
	prefixLen := 0
	for ; prefixLen < len(e.sampTasks)-1; prefixLen++ {
		if !bytes.Equal(e.sampTasks[prefixLen].Location.EndKey, e.sampTasks[prefixLen+1].Location.StartKey) {
			break
		}
	}
	// Find the last one that could align with region bound.
	for ; prefixLen >= 0; prefixLen-- {
		loc, err := e.cache.LocateKey(bo, e.sampTasks[prefixLen].Location.EndKey)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(loc.StartKey, e.sampTasks[prefixLen].Location.EndKey) {
			startKey = loc.StartKey
			break
		}
	}
	e.sampTasks = e.sampTasks[:prefixLen+1]
	for i := len(e.scanTasks) - 1; i >= 0; i-- {
		if bytes.Compare(startKey, e.scanTasks[i].EndKey) < 0 {
			e.scanTasks = e.scanTasks[:i]
		}
	}
	return startKey, nil
}

// buildSampTask returns two variables, the first bool is whether the task meets region error
// and need to rebuild.
func (e *AnalyzeFastExec) buildSampTask() (needRebuild bool, err error) {
	// Do get regions row count.
	bo := tikv.NewBackoffer(context.Background(), 500)
	needRebuildForRoutine := make([]bool, e.concurrency)
	errs := make([]error, e.concurrency)
	sampTasksForRoutine := make([][]*AnalyzeFastTask, e.concurrency)
	e.sampLocs = make(chan *tikv.KeyLocation, e.concurrency)
	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.getSampRegionsRowCount(bo, &needRebuildForRoutine[i], &errs[i], &sampTasksForRoutine[i])
	}

	defer func() {
		close(e.sampLocs)
		e.wg.Wait()
		if err != nil {
			return
		}
		for i := 0; i < e.concurrency; i++ {
			if errs[i] != nil {
				err = errs[i]
			}
			needRebuild = needRebuild || needRebuildForRoutine[i]
			e.sampTasks = append(e.sampTasks, sampTasksForRoutine[i]...)
		}
	}()

	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(e.physicalTableID)
	targetKey, err := e.getNextSampleKey(bo, startKey)
	if err != nil {
		return false, err
	}
	e.rowCount = 0
	for _, task := range e.sampTasks {
		cnt := task.EndOffset - task.BeginOffset
		task.BeginOffset = e.rowCount
		task.EndOffset = e.rowCount + cnt
		e.rowCount += cnt
	}
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return false, err
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampLocs <- loc
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

	return false, nil
}

func (e *AnalyzeFastExec) decodeValues(sValue []byte) (values map[int64]types.Datum, err error) {
	colID2FieldTypes := make(map[int64]*types.FieldType, len(e.colsInfo))
	if e.pkInfo != nil {
		colID2FieldTypes[e.pkInfo.ID] = &e.pkInfo.FieldType
	}
	for _, col := range e.colsInfo {
		colID2FieldTypes[col.ID] = &col.FieldType
	}
	return tablecodec.DecodeRow(sValue, colID2FieldTypes, e.ctx.GetSessionVars().Location())
}

func (e *AnalyzeFastExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	val, ok := values[colInfo.ID]
	if !ok {
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	return val, nil
}

func (e *AnalyzeFastExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32, hasPKInfo int) (err error) {
	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(sValue)
	if err != nil {
		return err
	}
	var rowID int64
	rowID, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		return err
	}
	// Update the primary key collector.
	if hasPKInfo > 0 {
		v, ok := values[e.pkInfo.ID]
		if !ok {
			var key int64
			_, key, err = tablecodec.DecodeRecordKey(sKey)
			if err != nil {
				return err
			}
			v = types.NewIntDatum(key)
		}
		if mysql.HasUnsignedFlag(e.pkInfo.Flag) {
			v.SetUint64(uint64(v.GetInt64()))
		}
		if e.collectors[0].Samples[samplePos] == nil {
			e.collectors[0].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[0].Samples[samplePos].RowID = rowID
		e.collectors[0].Samples[samplePos].Value = v
	}
	// Update the columns' collectors.
	for j, colInfo := range e.colsInfo {
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			return err
		}
		if e.collectors[hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[hasPKInfo+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			for _, colInfo := range e.tblInfo.Columns {
				if colInfo.Name == idxCol.Name {
					v, err := e.getValueByInfo(colInfo, values)
					if err != nil {
						return err
					}
					idxVals = append(idxVals, v)
					break
				}
			}
		}
		var bytes []byte
		bytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, bytes, idxVals...)
		if err != nil {
			return err
		}
		if e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].Value = types.NewBytesDatum(bytes)
	}
	return nil
}

func (e *AnalyzeFastExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos, hasPKInfo)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeFastExec) handleScanIter(iter kv.Iterator) (err error) {
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	rander := rand.New(rand.NewSource(e.randSeed + int64(e.rowCount)))
	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		e.rowCount++
		randNum := rander.Int63n(int64(e.rowCount))
		if randNum > int64(MaxSampleSize) && e.sampCursor == int32(MaxSampleSize) {
			continue
		}

		p := rander.Int31n(int32(MaxSampleSize))
		if e.sampCursor < int32(MaxSampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p, hasPKInfo)
		if err != nil {
			return err
		}
	}
	return err
}

func (e *AnalyzeFastExec) handleScanTasks(bo *tikv.Backoffer) error {
	snapshot, err := e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if err != nil {
		return err
	}
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(t.StartKey, t.EndKey)
		if err != nil {
			return err
		}
		err = e.handleScanIter(iter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *AnalyzeFastExec) handleSampTasks(bo *tikv.Backoffer, workID int, err *error) {
	defer e.wg.Done()
	var snapshot kv.Snapshot
	snapshot, *err = e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	rander := rand.New(rand.NewSource(e.randSeed + int64(workID)))
	if *err != nil {
		return
	}
	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		if task.SampSize == 0 {
			continue
		}

		var tableID, minRowID, maxRowID int64
		startKey, endKey := task.Location.StartKey, task.Location.EndKey
		tableID, minRowID, *err = tablecodec.DecodeRecordKey(startKey)
		if *err != nil {
			return
		}
		_, maxRowID, *err = tablecodec.DecodeRecordKey(endKey)
		if *err != nil {
			return
		}

		keys := make([]kv.Key, 0, task.SampSize)
		for i := 0; i < int(task.SampSize); i++ {
			randKey := rander.Int63n(maxRowID-minRowID) + minRowID
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(tableID, randKey))
		}

		kvMap := make(map[string][]byte, len(keys))
		for _, key := range keys {
			var iter kv.Iterator
			iter, *err = snapshot.Iter(key, endKey)
			if *err != nil {
				return
			}
			if iter.Valid() {
				kvMap[string(iter.Key())] = iter.Value()
			}
		}

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeFastExec) buildColumnStats(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	data := make([][]byte, 0, len(collector.Samples))
	for i, sample := range collector.Samples {
		sample.Ordinal = i
		if sample.Value.IsNull() {
			collector.NullCount++
			continue
		}
		bytes, err := tablecodec.EncodeValue(e.ctx.GetSessionVars().StmtCtx, sample.Value)
		if err != nil {
			return nil, nil, err
		}
		data = append(data, bytes)
	}
	// Build CMSketch.
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data, 20, uint64(rowCount))
	// Build Histogram.
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.maxNumBuckets), ID, collector, tp, rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) buildIndexStats(idxInfo *model.IndexInfo, collector *statistics.SampleCollector, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	data := make([][][]byte, len(idxInfo.Columns), len(idxInfo.Columns))
	for _, sample := range collector.Samples {
		var preLen int
		remained := sample.Value.GetBytes()
		// We need to insert each prefix values into CM Sketch.
		for i := 0; i < len(idxInfo.Columns); i++ {
			var err error
			var value []byte
			value, remained, err = codec.CutOne(remained)
			if err != nil {
				return nil, nil, err
			}
			preLen += len(value)
			data[i] = append(data[i], sample.Value.GetBytes()[:preLen])
		}
	}
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data[0], defaultNumTopN, uint64(rowCount))
	// Build CM Sketch for each prefix and merge them into one.
	for i := 1; i < len(idxInfo.Columns); i++ {
		var curCMSketch *statistics.CMSketch
		// `ndv` should be the ndv of full index, so just rewrite it here.
		curCMSketch, ndv, scaleRatio = statistics.NewCMSketchWithTopN(defaultCMSketchDepth, defaultCMSketchWidth, data[i], defaultNumTopN, uint64(rowCount))
		err := cmSketch.MergeCMSketch(curCMSketch, defaultNumTopN)
		if err != nil {
			return nil, nil, err
		}
	}
	// Build Histogram.
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.maxNumBuckets), idxInfo.ID, collector, types.NewFieldType(mysql.TypeBlob), rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, error) {
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	// collect column samples and primary key samples and index samples.
	length := len(e.colsInfo) + hasPKInfo + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(MaxSampleSize),
			Samples:       make([]*statistics.SampleItem, MaxSampleSize),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackoffer(context.Background(), 500)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(bo, i, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, err
		}
	}

	err := e.handleScanTasks(bo)
	if err != nil {
		return nil, nil, err
	}

	handle := domain.GetDomain(e.ctx).StatsHandle()
	tblStats := handle.GetTableStats(e.tblInfo)
	rowCount := int64(e.rowCount)
	if handle.Lease() > 0 && !tblStats.Pseudo {
		rowCount = mathutil.MinInt64(tblStats.Count, rowCount)
	}
	// Adjust the row count in case the count of `tblStats` is not accurate and too small.
	rowCount = mathutil.MaxInt64(rowCount, int64(e.sampCursor))
	hists, cms := make([]*statistics.Histogram, length), make([]*statistics.CMSketch, length)
	for i := 0; i < length; i++ {
		// Build collector properties.
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool { return collector.Samples[i].RowID < collector.Samples[j].RowID })
		collector.CalcTotalSize()
		// Scale the total column size.
		if len(collector.Samples) > 0 {
			collector.TotalSize *= rowCount / int64(len(collector.Samples))
		}
		if i < hasPKInfo {
			hists[i], cms[i], err = e.buildColumnStats(e.pkInfo.ID, e.collectors[i], &e.pkInfo.FieldType, rowCount)
		} else if i < hasPKInfo+len(e.colsInfo) {
			hists[i], cms[i], err = e.buildColumnStats(e.colsInfo[i-hasPKInfo].ID, e.collectors[i], &e.colsInfo[i-hasPKInfo].FieldType, rowCount)
		} else {
			hists[i], cms[i], err = e.buildIndexStats(e.idxsInfo[i-hasPKInfo-len(e.colsInfo)], e.collectors[i], rowCount)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return hists, cms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	// To set rand seed, it's for unit test.
	// To ensure that random sequences are different in non-test environments, RandSeed must be set time.Now().
	if RandSeed == 1 {
		e.randSeed = time.Now().UnixNano()
	} else {
		e.randSeed = RandSeed
	}
	rander := rand.New(rand.NewSource(e.randSeed))

	// Only four rebuilds for sample task are allowed.
	needRebuild, maxBuildTimes := true, 5
	for counter := maxBuildTimes; needRebuild && counter > 0; counter-- {
		needRebuild, err = e.buildSampTask()
		if err != nil {
			return nil, nil, err
		}
	}
	if needRebuild {
		errMsg := "build fast analyze task failed, exceed maxBuildTimes: %v"
		return nil, nil, errors.Errorf(errMsg, maxBuildTimes)
	}

	defer e.job.Update(int64(e.rowCount))

	// If total row count of the table is smaller than 2*MaxSampleSize, we
	// translate all the sample tasks to scan tasks.
	if e.rowCount < uint64(MaxSampleSize)*2 {
		for _, task := range e.sampTasks {
			e.scanTasks = append(e.scanTasks, task.Location)
		}
		e.sampTasks = e.sampTasks[:0]
		e.rowCount = 0
		return e.runTasks()
	}

	randPos := make([]uint64, 0, MaxSampleSize+1)
	for i := 0; i < int(MaxSampleSize); i++ {
		randPos = append(randPos, uint64(rander.Int63n(int64(e.rowCount))))
	}
	sort.Slice(randPos, func(i, j int) bool { return randPos[i] < randPos[j] })

	for _, task := range e.sampTasks {
		begin := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.BeginOffset })
		end := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.EndOffset })
		task.SampSize = uint64(end - begin)
	}
	return e.runTasks()
}

// AnalyzeTestFastExec is for fast sample in unit test.
type AnalyzeTestFastExec struct {
	AnalyzeFastExec
	Ctx             sessionctx.Context
	PhysicalTableID int64
	PKInfo          *model.ColumnInfo
	ColsInfo        []*model.ColumnInfo
	IdxsInfo        []*model.IndexInfo
	Concurrency     int
	Collectors      []*statistics.SampleCollector
	TblInfo         *model.TableInfo
}

// TestFastSample only test the fast sample in unit test.
func (e *AnalyzeTestFastExec) TestFastSample() error {
	e.ctx = e.Ctx
	e.pkInfo = e.PKInfo
	e.colsInfo = e.ColsInfo
	e.idxsInfo = e.IdxsInfo
	e.concurrency = e.Concurrency
	e.physicalTableID = e.PhysicalTableID
	e.wg = &sync.WaitGroup{}
	e.job = &statistics.AnalyzeJob{}
	e.tblInfo = e.TblInfo
	_, _, err := e.buildStats()
	e.Collectors = e.collectors
	return err
}

type analyzeIndexIncrementalExec struct {
	AnalyzeIndexExec
	oldHist *statistics.Histogram
	oldCMS  *statistics.CMSketch
}

func analyzeIndexIncremental(idxExec *analyzeIndexIncrementalExec) analyzeResult {
	startPos := idxExec.oldHist.GetUpper(idxExec.oldHist.Len() - 1)
	values, _, err := codec.DecodeRange(startPos.GetBytes(), len(idxExec.idxInfo.Columns))
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	ran := ranger.Range{LowVal: values, HighVal: []types.Datum{types.MaxValueDatum()}}
	hist, cms, err := idxExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	hist, err = statistics.MergeHistograms(idxExec.ctx.GetSessionVars().StmtCtx, idxExec.oldHist, hist, int(idxExec.maxNumBuckets))
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	if idxExec.oldCMS != nil && cms != nil {
		err = cms.MergeCMSketch4IncrementalAnalyze(idxExec.oldCMS, defaultNumTopN)
		if err != nil {
			return analyzeResult{Err: err, job: idxExec.job}
		}
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	result := analyzeResult{
		PhysicalTableID: idxExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{cms},
		IsIndex:         1,
		job:             idxExec.job,
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
	if mysql.HasUnsignedFlag(colExec.pkInfo.Flag) {
		maxVal = types.NewUintDatum(math.MaxUint64)
	} else {
		maxVal = types.NewIntDatum(math.MaxInt64)
	}
	startPos := *colExec.oldHist.GetUpper(colExec.oldHist.Len() - 1)
	ran := ranger.Range{LowVal: []types.Datum{startPos}, LowExclude: true, HighVal: []types.Datum{maxVal}}
	hists, _, err := colExec.buildStats([]*ranger.Range{&ran})
	if err != nil {
		return analyzeResult{Err: err, job: colExec.job}
	}
	hist := hists[0]
	hist, err = statistics.MergeHistograms(colExec.ctx.GetSessionVars().StmtCtx, colExec.oldHist, hist, int(colExec.maxNumBuckets))
	if err != nil {
		return analyzeResult{Err: err, job: colExec.job}
	}
	result := analyzeResult{
		PhysicalTableID: colExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{nil},
		job:             colExec.job,
	}
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// analyzeResult is used to represent analyze result.
type analyzeResult struct {
	// PhysicalTableID is the id of a partition or a table.
	PhysicalTableID int64
	Hist            []*statistics.Histogram
	Cms             []*statistics.CMSketch
	Count           int64
	IsIndex         int
	Err             error
	job             *statistics.AnalyzeJob
}
