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
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
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
}

const (
	maxSampleSize        = 10000
	maxRegionSampleSize  = 1000
	maxSketchSize        = 10000
	defaultCMSketchDepth = 5
	defaultCMSketchWidth = 2048
)

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return errors.Trace(err)
	}
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan statistics.AnalyzeResult, len(e.tasks))
	for i := 0; i < concurrency; i++ {
		go e.analyzeWorker(taskCh, resultCh)
	}
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	for i, panicCnt := 0, 0; i < len(e.tasks) && panicCnt < concurrency; i++ {
		result := <-resultCh
		if result.Err != nil {
			err = result.Err
			if errors.Trace(err) == errAnalyzeWorkerPanic {
				panicCnt++
			} else {
				logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
			}
			continue
		}
		for i, hg := range result.Hist {
			err1 := statsHandle.SaveStatsToStorage(result.PhysicalTableID, result.Count, result.IsIndex, hg, result.Cms[i], 1)
			if err1 != nil {
				err = err1
				logutil.Logger(ctx).Error("save stats to storage failed", zap.Error(err))
				continue
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(statsHandle.Update(GetInfoSchema(e.ctx)))
}

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), errors.Trace(err)
}

type taskType int

const (
	colTask taskType = iota
	idxTask
	fastTask
)

type analyzeTask struct {
	taskType taskType
	idxExec  *AnalyzeIndexExec
	colExec  *AnalyzeColumnsExec
	fastExec *AnalyzeFastExec
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- statistics.AnalyzeResult) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(context.Background()).Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- statistics.AnalyzeResult{
				Err: errAnalyzeWorkerPanic,
			}
		}
	}()
	for task := range taskCh {
		switch task.taskType {
		case colTask:
			resultCh <- analyzeColumnsPushdown(task.colExec)
		case idxTask:
			resultCh <- analyzeIndexPushdown(task.idxExec)
		case fastTask:
			resultCh <- analyzeFastExec(task.fastExec)
		}
	}
}

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) statistics.AnalyzeResult {
	hist, cms, err := idxExec.buildStats()
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{
		PhysicalTableID: idxExec.physicalTableID,
		Hist:            []*statistics.Histogram{hist},
		Cms:             []*statistics.CMSketch{cms},
		IsIndex:         1,
	}
	if hist.Len() > 0 {
		result.Count = hist.Buckets[hist.Len()-1].Count
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
	maxNumBuckets   uint64
}

func (e *AnalyzeIndexExec) open() error {
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.idxInfo.ID, ranger.FullRange()).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.TODO()
	e.result, err = distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(ctx)
	return nil
}

func (e *AnalyzeIndexExec) buildStats() (hist *statistics.Histogram, cms *statistics.CMSketch, err error) {
	if err = e.open(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer func() {
		if err1 := e.result.Close(); err1 != nil {
			hist = nil
			cms = nil
			err = errors.Trace(err1)
		}
	}()
	hist = &statistics.Histogram{}
	cms = statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth)
	for {
		data, err := e.result.NextRaw(context.TODO())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		hist, err = statistics.MergeHistograms(e.ctx.GetSessionVars().StmtCtx, hist, statistics.HistogramFromProto(resp.Hist), int(e.maxNumBuckets))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Cms != nil {
			err := cms.MergeCMSketch(statistics.CMSketchFromProto(resp.Cms))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, cms, nil
}

func analyzeColumnsPushdown(colExec *AnalyzeColumnsExec) statistics.AnalyzeResult {
	hists, cms, err := colExec.buildStats()
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{
		PhysicalTableID: colExec.physicalTableID,
		Hist:            hists,
		Cms:             cms,
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
}

func (e *AnalyzeColumnsExec) open() error {
	var ranges []*ranger.Range
	if e.pkInfo != nil {
		ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(e.pkInfo.Flag))
	} else {
		ranges = ranger.FullIntRange(false)
	}
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(ranges, true)
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		return errors.Trace(err)
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		return errors.Trace(err)
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
		return nil, errors.Trace(err)
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

func (e *AnalyzeColumnsExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	if err = e.open(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			hists = nil
			cms = nil
			err = errors.Trace(err1)
		}
	}()
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: maxSampleSize,
			CMSketch:      statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth),
		}
	}
	for {
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeColumnsResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		sc := e.ctx.GetSessionVars().StmtCtx
		if e.pkInfo != nil {
			pkHist, err = statistics.MergeHistograms(sc, pkHist, statistics.HistogramFromProto(resp.PkHist), int(e.maxNumBuckets))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		for i, rc := range resp.Collectors {
			collectors[i].MergeSampleCollector(sc, statistics.SampleCollectorFromProto(rc))
		}
	}
	timeZone := e.ctx.GetSessionVars().Location()
	if e.pkInfo != nil {
		pkHist.ID = e.pkInfo.ID
		err = pkHist.DecodeTo(&e.pkInfo.FieldType, timeZone)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		hists = append(hists, pkHist)
		cms = append(cms, nil)
	}
	for i, col := range e.colsInfo {
		for j, s := range collectors[i].Samples {
			collectors[i].Samples[j].Ordinal = j
			collectors[i].Samples[j].Value, err = tablecodec.DecodeColumnValue(s.Value.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		hg, err := statistics.BuildColumn(e.ctx, int64(e.maxNumBuckets), col.ID, collectors[i], &col.FieldType)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		hists = append(hists, hg)
		cms = append(cms, collectors[i].CMSketch)
	}
	return hists, cms, nil
}

func analyzeFastExec(exec *AnalyzeFastExec) statistics.AnalyzeResult {
	hists, cms, err := exec.buildStats()
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{
		PhysicalTableID: exec.PhysicalTableID,
		Hist:            hists,
		Cms:             cms,
	}
	hist := hists[0]
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// AnalyzeFastTask is the task for build stats.
type AnalyzeFastTask struct {
	Location  *tikv.KeyLocation
	isScan    bool
	SampSize  uint64
	LRowCount uint64
	RRowCount uint64
}

// AnalyzeFastExec represents Fast Analyze Columns executor.
type AnalyzeFastExec struct {
	ctx             sessionctx.Context
	PhysicalTableID int64
	pkInfo          *model.ColumnInfo
	colsInfo        []*model.ColumnInfo
	idxInfo         *model.IndexInfo
	concurrency     int
	maxNumBuckets   uint64
	table           table.Table
	cache           *tikv.RegionCache
	wg              *sync.WaitGroup
	sampLocs        chan *tikv.KeyLocation
	sampLocRowCount uint64
	tasks           chan *AnalyzeFastTask
}

func (e *AnalyzeFastExec) getSampRegionsRowCount(bo *tikv.Backoffer, needRebuild *bool, err *error) {
	defer func() {
		e.wg.Done()
		if *needRebuild == true {
			for _, ok := <-e.sampLocs; ok; _, ok = <-e.sampLocs {
				// do nothing
			}
		}
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
			*err = errors.Trace(*err)
			return
		}
		ctx := context.Background()
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}
		// TODO: check the region not_found
		if ctx.Err() != nil {
			fmt.Println(ctx.Err().Error())
			*needRebuild = true
			return
		}
		for _, prop := range resp.DebugGetRegionProperties.Props {
			if prop.Name == "num_rows" {
				var cnt uint64
				cnt, *err = strconv.ParseUint(prop.Value, 10, 64)
				if *err != nil {
					*err = errors.Trace(*err)
					return
				}
				newCount := atomic.AddUint64(&e.sampLocRowCount, cnt)
				task := &AnalyzeFastTask{
					Location:  loc,
					LRowCount: newCount - cnt,
					RRowCount: newCount,
				}
				e.tasks <- task
			}
		}
	}
}

func (e *AnalyzeFastExec) buildSampTask() (bool, error) {
	if e.wg == nil {
		e.wg = &sync.WaitGroup{}
	}
	e.wg.Wait()

	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(e.table.Meta().ID)
	var loc *tikv.KeyLocation
	var err error
	bo := tikv.NewBackoffer(context.Background(), 500)
	for loc, err = e.cache.LocateKey(bo, startKey); bytes.Compare(loc.StartKey, endKey) <= 0 && err == nil; loc, err = e.cache.LocateKey(bo, append(loc.EndKey, byte(0))) {
		if bytes.Compare(endKey, loc.EndKey) < 0 || bytes.Compare(loc.StartKey, startKey) < 0 {
			e.tasks <- &AnalyzeFastTask{Location: loc, isScan: true}
		} else {
			e.sampLocs <- loc
		}
	}
	if err != nil {
		return false, errors.Trace(err)
	}

	// Do get regions row count.
	close(e.sampLocs)
	atomic.StoreUint64(&e.sampLocRowCount, 0)
	needRebuildForRoutine := make([]bool, e.concurrency)
	errs := make([]error, e.concurrency)
	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.getSampRegionsRowCount(bo, &needRebuildForRoutine[i], &errs[i])
	}
	e.wg.Wait()
	for i := 0; i < e.concurrency; i++ {
		if errs[i] != nil {
			return false, errors.Trace(errs[i])
		}
	}
	for i := 0; i < e.concurrency; i++ {
		if needRebuildForRoutine[i] == true {
			return true, nil
		}
	}

	return false, nil
}

func (e *AnalyzeFastExec) handleBatchGetResponse(resp *kvrpcpb.BatchGetResponse, collectors []*statistics.SampleCollector, cursor *int32) (err error) {
	length := int32(len(resp.Pairs))
	timeZone := e.ctx.GetSessionVars().Location()
	newCursor := atomic.AddInt32(cursor, length)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	hasIdxInfo := 0
	if e.idxInfo != nil {
		hasIdxInfo = 1
	}
	for i, pair := range resp.Pairs {
		samplePos := newCursor - length + int32(i)
		if hasPKInfo > 0 {
			collectors[0].Samples[samplePos].Ordinal = int(samplePos)
			collectors[0].Samples[samplePos].Value, err = tablecodec.DecodeColumnValue(pair.Value, &e.pkInfo.FieldType, timeZone)
			if err != nil {
				return errors.Trace(err)
			}
		}
		for j, colInfo := range e.colsInfo {
			collectors[hasPKInfo+j].Samples[samplePos].Ordinal = int(samplePos)
			collectors[hasPKInfo+j].Samples[samplePos].Value, err = tablecodec.DecodeColumnValue(pair.Value, &colInfo.FieldType, timeZone)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	if hasIdxInfo > 0 {
		// TODO: index info
	}
	return nil
}

func (e *AnalyzeFastExec) handleSampTasks(bo *tikv.Backoffer, err *error, collectors []*statistics.SampleCollector, sampCursor *int32) {
	defer func() {
		if *err != nil {
			for _, ok := <-e.tasks; ok; _, ok = <-e.tasks {
				// Do nothing.
			}
		}
	}()
	client := e.ctx.GetStore().(tikv.Storage).GetTiKVClient()
	for {
		task, ok := <-e.tasks
		if !ok {
			return
		}

		if task.isScan {
			// TODO: handle scan task.
		}

		var tableID, minRowID, maxRowID int64
		startKey, endKey := task.Location.StartKey, task.Location.EndKey
		tableID, minRowID, *err = tablecodec.DecodeRecordKey(startKey)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}
		_, maxRowID, *err = tablecodec.DecodeRecordKey(endKey)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}

		keys := make([][]byte, 0, task.SampSize)
		for i := 0; i < int(task.SampSize); i++ {
			randKey := rand.Int63n(maxRowID-minRowID) + minRowID
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(tableID, randKey))
		}

		var resp *tikvrpc.Response
		var rpcCtx *tikv.RPCContext
		rpcCtx, *err = e.cache.GetRPCContext(bo, task.Location.Region)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}
		ctx := context.Background()
		req := &tikvrpc.Request{
			Type:        tikvrpc.CmdBatchGet,
			RawBatchGet: &kvrpcpb.RawBatchGetRequest{Keys: keys},
		}
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}

		*err = e.handleBatchGetResponse(resp.BatchGet, collectors, sampCursor)
		if *err != nil {
			*err = errors.Trace(*err)
			return
		}
	}
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, error) {
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	hasIdxInfo := 0
	if e.idxInfo != nil {
		hasIdxInfo = 1
	}
	// collect column samples and primary key samples and index samples.
	length := len(e.colsInfo) + hasPKInfo + hasIdxInfo
	collectors := make([]*statistics.SampleCollector, length)
	var sampCursor int32
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: maxSampleSize,
			CMSketch:      statistics.NewCMSketch(defaultCMSketchDepth, defaultCMSketchWidth),
		}
	}

	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		bo := tikv.NewBackoffer(context.Background(), 500)
		go e.handleSampTasks(bo, &errs[i], collectors, &sampCursor)
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	var err error
	hists, cms := make([]*statistics.Histogram, e.concurrency), make([]*statistics.CMSketch, e.concurrency)
	for i := 0; i < length; i++ {
		if i == 0 && hasPKInfo > 0 {
			hists[i], err = statistics.BuildColumn(e.ctx, int64(e.maxNumBuckets), e.pkInfo.ID, collectors[i], &e.pkInfo.FieldType)
		} else {
			hists[i], err = statistics.BuildColumn(e.ctx, int64(e.maxNumBuckets), e.colsInfo[i+hasPKInfo].ID, collectors[i], &e.colsInfo[i+hasPKInfo].FieldType)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cms[i] = collectors[i].CMSketch
	}
	return hists, cms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	for {
		needRebuild, err := e.buildSampTask()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if needRebuild {
			continue
		}

		if e.sampLocRowCount < maxSampleSize*2 {
			// TODO: return normal Analyze
		}

		randPos := make([]uint64, 0, maxSampleSize+1)
		for i := 0; i < maxSampleSize; i++ {
			randPos = append(randPos, uint64(rand.Int63n(int64(e.sampLocRowCount))))
		}
		sort.Slice(randPos, func(i, j int) bool {
			return randPos[i] < randPos[j]
		})

		for task := range e.tasks {
			if task.isScan == true {
				continue
			}
			task.SampSize = uint64(sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.RRowCount }) - sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.LRowCount }))
		}

		close(e.tasks)
		hists, cms, err = e.runTasks()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return hists, cms, nil
	}
}
