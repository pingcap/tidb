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

package executor

import (
	"context"
	stderrors "errors"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tiancaiamao/gp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// AnalyzeColumnsExecV2 is used to maintain v2 analyze process
type AnalyzeColumnsExecV2 struct {
	*AnalyzeColumnsExec
}

func (e *AnalyzeColumnsExecV2) analyzeColumnsPushDownV2(gp *gp.Pool) *statistics.AnalyzeResults {
	var ranges []*ranger.Range
	if hc := e.handleCols; hc != nil {
		if hc.IsInt() {
			ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(hc.GetCol(0).RetType.GetFlag()))
		} else {
			ranges = ranger.FullNotNullRange()
		}
	} else {
		ranges = ranger.FullIntRange(false)
	}

	collExtStats := e.ctx.GetSessionVars().EnableExtendedStats
	specialIndexes := make([]*model.IndexInfo, 0, len(e.indexes))
	specialIndexesOffsets := make([]int, 0, len(e.indexes))
	for i, idx := range e.indexes {
		isSpecial := false
		for _, col := range idx.Columns {
			colInfo := e.colsInfo[col.Offset]
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
	samplingStatsConcurrency, err := getBuildSamplingStatsConcurrency(e.ctx)
	if err != nil {
		e.memTracker.Release(e.memTracker.BytesConsumed())
		return &statistics.AnalyzeResults{Err: err, Job: e.job}
	}
	statsConcurrncy, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		e.memTracker.Release(e.memTracker.BytesConsumed())
		return &statistics.AnalyzeResults{Err: err, Job: e.job}
	}
	idxNDVPushDownCh := make(chan analyzeIndexNDVTotalResult, 1)
	// subIndexWorkerWg is better to be initialized in handleNDVForSpecialIndexes, however if we do so, golang would
	// report unexpected/unreasonable data race error on subIndexWorkerWg when running TestAnalyzeVirtualCol test
	// case with `-race` flag now.
	wg := util.NewWaitGroupPool(gp)
	wg.Run(func() {
		e.handleNDVForSpecialIndexes(specialIndexes, idxNDVPushDownCh, statsConcurrncy)
	})
	defer wg.Wait()

	count, hists, topNs, fmSketches, extStats, err := e.buildSamplingStats(gp, ranges, collExtStats, specialIndexesOffsets, idxNDVPushDownCh, samplingStatsConcurrency)
	if err != nil {
		e.memTracker.Release(e.memTracker.BytesConsumed())
		return &statistics.AnalyzeResults{Err: err, Job: e.job}
	}
	cLen := len(e.analyzePB.ColReq.ColumnsInfo)
	colGroupResult := &statistics.AnalyzeResult{
		Hist:    hists[cLen:],
		TopNs:   topNs[cLen:],
		Fms:     fmSketches[cLen:],
		IsIndex: 1,
	}
	// Discard stats of _tidb_rowid.
	// Because the process of analyzing will keep the order of results be the same as the colsInfo in the analyze task,
	// and in `buildAnalyzeFullSamplingTask` we always place the _tidb_rowid at the last of colsInfo, so if there are
	// stats for _tidb_rowid, it must be at the end of the column stats.
	// Virtual column has no histogram yet. So we check nil here.
	if hists[cLen-1] != nil && hists[cLen-1].ID == -1 {
		cLen--
	}
	colResult := &statistics.AnalyzeResult{
		Hist:  hists[:cLen],
		TopNs: topNs[:cLen],
		Fms:   fmSketches[:cLen],
	}

	return &statistics.AnalyzeResults{
		TableID:       e.tableID,
		Ars:           []*statistics.AnalyzeResult{colResult, colGroupResult},
		Job:           e.job,
		StatsVer:      e.StatsVersion,
		Count:         count,
		Snapshot:      e.snapshot,
		ExtStats:      extStats,
		BaseCount:     e.baseCount,
		BaseModifyCnt: e.baseModifyCnt,
	}
}

// decodeSampleDataWithVirtualColumn constructs the virtual column by evaluating from the deocded normal columns.
// If it failed, it would return false to trigger normal decoding way without the virtual column.
func (e *AnalyzeColumnsExecV2) decodeSampleDataWithVirtualColumn(
	collector statistics.RowSampleCollector,
	fieldTps []*types.FieldType,
	virtualColIdx []int,
	schema *expression.Schema,
) error {
	totFts := make([]*types.FieldType, 0, e.schemaForVirtualColEval.Len())
	for _, col := range e.schemaForVirtualColEval.Columns {
		totFts = append(totFts, col.RetType)
	}
	chk := chunk.NewChunkWithCapacity(totFts, len(collector.Base().Samples))
	decoder := codec.NewDecoder(chk, e.ctx.GetSessionVars().Location())
	for _, sample := range collector.Base().Samples {
		for i, columns := range sample.Columns {
			if schema.Columns[i].VirtualExpr != nil {
				continue
			}
			_, err := decoder.DecodeOne(columns.GetBytes(), i, e.schemaForVirtualColEval.Columns[i].RetType)
			if err != nil {
				return err
			}
		}
	}
	err := table.FillVirtualColumnValue(fieldTps, virtualColIdx, schema.Columns, e.colsInfo, e.ctx.GetExprCtx(), chk)
	if err != nil {
		return err
	}
	iter := chunk.NewIterator4Chunk(chk)
	for row, i := iter.Begin(), 0; row != iter.End(); row, i = iter.Next(), i+1 {
		datums := row.GetDatumRow(totFts)
		collector.Base().Samples[i].Columns = datums
	}
	return nil
}

func printAnalyzeMergeCollectorLog(oldRootCount, newRootCount, subCount, tableID, partitionID int64, isPartition bool, info string, index int) {
	if index < 0 {
		logutil.BgLogger().Debug(info,
			zap.Int64("tableID", tableID),
			zap.Int64("partitionID", partitionID),
			zap.Bool("isPartitionTable", isPartition),
			zap.Int64("oldRootCount", oldRootCount),
			zap.Int64("newRootCount", newRootCount),
			zap.Int64("subCount", subCount))
	} else {
		logutil.BgLogger().Debug(info,
			zap.Int64("tableID", tableID),
			zap.Int64("partitionID", partitionID),
			zap.Bool("isPartitionTable", isPartition),
			zap.Int64("oldRootCount", oldRootCount),
			zap.Int64("newRootCount", newRootCount),
			zap.Int64("subCount", subCount),
			zap.Int("subCollectorIndex", index))
	}
}

func (e *AnalyzeColumnsExecV2) buildSamplingStats(
	gp *gp.Pool,
	ranges []*ranger.Range,
	needExtStats bool,
	indexesWithVirtualColOffsets []int,
	idxNDVPushDownCh chan analyzeIndexNDVTotalResult,
	samplingStatsConcurrency int,
) (
	count int64,
	hists []*statistics.Histogram,
	topns []*statistics.TopN,
	fmSketches []*statistics.FMSketch,
	extStats *statistics.ExtendedStatsColl,
	err error,
) {
	// Open memory tracker and resultHandler.
	if err = e.open(ranges); err != nil {
		return 0, nil, nil, nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			err = err1
		}
	}()

	l := len(e.analyzePB.ColReq.ColumnsInfo) + len(e.analyzePB.ColReq.ColumnGroups)
	rootRowCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), l)
	for i := 0; i < l; i++ {
		rootRowCollector.Base().FMSketches = append(rootRowCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	}

	sc := e.ctx.GetSessionVars().StmtCtx

	// Start workers to merge the result from collectors.
	mergeResultCh := make(chan *samplingMergeResult, 1)
	mergeTaskCh := make(chan []byte, 1)
	var taskEg errgroup.Group
	// Start read data from resultHandler and send them to mergeTaskCh.
	taskEg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = getAnalyzePanicErr(r)
			}
		}()
		return readDataAndSendTask(e.ctx, e.resultHandler, mergeTaskCh, e.memTracker)
	})
	e.samplingMergeWg = &util.WaitGroupWrapper{}
	e.samplingMergeWg.Add(samplingStatsConcurrency)
	for i := 0; i < samplingStatsConcurrency; i++ {
		id := i
		gp.Go(func() {
			e.subMergeWorker(mergeResultCh, mergeTaskCh, l, id)
		})
	}
	// Merge the result from collectors.
	mergeWorkerPanicCnt := 0
	mergeEg, mergeCtx := errgroup.WithContext(context.Background())
	mergeEg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = getAnalyzePanicErr(r)
			}
		}()
		for mergeWorkerPanicCnt < samplingStatsConcurrency {
			mergeResult, ok := <-mergeResultCh
			if !ok {
				break
			}
			if mergeResult.err != nil {
				err = mergeResult.err
				if isAnalyzeWorkerPanic(mergeResult.err) {
					mergeWorkerPanicCnt++
				}
				continue
			}
			oldRootCollectorSize := rootRowCollector.Base().MemSize
			oldRootCollectorCount := rootRowCollector.Base().Count
			// Merge the result from sub-collectors.
			rootRowCollector.MergeCollector(mergeResult.collector)
			newRootCollectorCount := rootRowCollector.Base().Count
			printAnalyzeMergeCollectorLog(oldRootCollectorCount, newRootCollectorCount,
				mergeResult.collector.Base().Count, e.tableID.TableID, e.tableID.PartitionID, e.tableID.IsPartitionTable(),
				"merge subMergeWorker in AnalyzeColumnsExecV2", -1)
			e.memTracker.Consume(rootRowCollector.Base().MemSize - oldRootCollectorSize - mergeResult.collector.Base().MemSize)
			mergeResult.collector.DestroyAndPutToPool()
		}
		return err
	})
	err = taskEg.Wait()
	if err != nil {
		mergeCtx.Done()
		if err1 := mergeEg.Wait(); err1 != nil {
			err = stderrors.Join(err, err1)
		}
		return 0, nil, nil, nil, nil, getAnalyzePanicErr(err)
	}
	err = mergeEg.Wait()
	defer e.memTracker.Release(rootRowCollector.Base().MemSize)
	if err != nil {
		return 0, nil, nil, nil, nil, err
	}

	// Decode the data from sample collectors.
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
		for _, sample := range rootRowCollector.Base().Samples {
			for i := range sample.Columns {
				sample.Columns[i], err = tablecodec.DecodeColumnValue(sample.Columns[i].GetBytes(), &e.colsInfo[i].FieldType, sc.TimeZone())
				if err != nil {
					return 0, nil, nil, nil, nil, err
				}
			}
		}
	}

	// Calculate handle from the row data for each row. It will be used to sort the samples.
	for _, sample := range rootRowCollector.Base().Samples {
		sample.Handle, err = e.handleCols.BuildHandleByDatums(sample.Columns)
		if err != nil {
			return 0, nil, nil, nil, nil, err
		}
	}
	colLen := len(e.colsInfo)
	// The order of the samples are broken when merging samples from sub-collectors.
	// So now we need to sort the samples according to the handle in order to calculate correlation.
	slices.SortFunc(rootRowCollector.Base().Samples, func(i, j *statistics.ReservoirRowSampleItem) int {
		return i.Handle.Compare(j.Handle)
	})

	totalLen := len(e.colsInfo) + len(e.indexes)
	hists = make([]*statistics.Histogram, totalLen)
	topns = make([]*statistics.TopN, totalLen)
	fmSketches = make([]*statistics.FMSketch, 0, totalLen)
	buildResultChan := make(chan error, totalLen)
	buildTaskChan := make(chan *samplingBuildTask, totalLen)
	if totalLen < samplingStatsConcurrency {
		samplingStatsConcurrency = totalLen
	}
	e.samplingBuilderWg = newNotifyErrorWaitGroupWrapper(gp, buildResultChan)
	sampleCollectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	exitCh := make(chan struct{})
	e.samplingBuilderWg.Add(samplingStatsConcurrency)

	// Start workers to build stats.
	for i := 0; i < samplingStatsConcurrency; i++ {
		e.samplingBuilderWg.Run(func() {
			e.subBuildWorker(buildResultChan, buildTaskChan, hists, topns, sampleCollectors, exitCh)
		})
	}
	// Generate tasks for building stats.
	for i, col := range e.colsInfo {
		buildTaskChan <- &samplingBuildTask{
			id:               col.ID,
			rootRowCollector: rootRowCollector,
			tp:               &col.FieldType,
			isColumn:         true,
			slicePos:         i,
		}
		fmSketches = append(fmSketches, rootRowCollector.Base().FMSketches[i])
	}

	indexPushedDownResult := <-idxNDVPushDownCh
	if indexPushedDownResult.err != nil {
		close(exitCh)
		e.samplingBuilderWg.Wait()
		return 0, nil, nil, nil, nil, indexPushedDownResult.err
	}
	for _, offset := range indexesWithVirtualColOffsets {
		ret := indexPushedDownResult.results[e.indexes[offset].ID]
		rootRowCollector.Base().NullCount[colLen+offset] = ret.Count
		rootRowCollector.Base().FMSketches[colLen+offset] = ret.Ars[0].Fms[0]
	}

	// Generate tasks for building stats for indexes.
	for i, idx := range e.indexes {
		buildTaskChan <- &samplingBuildTask{
			id:               idx.ID,
			rootRowCollector: rootRowCollector,
			tp:               types.NewFieldType(mysql.TypeBlob),
			isColumn:         false,
			slicePos:         colLen + i,
		}
		fmSketches = append(fmSketches, rootRowCollector.Base().FMSketches[colLen+i])
	}
	close(buildTaskChan)

	panicCnt := 0
	for panicCnt < samplingStatsConcurrency {
		err1, ok := <-buildResultChan
		if !ok {
			break
		}
		if err1 != nil {
			err = err1
			if isAnalyzeWorkerPanic(err1) {
				panicCnt++
			}
			continue
		}
	}
	defer func() {
		totalSampleCollectorSize := int64(0)
		for _, sampleCollector := range sampleCollectors {
			if sampleCollector != nil {
				totalSampleCollectorSize += sampleCollector.MemSize
			}
		}
		e.memTracker.Release(totalSampleCollectorSize)
	}()
	if err != nil {
		return 0, nil, nil, nil, nil, err
	}

	count = rootRowCollector.Base().Count
	if needExtStats {
		extStats, err = statistics.BuildExtendedStats(e.ctx, e.TableID.GetStatisticsID(), e.colsInfo, sampleCollectors)
		if err != nil {
			return 0, nil, nil, nil, nil, err
		}
	}

	return
}

// handleNDVForSpecialIndexes deals with the logic to analyze the index containing the virtual column when the mode is full sampling.
func (e *AnalyzeColumnsExecV2) handleNDVForSpecialIndexes(indexInfos []*model.IndexInfo, totalResultCh chan analyzeIndexNDVTotalResult, statsConcurrncy int) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze ndv for special index panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			totalResultCh <- analyzeIndexNDVTotalResult{
				err: getAnalyzePanicErr(r),
			}
		}
	}()
	tasks := e.buildSubIndexJobForSpecialIndex(indexInfos)
	taskCh := make(chan *analyzeTask, len(tasks))
	for _, task := range tasks {
		AddNewAnalyzeJob(e.ctx, task.job)
	}
	resultsCh := make(chan *statistics.AnalyzeResults, len(tasks))
	if len(tasks) < statsConcurrncy {
		statsConcurrncy = len(tasks)
	}
	var subIndexWorkerWg = NewAnalyzeResultsNotifyWaitGroupWrapper(resultsCh)
	subIndexWorkerWg.Add(statsConcurrncy)
	for i := 0; i < statsConcurrncy; i++ {
		subIndexWorkerWg.Run(func() { e.subIndexWorkerForNDV(taskCh, resultsCh) })
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	panicCnt := 0
	totalResult := analyzeIndexNDVTotalResult{
		results: make(map[int64]*statistics.AnalyzeResults, len(indexInfos)),
	}
	var err error
	for panicCnt < statsConcurrncy {
		results, ok := <-resultsCh
		if !ok {
			break
		}
		if results.Err != nil {
			err = results.Err
			FinishAnalyzeJob(e.ctx, results.Job, err)
			if isAnalyzeWorkerPanic(err) {
				panicCnt++
			}
			continue
		}
		FinishAnalyzeJob(e.ctx, results.Job, nil)
		totalResult.results[results.Ars[0].Hist[0].ID] = results
	}
	if err != nil {
		totalResult.err = err
	}
	totalResultCh <- totalResult
}

// subIndexWorker receive the task for each index and return the result for them.
func (e *AnalyzeColumnsExecV2) subIndexWorkerForNDV(taskCh chan *analyzeTask, resultsCh chan *statistics.AnalyzeResults) {
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
		if task.taskType != idxTask {
			resultsCh <- &statistics.AnalyzeResults{
				Err: errors.Errorf("incorrect analyze type"),
				Job: task.job,
			}
			continue
		}
		task.idxExec.job = task.job
		resultsCh <- analyzeIndexNDVPushDown(task.idxExec)
	}
}

// buildSubIndexJobForSpecialIndex builds sub index pushed down task to calculate the NDV information for indexes containing virtual column.
// This is because we cannot push the calculation of the virtual column down to the tikv side.
func (e *AnalyzeColumnsExecV2) buildSubIndexJobForSpecialIndex(indexInfos []*model.IndexInfo) []*analyzeTask {
	_, offset := timeutil.Zone(e.ctx.GetSessionVars().Location())
	tasks := make([]*analyzeTask, 0, len(indexInfos))
	sc := e.ctx.GetSessionVars().StmtCtx
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), e.ctx)
	for _, indexInfo := range indexInfos {
		base := baseAnalyzeExec{
			ctx:         e.ctx,
			tableID:     e.TableID,
			concurrency: concurrency,
			analyzePB: &tipb.AnalyzeReq{
				Tp:             tipb.AnalyzeType_TypeIndex,
				Flags:          sc.PushDownFlags(),
				TimeZoneOffset: offset,
			},
			snapshot: e.snapshot,
		}
		idxExec := &AnalyzeIndexExec{
			baseAnalyzeExec: base,
			isCommonHandle:  e.tableInfo.IsCommonHandle,
			idxInfo:         indexInfo,
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
			SketchSize: statistics.MaxSketchSize,
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
		idxExec.job = job
		tasks = append(tasks, &analyzeTask{
			taskType: idxTask,
			idxExec:  idxExec,
			job:      job,
		})
	}
	return tasks
}

func (e *AnalyzeColumnsExecV2) subMergeWorker(resultCh chan<- *samplingMergeResult, taskCh <-chan []byte, l int, index int) {
	// Only close the resultCh in the first worker.
	closeTheResultCh := index == 0
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- &samplingMergeResult{err: getAnalyzePanicErr(r)}
		}
		// Consume the remaining things.
		for {
			_, ok := <-taskCh
			if !ok {
				break
			}
		}
		e.samplingMergeWg.Done()
		if closeTheResultCh {
			e.samplingMergeWg.Wait()
			close(resultCh)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingMergeWorkerPanic", func() {
		panic("failpoint triggered")
	})
	failpoint.Inject("mockAnalyzeMergeWorkerSlowConsume", func(val failpoint.Value) {
		times := val.(int)
		for i := 0; i < times; i++ {
			e.memTracker.Consume(5 << 20)
			time.Sleep(100 * time.Millisecond)
		}
	})
	retCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), l)
	for i := 0; i < l; i++ {
		retCollector.Base().FMSketches = append(retCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	}
	for {
		data, ok := <-taskCh
		if !ok {
			break
		}

		// Unmarshal the data.
		dataSize := int64(cap(data))
		colResp := &tipb.AnalyzeColumnsResp{}
		err := colResp.Unmarshal(data)
		if err != nil {
			resultCh <- &samplingMergeResult{err: err}
			return
		}
		// Consume the memory of the data.
		colRespSize := int64(colResp.Size())
		e.memTracker.Consume(colRespSize)

		// Update processed rows.
		subCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), l)
		subCollector.Base().FromProto(colResp.RowCollector, e.memTracker)
		UpdateAnalyzeJob(e.ctx, e.job, subCollector.Base().Count)

		// Print collect log.
		oldRetCollectorSize := retCollector.Base().MemSize
		oldRetCollectorCount := retCollector.Base().Count
		retCollector.MergeCollector(subCollector)
		newRetCollectorCount := retCollector.Base().Count
		printAnalyzeMergeCollectorLog(oldRetCollectorCount, newRetCollectorCount, subCollector.Base().Count,
			e.tableID.TableID, e.tableID.PartitionID, e.TableID.IsPartitionTable(),
			"merge subCollector in concurrency in AnalyzeColumnsExecV2", index)

		// Consume the memory of the result.
		newRetCollectorSize := retCollector.Base().MemSize
		subCollectorSize := subCollector.Base().MemSize
		e.memTracker.Consume(newRetCollectorSize - oldRetCollectorSize - subCollectorSize)
		e.memTracker.Release(dataSize + colRespSize)
		subCollector.DestroyAndPutToPool()
	}

	resultCh <- &samplingMergeResult{collector: retCollector}
}

func (e *AnalyzeColumnsExecV2) subBuildWorker(resultCh chan error, taskCh chan *samplingBuildTask, hists []*statistics.Histogram, topns []*statistics.TopN, collectors []*statistics.SampleCollector, exitCh chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- getAnalyzePanicErr(r)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingBuildWorkerPanic", func() {
		panic("failpoint triggered")
	})

	colLen := len(e.colsInfo)
	bufferedMemSize := int64(0)
	bufferedReleaseSize := int64(0)
	defer e.memTracker.Consume(bufferedMemSize)
	defer e.memTracker.Release(bufferedReleaseSize)

workLoop:
	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				break workLoop
			}
			var collector *statistics.SampleCollector
			if task.isColumn {
				if e.colsInfo[task.slicePos].IsGenerated() && !e.colsInfo[task.slicePos].GeneratedStored {
					hists[task.slicePos] = nil
					topns[task.slicePos] = nil
					continue
				}
				sampleNum := task.rootRowCollector.Base().Samples.Len()
				sampleItems := make([]*statistics.SampleItem, 0, sampleNum)
				// consume mandatory memory at the beginning, including empty SampleItems of all sample rows, if exceeds, fast fail
				collectorMemSize := int64(sampleNum) * (8 + statistics.EmptySampleItemSize)
				e.memTracker.Consume(collectorMemSize)
				var collator collate.Collator
				ft := e.colsInfo[task.slicePos].FieldType
				// When it's new collation data, we need to use its collate key instead of original value because only
				// the collate key can ensure the correct ordering.
				// This is also corresponding to similar operation in (*statistics.Column).GetColumnRowCount().
				if ft.EvalType() == types.ETString && ft.GetType() != mysql.TypeEnum && ft.GetType() != mysql.TypeSet {
					collator = collate.GetCollator(ft.GetCollate())
				}
				for j, row := range task.rootRowCollector.Base().Samples {
					if row.Columns[task.slicePos].IsNull() {
						continue
					}
					val := row.Columns[task.slicePos]
					// If this value is very big, we think that it is not a value that can occur many times. So we don't record it.
					if len(val.GetBytes()) > statistics.MaxSampleValueLength {
						continue
					}
					if collator != nil {
						val.SetBytes(collator.Key(val.GetString()))
						deltaSize := int64(cap(val.GetBytes()))
						collectorMemSize += deltaSize
						e.memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
					}
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value:   val,
						Ordinal: j,
					})
					// tmp memory usage
					deltaSize := val.MemUsage() + 4 // content of SampleItem is copied
					e.memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
					e.memTracker.BufferedRelease(&bufferedReleaseSize, deltaSize)
				}
				collector = &statistics.SampleCollector{
					Samples:   sampleItems,
					NullCount: task.rootRowCollector.Base().NullCount[task.slicePos],
					Count:     task.rootRowCollector.Base().Count - task.rootRowCollector.Base().NullCount[task.slicePos],
					FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
					TotalSize: task.rootRowCollector.Base().TotalSizes[task.slicePos],
					MemSize:   collectorMemSize,
				}
			} else {
				var tmpDatum types.Datum
				var err error
				idx := e.indexes[task.slicePos-colLen]
				sampleNum := task.rootRowCollector.Base().Samples.Len()
				sampleItems := make([]*statistics.SampleItem, 0, sampleNum)
				// consume mandatory memory at the beginning, including all SampleItems, if exceeds, fast fail
				// 8 is size of reference, 8 is the size of "b := make([]byte, 0, 8)"
				collectorMemSize := int64(sampleNum) * (8 + statistics.EmptySampleItemSize + 8)
				e.memTracker.Consume(collectorMemSize)
				errCtx := e.ctx.GetSessionVars().StmtCtx.ErrCtx()
			indexSampleCollectLoop:
				for _, row := range task.rootRowCollector.Base().Samples {
					if len(idx.Columns) == 1 && row.Columns[idx.Columns[0].Offset].IsNull() {
						continue
					}
					b := make([]byte, 0, 8)
					for _, col := range idx.Columns {
						// If the index value contains one value which is too long, we think that it's a value that doesn't occur many times.
						if len(row.Columns[col.Offset].GetBytes()) > statistics.MaxSampleValueLength {
							continue indexSampleCollectLoop
						}
						if col.Length != types.UnspecifiedLength {
							row.Columns[col.Offset].Copy(&tmpDatum)
							ranger.CutDatumByPrefixLen(&tmpDatum, col.Length, &e.colsInfo[col.Offset].FieldType)
							b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx.TimeZone(), b, tmpDatum)
							err = errCtx.HandleError(err)
							if err != nil {
								resultCh <- err
								continue workLoop
							}
							continue
						}
						b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx.TimeZone(), b, row.Columns[col.Offset])
						err = errCtx.HandleError(err)
						if err != nil {
							resultCh <- err
							continue workLoop
						}
					}
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value: types.NewBytesDatum(b),
					})
					// tmp memory usage
					deltaSize := sampleItems[len(sampleItems)-1].Value.MemUsage()
					e.memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
					e.memTracker.BufferedRelease(&bufferedReleaseSize, deltaSize)
				}
				collector = &statistics.SampleCollector{
					Samples:   sampleItems,
					NullCount: task.rootRowCollector.Base().NullCount[task.slicePos],
					Count:     task.rootRowCollector.Base().Count - task.rootRowCollector.Base().NullCount[task.slicePos],
					FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
					TotalSize: task.rootRowCollector.Base().TotalSizes[task.slicePos],
					MemSize:   collectorMemSize,
				}
			}
			if task.isColumn {
				collectors[task.slicePos] = collector
			}
			releaseCollectorMemory := func() {
				if !task.isColumn {
					e.memTracker.Release(collector.MemSize)
				}
			}
			hist, topn, err := statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), task.id, collector, task.tp, task.isColumn, e.memTracker, e.ctx.GetSessionVars().EnableExtendedStats)
			if err != nil {
				resultCh <- err
				releaseCollectorMemory()
				continue
			}
			finalMemSize := hist.MemoryUsage() + topn.MemoryUsage()
			e.memTracker.Consume(finalMemSize)
			hists[task.slicePos] = hist
			topns[task.slicePos] = topn
			resultCh <- nil
			releaseCollectorMemory()
		case <-exitCh:
			return
		}
	}
}

type analyzeIndexNDVTotalResult struct {
	results map[int64]*statistics.AnalyzeResults
	err     error
}

type samplingMergeResult struct {
	collector statistics.RowSampleCollector
	err       error
}

type samplingBuildTask struct {
	id               int64
	rootRowCollector statistics.RowSampleCollector
	tp               *types.FieldType
	isColumn         bool
	slicePos         int
}

func readDataAndSendTask(ctx sessionctx.Context, handler *tableResultHandler, mergeTaskCh chan []byte, memTracker *memory.Tracker) error {
	// After all tasks are sent, close the mergeTaskCh to notify the mergeWorker that all tasks have been sent.
	defer close(mergeTaskCh)
	for {
		failpoint.Inject("mockKillRunningV2AnalyzeJob", func() {
			dom := domain.GetDomain(ctx)
			dom.SysProcTracker().KillSysProcess(dom.GetAutoAnalyzeProcID())
		})
		if err := ctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return err
		}
		failpoint.Inject("mockSlowAnalyzeV2", func() {
			time.Sleep(1000 * time.Second)
		})

		data, err := handler.nextRaw(context.TODO())
		if err != nil {
			return errors.Trace(err)
		}
		if data == nil {
			break
		}

		memTracker.Consume(int64(cap(data)))
		mergeTaskCh <- data
	}

	return nil
}
