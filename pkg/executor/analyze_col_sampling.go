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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tiancaiamao/gp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

const analyzeFullSamplingTraceLog = "analyze full sampling trace"

type analyzeFullSamplingTraceSummary struct {
	marshalRequests             atomic.Int64
	marshalFailures             atomic.Int64
	marshalElapsedNanos         atomic.Int64
	marshalRequestBytes         atomic.Int64
	buildKeyRangeRequests       atomic.Int64
	buildKeyRangeElapsedNanos   atomic.Int64
	buildKeyRangeLogicalRanges  atomic.Int64
	buildAnalyzeRequests        atomic.Int64
	buildAnalyzeFailures        atomic.Int64
	buildAnalyzeElapsedNanos    atomic.Int64
	buildAnalyzeRequestBytes    atomic.Int64
	buildAnalyzePhysicalRanges  atomic.Int64
	buildAnalyzeStoreBatchSize  atomic.Int64
	buildAnalyzeConcurrency     atomic.Int64
	dispatchRequests            atomic.Int64
	dispatchFailures            atomic.Int64
	dispatchElapsedNanos        atomic.Int64
	distsqlSendRequests         atomic.Int64
	distsqlSendFailures         atomic.Int64
	distsqlSendElapsedNanos     atomic.Int64
	distsqlSendRequestBytes     atomic.Int64
	distsqlSendStoreBatchSize   atomic.Int64
	distsqlSendConcurrency      atomic.Int64
	copBuildTaskRequests        atomic.Int64
	copBuildTaskElapsedNanos    atomic.Int64
	copBuildTaskInputRanges     atomic.Int64
	copBuildTaskPhysicalRegions atomic.Int64
	copBuildTaskTopLevelTasks   atomic.Int64
	copBuildTaskBatchedSubTasks atomic.Int64
	copBuildTaskStoreBatchSize  atomic.Int64
	copSendRequests             atomic.Int64
	copSendFailures             atomic.Int64
	copSendElapsedNanos         atomic.Int64
	copSendResponseBytes        atomic.Int64
	copSendBatchResponses       atomic.Int64
	copSendBatchedSubTasks      atomic.Int64
	copSplitRequests            atomic.Int64
	copSplitFailures            atomic.Int64
	copSplitElapsedNanos        atomic.Int64
	copSplitExpectedTasks       atomic.Int64
	copSplitBatchResponses      atomic.Int64
	copSplitResultResponses     atomic.Int64
	copSplitRemainTasks         atomic.Int64
}

// RecordMarshalAnalyzeReq records AnalyzeReq marshaling cost.
func (s *analyzeFullSamplingTraceSummary) RecordMarshalAnalyzeReq(elapsed time.Duration, requestBytes int, success bool) {
	s.marshalRequests.Add(1)
	if !success {
		s.marshalFailures.Add(1)
	}
	s.marshalElapsedNanos.Add(elapsed.Nanoseconds())
	s.marshalRequestBytes.Add(int64(requestBytes))
}

func (s *analyzeFullSamplingTraceSummary) recordBuildKeyRanges(elapsed time.Duration, logicalRanges int) {
	s.buildKeyRangeRequests.Add(1)
	s.buildKeyRangeElapsedNanos.Add(elapsed.Nanoseconds())
	s.buildKeyRangeLogicalRanges.Add(int64(logicalRanges))
}

func (s *analyzeFullSamplingTraceSummary) recordBuildAnalyzeRequest(elapsed time.Duration, requestBytes, physicalKeyRanges, storeBatchSize, concurrency int, success bool) {
	s.buildAnalyzeRequests.Add(1)
	if !success {
		s.buildAnalyzeFailures.Add(1)
	}
	s.buildAnalyzeElapsedNanos.Add(elapsed.Nanoseconds())
	s.buildAnalyzeRequestBytes.Add(int64(requestBytes))
	s.buildAnalyzePhysicalRanges.Add(int64(physicalKeyRanges))
	s.buildAnalyzeStoreBatchSize.Store(int64(storeBatchSize))
	s.buildAnalyzeConcurrency.Store(int64(concurrency))
}

func (s *analyzeFullSamplingTraceSummary) recordDispatchAnalyzeRequest(elapsed time.Duration, success bool) {
	s.dispatchRequests.Add(1)
	if !success {
		s.dispatchFailures.Add(1)
	}
	s.dispatchElapsedNanos.Add(elapsed.Nanoseconds())
}

// RecordDistSQLAnalyzeSend records the cost of opening the DistSQL analyze response.
func (s *analyzeFullSamplingTraceSummary) RecordDistSQLAnalyzeSend(elapsed time.Duration, requestBytes, storeBatchSize, concurrency int, success bool) {
	s.distsqlSendRequests.Add(1)
	if !success {
		s.distsqlSendFailures.Add(1)
	}
	s.distsqlSendElapsedNanos.Add(elapsed.Nanoseconds())
	s.distsqlSendRequestBytes.Add(int64(requestBytes))
	s.distsqlSendStoreBatchSize.Store(int64(storeBatchSize))
	s.distsqlSendConcurrency.Store(int64(concurrency))
}

// RecordBuildCopTasks records coprocessor task construction for the statement.
func (s *analyzeFullSamplingTraceSummary) RecordBuildCopTasks(elapsed time.Duration, inputRangeCount, physicalRegionTasks, topLevelTasks, batchedSubTasks, storeBatchSize int) {
	s.copBuildTaskRequests.Add(1)
	s.copBuildTaskElapsedNanos.Add(elapsed.Nanoseconds())
	s.copBuildTaskInputRanges.Add(int64(inputRangeCount))
	s.copBuildTaskPhysicalRegions.Add(int64(physicalRegionTasks))
	s.copBuildTaskTopLevelTasks.Add(int64(topLevelTasks))
	s.copBuildTaskBatchedSubTasks.Add(int64(batchedSubTasks))
	s.copBuildTaskStoreBatchSize.Store(int64(storeBatchSize))
}

// RecordCopSendRequest records one TiKV coprocessor send attempt.
func (s *analyzeFullSamplingTraceSummary) RecordCopSendRequest(elapsed time.Duration, responseBytes, batchResponses, batchedSubTasks int, success bool) {
	s.copSendRequests.Add(1)
	if !success {
		s.copSendFailures.Add(1)
	}
	s.copSendElapsedNanos.Add(elapsed.Nanoseconds())
	s.copSendResponseBytes.Add(int64(responseBytes))
	s.copSendBatchResponses.Add(int64(batchResponses))
	s.copSendBatchedSubTasks.Add(int64(batchedSubTasks))
}

// RecordCopSplitBatchResponse records splitting one store-batched response.
func (s *analyzeFullSamplingTraceSummary) RecordCopSplitBatchResponse(elapsed time.Duration, expectedTasks, batchResponses, resultResponses, remainTasks int, success bool) {
	s.copSplitRequests.Add(1)
	if !success {
		s.copSplitFailures.Add(1)
	}
	s.copSplitElapsedNanos.Add(elapsed.Nanoseconds())
	s.copSplitExpectedTasks.Add(int64(expectedTasks))
	s.copSplitBatchResponses.Add(int64(batchResponses))
	s.copSplitResultResponses.Add(int64(resultResponses))
	s.copSplitRemainTasks.Add(int64(remainTasks))
}

func (s *analyzeFullSamplingTraceSummary) log(e *AnalyzeColumnsExec) {
	if s == nil {
		return
	}
	e.logAnalyzeFullSamplingTrace("tidb.marshal_analyze_req", "summary",
		zap.Int64("requests", s.marshalRequests.Load()),
		zap.Int64("failures", s.marshalFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.marshalElapsedNanos.Load())),
		zap.Int64("requestBytes", s.marshalRequestBytes.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.build_key_ranges", "summary",
		zap.Int64("requests", s.buildKeyRangeRequests.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.buildKeyRangeElapsedNanos.Load())),
		zap.Int64("logicalRanges", s.buildKeyRangeLogicalRanges.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.build_analyze_request", "summary",
		zap.Int64("requests", s.buildAnalyzeRequests.Load()),
		zap.Int64("failures", s.buildAnalyzeFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.buildAnalyzeElapsedNanos.Load())),
		zap.Int64("requestBytes", s.buildAnalyzeRequestBytes.Load()),
		zap.Int64("physicalKeyRanges", s.buildAnalyzePhysicalRanges.Load()),
		zap.Int64("storeBatchSize", s.buildAnalyzeStoreBatchSize.Load()),
		zap.Int64("concurrency", s.buildAnalyzeConcurrency.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.dispatch_analyze_request", "summary",
		zap.Int64("requests", s.dispatchRequests.Load()),
		zap.Int64("failures", s.dispatchFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.dispatchElapsedNanos.Load())))
	e.logAnalyzeFullSamplingTrace("tidb.distsql_analyze_send", "summary",
		zap.Int64("requests", s.distsqlSendRequests.Load()),
		zap.Int64("failures", s.distsqlSendFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.distsqlSendElapsedNanos.Load())),
		zap.Int64("requestBytes", s.distsqlSendRequestBytes.Load()),
		zap.Int64("storeBatchSize", s.distsqlSendStoreBatchSize.Load()),
		zap.Int64("concurrency", s.distsqlSendConcurrency.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.copr.build_tasks", "summary",
		zap.Int64("requests", s.copBuildTaskRequests.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.copBuildTaskElapsedNanos.Load())),
		zap.Int64("inputRangeCount", s.copBuildTaskInputRanges.Load()),
		zap.Int64("physicalRegionTasks", s.copBuildTaskPhysicalRegions.Load()),
		zap.Int64("topLevelTasks", s.copBuildTaskTopLevelTasks.Load()),
		zap.Int64("batchedSubTasks", s.copBuildTaskBatchedSubTasks.Load()),
		zap.Int64("storeBatchSize", s.copBuildTaskStoreBatchSize.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.copr.send_request", "summary",
		zap.Int64("requests", s.copSendRequests.Load()),
		zap.Int64("failures", s.copSendFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.copSendElapsedNanos.Load())),
		zap.Int64("responseBytes", s.copSendResponseBytes.Load()),
		zap.Int64("batchResponses", s.copSendBatchResponses.Load()),
		zap.Int64("batchedSubTasks", s.copSendBatchedSubTasks.Load()))
	e.logAnalyzeFullSamplingTrace("tidb.copr.split_batch_response", "summary",
		zap.Int64("requests", s.copSplitRequests.Load()),
		zap.Int64("failures", s.copSplitFailures.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.copSplitElapsedNanos.Load())),
		zap.Int64("expectedBatchedTasks", s.copSplitExpectedTasks.Load()),
		zap.Int64("batchResponses", s.copSplitBatchResponses.Load()),
		zap.Int64("resultResponses", s.copSplitResultResponses.Load()),
		zap.Int64("remainTasks", s.copSplitRemainTasks.Load()))
}

type samplingProtoTraceSummary struct {
	responseCount              atomic.Int64
	responseBytes              atomic.Int64
	rowCount                   atomic.Int64
	sampleCount                atomic.Int64
	fmSketchCount              atomic.Int64
	unmarshalElapsedNanos      atomic.Int64
	fromProtoElapsedNanos      atomic.Int64
	mergeCollectorElapsedNanos atomic.Int64
}

type samplingBuildTraceSummary struct {
	taskCount       atomic.Int64
	failureCount    atomic.Int64
	elapsedNanos    atomic.Int64
	sampleItems     atomic.Int64
	histMemoryBytes atomic.Int64
	topNMemoryBytes atomic.Int64
}

type samplingReadTraceSummary struct {
	fetches             int
	dataResponses       int
	enqueuedResponses   int
	responseBytes       int64
	fetchElapsedTotal   time.Duration
	enqueueElapsedTotal time.Duration
}

func (s *samplingReadTraceSummary) log(e *AnalyzeColumnsExec, success bool, err error) {
	e.logAnalyzeFullSamplingTrace("tidb.fetch_raw_response", "summary",
		zap.Int("fetches", s.fetches),
		zap.Int("dataResponses", s.dataResponses),
		zap.Int64("responseBytes", s.responseBytes),
		zap.Duration("fetchElapsedTotal", s.fetchElapsedTotal),
		zap.Bool("success", success),
		zap.Error(err))
	e.logAnalyzeFullSamplingTrace("tidb.enqueue_merge_task", "summary",
		zap.Int("enqueuedResponses", s.enqueuedResponses),
		zap.Int64("responseBytes", s.responseBytes),
		zap.Duration("enqueueElapsedTotal", s.enqueueElapsedTotal),
		zap.Bool("success", success),
		zap.Error(err))
}

func (s *samplingBuildTraceSummary) record(elapsed time.Duration, sampleItems int, histMemoryBytes int64, topNMemoryBytes int64, success bool) {
	s.taskCount.Add(1)
	if !success {
		s.failureCount.Add(1)
	}
	s.elapsedNanos.Add(elapsed.Nanoseconds())
	s.sampleItems.Add(int64(sampleItems))
	s.histMemoryBytes.Add(histMemoryBytes)
	s.topNMemoryBytes.Add(topNMemoryBytes)
}

func (s *samplingBuildTraceSummary) log(e *AnalyzeColumnsExec) {
	e.logAnalyzeFullSamplingTrace("tidb.build_hist_topn", "summary",
		zap.Int64("tasks", s.taskCount.Load()),
		zap.Int64("failures", s.failureCount.Load()),
		zap.Duration("elapsedTotal", time.Duration(s.elapsedNanos.Load())),
		zap.Int64("sampleItems", s.sampleItems.Load()),
		zap.Int64("histMemoryBytes", s.histMemoryBytes.Load()),
		zap.Int64("topNMemoryBytes", s.topNMemoryBytes.Load()))
}

func (s *samplingProtoTraceSummary) recordUnmarshal(elapsed time.Duration, responseBytes int, rowCount int64, sampleCount int, fmSketchCount int) {
	s.responseCount.Add(1)
	s.responseBytes.Add(int64(responseBytes))
	s.rowCount.Add(rowCount)
	s.sampleCount.Add(int64(sampleCount))
	s.fmSketchCount.Add(int64(fmSketchCount))
	s.unmarshalElapsedNanos.Add(elapsed.Nanoseconds())
}

func (s *samplingProtoTraceSummary) recordFromProto(elapsed time.Duration) {
	s.fromProtoElapsedNanos.Add(elapsed.Nanoseconds())
}

func (s *samplingProtoTraceSummary) recordMergeCollector(elapsed time.Duration) {
	s.mergeCollectorElapsedNanos.Add(elapsed.Nanoseconds())
}

func (s *samplingProtoTraceSummary) log(e *AnalyzeColumnsExec) {
	baseFields := []zap.Field{
		zap.Int64("protoResponses", s.responseCount.Load()),
		zap.Int64("responseBytes", s.responseBytes.Load()),
		zap.Int64("rowCount", s.rowCount.Load()),
		zap.Int64("sampleCount", s.sampleCount.Load()),
		zap.Int64("fmSketches", s.fmSketchCount.Load()),
	}
	e.logAnalyzeFullSamplingTrace("tidb.unmarshal_sampling_response", "summary",
		append(baseFields, zap.Duration("elapsedTotal", time.Duration(s.unmarshalElapsedNanos.Load())))...)
	e.logAnalyzeFullSamplingTrace("tidb.from_proto_sampling_collector", "summary",
		append(baseFields, zap.Duration("elapsedTotal", time.Duration(s.fromProtoElapsedNanos.Load())))...)
	e.logAnalyzeFullSamplingTrace("tidb.merge_sampling_collector", "summary",
		append(baseFields, zap.Duration("elapsedTotal", time.Duration(s.mergeCollectorElapsedNanos.Load())))...)
}

func (e *AnalyzeColumnsExec) logAnalyzeFullSamplingTrace(phase string, event string, fields ...zap.Field) {
	baseFields := []zap.Field{
		zap.String("component", "tidb"),
		zap.String("phase", phase),
		zap.String("event", event),
		zap.Int64("tableID", e.tableID.TableID),
		zap.Int64("partitionID", e.tableID.PartitionID),
		zap.Bool("partitionTable", e.tableID.IsPartitionTable()),
		zap.Uint64("connID", e.ctx.GetSessionVars().ConnectionID),
		zap.Uint64("snapshot", e.snapshot),
	}
	logutil.BgLogger().Info(analyzeFullSamplingTraceLog, append(baseFields, fields...)...)
}

func (e *AnalyzeColumnsExec) logAnalyzeFullSamplingTraceDebug(phase string, event string, fields ...zap.Field) {
	logger := logutil.BgLogger()
	if !logger.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	baseFields := []zap.Field{
		zap.String("component", "tidb"),
		zap.String("phase", phase),
		zap.String("event", event),
		zap.Int64("tableID", e.tableID.TableID),
		zap.Int64("partitionID", e.tableID.PartitionID),
		zap.Bool("partitionTable", e.tableID.IsPartitionTable()),
		zap.Uint64("connID", e.ctx.GetSessionVars().ConnectionID),
		zap.Uint64("snapshot", e.snapshot),
	}
	logger.Debug(analyzeFullSamplingTraceLog, append(baseFields, fields...)...)
}

func analyzeFullSamplingTraceDebugEnabled() bool {
	return logutil.BgLogger().Core().Enabled(zapcore.DebugLevel)
}

func (e *AnalyzeColumnsExec) analyzeColumnsPushDown(ctx context.Context, gp *gp.Pool) *statistics.AnalyzeResults {
	analyzeStart := time.Now()
	e.fullSamplingTraceSummary = &analyzeFullSamplingTraceSummary{}
	e.logAnalyzeFullSamplingTrace("tidb.analyze_columns_push_down", "start",
		zap.Int("columns", len(e.colsInfo)),
		zap.Int("indexes", len(e.indexes)))
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

	// specialIndexes holds indexes that include virtual or prefix columns. For these indexes,
	// only the number of distinct values (NDV) is computed using TiKV. Other statistic
	// are derived from sample data processed within TiDB.
	// The reason is that we want to keep the same row sampling for all columns.
	specialIndexes := make([]*model.IndexInfo, 0, len(e.indexes))
	specialIndexesOffsets := make([]int, 0, len(e.indexes))
	for i, idx := range e.indexes {
		isSpecial := false
		for _, col := range idx.Columns {
			colInfo := e.colsInfo[col.Offset]
			isPrefixCol := col.Length != types.UnspecifiedLength
			if colInfo.IsVirtualGenerated() || isPrefixCol {
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
		e.logAnalyzeFullSamplingTrace("tidb.analyze_columns_push_down", "finish",
			zap.Duration("elapsed", time.Since(analyzeStart)),
			zap.Bool("success", false),
			zap.Error(err))
		return &statistics.AnalyzeResults{Err: err, Job: e.job}
	}
	idxNDVPushDownCh := make(chan analyzeIndexNDVTotalResult, 1)
	e.handleNDVForSpecialIndexes(ctx, specialIndexes, idxNDVPushDownCh, samplingStatsConcurrency)
	count, hists, topNs, fmSketches, err := e.buildSamplingStats(ctx, gp, ranges, specialIndexesOffsets, idxNDVPushDownCh, samplingStatsConcurrency)
	if err != nil {
		e.memTracker.Release(e.memTracker.BytesConsumed())
		e.logAnalyzeFullSamplingTrace("tidb.analyze_columns_push_down", "finish",
			zap.Duration("elapsed", time.Since(analyzeStart)),
			zap.Bool("success", false),
			zap.Error(err))
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

	e.logAnalyzeFullSamplingTrace("tidb.analyze_columns_push_down", "finish",
		zap.Duration("elapsed", time.Since(analyzeStart)),
		zap.Bool("success", true),
		zap.Int64("rowCount", count))
	return &statistics.AnalyzeResults{
		TableID:       e.tableID,
		Ars:           []*statistics.AnalyzeResult{colResult, colGroupResult},
		Job:           e.job,
		StatsVer:      e.StatsVersion,
		Count:         count,
		Snapshot:      e.snapshot,
		BaseCount:     e.baseCount,
		BaseModifyCnt: e.baseModifyCnt,
	}
}

// decodeSampleDataWithVirtualColumn constructs the virtual column by evaluating from the decoded normal columns.
func (e *AnalyzeColumnsExec) decodeSampleDataWithVirtualColumn(
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
			// Virtual columns will be decoded as null first.
			_, err := decoder.DecodeOne(columns.GetBytes(), i, e.schemaForVirtualColEval.Columns[i].RetType)
			if err != nil {
				return err
			}
		}
	}
	intest.AssertFunc(func() bool {
		// Ensure all columns in the chunk have the same number of rows.
		// Checking for virtual columns.
		for i := 1; i < chk.NumCols(); i++ {
			if chk.Column(i).Rows() != chk.Column(0).Rows() {
				return false
			}
		}
		return true
	}, "all columns in chunk should have the same number of rows")
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

func (e *AnalyzeColumnsExec) buildSamplingStats(
	ctx context.Context,
	gp *gp.Pool,
	ranges []*ranger.Range,
	indexesWithVirtualColOffsets []int,
	idxNDVPushDownCh chan analyzeIndexNDVTotalResult,
	samplingStatsConcurrency int,
) (
	count int64,
	hists []*statistics.Histogram,
	topns []*statistics.TopN,
	fmSketches []*statistics.FMSketch,
	err error,
) {
	buildStart := time.Now()
	e.logAnalyzeFullSamplingTrace("tidb.build_sampling_stats", "start",
		zap.Int("logicalRanges", len(ranges)),
		zap.Int("indexesWithVirtualOrPrefixColumns", len(indexesWithVirtualColOffsets)),
		zap.Int("samplingStatsConcurrency", samplingStatsConcurrency))
	// Open memory tracker and resultHandler.
	openStart := time.Now()
	if err = e.open(ctx, ranges); err != nil {
		e.logAnalyzeFullSamplingTrace("tidb.open_result_handler", "finish",
			zap.Duration("elapsed", time.Since(openStart)),
			zap.Bool("success", false),
			zap.Error(err))
		e.fullSamplingTraceSummary.log(e)
		return 0, nil, nil, nil, err
	}
	e.logAnalyzeFullSamplingTrace("tidb.open_result_handler", "finish",
		zap.Duration("elapsed", time.Since(openStart)),
		zap.Bool("success", true))
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			err = err1
		}
	}()

	totalLen := len(e.analyzePB.ColReq.ColumnsInfo) + len(e.analyzePB.ColReq.ColumnGroups)
	rootRowCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), totalLen)
	for range totalLen {
		rootRowCollector.Base().FMSketches = append(rootRowCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	}

	sc := e.ctx.GetSessionVars().StmtCtx

	// Start workers to merge the result from collectors.
	mergeResultCh := make(chan *samplingMergeResult, 1)
	mergeTaskCh := make(chan []byte, 1)
	protoTraceSummary := &samplingProtoTraceSummary{}
	readTraceSummary := &samplingReadTraceSummary{}
	taskCtx, taskCancel := context.WithCancelCause(ctx)
	defer taskCancel(nil)
	var taskEg errgroup.Group
	// Start read data from resultHandler and send them to mergeTaskCh.
	taskEg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = getAnalyzePanicErr(r)
			}
		}()
		err = readDataAndSendTask(taskCtx, e.ctx, e.resultHandler, mergeTaskCh, e.memTracker, readTraceSummary)
		if err != nil {
			taskCancel(err)
		}
		return err
	})
	e.samplingMergeWg = &util.WaitGroupWrapper{}
	e.samplingMergeWg.Add(samplingStatsConcurrency)
	mergeWorkerPanicCnt := 0
	mergeEg, mergeCtx := errgroup.WithContext(taskCtx)
	for i := range samplingStatsConcurrency {
		id := i
		gp.Go(func() {
			e.subMergeWorker(mergeCtx, taskCancel, mergeResultCh, mergeTaskCh, totalLen, id, protoTraceSummary)
		})
	}
	// Merge the result from collectors.
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
				"merge subMergeWorker in AnalyzeColumnsExec", -1)
			e.memTracker.Consume(rootRowCollector.Base().MemSize - oldRootCollectorSize - mergeResult.collector.Base().MemSize)
			mergeResult.collector.DestroyAndPutToPool()
		}
		return err
	})
	mergeStart := time.Now()
	err = taskEg.Wait()
	if err != nil {
		if err1 := mergeEg.Wait(); err1 != nil {
			if !stderrors.Is(err1, err) && err1.Error() != err.Error() {
				err = stderrors.Join(err, err1)
			}
		}
		drainPendingSamplingMergeTasks(mergeTaskCh, e.memTracker)
		e.fullSamplingTraceSummary.log(e)
		readTraceSummary.log(e, false, err)
		protoTraceSummary.log(e)
		e.logAnalyzeFullSamplingTrace("tidb.merge_sampling_response", "finish",
			zap.Duration("elapsed", time.Since(mergeStart)),
			zap.Bool("success", false),
			zap.Error(err))
		return 0, nil, nil, nil, getAnalyzePanicErr(err)
	}
	err = mergeEg.Wait()
	drainPendingSamplingMergeTasks(mergeTaskCh, e.memTracker)
	e.fullSamplingTraceSummary.log(e)
	readTraceSummary.log(e, err == nil, err)
	protoTraceSummary.log(e)
	defer e.memTracker.Release(rootRowCollector.Base().MemSize)
	if err != nil {
		taskCancel(err)
		e.logAnalyzeFullSamplingTrace("tidb.merge_sampling_response", "finish",
			zap.Duration("elapsed", time.Since(mergeStart)),
			zap.Bool("success", false),
			zap.Error(err))
		return 0, nil, nil, nil, err
	}
	e.logAnalyzeFullSamplingTrace("tidb.merge_sampling_response", "finish",
		zap.Duration("elapsed", time.Since(mergeStart)),
		zap.Bool("success", true),
		zap.Int64("rowCount", rootRowCollector.Base().Count),
		zap.Int("sampleCount", rootRowCollector.Base().Samples.Len()))

	// Decode the data from sample collectors.
	decodeStart := time.Now()
	virtualColIdx := buildVirtualColumnIndex(e.schemaForVirtualColEval, e.colsInfo)
	// Filling virtual columns is necessary here because these samples are used to build statistics for indexes that constructed by virtual columns.
	if len(virtualColIdx) > 0 {
		fieldTps := make([]*types.FieldType, 0, len(virtualColIdx))
		for _, colOffset := range virtualColIdx {
			fieldTps = append(fieldTps, e.schemaForVirtualColEval.Columns[colOffset].RetType)
		}
		err = e.decodeSampleDataWithVirtualColumn(rootRowCollector, fieldTps, virtualColIdx, e.schemaForVirtualColEval)
		if err != nil {
			e.logAnalyzeFullSamplingTrace("tidb.decode_samples", "finish",
				zap.Duration("elapsed", time.Since(decodeStart)),
				zap.Bool("success", false),
				zap.Int("virtualColumns", len(virtualColIdx)),
				zap.Error(err))
			return 0, nil, nil, nil, err
		}
	} else {
		// If there's no virtual column, normal decode way is enough.
		for _, sample := range rootRowCollector.Base().Samples {
			for i := range sample.Columns {
				sample.Columns[i], err = tablecodec.DecodeColumnValue(sample.Columns[i].GetBytes(), &e.colsInfo[i].FieldType, sc.TimeZone())
				if err != nil {
					e.logAnalyzeFullSamplingTrace("tidb.decode_samples", "finish",
						zap.Duration("elapsed", time.Since(decodeStart)),
						zap.Bool("success", false),
						zap.Int("virtualColumns", 0),
						zap.Error(err))
					return 0, nil, nil, nil, err
				}
			}
		}
	}
	e.logAnalyzeFullSamplingTrace("tidb.decode_samples", "finish",
		zap.Duration("elapsed", time.Since(decodeStart)),
		zap.Bool("success", true),
		zap.Int("virtualColumns", len(virtualColIdx)),
		zap.Int("sampleCount", rootRowCollector.Base().Samples.Len()))

	// Calculate handle from the row data for each row. It will be used to sort the samples.
	sortStart := time.Now()
	for _, sample := range rootRowCollector.Base().Samples {
		sample.Handle, err = e.handleCols.BuildHandleByDatums(sc, sample.Columns)
		if err != nil {
			e.logAnalyzeFullSamplingTrace("tidb.sort_samples", "finish",
				zap.Duration("elapsed", time.Since(sortStart)),
				zap.Bool("success", false),
				zap.Error(err))
			return 0, nil, nil, nil, err
		}
	}
	colLen := len(e.colsInfo)
	// The order of the samples are broken when merging samples from sub-collectors.
	// So now we need to sort the samples according to the handle in order to calculate correlation.
	slices.SortFunc(rootRowCollector.Base().Samples, func(i, j *statistics.ReservoirRowSampleItem) int {
		return i.Handle.Compare(j.Handle)
	})
	e.logAnalyzeFullSamplingTrace("tidb.sort_samples", "finish",
		zap.Duration("elapsed", time.Since(sortStart)),
		zap.Bool("success", true),
		zap.Int("sampleCount", rootRowCollector.Base().Samples.Len()))

	hists = make([]*statistics.Histogram, totalLen)
	topns = make([]*statistics.TopN, totalLen)
	fmSketches = make([]*statistics.FMSketch, 0, totalLen)
	buildTraceSummary := &samplingBuildTraceSummary{}
	buildResultChan := make(chan error, totalLen+samplingStatsConcurrency)
	buildTaskChan := make(chan *samplingBuildTask, totalLen)
	if totalLen < samplingStatsConcurrency {
		samplingStatsConcurrency = totalLen
	}
	e.samplingBuilderWg = newNotifyErrorWaitGroupWrapper(gp, buildResultChan)
	exitCh := make(chan struct{})
	e.samplingBuilderWg.Add(samplingStatsConcurrency)

	// Start workers to build stats.
	for range samplingStatsConcurrency {
		e.samplingBuilderWg.Run(func() {
			e.subBuildWorker(ctx, buildResultChan, buildTaskChan, hists, topns, exitCh, buildTraceSummary)
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
		channel.Clear(buildResultChan)
		e.logAnalyzeFullSamplingTrace("tidb.build_sampling_stats", "finish",
			zap.Duration("elapsed", time.Since(buildStart)),
			zap.Bool("success", false),
			zap.Error(indexPushedDownResult.err))
		return 0, nil, nil, nil, indexPushedDownResult.err
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
	if err != nil {
		buildTraceSummary.log(e)
		e.logAnalyzeFullSamplingTrace("tidb.build_sampling_stats", "finish",
			zap.Duration("elapsed", time.Since(buildStart)),
			zap.Bool("success", false),
			zap.Error(err))
		return 0, nil, nil, nil, err
	}

	count = rootRowCollector.Base().Count
	buildTraceSummary.log(e)
	e.logAnalyzeFullSamplingTrace("tidb.build_sampling_stats", "finish",
		zap.Duration("elapsed", time.Since(buildStart)),
		zap.Bool("success", true),
		zap.Int64("rowCount", count),
		zap.Int("histograms", len(hists)),
		zap.Int("topNs", len(topns)),
		zap.Int("fmSketches", len(fmSketches)))
	return
}

// handleNDVForSpecialIndexes deals with the logic to analyze the index containing the virtual column when the mode is full sampling.
func (e *AnalyzeColumnsExec) handleNDVForSpecialIndexes(ctx context.Context, indexInfos []*model.IndexInfo, totalResultCh chan analyzeIndexNDVTotalResult, samplingStatsConcurrency int) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("analyze ndv for special index panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			totalResultCh <- analyzeIndexNDVTotalResult{
				err: getAnalyzePanicErr(r),
			}
		}
	}()
	tasks := e.buildSubIndexJobForSpecialIndex(ctx, indexInfos)
	taskCh := make(chan *analyzeTask, len(tasks))
	pendingJobs := make(map[uint64]*statistics.AnalyzeJob, len(tasks))
	for _, task := range tasks {
		AddNewAnalyzeJob(e.ctx, task.job)
		if task.job != nil && task.job.ID != nil {
			pendingJobs[*task.job.ID] = task.job
		}
	}
	resultsCh := make(chan *statistics.AnalyzeResults, len(tasks))
	if len(tasks) < samplingStatsConcurrency {
		samplingStatsConcurrency = len(tasks)
	}
	var subIndexWorkerWg = NewAnalyzeResultsNotifyWaitGroupWrapper(resultsCh)
	subIndexWorkerWg.Add(samplingStatsConcurrency)
	for range samplingStatsConcurrency {
		subIndexWorkerWg.Run(func() { e.subIndexWorkerForNDV(ctx, taskCh, resultsCh) })
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
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	for panicCnt < samplingStatsConcurrency {
		results, ok := <-resultsCh
		if !ok {
			break
		}
		if results.Job != nil && results.Job.ID != nil {
			delete(pendingJobs, *results.Job.ID)
		}
		if results.Err != nil {
			err = results.Err
			statsHandle.FinishAnalyzeJob(results.Job, err, statistics.TableAnalysisJob)
			if isAnalyzeWorkerPanic(err) {
				panicCnt++
			}
			continue
		}
		statsHandle.FinishAnalyzeJob(results.Job, nil, statistics.TableAnalysisJob)
		totalResult.results[results.Ars[0].Hist[0].ID] = results
	}
	if err == nil {
		if ctxErr := normalizeCtxErrWithCause(ctx, ctx.Err()); ctxErr != nil {
			err = ctxErr
		}
	}
	if err != nil && len(pendingJobs) > 0 {
		for _, job := range pendingJobs {
			statsHandle.FinishAnalyzeJob(job, err, statistics.TableAnalysisJob)
		}
	}
	if err != nil {
		totalResult.err = err
	}
	totalResultCh <- totalResult
}

// subIndexWorker receive the task for each index and return the result for them.
func (e *AnalyzeColumnsExec) subIndexWorkerForNDV(ctx context.Context, taskCh chan *analyzeTask, resultsCh chan *statistics.AnalyzeResults) {
	var task *analyzeTask
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultsCh <- &statistics.AnalyzeResults{
				Err: getAnalyzePanicErr(r),
				Job: task.job,
			}
		}
	}()
	for {
		var ok bool
		select {
		case task, ok = <-taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
		statsHandle.StartAnalyzeJob(task.job)
		if task.taskType != idxTask {
			resultsCh <- &statistics.AnalyzeResults{
				Err: errors.Errorf("incorrect analyze type"),
				Job: task.job,
			}
			continue
		}
		task.idxExec.job = task.job
		resultsCh <- analyzeIndexNDVPushDown(ctx, task.idxExec)
	}
}

// buildSubIndexJobForSpecialIndex builds sub index pushed down task to calculate the NDV information for indexes containing virtual column.
// This is because we cannot push the calculation of the virtual column down to the tikv side.
func (e *AnalyzeColumnsExec) buildSubIndexJobForSpecialIndex(ctx context.Context, indexInfos []*model.IndexInfo) []*analyzeTask {
	_, offset := timeutil.Zone(e.ctx.GetSessionVars().Location())
	tasks := make([]*analyzeTask, 0, len(indexInfos))
	sc := e.ctx.GetSessionVars().StmtCtx
	concurrency := adaptiveAnlayzeDistSQLConcurrency(ctx, e.ctx)
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
		*statsVersion = int32(e.StatsVersion)
		intest.Assert(*statsVersion == statistics.Version2, "the stats version should be 2 when analyzing index with virtual column")
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

func (e *AnalyzeColumnsExec) subMergeWorker(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	resultCh chan<- *samplingMergeResult,
	taskCh <-chan []byte,
	totalLen int,
	index int,
	protoTraceSummary *samplingProtoTraceSummary,
) {
	// Only close the resultCh in the first worker.
	closeTheResultCh := index == 0
	var inflightDataSize int64
	var inflightRespSize int64
	defer func() {
		if r := recover(); r != nil {
			panicErr := getAnalyzePanicErr(r)
			logutil.BgLogger().Warn("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			cancel(panicErr)
			resultCh <- &samplingMergeResult{err: panicErr}
		}
		if inflightRespSize > 0 {
			e.memTracker.Release(inflightRespSize)
		}
		if inflightDataSize > 0 {
			e.memTracker.Release(inflightDataSize)
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
		for range times {
			e.memTracker.Consume(5 << 20)
			time.Sleep(100 * time.Millisecond)
		}
	})
	// Keep one private collector per merge worker and flush it when taskCh is closed.
	retCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), totalLen)
	for range totalLen {
		retCollector.Base().FMSketches = append(retCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	}
	// Early-return paths need to release the worker-local collector explicitly.
	cleanupCollector := func() {
		e.memTracker.Release(retCollector.Base().MemSize)
		retCollector.DestroyAndPutToPool()
	}
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	// Merge sampled batches until the producer closes taskCh or cancellation arrives.
	for {
		select {
		case data, ok := <-taskCh:
			if !ok {
				resultCh <- &samplingMergeResult{collector: retCollector}
				return
			}
			inflightDataSize = int64(cap(data))
			colResp := &tipb.AnalyzeColumnsResp{}
			unmarshalStart := time.Now()
			err := colResp.Unmarshal(data)
			unmarshalElapsed := time.Since(unmarshalStart)
			rowCollector := colResp.GetRowCollector()
			protoTraceSummary.recordUnmarshal(unmarshalElapsed, len(data), rowCollector.GetCount(),
				len(rowCollector.GetSamples()), len(rowCollector.GetFmSketch()))
			if analyzeFullSamplingTraceDebugEnabled() {
				e.logAnalyzeFullSamplingTraceDebug("tidb.unmarshal_sampling_response", "finish",
					zap.Duration("elapsed", unmarshalElapsed),
					zap.Int("responseBytes", len(data)),
					zap.Int64("rowCount", rowCollector.GetCount()),
					zap.Int("sampleCount", len(rowCollector.GetSamples())),
					zap.Int("fmSketches", len(rowCollector.GetFmSketch())),
					zap.Int("mergeWorker", index),
					zap.Error(err))
			}
			if err != nil {
				e.memTracker.Release(inflightDataSize)
				inflightDataSize = 0
				cleanupCollector()
				resultCh <- &samplingMergeResult{err: err}
				return
			}
			inflightRespSize = int64(colResp.Size())
			e.memTracker.Consume(inflightRespSize)

			subCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), totalLen)
			fromProtoStart := time.Now()
			subCollector.Base().FromProto(colResp.RowCollector, e.memTracker)
			fromProtoElapsed := time.Since(fromProtoStart)
			protoTraceSummary.recordFromProto(fromProtoElapsed)
			if analyzeFullSamplingTraceDebugEnabled() {
				e.logAnalyzeFullSamplingTraceDebug("tidb.from_proto_sampling_collector", "finish",
					zap.Duration("elapsed", fromProtoElapsed),
					zap.Int64("rowCount", subCollector.Base().Count),
					zap.Int("sampleCount", subCollector.Base().Samples.Len()),
					zap.Int("mergeWorker", index))
			}
			statsHandle.UpdateAnalyzeJobProgress(e.job, subCollector.Base().Count)

			oldRetCollectorSize := retCollector.Base().MemSize
			oldRetCollectorCount := retCollector.Base().Count
			mergeStart := time.Now()
			retCollector.MergeCollector(subCollector)
			mergeElapsed := time.Since(mergeStart)
			protoTraceSummary.recordMergeCollector(mergeElapsed)
			newRetCollectorCount := retCollector.Base().Count
			if analyzeFullSamplingTraceDebugEnabled() {
				e.logAnalyzeFullSamplingTraceDebug("tidb.merge_sampling_collector", "finish",
					zap.Duration("elapsed", mergeElapsed),
					zap.Int64("oldRowCount", oldRetCollectorCount),
					zap.Int64("newRowCount", newRetCollectorCount),
					zap.Int64("subRowCount", subCollector.Base().Count),
					zap.Int("mergeWorker", index))
			}
			printAnalyzeMergeCollectorLog(oldRetCollectorCount, newRetCollectorCount, subCollector.Base().Count,
				e.tableID.TableID, e.tableID.PartitionID, e.TableID.IsPartitionTable(),
				"merge subMergeWorker in AnalyzeColumnsExec", index)

			newRetCollectorSize := retCollector.Base().MemSize
			subCollectorSize := subCollector.Base().MemSize
			e.memTracker.Consume(newRetCollectorSize - oldRetCollectorSize - subCollectorSize)
			e.memTracker.Release(inflightDataSize + inflightRespSize)
			inflightDataSize = 0
			inflightRespSize = 0
			subCollector.DestroyAndPutToPool()
		case <-ctx.Done():
			err := normalizeCtxErrWithCause(ctx, ctx.Err())
			if err != nil {
				cleanupCollector()
				resultCh <- &samplingMergeResult{err: err}
				return
			}
			if intest.InTest {
				panic("this ctx should be canceled with the error")
			}
			cleanupCollector()
			resultCh <- &samplingMergeResult{err: errors.New("context canceled without error")}
			return
		}
	}
}

func drainPendingSamplingMergeTasks(taskCh <-chan []byte, memTracker *memory.Tracker) {
	for data := range taskCh {
		memTracker.Release(int64(cap(data)))
	}
}

func (e *AnalyzeColumnsExec) subBuildWorker(ctx context.Context, resultCh chan error, taskCh chan *samplingBuildTask, hists []*statistics.Histogram, topns []*statistics.TopN, exitCh chan struct{}, buildTraceSummary *samplingBuildTraceSummary) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("analyze subBuildWorker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- getAnalyzePanicErr(r)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingBuildWorkerPanic", func() {
		panic("failpoint triggered")
	})

	colLen := len(e.colsInfo)

workLoop:
	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				break workLoop
			}
			taskStart := time.Now()
			// Track per-task allocations: curBufferedMemSize is pending charges to the tracker,
			// totalBuffered accumulates bytes that become part of collector.MemSize.
			curBufferedMemSize := int64(0)
			totalBuffered := int64(0)

			consumeBuffered := func(bytes int64) {
				totalBuffered += bytes
				e.memTracker.BufferedConsume(&curBufferedMemSize, bytes)
			}
			flushBuffered := func(cum *int64) {
				if curBufferedMemSize != 0 {
					e.memTracker.Consume(curBufferedMemSize)
					curBufferedMemSize = 0
				}
				*cum += totalBuffered
				totalBuffered = 0
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
				// 8 means the pointer size of sampleItems slice.
				// statistics.EmptySampleItemSize already accounts for the embedded types.Datum in SampleItem.Value.
				// The real underlying byte slice of Datum in row.Columns has already be accounted FromProto().
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
						consumeBuffered(deltaSize)
					}
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value:   val,
						Ordinal: j,
					})
				}
				flushBuffered(&collectorMemSize)
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
				// statistics.EmptySampleItemSize already accounts for the embedded types.Datum in SampleItem.Value.
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
					if cap(b) > 8 {
						// We already accounted 8 bytes before the loop started,
						// here we need to account the remaining bytes.
						consumeBuffered(int64(cap(b) - 8))
					}
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value: types.NewBytesDatum(b),
					})
				}
				flushBuffered(&collectorMemSize)
				collector = &statistics.SampleCollector{
					Samples:   sampleItems,
					NullCount: task.rootRowCollector.Base().NullCount[task.slicePos],
					Count:     task.rootRowCollector.Base().Count - task.rootRowCollector.Base().NullCount[task.slicePos],
					FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
					TotalSize: task.rootRowCollector.Base().TotalSizes[task.slicePos],
					MemSize:   collectorMemSize,
				}
			}
			releaseCollectorMemory := func() {
				collectorMemSize := collector.MemSize
				failpoint.InjectCall("analyzeSamplingBuildBeforeReleaseCollectorMemory", collectorMemSize, e.memTracker.BytesConsumed())
				intest.Assert(collectorMemSize >= 0, "collector memory size should be non-negative")
				e.memTracker.Release(collectorMemSize)
				collector.Destroy()
				failpoint.InjectCall("analyzeSamplingBuildAfterReleaseCollectorMemory", collectorMemSize, e.memTracker.BytesConsumed())
			}
			numTopN := int(e.opts[ast.AnalyzeOptNumTopN])
			if task.isColumn {
				if e.tableInfo != nil && isColumnCoveredBySingleColUniqueIndex(e.tableInfo, e.colsInfo[task.slicePos].Offset) {
					numTopN = 0
				}
			} else {
				idx := e.indexes[task.slicePos-colLen]
				if isSingleColNonPrefixUniqueIndex(idx) {
					numTopN = 0
				}
			}
			hist, topn, err := statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), numTopN, task.id, collector, task.tp, task.isColumn, e.memTracker)
			buildElapsed := time.Since(taskStart)
			if err != nil {
				buildTraceSummary.record(buildElapsed, len(collector.Samples), 0, 0, false)
				if analyzeFullSamplingTraceDebugEnabled() {
					e.logAnalyzeFullSamplingTraceDebug("tidb.build_hist_topn", "finish",
						zap.Duration("elapsed", buildElapsed),
						zap.Bool("success", false),
						zap.Int64("statsID", task.id),
						zap.Bool("isColumn", task.isColumn),
						zap.Int("slicePos", task.slicePos),
						zap.Int("sampleItems", len(collector.Samples)),
						zap.Error(err))
				}
				resultCh <- err
				releaseCollectorMemory()
				continue
			}
			histMemSize, topNMemSize := hist.MemoryUsage(), topn.MemoryUsage()
			finalMemSize := histMemSize + topNMemSize
			e.memTracker.Consume(finalMemSize)
			hists[task.slicePos] = hist
			topns[task.slicePos] = topn
			buildTraceSummary.record(buildElapsed, len(collector.Samples), histMemSize, topNMemSize, true)
			if analyzeFullSamplingTraceDebugEnabled() {
				e.logAnalyzeFullSamplingTraceDebug("tidb.build_hist_topn", "finish",
					zap.Duration("elapsed", buildElapsed),
					zap.Bool("success", true),
					zap.Int64("statsID", task.id),
					zap.Bool("isColumn", task.isColumn),
					zap.Int("slicePos", task.slicePos),
					zap.Int("sampleItems", len(collector.Samples)),
					zap.Int64("histMemoryBytes", histMemSize),
					zap.Int64("topNMemoryBytes", topNMemSize))
			}
			resultCh <- nil
			releaseCollectorMemory()
		case <-exitCh:
			return
		case <-ctx.Done():
			resultCh <- normalizeCtxErrWithCause(ctx, ctx.Err())
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

func readDataAndSendTask(ctx context.Context, sctx sessionctx.Context, handler *tableResultHandler, mergeTaskCh chan []byte, memTracker *memory.Tracker, traceSummary *samplingReadTraceSummary) (err error) {
	// After all tasks are sent, close the mergeTaskCh to notify the mergeWorker that all tasks have been sent.
	defer close(mergeTaskCh)
	for {
		failpoint.Inject("mockKillRunningV2AnalyzeJob", func() {
			dom := domain.GetDomain(sctx)
			for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
				dom.SysProcTracker().KillSysProcess(id)
			}
		})
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return err
		}
		failpoint.Inject("mockSlowAnalyzeV2", func() {
			select {
			case <-ctx.Done():
				err := context.Cause(ctx)
				if err == nil {
					err = ctx.Err()
				}
				failpoint.Return(err)
			case <-time.After(1000 * time.Second):
			}
		})

		nextStart := time.Now()
		data, err := handler.nextRaw(ctx)
		traceSummary.fetches++
		traceSummary.fetchElapsedTotal += time.Since(nextStart)
		if err != nil {
			err = normalizeCtxErrWithCause(ctx, err)
			return errors.Trace(err)
		}
		if data == nil {
			break
		}
		traceSummary.dataResponses++
		traceSummary.responseBytes += int64(len(data))

		dataSize := int64(cap(data))
		memTracker.Consume(dataSize)
		enqueueStart := time.Now()
		select {
		case mergeTaskCh <- data:
			traceSummary.enqueuedResponses++
			traceSummary.enqueueElapsedTotal += time.Since(enqueueStart)
		case <-ctx.Done():
			memTracker.Release(dataSize)
			return errors.Trace(normalizeCtxErrWithCause(ctx, ctx.Err()))
		}
	}

	return nil
}
