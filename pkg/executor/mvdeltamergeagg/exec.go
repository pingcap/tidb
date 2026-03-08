// Copyright 2026 PingCAP, Inc.
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

package mvdeltamergeagg

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"golang.org/x/sync/errgroup"
)

// Mapping is one aggregate merge rule.
type Mapping struct {
	// AggFunc must be non-nil.
	AggFunc *aggregation.AggFuncDesc
	// ColID are output column IDs in child schema.
	ColID []int
	// DependencyColID are dependency column IDs.
	// A dependency is either a delta-agg column (< DeltaAggColCount) or a
	// previously computed output column.
	DependencyColID []int
	// MinMaxRecompute is per-mapping MIN/MAX recompute metadata.
	// It must be nil for non-MIN/MAX mappings.
	MinMaxRecompute *MinMaxRecomputeSpec
}

// MinMaxRecomputeStrategy is the recompute mode for MIN/MAX fallback.
type MinMaxRecomputeStrategy uint8

const (
	// MinMaxRecomputeSingleRow recomputes one group key per execution.
	MinMaxRecomputeSingleRow MinMaxRecomputeStrategy = iota + 1
	// MinMaxRecomputeBatch recomputes multiple group keys in one execution.
	MinMaxRecomputeBatch
)

// MinMaxBatchLookupContent is one batch lookup key.
type MinMaxBatchLookupContent struct {
	// Keys are group-key datums in planner-defined key order.
	Keys []types.Datum
}

// MinMaxBatchBuildRequest is the input to build one batch recompute executor.
type MinMaxBatchBuildRequest struct {
	// LookupKeys are deduplicated keys for this batch.
	LookupKeys []*MinMaxBatchLookupContent
}

// MinMaxBatchExecBuilder builds one opened recompute executor for one batch request.
type MinMaxBatchExecBuilder interface {
	// Build must return an opened executor.
	// The result schema must put group-key columns first, in the same order as MinMaxRecomputeExec.KeyInputColIDs.
	Build(context.Context, *MinMaxBatchBuildRequest) (exec.Executor, error)
}

// MinMaxRecomputeSingleRowWorker is one worker-local single-row slot.
type MinMaxRecomputeSingleRowWorker struct {
	// KeyCols are correlated key columns bound to this worker slot.
	KeyCols []*expression.CorrelatedColumn
	// Exec is the worker-local reusable executor.
	Exec exec.Executor
}

// MinMaxRecomputeSingleRowExec stores single-row recompute worker slots.
type MinMaxRecomputeSingleRowExec struct {
	// Workers has one slot per MV merge worker.
	Workers []MinMaxRecomputeSingleRowWorker
}

// MinMaxRecomputeSpec is recompute metadata for one MIN/MAX mapping.
type MinMaxRecomputeSpec struct {
	// Strategy is the recompute mode.
	Strategy MinMaxRecomputeStrategy
	// SingleRow is set when Strategy is MinMaxRecomputeSingleRow.
	SingleRow *MinMaxRecomputeSingleRowExec
	// BatchResultColIdxes are result-column indexes in batch recompute output.
	// It is valid only when Strategy is MinMaxRecomputeBatch.
	BatchResultColIdxes []int
}

// MinMaxRecomputeExec stores shared MIN/MAX recompute metadata.
type MinMaxRecomputeExec struct {
	// KeyInputColIDs are group-key columns in child schema.
	KeyInputColIDs []int
	// KeyResultColIdxes are group-key columns in batch recompute result schema, in the same order as KeyInputColIDs.
	// When empty, [0..len(KeyInputColIDs)) is used.
	KeyResultColIdxes []int
	// BatchBuilder creates one batch recompute executor.
	// Build must be safe for concurrent calls across MV merge workers.
	BatchBuilder MinMaxBatchExecBuilder
}

// RowOpType is the row write operation on MV table.
type RowOpType uint8

const (
	// RowOpNoOp means there is no MV row to touch.
	RowOpNoOp RowOpType = iota
	// RowOpInsert means insert a new MV row.
	RowOpInsert
	// RowOpUpdate means update an existing MV row.
	RowOpUpdate
	// RowOpDelete means delete an existing MV row.
	RowOpDelete
)

// RowOp is one row-level write action.
type RowOp struct {
	RowIdx int
	Tp     RowOpType
	// updateOrdinal is the index in update candidates for this chunk.
	// It is valid for rows that were initially update candidates.
	updateOrdinal int32
}

// ChunkResult is one worker result for one input chunk.
type ChunkResult struct {
	// Input is the original joined chunk.
	Input *chunk.Chunk
	// ComputedCols is indexed by input column ID. Nil means not computed.
	ComputedCols []*chunk.Column
	// RowOps are row write operations.
	RowOps []RowOp
	// UpdateTouchedBitmap stores touched-column bitmaps for update candidates.
	UpdateTouchedBitmap []uint8
	// UpdateTouchedStride is bytes per update row in UpdateTouchedBitmap.
	UpdateTouchedStride int
	// UpdateTouchedBitCnt is touched-bit count per update row.
	UpdateTouchedBitCnt int
}

// ResultWriter consumes merged chunk results.
type ResultWriter interface {
	WriteChunk(ctx context.Context, result *ChunkResult) error
}

// Exec is the sink executor for incremental MV merge.
type Exec struct {
	exec.BaseExecutor

	AggMappings      []Mapping
	DeltaAggColCount int
	// MinMaxRecompute stores MIN/MAX recompute metadata.
	MinMaxRecompute *MinMaxRecomputeExec

	WorkerCnt int
	Writer    ResultWriter

	TargetTable table.Table
	TargetInfo  *model.TableInfo
	// TargetHandleCols builds row handles for update/delete from child input rows.
	TargetHandleCols plannerutil.HandleCols
	// TargetWritableColIDs maps writable target column index to input column index.
	TargetWritableColIDs []int

	compiledMergers      []aggMerger
	compiledOutputColCnt int
	aggOutputColIDs      []int
	prepared             bool
	executed             bool
	runtimeStats         *mvDeltaMergeAggRuntimeStats
}

type mvMergeAggWorkerData struct {
	updateRows                   []int
	updateOpIndexes              []int
	updateChanged                []bool
	minMaxRecomputeRowsByMapping [][]int
	batchUniqueRows              []int
	batchResultRows              []int
	batchRowKeyIdx               []int
	batchInputEncodedKeys        [][]byte
	batchResultEncodedKeys       [][]byte
	batchInputNullByRow          []bool
	batchResultNullByRow         []bool
}

type mvDeltaMergeAggPipelineStats struct {
	readerTime      time.Duration
	writerTime      time.Duration
	mergeWorkerTime []time.Duration
	writerDetail    mvDeltaMergeAggWriterStats
}

type mvDeltaMergeAggRuntimeStats struct {
	readerTime      time.Duration
	writerTime      time.Duration
	mergeWorkerTime []time.Duration
	writerDetail    mvDeltaMergeAggWriterStats
}

type mvDeltaMergeAggWriterStats struct {
	chunks int64
	rowOps int64

	noopRows   int64
	insertRows int64
	updateRows int64
	deleteRows int64
}

func (s *mvDeltaMergeAggWriterStats) merge(other mvDeltaMergeAggWriterStats) {
	s.chunks += other.chunks
	s.rowOps += other.rowOps
	s.noopRows += other.noopRows
	s.insertRows += other.insertRows
	s.updateRows += other.updateRows
	s.deleteRows += other.deleteRows
}

func newMVDeltaMergeAggRuntimeStats(workerCnt int) *mvDeltaMergeAggRuntimeStats {
	if workerCnt < 0 {
		workerCnt = 0
	}
	return &mvDeltaMergeAggRuntimeStats{
		mergeWorkerTime: make([]time.Duration, workerCnt),
	}
}

func (s *mvDeltaMergeAggRuntimeStats) reset(workerCnt int) {
	if workerCnt < 0 {
		workerCnt = 0
	}
	s.readerTime = 0
	s.writerTime = 0
	if cap(s.mergeWorkerTime) < workerCnt {
		s.mergeWorkerTime = make([]time.Duration, workerCnt)
		return
	}
	s.mergeWorkerTime = s.mergeWorkerTime[:workerCnt]
	clear(s.mergeWorkerTime)
}

func (s *mvDeltaMergeAggRuntimeStats) fillFromPipelineStats(stats *mvDeltaMergeAggPipelineStats) {
	if s == nil || stats == nil {
		return
	}
	s.readerTime = stats.readerTime
	s.writerTime = stats.writerTime
	if cap(s.mergeWorkerTime) < len(stats.mergeWorkerTime) {
		s.mergeWorkerTime = make([]time.Duration, len(stats.mergeWorkerTime))
	}
	s.mergeWorkerTime = s.mergeWorkerTime[:len(stats.mergeWorkerTime)]
	copy(s.mergeWorkerTime, stats.mergeWorkerTime)
	s.writerDetail = stats.writerDetail
}

func (s *mvDeltaMergeAggRuntimeStats) String() string {
	if s == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("mv_delta_merge_agg:{merge_worker:{total:")
	buf.WriteString(strconv.Itoa(len(s.mergeWorkerTime)))
	active := 0
	var minDur, maxDur, sumDur time.Duration
	for _, d := range s.mergeWorkerTime {
		if d <= 0 {
			continue
		}
		if active == 0 || d < minDur {
			minDur = d
		}
		if active == 0 || d > maxDur {
			maxDur = d
		}
		sumDur += d
		active++
	}
	buf.WriteString(", active:")
	buf.WriteString(strconv.Itoa(active))
	if active > 0 {
		buf.WriteString(", min:")
		buf.WriteString(execdetails.FormatDuration(minDur))
		buf.WriteString(", max:")
		buf.WriteString(execdetails.FormatDuration(maxDur))
		buf.WriteString(", avg:")
		buf.WriteString(execdetails.FormatDuration(sumDur / time.Duration(active)))
	}
	buf.WriteString("}")
	buf.WriteString(", reader:")
	buf.WriteString(execdetails.FormatDuration(s.readerTime))
	buf.WriteString(", writer:{time:")
	buf.WriteString(execdetails.FormatDuration(s.writerTime))
	buf.WriteString(", chunks:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.chunks, 10))
	buf.WriteString(", row_ops:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.rowOps, 10))
	buf.WriteString(", rows:{insert:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.insertRows, 10))
	buf.WriteString(", update:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.updateRows, 10))
	buf.WriteString(", delete:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.deleteRows, 10))
	buf.WriteString(", noop:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.noopRows, 10))
	buf.WriteString("}")
	buf.WriteString("}")
	buf.WriteString("}")
	return buf.String()
}

func (s *mvDeltaMergeAggRuntimeStats) Clone() execdetails.RuntimeStats {
	if s == nil {
		return &mvDeltaMergeAggRuntimeStats{}
	}
	newStats := &mvDeltaMergeAggRuntimeStats{
		readerTime:      s.readerTime,
		writerTime:      s.writerTime,
		mergeWorkerTime: make([]time.Duration, len(s.mergeWorkerTime)),
		writerDetail:    s.writerDetail,
	}
	copy(newStats.mergeWorkerTime, s.mergeWorkerTime)
	return newStats
}

func (s *mvDeltaMergeAggRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*mvDeltaMergeAggRuntimeStats)
	if !ok || tmp == nil {
		return
	}
	s.readerTime += tmp.readerTime
	s.writerTime += tmp.writerTime
	s.writerDetail.merge(tmp.writerDetail)
	if len(tmp.mergeWorkerTime) == 0 {
		return
	}
	if len(s.mergeWorkerTime) < len(tmp.mergeWorkerTime) {
		ext := make([]time.Duration, len(tmp.mergeWorkerTime))
		copy(ext, s.mergeWorkerTime)
		s.mergeWorkerTime = ext
	}
	for idx, d := range tmp.mergeWorkerTime {
		s.mergeWorkerTime[idx] += d
	}
}

func (*mvDeltaMergeAggRuntimeStats) Tp() int {
	return execdetails.TpMVDeltaMergeAggRuntimeStats
}

// Open implements the Executor interface.
func (e *Exec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	if e.WorkerCnt <= 0 {
		e.WorkerCnt = e.Ctx().GetSessionVars().ExecutorConcurrency
		if e.WorkerCnt <= 0 {
			e.WorkerCnt = 1
		}
	}
	if err := e.prepareMergers(); err != nil {
		return err
	}
	if e.Writer == nil {
		if e.TargetTable != nil {
			tblWriter, err := e.buildTableResultWriter()
			if err != nil {
				return err
			}
			e.Writer = tblWriter
		} else {
			e.Writer = noopWriter{}
		}
	}
	e.prepared = true
	e.executed = false
	return nil
}

// Next implements the Executor interface.
// Exec is a sink executor and always returns an empty chunk.
func (e *Exec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		return errors.New("Exec is not opened")
	}
	if e.executed {
		return nil
	}
	e.executed = true
	if e.BaseExecutor.RuntimeStats() != nil {
		if e.runtimeStats == nil {
			e.runtimeStats = newMVDeltaMergeAggRuntimeStats(e.WorkerCnt)
		} else {
			e.runtimeStats.reset(e.WorkerCnt)
		}
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)
	}
	return e.runMergePipeline(ctx)
}

// Close implements the Executor interface.
func (e *Exec) Close() error {
	e.compiledMergers = nil
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = nil
	e.runtimeStats = nil
	e.prepared = false
	e.executed = false
	return e.BaseExecutor.Close()
}

func (e *Exec) runMergePipeline(ctx context.Context) error {
	workerCnt := e.WorkerCnt
	if workerCnt <= 0 {
		workerCnt = 1
	}
	var pipelineStats *mvDeltaMergeAggPipelineStats
	if e.runtimeStats != nil {
		pipelineStats = &mvDeltaMergeAggPipelineStats{
			mergeWorkerTime: make([]time.Duration, workerCnt),
		}
		if statsWriter, ok := e.Writer.(writerRuntimeStatsAware); ok {
			statsWriter.setRuntimeStats(&pipelineStats.writerDetail)
		}
		defer e.runtimeStats.fillFromPipelineStats(pipelineStats)
	} else if statsWriter, ok := e.Writer.(writerRuntimeStatsAware); ok {
		statsWriter.setRuntimeStats(nil)
	}

	inputBufSize := max(workerCnt*2, 2)

	inputCh := make(chan *chunk.Chunk, inputBufSize)
	freeInputCh := make(chan *chunk.Chunk, inputBufSize)
	resultCh := make(chan *ChunkResult, workerCnt)

	for i := 0; i < inputBufSize; i++ {
		freeInputCh <- exec.NewFirstChunk(e.Children(0))
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return e.runReader(gctx, inputCh, freeInputCh, pipelineStats)
	})

	workerWG := sync.WaitGroup{}
	workerWG.Add(workerCnt)
	for i := 0; i < workerCnt; i++ {
		workerIdx := i
		g.Go(func() error {
			defer workerWG.Done()
			return e.runWorker(gctx, inputCh, resultCh, workerIdx, pipelineStats)
		})
	}

	go func() {
		workerWG.Wait()
		close(resultCh)
	}()

	g.Go(func() error {
		return e.runWriter(gctx, resultCh, freeInputCh, pipelineStats)
	})

	return g.Wait()
}

func (e *Exec) runReader(
	ctx context.Context,
	inputCh chan<- *chunk.Chunk,
	freeInputCh <-chan *chunk.Chunk,
	stats *mvDeltaMergeAggPipelineStats,
) error {
	defer close(inputCh)
	var total time.Duration
	defer func() {
		if stats != nil {
			stats.readerTime = total
		}
	}()
	child := e.Children(0)

	for {
		var chk *chunk.Chunk
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chk = <-freeInputCh:
		}

		chk.Reset()
		start := time.Now()
		if err := exec.Next(ctx, child, chk); err != nil {
			return err
		}
		total += time.Since(start)
		if chk.NumRows() == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case inputCh <- chk:
		}
	}
}

func (e *Exec) runWorker(
	ctx context.Context,
	inputCh <-chan *chunk.Chunk,
	resultCh chan<- *ChunkResult,
	workerIdx int,
	stats *mvDeltaMergeAggPipelineStats,
) error {
	var workerData mvMergeAggWorkerData
	var total time.Duration
	defer func() {
		if stats != nil && workerIdx >= 0 && workerIdx < len(stats.mergeWorkerTime) {
			stats.mergeWorkerTime[workerIdx] = total
		}
	}()
	for {
		var (
			chk *chunk.Chunk
			ok  bool
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chk, ok = <-inputCh:
			if !ok {
				return nil
			}
		}

		start := time.Now()
		result, err := e.mergeOneChunk(ctx, chk, &workerData, workerIdx)
		if err != nil {
			return err
		}
		total += time.Since(start)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- result:
		}
	}
}

func (e *Exec) runWriter(
	ctx context.Context,
	resultCh <-chan *ChunkResult,
	freeInputCh chan<- *chunk.Chunk,
	stats *mvDeltaMergeAggPipelineStats,
) error {
	var total time.Duration
	defer func() {
		if stats != nil {
			stats.writerTime = total
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-resultCh:
			if !ok {
				return nil
			}
			start := time.Now()
			if err := e.Writer.WriteChunk(ctx, result); err != nil {
				return err
			}
			total += time.Since(start)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case freeInputCh <- result.Input:
			}
		}
	}
}

func isFirstAggCountAllRows(aggFunc *aggregation.AggFuncDesc) bool {
	if aggFunc == nil {
		return false
	}
	if aggFunc.Name != ast.AggFuncCount || aggFunc.HasDistinct {
		return false
	}
	if len(aggFunc.Args) != 1 {
		return false
	}
	con, ok := aggFunc.Args[0].(*expression.Constant)
	if !ok {
		return false
	}
	// Require a strict non-NULL compile-time constant.
	if con.ConstLevel() != expression.ConstStrict || con.Value.IsNull() {
		return false
	}
	return true
}

func aggFuncForErr(aggFunc *aggregation.AggFuncDesc) string {
	if aggFunc == nil {
		return "<nil>"
	}
	return aggFunc.StringWithCtx(exprctx.EmptyParamValues, errors.RedactLogDisable)
}

func isMinMaxAgg(aggName string) bool {
	return aggName == ast.AggFuncMin || aggName == ast.AggFuncMax
}

func resolveMinMaxKeyResultColIdxes(meta *MinMaxRecomputeExec) []int {
	if meta == nil || len(meta.KeyInputColIDs) == 0 {
		return nil
	}
	if len(meta.KeyResultColIdxes) == 0 {
		idxes := make([]int, len(meta.KeyInputColIDs))
		for i := range idxes {
			idxes[i] = i
		}
		return idxes
	}
	return meta.KeyResultColIdxes
}

func (e *Exec) validateMinMaxRecompute(childTypes []*types.FieldType) error {
	if e.MinMaxRecompute == nil {
		for mappingIdx, agg := range e.AggMappings {
			if agg.MinMaxRecompute != nil {
				return errors.Errorf("AggMappings[%d].MinMaxRecompute is set but MinMaxRecompute is nil", mappingIdx)
			}
		}
		return nil
	}
	meta := e.MinMaxRecompute
	if len(meta.KeyInputColIDs) == 0 {
		return errors.New("MinMaxRecompute requires non-empty KeyInputColIDs")
	}
	seenKey := make(map[int]struct{}, len(meta.KeyInputColIDs))
	for _, keyColID := range meta.KeyInputColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("MinMaxRecompute key col %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if _, dup := seenKey[keyColID]; dup {
			return errors.Errorf("duplicate MinMaxRecompute key col %d", keyColID)
		}
		seenKey[keyColID] = struct{}{}
	}
	keyResultColIdxes := resolveMinMaxKeyResultColIdxes(meta)
	if len(keyResultColIdxes) != len(meta.KeyInputColIDs) {
		return errors.Errorf(
			"MinMaxRecompute key result column count mismatch: expect %d, got %d",
			len(meta.KeyInputColIDs),
			len(keyResultColIdxes),
		)
	}
	seenKeyResult := make(map[int]struct{}, len(keyResultColIdxes))
	for _, resultColIdx := range keyResultColIdxes {
		if resultColIdx < 0 {
			return errors.Errorf("MinMaxRecompute key result col idx %d must be non-negative", resultColIdx)
		}
		if _, dup := seenKeyResult[resultColIdx]; dup {
			return errors.Errorf("duplicate MinMaxRecompute key result col idx %d", resultColIdx)
		}
		seenKeyResult[resultColIdx] = struct{}{}
	}
	hasBatch := false
	for mappingIdx := range e.AggMappings {
		agg := e.AggMappings[mappingIdx]
		if agg.AggFunc == nil {
			return errors.Errorf("MinMaxRecompute validation requires AggMappings[%d].AggFunc", mappingIdx)
		}
		aggName := agg.AggFunc.Name
		minMaxRecompute := agg.MinMaxRecompute
		if !isMinMaxAgg(aggName) {
			if minMaxRecompute != nil {
				return errors.Errorf("AggMappings[%d].MinMaxRecompute is set for non-MIN/MAX agg=%s", mappingIdx, aggName)
			}
			continue
		}
		if minMaxRecompute == nil {
			return errors.Errorf("missing AggMappings[%d].MinMaxRecompute for agg=%s", mappingIdx, aggName)
		}
		switch minMaxRecompute.Strategy {
		case MinMaxRecomputeSingleRow:
			if minMaxRecompute.SingleRow == nil {
				return errors.Errorf("MinMaxRecompute mapping %d requires SingleRow execution metadata", mappingIdx)
			}
			if len(minMaxRecompute.BatchResultColIdxes) > 0 {
				return errors.Errorf("MinMaxRecompute mapping %d single-row strategy should not set BatchResultColIdxes", mappingIdx)
			}
			if e.WorkerCnt > 0 && len(minMaxRecompute.SingleRow.Workers) != e.WorkerCnt {
				return errors.Errorf("MinMaxRecompute mapping %d single-row worker slots mismatch: expect %d, got %d", mappingIdx, e.WorkerCnt, len(minMaxRecompute.SingleRow.Workers))
			}
			for workerIdx := range minMaxRecompute.SingleRow.Workers {
				worker := minMaxRecompute.SingleRow.Workers[workerIdx]
				if worker.Exec == nil {
					return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d requires executor", mappingIdx, workerIdx)
				}
				if len(worker.KeyCols) != len(meta.KeyInputColIDs) {
					return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d key column mismatch: expect %d, got %d", mappingIdx, workerIdx, len(meta.KeyInputColIDs), len(worker.KeyCols))
				}
				for keyIdx, keyCol := range worker.KeyCols {
					if keyCol == nil || keyCol.Data == nil {
						return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d key column %d is not initialized", mappingIdx, workerIdx, keyIdx)
					}
				}
			}
		case MinMaxRecomputeBatch:
			hasBatch = true
			if minMaxRecompute.SingleRow != nil {
				return errors.Errorf("MinMaxRecompute mapping %d batch strategy should not set SingleRow metadata", mappingIdx)
			}
			if len(minMaxRecompute.BatchResultColIdxes) == 0 {
				return errors.Errorf("MinMaxRecompute mapping %d batch strategy requires BatchResultColIdxes", mappingIdx)
			}
			if len(minMaxRecompute.BatchResultColIdxes) != len(agg.ColID) {
				return errors.Errorf("MinMaxRecompute mapping %d batch result column mismatch: Mapping.ColID=%d BatchResultColIdxes=%d", mappingIdx, len(agg.ColID), len(minMaxRecompute.BatchResultColIdxes))
			}
			seenBatchResult := make(map[int]struct{}, len(minMaxRecompute.BatchResultColIdxes))
			for _, resultColIdx := range minMaxRecompute.BatchResultColIdxes {
				if resultColIdx < 0 {
					return errors.Errorf("MinMaxRecompute mapping %d batch result col idx %d must be non-negative", mappingIdx, resultColIdx)
				}
				if _, conflictsWithKey := seenKeyResult[resultColIdx]; conflictsWithKey {
					return errors.Errorf(
						"MinMaxRecompute mapping %d batch result col idx %d conflicts with key prefix/result columns",
						mappingIdx,
						resultColIdx,
					)
				}
				if _, dup := seenBatchResult[resultColIdx]; dup {
					return errors.Errorf("duplicate batch result col idx %d in MinMaxRecompute mapping %d", resultColIdx, mappingIdx)
				}
				seenBatchResult[resultColIdx] = struct{}{}
			}
		default:
			return errors.Errorf("MinMaxRecompute mapping %d has unknown strategy %d", mappingIdx, minMaxRecompute.Strategy)
		}
	}
	if hasBatch && meta.BatchBuilder == nil {
		return errors.New("MinMaxRecompute batch strategy requires BatchBuilder")
	}
	if !hasBatch && meta.BatchBuilder != nil {
		return errors.New("MinMaxRecompute BatchBuilder is set but no batch strategy mapping exists")
	}
	return nil
}

func (e *Exec) prepareMergers() error {
	if e.ChildrenLen() != 1 {
		return errors.Errorf("Exec expects exactly 1 child, got %d", e.ChildrenLen())
	}
	if len(e.AggMappings) == 0 {
		return errors.New("Exec requires non-empty AggMappings")
	}

	childTypes := exec.RetTypes(e.Children(0))
	if e.DeltaAggColCount < 0 || e.DeltaAggColCount > len(childTypes) {
		return errors.Errorf("DeltaAggColCount %d out of range [0,%d]", e.DeltaAggColCount, len(childTypes))
	}
	if err := e.validateMinMaxRecompute(childTypes); err != nil {
		return err
	}
	colID2ComputedIdx := make(map[int]int, len(e.AggMappings))
	e.compiledMergers = make([]aggMerger, 0, len(e.AggMappings))
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = make([]int, 0, len(e.AggMappings))

	firstAgg := e.AggMappings[0].AggFunc
	if firstAgg == nil {
		return errors.New("MVDeltaMergeAgg mapping requires AggFunc")
	}
	if !isFirstAggCountAllRows(firstAgg) {
		return errors.Errorf("the first MVDeltaMergeAgg mapping must be COUNT(*)/COUNT(non-NULL constant), got %s", aggFuncForErr(firstAgg))
	}
	if len(e.AggMappings[0].ColID) != 1 {
		return errors.Errorf("the first MVDeltaMergeAgg mapping must output exactly 1 column, got %d", len(e.AggMappings[0].ColID))
	}
	minMaxOutputCols := make(map[int]struct{})
	for i := range e.AggMappings {
		mapping := e.AggMappings[i]
		if mapping.AggFunc == nil {
			continue
		}
		if !isMinMaxAgg(mapping.AggFunc.Name) {
			continue
		}
		for _, outputColID := range mapping.ColID {
			minMaxOutputCols[outputColID] = struct{}{}
		}
	}
	if len(minMaxOutputCols) > 0 {
		for mappingIdx := range e.AggMappings {
			mapping := e.AggMappings[mappingIdx]
			for _, depColID := range mapping.DependencyColID {
				if _, isMinMaxOutput := minMaxOutputCols[depColID]; isMinMaxOutput {
					return errors.Errorf(
						"AggMappings[%d] depends on MIN/MAX output col %d, which is unsupported in stage1",
						mappingIdx,
						depColID,
					)
				}
			}
		}
	}

	for i := range e.AggMappings {
		mapping := e.AggMappings[i]
		if mapping.AggFunc == nil {
			return errors.New("MVDeltaMergeAgg mapping requires AggFunc")
		}
		aggName := mapping.AggFunc.Name
		if len(mapping.ColID) == 0 {
			return errors.Errorf("mapping for agg=%s must output at least 1 column", aggName)
		}
		seenInMapping := make(map[int]struct{}, len(mapping.ColID))
		for _, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("output col id %d out of range [0,%d)", outputColID, len(childTypes))
			}
			if _, dup := seenInMapping[outputColID]; dup {
				return errors.Errorf("duplicate output col id %d in one mapping", outputColID)
			}
			if _, dup := colID2ComputedIdx[outputColID]; dup {
				return errors.Errorf("duplicate output col id %d in AggMappings", outputColID)
			}
			seenInMapping[outputColID] = struct{}{}
		}

		var merger aggMerger
		var err error
		switch aggName {
		case ast.AggFuncCount:
			merger, err = e.buildCountMerger(mapping, colID2ComputedIdx, childTypes)
		case ast.AggFuncSum:
			merger, err = e.buildSumMerger(mapping, colID2ComputedIdx, childTypes)
		case ast.AggFuncMin, ast.AggFuncMax:
			if e.MinMaxRecompute == nil {
				err = errors.Errorf("%s merge requires MinMaxRecompute metadata", aggName)
				break
			}
			merger, err = e.buildMinMaxMerger(i, mapping, colID2ComputedIdx, childTypes)
		default:
			err = errors.Errorf("unsupported agg function in Exec: %s", aggName)
		}
		if err != nil {
			return err
		}

		outputColIDs := merger.outputColIDs()
		if len(outputColIDs) != len(mapping.ColID) {
			return errors.Errorf("agg=%s output col count mismatch: mapping has %d but merger has %d", aggName, len(mapping.ColID), len(outputColIDs))
		}
		for idx, outputColID := range outputColIDs {
			if outputColID != mapping.ColID[idx] {
				return errors.Errorf("agg=%s output col mismatch at position %d: mapping=%d merger=%d", aggName, idx, mapping.ColID[idx], outputColID)
			}
			colID2ComputedIdx[outputColID] = e.compiledOutputColCnt
			e.compiledOutputColCnt++
			e.aggOutputColIDs = append(e.aggOutputColIDs, outputColID)
		}
		e.compiledMergers = append(e.compiledMergers, merger)
	}

	return nil
}

func (e *Exec) mergeOneChunk(ctx context.Context, chk *chunk.Chunk, workerData *mvMergeAggWorkerData, workerIdx int) (*ChunkResult, error) {
	if workerData != nil {
		workerData.prepareMinMaxRecomputeRows(len(e.AggMappings))
	}
	computedByOrder := make([]*chunk.Column, e.compiledOutputColCnt)
	computedBuiltCnt := 0
	computedByColID := make([]*chunk.Column, chk.NumCols())
	for _, merger := range e.compiledMergers {
		outputColIDs := merger.outputColIDs()
		outputCnt := len(outputColIDs)
		if outputCnt == 0 {
			return nil, errors.New("agg merger has no output columns")
		}
		if computedBuiltCnt+outputCnt > len(computedByOrder) {
			return nil, errors.Errorf("computed output overflow: built=%d current=%d total=%d", computedBuiltCnt, outputCnt, len(computedByOrder))
		}
		outputCols := computedByOrder[computedBuiltCnt : computedBuiltCnt+outputCnt]
		if err := merger.mergeChunk(chk, computedByOrder[:computedBuiltCnt], outputCols, workerData); err != nil {
			return nil, err
		}
		for idx, col := range outputCols {
			if col == nil {
				return nil, errors.Errorf("agg merger output column is nil at position %d", idx)
			}
			outputColID := outputColIDs[idx]
			computedByColID[outputColID] = col
		}
		computedBuiltCnt += outputCnt
	}
	if computedBuiltCnt != e.compiledOutputColCnt {
		return nil, errors.Errorf("computed output count mismatch, expected=%d got=%d", e.compiledOutputColCnt, computedBuiltCnt)
	}
	if err := e.recomputeMinMaxRows(ctx, chk, computedByColID, workerData, workerIdx); err != nil {
		return nil, err
	}

	rowOps, updateTouchedBitmap, updateTouchedStride, updateTouchedBitCnt, err := e.buildRowOps(chk, computedByColID, workerData)
	if err != nil {
		return nil, err
	}
	return &ChunkResult{
		Input:               chk,
		ComputedCols:        computedByColID,
		RowOps:              rowOps,
		UpdateTouchedBitmap: updateTouchedBitmap,
		UpdateTouchedStride: updateTouchedStride,
		UpdateTouchedBitCnt: updateTouchedBitCnt,
	}, nil
}

func (d *mvMergeAggWorkerData) prepareMinMaxRecomputeRows(mappingCnt int) {
	if mappingCnt <= 0 {
		d.minMaxRecomputeRowsByMapping = nil
		return
	}
	if len(d.minMaxRecomputeRowsByMapping) < mappingCnt {
		old := d.minMaxRecomputeRowsByMapping
		d.minMaxRecomputeRowsByMapping = make([][]int, mappingCnt)
		copy(d.minMaxRecomputeRowsByMapping, old)
	}
	d.minMaxRecomputeRowsByMapping = d.minMaxRecomputeRowsByMapping[:mappingCnt]
	for idx := range d.minMaxRecomputeRowsByMapping {
		d.minMaxRecomputeRowsByMapping[idx] = d.minMaxRecomputeRowsByMapping[idx][:0]
	}
}

func (e *Exec) recomputeMinMaxRows(
	ctx context.Context,
	input *chunk.Chunk,
	computedByColID []*chunk.Column,
	workerData *mvMergeAggWorkerData,
	workerIdx int,
) error {
	if e.MinMaxRecompute == nil || workerData == nil {
		return nil
	}
	if len(workerData.minMaxRecomputeRowsByMapping) == 0 {
		return nil
	}
	if len(e.aggOutputColIDs) == 0 {
		return errors.New("min/max recompute requires aggregate outputs")
	}
	countStarOutputColID := e.aggOutputColIDs[0]
	if countStarOutputColID < 0 || countStarOutputColID >= len(computedByColID) {
		return errors.Errorf("count(*) output col %d out of computed-by-col-id range [0,%d)", countStarOutputColID, len(computedByColID))
	}
	countStarCol := computedByColID[countStarOutputColID]
	if countStarCol == nil {
		return errors.Errorf("count(*) output col %d is missing in computed columns", countStarOutputColID)
	}
	countStarVals := countStarCol.Int64s()
	childTypes := e.Children(0).RetFieldTypes()
	overrides := make([]*mappingRecomputeOverride, len(e.AggMappings))
	batchMappings := make([]int, 0)

	for mappingIdx, rows := range workerData.minMaxRecomputeRowsByMapping {
		if len(rows) == 0 {
			continue
		}
		if mappingIdx < 0 || mappingIdx >= len(e.AggMappings) {
			return errors.Errorf("min/max recompute mapping idx %d out of range [0,%d)", mappingIdx, len(e.AggMappings))
		}
		recomputeMeta := e.AggMappings[mappingIdx].MinMaxRecompute
		if recomputeMeta == nil {
			return errors.Errorf("missing AggMappings[%d].MinMaxRecompute", mappingIdx)
		}
		switch recomputeMeta.Strategy {
		case MinMaxRecomputeSingleRow:
			if err := e.recomputeMinMaxSingleRow(
				ctx,
				input,
				childTypes,
				countStarVals,
				workerIdx,
				mappingIdx,
				rows,
				overrides,
			); err != nil {
				return err
			}
		case MinMaxRecomputeBatch:
			batchMappings = append(batchMappings, mappingIdx)
		default:
			return errors.Errorf("unknown MinMaxRecompute strategy %d for mapping %d", recomputeMeta.Strategy, mappingIdx)
		}
	}
	if len(batchMappings) > 0 {
		if err := e.recomputeMinMaxBatch(
			ctx,
			input,
			childTypes,
			countStarVals,
			batchMappings,
			workerData,
			workerData.minMaxRecomputeRowsByMapping,
			overrides,
		); err != nil {
			return err
		}
	}
	return e.applyMinMaxRecomputeOverrides(computedByColID, childTypes, overrides)
}

func (e *Exec) recomputeMinMaxSingleRow(
	ctx context.Context,
	input *chunk.Chunk,
	childTypes []*types.FieldType,
	countStarVals []int64,
	workerIdx int,
	mappingIdx int,
	recomputeRows []int,
	overrides []*mappingRecomputeOverride,
) error {
	if mappingIdx < 0 || mappingIdx >= len(e.AggMappings) {
		return errors.Errorf("single-row MinMaxRecompute mapping idx %d out of range [0,%d)", mappingIdx, len(e.AggMappings))
	}
	recomputeMeta := e.AggMappings[mappingIdx].MinMaxRecompute
	if recomputeMeta == nil || recomputeMeta.SingleRow == nil {
		return errors.Errorf("single-row MinMaxRecompute metadata is missing for mapping %d", mappingIdx)
	}
	if workerIdx < 0 || workerIdx >= len(recomputeMeta.SingleRow.Workers) {
		return errors.Errorf(
			"min/max single-row worker idx %d out of range [0,%d) for mapping %d",
			workerIdx,
			len(recomputeMeta.SingleRow.Workers),
			mappingIdx,
		)
	}
	worker := recomputeMeta.SingleRow.Workers[workerIdx]
	if worker.Exec == nil {
		return errors.Errorf("min/max single-row worker %d executor is nil for mapping %d", workerIdx, mappingIdx)
	}
	keyColIDs := e.MinMaxRecompute.KeyInputColIDs
	if len(worker.KeyCols) != len(keyColIDs) {
		return errors.Errorf("min/max single-row key column mismatch for mapping %d: expect %d, got %d", mappingIdx, len(keyColIDs), len(worker.KeyCols))
	}
	keyTypes := make([]*types.FieldType, len(keyColIDs))
	for keyPos, keyColID := range keyColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("min/max single-row key col id %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if worker.KeyCols[keyPos] == nil || worker.KeyCols[keyPos].Data == nil {
			return errors.Errorf("min/max single-row worker %d key col %d is not initialized", workerIdx, keyPos)
		}
		keyTypes[keyPos] = childTypes[keyColID]
	}
	resultChk := exec.NewFirstChunk(worker.Exec)
	mapping := e.AggMappings[mappingIdx]
	if _, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(recomputeRows)); err != nil {
		return err
	}
	rowValues := make([]types.Datum, len(mapping.ColID))

	for _, rowIdx := range recomputeRows {
		if rowIdx < 0 || rowIdx >= input.NumRows() {
			return errors.Errorf("min/max single-row recompute row idx %d out of range [0,%d)", rowIdx, input.NumRows())
		}
		if countStarVals[rowIdx] < 0 {
			return errors.Errorf("count(*) becomes negative (%d) at row %d", countStarVals[rowIdx], rowIdx)
		}
		if countStarVals[rowIdx] == 0 {
			continue
		}
		inputRow := input.GetRow(rowIdx)
		for keyPos, keyColID := range keyColIDs {
			keyDatum := inputRow.GetDatum(keyColID, keyTypes[keyPos])
			keyDatum.Copy(worker.KeyCols[keyPos].Data)
		}

		if err := exec.Open(ctx, worker.Exec); err != nil {
			return err
		}
		resultChk.Reset()
		nextErr := exec.Next(ctx, worker.Exec, resultChk)
		if nextErr != nil {
			closeErr := worker.Exec.Close()
			if closeErr != nil {
				return closeErr
			}
			return nextErr
		}
		if resultChk.NumRows() == 0 {
			closeErr := worker.Exec.Close()
			if closeErr != nil {
				return closeErr
			}
			return errors.Errorf("min/max single-row recompute returns no row for mapping %d row %d", mappingIdx, rowIdx)
		}
		if resultChk.NumRows() > 1 {
			closeErr := worker.Exec.Close()
			if closeErr != nil {
				return closeErr
			}
			return errors.Errorf("min/max single-row recompute returns more than one row for mapping %d row %d", mappingIdx, rowIdx)
		}

		resultRow := resultChk.GetRow(0)
		for colPos, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("mapping %d output col id %d out of range [0,%d)", mappingIdx, outputColID, len(childTypes))
			}
			rowValues[colPos] = resultRow.GetDatum(colPos, childTypes[outputColID])
		}

		resultChk.Reset()
		nextErr = exec.Next(ctx, worker.Exec, resultChk)
		closeErr := worker.Exec.Close()
		if nextErr != nil {
			return nextErr
		}
		if closeErr != nil {
			return closeErr
		}
		if resultChk.NumRows() != 0 {
			return errors.Errorf("min/max single-row recompute returns more than one row for mapping %d row %d", mappingIdx, rowIdx)
		}

		if err := appendMappingOverrideFromValues(overrides, mappingIdx, rowIdx, rowValues); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exec) recomputeMinMaxBatch(
	ctx context.Context,
	input *chunk.Chunk,
	childTypes []*types.FieldType,
	countStarVals []int64,
	batchMappings []int,
	workerData *mvMergeAggWorkerData,
	recomputeRowsByMapping [][]int,
	overrides []*mappingRecomputeOverride,
) (retErr error) {
	if e.MinMaxRecompute.BatchBuilder == nil {
		return errors.New("min/max batch recompute requires BatchBuilder")
	}
	if workerData == nil {
		return errors.New("min/max batch recompute requires worker data")
	}
	typeCtx := e.Ctx().GetSessionVars().StmtCtx.TypeCtx()
	keyColIDs := e.MinMaxRecompute.KeyInputColIDs
	keyTypes := make([]*types.FieldType, len(keyColIDs))
	for keyPos, keyColID := range keyColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("min/max batch key col id %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if childTypes[keyColID] == nil {
			return errors.Errorf("min/max batch key col id %d type is unavailable", keyColID)
		}
		keyTypes[keyPos] = childTypes[keyColID]
	}
	rowCnt := input.NumRows()

	const (
		batchRowKeyUnknown = -2
		batchRowKeySkipped = -1
		batchRowKeyActive  = -3
	)

	rowKeyIdx := workerData.batchRowKeyIdx
	if cap(rowKeyIdx) < rowCnt {
		rowKeyIdx = make([]int, rowCnt)
	} else {
		rowKeyIdx = rowKeyIdx[:rowCnt]
	}
	for i := range rowKeyIdx {
		rowKeyIdx[i] = batchRowKeyUnknown
	}
	workerData.batchRowKeyIdx = rowKeyIdx

	uniqueRows := workerData.batchUniqueRows[:0]
	lookupKeys := make([]*MinMaxBatchLookupContent, 0)
	refsByLookupKey := make([][]batchRowRef, 0)
	seenStateByMapping := make([]*batchMappingSeenState, len(e.AggMappings))

	for _, mappingIdx := range batchMappings {
		rows := recomputeRowsByMapping[mappingIdx]
		if len(rows) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		if _, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(rows)); err != nil {
			return err
		}
		filteredRows := make([]int, 0, len(rows))
		dupCheck := make(map[int]struct{}, len(rows))
		for _, rowIdx := range rows {
			if rowIdx < 0 || rowIdx >= rowCnt {
				return errors.Errorf("min/max batch recompute row idx %d out of range [0,%d)", rowIdx, rowCnt)
			}
			switch rowKeyIdx[rowIdx] {
			case batchRowKeyUnknown:
				if countStarVals[rowIdx] < 0 {
					return errors.Errorf("count(*) becomes negative (%d) at row %d", countStarVals[rowIdx], rowIdx)
				}
				if countStarVals[rowIdx] == 0 {
					rowKeyIdx[rowIdx] = batchRowKeySkipped
					continue
				}
				rowKeyIdx[rowIdx] = batchRowKeyActive
				uniqueRows = append(uniqueRows, rowIdx)
			case batchRowKeySkipped:
				continue
			}

			if _, exists := dupCheck[rowIdx]; exists {
				return errors.Errorf("min/max batch recompute has duplicate row %d for mapping %d", rowIdx, mappingIdx)
			}
			dupCheck[rowIdx] = struct{}{}
			filteredRows = append(filteredRows, rowIdx)
		}
		if len(filteredRows) == 0 {
			continue
		}
		seenStateByMapping[mappingIdx] = &batchMappingSeenState{
			rows:           filteredRows,
			seen:           make([]uint64, (len(filteredRows)+63)>>6),
			valuesByOutput: makeBatchMappingResultBuffer(len(mapping.ColID), len(filteredRows)),
		}
	}
	workerData.batchUniqueRows = uniqueRows

	if len(uniqueRows) == 0 {
		return nil
	}

	keyDatumBuffer := make([]types.Datum, len(keyColIDs))
	encodedInputKeys, nullByInputRow, err := encodeChunkKeyRowsColumnar(
		typeCtx,
		input,
		keyColIDs,
		keyTypes,
		uniqueRows,
		workerData.batchInputNullByRow,
		workerData.batchInputEncodedKeys,
	)
	if err != nil {
		return err
	}
	workerData.batchInputEncodedKeys = encodedInputKeys
	workerData.batchInputNullByRow = nullByInputRow

	keyIdxByEncoded := make(map[hack.MutableString]int, len(uniqueRows))
	for logicalPos, rowIdx := range uniqueRows {
		encodedKey := encodedInputKeys[logicalPos]
		lookupKey := hack.String(encodedKey)
		keyIdx, exists := keyIdxByEncoded[lookupKey]
		if !exists {
			keyIdx = len(lookupKeys)
			// encodedKey aliases worker scratch; clone before putting into map.
			stableEncodedKey := append([]byte(nil), encodedKey...)
			keyIdxByEncoded[hack.String(stableEncodedKey)] = keyIdx

			inputRow := input.GetRow(rowIdx)
			keyDatums := make([]types.Datum, len(keyDatumBuffer))
			for keyPos, keyColID := range keyColIDs {
				inputRow.DatumWithBuffer(keyColID, keyTypes[keyPos], &keyDatumBuffer[keyPos])
				keyDatumBuffer[keyPos].Copy(&keyDatums[keyPos])
			}
			lookupKeys = append(lookupKeys, &MinMaxBatchLookupContent{Keys: keyDatums})
			refsByLookupKey = append(refsByLookupKey, nil)
		}
		rowKeyIdx[rowIdx] = keyIdx
	}

	for _, mappingIdx := range batchMappings {
		state := seenStateByMapping[mappingIdx]
		if state == nil {
			continue
		}
		for rowPos, rowIdx := range state.rows {
			keyIdx := rowKeyIdx[rowIdx]
			if keyIdx < 0 || keyIdx >= len(lookupKeys) {
				return errors.Errorf("min/max batch recompute has invalid key index %d for mapping %d row %d", keyIdx, mappingIdx, rowIdx)
			}
			refsByLookupKey[keyIdx] = append(refsByLookupKey[keyIdx], batchRowRef{
				mappingIdx: mappingIdx,
				rowIdx:     rowIdx,
				rowPos:     rowPos,
			})
		}
	}

	if len(lookupKeys) == 0 {
		return nil
	}

	batchExec, err := e.MinMaxRecompute.BatchBuilder.Build(ctx, &MinMaxBatchBuildRequest{LookupKeys: lookupKeys})
	if err != nil {
		return err
	}
	if batchExec == nil {
		return errors.New("min/max batch recompute builder returns nil executor")
	}
	defer func() {
		closeErr := batchExec.Close()
		if retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()

	batchTypes := exec.RetTypes(batchExec)
	resultKeyColIdxes := resolveMinMaxKeyResultColIdxes(e.MinMaxRecompute)
	if len(resultKeyColIdxes) != len(keyColIDs) {
		return errors.Errorf(
			"min/max batch recompute key result column count mismatch: key columns=%d, result key columns=%d",
			len(keyColIDs),
			len(resultKeyColIdxes),
		)
	}
	resultKeyTypes := make([]*types.FieldType, len(keyColIDs))
	for keyPos, resultKeyColIdx := range resultKeyColIdxes {
		if resultKeyColIdx < 0 || resultKeyColIdx >= len(batchTypes) {
			return errors.Errorf(
				"min/max batch recompute result key col idx %d out of range [0,%d) at key position %d",
				resultKeyColIdx,
				len(batchTypes),
				keyPos,
			)
		}
		resultKeyTypes[keyPos] = batchTypes[resultKeyColIdx]
	}
	if err := e.validateMinMaxBatchSchemaTypes(
		childTypes,
		keyTypes,
		resultKeyColIdxes,
		resultKeyTypes,
		batchMappings,
		batchTypes,
	); err != nil {
		return err
	}
	resultChk := exec.NewFirstChunk(batchExec)

	for {
		resultChk.Reset()
		if retErr = exec.Next(ctx, batchExec, resultChk); retErr != nil {
			return retErr
		}
		if resultChk.NumRows() == 0 {
			break
		}
		resultRows := buildContiguousRowIdxes(workerData.batchResultRows, resultChk.NumRows())
		workerData.batchResultRows = resultRows
		encodedResultKeys, nullByResultRow, err := encodeChunkKeyRowsColumnar(
			typeCtx,
			resultChk,
			resultKeyColIdxes,
			resultKeyTypes,
			resultRows,
			workerData.batchResultNullByRow,
			workerData.batchResultEncodedKeys,
		)
		if err != nil {
			return err
		}
		workerData.batchResultEncodedKeys = encodedResultKeys
		workerData.batchResultNullByRow = nullByResultRow

		for logicalPos, rowIdx := range resultRows {
			resultRow := resultChk.GetRow(rowIdx)
			encodedKey := encodedResultKeys[logicalPos]
			keyIdx, exists := keyIdxByEncoded[hack.String(encodedKey)]
			if !exists {
				return errors.Errorf("min/max batch recompute returns an unexpected key at result row %d", rowIdx)
			}
			refs := refsByLookupKey[keyIdx]
			if len(refs) == 0 {
				continue
			}
			for _, ref := range refs {
				state := seenStateByMapping[ref.mappingIdx]
				if state == nil {
					return errors.Errorf("min/max batch recompute seen-rows bitmap is missing for mapping %d", ref.mappingIdx)
				}
				if ref.rowPos < 0 || ref.rowPos >= len(state.rows) {
					return errors.Errorf("min/max batch recompute row position %d out of range for mapping %d", ref.rowPos, ref.mappingIdx)
				}
				if bitsetGet(state.seen, ref.rowPos) {
					return errors.Errorf("min/max batch recompute has duplicate result for mapping %d row %d", ref.mappingIdx, ref.rowIdx)
				}
				recomputeMeta := e.AggMappings[ref.mappingIdx].MinMaxRecompute
				if recomputeMeta == nil {
					return errors.Errorf("min/max batch recompute metadata is missing for mapping %d", ref.mappingIdx)
				}
				mapping := e.AggMappings[ref.mappingIdx]
				if err := storeBatchMappingResultAtRowPos(
					state,
					ref.rowPos,
					resultRow,
					childTypes,
					mapping,
					recomputeMeta.BatchResultColIdxes,
					len(batchTypes),
				); err != nil {
					return err
				}
				bitsetSet(state.seen, ref.rowPos)
			}
		}
	}

	for _, mappingIdx := range batchMappings {
		state := seenStateByMapping[mappingIdx]
		if state == nil {
			continue
		}
		for rowPos, rowIdx := range state.rows {
			if !bitsetGet(state.seen, rowPos) {
				return errors.Errorf("min/max batch recompute misses row %d for mapping %d", rowIdx, mappingIdx)
			}
		}
		mapping := e.AggMappings[mappingIdx]
		if err := appendMappingOverrideFromBatchState(overrides, mappingIdx, mapping, state); err != nil {
			return err
		}
	}
	return nil
}

type batchMappingSeenState struct {
	rows           []int
	seen           []uint64
	valuesByOutput [][]types.Datum
}

func bitsetGet(bits []uint64, pos int) bool {
	return bits[pos>>6]&(uint64(1)<<uint(pos&63)) != 0
}

func bitsetSet(bits []uint64, pos int) {
	bits[pos>>6] |= uint64(1) << uint(pos&63)
}

func makeBatchMappingResultBuffer(outputCnt int, rowCnt int) [][]types.Datum {
	valuesByOutput := make([][]types.Datum, outputCnt)
	for idx := range valuesByOutput {
		valuesByOutput[idx] = make([]types.Datum, rowCnt)
	}
	return valuesByOutput
}

func storeBatchMappingResultAtRowPos(
	state *batchMappingSeenState,
	rowPos int,
	resultRow chunk.Row,
	childTypes []*types.FieldType,
	mapping Mapping,
	resultColIdxes []int,
	resultColCnt int,
) error {
	if state == nil {
		return errors.New("min/max batch recompute state is nil")
	}
	if rowPos < 0 || rowPos >= len(state.rows) {
		return errors.Errorf("min/max batch recompute row position %d out of range [0,%d)", rowPos, len(state.rows))
	}
	if len(resultColIdxes) != len(mapping.ColID) {
		return errors.Errorf(
			"batch result column count mismatch for mapping output columns: result=%d mapping=%d",
			len(resultColIdxes),
			len(mapping.ColID),
		)
	}
	if len(state.valuesByOutput) != len(mapping.ColID) {
		return errors.Errorf(
			"batch state output column count mismatch: state=%d mapping=%d",
			len(state.valuesByOutput),
			len(mapping.ColID),
		)
	}
	for colPos, outputColID := range mapping.ColID {
		resultColIdx := resultColIdxes[colPos]
		if resultColIdx < 0 || resultColIdx >= resultColCnt {
			return errors.Errorf(
				"min/max batch recompute result col idx %d out of range [0,%d)",
				resultColIdx,
				resultColCnt,
			)
		}
		if outputColID < 0 || outputColID >= len(childTypes) {
			return errors.Errorf("mapping output col id %d out of range [0,%d)", outputColID, len(childTypes))
		}
		if childTypes[outputColID] == nil {
			return errors.Errorf("mapping output col id %d type is unavailable", outputColID)
		}
		if rowPos >= len(state.valuesByOutput[colPos]) {
			return errors.Errorf(
				"batch state value row position %d out of range [0,%d) for output position %d",
				rowPos,
				len(state.valuesByOutput[colPos]),
				colPos,
			)
		}
		d := resultRow.GetDatum(resultColIdx, childTypes[outputColID])
		d.Copy(&state.valuesByOutput[colPos][rowPos])
	}
	return nil
}

func appendMappingOverrideFromBatchState(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	mapping Mapping,
	state *batchMappingSeenState,
) error {
	if state == nil {
		return errors.New("min/max batch recompute state is nil")
	}
	override, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(state.rows))
	if err != nil {
		return err
	}
	for rowPos, rowIdx := range state.rows {
		override.rowIdxes = append(override.rowIdxes, rowIdx)
		for colPos := range mapping.ColID {
			if rowPos >= len(state.valuesByOutput[colPos]) {
				return errors.Errorf(
					"batch state value row position %d out of range [0,%d) for mapping %d output position %d",
					rowPos,
					len(state.valuesByOutput[colPos]),
					mappingIdx,
					colPos,
				)
			}
			override.valuesByOutput[colPos] = append(override.valuesByOutput[colPos], state.valuesByOutput[colPos][rowPos])
		}
	}
	return nil
}

func buildContiguousRowIdxes(buf []int, rowCnt int) []int {
	if cap(buf) < rowCnt {
		buf = make([]int, rowCnt)
	} else {
		buf = buf[:rowCnt]
	}
	for i := 0; i < rowCnt; i++ {
		buf[i] = i
	}
	return buf
}

func encodeChunkKeyRowsColumnar(
	typeCtx types.Context,
	chk *chunk.Chunk,
	keyColIdxes []int,
	keyTypes []*types.FieldType,
	usedRows []int,
	nullByPhysicalRow []bool,
	keyBuf [][]byte,
) ([][]byte, []bool, error) {
	if len(keyColIdxes) != len(keyTypes) {
		return nil, nullByPhysicalRow, errors.Errorf("key column/type count mismatch: cols=%d types=%d", len(keyColIdxes), len(keyTypes))
	}
	if cap(keyBuf) < len(usedRows) {
		keyBuf = make([][]byte, len(usedRows))
	} else {
		keyBuf = keyBuf[:len(usedRows)]
	}
	for i := range usedRows {
		keyBuf[i] = keyBuf[i][:0]
	}
	if len(usedRows) == 0 {
		return keyBuf, nullByPhysicalRow, nil
	}

	rowCnt := chk.NumRows()
	if cap(nullByPhysicalRow) < rowCnt {
		nullByPhysicalRow = make([]bool, rowCnt)
	} else {
		nullByPhysicalRow = nullByPhysicalRow[:rowCnt]
	}

	for keyPos, keyColIdx := range keyColIdxes {
		if keyColIdx < 0 || keyColIdx >= chk.NumCols() {
			return nil, nullByPhysicalRow, errors.Errorf("key col idx %d out of chunk range [0,%d)", keyColIdx, chk.NumCols())
		}
		col := chk.Column(keyColIdx)
		for logicalPos, physicalRow := range usedRows {
			if physicalRow < 0 || physicalRow >= rowCnt {
				return nil, nullByPhysicalRow, errors.Errorf("physical row idx %d out of chunk row range [0,%d)", physicalRow, rowCnt)
			}
			isNull := col.IsNull(physicalRow)
			nullByPhysicalRow[physicalRow] = isNull
			if isNull {
				keyBuf[logicalPos] = append(keyBuf[logicalPos], 0)
			} else {
				keyBuf[logicalPos] = append(keyBuf[logicalPos], 1)
			}
		}
		if err := codec.SerializeKeys(typeCtx, chk, keyTypes[keyPos], keyColIdx, usedRows, nil, nullByPhysicalRow, codec.KeepVarColumnLength, keyBuf); err != nil {
			return nil, nullByPhysicalRow, err
		}
	}
	return keyBuf, nullByPhysicalRow, nil
}

func (e *Exec) validateMinMaxBatchSchemaTypes(
	childTypes []*types.FieldType,
	keyTypes []*types.FieldType,
	resultKeyColIdxes []int,
	resultKeyTypes []*types.FieldType,
	batchMappings []int,
	batchTypes []*types.FieldType,
) error {
	if len(resultKeyColIdxes) != len(keyTypes) {
		return errors.Errorf(
			"min/max batch key column count mismatch: key types=%d result key indexes=%d",
			len(keyTypes),
			len(resultKeyColIdxes),
		)
	}
	if len(resultKeyTypes) != len(keyTypes) {
		return errors.Errorf(
			"min/max batch key column count mismatch: key types=%d result key types=%d",
			len(keyTypes),
			len(resultKeyTypes),
		)
	}

	keyResultSet := make(map[int]struct{}, len(resultKeyColIdxes))
	for i := range keyTypes {
		keyTp := keyTypes[i]
		resultColIdx := resultKeyColIdxes[i]
		if resultColIdx < 0 || resultColIdx >= len(batchTypes) {
			return errors.Errorf(
				"min/max batch result key col idx %d out of schema range [0,%d) at key position %d",
				resultColIdx,
				len(batchTypes),
				i,
			)
		}
		if _, dup := keyResultSet[resultColIdx]; dup {
			return errors.Errorf("duplicate min/max batch result key col idx %d", resultColIdx)
		}
		keyResultSet[resultColIdx] = struct{}{}

		resultTp := resultKeyTypes[i]
		if keyTp == nil || resultTp == nil {
			return errors.Errorf("min/max batch key type is unavailable at position %d", i)
		}
		if !resultTp.Equal(batchTypes[resultColIdx]) {
			return errors.Errorf(
				"min/max batch key type mismatch at position %d: result key type=%s batch schema=%s",
				i,
				resultTp.String(),
				batchTypes[resultColIdx].String(),
			)
		}
		if !keyTp.Equal(resultTp) {
			return errors.Errorf(
				"min/max batch key type mismatch at position %d: expected=%s result=%s",
				i,
				keyTp.String(),
				resultTp.String(),
			)
		}
	}

	for _, mappingIdx := range batchMappings {
		if mappingIdx < 0 || mappingIdx >= len(e.AggMappings) {
			return errors.Errorf("min/max batch mapping idx %d out of range [0,%d)", mappingIdx, len(e.AggMappings))
		}
		mapping := e.AggMappings[mappingIdx]
		minMaxRecompute := mapping.MinMaxRecompute
		if minMaxRecompute == nil {
			return errors.Errorf("min/max batch recompute metadata is missing for mapping %d", mappingIdx)
		}
		if len(minMaxRecompute.BatchResultColIdxes) != len(mapping.ColID) {
			return errors.Errorf(
				"min/max batch result column count mismatch for mapping %d: BatchResultColIdxes=%d Mapping.ColID=%d",
				mappingIdx,
				len(minMaxRecompute.BatchResultColIdxes),
				len(mapping.ColID),
			)
		}
		for pos, resultColIdx := range minMaxRecompute.BatchResultColIdxes {
			if _, conflictsWithKey := keyResultSet[resultColIdx]; conflictsWithKey {
				return errors.Errorf(
					"min/max batch result col idx %d for mapping %d conflicts with key prefix/result columns",
					resultColIdx,
					mappingIdx,
				)
			}
			if resultColIdx < 0 || resultColIdx >= len(batchTypes) {
				return errors.Errorf(
					"min/max batch result col idx %d out of schema range [0,%d) for mapping %d",
					resultColIdx,
					len(batchTypes),
					mappingIdx,
				)
			}
			outputColID := mapping.ColID[pos]
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("mapping %d output col id %d out of range [0,%d)", mappingIdx, outputColID, len(childTypes))
			}
			outputTp := childTypes[outputColID]
			if outputTp == nil || batchTypes[resultColIdx] == nil {
				return errors.Errorf(
					"min/max batch result type is unavailable for mapping %d: output_col=%d result_col=%d",
					mappingIdx,
					outputColID,
					resultColIdx,
				)
			}
			if !outputTp.Equal(batchTypes[resultColIdx]) {
				return errors.Errorf(
					"min/max batch result type mismatch for mapping %d output position %d: output=%s result=%s",
					mappingIdx,
					pos,
					outputTp.String(),
					batchTypes[resultColIdx].String(),
				)
			}
		}
	}
	return nil
}

func (e *Exec) applyMinMaxRecomputeOverrides(
	computedByColID []*chunk.Column,
	childTypes []*types.FieldType,
	overrides []*mappingRecomputeOverride,
) error {
	for mappingIdx, override := range overrides {
		if override == nil || len(override.rowIdxes) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		if len(override.valuesByOutput) != len(mapping.ColID) {
			return errors.Errorf(
				"min/max override output count mismatch for mapping %d: override=%d mapping=%d",
				mappingIdx,
				len(override.valuesByOutput),
				len(mapping.ColID),
			)
		}
		for colPos, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(computedByColID) {
				return errors.Errorf("min/max override output col id %d out of range [0,%d)", outputColID, len(computedByColID))
			}
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("min/max override output col id %d out of field type range [0,%d)", outputColID, len(childTypes))
			}
			if len(override.valuesByOutput[colPos]) != len(override.rowIdxes) {
				return errors.Errorf(
					"min/max override row/value count mismatch for mapping %d output position %d: rows=%d values=%d",
					mappingIdx,
					colPos,
					len(override.rowIdxes),
					len(override.valuesByOutput[colPos]),
				)
			}
			oldCol := computedByColID[outputColID]
			if oldCol == nil {
				return errors.Errorf("min/max override target output col %d is nil", outputColID)
			}
			ft := childTypes[outputColID]
			if ft == nil {
				return errors.Errorf("min/max override output col %d type is unavailable", outputColID)
			}
			newCol, err := rebuildColumnWithOverrides(oldCol, ft, override.rowIdxes, override.valuesByOutput[colPos])
			if err != nil {
				return err
			}
			computedByColID[outputColID] = newCol
		}
	}
	return nil
}

type mappingRecomputeOverride struct {
	rowIdxes       []int
	valuesByOutput [][]types.Datum
}

type batchRowRef struct {
	mappingIdx int
	rowIdx     int
	rowPos     int
}

func ensureMappingOverride(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	outputCnt int,
	rowCap int,
) (*mappingRecomputeOverride, error) {
	if mappingIdx < 0 || mappingIdx >= len(overrides) {
		return nil, errors.Errorf("mapping idx %d out of override range [0,%d)", mappingIdx, len(overrides))
	}
	if outputCnt < 0 {
		return nil, errors.Errorf("output count must be non-negative, got %d", outputCnt)
	}
	if rowCap < 0 {
		rowCap = 0
	}
	override := overrides[mappingIdx]
	if override == nil {
		override = &mappingRecomputeOverride{
			rowIdxes:       make([]int, 0, rowCap),
			valuesByOutput: make([][]types.Datum, outputCnt),
		}
		for i := range override.valuesByOutput {
			override.valuesByOutput[i] = make([]types.Datum, 0, rowCap)
		}
		overrides[mappingIdx] = override
		return override, nil
	}
	if len(override.valuesByOutput) != outputCnt {
		return nil, errors.Errorf(
			"override output column count mismatch for mapping %d: override=%d values=%d",
			mappingIdx,
			len(override.valuesByOutput),
			outputCnt,
		)
	}
	return override, nil
}

func appendMappingOverrideFromValues(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	rowIdx int,
	values []types.Datum,
) error {
	override, err := ensureMappingOverride(overrides, mappingIdx, len(values), 0)
	if err != nil {
		return err
	}
	override.rowIdxes = append(override.rowIdxes, rowIdx)
	for idx := range values {
		var copied types.Datum
		values[idx].Copy(&copied)
		override.valuesByOutput[idx] = append(override.valuesByOutput[idx], copied)
	}
	return nil
}

func rebuildColumnWithOverrides(
	oldCol *chunk.Column,
	ft *types.FieldType,
	rowIdxes []int,
	values []types.Datum,
) (*chunk.Column, error) {
	if oldCol == nil {
		return nil, errors.New("cannot rebuild nil column")
	}
	if len(rowIdxes) != len(values) {
		return nil, errors.Errorf("override row/value count mismatch: rows=%d values=%d", len(rowIdxes), len(values))
	}
	rowCnt := oldCol.Rows()
	newCol := chunk.NewColumn(ft, rowCnt)
	overridePos := 0
	prevOverrideRow := -1
	for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
		if overridePos < len(rowIdxes) {
			overrideRow := rowIdxes[overridePos]
			if overrideRow < prevOverrideRow {
				return nil, errors.Errorf("override rows are not in ascending order: prev=%d curr=%d", prevOverrideRow, overrideRow)
			}
			if overrideRow < rowIdx {
				return nil, errors.Errorf("override row %d repeats or is out of order", overrideRow)
			}
			if overrideRow == rowIdx {
				if err := appendDatumToColumn(newCol, &values[overridePos], ft); err != nil {
					return nil, err
				}
				prevOverrideRow = overrideRow
				overridePos++
				continue
			}
		}
		newCol.AppendCellNTimes(oldCol, rowIdx, 1)
	}
	if overridePos != len(rowIdxes) {
		return nil, errors.Errorf("override rows out of range: consumed=%d total=%d rows=%d", overridePos, len(rowIdxes), rowCnt)
	}
	return newCol, nil
}

func appendDatumToColumn(col *chunk.Column, d *types.Datum, ft *types.FieldType) error {
	if d == nil || d.IsNull() {
		col.AppendNull()
		return nil
	}
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			col.AppendUint64(d.GetUint64())
		} else {
			col.AppendInt64(d.GetInt64())
		}
	case mysql.TypeYear:
		col.AppendInt64(d.GetInt64())
	case mysql.TypeFloat:
		col.AppendFloat32(d.GetFloat32())
	case mysql.TypeDouble:
		col.AppendFloat64(d.GetFloat64())
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		col.AppendString(d.GetString())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		col.AppendTime(d.GetMysqlTime())
	case mysql.TypeDuration:
		col.AppendDuration(d.GetMysqlDuration())
	case mysql.TypeNewDecimal:
		col.AppendMyDecimal(d.GetMysqlDecimal())
	case mysql.TypeEnum:
		col.AppendEnum(d.GetMysqlEnum())
	case mysql.TypeSet:
		col.AppendSet(d.GetMysqlSet())
	case mysql.TypeBit:
		col.AppendBytes(d.GetBytes())
	case mysql.TypeJSON:
		col.AppendJSON(d.GetMysqlJSON())
	case mysql.TypeTiDBVectorFloat32:
		col.AppendVectorFloat32(d.GetVectorFloat32())
	default:
		return errors.Errorf("unsupported MySQL type %d when appending recompute datum", ft.GetType())
	}
	return nil
}

func (e *Exec) buildRowOps(input *chunk.Chunk, computedByColID []*chunk.Column, workerData *mvMergeAggWorkerData) ([]RowOp, []uint8, int, int, error) {
	if len(e.aggOutputColIDs) == 0 {
		return nil, nil, 0, 0, errors.New("no aggregate outputs in Exec")
	}
	countStarOutputColID := e.aggOutputColIDs[0]
	if countStarOutputColID < 0 || countStarOutputColID >= input.NumCols() {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d out of input chunk range", countStarOutputColID)
	}
	if countStarOutputColID >= len(computedByColID) {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d out of computed-by-col-id range [0,%d)", countStarOutputColID, len(computedByColID))
	}
	newCountStarCol := computedByColID[countStarOutputColID]
	if newCountStarCol == nil {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d is missing in computed columns", countStarOutputColID)
	}

	oldCountStarCol := input.Column(countStarOutputColID)
	newCountStarVals := newCountStarCol.Int64s()
	rowOps := make([]RowOp, 0, input.NumRows())

	var updateRows []int
	var updateOpIndexes []int
	if workerData != nil {
		updateRows = workerData.updateRows[:0]
		updateOpIndexes = workerData.updateOpIndexes[:0]
	}
	for rowIdx := 0; rowIdx < input.NumRows(); rowIdx++ {
		newCount := newCountStarVals[rowIdx]
		if newCount < 0 {
			return nil, nil, 0, 0, errors.Errorf("count(*) becomes negative (%d) at row %d", newCount, rowIdx)
		}
		oldExists := !oldCountStarCol.IsNull(rowIdx)

		switch {
		case newCount == 0:
			if oldExists {
				rowOps = append(rowOps, RowOp{RowIdx: rowIdx, Tp: RowOpDelete, updateOrdinal: -1})
			}
		case !oldExists:
			rowOps = append(rowOps, RowOp{RowIdx: rowIdx, Tp: RowOpInsert, updateOrdinal: -1})
		default:
			updateOrdinal := len(updateRows)
			rowOps = append(rowOps, RowOp{
				RowIdx:        rowIdx,
				Tp:            RowOpUpdate,
				updateOrdinal: int32(updateOrdinal),
			})
			updateRows = append(updateRows, rowIdx)
			updateOpIndexes = append(updateOpIndexes, len(rowOps)-1)
		}
	}

	if workerData != nil {
		workerData.updateRows = updateRows
		workerData.updateOpIndexes = updateOpIndexes
	}

	updateTouchedBitCnt := len(e.aggOutputColIDs)
	updateTouchedStride := (updateTouchedBitCnt + 7) >> 3
	updateCnt := len(updateRows)
	if updateCnt == 0 {
		return rowOps, nil, updateTouchedStride, updateTouchedBitCnt, nil
	}
	updateTouchedBitmap := make([]uint8, updateCnt*updateTouchedStride)

	var updateChanged []bool
	if workerData != nil {
		updateChanged = workerData.updateChanged
	}
	if cap(updateChanged) < updateCnt {
		updateChanged = make([]bool, updateCnt)
	} else {
		updateChanged = updateChanged[:updateCnt]
		clear(updateChanged)
	}
	if workerData != nil {
		workerData.updateChanged = updateChanged
	}

	fieldTypes := e.Children(0).RetFieldTypes()
	for bitPos, colID := range e.aggOutputColIDs {
		if colID < 0 || colID >= input.NumCols() {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of input chunk range", colID)
		}
		if colID >= len(computedByColID) {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of computed-by-col-id range [0,%d)", colID, len(computedByColID))
		}
		if colID >= len(fieldTypes) {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of field type range [0,%d)", colID, len(fieldTypes))
		}
		oldCol := input.Column(colID)
		newCol := computedByColID[colID]
		if newCol == nil {
			return nil, nil, 0, 0, errors.Errorf("computed agg col %d is nil", colID)
		}
		if err := markUpdateTouchedRowsByColumn(
			updateRows,
			updateChanged,
			updateTouchedBitmap,
			updateTouchedStride,
			bitPos,
			oldCol,
			newCol,
			fieldTypes[colID],
		); err != nil {
			return nil, nil, 0, 0, err
		}
	}

	for updateOrdinal, opIdx := range updateOpIndexes {
		if !updateChanged[updateOrdinal] {
			rowOps[opIdx].Tp = RowOpNoOp
		}
	}
	return rowOps, updateTouchedBitmap, updateTouchedStride, updateTouchedBitCnt, nil
}

func markUpdateTouchedRowsByColumn(
	updateRows []int,
	updateChanged []bool,
	updateTouchedBitmap []uint8,
	updateTouchedStride int,
	updateBitPos int,
	oldCol *chunk.Column,
	newCol *chunk.Column,
	ft *types.FieldType,
) error {
	if ft == nil {
		return errors.New("field type is nil when comparing aggregate outputs")
	}
	if len(updateRows) == 0 {
		return nil
	}
	bitMask := uint8(1 << (updateBitPos & 7))
	bitByteOffset := updateBitPos >> 3
	singleByteStride := updateTouchedStride == 1

	switch ft.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						updateChanged[updateOrdinal] = true
						if singleByteStride {
							updateTouchedBitmap[updateOrdinal] |= bitMask
						} else {
							updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
						}
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETReal:
		if ft.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						updateChanged[updateOrdinal] = true
						if singleByteStride {
							updateTouchedBitmap[updateOrdinal] |= bitMask
						} else {
							updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
						}
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETString:
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in aggregate change comparison", ft.EvalType())
	}
}

func markChangedRowsByColumn(changedMask []bool, oldCol, newCol *chunk.Column, ft *types.FieldType) error {
	if ft == nil {
		return errors.New("field type is nil when comparing aggregate outputs")
	}
	rowCnt := len(changedMask)
	switch ft.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						changedMask[rowIdx] = true
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					changedMask[rowIdx] = true
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETReal:
		if ft.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						changedMask[rowIdx] = true
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					changedMask[rowIdx] = true
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETString:
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				changedMask[rowIdx] = true
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in aggregate change comparison", ft.EvalType())
	}
}

func chunkRowColDatum(col *chunk.Column, rowIdx int, ft *types.FieldType) types.Datum {
	var d types.Datum
	if col.IsNull(rowIdx) {
		d.SetNull()
		return d
	}
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			d.SetUint64(col.GetUint64(rowIdx))
		} else {
			d.SetInt64(col.GetInt64(rowIdx))
		}
	case mysql.TypeYear:
		d.SetInt64(col.GetInt64(rowIdx))
	case mysql.TypeFloat:
		d.SetFloat32(col.GetFloat32(rowIdx))
	case mysql.TypeDouble:
		d.SetFloat64(col.GetFloat64(rowIdx))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString(col.GetString(rowIdx), ft.GetCollate())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		d.SetMysqlTime(col.GetTime(rowIdx))
	case mysql.TypeDuration:
		d.SetMysqlDuration(col.GetDuration(rowIdx, ft.GetDecimal()))
	case mysql.TypeNewDecimal:
		dec := col.GetDecimal(rowIdx)
		d.SetMysqlDecimal(dec)
		d.SetLength(ft.GetFlen())
		if ft.GetDecimal() == types.UnspecifiedLength {
			d.SetFrac(int(dec.GetDigitsFrac()))
		} else {
			d.SetFrac(ft.GetDecimal())
		}
	case mysql.TypeEnum:
		d.SetMysqlEnum(col.GetEnum(rowIdx), ft.GetCollate())
	case mysql.TypeSet:
		d.SetMysqlSet(col.GetSet(rowIdx), ft.GetCollate())
	case mysql.TypeBit:
		d.SetMysqlBit(col.GetBytes(rowIdx))
	case mysql.TypeJSON:
		d.SetMysqlJSON(col.GetJSON(rowIdx))
	case mysql.TypeTiDBVectorFloat32:
		d.SetVectorFloat32(col.GetVectorFloat32(rowIdx))
	default:
		d.SetBytes(col.GetRaw(rowIdx))
	}
	return d
}
