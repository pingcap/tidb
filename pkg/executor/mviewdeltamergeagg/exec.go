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

package mviewdeltamergeagg

import (
	"bytes"
	"context"
	"sort"
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
	"github.com/pingcap/tidb/pkg/util/collate"
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
	// MinMaxRecomputeUnknown is the zero value and means the strategy is unset or invalid.
	MinMaxRecomputeUnknown MinMaxRecomputeStrategy = iota
	// MinMaxRecomputeSingleRow recomputes one group key per execution.
	MinMaxRecomputeSingleRow
	// MinMaxRecomputeBatch recomputes multiple group keys in one execution.
	MinMaxRecomputeBatch
)

// The default max_chunk_size is 1024, so 4 * 1024 = 4096 is enough for most cases.
const globalContiguousRowIdxCap = 4096

var globalContiguousRowIdxes = initContiguousRowIdxes(globalContiguousRowIdxCap)

func initContiguousRowIdxes(rowCnt int) []int {
	rowIdxes := make([]int, rowCnt)
	for i := 0; i < rowCnt; i++ {
		rowIdxes[i] = i
	}
	return rowIdxes
}

// MinMaxBatchLookupContent is one batch lookup key.
type MinMaxBatchLookupContent struct {
	// Keys are group-key datums in planner-defined key order.
	Keys []types.Datum
}

// MinMaxBatchBuildRequest is the input to build one batch recompute executor.
type MinMaxBatchBuildRequest struct {
	// LookupKeys are deduplicated keys for this batch.
	LookupKeys []MinMaxBatchLookupContent
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
	// Workers has one slot per MView merge worker.
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
	KeyResultColIdxes []int
	// BatchBuilder creates one batch recompute executor.
	// Build must be safe for concurrent calls across MView merge workers.
	BatchBuilder MinMaxBatchExecBuilder
}

// RowOpType is the row write operation on MView table.
type RowOpType uint8

const (
	// RowOpNoOp means there is no MView row to touch.
	RowOpNoOp RowOpType = iota
	// RowOpInsert means insert a new MView row.
	RowOpInsert
	// RowOpUpdate means update an existing MView row.
	RowOpUpdate
	// RowOpDelete means delete an existing MView row.
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

// Exec is the sink executor for incremental MView merge.
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

	compiledMergers      []aggMerger
	compiledOutputColCnt int
	aggOutputColIDs      []int
	keyTypes             []*types.FieldType
	prepared             bool
	executed             bool
	runtimeStats         *mergeRuntimeStats
}

type mergeWorkerData struct {
	// minMaxMappingIdxToRows[mappingIdx] stores recompute row indexes for one mapping.
	// Each inner slice must be strictly increasing and in chunk row range.
	minMaxMappingIdxToRows            [][]int
	minMaxMappingIdxToOverrides       []*mappingRecomputeOverride
	minMaxBatchMappingIdxes           []int
	minMaxUniqueRows                  []int
	minMaxResultRows                  []int
	minMaxRowSeen                     []bool
	minMaxRowIdxToLookupKeyPos        []int
	minMaxLookupKeyContents           []MinMaxBatchLookupContent
	minMaxEncodedKeyToLookupKeyPosMap map[hack.MutableString]int
	minMaxLookupKeyPosToRowRefs       [][]minMaxRowRef
	minMaxLookupKeyDatums             []types.Datum
	minMaxInputNullRows               []bool
	minMaxInputEncodedKeys            [][]byte
	minMaxResultNullRows              []bool
	minMaxResultEncodedKeys           [][]byte
	// For building update operations.
	updateRows      []int
	updateOpIndexes []int
	updateChanged   []bool
}

type mergeRuntimeStats struct {
	readerTime      time.Duration
	writerTime      time.Duration
	mergeWorkerTime []time.Duration
	writerDetail    mergeWriterStats
}

type mergeWriterStats struct {
	chunks int64
	rowOps int64

	noopRows   int64
	insertRows int64
	updateRows int64
	deleteRows int64
}

func (s *mergeWriterStats) merge(other mergeWriterStats) {
	s.chunks += other.chunks
	s.rowOps += other.rowOps
	s.noopRows += other.noopRows
	s.insertRows += other.insertRows
	s.updateRows += other.updateRows
	s.deleteRows += other.deleteRows
}

func newMergeRuntimeStats(workerCnt int) *mergeRuntimeStats {
	if workerCnt < 0 {
		workerCnt = 0
	}
	return &mergeRuntimeStats{
		mergeWorkerTime: make([]time.Duration, workerCnt),
	}
}

func (s *mergeRuntimeStats) reset(workerCnt int) {
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

func (s *mergeRuntimeStats) copyFrom(stats *mergeRuntimeStats) {
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

func (s *mergeRuntimeStats) String() string {
	if s == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("mview_delta_merge_agg:{merge_worker:{total:")
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

func (s *mergeRuntimeStats) Clone() execdetails.RuntimeStats {
	if s == nil {
		return &mergeRuntimeStats{}
	}
	newStats := &mergeRuntimeStats{
		readerTime:      s.readerTime,
		writerTime:      s.writerTime,
		mergeWorkerTime: make([]time.Duration, len(s.mergeWorkerTime)),
		writerDetail:    s.writerDetail,
	}
	copy(newStats.mergeWorkerTime, s.mergeWorkerTime)
	return newStats
}

func (s *mergeRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*mergeRuntimeStats)
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

func (*mergeRuntimeStats) Tp() int {
	return execdetails.TpMViewDeltaMergeAggRuntimeStats
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
			e.runtimeStats = newMergeRuntimeStats(e.WorkerCnt)
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
	var stats *mergeRuntimeStats
	if e.runtimeStats != nil {
		stats = &mergeRuntimeStats{
			mergeWorkerTime: make([]time.Duration, workerCnt),
		}
		if statsWriter, ok := e.Writer.(writerRuntimeStatsAware); ok {
			statsWriter.setRuntimeStats(&stats.writerDetail)
		}
		defer e.runtimeStats.copyFrom(stats)
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
		return e.runReader(gctx, inputCh, freeInputCh, stats)
	})

	workerWG := sync.WaitGroup{}
	workerWG.Add(workerCnt)
	for i := 0; i < workerCnt; i++ {
		workerIdx := i
		g.Go(func() error {
			defer workerWG.Done()
			return e.runWorker(gctx, inputCh, resultCh, workerIdx, stats)
		})
	}

	go func() {
		workerWG.Wait()
		close(resultCh)
	}()

	g.Go(func() error {
		return e.runWriter(gctx, resultCh, freeInputCh, stats)
	})

	return g.Wait()
}

func (e *Exec) runReader(
	ctx context.Context,
	inputCh chan<- *chunk.Chunk,
	freeInputCh <-chan *chunk.Chunk,
	stats *mergeRuntimeStats,
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
	stats *mergeRuntimeStats,
) error {
	var workerData mergeWorkerData
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
	stats *mergeRuntimeStats,
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
	keyTypes := make([]*types.FieldType, len(meta.KeyInputColIDs))
	seenKey := make(map[int]struct{}, len(meta.KeyInputColIDs))
	for keyPos, keyColID := range meta.KeyInputColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("MinMaxRecompute key col %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if _, dup := seenKey[keyColID]; dup {
			return errors.Errorf("duplicate MinMaxRecompute key col %d", keyColID)
		}
		keyTp := childTypes[keyColID]
		if keyTp == nil {
			return errors.Errorf("MinMaxRecompute key col %d type is unavailable", keyColID)
		}
		keyTypes[keyPos] = keyTp
		seenKey[keyColID] = struct{}{}
	}
	e.keyTypes = keyTypes
	keyResultColIdxes := meta.KeyResultColIdxes
	if len(keyResultColIdxes) != len(meta.KeyInputColIDs) {
		return errors.Errorf(
			"MinMaxRecompute key result column count mismatch: expect %d, got %d",
			len(meta.KeyInputColIDs),
			len(keyResultColIdxes),
		)
	}
	clear(seenKey)
	seenKeyResult := seenKey
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
				for keyPos, keyCol := range worker.KeyCols {
					if keyCol == nil || keyCol.Data == nil {
						return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d key column %d is not initialized", mappingIdx, workerIdx, keyPos)
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
		return errors.New("MViewDeltaMergeAgg mapping requires AggFunc")
	}
	if !isFirstAggCountAllRows(firstAgg) {
		return errors.Errorf("the first MViewDeltaMergeAgg mapping must be COUNT(*)/COUNT(non-NULL constant), got %s", aggFuncForErr(firstAgg))
	}
	if len(e.AggMappings[0].ColID) != 1 {
		return errors.Errorf("the first MViewDeltaMergeAgg mapping must output exactly 1 column, got %d", len(e.AggMappings[0].ColID))
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
						"AggMappings[%d] depends on MIN/MAX output col %d, which is unsupported",
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
			return errors.New("MViewDeltaMergeAgg mapping requires AggFunc")
		}
		aggName := mapping.AggFunc.Name
		if len(mapping.ColID) == 0 {
			return errors.Errorf("mapping for agg=%s must output at least 1 column", aggName)
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
			if _, dup := colID2ComputedIdx[outputColID]; dup {
				return errors.Errorf("duplicate output col id %d in AggMappings", outputColID)
			}
			colID2ComputedIdx[outputColID] = e.compiledOutputColCnt
			e.compiledOutputColCnt++
			e.aggOutputColIDs = append(e.aggOutputColIDs, outputColID)
		}
		e.compiledMergers = append(e.compiledMergers, merger)
	}

	return nil
}

func (e *Exec) mergeOneChunk(ctx context.Context, chk *chunk.Chunk, workerData *mergeWorkerData, workerIdx int) (*ChunkResult, error) {
	if workerData == nil {
		return nil, errors.New("merge worker data is nil")
	}
	workerData.prepareMinMaxRecompute(len(e.AggMappings))
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

func (d *mergeWorkerData) prepareMinMaxRecompute(mappingCnt int) {
	if mappingCnt <= 0 {
		return
	}
	if cap(d.minMaxMappingIdxToRows) < mappingCnt {
		d.minMaxMappingIdxToRows = make([][]int, mappingCnt)
	} else {
		d.minMaxMappingIdxToRows = d.minMaxMappingIdxToRows[:mappingCnt]
	}
	for idx := range d.minMaxMappingIdxToRows {
		d.minMaxMappingIdxToRows[idx] = d.minMaxMappingIdxToRows[idx][:0]
	}
	if cap(d.minMaxMappingIdxToOverrides) < mappingCnt {
		d.minMaxMappingIdxToOverrides = make([]*mappingRecomputeOverride, mappingCnt)
	} else {
		d.minMaxMappingIdxToOverrides = d.minMaxMappingIdxToOverrides[:mappingCnt]
	}
	for idx := range d.minMaxMappingIdxToOverrides {
		override := d.minMaxMappingIdxToOverrides[idx]
		if override == nil {
			continue
		}
		override.rowIdxes = override.rowIdxes[:0]
		for colPos := range override.valuesByOutput {
			override.valuesByOutput[colPos] = override.valuesByOutput[colPos][:0]
		}
	}
	d.minMaxBatchMappingIdxes = d.minMaxBatchMappingIdxes[:0]
}

func (e *Exec) recomputeMinMaxRows(
	ctx context.Context,
	input *chunk.Chunk,
	computedByColID []*chunk.Column,
	workerData *mergeWorkerData,
	workerIdx int,
) error {
	if e.MinMaxRecompute == nil || workerData == nil {
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
	childTypes := e.Children(0).RetFieldTypes()
	keyColIDs := e.MinMaxRecompute.KeyInputColIDs
	keyTypes := e.keyTypes
	overrides := workerData.minMaxMappingIdxToOverrides
	batchMappings := workerData.minMaxBatchMappingIdxes[:0]

	for mappingIdx, rows := range workerData.minMaxMappingIdxToRows {
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
				keyColIDs,
				keyTypes,
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
	workerData.minMaxBatchMappingIdxes = batchMappings
	if len(batchMappings) > 0 {
		if err := e.recomputeMinMaxBatch(
			ctx,
			input,
			childTypes,
			keyColIDs,
			keyTypes,
			batchMappings,
			workerData,
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
	keyColIDs []int,
	keyTypes []*types.FieldType,
	workerIdx int,
	mappingIdx int,
	recomputeRows []int,
	overrides []*mappingRecomputeOverride,
) error {
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
	mapping := e.AggMappings[mappingIdx]

	override, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(recomputeRows))
	if err != nil {
		return err
	}
	override.rowIdxes = recomputeRows
	for colPos := range mapping.ColID {
		if cap(override.valuesByOutput[colPos]) < len(recomputeRows) {
			override.valuesByOutput[colPos] = make([]types.Datum, len(recomputeRows))
		} else {
			override.valuesByOutput[colPos] = override.valuesByOutput[colPos][:len(recomputeRows)]
		}
	}

	rowCnt := input.NumRows()
	resultChk := exec.NewFirstChunk(worker.Exec)

	for rowPos, rowIdx := range recomputeRows {
		if rowIdx < 0 || rowIdx >= rowCnt {
			return errors.Errorf("min/max single-row recompute row idx %d out of range [0,%d)", rowIdx, rowCnt)
		}
		inputRow := input.GetRow(rowIdx)
		for keyPos, keyColID := range keyColIDs {
			inputRow.DatumWithBuffer(keyColID, keyTypes[keyPos], worker.KeyCols[keyPos].Data)
		}

		if err := exec.Open(ctx, worker.Exec); err != nil {
			return err
		}
		resultChk.Reset()
		nextErr := exec.Next(ctx, worker.Exec, resultChk)
		var retErr error
		if nextErr != nil {
			retErr = nextErr
		} else if resultChk.NumRows() == 0 {
			retErr = errors.Errorf("min/max single-row recompute returns no row for mapping %d row %d", mappingIdx, rowIdx)
		} else if resultChk.NumRows() > 1 {
			retErr = errors.Errorf("min/max single-row recompute returns more than one row for mapping %d row %d", mappingIdx, rowIdx)
		}
		if retErr == nil {
			resultRow := resultChk.GetRow(0)
			for colPos, outputColID := range mapping.ColID {
				d := resultRow.GetDatum(colPos, childTypes[outputColID])
				d.Copy(&override.valuesByOutput[colPos][rowPos])
			}
			resultChk.Reset()
			nextErr = exec.Next(ctx, worker.Exec, resultChk)
			if nextErr != nil {
				retErr = nextErr
			} else if resultChk.NumRows() != 0 {
				retErr = errors.Errorf("min/max single-row recompute returns more than one row for mapping %d row %d", mappingIdx, rowIdx)
			}
		}

		closeErr := worker.Exec.Close()
		if retErr != nil {
			return retErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (e *Exec) recomputeMinMaxBatch(
	ctx context.Context,
	input *chunk.Chunk,
	childTypes []*types.FieldType,
	keyColIDs []int,
	keyTypes []*types.FieldType,
	batchMappings []int,
	workerData *mergeWorkerData,
	overrides []*mappingRecomputeOverride,
) (retErr error) {
	if e.MinMaxRecompute.BatchBuilder == nil {
		return errors.New("min/max batch recompute requires BatchBuilder")
	}

	rowCnt := input.NumRows()
	seen := workerData.minMaxRowSeen
	if cap(seen) < rowCnt {
		seen = make([]bool, rowCnt)
	} else {
		seen = seen[:rowCnt]
		clear(seen)
	}
	workerData.minMaxRowSeen = seen

	uniqueRows := workerData.minMaxUniqueRows[:0]
	for _, mappingIdx := range batchMappings {
		rows := workerData.minMaxMappingIdxToRows[mappingIdx]
		if len(rows) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		for _, rowIdx := range rows {
			if rowIdx < 0 || rowIdx >= rowCnt {
				return errors.Errorf("min/max batch recompute row idx %d out of range [0,%d)", rowIdx, rowCnt)
			}
			if !seen[rowIdx] {
				uniqueRows = append(uniqueRows, rowIdx)
				seen[rowIdx] = true
			}
		}
		override := overrides[mappingIdx]
		if override == nil {
			override = &mappingRecomputeOverride{
				rowIdxes:       make([]int, 0, len(rows)),
				valuesByOutput: make([][]types.Datum, len(mapping.ColID)),
			}
			for i := range override.valuesByOutput {
				override.valuesByOutput[i] = make([]types.Datum, 0, len(rows))
			}
			overrides[mappingIdx] = override
		}
		override.rowIdxes = rows
		for colPos := range mapping.ColID {
			if cap(override.valuesByOutput[colPos]) < len(rows) {
				override.valuesByOutput[colPos] = make([]types.Datum, len(rows))
			} else {
				override.valuesByOutput[colPos] = override.valuesByOutput[colPos][:len(rows)]
			}
		}
	}
	sort.Ints(uniqueRows)
	workerData.minMaxUniqueRows = uniqueRows

	if len(uniqueRows) == 0 {
		return nil
	}

	rowIdxToLookupKeyPos := workerData.minMaxRowIdxToLookupKeyPos
	if cap(rowIdxToLookupKeyPos) < rowCnt {
		rowIdxToLookupKeyPos = make([]int, rowCnt)
	} else {
		rowIdxToLookupKeyPos = rowIdxToLookupKeyPos[:rowCnt]
	}
	workerData.minMaxRowIdxToLookupKeyPos = rowIdxToLookupKeyPos

	lookupKeyContents := workerData.minMaxLookupKeyContents
	if cap(lookupKeyContents) < len(uniqueRows) {
		lookupKeyContents = make([]MinMaxBatchLookupContent, len(uniqueRows))
	} else {
		lookupKeyContents = lookupKeyContents[:len(uniqueRows)]
	}
	workerData.minMaxLookupKeyContents = lookupKeyContents

	lookupKeyPosToRowRefs := workerData.minMaxLookupKeyPosToRowRefs
	if cap(lookupKeyPosToRowRefs) < len(uniqueRows) {
		lookupKeyPosToRowRefs = make([][]minMaxRowRef, len(uniqueRows))
	} else {
		lookupKeyPosToRowRefs = lookupKeyPosToRowRefs[:len(uniqueRows)]
	}
	workerData.minMaxLookupKeyPosToRowRefs = lookupKeyPosToRowRefs

	keyColCnt := len(keyColIDs)
	typeCtx := e.Ctx().GetSessionVars().StmtCtx.TypeCtx()
	encodedInputKeys, inputNullRows, err := encodeChunkKeyRows(
		typeCtx,
		input,
		keyColIDs,
		keyTypes,
		uniqueRows,
		workerData.minMaxInputNullRows,
		workerData.minMaxInputEncodedKeys,
	)
	if err != nil {
		return err
	}
	workerData.minMaxInputEncodedKeys = encodedInputKeys
	workerData.minMaxInputNullRows = inputNullRows

	encodedKeyToLookupKeyPos := workerData.minMaxEncodedKeyToLookupKeyPosMap
	if encodedKeyToLookupKeyPos == nil {
		encodedKeyToLookupKeyPos = make(map[hack.MutableString]int, len(uniqueRows))
	} else {
		clear(encodedKeyToLookupKeyPos)
	}
	workerData.minMaxEncodedKeyToLookupKeyPosMap = encodedKeyToLookupKeyPos

	totalLookupKeyDatums := len(uniqueRows) * keyColCnt
	flatLookupKeyDatums := workerData.minMaxLookupKeyDatums
	if len(flatLookupKeyDatums) < totalLookupKeyDatums {
		if cap(flatLookupKeyDatums) < totalLookupKeyDatums {
			flatLookupKeyDatums = make([]types.Datum, totalLookupKeyDatums)
		} else {
			flatLookupKeyDatums = flatLookupKeyDatums[:totalLookupKeyDatums]
		}
	}
	workerData.minMaxLookupKeyDatums = flatLookupKeyDatums

	for rowPos, rowIdx := range uniqueRows {
		encodedKey := encodedInputKeys[rowPos]
		if _, exists := encodedKeyToLookupKeyPos[hack.String(encodedKey)]; exists {
			return errors.Errorf("duplicate lookup key for min/max batch recompute at input")
		}

		encodedKeyToLookupKeyPos[hack.String(encodedKey)] = rowPos
		rowIdxToLookupKeyPos[rowIdx] = rowPos

		inputRow := input.GetRow(rowIdx)
		base := rowPos * keyColCnt
		// Defensive for robustness: cap=len prevents append from mutating adjacent key slices.
		keyDatums := flatLookupKeyDatums[base : base+keyColCnt : base+keyColCnt]
		for keyPos, keyColID := range keyColIDs {
			keyDatums[keyPos] = types.Datum{}
			inputRow.DatumWithBuffer(keyColID, keyTypes[keyPos], &keyDatums[keyPos])
		}
		lookupKeyContents[rowPos] = MinMaxBatchLookupContent{Keys: keyDatums}
		lookupKeyPosToRowRefs[rowPos] = lookupKeyPosToRowRefs[rowPos][:0]
	}

	for _, mappingIdx := range batchMappings {
		override := overrides[mappingIdx]
		for rowPos, rowIdx := range override.rowIdxes {
			lookupKeyPos := rowIdxToLookupKeyPos[rowIdx]
			lookupKeyPosToRowRefs[lookupKeyPos] = append(lookupKeyPosToRowRefs[lookupKeyPos], minMaxRowRef{
				mappingIdx: mappingIdx,
				rowPos:     rowPos,
			})
		}
	}

	clear(seen)
	seenResultKeyCnt := 0

	batchExec, err := e.MinMaxRecompute.BatchBuilder.Build(ctx, &MinMaxBatchBuildRequest{LookupKeys: lookupKeyContents})
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
	resultKeyColIdxes := e.MinMaxRecompute.KeyResultColIdxes
	if err := e.validateMinMaxBatchSchemaTypes(
		childTypes,
		keyTypes,
		resultKeyColIdxes,
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
		resultRowCnt := resultChk.NumRows()
		if resultRowCnt == 0 {
			break
		}
		var resultRows []int
		if resultRowCnt <= len(globalContiguousRowIdxes) {
			resultRows = globalContiguousRowIdxes[:resultRowCnt]
		} else {
			resultRows = workerData.minMaxResultRows
			if cap(resultRows) < resultRowCnt {
				resultRows = make([]int, resultRowCnt)
				for i := range resultRowCnt {
					resultRows[i] = i
				}
			} else {
				resultRows = resultRows[:resultRowCnt]
			}
			workerData.minMaxResultRows = resultRows
		}
		encodedResultKeys, resultNullRows, err := encodeChunkKeyRows(
			typeCtx,
			resultChk,
			resultKeyColIdxes,
			keyTypes,
			resultRows,
			workerData.minMaxResultNullRows,
			workerData.minMaxResultEncodedKeys,
		)
		if err != nil {
			return err
		}
		workerData.minMaxResultEncodedKeys = encodedResultKeys
		workerData.minMaxResultNullRows = resultNullRows

		for rowIdx := range resultRowCnt {
			resultRow := resultChk.GetRow(rowIdx)
			encodedKey := encodedResultKeys[rowIdx]
			lookupKeyPos, exists := encodedKeyToLookupKeyPos[hack.String(encodedKey)]
			if !exists {
				return errors.Errorf("min/max batch recompute returns a key outside lookup set at result row %d", rowIdx)
			}
			if seen[lookupKeyPos] {
				return errors.Errorf("min/max batch recompute returns duplicate key at result row %d", rowIdx)
			}
			seen[lookupKeyPos] = true
			seenResultKeyCnt++

			refs := lookupKeyPosToRowRefs[lookupKeyPos]
			for _, ref := range refs {
				override := overrides[ref.mappingIdx]
				recomputeMeta := e.AggMappings[ref.mappingIdx].MinMaxRecompute
				mapping := e.AggMappings[ref.mappingIdx]
				for colPos, outputColID := range mapping.ColID {
					resultColIdx := recomputeMeta.BatchResultColIdxes[colPos]
					d := resultRow.GetDatum(resultColIdx, childTypes[outputColID])
					d.Copy(&override.valuesByOutput[colPos][ref.rowPos])
				}
			}
		}
	}

	if seenResultKeyCnt != len(lookupKeyContents) {
		return errors.Errorf(
			"min/max batch recompute result key count mismatch: expected %d, got %d",
			len(lookupKeyContents),
			seenResultKeyCnt,
		)
	}
	return nil
}

// encodeChunkKeyRows encodes key columns for each used row into keyBuf.
// Contract:
// 1. len(keyColIdxes) == len(keyTypes).
// 2. Every used row index is in [0, chk.NumRows()).
// 3. Every key column index is in [0, chk.NumCols()).
func encodeChunkKeyRows(
	typeCtx types.Context,
	chk *chunk.Chunk,
	keyColIdxes []int,
	keyTypes []*types.FieldType,
	usedRows []int,
	physicalRowNulls []bool,
	keyBuf [][]byte,
) ([][]byte, []bool, error) {
	if cap(keyBuf) < len(usedRows) {
		keyBuf = make([][]byte, len(usedRows))
	} else {
		keyBuf = keyBuf[:len(usedRows)]
	}
	for i := range usedRows {
		keyBuf[i] = keyBuf[i][:0]
	}
	if len(usedRows) == 0 {
		return keyBuf, physicalRowNulls, nil
	}

	rowCnt := chk.NumRows()
	if cap(physicalRowNulls) < rowCnt {
		physicalRowNulls = make([]bool, rowCnt)
	} else {
		physicalRowNulls = physicalRowNulls[:rowCnt]
	}

	for keyPos, keyColIdx := range keyColIdxes {
		col := chk.Column(keyColIdx)
		for logicalPos, physicalRow := range usedRows {
			isNull := col.IsNull(physicalRow)
			physicalRowNulls[physicalRow] = isNull
			if isNull {
				keyBuf[logicalPos] = append(keyBuf[logicalPos], 0)
			} else {
				keyBuf[logicalPos] = append(keyBuf[logicalPos], 1)
			}
		}
		if err := codec.SerializeKeys(typeCtx, chk, keyTypes[keyPos], keyColIdx, usedRows, nil, physicalRowNulls, codec.KeepVarColumnLength, keyBuf); err != nil {
			return nil, physicalRowNulls, err
		}
	}
	return keyBuf, physicalRowNulls, nil
}

func (e *Exec) validateMinMaxBatchSchemaTypes(
	childTypes []*types.FieldType,
	keyTypes []*types.FieldType,
	resultKeyColIdxes []int,
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

		resultTp := batchTypes[resultColIdx]
		if keyTp == nil || resultTp == nil {
			return errors.Errorf("min/max batch key type is unavailable at position %d", i)
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
	if len(childTypes) != len(computedByColID) {
		return errors.Errorf(
			"min/max override child/computed column count mismatch: child_types=%d computed_cols=%d",
			len(childTypes),
			len(computedByColID),
		)
	}
	for mappingIdx, override := range overrides {
		if override == nil || len(override.rowIdxes) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		for colPos, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(computedByColID) {
				return errors.Errorf("min/max override output col id %d out of range [0,%d)", outputColID, len(computedByColID))
			}
			newCol, err := rebuildColumnWithOverrides(
				computedByColID[outputColID],
				childTypes[outputColID],
				override.rowIdxes,
				override.valuesByOutput[colPos],
			)
			if err != nil {
				return err
			}
			computedByColID[outputColID] = newCol
		}
	}
	return nil
}

type mappingRecomputeOverride struct {
	// rowIdxes must be strictly increasing.
	rowIdxes       []int
	valuesByOutput [][]types.Datum
}

type minMaxRowRef struct {
	mappingIdx int
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

// Rebuild one output column by applying override values at selected row indexes.
// Contract:
// 1. rowIdxes is strictly increasing.
// 2. every row index in rowIdxes is in [0, oldCol.Rows()).
// TODO: For fixed-size columns, override in place instead of rebuilding a new column.
func rebuildColumnWithOverrides(
	oldCol *chunk.Column,
	ft *types.FieldType,
	rowIdxes []int,
	values []types.Datum,
) (*chunk.Column, error) {
	if len(rowIdxes) != len(values) {
		return nil, errors.Errorf("override row/value count mismatch: rows=%d values=%d", len(rowIdxes), len(values))
	}
	rowCnt := oldCol.Rows()
	newCol := chunk.NewColumn(ft, rowCnt)
	copyStart := 0
	for overridePos, rowIdx := range rowIdxes {
		newCol.AppendCellRange(oldCol, copyStart, rowIdx)
		if err := appendDatumToColumn(newCol, &values[overridePos], ft); err != nil {
			return nil, err
		}
		copyStart = rowIdx + 1
	}
	if copyStart < rowCnt {
		newCol.AppendCellRange(oldCol, copyStart, rowCnt)
	}
	if newCol.Rows() != rowCnt {
		return nil, errors.Errorf("override row apply row count mismatch: expected=%d actual=%d", rowCnt, newCol.Rows())
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

func (e *Exec) buildRowOps(input *chunk.Chunk, computedByColID []*chunk.Column, workerData *mergeWorkerData) ([]RowOp, []uint8, int, int, error) {
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
			return nil, nil, 0, 0, errors.Errorf("count(*) becomes negative (%d)", newCount)
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
	typeCtx := e.Ctx().GetSessionVars().StmtCtx.TypeCtx()
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
			typeCtx,
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
	typeCtx types.Context,
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
		// Keep update touched detection consistent with updateRecord:
		// compare with binary collation instead of column collation.
		binaryCollator := collate.GetBinaryCollator()
		var oldDatum, newDatum types.Datum
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
			chunkRowColDatum(oldCol, rowIdx, ft, &oldDatum)
			chunkRowColDatum(newCol, rowIdx, ft, &newDatum)
			cmp, err := newDatum.Compare(typeCtx, &oldDatum, binaryCollator)
			if err != nil {
				return err
			}
			if cmp != 0 {
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

func chunkRowColDatum(col *chunk.Column, rowIdx int, ft *types.FieldType, d *types.Datum) {
	if col.IsNull(rowIdx) {
		d.SetNull()
		return
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
}
