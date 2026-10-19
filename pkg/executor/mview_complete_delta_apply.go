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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executil "github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

const (
	mviewCompleteDeltaDiffOpInsert = int64(1)
	mviewCompleteDeltaDiffOpDelete = int64(2)
	mviewCompleteDeltaDiffOpUpdate = int64(3)
)

// MViewCompleteDeltaApplyExec applies COMPLETE DELTA APPLY diff rows to the target MV table.
// It keeps the runtime single-threaded and batches UPDATE old/new comparisons by chunk.
type MViewCompleteDeltaApplyExec struct {
	exec.BaseExecutor

	TargetTable      table.Table
	TargetHandleCols plannerutil.HandleCols
	OpColID          int

	CurrentWritableInputColIDs    []int
	RecomputedWritableInputColIDs []int

	CompareWritableIdxes         []int
	CurrentCompareInputColIDs    []int
	RecomputedCompareInputColIDs []int

	writableFieldTypes []*types.FieldType
	compareColumns     []mviewCompleteDeltaCompareColumn
	oldRow             []types.Datum
	newRow             []types.Datum
	touched            []bool
	// currTouchedIdxes caches writable-column indexes touched by the current UPDATE row.
	// It lets us clear only previously-set bits in touched and patch only changed columns in newRow.
	currTouchedIdxes []int

	childChunk          *chunk.Chunk
	updateRows          []int
	updateTouchedBitmap []uint8
	updateTouchedStride int
	executed            bool
	runtimeStats        *mviewCompleteDeltaApplyRuntimeStats
}

type mviewCompleteDeltaCompareColumn struct {
	writableIdx          int
	currentInputColID    int
	recomputedInputColID int
	fieldType            *types.FieldType
	notNull              bool
}

type mviewCompleteDeltaApplyWriterStats struct {
	chunks int64
	rowOps int64

	insertRows int64
	updateRows int64
	deleteRows int64
}

type mviewCompleteDeltaApplyRuntimeStats struct {
	writerTime   time.Duration
	writerDetail mviewCompleteDeltaApplyWriterStats
}

func (s *mviewCompleteDeltaApplyRuntimeStats) reset() {
	if s == nil {
		return
	}
	s.writerTime = 0
	s.writerDetail = mviewCompleteDeltaApplyWriterStats{}
}

func (s *mviewCompleteDeltaApplyRuntimeStats) String() string {
	if s == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("mview_complete_delta_apply:{writer:{time:")
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
	buf.WriteString("}}}")
	return buf.String()
}

func (s *mviewCompleteDeltaApplyRuntimeStats) Clone() execdetails.RuntimeStats {
	if s == nil {
		return &mviewCompleteDeltaApplyRuntimeStats{}
	}
	return &mviewCompleteDeltaApplyRuntimeStats{writerTime: s.writerTime, writerDetail: s.writerDetail}
}

func (s *mviewCompleteDeltaApplyRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*mviewCompleteDeltaApplyRuntimeStats)
	if !ok || tmp == nil {
		return
	}
	s.writerTime += tmp.writerTime
	s.writerDetail.merge(tmp.writerDetail)
}

func (*mviewCompleteDeltaApplyRuntimeStats) Tp() int {
	return execdetails.TpMViewCompleteDeltaApplyRuntimeStats
}

func (s *mviewCompleteDeltaApplyWriterStats) merge(other mviewCompleteDeltaApplyWriterStats) {
	s.chunks += other.chunks
	s.rowOps += other.rowOps
	s.insertRows += other.insertRows
	s.updateRows += other.updateRows
	s.deleteRows += other.deleteRows
}

func (s mviewCompleteDeltaApplyWriterStats) affectedRows() uint64 {
	return uint64(s.insertRows + s.updateRows + s.deleteRows)
}

func (s mviewCompleteDeltaApplyWriterStats) stmtMessage() string {
	return fmt.Sprintf(
		"Rows inserted: %d  Updated: %d  Deleted: %d",
		s.insertRows,
		s.updateRows,
		s.deleteRows,
	)
}

// Open implements the Executor interface.
func (e *MViewCompleteDeltaApplyExec) Open(ctx context.Context) error {
	e.executed = false
	e.childChunk = nil
	e.updateRows = e.updateRows[:0]
	e.updateTouchedBitmap = e.updateTouchedBitmap[:0]
	e.updateTouchedStride = 0
	e.currTouchedIdxes = e.currTouchedIdxes[:0]
	clear(e.touched)

	if e.TargetTable == nil {
		return errors.New("MViewCompleteDeltaApply target table is nil")
	}
	if e.TargetHandleCols == nil {
		return errors.New("MViewCompleteDeltaApply target handle cols is nil")
	}
	child := e.Children(0)
	if child == nil {
		return errors.New("MViewCompleteDeltaApply child executor is nil")
	}
	childTypes := child.RetFieldTypes()
	if err := validateMViewCompleteDeltaWritableInputColTypes(e.TargetTable, childTypes, e.CurrentWritableInputColIDs); err != nil {
		return err
	}
	if err := validateMViewCompleteDeltaWritableInputColTypes(e.TargetTable, childTypes, e.RecomputedWritableInputColIDs); err != nil {
		return err
	}

	writableCols := e.TargetTable.WritableCols()
	e.writableFieldTypes = make([]*types.FieldType, len(writableCols))
	for i := range writableCols {
		e.writableFieldTypes[i] = &writableCols[i].FieldType
	}
	e.oldRow = make([]types.Datum, len(writableCols))
	e.newRow = make([]types.Datum, len(writableCols))
	e.touched = make([]bool, len(writableCols))
	if err := e.initCompareColumns(len(childTypes)); err != nil {
		return err
	}

	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.currTouchedIdxes = make([]int, 0, len(e.compareColumns))
	e.updateTouchedStride = (len(e.compareColumns) + 7) >> 3
	e.childChunk = exec.NewFirstChunk(child)
	return nil
}

// Next implements the Executor interface.
func (e *MViewCompleteDeltaApplyExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.executed {
		return nil
	}
	e.executed = true
	if e.BaseExecutor.RuntimeStats() != nil {
		if e.runtimeStats == nil {
			e.runtimeStats = &mviewCompleteDeltaApplyRuntimeStats{}
		} else {
			e.runtimeStats.reset()
		}
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)
	}

	child := e.Children(0)
	if child == nil {
		return errors.New("MViewCompleteDeltaApply child executor is nil")
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	tableCtx := e.Ctx().GetTableCtx()
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	insertSizeHintStep := int(e.Ctx().GetSessionVars().ShardAllocateStep)
	if insertSizeHintStep <= 0 {
		insertSizeHintStep = 1
	}
	var writerStats mviewCompleteDeltaApplyWriterStats

	for {
		e.childChunk.Reset()
		if err := exec.Next(ctx, child, e.childChunk); err != nil {
			return err
		}
		if e.childChunk.NumRows() == 0 {
			stmtCtx.SetAffectedRows(writerStats.affectedRows())
			stmtCtx.SetMessage(writerStats.stmtMessage())
			return nil
		}
		writeStart := time.Time{}
		if e.runtimeStats != nil {
			writeStart = time.Now()
		}
		if err := e.applyChunk(txn, tableCtx, stmtCtx, insertSizeHintStep, e.childChunk, &writerStats); err != nil {
			return err
		}
		if e.runtimeStats != nil {
			e.runtimeStats.writerTime += time.Since(writeStart)
		}
	}
}

// Close implements the Executor interface.
func (e *MViewCompleteDeltaApplyExec) Close() error {
	e.writableFieldTypes = nil
	e.compareColumns = nil
	e.oldRow = nil
	e.newRow = nil
	e.touched = nil
	e.currTouchedIdxes = nil
	e.childChunk = nil
	e.updateRows = nil
	e.updateTouchedBitmap = nil
	e.updateTouchedStride = 0
	e.executed = false
	e.runtimeStats = nil
	return e.BaseExecutor.Close()
}

func (e *MViewCompleteDeltaApplyExec) applyChunk(
	txn kv.Transaction,
	tableCtx table.MutateContext,
	stmtCtx *stmtctx.StatementContext,
	insertSizeHintStep int,
	input *chunk.Chunk,
	stmtWriterStats *mviewCompleteDeltaApplyWriterStats,
) error {
	ops := input.Column(e.OpColID).Int64s()[:input.NumRows()]
	insertRemain, err := e.collectChunkUpdateRows(ops)
	if err != nil {
		return err
	}
	if err := e.markChunkUpdateTouchedColumns(input); err != nil {
		return err
	}
	writerStatsDelta := mviewCompleteDeltaApplyWriterStats{
		chunks: 1,
		rowOps: int64(input.NumRows()),
	}
	defer func() {
		if stmtWriterStats != nil {
			stmtWriterStats.merge(writerStatsDelta)
		}
		if e.runtimeStats != nil {
			e.runtimeStats.writerDetail.merge(writerStatsDelta)
		}
	}()

	insertOrdinal := 0
	updateOrdinal := 0
	for rowIdx := range input.NumRows() {
		row := input.GetRow(rowIdx)
		op := ops[rowIdx]
		switch op {
		case mviewCompleteDeltaDiffOpInsert:
			writerStatsDelta.insertRows++
			e.buildInsertRow(row)

			sizeHint := 0
			if insertOrdinal%insertSizeHintStep == 0 {
				sizeHint = min(insertSizeHintStep, insertRemain)
			}
			insertOrdinal++
			insertRemain--
			if sizeHint > 0 {
				_, err = e.TargetTable.AddRecord(
					tableCtx,
					txn,
					e.newRow,
					table.WithReserveAutoIDHint(sizeHint),
					table.DupKeyCheckLazy,
				)
			} else {
				_, err = e.TargetTable.AddRecord(tableCtx, txn, e.newRow, table.DupKeyCheckLazy)
			}
			if err != nil {
				return err
			}
		case mviewCompleteDeltaDiffOpDelete:
			writerStatsDelta.deleteRows++
			e.buildDeleteRow(row)
			handle, err := e.TargetHandleCols.BuildHandle(stmtCtx, row)
			if err != nil {
				return err
			}
			if err := e.TargetTable.RemoveRecord(tableCtx, txn, handle, e.oldRow); err != nil {
				return err
			}
		case mviewCompleteDeltaDiffOpUpdate:
			changed := e.buildTouchedFromBitmap(updateOrdinal)
			if changed {
				writerStatsDelta.updateRows++
				e.buildUpdateRows(row)
				handle, err := e.TargetHandleCols.BuildHandle(stmtCtx, row)
				if err != nil {
					return err
				}
				if err := e.TargetTable.UpdateRecord(tableCtx, txn, handle, e.oldRow, e.newRow, e.touched); err != nil {
					return err
				}
			}
			updateOrdinal++
		default:
			return errors.Errorf("MViewCompleteDeltaApply invalid diff op %d at row %d", op, rowIdx)
		}
	}
	return nil
}

func (e *MViewCompleteDeltaApplyExec) collectChunkUpdateRows(ops []int64) (int, error) {
	if cap(e.updateRows) >= len(ops) {
		e.updateRows = e.updateRows[:0]
	} else {
		e.updateRows = make([]int, 0, len(ops))
	}
	insertRemain := 0
	for rowIdx, op := range ops {
		switch op {
		case mviewCompleteDeltaDiffOpInsert:
			insertRemain++
		case mviewCompleteDeltaDiffOpDelete:
		case mviewCompleteDeltaDiffOpUpdate:
			e.updateRows = append(e.updateRows, rowIdx)
		default:
			return 0, errors.Errorf("MViewCompleteDeltaApply invalid diff op %d at row %d", op, rowIdx)
		}
	}
	return insertRemain, nil
}

func (e *MViewCompleteDeltaApplyExec) initCompareColumns(inputColCount int) error {
	if len(e.CurrentCompareInputColIDs) != len(e.CompareWritableIdxes) || len(e.RecomputedCompareInputColIDs) != len(e.CompareWritableIdxes) {
		return errors.Errorf(
			"MViewCompleteDeltaApply compare mapping length mismatch (compare=%d, current=%d, recomputed=%d)",
			len(e.CompareWritableIdxes),
			len(e.CurrentCompareInputColIDs),
			len(e.RecomputedCompareInputColIDs),
		)
	}
	if cap(e.compareColumns) >= len(e.CompareWritableIdxes) {
		e.compareColumns = e.compareColumns[:len(e.CompareWritableIdxes)]
	} else {
		e.compareColumns = make([]mviewCompleteDeltaCompareColumn, len(e.CompareWritableIdxes))
	}
	for compareIdx, writableIdx := range e.CompareWritableIdxes {
		if writableIdx < 0 || writableIdx >= len(e.writableFieldTypes) {
			return errors.Errorf(
				"MViewCompleteDeltaApply writable compare index %d out of field type range [0,%d)",
				writableIdx,
				len(e.writableFieldTypes),
			)
		}
		currentInputColID := e.CurrentCompareInputColIDs[compareIdx]
		if currentInputColID < 0 || currentInputColID >= inputColCount {
			return errors.Errorf(
				"MViewCompleteDeltaApply current compare input col id %d out of source range [0,%d)",
				currentInputColID,
				inputColCount,
			)
		}
		recomputedInputColID := e.RecomputedCompareInputColIDs[compareIdx]
		if recomputedInputColID < 0 || recomputedInputColID >= inputColCount {
			return errors.Errorf(
				"MViewCompleteDeltaApply recomputed compare input col id %d out of source range [0,%d)",
				recomputedInputColID,
				inputColCount,
			)
		}
		fieldType := e.writableFieldTypes[writableIdx]
		e.compareColumns[compareIdx] = mviewCompleteDeltaCompareColumn{
			writableIdx:          writableIdx,
			currentInputColID:    currentInputColID,
			recomputedInputColID: recomputedInputColID,
			fieldType:            fieldType,
			notNull:              mysql.HasNotNullFlag(fieldType.GetFlag()),
		}
	}
	return nil
}

func (e *MViewCompleteDeltaApplyExec) markChunkUpdateTouchedColumns(input *chunk.Chunk) error {
	updateCnt := len(e.updateRows)
	if updateCnt == 0 || e.updateTouchedStride == 0 {
		e.updateTouchedBitmap = e.updateTouchedBitmap[:0]
		return nil
	}

	requiredLen := updateCnt * e.updateTouchedStride
	if cap(e.updateTouchedBitmap) < requiredLen {
		e.updateTouchedBitmap = make([]uint8, requiredLen)
	} else {
		e.updateTouchedBitmap = e.updateTouchedBitmap[:requiredLen]
		clear(e.updateTouchedBitmap)
	}

	for compareIdx, compareCol := range e.compareColumns {
		if err := executil.MarkTouchedRowsByColumn(
			e.updateRows,
			e.updateTouchedBitmap,
			e.updateTouchedStride,
			compareIdx,
			input.Column(compareCol.currentInputColID),
			input.Column(compareCol.recomputedInputColID),
			compareCol.fieldType,
			compareCol.notNull,
			"COMPLETE DELTA APPLY",
		); err != nil {
			return err
		}
	}
	return nil
}

func (e *MViewCompleteDeltaApplyExec) buildDeleteRow(row chunk.Row) {
	for writableIdx, colID := range e.CurrentWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.oldRow[writableIdx])
	}
}

func (e *MViewCompleteDeltaApplyExec) buildInsertRow(row chunk.Row) {
	for writableIdx, colID := range e.RecomputedWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.newRow[writableIdx])
	}
}

func (e *MViewCompleteDeltaApplyExec) buildUpdateRows(row chunk.Row) {
	for writableIdx, colID := range e.CurrentWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.oldRow[writableIdx])
	}
	copy(e.newRow, e.oldRow)
	for _, writableIdx := range e.currTouchedIdxes {
		row.DatumWithBuffer(e.RecomputedWritableInputColIDs[writableIdx], e.writableFieldTypes[writableIdx], &e.newRow[writableIdx])
	}
}

func (e *MViewCompleteDeltaApplyExec) buildTouchedFromBitmap(updateOrdinal int) bool {
	if e.updateTouchedStride == 0 {
		return false
	}
	for _, idx := range e.currTouchedIdxes {
		e.touched[idx] = false
	}
	e.currTouchedIdxes = e.currTouchedIdxes[:0]

	offset := updateOrdinal * e.updateTouchedStride
	rowBits := e.updateTouchedBitmap[offset : offset+e.updateTouchedStride]
	changed := false
	for byteIdx, b := range rowBits {
		for b != 0 {
			bitInByte := bits.TrailingZeros8(b)
			bitPos := (byteIdx << 3) + bitInByte
			writableIdx := e.compareColumns[bitPos].writableIdx
			e.touched[writableIdx] = true
			e.currTouchedIdxes = append(e.currTouchedIdxes, writableIdx)
			changed = true
			b &= b - 1
		}
	}
	return changed
}

func validateMViewCompleteDeltaWritableInputColTypes(target table.Table, childTypes []*types.FieldType, writableInputColIDs []int) error {
	if target == nil {
		return errors.New("MViewCompleteDeltaApply target table is nil")
	}
	writableCols := target.WritableCols()
	if len(writableInputColIDs) != len(writableCols) {
		return errors.Errorf("MViewCompleteDeltaApply writable input column count %d != target writable column count %d", len(writableInputColIDs), len(writableCols))
	}
	for i, inputColID := range writableInputColIDs {
		if inputColID < 0 || inputColID >= len(childTypes) {
			return errors.Errorf("MViewCompleteDeltaApply writable input col id %d at writable offset %d out of source range [0,%d)", inputColID, i, len(childTypes))
		}
		if childTypes[inputColID] == nil {
			return errors.Errorf("MViewCompleteDeltaApply writable input col id %d type is unavailable", inputColID)
		}
		if !(&writableCols[i].FieldType).Equal(childTypes[inputColID]) {
			return errors.Errorf("MViewCompleteDeltaApply writable input col id %d type mismatch for target column %s", inputColID, writableCols[i].Name.O)
		}
	}
	return nil
}
