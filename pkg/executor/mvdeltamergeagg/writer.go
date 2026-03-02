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
	"context"
	"math/bits"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
)

type noopWriter struct{}

func (noopWriter) WriteChunk(_ context.Context, _ *ChunkResult) error {
	return nil
}

type tableResultWriter struct {
	exec *Exec

	writableColIDs        []int
	writableFieldTypes    []*types.FieldType
	aggWritableIDs        []int
	aggWritableInputColID []int

	oldRow  []types.Datum
	newRow  []types.Datum
	touched []bool
	// prevTouched caches touched indexes from previous update row.
	prevTouched []int
}

func (e *Exec) buildTableResultWriter() (ResultWriter, error) {
	if e.TargetTable == nil {
		return nil, errors.New("TargetTable is nil")
	}
	if e.TargetInfo != nil && e.TargetInfo.ID != e.TargetTable.Meta().ID {
		return nil, errors.Errorf("TargetInfo.ID(%d) does not match TargetTable.ID(%d)", e.TargetInfo.ID, e.TargetTable.Meta().ID)
	}
	if e.TargetHandleCols == nil {
		return nil, errors.New("TargetHandleCols is nil")
	}
	childTypes := e.Children(0).RetFieldTypes()
	writableCols := e.TargetTable.WritableCols()
	if len(writableCols) == 0 {
		return nil, errors.New("target table has no writable columns")
	}
	publicCols := e.TargetTable.Cols()
	if len(publicCols) != len(writableCols) {
		return nil, errors.New("Exec stage1 does not support target table with non-public writable columns")
	}

	colIDs := e.TargetWritableColIDs
	if len(colIDs) == 0 {
		colIDs = make([]int, len(writableCols))
		for i := range writableCols {
			colIDs[i] = e.DeltaAggColCount + writableCols[i].Offset
		}
	}
	if len(colIDs) != len(writableCols) {
		return nil, errors.Errorf("TargetWritableColIDs size %d does not match target writable columns %d", len(colIDs), len(writableCols))
	}
	for i := 0; i < e.TargetHandleCols.NumCols(); i++ {
		handleInputIdx := e.TargetHandleCols.GetCol(i).Index
		if handleInputIdx < 0 || handleInputIdx >= len(childTypes) {
			return nil, errors.Errorf("TargetHandleCols col index %d out of input range [0,%d)", handleInputIdx, len(childTypes))
		}
	}

	output2Writable := make([]int, len(childTypes))
	for i := range output2Writable {
		output2Writable[i] = -1
	}
	for writableIdx, colID := range colIDs {
		if colID < 0 || colID >= len(childTypes) {
			return nil, errors.Errorf("TargetWritableColIDs[%d]=%d out of range [0,%d)", writableIdx, colID, len(childTypes))
		}
		inputTp := childTypes[colID]
		if inputTp == nil {
			return nil, errors.Errorf("TargetWritableColIDs[%d]=%d type is unavailable", writableIdx, colID)
		}
		targetTp := &writableCols[writableIdx].FieldType
		if !targetTp.Equal(inputTp) {
			return nil, errors.Errorf(
				"TargetWritableColIDs[%d]=%d type mismatch, target col `%s` expects %s but input is %s",
				writableIdx, colID, writableCols[writableIdx].Name.O, targetTp.String(), inputTp.String(),
			)
		}
		output2Writable[colID] = writableIdx
	}

	aggWritableIDs := make([]int, 0, len(e.aggOutputColIDs))
	seenWritable := make([]bool, len(writableCols))
	for _, outputColID := range e.aggOutputColIDs {
		if outputColID < 0 || outputColID >= len(output2Writable) {
			return nil, errors.Errorf("agg output col id %d out of range [0,%d)", outputColID, len(output2Writable))
		}
		writableIdx := output2Writable[outputColID]
		if writableIdx < 0 {
			return nil, errors.Errorf("agg output col id %d does not map to target writable columns", outputColID)
		}
		if !seenWritable[writableIdx] {
			seenWritable[writableIdx] = true
			aggWritableIDs = append(aggWritableIDs, writableIdx)
		}
	}
	writableFieldTypes := make([]*types.FieldType, len(writableCols))
	for i := range writableCols {
		writableFieldTypes[i] = &writableCols[i].FieldType
	}
	aggWritableInputColID := make([]int, len(aggWritableIDs))
	for i, writableIdx := range aggWritableIDs {
		aggWritableInputColID[i] = colIDs[writableIdx]
	}

	return &tableResultWriter{
		exec:                  e,
		writableColIDs:        colIDs,
		writableFieldTypes:    writableFieldTypes,
		aggWritableIDs:        aggWritableIDs,
		aggWritableInputColID: aggWritableInputColID,
		oldRow:                make([]types.Datum, len(writableCols)),
		newRow:                make([]types.Datum, len(writableCols)),
		touched:               make([]bool, len(writableCols)),
		prevTouched:           make([]int, 0, len(aggWritableIDs)),
	}, nil
}

func (w *tableResultWriter) WriteChunk(_ context.Context, result *ChunkResult) error {
	if len(result.RowOps) == 0 {
		return nil
	}
	if err := w.validateChunkResult(result); err != nil {
		return err
	}

	txn, err := w.exec.Ctx().Txn(true)
	if err != nil {
		return err
	}
	tableCtx := w.exec.Ctx().GetTableCtx()
	stmtCtx := w.exec.Ctx().GetSessionVars().StmtCtx

	for _, op := range result.RowOps {
		switch op.Tp {
		case RowOpNoOp:
			continue
		case RowOpInsert:
			w.buildInsertRow(result, op.RowIdx)
			_, err = w.exec.TargetTable.AddRecord(tableCtx, txn, w.newRow)
			if err != nil {
				return err
			}
		case RowOpUpdate:
			w.buildUpdateRows(result, op.RowIdx)
			updateOrdinal := int(op.updateOrdinal)
			w.buildTouchedFromBitmap(result.UpdateTouchedBitmap, result.UpdateTouchedStride, updateOrdinal)
			handle, err := w.exec.TargetHandleCols.BuildHandle(stmtCtx, result.Input.GetRow(op.RowIdx))
			if err != nil {
				return err
			}
			if err := w.exec.TargetTable.UpdateRecord(tableCtx, txn, handle, w.oldRow, w.newRow, w.touched); err != nil {
				return err
			}
		case RowOpDelete:
			w.buildDeleteRow(result, op.RowIdx)
			handle, err := w.exec.TargetHandleCols.BuildHandle(stmtCtx, result.Input.GetRow(op.RowIdx))
			if err != nil {
				return err
			}
			if err := w.exec.TargetTable.RemoveRecord(tableCtx, txn, handle, w.oldRow); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown MVDeltaMergeAgg row op %d", op.Tp)
		}
	}
	return txn.MayFlush()
}

func (w *tableResultWriter) validateChunkResult(result *ChunkResult) error {
	if result.UpdateTouchedBitCnt != len(w.aggWritableIDs) {
		return errors.Errorf(
			"update touched bit count mismatch, result=%d writer=%d",
			result.UpdateTouchedBitCnt,
			len(w.aggWritableIDs),
		)
	}
	expectedStride := (result.UpdateTouchedBitCnt + 7) >> 3
	if result.UpdateTouchedStride != expectedStride {
		return errors.Errorf(
			"update touched stride mismatch, result=%d expected=%d",
			result.UpdateTouchedStride,
			expectedStride,
		)
	}
	updateCandidateCnt := 0
	for _, op := range result.RowOps {
		if op.Tp == RowOpUpdate || (op.Tp == RowOpNoOp && op.updateOrdinal >= 0) {
			if int(op.updateOrdinal) != updateCandidateCnt {
				return errors.Errorf(
					"update touched ordinal mismatch, expected=%d got=%d",
					updateCandidateCnt,
					op.updateOrdinal,
				)
			}
			updateCandidateCnt++
		}
	}
	if len(result.UpdateTouchedBitmap) != updateCandidateCnt*result.UpdateTouchedStride {
		return errors.Errorf(
			"update touched bitmap size mismatch, bitmap_len=%d expected=%d",
			len(result.UpdateTouchedBitmap),
			updateCandidateCnt*result.UpdateTouchedStride,
		)
	}
	for _, colID := range w.aggWritableInputColID {
		if colID < 0 || colID >= len(result.ComputedCols) {
			return errors.Errorf("agg computed col id %d out of range [0,%d)", colID, len(result.ComputedCols))
		}
		if result.ComputedCols[colID] == nil {
			return errors.Errorf("agg computed col %d is nil in chunk result", colID)
		}
	}
	return nil
}

func (w *tableResultWriter) buildDeleteRow(result *ChunkResult, rowIdx int) {
	row := result.Input.GetRow(rowIdx)
	for writableIdx, colID := range w.writableColIDs {
		row.DatumWithBuffer(colID, w.writableFieldTypes[writableIdx], &w.oldRow[writableIdx])
	}
}

func (w *tableResultWriter) buildInsertRow(result *ChunkResult, rowIdx int) {
	row := result.Input.GetRow(rowIdx)
	for writableIdx, colID := range w.writableColIDs {
		row.DatumWithBuffer(colID, w.writableFieldTypes[writableIdx], &w.newRow[writableIdx])
	}
	for aggPos, writableIdx := range w.aggWritableIDs {
		colID := w.aggWritableInputColID[aggPos]
		w.newRow[writableIdx] = chunkRowColDatum(result.ComputedCols[colID], rowIdx, w.writableFieldTypes[writableIdx])
	}
}

func (w *tableResultWriter) buildUpdateRows(result *ChunkResult, rowIdx int) {
	row := result.Input.GetRow(rowIdx)
	for writableIdx, colID := range w.writableColIDs {
		row.DatumWithBuffer(colID, w.writableFieldTypes[writableIdx], &w.oldRow[writableIdx])
	}
	copy(w.newRow, w.oldRow)
	for aggPos, writableIdx := range w.aggWritableIDs {
		colID := w.aggWritableInputColID[aggPos]
		w.newRow[writableIdx] = chunkRowColDatum(result.ComputedCols[colID], rowIdx, w.writableFieldTypes[writableIdx])
	}
}

func (w *tableResultWriter) buildTouchedFromBitmap(updateTouchedBitmap []uint8, updateTouchedStride, updateOrdinal int) {
	for _, idx := range w.prevTouched {
		w.touched[idx] = false
	}
	w.prevTouched = w.prevTouched[:0]

	offset := updateOrdinal * updateTouchedStride
	rowBits := updateTouchedBitmap[offset : offset+updateTouchedStride]
	for byteIdx, b := range rowBits {
		for b != 0 {
			bitInByte := bits.TrailingZeros8(b)
			bitPos := (byteIdx << 3) + bitInByte
			idx := w.aggWritableIDs[bitPos]
			w.touched[idx] = true
			w.prevTouched = append(w.prevTouched, idx)
			b &= b - 1
		}
	}
}
