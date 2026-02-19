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

package mvmergeagg

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

type noopWriter struct{}

func (noopWriter) WriteChunk(context.Context, *MVMergeAggChunkResult) error {
	return nil
}

type tableResultWriter struct {
	exec *MVMergeAggExec

	writableCols         []*table.Column
	writableOldColIDs    []int
	writableInsertColIDs []int
	aggWritableIDs       []int

	oldRow  []types.Datum
	newRow  []types.Datum
	touched []bool
}

func (e *MVMergeAggExec) buildTableResultWriter() (MVMergeAggResultWriter, error) {
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
		return nil, errors.New("MVMergeAggExec stage1 does not support target table with non-public writable columns")
	}

	oldColIDs := e.TargetWritableOldColIDs
	if len(oldColIDs) == 0 {
		oldColIDs = make([]int, len(writableCols))
		for i := range writableCols {
			oldColIDs[i] = e.DeltaAggColCount + writableCols[i].Offset
		}
	}
	if len(oldColIDs) != len(writableCols) {
		return nil, errors.Errorf("TargetWritableOldColIDs size %d does not match target writable columns %d", len(oldColIDs), len(writableCols))
	}

	insertColIDs := e.TargetWritableInsertColIDs
	if len(insertColIDs) == 0 {
		insertColIDs = oldColIDs
	}
	if len(insertColIDs) != len(writableCols) {
		return nil, errors.Errorf("TargetWritableInsertColIDs size %d does not match target writable columns %d", len(insertColIDs), len(writableCols))
	}
	for i := 0; i < e.TargetHandleCols.NumCols(); i++ {
		handleDatumIdx := e.TargetHandleCols.GetCol(i).Index
		if handleDatumIdx < 0 || handleDatumIdx >= len(writableCols) {
			return nil, errors.Errorf("TargetHandleCols col index %d out of writable datum range [0,%d)", handleDatumIdx, len(writableCols))
		}
	}

	output2Writable := make([]int, len(childTypes))
	for i := range output2Writable {
		output2Writable[i] = -1
	}
	for writableIdx, colID := range oldColIDs {
		if colID < 0 || colID >= len(childTypes) {
			return nil, errors.Errorf("TargetWritableOldColIDs[%d]=%d out of range [0,%d)", writableIdx, colID, len(childTypes))
		}
		output2Writable[colID] = writableIdx
	}
	for writableIdx, colID := range insertColIDs {
		if colID < 0 || colID >= len(childTypes) {
			return nil, errors.Errorf("TargetWritableInsertColIDs[%d]=%d out of range [0,%d)", writableIdx, colID, len(childTypes))
		}
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

	return &tableResultWriter{
		exec:                 e,
		writableCols:         writableCols,
		writableOldColIDs:    oldColIDs,
		writableInsertColIDs: insertColIDs,
		aggWritableIDs:       aggWritableIDs,
		oldRow:               make([]types.Datum, len(writableCols)),
		newRow:               make([]types.Datum, len(writableCols)),
		touched:              make([]bool, len(writableCols)),
	}, nil
}

func (w *tableResultWriter) WriteChunk(ctx context.Context, result *MVMergeAggChunkResult) error {
	if len(result.RowOps) == 0 {
		return nil
	}
	txn, err := w.exec.Ctx().Txn(true)
	if err != nil {
		return err
	}
	tableCtx := w.exec.Ctx().GetTableCtx()
	stmtCtx := w.exec.Ctx().GetSessionVars().StmtCtx

	for _, op := range result.RowOps {
		if err := w.buildRows(result, op.RowIdx); err != nil {
			return err
		}

		switch op.Tp {
		case MVMergeAggRowOpNoOp:
			continue
		case MVMergeAggRowOpInsert:
			_, err = w.exec.TargetTable.AddRecord(tableCtx, txn, w.newRow)
			if err != nil {
				return err
			}
		case MVMergeAggRowOpUpdate:
			if err := w.buildTouched(); err != nil {
				return err
			}
			handle, err := w.exec.TargetHandleCols.BuildHandleByDatums(stmtCtx, w.oldRow)
			if err != nil {
				return err
			}
			if err := w.exec.TargetTable.UpdateRecord(tableCtx, txn, handle, w.oldRow, w.newRow, w.touched); err != nil {
				return err
			}
		case MVMergeAggRowOpDelete:
			handle, err := w.exec.TargetHandleCols.BuildHandleByDatums(stmtCtx, w.oldRow)
			if err != nil {
				return err
			}
			if err := w.exec.TargetTable.RemoveRecord(tableCtx, txn, handle, w.oldRow); err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown MVMergeAgg row op %d", op.Tp)
		}
	}
	return txn.MayFlush()
}

func (w *tableResultWriter) buildRows(result *MVMergeAggChunkResult, rowIdx int) error {
	row := result.Input.GetRow(rowIdx)
	for writableIdx, col := range w.writableCols {
		retType := &col.FieldType
		oldColID := w.writableOldColIDs[writableIdx]
		insertColID := w.writableInsertColIDs[writableIdx]

		oldDatum := row.GetDatum(oldColID, retType)
		w.oldRow[writableIdx] = oldDatum

		if computedCol := result.ComputedCols[oldColID]; computedCol != nil {
			w.newRow[writableIdx] = chunkRowColDatum(computedCol, rowIdx, retType)
			continue
		}
		if oldDatum.IsNull() && insertColID != oldColID {
			w.newRow[writableIdx] = row.GetDatum(insertColID, retType)
			continue
		}
		w.newRow[writableIdx] = oldDatum
	}
	return nil
}

func (w *tableResultWriter) buildTouched() error {
	for _, idx := range w.aggWritableIDs {
		w.touched[idx] = false
	}
	typeCtx := w.exec.Ctx().GetSessionVars().StmtCtx.TypeCtx()
	for _, idx := range w.aggWritableIDs {
		cmp, err := w.oldRow[idx].Compare(typeCtx, &w.newRow[idx], collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		if cmp != 0 {
			w.touched[idx] = true
		}
	}
	return nil
}
