// Copyright 2016 PingCAP, Inc.
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
	"context"
	"fmt"
	"runtime/trace"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// UnionScanExec merges the rows from dirty table and the rows from distsql request.
type UnionScanExec struct {
	baseExecutor

	memBuf     kv.MemBuffer
	memBufSnap kv.Getter

	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex            []int
	desc                 bool
	conditions           []expression.Expression
	conditionsWithVirCol []expression.Expression
	columns              []*model.ColumnInfo
	table                table.Table
	// belowHandleCols is the handle's position of the below scan plan.
	belowHandleCols plannercore.HandleCols

	addedRows           [][]types.Datum
	cursor4AddRows      int
	sortErr             error
	snapshotRows        [][]types.Datum
	cursor4SnapshotRows int
	snapshotChunkBuffer *chunk.Chunk
	mutableRow          chunk.MutRow
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int
}

// Open implements the Executor Open interface.
func (us *UnionScanExec) Open(ctx context.Context) error {
	if err := us.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return us.open(ctx)
}

func (us *UnionScanExec) open(ctx context.Context) error {
	var err error
	reader := us.children[0]

	// If the push-downed condition contains virtual column, we may build a selection upon reader. Since unionScanExec
	// has already contained condition, we can ignore the selection.
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.children[0]
	}

	defer trace.StartRegion(ctx, "UnionScanBuildRows").End()
	txn, err := us.ctx.Txn(false)
	if err != nil {
		return err
	}

	mb := txn.GetMemBuffer()
	mb.RLock()
	defer mb.RUnlock()

	us.memBuf = mb
	us.memBufSnap = mb.SnapshotGetter()

	// 1. select without virtual columns
	// 2. build virtual columns and select with virtual columns
	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.addedRows, err = buildMemTableReader(us, x).getMemRows()
	case *IndexReaderExecutor:
		us.addedRows, err = buildMemIndexReader(us, x).getMemRows()
	case *IndexLookUpExecutor:
		us.addedRows, err = buildMemIndexLookUpReader(us, x).getMemRows()
	default:
		err = fmt.Errorf("unexpected union scan children:%T", reader)
	}
	if err != nil {
		return err
	}
	us.snapshotChunkBuffer = newFirstChunk(us)
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	us.memBuf.RLock()
	defer us.memBuf.RUnlock()
	req.GrowAndReset(us.maxChunkSize)
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	for i, batchSize := 0, req.Capacity(); i < batchSize; i++ {
		row, err := us.getOneRow(ctx)
		if err != nil {
			return err
		}
		// no more data.
		if row == nil {
			return nil
		}
		mutableRow.SetDatums(row...)

		for _, idx := range us.virtualColumnIndex {
			datum, err := us.schema.Columns[idx].EvalVirtualColumn(mutableRow.ToRow())
			if err != nil {
				return err
			}
			// Because the expression might return different type from
			// the generated column, we should wrap a CAST on the result.
			castDatum, err := table.CastValue(us.ctx, datum, us.columns[idx], false, true)
			if err != nil {
				return err
			}
			// Handle the bad null error.
			if (mysql.HasNotNullFlag(us.columns[idx].Flag) || mysql.HasPreventNullInsertFlag(us.columns[idx].Flag)) && castDatum.IsNull() {
				castDatum = table.GetZeroValue(us.columns[idx])
			}
			mutableRow.SetDatum(idx, castDatum)
		}

		matched, _, err := expression.EvalBool(us.ctx, us.conditionsWithVirCol, mutableRow.ToRow())
		if err != nil {
			return err
		}
		if matched {
			req.AppendRow(mutableRow.ToRow())
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (us *UnionScanExec) Close() error {
	us.cursor4AddRows = 0
	us.cursor4SnapshotRows = 0
	us.addedRows = us.addedRows[:0]
	us.snapshotRows = us.snapshotRows[:0]
	return us.children[0].Close()
}

// getOneRow gets one result row from dirty table or child.
func (us *UnionScanExec) getOneRow(ctx context.Context) ([]types.Datum, error) {
	snapshotRow, err := us.getSnapshotRow(ctx)
	if err != nil {
		return nil, err
	}
	addedRow := us.getAddedRow()

	var row []types.Datum
	var isSnapshotRow bool
	if addedRow == nil {
		row = snapshotRow
		isSnapshotRow = true
	} else if snapshotRow == nil {
		row = addedRow
	} else {
		isSnapshotRow, err = us.shouldPickFirstRow(snapshotRow, addedRow)
		if err != nil {
			return nil, err
		}
		if isSnapshotRow {
			row = snapshotRow
		} else {
			row = addedRow
		}
	}
	if row == nil {
		return nil, nil
	}

	if isSnapshotRow {
		us.cursor4SnapshotRows++
	} else {
		us.cursor4AddRows++
	}
	return row, nil
}

func (us *UnionScanExec) getSnapshotRow(ctx context.Context) ([]types.Datum, error) {
	if us.cursor4SnapshotRows < len(us.snapshotRows) {
		return us.snapshotRows[us.cursor4SnapshotRows], nil
	}
	var err error
	us.cursor4SnapshotRows = 0
	us.snapshotRows = us.snapshotRows[:0]
	for len(us.snapshotRows) == 0 {
		err = Next(ctx, us.children[0], us.snapshotChunkBuffer)
		if err != nil || us.snapshotChunkBuffer.NumRows() == 0 {
			return nil, err
		}
		iter := chunk.NewIterator4Chunk(us.snapshotChunkBuffer)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			var snapshotHandle kv.Handle
			snapshotHandle, err = us.belowHandleCols.BuildHandle(row)
			if err != nil {
				return nil, err
			}
			checkKey := us.table.RecordKey(snapshotHandle)
			if _, err := us.memBufSnap.Get(context.TODO(), checkKey); err == nil {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			us.snapshotRows = append(us.snapshotRows, row.GetDatumRow(retTypes(us.children[0])))
		}
	}
	return us.snapshotRows[0], nil
}

func (us *UnionScanExec) getAddedRow() []types.Datum {
	var addedRow []types.Datum
	if us.cursor4AddRows < len(us.addedRows) {
		addedRow = us.addedRows[us.cursor4AddRows]
	}
	return addedRow
}

// shouldPickFirstRow picks the suitable row in order.
// The value returned is used to determine whether to pick the first input row.
func (us *UnionScanExec) shouldPickFirstRow(a, b []types.Datum) (bool, error) {
	var isFirstRow bool
	addedCmpSrc, err := us.compare(a, b)
	if err != nil {
		return isFirstRow, err
	}
	// Compare result will never be 0.
	if us.desc {
		if addedCmpSrc > 0 {
			isFirstRow = true
		}
	} else {
		if addedCmpSrc < 0 {
			isFirstRow = true
		}
	}
	return isFirstRow, nil
}

func (us *UnionScanExec) compare(a, b []types.Datum) (int, error) {
	sc := us.ctx.GetSessionVars().StmtCtx
	for _, colOff := range us.usedIndex {
		aColumn := a[colOff]
		bColumn := b[colOff]
		cmp, err := aColumn.CompareDatum(sc, &bColumn)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return us.belowHandleCols.Compare(a, b)
}
