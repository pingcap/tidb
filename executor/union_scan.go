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
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
)

func getTableDeletedRows(s sessionctx.Context, tid int64) map[int64]struct{} {
	if s.GetSessionVars().TxnCtx.DeletedTableRows == nil {
		return make(map[int64]struct{})
	}
	return s.GetSessionVars().TxnCtx.DeletedTableRows[tid]
}

// UnionScanExec merges the rows from dirty table and the rows from distsql request.
type UnionScanExec struct {
	baseExecutor

	deletedRows map[int64]struct{}
	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex  []int
	desc       bool
	conditions []expression.Expression
	columns    []*model.ColumnInfo
	table      table.Table
	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int

	addedRows [][]types.Datum
	// memProcessedHandles is uses to store the handle ids that has been read by memIndexReader.
	memProcessedHandles set.Int64Set
	cursor4AddRows      int
	sortErr             error
	snapshotRows        [][]types.Datum
	cursor4SnapshotRows int
	snapshotChunkBuffer *chunk.Chunk
	mutableRow          chunk.MutRow
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
	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.addedRows, err = buildMemTableReader(us, x).getMemRows()
	case *IndexReaderExecutor:
		tid := getPhysicalTableID(x.table)
		if us.ctx.IsUntouchedIndex(tid, x.index.ID) {
			// use full range table scan.
			err = us.buildAndSortAddedRowsFromMemTableReader()
		} else {
			mIdxReader := buildMemIndexReader(us, x)
			us.addedRows, err = mIdxReader.getMemRows()
			us.memProcessedHandles = mIdxReader.memProcessedHandles
		}
	case *IndexLookUpExecutor:
		tid := getPhysicalTableID(x.table)
		if us.ctx.IsUntouchedIndex(tid, x.index.ID) {
			// use full range table scan.
			err = us.buildAndSortAddedRowsFromMemTableReader()
		} else {
			idxLookup := buildMemIndexLookUpReader(us, x)
			us.addedRows, err = idxLookup.getMemRows()
			us.memProcessedHandles = idxLookup.idxReader.memProcessedHandles
		}
	}
	if err != nil {
		return err
	}
	us.snapshotChunkBuffer = newFirstChunk(us)
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
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
		req.AppendRow(mutableRow.ToRow())
	}
	return nil
}

// getOneRow gets one result row from dirty table or child.
func (us *UnionScanExec) getOneRow(ctx context.Context) ([]types.Datum, error) {
	for {
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
			snapshotHandle := row.GetInt64(us.belowHandleIndex)
			if _, ok := us.deletedRows[snapshotHandle]; ok {
				continue
			}
			if _, ok := us.memProcessedHandles[snapshotHandle]; ok {
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
	aHandle := a[us.belowHandleIndex].GetInt64()
	bHandle := b[us.belowHandleIndex].GetInt64()
	var cmp int
	if aHandle == bHandle {
		cmp = 0
	} else if aHandle > bHandle {
		cmp = 1
	} else {
		cmp = -1
	}
	return cmp, nil
}

//TableRangesToKVRanges
func (us *UnionScanExec) buildAndSortAddedRowsFromMemTableReader() error {
	tableReaderWithFullRange := buildMemTableReaderWithFullRange(us)
	rows, err := tableReaderWithFullRange.getMemRows()
	if err != nil {
		return err
	}
	us.memProcessedHandles = tableReaderWithFullRange.memProcessedHandles
	us.addedRows = make([][]types.Datum, 0, len(rows))
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	for _, row := range rows {
		mutableRow.SetDatums(row...)
		matched, _, err := expression.EvalBool(us.ctx, us.conditions, mutableRow.ToRow())
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		us.addedRows = append(us.addedRows, row)
	}

	if us.desc {
		sort.Sort(sort.Reverse(us))
	} else {
		sort.Sort(us)
	}
	if us.sortErr != nil {
		return errors.Trace(us.sortErr)
	}
	return nil
}

// Len implements sort.Interface interface.
func (us *UnionScanExec) Len() int {
	return len(us.addedRows)
}

// Less implements sort.Interface interface.
func (us *UnionScanExec) Less(i, j int) bool {
	cmp, err := us.compare(us.addedRows[i], us.addedRows[j])
	if err != nil {
		us.sortErr = errors.Trace(err)
		return true
	}
	return cmp < 0
}

// Swap implements sort.Interface interface.
func (us *UnionScanExec) Swap(i, j int) {
	us.addedRows[i], us.addedRows[j] = us.addedRows[j], us.addedRows[i]
}
