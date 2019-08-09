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

// DirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type DirtyDB struct {
	// tables is a map whose key is tableID.
	tables map[int64]*DirtyTable
}

// GetDirtyTable gets the DirtyTable by id from the DirtyDB.
func (udb *DirtyDB) GetDirtyTable(tid int64) *DirtyTable {
	dt, ok := udb.tables[tid]
	if !ok {
		dt = &DirtyTable{
			tid:         tid,
			addedRows:   make(map[int64]struct{}),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	return dt
}

// DirtyTable stores uncommitted write operation for a transaction.
type DirtyTable struct {
	tid int64
	// addedRows ...
	// the key is handle.
	addedRows   map[int64]struct{}
	deletedRows map[int64]struct{}
	truncated   bool
}

// AddRow adds a row to the DirtyDB.
func (dt *DirtyTable) AddRow(handle int64, row []types.Datum) {
	dt.addedRows[handle] = struct{}{}
}

// DeleteRow deletes a row from the DirtyDB.
func (dt *DirtyTable) DeleteRow(handle int64) {
	delete(dt.addedRows, handle)
	dt.deletedRows[handle] = struct{}{}
}

// TruncateTable truncates a table.
func (dt *DirtyTable) TruncateTable() {
	dt.addedRows = make(map[int64]struct{})
	dt.truncated = true
}

// GetDirtyDB returns the DirtyDB bind to the context.
func GetDirtyDB(ctx sessionctx.Context) *DirtyDB {
	var udb *DirtyDB
	x := ctx.GetSessionVars().TxnCtx.DirtyDB
	if x == nil {
		udb = &DirtyDB{tables: make(map[int64]*DirtyTable)}
		ctx.GetSessionVars().TxnCtx.DirtyDB = udb
	} else {
		udb = x.(*DirtyDB)
	}
	return udb
}

// UnionScanExec merges the rows from dirty table and the rows from distsql request.
type UnionScanExec struct {
	baseExecutor

	dirty *DirtyTable
	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex  []int
	desc       bool
	conditions []expression.Expression
	columns    []*model.ColumnInfo
	table      table.Table
	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int

	addedRows [][]types.Datum
	// memIdxHandles is uses to store the handle ids that has been read by memIndexReader.
	memIdxHandles       set.Int64Set
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
		mIdxReader := buildMemIndexReader(us, x)
		us.addedRows, err = mIdxReader.getMemRows()
		us.memIdxHandles = mIdxReader.memIdxHandles
	case *IndexLookUpExecutor:
		us.memIdxHandles = set.NewInt64Set()
		err = us.buildAndSortAddedRows(ctx, x.table)
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
	if us.dirty.truncated {
		return nil, nil
	}
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
			if _, ok := us.dirty.deletedRows[snapshotHandle]; ok {
				err = us.getMissIndexRowsByHandle(ctx, snapshotHandle)
				if err != nil {
					return nil, err
				}
				continue
			}
			if _, ok := us.dirty.addedRows[snapshotHandle]; ok {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				err = us.getMissIndexRowsByHandle(ctx, snapshotHandle)
				if err != nil {
					return nil, err
				}
				continue
			}
			us.snapshotRows = append(us.snapshotRows, row.GetDatumRow(retTypes(us.children[0])))
		}
	}
	return us.snapshotRows[0], nil
}

// For index reader and index look up reader, update doesn't write index to txn memBuffer when the idx column
// is unchanged. So the `memIndexReader` and `memIndexLookUpReader` can't read the index from txn memBuffer.
// This function is used to get the missing row by the handle if the handle is in dirtyTable.addedRows.
func (us *UnionScanExec) getMissIndexRowsByHandle(ctx context.Context, handle int64) error {
	reader := us.children[0]
	switch reader.(type) {
	case *TableReaderExecutor:
		return nil
	}
	if _, ok := us.dirty.addedRows[handle]; !ok {
		return nil
	}
	// Don't miss in memBuffer reader.
	if us.memIdxHandles.Exist(handle) {
		return nil
	}
	memRow, err := us.getMemRow(ctx, handle)
	if memRow == nil || err != nil {
		return err
	}
	us.snapshotRows = append(us.snapshotRows, memRow)
	return nil
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

// rowWithColsInTxn gets the row from the transaction buffer.
func (us *UnionScanExec) rowWithColsInTxn(ctx context.Context, t table.Table, h int64) ([]types.Datum, error) {
	key := t.RecordKey(h)
	txn, err := us.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.GetMemBuffer().Get(ctx, key)
	if err != nil {
		return nil, err
	}
	colIDs := make(map[int64]int)
	for i, col := range us.columns {
		colIDs[col.ID] = i
	}
	return decodeRowData(us.ctx, us.table.Meta(), us.columns, colIDs, h, []byte{}, value)
}

func (us *UnionScanExec) getMemRow(ctx context.Context, h int64) ([]types.Datum, error) {
	data, err := us.rowWithColsInTxn(ctx, us.table, h)
	if err != nil {
		return nil, err
	}
	us.mutableRow.SetDatums(data...)
	matched, _, err := expression.EvalBool(us.ctx, us.conditions, us.mutableRow.ToRow())
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return data, nil
}

// TODO: remove `buildAndSortAddedRows` functions and `DirtyTable`.
func (us *UnionScanExec) buildAndSortAddedRows(ctx context.Context, t table.Table) error {
	us.addedRows = make([][]types.Datum, 0, len(us.dirty.addedRows))
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	for h := range us.dirty.addedRows {
		us.memIdxHandles.Insert(h)
		newData, err := us.rowWithColsInTxn(ctx, t, h)
		if err != nil {
			return err
		}
		mutableRow.SetDatums(newData...)
		matched, _, err := expression.EvalBool(us.ctx, us.conditions, mutableRow.ToRow())
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		us.addedRows = append(us.addedRows, newData)
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
