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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// DirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type DirtyDB struct {
	// tables is a map whose key is tableID.
	tables map[int64]*DirtyTable
}

// AddRow adds a row to the DirtyDB.
func (udb *DirtyDB) AddRow(tid, handle int64, row []types.Datum) {
	dt := udb.GetDirtyTable(tid)
	for i := range row {
		if row[i].Kind() == types.KindString {
			row[i].SetBytes(row[i].GetBytes())
		}
	}
	dt.addedRows[handle] = row
}

// DeleteRow deletes a row from the DirtyDB.
func (udb *DirtyDB) DeleteRow(tid int64, handle int64) {
	dt := udb.GetDirtyTable(tid)
	delete(dt.addedRows, handle)
	dt.deletedRows[handle] = struct{}{}
}

// TruncateTable truncates a table.
func (udb *DirtyDB) TruncateTable(tid int64) {
	dt := udb.GetDirtyTable(tid)
	dt.addedRows = make(map[int64][]types.Datum)
	dt.truncated = true
}

// GetDirtyTable gets the DirtyTable by id from the DirtyDB.
func (udb *DirtyDB) GetDirtyTable(tid int64) *DirtyTable {
	dt, ok := udb.tables[tid]
	if !ok {
		dt = &DirtyTable{
			addedRows:   make(map[int64][]types.Datum),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	return dt
}

// DirtyTable stores uncommitted write operation for a transaction.
type DirtyTable struct {
	// addedRows ...
	// the key is handle.
	addedRows   map[int64][]types.Datum
	deletedRows map[int64]struct{}
	truncated   bool
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

	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int

	addedRows           [][]types.Datum
	cursor4AddRows      int
	sortErr             error
	snapshotRows        [][]types.Datum
	cursor4SnapshotRows int
	snapshotChunkBuffer *chunk.Chunk
}

// Open implements the Executor Open interface.
func (us *UnionScanExec) Open(ctx context.Context) error {
	if err := us.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	us.snapshotChunkBuffer = us.newChunk()
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	mutableRow := chunk.MutRowFromTypes(us.retTypes())
	for i, batchSize := 0, us.ctx.GetSessionVars().MaxChunkSize; i < batchSize; i++ {
		row, err := us.getOneRow(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if row == nil {
			return nil
		}
		mutableRow.SetDatums(row...)
		chk.AppendRow(mutableRow.ToRow())
	}
	return nil
}

// getOneRow gets one result row from dirty table or child.
func (us *UnionScanExec) getOneRow(ctx context.Context) ([]types.Datum, error) {
	for {
		snapshotRow, err := us.getSnapshotRow(ctx)
		if err != nil {
			return nil, errors.Trace(err)
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
				return nil, errors.Trace(err)
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
		err = us.children[0].Next(ctx, us.snapshotChunkBuffer)
		if err != nil || us.snapshotChunkBuffer.NumRows() == 0 {
			return nil, errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(us.snapshotChunkBuffer)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			snapshotHandle := row.GetInt64(us.belowHandleIndex)
			if _, ok := us.dirty.deletedRows[snapshotHandle]; ok {
				continue
			}
			if _, ok := us.dirty.addedRows[snapshotHandle]; ok {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			us.snapshotRows = append(us.snapshotRows, row.GetDatumRow(us.children[0].retTypes()))
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
		return isFirstRow, errors.Trace(err)
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
			return 0, errors.Trace(err)
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

func (us *UnionScanExec) buildAndSortAddedRows() error {
	us.addedRows = make([][]types.Datum, 0, len(us.dirty.addedRows))
	for h, data := range us.dirty.addedRows {
		newData := make([]types.Datum, 0, us.schema.Len())
		for _, col := range us.columns {
			if col.ID == model.ExtraHandleID {
				newData = append(newData, types.NewIntDatum(h))
			} else {
				newData = append(newData, data[col.Offset])
			}
		}
		matched, err := expression.EvalBool(us.ctx, us.conditions, chunk.MutRowFromDatums(newData).ToRow())
		if err != nil {
			return errors.Trace(err)
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
