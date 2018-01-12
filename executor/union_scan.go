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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
)

// dirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type dirtyDB struct {
	// tables is a map whose key is tableID.
	tables map[int64]*dirtyTable
}

func (udb *dirtyDB) addRow(tid, handle int64, row []types.Datum) {
	dt := udb.getDirtyTable(tid)
	for i := range row {
		if row[i].Kind() == types.KindString {
			row[i].SetBytes(row[i].GetBytes())
		}
	}
	dt.addedRows[handle] = row
}

func (udb *dirtyDB) deleteRow(tid int64, handle int64) {
	dt := udb.getDirtyTable(tid)
	delete(dt.addedRows, handle)
	dt.deletedRows[handle] = struct{}{}
}

func (udb *dirtyDB) truncateTable(tid int64) {
	dt := udb.getDirtyTable(tid)
	dt.addedRows = make(map[int64]Row)
	dt.truncated = true
}

func (udb *dirtyDB) getDirtyTable(tid int64) *dirtyTable {
	dt, ok := udb.tables[tid]
	if !ok {
		dt = &dirtyTable{
			addedRows:   make(map[int64]Row),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	return dt
}

type dirtyTable struct {
	// addedRows ...
	// the key is handle.
	addedRows   map[int64]Row
	deletedRows map[int64]struct{}
	truncated   bool
}

func getDirtyDB(ctx context.Context) *dirtyDB {
	var udb *dirtyDB
	x := ctx.GetSessionVars().TxnCtx.DirtyDB
	if x == nil {
		udb = &dirtyDB{tables: make(map[int64]*dirtyTable)}
		ctx.GetSessionVars().TxnCtx.DirtyDB = udb
	} else {
		udb = x.(*dirtyDB)
	}
	return udb
}

// UnionScanExec merges the rows from dirty table and the rows from XAPI request.
type UnionScanExec struct {
	baseExecutor

	dirty *dirtyTable
	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex  []int
	desc       bool
	conditions []expression.Expression
	columns    []*model.ColumnInfo

	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int

	addedRows   []Row
	cursor      int
	sortErr     error
	snapshotRow Row
}

// Next implements Execution Next interface.
func (us *UnionScanExec) Next(goCtx goctx.Context) (Row, error) {
	row, err := us.getOneRow(goCtx)
	return row, errors.Trace(err)
}

// NextChunk implements the Executor NextChunk interface.
func (us *UnionScanExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	mutableRow := chunk.MutRowFromTypes(us.retTypes())
	for i, batchSize := 0, us.ctx.GetSessionVars().MaxChunkSize; i < batchSize; i++ {
		row, err := us.getOneRow(goCtx)
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
func (us *UnionScanExec) getOneRow(goCtx goctx.Context) (Row, error) {
	for {
		snapshotRow, err := us.getSnapshotRow(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		addedRow := us.getAddedRow()
		var row Row
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
			us.snapshotRow = nil
		} else {
			us.cursor++
		}
		return row, nil
	}
}

func (us *UnionScanExec) getSnapshotRow(goCtx goctx.Context) (Row, error) {
	if us.dirty.truncated {
		return nil, nil
	}
	var err error
	if us.snapshotRow == nil {
		for {
			us.snapshotRow, err = us.children[0].Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if us.snapshotRow == nil {
				break
			}
			snapshotHandle := us.snapshotRow[us.belowHandleIndex].GetInt64()
			if _, ok := us.dirty.deletedRows[snapshotHandle]; ok {
				continue
			}
			if _, ok := us.dirty.addedRows[snapshotHandle]; ok {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			break
		}
	}
	return us.snapshotRow, nil
}

func (us *UnionScanExec) getAddedRow() Row {
	var addedRow Row
	if us.cursor < len(us.addedRows) {
		addedRow = us.addedRows[us.cursor]
	}
	return addedRow
}

// shouldPickFirstRow picks the suitable row in order.
// The value returned is used to determine whether to pick the first input row.
func (us *UnionScanExec) shouldPickFirstRow(a, b Row) (bool, error) {
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

func (us *UnionScanExec) compare(a, b Row) (int, error) {
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
	us.addedRows = make([]Row, 0, len(us.dirty.addedRows))
	for h, data := range us.dirty.addedRows {
		newData := make(types.DatumRow, 0, us.schema.Len())
		for _, col := range us.columns {
			if col.ID == model.ExtraHandleID {
				newData = append(newData, types.NewIntDatum(h))
			} else {
				newData = append(newData, data[col.Offset])
			}
		}
		matched, err := expression.EvalBool(us.conditions, newData, us.ctx)
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
