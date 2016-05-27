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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

// dirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type dirtyDB struct {
	// Key is tableID.
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
	dt.addedRows = make(map[int64][]types.Datum)
	dt.truncated = true
}

func (udb *dirtyDB) getDirtyTable(tid int64) *dirtyTable {
	dt, ok := udb.tables[tid]
	if !ok {
		dt = &dirtyTable{
			addedRows:   make(map[int64][]types.Datum),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	return dt
}

type dirtyTable struct {
	// key is handle.
	addedRows   map[int64][]types.Datum
	deletedRows map[int64]struct{}
	truncated   bool
}

type dirtyDBKeyType int

func (u dirtyDBKeyType) String() string {
	return "dirtyDBKeyType"
}

// DirtyDBKey is the key to *dirtyDB for a context.
const DirtyDBKey dirtyDBKeyType = 1

func getDirtyDB(ctx context.Context) *dirtyDB {
	var udb *dirtyDB
	x := ctx.Value(DirtyDBKey)
	if x == nil {
		udb = &dirtyDB{tables: make(map[int64]*dirtyTable)}
		ctx.SetValue(DirtyDBKey, udb)
	} else {
		udb = x.(*dirtyDB)
	}
	return udb
}

// UnionScanExec merges the rows from dirty table and the rows from XAPI request.
type UnionScanExec struct {
	ctx   context.Context
	Src   Executor
	dirty *dirtyTable
	// srcUsedIndex is the column offsets of the index which Src executor has used.
	usedIndex []int
	desc      bool
	condition ast.ExprNode

	addedRows   []*Row
	cursor      int
	sortErr     error
	snapshotRow *Row
}

// Fields implements Executor Fields interface.
func (us *UnionScanExec) Fields() []*ast.ResultField {
	return us.Src.Fields()
}

// Next implements Execution Next interface.
func (us *UnionScanExec) Next() (*Row, error) {
	for {
		snapshotRow, err := us.getSnapshotRow()
		if err != nil {
			return nil, errors.Trace(err)
		}
		addedRow := us.getAddedRow()
		var row *Row
		if addedRow == nil {
			row = snapshotRow
		} else if snapshotRow == nil {
			row = addedRow
		} else {
			row, err = us.pickRow(addedRow, snapshotRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if row == nil {
			return nil, nil
		}
		if row == snapshotRow {
			us.snapshotRow = nil
		} else {
			us.cursor++
		}
		for i, field := range us.Src.Fields() {
			field.Expr.SetDatum(row.Data[i])
		}
		return row, nil
	}
}

func (us *UnionScanExec) getSnapshotRow() (*Row, error) {
	if us.dirty.truncated {
		return nil, nil
	}
	var err error
	if us.snapshotRow == nil {
		for {
			us.snapshotRow, err = us.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if us.snapshotRow == nil {
				break
			}
			if len(us.snapshotRow.RowKeys) != 1 {
				return nil, ErrRowKeyCount
			}
			snapshotHandle := us.snapshotRow.RowKeys[0].Handle
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

func (us *UnionScanExec) getAddedRow() *Row {
	var addedRow *Row
	if us.cursor < len(us.addedRows) {
		addedRow = us.addedRows[us.cursor]
	}
	return addedRow
}

func (us *UnionScanExec) pickRow(a, b *Row) (*Row, error) {
	addedCmpSrc, err := us.compare(a, b)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var row *Row
	// Compare result will never be 0.
	if us.desc {
		if addedCmpSrc < 0 {
			row = b
		} else {
			row = a
		}
	} else {
		if addedCmpSrc < 0 {
			row = a
		} else {
			row = b
		}
	}
	return row, nil
}

// Close implements Executor Close interface.
func (us *UnionScanExec) Close() error {
	return us.Src.Close()
}

func (us *UnionScanExec) compare(a, b *Row) (int, error) {
	for _, colOff := range us.usedIndex {
		aColumn := a.Data[colOff]
		bColumn := b.Data[colOff]
		cmp, err := aColumn.CompareDatum(bColumn)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	aHandle := a.RowKeys[0].Handle
	bHandle := b.RowKeys[0].Handle
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

func (us *UnionScanExec) buildAndSortAddedRows(t table.Table, asName *model.CIStr) error {
	us.addedRows = make([]*Row, 0, len(us.dirty.addedRows))
	for h, data := range us.dirty.addedRows {
		for i, field := range us.Src.Fields() {
			field.Expr.SetDatum(data[i])
		}
		if us.condition != nil {
			matched, err := evaluator.EvalBool(us.ctx, us.condition)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		rowKeyEntry := &RowKeyEntry{Handle: h, Tbl: t, TableAsName: asName}
		row := &Row{Data: data, RowKeys: []*RowKeyEntry{rowKeyEntry}}
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
