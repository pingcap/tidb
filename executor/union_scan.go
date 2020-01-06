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
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// DirtyDB stores uncommitted write operations for a transaction.
// It is stored and retrieved by context.Value and context.SetValue method.
type DirtyDB struct {
	sync.Mutex

	// tables is a map whose key is tableID.
	tables map[int64]*DirtyTable
}

// GetDirtyTable gets the DirtyTable by id from the DirtyDB.
func (udb *DirtyDB) GetDirtyTable(tid int64) *DirtyTable {
	// The index join access the tables map parallelly.
	// But the map throws panic in this case. So it's locked.
	udb.Lock()
	dt, ok := udb.tables[tid]
	if !ok {
		dt = &DirtyTable{
			tid:         tid,
			addedRows:   make(map[int64]struct{}),
			deletedRows: make(map[int64]struct{}),
		}
		udb.tables[tid] = dt
	}
	udb.Unlock()
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

// IsEmpty checks whether the table is empty.
func (dt *DirtyTable) IsEmpty() bool {
	return len(dt.addedRows)+len(dt.deletedRows) == 0
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
		return err
	}
	us.snapshotChunkBuffer = newFirstChunk(us)
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("unionScan.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	if us.runtimeStats != nil {
		start := time.Now()
		defer func() { us.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
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
				continue
			}
			if _, ok := us.dirty.addedRows[snapshotHandle]; ok {
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
func (us *UnionScanExec) rowWithColsInTxn(t table.Table, h int64, cols []*table.Column) ([]types.Datum, error) {
	key := t.RecordKey(h)
	txn, err := us.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.GetMemBuffer().Get(key)
	if err != nil {
		return nil, err
	}
	v, _, err := tables.DecodeRawRowData(us.ctx, t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (us *UnionScanExec) buildAndSortAddedRows(t table.Table) error {
	us.addedRows = make([][]types.Datum, 0, len(us.dirty.addedRows))
	mutableRow := chunk.MutRowFromTypes(retTypes(us))
	cols := t.WritableCols()
	for h := range us.dirty.addedRows {
		newData := make([]types.Datum, 0, us.schema.Len())
		data, err := us.rowWithColsInTxn(t, h, cols)
		if err != nil {
			return err
		}
		for _, col := range us.columns {
			if col.ID == model.ExtraHandleID {
				newData = append(newData, types.NewIntDatum(h))
			} else {
				newData = append(newData, data[col.Offset])
			}
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
