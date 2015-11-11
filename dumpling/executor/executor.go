// Copyright 2015 PingCAP, Inc.
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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &TableScanExec{}
	_ Executor = &IndexScanExec{}
	_ Executor = &IndexRangeExec{}
	_ Executor = &SelectFieldsExec{}
	_ Executor = &FilterExec{}
	_ Executor = &LimitExec{}
	_ Executor = &SortExec{}
)

// Row represents a record row.
type Row struct {
	// Data is the output record data for current Plan.
	Data []interface{}

	RowKeys []*RowKeyEntry
}

// RowKeyEntry is designed for Delete statement in multi-table mode,
// we should know which table this row comes from.
type RowKeyEntry struct {
	// The table which this row come from.
	Tbl table.Table
	// Row handle.
	Key string
}

// Executor executes a query.
type Executor interface {
	Fields() []*ast.ResultField
	Next() (*Row, error)
	Close() error
}

// TableScanExec represents a table scan executor.
type TableScanExec struct {
	t      table.Table
	fields []*ast.ResultField
	iter   kv.Iterator
	ctx    context.Context
}

// Fields implements Executor Fields interface.
func (e *TableScanExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Execution Next interface.
func (e *TableScanExec) Next() (row *Row, err error) {
	if e.iter == nil {
		var txn kv.Transaction
		txn, err = e.ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.iter, err = txn.Seek([]byte(e.t.FirstKey()))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !e.iter.Valid() || !strings.HasPrefix(e.iter.Key(), e.t.KeyPrefix()) {
		return
	}
	// TODO: check if lock valid
	// the record layout in storage (key -> value):
	// r1 -> lock-version
	// r1_col1 -> r1 col1 value
	// r1_col2 -> r1 col2 value
	// r2 -> lock-version
	// r2_col1 -> r2 col1 value
	// r2_col2 -> r2 col2 value
	// ...
	rowKey := e.iter.Key()
	handle, err := util.DecodeHandleFromRowKey(rowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO: we could just fetch mentioned columns' values
	row = &Row{}
	row.Data, err = e.t.Row(e.ctx, handle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Put rowKey to the tail of record row
	rke := &RowKeyEntry{
		Tbl: e.t,
		Key: rowKey,
	}
	row.RowKeys = append(row.RowKeys, rke)

	rk := e.t.RecordKey(handle, nil)
	err = kv.NextUntil(e.iter, util.RowKeyPrefixFilter(rk))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

// Close implements plan.Plan Close interface.
func (e *TableScanExec) Close() error {
	if e.iter != nil {
		e.iter.Close()
		e.iter = nil
	}
	return nil
}

type bound int

const (
	minNotNullVal bound = 0
	maxVal        bound = 1
)

// IndexRangeExec represents an index range scan executor.
type IndexRangeExec struct {
	ctx    context.Context
	tbl    table.Table
	fields []*ast.ResultField
	idx    kv.Index
	// seekVal is different from lowVal, it is casted from lowVal and
	// must be less than or equal to lowVal, used to seek the index.
	seekVal     interface{}
	lowVal      interface{}
	lowExclude  bool
	highVal     interface{}
	highExclude bool
	Desc        bool

	iter kv.IndexIterator
}

// Fields implements Executor Fields interface.
func (e *IndexRangeExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *IndexRangeExec) Next() (*Row, error) {
	if e.iter == nil {
		seekVal := e.seekVal
		if e.lowVal == minNotNullVal {
			seekVal = []byte{}
		}
		txn, err := e.ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.iter, _, err = e.idx.Seek(txn, []interface{}{seekVal})
		if err != nil {
			return nil, types.EOFAsNil(err)
		}
	}
	// TODO: implement
	return nil, nil
}

// Close implements Executor Close interface.
func (e *IndexRangeExec) Close() error {
	return nil
}

// IndexScanExec represents an index scan executor.
type IndexScanExec struct {
	Table    table.Table
	fields   []*ast.ResultField
	Ranges   []*IndexRangeExec
	rangeIdx int
}

// Fields implements Executor Fields interface.
func (e *IndexScanExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *IndexScanExec) Next() (*Row, error) {
	for e.rangeIdx < len(e.Ranges) {
		ran := e.Ranges[e.rangeIdx]
		row, err := ran.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			for i, val := range row.Data {
				e.fields[i].Expr.SetValue(val)
			}
			return row, nil
		}
		ran.Close()
		e.rangeIdx++
	}
	return nil, nil
}

// Close implements Executor Close interface.
func (e *IndexScanExec) Close() error {
	return nil
}

// SelectFieldsExec represents a select fields executor.
type SelectFieldsExec struct {
	Src          Executor
	ResultFields []*ast.ResultField
}

// Fields implements Executor Fields interface.
func (e *SelectFieldsExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
func (e *SelectFieldsExec) Next() (*Row, error) {
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	row := &Row{
		RowKeys: srcRow.RowKeys,
		Data:    make([]interface{}, len(e.ResultFields)),
	}
	for i, field := range e.ResultFields {
		val, err := Eval(field.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row.Data[i] = val
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *SelectFieldsExec) Close() error {
	return e.Src.Close()
}

// FilterExec represents an filter executor.
type FilterExec struct {
	Src       Executor
	Condition ast.ExprNode
}

// Fields implements Executor Fields interface.
func (e *FilterExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *FilterExec) Next() (*Row, error) {
	for {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		truth, err := EvalBool(e.Condition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if truth {
			return srcRow, nil
		}
	}
}

// Close implements Executor Close interface.
func (e *FilterExec) Close() error {
	return e.Src.Close()
}

// LimitExec represents limit executor
type LimitExec struct {
	Src    Executor
	Offset int
	Limit  int
	Idx    int
}

// Fields implements Executor Fields interface.
func (e *LimitExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *LimitExec) Next() (*Row, error) {
	for e.Idx < e.Offset {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		e.Idx++
	}
	// Negative Limit means no limit.
	if e.Limit >= 0 && e.Idx >= e.Offset+e.Limit {
		return nil, nil
	}
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	e.Idx++
	return srcRow, nil
}

// Close implements Executor Close interface.
func (e *LimitExec) Close() error {
	return e.Src.Close()
}

// orderByRow bind a row to its order values, so it can be sorted.
type orderByRow struct {
	key []interface{}
	row *Row
}

// SortExec represents sorting executor.
type SortExec struct {
	Ctx     context.Context
	Src     Executor
	ByItems []*ast.ByItem
	Rows    []*orderByRow
	Idx     int
	fetched bool
}

// Fields implements Executor Fields interface.
func (e *SortExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Len returns the number of rows.
func (e *SortExec) Len() int {
	return len(e.Rows)
}

// Swap implements sort.Interface Swap interface.
func (e *SortExec) Swap(i, j int) {
	e.Rows[i], e.Rows[j] = e.Rows[j], e.Rows[i]
}

// Less implements sort.Interface Less interface.
func (e *SortExec) Less(i, j int) bool {
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := types.Compare(v1, v2)
		if err != nil {
			// we just have to log this error and skip it.
			// TODO: record this error and handle it out later.
			log.Errorf("compare %v %v err %v", v1, v2, err)
		}

		if by.Desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// Next implements Executor Next interface.
func (e *SortExec) Next() (*Row, error) {
	if !e.fetched {
		for {
			srcRow, err := e.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]interface{}, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				orderRow.key[i], err = Eval(byItem.Expr)
			}
			e.Rows = append(e.Rows, orderRow)
		}
		e.fetched = true
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}

// Close implements Executor Close interface.
func (e *SortExec) Close() error {
	return nil
}
