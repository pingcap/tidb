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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

// NewUpdateExec represents a new update executor.
type NewUpdateExec struct {
	SelectExec  Executor
	OrderedList []*expression.Assignment

	// Map for unique (Table, handle) pair.
	updatedRowKeys map[table.Table]map[int64]struct{}
	ctx            context.Context

	rows        []*Row          // The rows fetched from TableExec.
	newRowsData [][]types.Datum // The new values to be set.
	fetched     bool
	cursor      int
}

// Schema implements Executor Schema interface.
func (e *NewUpdateExec) Schema() expression.Schema {
	return nil
}

// Next implements Executor Next interface.
func (e *NewUpdateExec) Next() (*Row, error) {
	if !e.fetched {
		err := e.fetchRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.fetched = true
	}

	assignFlag, err := getNewUpdateColumns(e.OrderedList)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = make(map[table.Table]map[int64]struct{})
	}
	row := e.rows[e.cursor]
	newData := e.newRowsData[e.cursor]
	for _, entry := range row.RowKeys {
		tbl := entry.Tbl
		if e.updatedRowKeys[tbl] == nil {
			e.updatedRowKeys[tbl] = make(map[int64]struct{})
		}
		offset := e.getTableOffset(*entry)
		handle := entry.Handle
		oldData := row.Data[offset : offset+len(tbl.WritableCols())]
		newTableData := newData[offset : offset+len(tbl.WritableCols())]
		_, ok := e.updatedRowKeys[tbl][handle]
		if ok {
			// Each matched row is updated once, even if it matches the conditions multiple times.
			continue
		}
		// Update row
		err1 := updateRecord(e.ctx, handle, oldData, newTableData, assignFlag, tbl, offset, false)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		e.updatedRowKeys[tbl][handle] = struct{}{}
	}
	e.cursor++
	return &Row{}, nil
}

func getNewUpdateColumns(assignList []*expression.Assignment) (map[int]bool, error) {
	assignFlag := make(map[int]bool, len(assignList))
	for i, v := range assignList {
		if v != nil {
			assignFlag[i] = true
		} else {
			assignFlag[i] = false
		}
	}
	return assignFlag, nil
}

func (e *NewUpdateExec) fetchRows() error {
	for {
		row, err := e.SelectExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}
		data := make([]types.Datum, len(e.SelectExec.Schema()))
		newData := make([]types.Datum, len(e.SelectExec.Schema()))
		for i, s := range e.SelectExec.Schema() {
			data[i], err = s.Eval(row.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			newData[i] = data[i]
			if e.OrderedList[i] != nil {
				val, err := e.OrderedList[i].Expr.Eval(row.Data, e.ctx)
				if err != nil {
					return errors.Trace(err)
				}
				newData[i] = val
			}
		}
		row.Data = data
		e.rows = append(e.rows, row)
		e.newRowsData = append(e.newRowsData, newData)
	}
}

func (e *NewUpdateExec) getTableOffset(entry RowKeyEntry) int {
	t := entry.Tbl
	var tblName string
	if entry.TableAsName == nil || len(entry.TableAsName.L) == 0 {
		tblName = t.Meta().Name.L
	} else {
		tblName = entry.TableAsName.L
	}
	schema := e.SelectExec.Schema()
	for i := 0; i < len(schema); i++ {
		s := schema[i]
		if s.TblName.L == tblName {
			return i
		}
	}
	return 0
}

// Fields implements Executor Fields interface.
// Returns nil to indicate there is no output.
func (e *NewUpdateExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *NewUpdateExec) Close() error {
	return e.SelectExec.Close()
}
