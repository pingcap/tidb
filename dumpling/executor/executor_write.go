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
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
)

// UpdateExec represents an update executor.
type UpdateExec struct {
	SelectExec  Executor
	OrderedList []*ast.Assignment

	updatedRowKeys map[string]bool
	ctx            context.Context

	rows        []*Row          // The rows fetched from TableExec.
	newRowsData [][]interface{} // The new values to be set.
	fetched     bool
	cursor      int
}

// Next implements Executor Next interface.
func (e *UpdateExec) Next() (*Row, error) {
	if !e.fetched {
		err := e.fetchRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.fetched = true
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = map[string]bool{}
	}
	row := e.rows[e.cursor]
	newData := e.newRowsData[e.cursor]
	for _, entry := range row.RowKeys {
		tbl := entry.Tbl
		offset := e.getTableOffset(tbl)
		k := entry.Key
		oldData := row.Data[offset : offset+len(tbl.Cols())]
		newTableData := newData[offset : offset+len(tbl.Cols())]

		_, ok := e.updatedRowKeys[k]
		if ok {
			// Each matching row is updated once, even if it matches the conditions multiple times.
			continue
		}

		// Update row
		handle, err1 := tables.DecodeRecordKeyHandle(kv.Key(k))
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		err1 = e.updateRecord(handle, oldData, newTableData, tbl, offset, false)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		e.updatedRowKeys[k] = true
	}
	e.cursor++
	return &Row{}, nil
}

func (e *UpdateExec) fetchRows() error {
	for {
		row, err := e.SelectExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}
		data := make([]interface{}, len(e.SelectExec.Fields()))
		newData := make([]interface{}, len(e.SelectExec.Fields()))
		for i, f := range e.SelectExec.Fields() {
			data[i] = f.Expr.GetValue()
			newData[i] = data[i]
			if e.OrderedList[i] != nil {
				val, err := evaluator.Eval(e.ctx, e.OrderedList[i].Expr)
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

func (e *UpdateExec) getTableOffset(t table.Table) int {
	fields := e.SelectExec.Fields()
	i := 0
	for i < len(fields) {
		field := fields[i]
		if field.Table.Name.L == t.TableName().L {
			return i
		}
		i += len(field.Table.Columns)
	}
	return 0
}

func (e *UpdateExec) updateRecord(h int64, oldData, newData []interface{}, t table.Table, offset int, onDuplicateUpdate bool) error {
	if err := t.LockRow(e.ctx, h); err != nil {
		return errors.Trace(err)
	}

	cols := t.Cols()
	touched := make(map[int]bool, len(cols))

	assignExists := false
	var newHandle interface{}
	for i, asgn := range e.OrderedList {
		if asgn == nil {
			continue
		}
		if i < offset || i >= offset+len(cols) {
			// The assign expression is for another table, not this.
			continue
		}

		colIndex := i - offset
		col := cols[colIndex]
		if col.IsPKHandleColumn(t.Meta()) {
			newHandle = newData[i]
		}

		touched[colIndex] = true
		assignExists = true
	}

	// If no assign list for this table, no need to update.
	if !assignExists {
		return nil
	}

	// Check whether new value is valid.
	if err := column.CastValues(e.ctx, newData, cols); err != nil {
		return errors.Trace(err)
	}

	if err := column.CheckNotNull(cols, newData); err != nil {
		return errors.Trace(err)
	}

	// If row is not changed, we should do nothing.
	rowChanged := false
	for i := range oldData {
		if !touched[i] {
			continue
		}

		n, err := types.Compare(newData[i], oldData[i])
		if err != nil {
			return errors.Trace(err)
		}
		if n != 0 {
			rowChanged = true
			break
		}
	}

	if !rowChanged {
		// See: https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if variable.GetSessionVars(e.ctx).ClientCapability&mysql.ClientFoundRows > 0 {
			variable.GetSessionVars(e.ctx).AddAffectedRows(1)
		}
		return nil
	}

	var err error
	if newHandle != nil {
		err = t.RemoveRecord(e.ctx, h, oldData)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = t.AddRecord(e.ctx, newData)
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(e.ctx, h, oldData, newData, touched)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// Record affected rows.
	if !onDuplicateUpdate {
		variable.GetSessionVars(e.ctx).AddAffectedRows(1)
	} else {
		variable.GetSessionVars(e.ctx).AddAffectedRows(2)
	}
	return nil
}

// Fields implements Executor Fields interface.
// Returns nil to indicate there is no output.
func (e *UpdateExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *UpdateExec) Close() error {
	return e.SelectExec.Close()
}

// DeleteExec represents a delete executor.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	SelectExec Executor

	ctx          context.Context
	Tables       []*ast.TableName
	IsMultiTable bool

	finished bool
}

// Next implements Executor Next interface.
func (e *DeleteExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	defer func() {
		e.finished = true
	}()
	if e.IsMultiTable && len(e.Tables) == 0 {
		return &Row{}, nil
	}
	tblIDMap := make(map[int64]bool, len(e.Tables))
	// Get table alias map.
	tblNames := make(map[string]string)
	rowKeyMap := make(map[string]table.Table)
	if e.IsMultiTable {
		// Delete from multiple tables should consider table ident list.
		fs := e.SelectExec.Fields()
		for _, f := range fs {
			if len(f.TableAsName.L) > 0 {
				tblNames[f.TableAsName.L] = f.TableName.Name.L
			} else {
				tblNames[f.TableName.Name.L] = f.TableName.Name.L
			}
		}
		for _, t := range e.Tables {
			// Consider DBName.
			_, ok := tblNames[t.Name.L]
			if !ok {
				return nil, errors.Errorf("Unknown table '%s' in MULTI DELETE", t.Name.O)
			}
			tblIDMap[t.TableInfo.ID] = true
		}
	}
	for {
		row, err := e.SelectExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}

		for _, entry := range row.RowKeys {
			if e.IsMultiTable {
				tid := entry.Tbl.TableID()
				if _, ok := tblIDMap[tid]; !ok {
					continue
				}
			}
			rowKeyMap[entry.Key] = entry.Tbl
		}
	}
	for k, t := range rowKeyMap {
		handle, err := tables.DecodeRecordKeyHandle(kv.Key(k))
		if err != nil {
			return nil, errors.Trace(err)
		}
		data, err := t.Row(e.ctx, handle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = e.removeRow(e.ctx, t, handle, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}

func (e *DeleteExec) getTable(ctx context.Context, tableName *ast.TableName) (table.Table, error) {
	return sessionctx.GetDomain(ctx).InfoSchema().TableByName(tableName.Schema, tableName.Name)
}

func (e *DeleteExec) removeRow(ctx context.Context, t table.Table, h int64, data []interface{}) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return errors.Trace(err)
	}
	variable.GetSessionVars(ctx).AddAffectedRows(1)
	return nil
}

// Fields implements Executor Fields interface.
// Returns nil to indicate there is no output.
func (e *DeleteExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *DeleteExec) Close() error {
	return e.SelectExec.Close()
}
