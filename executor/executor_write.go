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
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
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

	columns, err := getUpdateColumns(e.OrderedList)
	if err != nil {
		return nil, errors.Trace(err)
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

		err1 = updateRecord(e.ctx, handle, oldData, newTableData, columns, tbl, offset, false)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		e.updatedRowKeys[k] = true
	}
	e.cursor++
	return &Row{}, nil
}

func getUpdateColumns(assignList []*ast.Assignment) (map[int]*ast.Assignment, error) {
	m := make(map[int]*ast.Assignment, len(assignList))
	for i, v := range assignList {
		m[i] = v
	}
	return m, nil
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

func updateRecord(ctx context.Context, h int64, oldData, newData []interface{}, updateColumns map[int]*ast.Assignment, t table.Table, offset int, onDuplicateUpdate bool) error {
	if err := t.LockRow(ctx, h); err != nil {
		return errors.Trace(err)
	}

	cols := t.Cols()
	touched := make(map[int]bool, len(cols))

	assignExists := false
	var newHandle interface{}
	for i, asgn := range updateColumns {
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
	if err := column.CastValues(ctx, newData, cols); err != nil {
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
		if variable.GetSessionVars(ctx).ClientCapability&mysql.ClientFoundRows > 0 {
			variable.GetSessionVars(ctx).AddAffectedRows(1)
		}
		return nil
	}

	var err error
	if newHandle != nil {
		err = t.RemoveRecord(ctx, h, oldData)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = t.AddRecord(ctx, newData)
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(ctx, h, oldData, newData, touched)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// Record affected rows.
	if !onDuplicateUpdate {
		variable.GetSessionVars(ctx).AddAffectedRows(1)
	} else {
		variable.GetSessionVars(ctx).AddAffectedRows(2)
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

// InsertValues is the data to insert.
type InsertValues struct {
	ctx        context.Context
	SelectExec Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]ast.ExprNode
	Setlist []*ast.Assignment
}

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues

	OnDuplicate []*ast.Assignment
	fields      []*ast.ResultField

	Priority int

	finished bool
}

// Next implements Executor Next interface.
func (e *InsertExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	toUpdateColumns, err := getOnDuplicateUpdateColumns(e.OnDuplicate, e.Table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows [][]interface{}
	if e.SelectExec != nil {
		rows, err = e.getRowsSelect(cols)
	} else {
		rows, err = e.getRows(cols)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, row := range rows {
		if len(e.OnDuplicate) == 0 {
			txn.SetOption(kv.PresumeKeyNotExists, nil)
		}
		h, err := e.Table.AddRecord(e.ctx, row)
		txn.DelOption(kv.PresumeKeyNotExists)
		if err == nil {
			continue
		}

		if len(e.OnDuplicate) == 0 || !terror.ErrorEqual(err, kv.ErrKeyExists) {
			return nil, errors.Trace(err)
		}
		if err = e.onDuplicateUpdate(row, h, toUpdateColumns); err != nil {
			return nil, errors.Trace(err)
		}
	}
	e.finished = true
	return nil, nil
}

// Fields implements Executor Fields interface.
// Returns nil to indicate there is no output.
func (e *InsertExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *InsertExec) Close() error {
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) getColumns(tableCols []*column.Col) ([]*column.Col, error) {
	var cols []*column.Col
	var err error

	if len(e.Setlist) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(e.Setlist))
		for _, v := range e.Setlist {
			columns = append(columns, v.Column.Name.O)
		}

		cols, err = column.FindCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.TableName().O, err)
		}

		if len(cols) == 0 {
			return nil, errors.Errorf("INSERT INTO %s: empty column", e.Table.TableName().O)
		}
	} else {
		// Process `name` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.O)
		}
		cols, err = column.FindCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.TableName().O, err)
		}

		// If cols are empty, use all columns instead.
		if len(cols) == 0 {
			cols = tableCols
		}
	}

	// Check column whether is specified only once.
	err = column.CheckOnce(cols)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cols, nil
}

func (e *InsertValues) fillValueList() error {
	if len(e.Setlist) > 0 {
		if len(e.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", e.Table)
		}
		var l []ast.ExprNode
		for _, v := range e.Setlist {
			l = append(l, v.Expr)
		}
		e.Lists = append(e.Lists, l)
	}
	return nil
}

func (e *InsertValues) checkValueCount(insertValueCount, valueCount, num int, cols []*column.Col) error {
	if insertValueCount != valueCount {
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		// So the value count must be same for all insert list.
		return errors.Errorf("Column count doesn't match value count at row %d", num+1)
	}
	if valueCount == 0 && len(e.Columns) > 0 {
		// "insert into t (c1) values ()" is not valid.
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", e.Table.TableName().O, len(e.Columns), 0)
	} else if valueCount > 0 && valueCount != len(cols) {
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", e.Table.TableName().O, len(cols), valueCount)
	}
	return nil
}

func (e *InsertValues) getColumnDefaultValues(cols []*column.Col) (map[string]interface{}, error) {
	defaultValMap := map[string]interface{}{}
	for _, col := range cols {
		if value, ok, err := tables.GetColDefaultValue(e.ctx, &col.ColumnInfo); ok {
			if err != nil {
				return nil, errors.Trace(err)
			}
			defaultValMap[col.Name.L] = value
		}
	}
	return defaultValMap, nil
}

func (e *InsertValues) getRows(cols []*column.Col) (rows [][]interface{}, err error) {
	// process `insert|replace ... set x=y...`
	if err = e.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	defaultVals, err := e.getColumnDefaultValues(e.Table.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([][]interface{}, len(e.Lists))
	for i, list := range e.Lists {
		if err = e.checkValueCount(len(e.Lists[0]), len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		rows[i], err = e.getRow(cols, list, defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

func (e *InsertValues) getRow(cols []*column.Col, list []ast.ExprNode, defaultVals map[string]interface{}) ([]interface{}, error) {
	vals := make([]interface{}, len(list))
	var err error
	for i, expr := range list {
		if d, ok := expr.(*ast.DefaultExpr); ok {
			cn := d.Name
			if cn != nil {
				var found bool
				vals[i], found = defaultVals[cn.Name.L]
				if !found {
					return nil, errors.Errorf("default column not found - %s", cn.Name.O)
				}
			} else {
				vals[i] = defaultVals[cols[i].Name.L]
			}
		} else {
			vals[i], err = evaluator.Eval(e.ctx, expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return e.fillRowData(cols, vals)
}

func (e *InsertValues) getRowsSelect(cols []*column.Col) ([][]interface{}, error) {
	// process `insert|replace into ... select ... from ...`
	if len(e.SelectExec.Fields()) != len(cols) {
		return nil, errors.Errorf("Column count %d doesn't match value count %d", len(cols), len(e.SelectExec.Fields()))
	}
	var rows [][]interface{}
	for {
		innerRow, err := e.SelectExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if innerRow == nil {
			break
		}
		row, err := e.fillRowData(cols, innerRow.Data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *InsertValues) fillRowData(cols []*column.Col, vals []interface{}) ([]interface{}, error) {
	row := make([]interface{}, len(e.Table.Cols()))
	marked := make(map[int]struct{}, len(vals))
	for i, v := range vals {
		offset := cols[i].Offset
		row[offset] = v
		marked[offset] = struct{}{}
	}
	err := e.initDefaultValues(row, marked)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = column.CastValues(e.ctx, row, cols); err != nil {
		return nil, errors.Trace(err)
	}
	if err = column.CheckNotNull(e.Table.Cols(), row); err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

func (e *InsertValues) initDefaultValues(row []interface{}, marked map[int]struct{}) error {
	var defaultValueCols []*column.Col
	for i, c := range e.Table.Cols() {
		if row[i] != nil {
			// Column value is not nil, continue.
			continue
		}

		// If the nil value is evaluated in insert list, we will use nil except auto increment column.
		if _, ok := marked[i]; ok && !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
			continue
		}

		if mysql.HasAutoIncrementFlag(c.Flag) {
			recordID, err := e.Table.AllocAutoID()
			if err != nil {
				return errors.Trace(err)
			}
			row[i] = recordID
			if c.IsPKHandleColumn(e.Table.Meta()) {
				// Notes: incompatible with mysql
				// MySQL will set last insert id to the first row, as follows:
				// `t(id int AUTO_INCREMENT, c1 int, PRIMARY KEY (id))`
				// `insert t (c1) values(1),(2),(3);`
				// Last insert id will be 1, not 3.
				variable.GetSessionVars(e.ctx).SetLastInsertID(uint64(recordID))
			}
		} else {
			var value interface{}
			value, _, err := tables.GetColDefaultValue(e.ctx, &c.ColumnInfo)
			if err != nil {
				return errors.Trace(err)
			}

			row[i] = value
		}

		defaultValueCols = append(defaultValueCols, c)
	}
	if err := column.CastValues(e.ctx, row, defaultValueCols); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *InsertExec) onDuplicateUpdate(row []interface{}, h int64, cols map[int]*ast.Assignment) error {
	// On duplicate key update the duplicate row.
	// Evaluate the updated value.
	// TODO: report rows affected and last insert id.
	data, err := e.Table.Row(e.ctx, h)
	if err != nil {
		return errors.Trace(err)
	}
	// For evaluate ValuesExpr
	// http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	for i, rf := range e.fields {
		rf.Expr.SetValue(row[i])
	}
	// Evaluate assignment
	newData := make([]interface{}, len(data))
	for i, c := range row {
		asgn, ok := cols[i]
		if !ok {
			newData[i] = c
			continue
		}
		newData[i], err = evaluator.Eval(e.ctx, asgn.Expr)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if err = updateRecord(e.ctx, h, data, newData, cols, e.Table, 0, true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func findColumnByName(t table.Table, name string) (*column.Col, error) {
	_, tableName, colName := field.SplitQualifiedName(name)
	if len(tableName) > 0 && tableName != t.TableName().O {
		return nil, errors.Errorf("unknown field %s.%s", tableName, colName)
	}

	c := column.FindCol(t.Cols(), colName)
	if c == nil {
		return nil, errors.Errorf("unknown field %s", colName)
	}
	return c, nil
}

func getOnDuplicateUpdateColumns(assignList []*ast.Assignment, t table.Table) (map[int]*ast.Assignment, error) {
	m := make(map[int]*ast.Assignment, len(assignList))

	for _, v := range assignList {
		col := v.Column
		c, err := findColumnByName(t, field.JoinQualifiedName("", col.Table.L, col.Name.L))
		if err != nil {
			return nil, errors.Trace(err)
		}
		m[c.Offset] = v
	}
	return m, nil
}

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
	finished bool
}

// Fields implements Executor Fields interface.
// Returns nil to indicate there is no output.
func (e *ReplaceExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *ReplaceExec) Close() error {
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Next implements Executor Next interface.
func (e *ReplaceExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows [][]interface{}
	if e.SelectExec != nil {
		rows, err = e.getRowsSelect(cols)
	} else {
		rows, err = e.getRows(cols)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, row := range rows {
		h, err := e.Table.AddRecord(e.ctx, row)
		if err == nil {
			continue
		}
		if err != nil && !terror.ErrorEqual(err, kv.ErrKeyExists) {
			return nil, errors.Trace(err)
		}

		// While the insertion fails because a duplicate-key error occurs for a primary key or unique index,
		// a storage engine may perform the REPLACE as an update rather than a delete plus insert.
		// See: http://dev.mysql.com/doc/refman/5.7/en/replace.html.
		if err = e.replaceRow(h, row); err != nil {
			return nil, errors.Trace(err)
		}
		variable.GetSessionVars(e.ctx).AddAffectedRows(1)
	}
	e.finished = true
	return nil, nil
}

func (e *ReplaceExec) replaceRow(handle int64, replaceRow []interface{}) error {
	row, err := e.Table.Row(e.ctx, handle)
	if err != nil {
		return errors.Trace(err)
	}

	isReplace := false
	touched := make(map[int]bool, len(row))
	for i, val := range row {
		v, err1 := types.Compare(val, replaceRow[i])
		if err1 != nil {
			return errors.Trace(err1)
		}
		if v != 0 {
			touched[i] = true
			isReplace = true
		}
	}
	if isReplace {
		variable.GetSessionVars(e.ctx).AddAffectedRows(1)
		if err = e.Table.UpdateRecord(e.ctx, handle, row, replaceRow, touched); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
