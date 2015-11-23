// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package stmts

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ stmt.Statement = (*InsertIntoStmt)(nil)

// Priority const values.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
const (
	NoPriority = iota
	LowPriority
	HighPriority
	DelayedPriority
)

// InsertValues is the rest part of insert/replace into statement.
type InsertValues struct {
	ColNames   []string
	Lists      [][]expression.Expression
	Sel        plan.Planner
	TableIdent table.Ident
	Setlist    []*expression.Assignment
	Priority   int
}

// InsertIntoStmt is a statement to insert new rows into an existing table.
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
type InsertIntoStmt struct {
	InsertValues
	OnDuplicate []*expression.Assignment

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *InsertIntoStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *InsertIntoStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *InsertIntoStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *InsertIntoStmt) SetText(text string) {
	s.Text = text
}

// execExecSelect implements `insert table select ... from ...`.
func (s *InsertValues) execSelect(t table.Table, cols []*column.Col, ctx context.Context) (rset.Recordset, error) {
	r, err := s.Sel.Plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer r.Close()

	if len(r.GetFields()) != len(cols) {
		return nil, errors.Errorf("Column count %d doesn't match value count %d", len(cols), len(r.GetFields()))
	}

	var bufRecords [][]interface{}
	var lastInsertIds []uint64
	for {
		var row *plan.Row
		row, err = r.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}

		currentRow := make([]interface{}, len(t.Cols()))
		marked := make(map[int]struct{}, len(t.Cols()))
		for i, data := range row.Data {
			offset := cols[i].Offset
			currentRow[offset] = data
			marked[offset] = struct{}{}
		}

		if err = s.initDefaultValues(ctx, t, currentRow, marked); err != nil {
			return nil, errors.Trace(err)
		}

		if err = column.CastValues(ctx, currentRow, cols); err != nil {
			return nil, errors.Trace(err)
		}

		if err = column.CheckNotNull(t.Cols(), currentRow); err != nil {
			return nil, errors.Trace(err)
		}

		var v interface{}
		v, err = types.Clone(currentRow)
		if err != nil {
			return nil, errors.Trace(err)
		}

		bufRecords = append(bufRecords, v.([]interface{}))
		lastInsertIds = append(lastInsertIds, variable.GetSessionVars(ctx).LastInsertID)
	}

	for i, r := range bufRecords {
		variable.GetSessionVars(ctx).SetLastInsertID(lastInsertIds[i])
		if _, err = t.AddRecord(ctx, r); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}

// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See: https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (s *InsertValues) getColumns(tableCols []*column.Col) ([]*column.Col, error) {
	var cols []*column.Col
	var err error

	if len(s.Setlist) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(s.Setlist))
		for _, v := range s.Setlist {
			columns = append(columns, v.ColName)
		}

		cols, err = column.FindCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", s.TableIdent, err)
		}

		if len(cols) == 0 {
			return nil, errors.Errorf("INSERT INTO %s: empty column", s.TableIdent)
		}
	} else {
		// Process `name` type column.
		cols, err = column.FindCols(tableCols, s.ColNames)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", s.TableIdent, err)
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

func (s *InsertValues) getColumnDefaultValues(ctx context.Context, cols []*column.Col) (map[interface{}]interface{}, error) {
	defaultValMap := map[interface{}]interface{}{}
	for _, col := range cols {
		if value, ok, err := tables.GetColDefaultValue(ctx, &col.ColumnInfo); ok {
			if err != nil {
				return nil, errors.Trace(err)
			}

			defaultValMap[col.Name.L] = value
		}
	}

	return defaultValMap, nil
}

func (s *InsertValues) fillValueList() error {
	if len(s.Setlist) > 0 {
		if len(s.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", s.TableIdent)
		}

		var l []expression.Expression
		for _, v := range s.Setlist {
			l = append(l, v.Expr)
		}
		s.Lists = append(s.Lists, l)
	}

	return nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *InsertIntoStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	t, err := getTable(ctx, s.TableIdent)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cols, err := s.getColumns(t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Process `insert ... (select ..) `
	// TODO: handles the duplicate-key in a primary key or a unique index.
	if s.Sel != nil {
		return s.execSelect(t, cols, ctx)
	}

	// Process `insert ... set x=y...`
	if err = s.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	defaultValMap, err := s.getColumnDefaultValues(ctx, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	insertValueCount := len(s.Lists[0])
	toUpdateColumns, err := getOnDuplicateUpdateColumns(s.OnDuplicate, t)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows := make([][]interface{}, len(s.Lists))
	lastInsertIds := make([]uint64, len(s.Lists))
	for i, list := range s.Lists {
		if err = s.checkValueCount(insertValueCount, len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}

		rows[i], err = s.fillRowData(ctx, t, cols, list, defaultValMap)
		if err != nil {
			return nil, errors.Trace(err)
		}

		lastInsertIds[i] = variable.GetSessionVars(ctx).LastInsertID
	}

	if len(s.OnDuplicate) > 0 {
		err = s.prefetchIndices(ctx, t, rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i, row := range rows {
		// Notes: incompatible with mysql
		// MySQL will set last insert id to the first row, as follows:
		// `t(id int AUTO_INCREMENT, c1 int, PRIMARY KEY (id))`
		// `insert t (c1) values(1),(2),(3);`
		// Last insert id will be 1, not 3.
		variable.GetSessionVars(ctx).SetLastInsertID(lastInsertIds[i])
		if len(s.OnDuplicate) == 0 {
			txn.SetOption(kv.PresumeKeyNotExists, nil)
		}
		h, err := t.AddRecord(ctx, row)
		txn.DelOption(kv.PresumeKeyNotExists)
		if err == nil {
			continue
		}

		if len(s.OnDuplicate) == 0 || !terror.ErrorEqual(err, kv.ErrKeyExists) {
			return nil, errors.Trace(err)
		}

		if err = execOnDuplicateUpdate(ctx, t, row, h, toUpdateColumns); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return nil, nil
}

// prefetchIndices uses a BatchPrefetch to load all unique indices that very likely
// will be checked later. The fetched data will be stored in kv cache.
func (s *InsertIntoStmt) prefetchIndices(ctx context.Context, t table.Table, rows [][]interface{}) error {
	var keys []kv.Key
	for _, index := range t.Indices() {
		if !index.Unique {
			continue
		}
		for _, row := range rows {
			colVals, err := index.FetchValues(row)
			if err != nil {
				return errors.Trace(err)
			}
			key, distinct, err := index.X.GenIndexKey(colVals, 0)
			if err != nil {
				return errors.Trace(err)
			}
			if distinct {
				keys = append(keys, key)
			}
		}
	}
	// Only activate if we got more than 1 distinct index.
	if len(keys) <= 1 {
		return nil
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}

	err = txn.BatchPrefetch(keys)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *InsertValues) checkValueCount(insertValueCount, valueCount, num int, cols []*column.Col) error {
	if insertValueCount != valueCount {
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		// So the value count must be same for all insert list.
		return errors.Errorf("Column count doesn't match value count at row %d", num+1)
	}
	if valueCount == 0 && len(s.ColNames) > 0 {
		// "insert into t (c1) values ()" is not valid.
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", s.TableIdent, len(s.ColNames), 0)
	} else if valueCount > 0 && valueCount != len(cols) {
		return errors.Errorf("INSERT INTO %s: expected %d value(s), have %d", s.TableIdent, len(cols), valueCount)
	}

	return nil
}

func (s *InsertValues) fillRowData(ctx context.Context, t table.Table, cols []*column.Col, list []expression.Expression, evalMap map[interface{}]interface{}) ([]interface{}, error) {
	row := make([]interface{}, len(t.Cols()))
	marked := make(map[int]struct{}, len(list))
	for i, expr := range list {
		// For "insert into t values (default)" Default Eval.
		evalMap[expression.ExprEvalDefaultName] = cols[i].Name.O

		val, err := expr.Eval(ctx, evalMap)
		if err != nil {
			return nil, errors.Trace(err)
		}

		offset := cols[i].Offset
		row[offset] = val
		marked[offset] = struct{}{}
	}

	// Clear last insert id.
	variable.GetSessionVars(ctx).SetLastInsertID(0)

	err := s.initDefaultValues(ctx, t, row, marked)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = column.CastValues(ctx, row, cols); err != nil {
		return nil, errors.Trace(err)
	}

	if err = column.CheckNotNull(t.Cols(), row); err != nil {
		return nil, errors.Trace(err)
	}

	return row, nil
}

func execOnDuplicateUpdate(ctx context.Context, t table.Table, row []interface{}, h int64, cols map[int]*expression.Assignment) error {
	// On duplicate key update the duplicate row.
	// Evaluate the updated value.
	// TODO: report rows affected and last insert id.
	data, err := t.Row(ctx, h)
	if err != nil {
		return errors.Trace(err)
	}

	toUpdateArgs := map[interface{}]interface{}{}
	toUpdateArgs[expression.ExprEvalValuesFunc] = func(name string) (interface{}, error) {
		c, err1 := findColumnByName(t, name)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		return row[c.Offset], nil
	}

	if err = updateRecord(ctx, h, data, t, cols, toUpdateArgs, 0, true); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func getOnDuplicateUpdateColumns(assignList []*expression.Assignment, t table.Table) (map[int]*expression.Assignment, error) {
	m := make(map[int]*expression.Assignment, len(assignList))

	for _, v := range assignList {
		c, err := findColumnByName(t, field.JoinQualifiedName("", v.TableName, v.ColName))
		if err != nil {
			return nil, errors.Trace(err)
		}
		m[c.Offset] = v
	}
	return m, nil
}

func (s *InsertValues) initDefaultValues(ctx context.Context, t table.Table, row []interface{}, marked map[int]struct{}) error {
	var err error
	var defaultValueCols []*column.Col
	for i, c := range t.Cols() {
		if row[i] != nil {
			// Column value is not nil, continue.
			continue
		}

		// If the nil value is evaluated in insert list, we will use nil except auto increment column.
		if _, ok := marked[i]; ok && !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
			continue
		}

		if mysql.HasAutoIncrementFlag(c.Flag) {
			var id int64
			if id, err = t.AllocAutoID(); err != nil {
				return errors.Trace(err)
			}
			row[i] = id
			variable.GetSessionVars(ctx).SetLastInsertID(uint64(id))
		} else {
			var value interface{}
			value, _, err = tables.GetColDefaultValue(ctx, &c.ColumnInfo)
			if err != nil {
				return errors.Trace(err)
			}

			row[i] = value
		}

		defaultValueCols = append(defaultValueCols, c)
	}

	if err = column.CastValues(ctx, row, defaultValueCols); err != nil {
		return errors.Trace(err)
	}

	return nil
}
