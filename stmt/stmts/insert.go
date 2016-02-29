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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/format"
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
		if value, ok, err := table.GetColDefaultValue(ctx, &col.ColumnInfo); ok {
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
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	toUpdateColumns, err := getOnDuplicateUpdateColumns(s.OnDuplicate, t)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows [][]interface{}
	if s.Sel != nil {
		rows, err = s.getRowsSelect(ctx, t, cols)
	} else {
		rows, err = s.getRows(ctx, t, cols)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, row := range rows {
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

func (s *InsertValues) getRows(ctx context.Context, t table.Table, cols []*column.Col) (rows [][]interface{}, err error) {
	// process `insert|replace ... set x=y...`
	if err = s.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	evalMap, err := s.getColumnDefaultValues(ctx, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([][]interface{}, len(s.Lists))
	for i, list := range s.Lists {
		if err = s.checkValueCount(len(s.Lists[0]), len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}

		vals := make([]interface{}, len(list))
		for j, expr := range list {
			// For "insert into t values (default)" Default Eval.
			evalMap[expression.ExprEvalDefaultName] = cols[j].Name.O

			vals[j], err = expr.Eval(ctx, evalMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rows[i], err = s.fillRowData(ctx, t, cols, vals)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

func (s *InsertValues) getRowsSelect(ctx context.Context, t table.Table, cols []*column.Col) (rows [][]interface{}, err error) {
	// process `insert|replace into ... select ... from ...`
	r, err := s.Sel.Plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer r.Close()
	if len(r.GetFields()) != len(cols) {
		return nil, errors.Errorf("Column count %d doesn't match value count %d", len(cols), len(r.GetFields()))
	}

	for {
		var planRow *plan.Row
		planRow, err = r.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if planRow == nil {
			break
		}

		var row []interface{}
		row, err = s.fillRowData(ctx, t, cols, planRow.Data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, row)
	}
	return
}

func (s *InsertValues) fillRowData(ctx context.Context, t table.Table, cols []*column.Col, vals []interface{}) ([]interface{}, error) {
	row := make([]interface{}, len(t.Cols()))
	marked := make(map[int]struct{}, len(vals))
	for i, v := range vals {
		offset := cols[i].Offset
		row[offset] = v
		marked[offset] = struct{}{}
	}
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
			recordID, err := t.AllocAutoID()
			if err != nil {
				return errors.Trace(err)
			}
			row[i] = recordID
			if c.IsPKHandleColumn(t.Meta()) {
				// Notes: incompatible with mysql
				// MySQL will set last insert id to the first row, as follows:
				// `t(id int AUTO_INCREMENT, c1 int, PRIMARY KEY (id))`
				// `insert t (c1) values(1),(2),(3);`
				// Last insert id will be 1, not 3.
				variable.GetSessionVars(ctx).SetLastInsertID(uint64(recordID))
			}
		} else {
			var value interface{}
			value, _, err := table.GetColDefaultValue(ctx, &c.ColumnInfo)
			if err != nil {
				return errors.Trace(err)
			}

			row[i] = value
		}

		defaultValueCols = append(defaultValueCols, c)
	}

	if err := column.CastValues(ctx, row, defaultValueCols); err != nil {
		return errors.Trace(err)
	}

	return nil
}
