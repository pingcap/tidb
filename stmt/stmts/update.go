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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ stmt.Statement = (*UpdateStmt)(nil)

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	TableIdent  table.Ident
	List        []expressions.Assignment
	Where       expression.Expression
	Order       *rsets.OrderByRset
	Limit       *rsets.LimitRset
	LowPriority bool
	Ignore      bool

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *UpdateStmt) Explain(ctx context.Context, w format.Formatter) {
	p, err := s.indexPlan(ctx)
	if err != nil {
		log.Error(err)
		return
	}
	if p != nil {
		p.Explain(w)
	} else {
		w.Format("┌Iterate all rows of table: %s\n", s.TableIdent)
	}
	w.Format("└Update fields %v\n", s.List)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *UpdateStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *UpdateStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *UpdateStmt) SetText(text string) {
	s.Text = text
}

func getUpdateColumns(t table.Table, assignList []expressions.Assignment) ([]*column.Col, error) {
	tcols := make([]*column.Col, len(assignList))
	for i, asgn := range assignList {
		col := column.FindCol(t.Cols(), asgn.ColName)
		if col == nil {
			return nil, errors.Errorf("UPDATE: unknown column %s", asgn.ColName)
		}
		tcols[i] = col
	}

	return tcols, nil
}

func (s *UpdateStmt) hitWhere(ctx context.Context, t table.Table, data []interface{}) (bool, error) {
	if s.Where == nil {
		return true, nil
	}
	m := make(map[interface{}]interface{}, len(t.Cols()))

	// Set parameter for evaluating expression.
	for _, col := range t.Cols() {
		m[col.Name.L] = data[col.Offset]
	}

	ok, err := expressions.EvalBoolExpr(ctx, s.Where, m)
	if err != nil {
		return false, errors.Trace(err)
	}
	return ok, nil
}

func getInsertValue(name string, cols []*column.Col, row []interface{}) (interface{}, error) {
	for i, col := range cols {
		if col.Name.L == name {
			return row[i], nil
		}
	}
	return nil, errors.Errorf("unknown field %s", name)
}

func updateRecord(ctx context.Context, h int64, data []interface{}, t table.Table, tcols []*column.Col, assignList []expressions.Assignment, insertData []interface{}) error {
	if err := t.LockRow(ctx, h, true); err != nil {
		return errors.Trace(err)
	}

	oldData := make([]interface{}, len(t.Cols()))
	touched := make([]bool, len(t.Cols()))
	copy(oldData, data)

	// Generate new values
	m := make(map[interface{}]interface{}, len(t.Cols()))

	// Set parameter for evaluating expression.
	for _, col := range t.Cols() {
		m[col.Name.L] = data[col.Offset]
	}

	if insertData != nil {
		m[expressions.ExprEvalValuesFunc] = func(name string) (interface{}, error) {
			return getInsertValue(name, t.Cols(), insertData)
		}
	}

	for i, asgn := range assignList {
		val, err := asgn.Expr.Eval(ctx, m)
		if err != nil {
			return err
		}
		colIndex := tcols[i].Offset
		touched[colIndex] = true
		data[colIndex] = val
	}

	// Check whether new value is valid.
	if err := column.CastValues(ctx, data, t.Cols()); err != nil {
		return err
	}

	if err := column.CheckNotNull(t.Cols(), data); err != nil {
		return err
	}

	// If row is not changed, we should do nothing.
	rowChanged := false
	for i, d := range data {
		if !touched[i] {
			continue
		}
		od := oldData[i]
		if types.Compare(d, od) != 0 {
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

	// Update record to new value and update index.
	err := t.UpdateRecord(ctx, h, oldData, data, touched)
	if err != nil {
		return errors.Trace(err)
	}
	// Record affected rows.
	if len(insertData) == 0 {
		variable.GetSessionVars(ctx).AddAffectedRows(1)
	} else {
		variable.GetSessionVars(ctx).AddAffectedRows(2)

	}
	return nil
}

func (s *UpdateStmt) indexPlan(ctx context.Context) (plan.Plan, error) {
	t, err := getTable(ctx, s.TableIdent)
	if err != nil {
		return nil, err
	}

	p, filtered, err := (&plans.TableDefaultPlan{T: t}).FilterForUpdateAndDelete(ctx, s.Where)
	if err != nil {
		return nil, err
	}
	if !filtered {
		return nil, nil
	}
	return p, nil
}

func (s *UpdateStmt) tryUpdateUsingIndex(ctx context.Context, t table.Table) (bool, error) {
	log.Info("update using index")
	p, err := s.indexPlan(ctx)
	if p == nil {
		return false, err
	}

	var handles []int64
	p.Do(ctx, func(id interface{}, _ []interface{}) (more bool, err error) {
		// Record handle ids for updating.
		handles = append(handles, id.(int64))
		return true, nil
	})

	tcols, err := getUpdateColumns(t, s.List)
	if err != nil {
		return false, err
	}
	var cnt uint64
	for _, id := range handles {
		data, err := t.Row(ctx, id)
		if err != nil {
			return false, err
		}
		log.Info("updating ", id)
		if s.Limit != nil && cnt >= s.Limit.Count {
			break
		}
		err = updateRecord(ctx, id, data, t, tcols, s.List, nil)
		if err != nil {
			return false, err
		}
		cnt++
	}
	return true, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *UpdateStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	t, err := getTable(ctx, s.TableIdent)
	if err != nil {
		return nil, err
	}

	var ok bool
	ok, err = s.tryUpdateUsingIndex(ctx, t)
	if err != nil {
		return nil, err
	}
	if ok {
		// Already updated using index, do nothing.
		return nil, nil
	}

	tcols, err := getUpdateColumns(t, s.List)
	if err != nil {
		return nil, err
	}

	firstKey := t.FirstKey()
	var cnt uint64
	err = t.IterRecords(ctx, firstKey, t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		if s.Limit != nil && cnt >= s.Limit.Count {
			return false, nil
		}

		ok, err = s.hitWhere(ctx, t, data)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !ok {
			return true, nil
		}
		if err := updateRecord(ctx, h, data, t, tcols, s.List, nil); err != nil {
			return false, err
		}
		cnt++
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}
