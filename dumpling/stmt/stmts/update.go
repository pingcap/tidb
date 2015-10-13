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
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ stmt.Statement = (*UpdateStmt)(nil)

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See: https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	TableRefs     *rsets.JoinRset
	List          []expression.Assignment
	Where         expression.Expression
	Order         *rsets.OrderByRset
	Limit         *rsets.LimitRset
	LowPriority   bool
	Ignore        bool
	MultipleTable bool

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *UpdateStmt) Explain(ctx context.Context, w format.Formatter) {
	p, err := s.plan(ctx)
	if err != nil {
		log.Error(err)
		return
	}
	if p != nil {
		p.Explain(w)
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

func checkUpdateColumns(assignList []expression.Assignment, fields []*field.ResultField, t table.Table) (map[int]expression.Assignment, error) {
	m := make(map[int]expression.Assignment, len(assignList))

	for _, v := range assignList {
		if fields != nil {
			name := v.ColName
			if len(v.TableName) > 0 {
				name = fmt.Sprintf("%s.%s", v.TableName, v.ColName)
			}
			// use result fields to check assign list, otherwise use origin table columns
			idx := field.GetResultFieldIndex(name, fields, field.DefaultFieldFlag)
			if n := len(idx); n > 1 {
				return nil, errors.Errorf("ambiguous field %s", name)
			} else if n == 0 {
				return nil, errors.Errorf("unknown field %s", name)
			}

			m[idx[0]] = v
		} else {
			c, err := findColumnByName(t, field.JoinQualifiedName("", v.TableName, v.ColName))
			if err != nil {
				return nil, errors.Trace(err)
			}
			m[c.Offset] = v
		}
	}

	return m, nil
}

func updateRecord(ctx context.Context, h int64, data []interface{}, t table.Table,
	updateColumns map[int]expression.Assignment, m map[interface{}]interface{},
	offset int, onDuplicateUpdate bool) error {
	if err := t.LockRow(ctx, h, true); err != nil {
		return errors.Trace(err)
	}

	oldData := make([]interface{}, len(t.Cols()))
	touched := make([]bool, len(t.Cols()))
	copy(oldData, data)

	cols := t.Cols()

	assignExists := false
	for i, asgn := range updateColumns {
		if i < offset || i >= offset+len(cols) {
			// The assign expression is for another table, not this.
			continue
		}
		val, err := asgn.Expr.Eval(ctx, m)
		if err != nil {
			return err
		}
		colIndex := i - offset
		touched[colIndex] = true
		data[colIndex] = val
		assignExists = true
	}

	// no assign list for this table, no need to update.
	if !assignExists {
		return nil
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
		n, err := types.Compare(d, od)
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

	// Update record to new value and update index.
	err := t.UpdateRecord(ctx, h, oldData, data, touched)
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

func (s *UpdateStmt) plan(ctx context.Context) (plan.Plan, error) {
	var (
		r   plan.Plan
		err error
	)
	if s.TableRefs != nil {
		r, err = s.TableRefs.Plan(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Where != nil {
		r, err = (&rsets.WhereRset{Expr: s.Where, Src: r}).Plan(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Order != nil {
		s.Order.Src = r
		r, err = s.Order.Plan(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.Limit != nil {
		s.Limit.Src = r
		r, err = s.Limit.Plan(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return r, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *UpdateStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	p, err := s.plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer p.Close()
	updatedRowKeys := make(map[string]bool)

	// Get table alias map.
	fs := p.GetFields()

	columns, err0 := checkUpdateColumns(s.List, fs, nil)
	if err0 != nil {
		return nil, errors.Trace(err0)
	}

	m := map[interface{}]interface{}{}
	for {
		row, err1 := p.Next(ctx)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if row == nil {
			break
		}
		rowData := row.Data
		if len(row.RowKeys) == 0 {
			// Nothing to update
			return nil, nil
		}
		// Set EvalIdentFunc
		m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return plans.GetIdentValue(name, p.GetFields(), rowData, field.DefaultFieldFlag)
		}

		// Update rows
		offset := 0
		for _, entry := range row.RowKeys {
			tbl := entry.Tbl
			k := entry.Key
			lastOffset := offset
			offset += len(tbl.Cols())
			data := rowData[lastOffset:offset]

			_, ok := updatedRowKeys[k]
			if ok {
				// Each matching row is updated once, even if it matches the conditions multiple times.
				continue
			}
			// Update row
			handle, err2 := util.DecodeHandleFromRowKey(k)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}

			err2 = updateRecord(ctx, handle, data, tbl, columns, m, lastOffset, false)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}

			updatedRowKeys[k] = true
		}
	}
	return nil, nil
}
