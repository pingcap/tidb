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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/format"
)

// ReplaceIntoStmt is a statement works exactly like insert, except that if
// an old row in the table has the same value as a new row for a PRIMARY KEY or a UNIQUE index,
// the old row is deleted before the new row is inserted.
// See: https://dev.mysql.com/doc/refman/5.7/en/replace.html
type ReplaceIntoStmt struct {
	ColNames   []string
	Lists      [][]expression.Expression
	Sel        plan.Planner
	TableIdent table.Ident
	SetList    []*expression.Assignment
	Priority   int

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *ReplaceIntoStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *ReplaceIntoStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *ReplaceIntoStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *ReplaceIntoStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *ReplaceIntoStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	stmt := &InsertIntoStmt{ColNames: s.ColNames, Lists: s.Lists, Priority: s.Priority,
		Sel: s.Sel, Setlist: s.SetList, TableIdent: s.TableIdent, Text: s.Text}
	t, err := getTable(ctx, stmt.TableIdent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cols, err := stmt.getColumns(t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Process `replace ... (select ...)`
	if stmt.Sel != nil {
		return stmt.execSelect(t, cols, ctx)
	}
	// Process `replace ... set x=y ...`
	if err = stmt.getSetList(); err != nil {
		return nil, errors.Trace(err)
	}
	m, err := stmt.getDefaultValues(ctx, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	replaceValueCount := len(stmt.Lists[0])

	for i, list := range stmt.Lists {
		if err = stmt.checkValueCount(replaceValueCount, len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		// TODO: remove that has the same value as a new row for a PRIMARY KEY or a UNIQUE index.
		if _, _, err = stmt.addRecord(ctx, t, cols, list, m); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return nil, nil
}
