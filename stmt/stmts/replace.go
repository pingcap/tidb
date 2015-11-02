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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/errors2"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

// ReplaceIntoStmt is a statement works exactly like insert, except that if
// an old row in the table has the same value as a new row for a PRIMARY KEY or a UNIQUE index,
// the old row is deleted before the new row is inserted.
// See: https://dev.mysql.com/doc/refman/5.7/en/replace.html
type ReplaceIntoStmt struct {
	InsertValues

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
	t, err := getTable(ctx, s.TableIdent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cols, err := s.getColumns(t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Process `replace ... (select ...)`
	// TODO: handles the duplicate-key in a primary key or a unique index.
	if s.Sel != nil {
		return s.execSelect(t, cols, ctx)
	}
	// Process `replace ... set x=y ...`
	if err = s.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}
	m, err := s.getDefaultValues(ctx, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	replaceValueCount := len(s.Lists[0])

	for i, list := range s.Lists {
		if err = s.checkValueCount(replaceValueCount, len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		row, err := s.getRow(ctx, t, cols, list, m)
		if err != nil {
			return nil, errors.Trace(err)
		}
		h, err := t.AddRecord(ctx, row)
		if err == nil {
			continue
		}
		if err != nil && !errors2.ErrorEqual(err, kv.ErrKeyExists) {
			return nil, errors.Trace(err)
		}

		// While the insertion fails because a duplicate-key error occurs for a primary key or unique index,
		// a storage engine may perform the REPLACE as an update rather than a delete plus insert.
		// See: http://dev.mysql.com/doc/refman/5.7/en/replace.html.
		if err = replaceRow(ctx, t, h, row); err != nil {
			return nil, errors.Trace(err)
		}
		variable.GetSessionVars(ctx).AddAffectedRows(1)
	}

	return nil, nil
}

func replaceRow(ctx context.Context, t table.Table, handle int64, replaceRow []interface{}) error {
	row, err := t.Row(ctx, handle)
	if err != nil {
		return errors.Trace(err)
	}

	isReplace := false
	touched := make([]bool, len(row))
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
		variable.GetSessionVars(ctx).AddAffectedRows(1)
		if err = t.UpdateRecord(ctx, handle, row, replaceRow, touched); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
