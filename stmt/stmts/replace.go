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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
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
	Setlist    []*expression.Assignment
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
		Sel: s.Sel, Setlist: s.Setlist, TableIdent: s.TableIdent, Text: s.Text}
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
	if err = stmt.getSetlist(); err != nil {
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
		row, err := stmt.getRow(ctx, t, cols, list, m)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if err = removeExistRow(ctx, t, row); err != nil {
			return nil, errors.Trace(err)
		}
		if _, err = t.AddRecord(ctx, row); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return nil, nil
}

func removeExistRow(ctx context.Context, t table.Table, replaceRow []interface{}) error {
	indices := make([]*column.IndexedCol, 0, len(t.Indices()))
	for _, idx := range t.Indices() {
		// TODO: handles the idx is composite primary key the affected rows.
		if idx.Unique || idx.Primary && len(idx.Columns) >= 1 {
			indices = append(indices, idx)
		}
	}
	if len(indices) == 0 {
		return nil
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	it, err := txn.Seek([]byte(t.FirstKey()))
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	prefix := t.KeyPrefix()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		handle, err0 := util.DecodeHandleFromRowKey(it.Key())
		if err0 != nil {
			return errors.Trace(err0)
		}
		row, err0 := t.Row(ctx, handle)
		if err0 != nil {
			return errors.Trace(err0)
		}
		for _, idx := range indices {
			v, err1 := compareIndex(t.Cols(), idx, row, replaceRow)
			if err1 != nil {
				return errors.Trace(err1)
			}
			if v != 0 {
				continue
			}
			if err = removeRow(ctx, t, handle, row); err != nil {
				return errors.Trace(err)
			}
		}

		rk := t.RecordKey(handle, nil)
		if it, err0 = kv.NextUntil(it, util.RowKeyPrefixFilter(rk)); err0 != nil {
			return errors.Trace(err0)
		}
	}

	return nil
}

// compareIndex returns an integer comparing the idx old with new.
// old > new -> 1
// old = new -> 0
// old < new -> -1
func compareIndex(cols []*column.Col, idx *column.IndexedCol, row, replaceRow []interface{}) (v int, err error) {
	nulls := 0
	for _, idxCol := range idx.Columns {
		col := column.FindCol(cols, idxCol.Name.L)
		if col == nil {
			return 0, errors.Errorf("No such column: %v", idx)
		}
		v, err = types.Compare(row[col.Offset], replaceRow[col.Offset])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if v != 0 {
			break
		}
		if row[col.Offset] == nil {
			nulls++
		}
	}
	if nulls == len(idx.Columns) {
		v = -1
	}

	return v, nil
}
