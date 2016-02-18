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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*DeleteStmt)(nil) // TODO optimizer plan

// DeleteStmt is a statement to delete rows from table.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	Where       expression.Expression
	Order       *rsets.OrderByRset
	Limit       *rsets.LimitRset
	LowPriority bool
	Ignore      bool
	Quick       bool
	MultiTable  bool
	BeforeFrom  bool
	TableIdents []table.Ident
	Refs        *rsets.JoinRset

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DeleteStmt) Explain(ctx context.Context, w format.Formatter) {
	p, err := s.plan(ctx)
	if err != nil {
		log.Error(err)
		return
	}
	p.Explain(w)
	w.Format("└Delete row\n")
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DeleteStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DeleteStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DeleteStmt) SetText(text string) {
	s.Text = text
}

func (s *DeleteStmt) plan(ctx context.Context) (plan.Plan, error) {
	var (
		r   plan.Plan
		err error
	)
	if s.Refs != nil {
		r, err = s.Refs.Plan(ctx)
		if err != nil {
			return nil, err
		}
	}
	if s.Where != nil {
		r, err = (&rsets.WhereRset{Expr: s.Where, Src: r}).Plan(ctx)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func removeRow(ctx context.Context, t table.Table, h int64, data []interface{}) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return errors.Trace(err)
	}

	variable.GetSessionVars(ctx).AddAffectedRows(1)
	return nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *DeleteStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	if s.MultiTable && len(s.TableIdents) == 0 {
		return nil, nil
	}
	p, err := s.plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if p == nil {
		return nil, nil
	}
	defer p.Close()
	tblIDMap := make(map[int64]bool, len(s.TableIdents))
	// Get table alias map.
	tblNames := make(map[string]string)
	if s.MultiTable {
		// Delete from multiple tables should consider table ident list.
		fs := p.GetFields()
		for _, f := range fs {
			if f.TableName != f.OrgTableName {
				tblNames[f.TableName] = f.OrgTableName
			} else {
				tblNames[f.TableName] = f.TableName
			}
		}
		for _, t := range s.TableIdents {
			// Consider DBName.
			oname, ok := tblNames[t.Name.O]
			if !ok {
				return nil, errors.Errorf("Unknown table '%s' in MULTI DELETE", t.Name.O)
			}

			t.Name.O = oname
			t.Name.L = strings.ToLower(oname)

			var tbl table.Table
			tbl, err = getTable(ctx, t)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tblIDMap[tbl.Meta().ID] = true
		}
	}
	rowKeyMap := make(map[string]table.Table)
	for {
		row, err1 := p.Next(ctx)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if row == nil {
			break
		}

		for _, entry := range row.RowKeys {
			if s.MultiTable {
				tid := entry.Tbl.Meta().ID
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
		data, err := t.Row(ctx, handle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = removeRow(ctx, t, handle, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}
