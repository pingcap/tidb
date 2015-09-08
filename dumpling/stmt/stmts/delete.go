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
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*DeleteStmt)(nil) // TODO optimizer plan

// DeleteStmt is a statement to delete rows from table.
// See: https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	TableIdent  table.Ident
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

func (s *DeleteStmt) indexPlan(ctx context.Context) (plan.Plan, error) {
	if s.MultiTable {
		return s.planMultiTable(ctx)
	}
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

func (s *DeleteStmt) planMultiTable(ctx context.Context) (plan.Plan, error) {
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

func (s *DeleteStmt) hitWhere(ctx context.Context, t table.Table, data []interface{}) (bool, error) {
	if s.Where == nil {
		return true, nil
	}
	m := map[interface{}]interface{}{}

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

func (s *DeleteStmt) removeRow(ctx context.Context, t table.Table, h int64, data []interface{}) error {
	// remove row's all indexies
	if err := t.RemoveRowAllIndex(ctx, h, data); err != nil {
		return err
	}
	// remove row
	if err := t.RemoveRow(ctx, h); err != nil {
		return err
	}
	variable.GetSessionVars(ctx).AddAffectedRows(1)
	return nil
}

func (s *DeleteStmt) tryDeleteUsingIndex(ctx context.Context, t table.Table) (bool, error) {
	log.Info("try delete with index", ctx)
	p, err := s.indexPlan(ctx)
	if err != nil {
		return false, err
	}
	if p != nil {
		var ids []int64
		err := p.Do(ctx, func(id interface{}, _ []interface{}) (bool, error) {
			// Generate ids for coming deletion.
			ids = append(ids, id.(int64))
			return true, nil
		})
		if err != nil {
			return false, err
		}
		var cnt uint64
		for _, id := range ids {
			if s.Limit != nil && cnt >= s.Limit.Count {
				break
			}
			data, err := t.Row(ctx, id)
			if err != nil {
				return false, err
			}
			log.Infof("try delete with index id:%d, ctx:%s", id, ctx)
			ok, err := s.hitWhere(ctx, t, data)
			if err != nil {
				return false, errors.Trace(err)
			}
			if !ok {
				return true, nil
			}
			err = s.removeRow(ctx, t, id, data)
			if err != nil {
				return false, err
			}
			cnt++
		}
		return true, nil
	}
	return false, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *DeleteStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	if s.MultiTable {
		return s.execMultiTable(ctx)
	}
	t, err := getTable(ctx, s.TableIdent)
	if err != nil {
		return nil, err
	}

	ok, err := s.tryDeleteUsingIndex(ctx, t)
	if err != nil {
		return nil, err
	}
	if ok {
		// Delete using index OK.
		log.Info("try delete with index OK")
		return nil, nil
	}

	var cnt uint64
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
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
		err = s.removeRow(ctx, t, h, data)
		if err != nil {
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

func (s *DeleteStmt) execMultiTable(ctx context.Context) (_ rset.Recordset, err error) {
	log.Info("Delete from multi-table")
	if len(s.TableIdents) == 0 {
		return nil, nil
	}
	p, err := s.indexPlan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if p == nil {
		return nil, nil
	}
	tblIDMap := make(map[int64]bool, len(s.TableIdents))
	// Get table alias map.
	fs := p.GetFields()
	tblAliasMap := make(map[string]string)
	for _, f := range fs {
		if f.TableName != f.OrgTableName {
			tblAliasMap[f.TableName] = f.OrgTableName
		}
	}

	for _, t := range s.TableIdents {
		// Consider DBName.
		oname, ok := tblAliasMap[t.Name.O]
		if ok {
			t.Name.O = oname
			t.Name.L = strings.ToLower(oname)
		}

		var tbl table.Table
		tbl, err = getTable(ctx, t)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblIDMap[tbl.TableID()] = true
	}
	rowKeyMap := make(map[string]table.Table)
	err = p.Do(ctx, func(_ interface{}, in []interface{}) (bool, error) {
		// Generate ids for coming deletion.
		var rowKeys *plans.RowKeyList
		if in != nil && len(in) > 0 {
			t := in[len(in)-1]
			switch vt := t.(type) {
			case *plans.RowKeyList:
				rowKeys = vt
			}
		}
		if rowKeys != nil {
			for _, entry := range rowKeys.Keys {
				tid := entry.Tbl.TableID()
				if _, ok := tblIDMap[tid]; !ok {
					continue
				}
				rowKeyMap[entry.Key] = entry.Tbl
			}
		}
		return true, nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	for k, t := range rowKeyMap {
		id, err := util.DecodeHandleFromRowKey(k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		data, err := t.Row(ctx, id)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = s.removeRow(ctx, t, id, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return nil, nil
}
