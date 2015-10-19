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
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*UnionStmt)(nil)

// UnionStmt is a statement to combine results from multiple SelectStmts.
// See: https://dev.mysql.com/doc/refman/5.7/en/union.html
type UnionStmt struct {
	Distincts []bool
	Selects   []*SelectStmt
	Limit     *rsets.LimitRset
	Offset    *rsets.OffsetRset
	OrderBy   *rsets.OrderByRset
	Text      string
}

// Explain implements the stmt.Statement Explain interface.
func (s *UnionStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *UnionStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *UnionStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *UnionStmt) SetText(text string) {
	s.Text = text
}

// Plan implements the plan.Planner interface.
func (s *UnionStmt) Plan(ctx context.Context) (plan.Plan, error) {
	srcs := make([]plan.Plan, 0, len(s.Selects))
	columnCount := 0
	for _, s := range s.Selects {
		p, err := s.Plan(ctx)
		if err != nil {
			return nil, err
		}
		if columnCount > 0 && columnCount != len(p.GetFields()) {
			return nil, errors.New("The used SELECT statements have a different number of columns")
		}
		columnCount = len(p.GetFields())

		srcs = append(srcs, p)
	}

	for i := len(s.Distincts) - 1; i >= 0; i-- {
		if s.Distincts[i] {
			// distinct overwrites all previous all
			// e.g, select * from t1 union all select * from t2 union distinct select * from t3.
			// The distinct will overwrite all for t1 and t2.
			i--
			for ; i >= 0; i-- {
				s.Distincts[i] = true
			}
			break
		}
	}

	fields := srcs[0].GetFields()
	selectList := &plans.SelectList{}
	selectList.ResultFields = make([]*field.ResultField, len(fields))
	selectList.HiddenFieldOffset = len(fields)
	selectList.Fields = s.Selects[0].Fields

	// Union uses first select return column names and ignores table name.
	// We only care result name and type here.
	for i, f := range fields {
		nf := f.Clone()
		nf.OrgTableName = ""
		nf.TableName = ""
		selectList.ResultFields[i] = nf
	}

	var (
		r   plan.Plan
		err error
	)

	r = &plans.UnionPlan{Srcs: srcs, Distincts: s.Distincts, RFields: selectList.ResultFields}

	// TODO: check aggregate function later.
	if s := s.OrderBy; s != nil {
		if r, err = (&rsets.OrderByRset{By: s.By,
			Src:        r,
			SelectList: selectList,
		}).Plan(ctx); err != nil {
			return nil, err
		}
	}

	if s := s.Offset; s != nil {
		r = &plans.OffsetDefaultPlan{Count: s.Count, Src: r, Fields: r.GetFields()}
	}
	if s := s.Limit; s != nil {
		r = &plans.LimitDefaultPlan{Count: s.Count, Src: r, Fields: r.GetFields()}
	}

	return r, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *UnionStmt) Exec(ctx context.Context) (rs rset.Recordset, err error) {
	r, err := s.Plan(ctx)
	if err != nil {
		return nil, err
	}
	return rsets.Recordset{Ctx: ctx, Plan: r}, nil
}
