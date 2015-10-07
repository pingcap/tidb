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
	"github.com/pingcap/tidb/context"
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
	for _, s := range s.Selects {
		p, err := s.Plan(ctx)
		if err != nil {
			return nil, err
		}
		srcs = append(srcs, p)
	}
	return &plans.UnionPlan{Srcs: srcs, Distincts: s.Distincts}, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *UnionStmt) Exec(ctx context.Context) (rs rset.Recordset, err error) {
	r, err := s.Plan(ctx)
	if err != nil {
		return nil, err
	}
	return rsets.Recordset{Ctx: ctx, Plan: r}, nil
}
