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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/autocommit"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*SelectStmt)(nil)

// SelectStmt is a statement to retrieve rows selected from one or more tables.
// See: https://dev.mysql.com/doc/refman/5.7/en/select.html
type SelectStmt struct {
	Distinct bool
	Fields   []*field.Field
	From     *rsets.JoinRset
	GroupBy  *rsets.GroupByRset
	Having   *rsets.HavingRset
	Limit    *rsets.LimitRset
	Offset   *rsets.OffsetRset
	OrderBy  *rsets.OrderByRset
	Where    *rsets.WhereRset
	// TODO: rename Lock
	Lock coldef.LockType

	selectList *plans.SelectList

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *SelectStmt) Explain(ctx context.Context, w format.Formatter) {
	p, err := s.Plan(ctx)
	if err != nil {
		w.Format("ERROR: %v\n", err)
		return
	}

	p.Explain(w)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *SelectStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *SelectStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *SelectStmt) SetText(text string) {
	s.Text = text
}

func (s *SelectStmt) checkOneColumn(ctx context.Context) error {
	// check select fields
	for _, f := range s.Fields {
		if err := expression.CheckOneColumn(ctx, f.Expr); err != nil {
			return errors.Trace(err)
		}
	}

	// check group by
	if s.GroupBy != nil {
		for _, f := range s.GroupBy.By {
			if err := expression.CheckOneColumn(ctx, f); err != nil {
				return errors.Trace(err)
			}
		}
	}

	// check order by
	if s.OrderBy != nil {
		for _, f := range s.OrderBy.By {
			if err := expression.CheckOneColumn(ctx, f.Expr); err != nil {
				return errors.Trace(err)
			}
		}
	}

	// check having
	if s.Having != nil {
		if err := expression.CheckOneColumn(ctx, s.Having.Expr); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Plan implements the plan.Planner interface.
// The whole phase for select is
// `from -> where -> lock -> group by -> having -> select fields -> distinct -> order by -> limit -> final`
func (s *SelectStmt) Plan(ctx context.Context) (plan.Plan, error) {
	var (
		r   plan.Plan
		err error
	)

	if s.From != nil {
		r, err = s.From.Plan(ctx)
		if err != nil {
			return nil, err
		}
	} else if s.Fields != nil {
		// Only evaluate fields values.
		fr := &rsets.SelectFromDualRset{Fields: s.Fields}
		r, err = fr.Plan(ctx)
		if err != nil {
			return nil, err
		}

	}

	// Put RowStackFromPlan here so that we can catch the origin from data after above FROM phase.
	r = &plans.RowStackFromPlan{Src: r}

	if w := s.Where; w != nil {
		r, err = (&rsets.WhereRset{Expr: w.Expr, Src: r}).Plan(ctx)
		if err != nil {
			return nil, err
		}
	}
	lock := s.Lock
	if lock != coldef.SelectLockNone && autocommit.ShouldAutocommit(ctx) {
		// Locking of rows for update using SELECT FOR UPDATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See: https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
		lock = coldef.SelectLockNone
	}
	r = &plans.SelectLockPlan{Src: r, Lock: lock}

	if err = s.checkOneColumn(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	// Get select list for futher field values evaluation.
	var selectList *plans.SelectList
	if s.selectList == nil {
		selectList, err = plans.ResolveSelectList(s.Fields, r.GetFields())
	} else {
		selectList = s.selectList
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	var groupBy []expression.Expression
	if s.GroupBy != nil {
		groupBy = s.GroupBy.By
	}

	if s.Having != nil && s.selectList == nil {
		// `having` may contain aggregate functions, and we will add this to hidden fields.
		if err = s.Having.CheckAggregate(selectList); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if s.OrderBy != nil && s.selectList == nil {
		// `order by` may contain aggregate functions, and we will add this to hidden fields.
		if err = s.OrderBy.CheckAggregate(selectList); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if s.selectList == nil {
		s.selectList = selectList
	}

	switch {
	case len(selectList.AggFields) == 0 && s.GroupBy == nil:
		// If no group by and no aggregate functions, we will use SelectFieldsPlan.
		if r, err = (&rsets.SelectFieldsRset{Src: r,
			SelectList: selectList,
		}).Plan(ctx); err != nil {
			return nil, err
		}

	default:
		if r, err = (&rsets.GroupByRset{By: groupBy,
			Src:        r,
			SelectList: selectList,
		}).Plan(ctx); err != nil {
			return nil, err
		}
	}

	if s := s.Having; s != nil {
		if r, err = (&rsets.HavingRset{
			Src:        r,
			Expr:       s.Expr,
			SelectList: selectList,
			GroupBy:    groupBy}).Plan(ctx); err != nil {
			return nil, err
		}
	}

	if s.Distinct {
		r = &plans.DistinctDefaultPlan{Src: r, SelectList: selectList}
	}

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
	r = &plans.SelectFinalPlan{Src: r, SelectList: selectList}
	return r, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *SelectStmt) Exec(ctx context.Context) (rs rset.Recordset, err error) {
	log.Info("Exec :", s.OriginText())
	r, err := s.Plan(ctx)
	if err != nil {
		return nil, err
	}

	return rsets.Recordset{Ctx: ctx, Plan: r}, nil
}
