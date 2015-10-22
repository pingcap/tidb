// Copyright 2014 The ql Authors. All rights reserved.
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

package rsets

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*OrderByRset)(nil)
)

// OrderByItem is order by item.
type OrderByItem struct {
	Expr expression.Expression
	Asc  bool
}

func (i OrderByItem) String() string {
	if i.Asc {
		return fmt.Sprintf("%s ASC", i.Expr)
	}

	return fmt.Sprintf("%s DESC", i.Expr)
}

// OrderByRset is record set for order by fields.
type OrderByRset struct {
	By         []OrderByItem
	Src        plan.Plan
	SelectList *plans.SelectList
}

func (r *OrderByRset) String() string {
	if r == nil || r.By == nil {
		return ""
	}
	a := make([]string, len(r.By))
	for i, v := range r.By {
		a[i] = v.String()
	}
	return strings.Join(a, ", ")
}

// CheckAggregate will check whether order by has aggregate function or not,
// if has, we will add it to select list hidden field.
func (r *OrderByRset) CheckAggregate(selectList *plans.SelectList) error {
	for i, v := range r.By {
		if expression.ContainAggregateFunc(v.Expr) {
			expr, err := selectList.UpdateAggFields(v.Expr)
			if err != nil {
				return errors.Errorf("%s in 'order clause'", err.Error())
			}

			r.By[i].Expr = expr
		}
	}
	return nil
}

type orderByVisitor struct {
	expression.BaseVisitor
	selectList *plans.SelectList
	rootIdent  *expression.Ident
}

func (v *orderByVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	// Order by ambiguous rule:
	//	select c1 as a, c2 as a from t order by a is ambiguous
	//	select c1 as a, c2 as a from t order by a + 1 is ambiguous
	//	select c1 as c2, c2 from t order by c2 is ambiguous
	//	select c1 as c2, c2 from t order by c2 + 1 is not ambiguous

	// Order by identifier reference check
	//	select c1 as c2 from t order by c2, c2 in order by references c1 in select field.
	//	select c1 as c2 from t order by c2 + 1, c2 in order by c2 + 1 references t.c2 in from table.
	//	select c1 as c2 from t order by sum(c2), c2 in order by sum(c2) references t.c2 in from table.
	//	select c1 as c2 from t order by sum(1) + c2, c2 in order by sum(1) + c2 references t.c2 in from table.
	//	select c1 as a from t order by a, a references c1 in select field.
	//	select c1 as a from t order by sum(a), a in order by sum(a) references c1 in select field.

	// TODO: unify VisitIdent for group by and order by visitor, they are little different.

	var (
		index int
		err   error
	)

	if v.rootIdent == i {
		// The order by is an identifier, we must check it first.
		index, err = checkIdentAmbiguous(i, v.selectList, OrderByClause)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if index >= 0 {
			// identifier references a select field. use it directly.
			// e,g. select c1 as c2 from t order by c2, here c2 references c1.
			i.ReferScope = expression.IdentReferSelectList
			i.ReferIndex = index
			return i, nil
		}
	}

	// find this identifier in FROM.
	idx := field.GetResultFieldIndex(i.L, v.selectList.FromFields)
	if len(idx) > 0 {
		i.ReferScope = expression.IdentReferFromTable
		i.ReferIndex = idx[0]
		return i, nil
	}

	if v.rootIdent != i {
		// This identifier is the part of the order by, check ambiguous here.
		index, err = checkIdentAmbiguous(i, v.selectList, OrderByClause)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// try to find in select list, we have got index using checkIdentAmbiguous before.
	if index >= 0 {
		// find in select list
		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = index
		return i, nil
	}

	// TODO: check in out query
	return i, errors.Errorf("Unknown column '%s' in 'order clause'", i)
}

func (v *orderByVisitor) VisitPosition(p *expression.Position) (expression.Expression, error) {
	n := p.N
	if p.N <= v.selectList.HiddenFieldOffset {
		// this position expression is not in hidden field, no need to check
		return p, nil
	}

	// we have added order by to hidden field before, now visit it here.
	expr := v.selectList.Fields[n-1].Expr
	e, err := expr.Accept(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.selectList.Fields[n-1].Expr = e

	return p, nil
}

// Plan get SrcPlan/OrderByDefaultPlan.
// If src is NullPlan or order by fields are empty, then gets SrcPlan.
// Default gets OrderByDefaultPlan.
func (r *OrderByRset) Plan(ctx context.Context) (plan.Plan, error) {
	if _, ok := r.Src.(*plans.NullPlan); ok {
		return r.Src, nil
	}

	var (
		by   []expression.Expression
		ascs []bool
	)

	visitor := &orderByVisitor{}
	visitor.BaseVisitor.V = visitor
	visitor.selectList = r.SelectList

	for i := range r.By {
		e := r.By[i].Expr

		pos, err := castPosition(e, r.SelectList, OrderByClause)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if pos != nil {
			// use Position expression for the associated field.
			r.By[i].Expr = pos
		} else {
			visitor.rootIdent = castIdent(e)
			e, err = e.Accept(visitor)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.By[i].Expr = e
		}

		by = append(by, r.By[i].Expr)
		ascs = append(ascs, r.By[i].Asc)
	}

	// if no order by key, use src plan instead.
	if len(by) == 0 {
		return r.Src, nil
	}

	return &plans.OrderByDefaultPlan{By: by, Ascs: ascs, Src: r.Src,
		SelectList: r.SelectList}, nil
}
