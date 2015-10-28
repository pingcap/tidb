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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*HavingRset)(nil)
)

// HavingRset is record set for having fields.
type HavingRset struct {
	Src        plan.Plan
	Expr       expression.Expression
	SelectList *plans.SelectList
	// Group by clause
	GroupBy []expression.Expression
}

// CheckAggregate will check whether order by has aggregate function or not,
// if has, we will add it to select list hidden field.
func (r *HavingRset) CheckAggregate(selectList *plans.SelectList) error {
	if expression.ContainAggregateFunc(r.Expr) {
		expr, err := selectList.UpdateAggFields(r.Expr)
		if err != nil {
			return errors.Errorf("%s in 'having clause'", err.Error())
		}

		r.Expr = expr
	}

	return nil
}

type havingVisitor struct {
	expression.BaseVisitor
	selectList *plans.SelectList

	// for group by
	groupBy []expression.Expression

	// true means we are visiting aggregate function arguments now.
	inAggregate bool
}

func (v *havingVisitor) visitIdentInAggregate(i *expression.Ident) (expression.Expression, error) {
	// if we are visiting aggregate function arguments, the identifier first checks in from table,
	// then in select list, and outer query finally.

	// find this identifier in FROM.
	idx := field.GetResultFieldIndex(i.L, v.selectList.FromFields)
	if len(idx) > 0 {
		i.ReferScope = expression.IdentReferFromTable
		i.ReferIndex = idx[0]
		return i, nil
	}

	// check in select list.
	index, err := checkIdentAmbiguous(i, v.selectList, HavingClause)
	if err != nil {
		return i, errors.Trace(err)
	}

	if index >= 0 {
		// find in select list
		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = index
		return i, nil
	}

	// TODO: check in out query
	// TODO: return unknown field error, but now just return directly.
	// Because this may reference outer query.
	return i, nil
}

func (v *havingVisitor) checkIdentInGroupBy(i *expression.Ident) (*expression.Ident, bool, error) {
	for _, by := range v.groupBy {
		e := castIdent(by)
		if e == nil {
			// group by must be a identifier too
			continue
		}

		if !field.CheckFieldsEqual(e.L, i.L) {
			// not same, continue
			continue
		}

		// if group by references select list or having is qualified identifier,
		// no other check.
		if e.ReferScope == expression.IdentReferSelectList || field.IsQualifiedName(i.L) {
			i.ReferScope = e.ReferScope
			i.ReferIndex = e.ReferIndex
			return i, true, nil
		}

		// having is unqualified name, e.g, select * from t1, t2 group by t1.c having c.
		// both t1 and t2 have column c, we must check ambiguous here.
		idx := field.GetResultFieldIndex(i.L, v.selectList.FromFields)
		if len(idx) > 1 {
			return i, false, errors.Errorf("Column '%s' in having clause is ambiguous", i)
		}

		i.ReferScope = e.ReferScope
		i.ReferIndex = e.ReferIndex
		return i, true, nil
	}

	return i, false, nil
}

func (v *havingVisitor) checkIdentInSelectList(i *expression.Ident) (*expression.Ident, bool, error) {
	index, err := checkIdentAmbiguous(i, v.selectList, HavingClause)
	if err != nil {
		return i, false, errors.Trace(err)
	}

	if index >= 0 {
		// identifier references a select field. use it directly.
		// e,g. select c1 as c2 from t having c2, here c2 references c1.
		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = index
		return i, true, nil
	}

	lastIndex := -1
	var lastFieldName string
	// we may meet this select c1 as c2 from t having c1, so we must check origin field name too.
	for index := 0; index < v.selectList.HiddenFieldOffset; index++ {
		e := castIdent(v.selectList.Fields[index].Expr)
		if e == nil {
			// not identifier
			continue
		}

		if !field.CheckFieldsEqual(e.L, i.L) {
			// not same, continue
			continue
		}

		if field.IsQualifiedName(i.L) {
			// qualified name, no need check
			i.ReferScope = expression.IdentReferSelectList
			i.ReferIndex = index
			return i, true, nil
		}

		if lastIndex == -1 {
			lastIndex = index
			lastFieldName = e.L
			continue
		}

		// we may meet select t1.c as a, t2.c as b from t1, t2 having c, must check ambiguous here
		if !field.CheckFieldsEqual(lastFieldName, e.L) {
			return i, false, errors.Errorf("Column '%s' in having clause is ambiguous", i)
		}

		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = index
		return i, true, nil
	}

	if lastIndex != -1 {
		i.ReferScope = expression.IdentReferSelectList
		i.ReferIndex = lastIndex
		return i, true, nil
	}

	return i, false, nil
}

func (v *havingVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	// Having has the most complex rule for ambiguous and identifer reference check.

	// Having ambiguous rule:
	//	select c1 as a, c2 as a from t having a is ambiguous
	//	select c1 as a, c2 as a from t having a + 1 is ambiguous
	//	select c1 as c2, c2 from t having c2 is ambiguous
	//	select c1 as c2, c2 from t having c2 + 1 is ambiguous

	// The identifier in having must exist in group by, select list or outer query, so select c1 from t having c2 is wrong,
	// the identifier will first try to find the reference in group by, then in select list and finally in outer query.

	// Having identifier reference check
	//	select c1 from t group by c2 having c2, c2 in having references group by c2.
	//	select c1 as c2 from t having c2, c2 in having references c1 in select field.
	//	select c1 as c2 from t group by c2 having c2, c2 in having references group by c2.
	// 	select c1 as c2 from t having sum(c2), c2 in having sum(c2) references t.c2 in from table.
	//	select c1 as c2 from t having sum(c2) + c2, c2 in having sum(c2) references t.c2 in from table, but another c2 references c1 in select list.
	//	select c1 as c2 from t group by c2 having sum(c2) + c2, c2 in having sum(c2) references t.c2 in from table, another c2 references c2 in group by.
	//	select c1 as a from t having a, a references c1 in select field.
	//	select c1 as a from t having sum(a), a in order by sum(a) references c1 in select field.
	//	select c1 as c2 from t having c1, c1 in having references c1 in select list.

	// TODO: unify VisitIdent for group by and order by visitor, they are little different.

	if v.inAggregate {
		return v.visitIdentInAggregate(i)
	}

	var (
		err error
		ok  bool
	)

	// first, check in group by
	i, ok, err = v.checkIdentInGroupBy(i)
	if err != nil || ok {
		return i, errors.Trace(err)
	}

	// then in select list
	i, ok, err = v.checkIdentInSelectList(i)
	if err != nil || ok {
		return i, errors.Trace(err)
	}

	// TODO: check in out query

	return i, errors.Errorf("Unknown column '%s' in 'having clause'", i)
}

func (v *havingVisitor) VisitPosition(p *expression.Position) (expression.Expression, error) {
	n := p.N

	// we have added order by to hidden field before, now visit it here.
	expr := v.selectList.Fields[n-1].Expr
	e, err := expr.Accept(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v.selectList.Fields[n-1].Expr = e

	return p, nil
}

func (v *havingVisitor) VisitCall(c *expression.Call) (expression.Expression, error) {
	isAggregate := expression.IsAggregateFunc(c.F)

	if v.inAggregate && isAggregate {
		// aggregate function can't contain aggregate function
		return nil, errors.Errorf("Invalid use of group function")
	}

	if isAggregate {
		// set true to let outer know we are in aggregate function.
		v.inAggregate = true
	}

	var err error
	for i, e := range c.Args {
		c.Args[i], err = e.Accept(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if isAggregate {
		v.inAggregate = false
	}

	return c, nil
}

// Plan gets HavingPlan.
func (r *HavingRset) Plan(ctx context.Context) (plan.Plan, error) {
	visitor := &havingVisitor{
		selectList:  r.SelectList,
		groupBy:     r.GroupBy,
		inAggregate: false,
	}
	visitor.BaseVisitor.V = visitor

	e, err := r.Expr.Accept(visitor)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.Expr = e

	return &plans.HavingPlan{Src: r.Src, Expr: r.Expr, SelectList: r.SelectList}, nil
}

//  String implements fmt.Stringer interface.
func (r *HavingRset) String() string {
	return r.Expr.String()
}
