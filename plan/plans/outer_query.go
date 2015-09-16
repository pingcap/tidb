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

package plans

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

// Phase type for plan.
const (
	FromPhase = iota + 1
	WherePhase
	LockPhase
	GroupByPhase
	HavingPhase
	SelectFieldsPhase
	DistinctPhase
	OrderByPhase
	LimitPhase
	BeforeFinalPhase
	FinalPhase
)

// OuterQueryPlan is for subquery fetching value from outer query.
// e.g, select c1 from t where c1 = (select c2 from t2 where t2.c2 = t.c2).
// You can see that the subquery uses outer table t's column in where condition.
type OuterQueryPlan struct {
	Src plan.Plan

	// SrcPhase is the phase for the src plan.
	SrcPhase int

	HiddenFieldOffset int

	OuterQuery *OuterQuery
}

// Explain implements the plan.Plan Explain interface.
func (p *OuterQueryPlan) Explain(w format.Formatter) {
	p.Src.Explain(w)
}

// GetFields implements the plan.Plan GetFields interface.
func (p *OuterQueryPlan) GetFields() []*field.ResultField {
	return p.Src.GetFields()
}

// Filter implements the plan.Plan Filter interface.
func (p *OuterQueryPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	r, b, err := p.Src.Filter(ctx, expr)
	if !b || err != nil {
		return p, false, errors.Trace(err)
	}

	p.Src = r
	return p, true, nil
}

// Next implements the plan.Plan Next interface.
func (p *OuterQueryPlan) Next(ctx context.Context) (*plan.Row, error) {
	row, err := p.Src.Next(ctx)
	if row == nil || err != nil {
		return nil, errors.Trace(err)
	}

	// Subquery fetches outer table data only after the following phase.
	switch p.SrcPhase {
	case FromPhase:
		// Here row.Data is the whole data from table.
		// We set it to FromData so that following phase can access the table reference.
		row.FromData = row.Data
		row.FromDataFields = p.Src.GetFields()
		p.updateOuterQuery(ctx, row)
	case WherePhase, LockPhase:
		p.updateOuterQuery(ctx, row)
	case GroupByPhase, SelectFieldsPhase, HavingPhase, DistinctPhase, OrderByPhase:
		row.DataFields = p.Src.GetFields()[0:p.HiddenFieldOffset]
		p.updateOuterQuery(ctx, row)
	case BeforeFinalPhase:
		if err := p.popOuterQuery(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return row, nil
}

// Close implements the plan.Plan Close interface.
func (p *OuterQueryPlan) Close() error {
	return p.Src.Close()
}

func (p *OuterQueryPlan) updateOuterQuery(ctx context.Context, row *plan.Row) {
	p.OuterQuery.row = row
	t := getOuterQuery(ctx)
	if t != nil && t != p.OuterQuery {
		p.OuterQuery.last = t
	}

	ctx.SetValue(outerQueryKey, p.OuterQuery)
}

func (p *OuterQueryPlan) popOuterQuery(ctx context.Context) error {
	t := getOuterQuery(ctx)

	if t == nil || t != p.OuterQuery {
		return errors.Errorf("invalid outer query stack")
	}

	if t.last == nil {
		ctx.ClearValue(outerQueryKey)
		return nil
	}

	ctx.SetValue(outerQueryKey, t.last)
	return nil
}

// A dummy type to avoid naming collision in context.
type outerQueryKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k outerQueryKeyType) String() string {
	return "outer query"
}

// outerQueryKey is used to retrive outer table references for sub query.
const outerQueryKey outerQueryKeyType = 0

// OuterQuery saves the outer table references.
// For every select, we will push a OuterQuery to a stack for inner sub query use,
// so the top OuterQuery is for current select.
// e.g, select c1 from t1 where c2 = (select c1 from t2 where t2.c1 = t1.c2 limit 1),
// the "select c1 from t1" is the outer query for the sub query in where phase, we will
// first push a OuterQuery to the stack saving the row data for "select c1 from t1", then
// push the second OuterQuery to the stack saving the row data for "select c1 from t2".
// We will push a OuterQuery after the from phase and pop it before the final phase.
// So we can guarantee that there is at least one OuterQuery certainly.
type OuterQuery struct {
	// outer is the last outer table reference.
	last *OuterQuery
	row  *plan.Row
}

func getOuterQuery(ctx context.Context) *OuterQuery {
	v := ctx.Value(outerQueryKey)
	if v == nil {
		return nil
	}
	// must be OuterQuery
	t := v.(*OuterQuery)
	return t
}

func getIdentValueFromOuterQuery(ctx context.Context, name string) (interface{}, error) {
	t := getOuterQuery(ctx)
	if t == nil {
		return nil, errors.Errorf("unknown field %s", name)
	}

	// The top is current OuterQuery, use its last.
	t = t.last
	for ; t != nil; t = t.last {
		// first try to get from outer table reference.
		v, err := GetIdentValue(name, t.row.FromDataFields, t.row.FromData, field.DefaultFieldFlag)
		if err == nil {
			return v, nil
		}

		// then try to get from outer select list.
		v, err = GetIdentValue(name, t.row.DataFields, t.row.Data, field.FieldNameFlag)
		if err == nil {
			return v, nil
		}
	}

	return nil, errors.Errorf("unknown field %s", name)
}
