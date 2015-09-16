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
	if !b {
		return r, false, errors.Trace(err)
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

	// We can let subquery fetching outer table data only after the following phase.
	switch p.SrcPhase {
	case FromPhase:
		// Here row.Data is the whole data from table.
		// We set it to FromData so that following phase can access the table reference.
		row.FromData = row.Data
		row.FromDataFields = p.Src.GetFields()
		pushOuterQuery(ctx, row)
	case WherePhase, LockPhase:
		updateTopOuterQuery(ctx, row)
	case GroupByPhase, SelectFieldsPhase, HavingPhase, DistinctPhase:
		row.DataFields = p.Src.GetFields()[0:p.HiddenFieldOffset]
		updateTopOuterQuery(ctx, row)
	case BeforeFinalPhase:
		popOuterQuery(ctx)
	}

	return row, nil
}

// Close implements the plan.Plan Close interface.
func (p *OuterQueryPlan) Close() error {
	return p.Src.Close()
}

// A dummy type to avoid naming collision in context.
type outerQueryKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k outerQueryKeyType) String() string {
	return "outer query"
}

// outerQueryKey is used to retrive outer table references for sub query.
const outerQueryKey outerQueryKeyType = 0

// outerQuery saves the outer table references.
// For every select, we will push a outerQuery to a stack for inner sub query use,
// and the top outerQuery is for current select.
// e.g, select c1 from t1 where c2 = (select c1 from t2 where t2.c1 = t1.c2 limit 1),
// the "select c1 from t1" is the outer query for the sub query in where phase, we will
// first push a outerQuery to the stack saving the row data for "select c1 from t1", then
// push the second outerQuery to the stack saving the row data for "select c1 from t2".
type outerQuery struct {
	// outer is the last outer table reference.
	last *outerQuery
	row  *plan.Row
}

// We will push a outerQuery after the from phase and pop it before the final phase.
// So we can guarantee that there is at least one outerQuery certainly.
func pushOuterQuery(ctx context.Context, row *plan.Row) {
	t := &outerQuery{
		row: row,
	}

	last := ctx.Value(outerQueryKey)
	if last != nil {
		// must be outerQuery
		t.last = last.(*outerQuery)
	}

	ctx.SetValue(outerQueryKey, t)
}

func getOuterQuery(ctx context.Context) *outerQuery {
	v := ctx.Value(outerQueryKey)
	// Cannot empty and must be outerQuery
	t := v.(*outerQuery)
	return t
}

func popOuterQuery(ctx context.Context) {
	t := getOuterQuery(ctx)
	if t.last == nil {
		ctx.ClearValue(outerQueryKey)
		return
	}

	ctx.SetValue(outerQueryKey, t.last)
}

func updateTopOuterQuery(ctx context.Context, row *plan.Row) {
	t := getOuterQuery(ctx)
	t.row = row
}

func getIdentValueFromOuterQuery(ctx context.Context, name string) (interface{}, error) {
	t := getOuterQuery(ctx)
	// The top is current outerQuery, use its last.
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

	return nil, errors.Errorf("can not find value from outer query for field %s", name)
}
