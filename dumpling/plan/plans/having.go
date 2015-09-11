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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*HavingPlan)(nil)
)

// HavingPlan executes the HAVING statement, HavingPlan's behavior is almost the
// same as FilterDefaultPlan.
type HavingPlan struct {
	Src  plan.Plan
	Expr expression.Expression
}

// Explain implements plan.Plan Explain interface.
func (r *HavingPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Having %s\n└Output field names %v\n", r.Expr.String(), r.Src.GetFields())
}

// Filter implements plan.Plan Filter interface.
func (r *HavingPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *HavingPlan) GetFields() []*field.ResultField {
	return r.Src.GetFields()
}

// Do implements plan.Plan Do interface.
// It scans rows over SrcPlan and check if it meets all conditions in Expr.
func (r *HavingPlan) Do(ctx context.Context, f plan.RowIterFunc) (err error) {
	m := map[interface{}]interface{}{}

	return r.Src.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return getIdentValue(name, r.Src.GetFields(), in, field.CheckFieldFlag)
		}

		m[expressions.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
			// position is in [1, len(fields)], so we must decrease 1 to get correct index
			// TODO: check position invalidation
			return in[position-1], nil
		}

		v, err := expressions.EvalBoolExpr(ctx, r.Expr, m)

		if err != nil {
			return false, err
		}

		if !v {
			return true, nil
		}

		return f(rid, in)
	})
}

// Next implements plan.Plan Next interface.
func (r *HavingPlan) Next(ctx context.Context) (data []interface{}, rowKeys []*plan.RowKeyEntry, err error) {
	return
}
