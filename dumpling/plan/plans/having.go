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
	Src      plan.Plan
	Expr     expression.Expression
	evalArgs map[interface{}]interface{}
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

// Next implements plan.Plan Next interface.
func (r *HavingPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.evalArgs == nil {
		r.evalArgs = map[interface{}]interface{}{}
	}
	for {
		var srcRow *plan.Row
		srcRow, err = r.Src.Next(ctx)
		if srcRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		r.evalArgs[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return GetIdentValue(name, r.Src.GetFields(), srcRow.Data, field.CheckFieldFlag)
		}
		r.evalArgs[expressions.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
			// position is in [1, len(fields)], so we must decrease 1 to get correct index
			// TODO: check position invalidation
			return srcRow.Data[position-1], nil
		}
		var v bool
		v, err = expressions.EvalBoolExpr(ctx, r.Expr, r.evalArgs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if v {
			row = srcRow
			return
		}
	}
}

// Close implements plan.Plan Close interface.
func (r *HavingPlan) Close() error {
	return r.Src.Close()
}
