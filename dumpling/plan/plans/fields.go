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

package plans

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*SelectFieldsDefaultPlan)(nil)
	_ plan.Plan = (*SelectFromDualPlan)(nil)
)

// SelectFieldsDefaultPlan extracts specific fields from Src Plan.
type SelectFieldsDefaultPlan struct {
	*SelectList
	Src      plan.Plan
	evalArgs map[interface{}]interface{}
}

// Explain implements the plan.Plan Explain interface.
func (r *SelectFieldsDefaultPlan) Explain(w format.Formatter) {
	// TODO: check for non existing fields
	r.Src.Explain(w)
	w.Format("┌Evaluate")
	for _, v := range r.Fields {
		w.Format(" %s as %s,", v.Expr, fmt.Sprintf("%q", v.Name))
	}
	w.Format("\n└Output field names %v\n", field.RFQNames(r.ResultFields))
}

// Filter implements the plan.Plan Filter interface.
func (r *SelectFieldsDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// Next implements plan.Plan Next interface.
func (r *SelectFieldsDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.evalArgs == nil {
		r.evalArgs = map[interface{}]interface{}{}
	}
	srcRow, err := r.Src.Next(ctx)
	if err != nil || srcRow == nil {
		return nil, errors.Trace(err)
	}

	r.evalArgs[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
		v, err0 := GetIdentValue(name, r.Src.GetFields(), srcRow.Data, field.DefaultFieldFlag)
		if err0 == nil {
			return v, nil
		}

		return getIdentValueFromOuterQuery(ctx, name)
	}
	row = &plan.Row{
		Data: make([]interface{}, len(r.Fields)),
		// must save FromData info for inner sub query use.
		FromData: srcRow.Data,
	}
	for i, fld := range r.Fields {
		var err error
		if row.Data[i], err = fld.Expr.Eval(ctx, r.evalArgs); err != nil {
			return nil, errors.Trace(err)
		}
	}
	updateRowStack(ctx, row.Data, row.FromData)
	return
}

// Close implements plan.Plan Close interface.
func (r *SelectFieldsDefaultPlan) Close() error {
	return r.Src.Close()
}

// SelectFromDualPlan is the plan for "select expr, expr, ..."" or "select expr, expr, ... from dual".
type SelectFromDualPlan struct {
	Fields []*field.Field
	done   bool
}

// Explain implements the plan.Plan Explain interface.
func (s *SelectFromDualPlan) Explain(w format.Formatter) {
	// TODO: finish this
}

// GetFields implements the plan.Plan GetFields interface.
func (s *SelectFromDualPlan) GetFields() []*field.ResultField {
	ans := make([]*field.ResultField, 0, len(s.Fields))
	if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			nm := f.Name
			ans = append(ans, &field.ResultField{Name: nm})
		}
	}

	return ans
}

// Filter implements the plan.Plan Filter interface.
func (s *SelectFromDualPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return s, false, nil
}

// Next implements plan.Plan Next interface.
func (s *SelectFromDualPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if s.done {
		return
	}
	// Here we must output an empty row and don't evaluate fields
	// because we will evaluate fields later in SelectFieldsDefaultPlan or GroupByDefaultPlan.
	row = &plan.Row{
		Data: make([]interface{}, len(s.Fields)),
	}
	s.done = true
	return
}

// Close implements plan.Plan Close interface.
func (s *SelectFromDualPlan) Close() error {
	s.done = false
	return nil
}
