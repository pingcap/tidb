// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*SelectFieldsDefaultPlan)(nil)
	_ plan.Plan = (*SelectEmptyFieldListPlan)(nil)
)

// SelectFieldsDefaultPlan extracts specific fields from Src Plan.
type SelectFieldsDefaultPlan struct {
	*SelectList
	Src plan.Plan
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

// Do implements the plan.Plan Do interface, extracts specific values by
// r.Fields from row data.
func (r *SelectFieldsDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	fields := r.Src.GetFields()
	m := map[interface{}]interface{}{}
	return r.Src.Do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return getIdentValue(name, fields, in, field.DefaultFieldFlag)
		}

		out := make([]interface{}, len(r.Fields))
		for i, fld := range r.Fields {
			var err error
			if out[i], err = fld.Expr.Eval(ctx, m); err != nil {
				return false, err
			}
		}
		return f(rid, out)
	})
}

// SelectEmptyFieldListPlan is the plan for "select expr, expr, ..."" with no FROM.
type SelectEmptyFieldListPlan struct {
	Fields []*field.Field
}

// Do implements the plan.Plan Do interface, returns empty row.
func (s *SelectEmptyFieldListPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	// here we must output an empty row and don't evaluate fields
	// because we will evaluate fields later in SelectFieldsDefaultPlan or GroupByDefaultPlan
	values := make([]interface{}, len(s.Fields))
	if _, err := f(0, values); err != nil {
		return err
	}

	return nil
}

// Explain implements the plan.Plan Explain interface.
func (s *SelectEmptyFieldListPlan) Explain(w format.Formatter) {
	// TODO: finish this
}

// GetFields implements the plan.Plan GetFields interface.
func (s *SelectEmptyFieldListPlan) GetFields() []*field.ResultField {
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
func (s *SelectEmptyFieldListPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return s, false, nil
}
