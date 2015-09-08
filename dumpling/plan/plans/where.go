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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Plan = (*FilterDefaultPlan)(nil)
)

// FilterDefaultPlan handles WHERE statement, filters rows by specific
// expressions.
type FilterDefaultPlan struct {
	plan.Plan
	Expr expression.Expression
}

// Explain implements plan.Plan Explain interface.
func (r *FilterDefaultPlan) Explain(w format.Formatter) {
	r.Plan.Explain(w)
	w.Format("┌FilterDefaultPlan Filter on %v\n", r.Expr)
	w.Format("└Output field names %v\n", field.RFQNames(r.GetFields()))
}

// Do implements plan.Plan Do interface.
func (r *FilterDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) (err error) {
	m := map[interface{}]interface{}{}
	fields := r.GetFields()
	return r.Plan.Do(ctx, func(rid interface{}, data []interface{}) (bool, error) {
		m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return getIdentValue(name, fields, data, field.DefaultFieldFlag)
		}
		val, err := r.Expr.Eval(ctx, m)
		if err != nil {
			return false, err
		}

		if val == nil {
			return true, nil
		}

		// Evaluate the expression, if the result is true, go on, otherwise
		// skip this row.
		x, err := types.ToBool(val)
		if err != nil {
			return false, err
		}

		if x == 0 {
			return true, nil
		}
		return f(rid, data)
	})
}
