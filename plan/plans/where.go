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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
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
	Expr     expression.Expression
	evalArgs map[interface{}]interface{}
}

// Explain implements plan.Plan Explain interface.
func (r *FilterDefaultPlan) Explain(w format.Formatter) {
	r.Plan.Explain(w)
	w.Format("┌FilterDefaultPlan Filter on %v\n", r.Expr)
	w.Format("└Output field names %v\n", field.RFQNames(r.GetFields()))
}

// Next implements plan.Plan Next interface.
func (r *FilterDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.evalArgs == nil {
		r.evalArgs = map[interface{}]interface{}{}
	}
	for {
		row, err = r.Plan.Next(ctx)
		if row == nil || err != nil {
			return nil, errors.Trace(err)
		}
		r.evalArgs[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
			if scope == expression.IdentReferFromTable {
				return row.Data[index], nil
			}

			// try to find in outer query
			return getIdentValueFromOuterQuery(ctx, name)
		}

		var meet bool
		meet, err = r.meetCondition(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if meet {
			return
		}
	}
}

func (r *FilterDefaultPlan) meetCondition(ctx context.Context) (bool, error) {
	val, err := r.Expr.Eval(ctx, r.evalArgs)
	if val == nil || err != nil {
		return false, errors.Trace(err)
	}
	x, err := types.ToBool(val)
	if err != nil {
		return false, errors.Trace(err)
	}
	return x == 1, nil
}

// Close implements plan.Plan Close interface.
func (r *FilterDefaultPlan) Close() error {
	return r.Plan.Close()
}
