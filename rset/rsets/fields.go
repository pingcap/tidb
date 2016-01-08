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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*SelectFieldsRset)(nil)
	_ plan.Planner = (*SelectFromDualRset)(nil)
)

// SelectFieldsRset is record set to select fields.
type SelectFieldsRset struct {
	Src        plan.Plan
	SelectList *plans.SelectList
}

func updateSelectFieldsRefer(selectList *plans.SelectList) error {
	visitor := NewFromIdentVisitor(selectList.FromFields, FieldListClause)

	// we only fix un-hidden fields here, for hidden fields, it should be
	// handled in their own clause, in order by or having.
	for i, f := range selectList.Fields[0:selectList.HiddenFieldOffset] {
		e, err := f.Expr.Accept(visitor)
		if err != nil {
			return errors.Trace(err)
		}
		selectList.Fields[i].Expr = e
	}
	return nil
}

// Plan gets SrcPlan/SelectFieldsDefaultPlan.
// If all fields are equal to src plan fields, then gets SrcPlan.
// Default gets SelectFieldsDefaultPlan.
func (r *SelectFieldsRset) Plan(ctx context.Context) (plan.Plan, error) {
	if err := updateSelectFieldsRefer(r.SelectList); err != nil {
		return nil, errors.Trace(err)
	}

	fields := r.SelectList.Fields
	srcFields := r.Src.GetFields()
	if len(fields) == len(srcFields) {
		match := true
		for i, v := range fields {
			// TODO: is it this check enough? e.g, the ident field is t.c.
			if x, ok := v.Expr.(*expression.Ident); ok && strings.EqualFold(x.L, srcFields[i].Name) {
				if len(v.AsName) > 0 && !strings.EqualFold(v.AsName, srcFields[i].Name) {
					// have alias name, but alias name is not the same with result field name.
					// e.g, select c as a from t;
					match = false
					break
				} else {
					// pass
				}

			} else {
				match = false
				break
			}

		}

		if match {
			return r.Src, nil
		}
	}

	src := r.Src
	if x, ok := src.(*plans.TableDefaultPlan); ok {
		// check whether src plan will be set TableNilPlan, like `select 1, 2 from t`.
		isConst := true
		for _, v := range fields {
			if expression.FastEval(v.Expr) == nil {
				isConst = false
				break
			}
		}
		if isConst {
			src = &plans.TableNilPlan{T: x.T}
		}
	}

	p := &plans.SelectFieldsDefaultPlan{Src: src, SelectList: r.SelectList}
	return p, nil
}

// SelectFromDualRset is Recordset for select from dual, like `select 1, 1+1` or `select 1 from dual`.
type SelectFromDualRset struct {
	Fields []*field.Field
}

// Plan gets SelectExprPlan.
func (r *SelectFromDualRset) Plan(ctx context.Context) (plan.Plan, error) {
	// field cannot contain identifier
	for _, f := range r.Fields {
		if cs := expression.MentionedColumns(f.Expr); len(cs) > 0 {
			// TODO: check in outer query, like select * from t where t.c = (select c limit 1);
			return nil, errors.Errorf("Unknown column '%s' in 'field list'", cs[0])
		}
	}
	return &plans.SelectFromDualPlan{Fields: r.Fields}, nil
}
