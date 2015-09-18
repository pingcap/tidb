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
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*OrderByRset)(nil)
)

// OrderByItem is order by item.
type OrderByItem struct {
	Expr expression.Expression
	Asc  bool
}

func (i OrderByItem) String() string {
	if i.Asc {
		return fmt.Sprintf("%s ASC", i.Expr)
	}

	return fmt.Sprintf("%s DESC", i.Expr)
}

// OrderByRset is record set for order by fields.
type OrderByRset struct {
	By         []OrderByItem
	Src        plan.Plan
	SelectList *plans.SelectList
}

func (r *OrderByRset) String() string {
	if r == nil || r.By == nil {
		return ""
	}
	a := make([]string, len(r.By))
	for i, v := range r.By {
		a[i] = v.String()
	}
	return strings.Join(a, ", ")
}

// CheckAndUpdateSelectList checks order by fields validity and set hidden fields to selectList.
func (r *OrderByRset) CheckAndUpdateSelectList(selectList *plans.SelectList, tableFields []*field.ResultField) error {
	for i, v := range r.By {
		if expressions.ContainAggregateFunc(v.Expr) {
			expr, err := selectList.UpdateAggFields(v.Expr, tableFields)
			if err != nil {
				return errors.Errorf("%s in 'order clause'", err.Error())
			}

			r.By[i].Expr = expr
		} else {
			names := expressions.MentionedColumns(v.Expr)
			for _, name := range names {
				// try to find in select list
				// TODO: mysql has confused result for this, see #555.
				// now we use select list then order by, later we should make it easier.
				if field.ContainFieldName(name, selectList.ResultFields, field.CheckFieldFlag) {
					// check ambiguous fields, like `select c1 as c2, c2 from t order by c2`.
					if err := field.CheckAmbiguousField(name, selectList.ResultFields, field.DefaultFieldFlag); err != nil {
						return errors.Errorf("Column '%s' in order statement is ambiguous", name)
					}
					continue
				}

				if !selectList.CloneHiddenField(name, tableFields) {
					return errors.Errorf("Unknown column '%s' in 'order clause'", name)
				}
			}
		}
	}

	return nil
}

// Plan get SrcPlan/OrderByDefaultPlan.
// If src is NullPlan or order by fields are empty, then gets SrcPlan.
// Default gets OrderByDefaultPlan.
func (r *OrderByRset) Plan(ctx context.Context) (plan.Plan, error) {
	if _, ok := r.Src.(*plans.NullPlan); ok {
		return r.Src, nil
	}

	var (
		by   []expression.Expression
		ascs []bool
	)

	fields := r.Src.GetFields()
	for i := range r.By {
		e := r.By[i].Expr
		if v, ok := e.(expressions.Value); ok {
			var (
				position   int
				isPosition = true
			)

			switch u := v.Val.(type) {
			case int64:
				position = int(u)
			case uint64:
				position = int(u)
			default:
				isPosition = false
				// only const value
			}

			if isPosition {
				if position < 1 || position > len(fields) {
					return nil, errors.Errorf("Unknown column '%d' in 'order clause'", position)
				}

				// use Position expression for the associated field.
				r.By[i].Expr = &expressions.Position{N: position}
			}
		} else {
			colNames := expressions.MentionedColumns(e)
			if err := field.CheckAllFieldNames(colNames, fields, field.CheckFieldFlag); err != nil {
				return nil, errors.Trace(err)
			}
		}

		by = append(by, r.By[i].Expr)
		ascs = append(ascs, r.By[i].Asc)
	}

	// if no order by key, use src plan instead.
	if len(by) == 0 {
		return r.Src, nil
	}

	return &plans.OrderByDefaultPlan{By: by, Ascs: ascs, Src: r.Src,
		SelectList: r.SelectList}, nil
}
