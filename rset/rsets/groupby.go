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
	_ plan.Planner = (*GroupByRset)(nil)
)

// GroupByRset is record set for group by fields.
type GroupByRset struct {
	By         []expression.Expression
	Src        plan.Plan
	SelectList *plans.SelectList
}

// HasAmbiguousField checks whether have ambiguous group by fields.
func (r *GroupByRset) HasAmbiguousField(indices []int, fields []*field.Field) bool {
	columnNameMap := map[string]struct{}{}
	for _, index := range indices {
		expr := fields[index].Expr

		// `select c1 + c2 as c1, c1 + c3 as c1 from t order by c1` is valid,
		// if it is not `Ident` expression, ignore it.
		v, ok := expr.(*expression.Ident)
		if !ok {
			continue
		}

		// `select c1 as c2, c1 as c2 from t order by c2` is valid,
		// use a map for it here.
		columnNameMap[v.L] = struct{}{}
	}

	return len(columnNameMap) > 1
}

// Plan gets GroupByDefaultPlan.
func (r *GroupByRset) Plan(ctx context.Context) (plan.Plan, error) {
	fields := r.SelectList.Fields
	resultfields := r.SelectList.ResultFields
	srcFields := r.Src.GetFields()

	r.SelectList.AggFields = GetAggFields(fields)
	aggFields := r.SelectList.AggFields

	for i, e := range r.By {
		if v, ok := e.(expression.Value); ok {
			var position int
			switch u := v.Val.(type) {
			case int64:
				position = int(u)
			case uint64:
				position = int(u)
			default:
				continue
			}

			if position < 1 || position > len(fields) {
				return nil, errors.Errorf("Unknown column '%d' in 'group statement'", position)
			}

			index := position - 1
			if _, ok := aggFields[index]; ok {
				return nil, errors.Errorf("Can't group on '%s'", fields[index].Name)
			}

			// use Position expression for the associated field.
			r.By[i] = &expression.Position{N: position}
		} else {
			names := expression.MentionedColumns(e)
			for _, name := range names {
				if field.ContainFieldName(name, srcFields, field.DefaultFieldFlag) {
					// check whether column is qualified, like `select t.c1 c1, t.c2 from t group by t.c1, t.c2`
					// no need to check ambiguous field.
					if expression.IsQualified(name) {
						continue
					}

					// check ambiguous fields, like `select c1 as c2, c2 from t group by c2`.
					if err := field.CheckAmbiguousField(name, resultfields, field.DefaultFieldFlag); err == nil {
						continue
					}
				}

				// check reference to group function name
				indices := field.GetFieldIndex(name, fields[0:r.SelectList.HiddenFieldOffset], field.CheckFieldFlag)
				if len(indices) > 1 {
					// check ambiguous fields, like `select c1 as a, c2 as a from t group by a`,
					// notice that `select c2 as c2, c2 as c2 from t group by c2;` is valid.
					if r.HasAmbiguousField(indices, fields[0:r.SelectList.HiddenFieldOffset]) {
						return nil, errors.Errorf("Column '%s' in group statement is ambiguous", name)
					}
				} else if len(indices) == 1 {
					// check reference to aggregate function, like `select c1, count(c1) as b from t group by b + 1`.
					index := indices[0]
					if _, ok := aggFields[index]; ok {
						return nil, errors.Errorf("Reference '%s' not supported (reference to group function)", name)
					}
				}
			}

			// group by should be an expression, a qualified field name or a select field position,
			// but can not contain any aggregate function.
			if e := r.By[i]; expression.ContainAggregateFunc(e) {
				return nil, errors.Errorf("group by cannot contain aggregate function %s", e.String())
			}
		}
	}

	return &plans.GroupByDefaultPlan{By: r.By, Src: r.Src,
		SelectList: r.SelectList}, nil
}

func (r *GroupByRset) String() string {
	a := make([]string, len(r.By))
	for i, v := range r.By {
		a[i] = v.String()
	}
	return strings.Join(a, ", ")
}
