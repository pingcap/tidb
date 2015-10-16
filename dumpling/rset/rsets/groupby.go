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

// Plan gets GroupByDefaultPlan.
func (r *GroupByRset) Plan(ctx context.Context) (plan.Plan, error) {
	fields := r.SelectList.Fields

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
				return nil, errors.Errorf("Can't group on '%s'", fields[index])
			}

			// use Position expression for the associated field.
			r.By[i] = &expression.Position{N: position}
		} else {
			index, err := r.SelectList.CheckReferAmbiguous(e)
			if err != nil {
				return nil, errors.Errorf("Column '%s' in group statement is ambiguous", e)
			} else if _, ok := aggFields[index]; ok {
				return nil, errors.Errorf("Can't group on '%s'", e)
			}

			// TODO: check more ambiguous case
			// Group by ambiguous rule:
			//	select c1 as a, c2 as a from t group by a is ambiguous
			//	select c1 as a, c2 as a from t group by a + 1 is ambiguous
			//	select c1 as c2, c2 from t group by c2 is ambiguous
			//	select c1 as c2, c2 from t group by c2 + 1 is ambiguous

			// TODO: use visitor to check aggregate function
			names := expression.MentionedColumns(e)
			for _, name := range names {
				indices := field.GetFieldIndex(name, fields[0:r.SelectList.HiddenFieldOffset], field.DefaultFieldFlag)
				if len(indices) == 1 {
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
