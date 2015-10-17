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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*HavingRset)(nil)
)

// HavingRset is record set for having fields.
type HavingRset struct {
	Src        plan.Plan
	Expr       expression.Expression
	SelectList *plans.SelectList
}

// CheckAggregate will check whether order by has aggregate function or not,
// if has, we will add it to select list hidden field.
func (r *HavingRset) CheckAggregate(selectList *plans.SelectList) error {
	if expression.ContainAggregateFunc(r.Expr) {
		expr, err := selectList.UpdateAggFields(r.Expr)
		if err != nil {
			return errors.Errorf("%s in 'having clause'", err.Error())
		}

		r.Expr = expr
	}

	return nil
}

// CheckAndUpdateSelectList checks having fields validity and set hidden fields to selectList.
func (r *HavingRset) CheckAndUpdateSelectList(selectList *plans.SelectList, groupBy []expression.Expression, tableFields []*field.ResultField) error {
	if expression.ContainAggregateFunc(r.Expr) {
		expr, err := selectList.UpdateAggFields(r.Expr)
		if err != nil {
			return errors.Errorf("%s in 'having clause'", err.Error())
		}

		r.Expr = expr
	} else {
		// having can only contain group by column and select list, e.g,
		// `select c1 from t group by c2 having c3 > 0` is invalid,
		// because c3 is not in group by and select list.
		names := expression.MentionedColumns(r.Expr)
		for _, name := range names {
			found := false

			// check name whether in select list.
			// notice that `select c1 as c2 from t group by c1, c2, c3 having c2 > c3`,
			// will use t.c2 not t.c1 here.
			if field.ContainFieldName(name, selectList.ResultFields, field.OrgFieldNameFlag) {
				continue
			}
			if field.ContainFieldName(name, selectList.ResultFields, field.FieldNameFlag) {
				if field.ContainFieldName(name, tableFields, field.OrgFieldNameFlag) {
					selectList.CloneHiddenField(name, tableFields)
				}
				continue
			}

			// check name whether in group by.
			// group by must only have column name, e.g,
			// `select c1 from t group by c2 having c2 > 0` is valid,
			// but `select c1 from t group by c2 + 1 having c2 > 0` is invalid.
			for _, by := range groupBy {
				if !field.CheckFieldsEqual(name, by.String()) {
					continue
				}

				// if name is not in table fields, it will get an unknown field error in GroupByRset,
				// so no need to check return value.
				selectList.CloneHiddenField(name, tableFields)

				found = true
				break
			}

			if !found {
				return errors.Errorf("Unknown column '%s' in 'having clause'", name)
			}
		}
	}

	return nil
}

// Plan gets HavingPlan.
func (r *HavingRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.HavingPlan{Src: r.Src, Expr: r.Expr, SelectList: r.SelectList}, nil
}

//  String implements fmt.Stringer interface.
func (r *HavingRset) String() string {
	return r.Expr.String()
}
