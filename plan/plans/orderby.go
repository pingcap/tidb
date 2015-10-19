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
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var _ plan.Plan = (*OrderByDefaultPlan)(nil)

// OrderByDefaultPlan handles ORDER BY statement, it uses an array to store
// results temporarily, and sorts them by given expression.
type OrderByDefaultPlan struct {
	*SelectList
	By   []expression.Expression
	Ascs []bool
	Src  plan.Plan

	ordTable *orderByTable
	cursor   int
}

// Explain implements plan.Plan Explain interface.
func (r *OrderByDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Order by")

	items := make([]string, len(r.By))
	for i, v := range r.By {
		order := "ASC"
		if !r.Ascs[i] {
			order = "DESC"
		}
		items[i] = fmt.Sprintf(" %s %s", v, order)
	}
	w.Format("%s", strings.Join(items, ","))
	w.Format("\n└Output field names %v\n", field.RFQNames(r.ResultFields))
}

// Filter implements plan.Plan Filter interface.
func (r *OrderByDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

type orderByRow struct {
	Key []interface{}
	Row *plan.Row
}

func (o *orderByRow) String() string {
	if o == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[orderByRow](%+v)", *o)
}

type orderByTable struct {
	Rows []*orderByRow
	Ascs []bool
}

// String implements fmt.Stringer interface. Just for debugging.
func (t *orderByTable) String() string {
	if t == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[orderByTable](%+v)", *t)
}

// Len returns the number of rows.
func (t *orderByTable) Len() int {
	return len(t.Rows)
}

// Swap implements sort.Interface Swap interface.
func (t *orderByTable) Swap(i, j int) {
	t.Rows[i], t.Rows[j] = t.Rows[j], t.Rows[i]
}

// Less implements sort.Interface Less interface.
func (t *orderByTable) Less(i, j int) bool {
	for index, asc := range t.Ascs {
		v1 := t.Rows[i].Key[index]
		v2 := t.Rows[j].Key[index]

		ret, err := types.Compare(v1, v2)
		if err != nil {
			// we just have to log this error and skip it.
			// TODO: record this error and handle it out later.
			log.Errorf("compare %v %v err %v", v1, v2, err)
		}

		if !asc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// Next implements plan.Plan Next interface.
func (r *OrderByDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.ordTable == nil {
		r.ordTable = &orderByTable{Ascs: r.Ascs}
		err = r.fetchAll(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if r.cursor == len(r.ordTable.Rows) {
		return
	}
	row = r.ordTable.Rows[r.cursor].Row
	updateRowStack(ctx, row.Data, row.FromData)
	r.cursor++
	return
}

func (r *OrderByDefaultPlan) fetchAll(ctx context.Context) error {
	evalArgs := map[interface{}]interface{}{}
	for {
		row, err := r.Src.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		evalArgs[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
			if scope == expression.IdentReferFromTable {
				return row.FromData[index], nil
			} else if scope == expression.IdentReferSelectList {
				return row.Data[index], nil
			}

			// try to find in outer query
			return getIdentValueFromOuterQuery(ctx, name)
		}

		evalArgs[expression.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
			// position is in [1, len(fields)], so we must decrease 1 to get correct index
			// TODO: check position invalidation
			return row.Data[position-1], nil
		}
		ordRow := &orderByRow{
			Row: row,
			Key: make([]interface{}, 0, len(r.By)),
		}
		for _, by := range r.By {
			// err1 is used for passing `go tool vet --shadow` check.
			val, err1 := by.Eval(ctx, evalArgs)
			if err1 != nil {
				return err1
			}

			if val != nil {
				if !types.IsOrderedType(val) {
					return errors.Errorf("cannot order by %v (type %T)", val, val)
				}
			}

			ordRow.Key = append(ordRow.Key, val)
		}
		r.ordTable.Rows = append(r.ordTable.Rows, ordRow)
	}
	sort.Sort(r.ordTable)
	return nil
}

// Close implements plan.Plan Close interface.
func (r *OrderByDefaultPlan) Close() error {
	r.ordTable = nil
	r.cursor = 0
	return r.Src.Close()
}
