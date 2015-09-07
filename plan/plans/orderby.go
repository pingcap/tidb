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
	"github.com/pingcap/tidb/expression/expressions"
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
	Row []interface{}
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

// Do implements plan.Plan Do interface, all records are added into an
// in-memory array, and sorted in ASC/DESC order.
func (r *OrderByDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	t := &orderByTable{Ascs: r.Ascs}

	m := map[interface{}]interface{}{}
	err := r.Src.Do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return getIdentValue(name, r.ResultFields, in, field.CheckFieldFlag)
		}

		m[expressions.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
			// position is in [1, len(fields)], so we must decrease 1 to get correct index
			// TODO: check position invalidation
			return in[position-1], nil
		}

		row := &orderByRow{
			Row: in,
			Key: make([]interface{}, 0, len(r.By)),
		}

		for _, by := range r.By {
			var (
				val interface{}
				err error
			)

			val, err = by.Eval(ctx, m)
			if err != nil {
				return false, err
			}

			if val != nil {
				var ordered bool
				val, ordered, err = types.IsOrderedType(val)
				if err != nil {
					return false, err
				}

				if !ordered {
					return false, errors.Errorf("cannot order by %v (type %T)", val, val)
				}
			}

			row.Key = append(row.Key, val)
		}

		t.Rows = append(t.Rows, row)
		return true, nil
	})
	if err != nil {
		return err
	}

	sort.Sort(t)

	var more bool
	for _, row := range t.Rows {
		if more, err = f(nil, row.Row); !more || err != nil {
			break
		}
	}
	return types.EOFAsNil(err)
}
