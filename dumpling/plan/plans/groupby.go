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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Plan = (*GroupByDefaultPlan)(nil)
)

// GroupByDefaultPlan handles GROUP BY statement, GroupByDefaultPlan uses an
// in-memory table to aggregate values.
type GroupByDefaultPlan struct {
	*SelectList
	Src plan.Plan
	By  []expression.Expression
}

// Explain implements plan.Plan Explain interface.
func (r *GroupByDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Evaluate")
	for _, v := range r.Fields {
		w.Format(" %s as %s,", v.Expr, fmt.Sprintf("%q", v.Name))
	}

	switch {
	case len(r.By) == 0: //TODO this case should not exist for this plan.Plan, should become TableDefaultPlan
		w.Format("\n│Group by distinct rows")
	default:
		w.Format("\n│Group by")
		for _, v := range r.By {
			w.Format(" %s,", v)
		}
	}
	w.Format("\n└Output field names %v\n", field.RFQNames(r.ResultFields))
}

// Filter implements plan.Plan Filter interface.
func (r *GroupByDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

type groupRow struct {
	Row []interface{}
	// for one group by row, we should have a map to save aggregate value
	Args map[interface{}]interface{}
}

// Do implements plan.Plan Do interface.
// Table: Subject_Selection
// Subject   Semester   Attendee
// ---------------------------------
// ITB001    1          John
// ITB001    1          Bob
// ITB001    1          Mickey
// ITB001    2          Jenny
// ITB001    2          James
// MKB114    1          John
// MKB114    1          Erica
// refs: http://stackoverflow.com/questions/2421388/using-group-by-on-multiple-columns
func (r *GroupByDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) (err error) {
	// TODO: now we have to use this to save group key -> row index
	// later we will serialize group by items into a string key and then use a map instead.
	t, err := memkv.CreateTemp(true)
	if err != nil {
		return err
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()

	k := make([]interface{}, len(r.By))

	// save output group by result
	var outRows []*groupRow

	err = r.Src.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		out := make([]interface{}, len(r.Fields))

		// TODO: later order by will use the same mechanism, so we may use another plan to do this
		m := map[interface{}]interface{}{}
		// must first eval none aggregate fields, because alias group by will use this.
		if err := r.evalNoneAggFields(ctx, out, m, in); err != nil {
			return false, err
		}

		if err := r.evalGroupKey(ctx, k, out, in); err != nil {
			return false, err
		}

		// get row index with k.
		v, err := t.Get(k)
		if err != nil {
			return false, err
		}

		index := 0
		if len(v) == 0 {
			// no group for key, save data for this group
			index = len(outRows)
			outRows = append(outRows, &groupRow{Row: out, Args: m})

			if err := t.Set(k, []interface{}{index}); err != nil {
				return false, err
			}
		} else {
			// we have already saved data in the group by key, use this
			index = v[0].(int)

			// we will use this context args to evaluate aggregate fields.
			m = outRows[index].Args
		}

		// eval aggregate fields
		if err := r.evalAggFields(ctx, out, m, in); err != nil {
			return false, err
		}

		return true, nil
	})

	if err != nil {
		return err
	}

	if len(outRows) == 0 {
		// empty table
		var out []interface{}
		out, err = r.evalEmptyTable(ctx)
		if err != nil || out == nil {
			return err
		}

		_, err = f(nil, out)

		return err
	}

	// we don't consider implicit GROUP BY sorting now.
	// Relying on implicit GROUP BY sorting in MySQL 5.7 is deprecated.
	// To achieve a specific sort order of grouped results,
	// it is preferable to use an explicit ORDER BY clause.
	// GROUP BY sorting is a MySQL extension that may change in a future release
	var more bool
	for _, row := range outRows {
		// eval aggregate done
		if err := r.evalAggDone(ctx, row.Row, row.Args); err != nil {
			return err
		}
		if more, err = f(nil, row.Row); !more || err != nil {
			break
		}
	}

	return types.EOFAsNil(err)
}

func (r *GroupByDefaultPlan) evalGroupKey(ctx context.Context, k []interface{}, outRow []interface{}, in []interface{}) error {
	// group by items can not contain aggregate field, so we can eval them safely.
	m := map[interface{}]interface{}{}
	m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
		v, err := getIdentValue(name, r.Src.GetFields(), in, field.DefaultFieldFlag)
		if err == nil {
			return v, nil
		}

		return r.getFieldValueByName(name, outRow)
	}

	m[expressions.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
		// position is in [1, len(fields)], so we must decrease 1 to get correct index
		// TODO: check position invalidation
		return outRow[position-1], nil
	}

	// Eval group by result
	for i, v := range r.By {
		val, err := v.Eval(ctx, m)
		if err != nil {
			return err
		}
		k[i] = val
	}
	return nil
}

func (r *GroupByDefaultPlan) getFieldValueByName(name string, out []interface{}) (interface{}, error) {
	for i, f := range r.Fields[0:r.HiddenFieldOffset] {
		if name == f.Name {
			return out[i], nil
		}
	}
	return nil, errors.Errorf("unknown field %s", name)
}

func (r *GroupByDefaultPlan) evalNoneAggFields(ctx context.Context, out []interface{},
	m map[interface{}]interface{}, in []interface{}) error {
	m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
		return getIdentValue(name, r.Src.GetFields(), in, field.DefaultFieldFlag)
	}

	var err error
	// Eval none aggregate field results in ctx
	for i, fld := range r.Fields {
		if _, ok := r.AggFields[i]; !ok {
			if out[i], err = fld.Expr.Eval(ctx, m); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *GroupByDefaultPlan) evalAggFields(ctx context.Context, out []interface{},
	m map[interface{}]interface{}, in []interface{}) error {
	// Eval aggregate field results in ctx
	for i := range r.AggFields {
		if i < r.HiddenFieldOffset {
			m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return getIdentValue(name, r.Src.GetFields(), in, field.DefaultFieldFlag)
			}
		} else {
			// having may contain aggregate function and we will add it to hidden field,
			// and this field can retrieve the data in select list, e.g.
			// 	select c1 as a from t having count(a) = 1
			// because all the select list data is generated before, so we can get it
			// when handling hidden field.
			m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				v, err := getIdentValue(name, r.Src.GetFields(), in, field.DefaultFieldFlag)
				if err == nil {
					return v, nil
				}

				// if we can not find in table, we will try to find in un-hidden select list
				// only hidden field can use this
				return r.getFieldValueByName(name, out)
			}
		}

		// we must evaluate aggregate function only, e.g, select col1 + count(*) in (count(*)),
		// we cannot evaluate it directly here, because col1 + count(*) returns nil before AggDone phase,
		// so we don't evaluate count(*) in In expression, and will get an invalid data in AggDone phase for it.
		// mention all aggregate functions
		// TODO: optimize, we can get these aggregate functions only once and reuse
		aggs := expressions.MentionedAggregateFuncs(r.Fields[i].Expr)
		for _, agg := range aggs {
			if _, err := agg.Eval(ctx, m); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *GroupByDefaultPlan) evalAggDone(ctx context.Context, out []interface{},
	m map[interface{}]interface{}) error {
	m[expressions.ExprAggDone] = true

	var err error
	// Eval aggregate field results done in ctx
	for i := range r.AggFields {
		if out[i], err = r.Fields[i].Expr.Eval(ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (r *GroupByDefaultPlan) evalEmptyTable(ctx context.Context) ([]interface{}, error) {
	// Calculate default value, eg: select count(*) from empty_table
	// if we have group by, we don't need to output an extra row
	if len(r.By) > 0 {
		return nil, nil
	}

	out := make([]interface{}, len(r.Fields))
	// aggregate empty record set
	m := map[interface{}]interface{}{expressions.ExprEvalArgAggEmpty: true}

	var err error
	for i, fld := range r.Fields {
		// we cannot only evaluate aggregate fields here, e.g.
		// "select max(c1), 123 from t where c1 = null" should get "NULL", 123 as the result
		// if we don't evaluate none aggregate fields, we will get incorrect "NULL", "NULL"
		if out[i], err = fld.Expr.Eval(ctx, m); err != nil {
			return nil, err
		}
	}
	return out, nil
}
