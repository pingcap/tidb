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
	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*GroupByDefaultPlan)(nil)
)

// GroupByDefaultPlan handles GROUP BY statement, GroupByDefaultPlan uses an
// in-memory table to aggregate values.
//
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
type GroupByDefaultPlan struct {
	*SelectList
	Src plan.Plan
	By  []expression.Expression

	rows   []*groupRow
	cursor int
}

// Explain implements plan.Plan Explain interface.
func (r *GroupByDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Evaluate")
	for _, v := range r.Fields {
		w.Format(" %s,", v)
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
	Row *plan.Row
	// for one group by row, we should have a map to save aggregate value
	Args map[interface{}]interface{}
}

func (r *GroupByDefaultPlan) evalGroupKey(ctx context.Context, k []interface{}, outRow []interface{}, in []interface{}) error {
	// group by items can not contain aggregate field, so we can eval them safely.
	m := map[interface{}]interface{}{}
	m[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
		if scope == expression.IdentReferFromTable {
			return in[index], nil
		} else if scope == expression.IdentReferSelectList {
			return outRow[index], nil
		}

		// try to find in outer query
		return getIdentValueFromOuterQuery(ctx, name)
	}

	m[expression.ExprEvalPositionFunc] = func(position int) (interface{}, error) {
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
		if name == f.AsName {
			return out[i], nil
		}
	}
	return nil, errors.Errorf("unknown field %s", name)
}

func (r *GroupByDefaultPlan) evalNoneAggFields(ctx context.Context, out []interface{},
	m map[interface{}]interface{}, in []interface{}) error {
	m[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
		if scope == expression.IdentReferFromTable {
			return in[index], nil
		} else if scope == expression.IdentReferSelectList {
			return out[index], nil
		}

		// try to find in outer query
		return getIdentValueFromOuterQuery(ctx, name)
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

	m[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
		if scope == expression.IdentReferFromTable {
			return in[index], nil
		} else if scope == expression.IdentReferSelectList {
			return out[index], nil
		}

		// try to find in outer query
		return getIdentValueFromOuterQuery(ctx, name)
	}

	// Eval aggregate field results in ctx
	for i := range r.AggFields {
		// we must evaluate aggregate function only, e.g, select col1 + count(*) in (count(*)),
		// we cannot evaluate it directly here, because col1 + count(*) returns nil before AggDone phase,
		// so we don't evaluate count(*) in In expression, and will get an invalid data in AggDone phase for it.
		// mention all aggregate functions
		// TODO: optimize, we can get these aggregate functions only once and reuse
		aggs, err := expression.MentionedAggregateFuncs(r.Fields[i].Expr)
		if err != nil {
			return errors.Trace(err)
		}
		for _, agg := range aggs {
			if _, err := agg.Eval(ctx, m); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (r *GroupByDefaultPlan) evalAggDone(ctx context.Context, out []interface{},
	m map[interface{}]interface{}) error {
	m[builtin.ExprAggDone] = true

	var err error
	// Eval aggregate field results done in ctx
	for i := range r.AggFields {
		if out[i], err = r.Fields[i].Expr.Eval(ctx, m); err != nil {
			return errors.Trace(err)
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
	m := map[interface{}]interface{}{builtin.ExprEvalArgAggEmpty: true}

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

// Next implements plan.Plan Next interface.
func (r *GroupByDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.rows == nil {
		r.rows = []*groupRow{}
		err = r.fetchAll(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if r.cursor == len(r.rows) {
		return
	}

	gRow := r.rows[r.cursor]
	r.cursor++
	row = gRow.Row
	updateRowStack(ctx, row.Data, row.FromData)
	return
}

func (r *GroupByDefaultPlan) fetchAll(ctx context.Context) error {
	// TODO: now we have to use this to save group key -> row index
	// later we will serialize group by items into a string key and then use a map instead.
	t, err := memkv.CreateTemp(true)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()
	k := make([]interface{}, len(r.By))
	for {
		srcRow, err1 := r.Src.Next(ctx)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if srcRow == nil {
			break
		}
		row := &plan.Row{
			Data: make([]interface{}, len(r.Fields)),
			// must save FromData info for inner sub query use.
			FromData: srcRow.Data,
		}
		// TODO: later order by will use the same mechanism, so we may use another plan to do this
		evalArgs := map[interface{}]interface{}{}
		// must first eval none aggregate fields, because alias group by will use this.
		if err = r.evalNoneAggFields(ctx, row.Data, evalArgs, srcRow.Data); err != nil {
			return errors.Trace(err)
		}

		// update outer query becuase group by may use it if it has a subquery.
		updateRowStack(ctx, row.Data, row.FromData)

		if err = r.evalGroupKey(ctx, k, row.Data, srcRow.Data); err != nil {
			return errors.Trace(err)
		}

		// get row index with k.
		v, err1 := t.Get(k)
		if err1 != nil {
			return errors.Trace(err1)
		}

		index := 0
		if len(v) == 0 {
			// no group for key, save data for this group
			index = len(r.rows)
			r.rows = append(r.rows, &groupRow{Row: row, Args: evalArgs})

			if err = t.Set(k, []interface{}{index}); err != nil {
				return errors.Trace(err)
			}
		} else {
			// we have already saved data in the group by key, use this
			index = v[0].(int)

			// we will use this context args to evaluate aggregate fields.
			evalArgs = r.rows[index].Args
		}

		// eval aggregate fields
		if err = r.evalAggFields(ctx, row.Data, evalArgs, srcRow.Data); err != nil {
			return errors.Trace(err)
		}
	}
	if len(r.rows) == 0 {
		// empty table
		var out []interface{}
		out, err = r.evalEmptyTable(ctx)
		if err != nil || out == nil {
			return errors.Trace(err)
		}
		row := &plan.Row{Data: out}
		r.rows = append(r.rows, &groupRow{Row: row, Args: map[interface{}]interface{}{}})
	} else {
		for _, row := range r.rows {
			// eval aggregate done
			if err = r.evalAggDone(ctx, row.Row.Data, row.Args); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// Close implements plan.Plan Close interface.
func (r *GroupByDefaultPlan) Close() error {
	r.rows = nil
	r.cursor = 0
	return r.Src.Close()
}
