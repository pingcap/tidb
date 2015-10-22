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
	_ plan.Plan = (*SelectFinalPlan)(nil)
)

// SelectFinalPlan sets info field for resuilt.
type SelectFinalPlan struct {
	*SelectList

	Src plan.Plan

	infered bool // If result field info is already infered
}

// Explain implements the plan.Plan Explain interface.
func (r *SelectFinalPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	if r.HiddenFieldOffset == len(r.Src.GetFields()) {
		// we have no hidden fields, can return.
		return
	}
	w.Format("┌Evaluate\n└Output field names %v\n", field.RFQNames(r.ResultFields[0:r.HiddenFieldOffset]))
}

// GetFields implements the plan.Plan GetFields interface.
func (r *SelectFinalPlan) GetFields() []*field.ResultField {
	return r.ResultFields[0:r.HiddenFieldOffset]
}

// Filter implements the plan.Plan Filter interface.
func (r *SelectFinalPlan) Filter(ctx context.Context, expr expression.Expression) (p plan.Plan, filtered bool, err error) {
	return r, false, nil
}

// Next implements plan.Plan Next interface.
func (r *SelectFinalPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	// push a new rowStackItem into RowStack
	pushRowStack(ctx, r.SelectList.ResultFields, r.SelectList.FromFields)

	row, err = r.Src.Next(ctx)

	// pop the rowStackItem
	if errPop := popRowStack(ctx); errPop != nil {
		return nil, errors.Wrap(err, errPop)
	}

	if row == nil || err != nil {
		return nil, errors.Trace(err)
	}

	// we should not output hidden fields to client.
	row.Data = row.Data[:r.HiddenFieldOffset]
	for i, o := range row.Data {
		d := types.RawData(o)
		switch v := d.(type) {
		case bool:
			// Convert bool field to int
			if v {
				row.Data[i] = uint8(1)
			} else {
				row.Data[i] = uint8(0)
			}
		default:
			row.Data[i] = d
		}
	}
	if !r.infered {
		setResultFieldInfo(r.ResultFields[0:r.HiddenFieldOffset], row.Data)
		r.infered = true
	}
	return
}

// Close implements plan.Plan Close interface.
func (r *SelectFinalPlan) Close() error {
	r.infered = false
	return r.Src.Close()
}
