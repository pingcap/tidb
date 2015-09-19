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
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*DistinctDefaultPlan)(nil)
)

// DistinctDefaultPlan e.g. SELECT distinct(id) FROM t;
type DistinctDefaultPlan struct {
	*SelectList
	Src    plan.Plan
	rows   []*plan.Row
	cursor int
}

// Explain implements the plan.Plan Explain interface.
func (r *DistinctDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Compute distinct rows\n└Output field names %v\n", r.ResultFields)
}

// Filter implements the plan.Plan Filter interface.
func (r *DistinctDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// Next implements plan.Plan Next interface.
func (r *DistinctDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.rows == nil {
		err = r.fetchAll(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if r.cursor == len(r.rows) {
		return
	}
	row = r.rows[r.cursor]
	updateRowStack(ctx, row.Data, row.FromData)
	r.cursor++
	return
}

func (r *DistinctDefaultPlan) fetchAll(ctx context.Context) error {
	t, err := memkv.CreateTemp(true)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if derr := t.Drop(); derr != nil && err == nil {
			err = derr
		}
	}()
	for {
		row, err := r.Src.Next(ctx)
		if row == nil || err != nil {
			return errors.Trace(err)
		}
		var v []interface{}
		// get distinct key
		key := row.Data[0:r.HiddenFieldOffset]
		v, err = t.Get(key)
		if err != nil {
			return errors.Trace(err)
		}

		if len(v) == 0 {
			// no group for key, save data for this group
			r.rows = append(r.rows, row)
			if err := t.Set(key, []interface{}{true}); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// Close implements plan.Plan Close interface.
func (r *DistinctDefaultPlan) Close() error {
	r.rows = nil
	r.cursor = 0
	return r.Src.Close()
}
