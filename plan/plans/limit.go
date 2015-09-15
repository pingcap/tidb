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
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*LimitDefaultPlan)(nil)
	_ plan.Plan = (*OffsetDefaultPlan)(nil)
)

// LimitDefaultPlan handles queries like SELECT .. FROM ... LIMIT N, returns at
// most N results.
type LimitDefaultPlan struct {
	Count  uint64
	cursor uint64
	Src    plan.Plan
	Fields []*field.ResultField
}

// Explain implements plan.Plan Explain interface.
func (r *LimitDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Limit %d records\n└Output field names %v\n", r.Count, r.Fields)
}

// Filter implements plan.Plan Filter interface.
func (r *LimitDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *LimitDefaultPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Next implements plan.Plan Next interface.
func (r *LimitDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.cursor == r.Count {
		return
	}
	r.cursor++
	// TODO: This is a temp solution for pass wordpress
	variable.GetSessionVars(ctx).AddFoundRows(1)
	row, err = r.Src.Next(ctx)
	return
}

// Close implements plan.Plan Close interface.
func (r *LimitDefaultPlan) Close() error {
	r.cursor = 0
	return r.Src.Close()
}

// OffsetDefaultPlan handles SELECT ... FROM ... OFFSET N, skips N records.
type OffsetDefaultPlan struct {
	Count  uint64
	cursor uint64
	Src    plan.Plan
	Fields []*field.ResultField
}

// Explain implements plan.Plan Explain interface.
func (r *OffsetDefaultPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	w.Format("┌Skip first %d records\n└Output field names %v\n", r.Count, field.RFQNames(r.Fields))
}

// Filter implements plan.Plan Filter interface.
func (r *OffsetDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *OffsetDefaultPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Next implements plan.Plan Next interface.
func (r *OffsetDefaultPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	for r.cursor < r.Count {
		_, err = r.Src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.cursor++
	}
	return r.Src.Next(ctx)
}

// Close implements plan.Plan Close interface.
func (r *OffsetDefaultPlan) Close() error {
	r.cursor = 0
	return r.Src.Close()
}
