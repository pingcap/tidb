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
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/forupdate"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*SelectLockPlan)(nil)
)

// SelectLockPlan handles SELECT ... FOR UPDATE, recording selected rows
// for acquiring locks explicitly.
type SelectLockPlan struct {
	Src  plan.Plan
	Lock coldef.LockType
}

// Do implements plan.Plan Do interface, acquiring locks.
func (r *SelectLockPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	return r.Src.Do(ctx, func(rid interface{}, in []interface{}) (bool, error) {
		var rowKeys *RowKeyList
		if in != nil && len(in) > 0 {
			t := in[len(in)-1]
			switch vt := t.(type) {
			case *RowKeyList:
				rowKeys = vt
				// Remove row key list from data tail
				in = in[:len(in)-1]
			}
		}
		if rowKeys != nil && r.Lock == coldef.SelectLockForUpdate {
			forupdate.SetForUpdate(ctx)
			txn, err := ctx.GetTxn(false)
			if err != nil {
				return false, errors.Trace(err)
			}
			for _, k := range rowKeys.Keys {
				err = txn.LockKeys([]byte(k.Key))
				if err != nil {
					return false, errors.Trace(err)
				}
			}
		}
		return f(rid, in)
	})
}

// Explain implements plan.Plan Explain interface.
func (r *SelectLockPlan) Explain(w format.Formatter) {
	r.Src.Explain(w)
	if r.Lock != coldef.SelectLockForUpdate {
		// no need to lock, just return.
		return
	}
	w.Format("┌Lock row keys for update\n")
}

// GetFields implements plan.Plan GetFields interface.
func (r *SelectLockPlan) GetFields() []*field.ResultField {
	return r.Src.GetFields()
}

// Filter implements plan.Plan Filter interface.
func (r *SelectLockPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	// Do nothing
	return r, false, nil
}

// Next implements plan.Plan Next interface.
func (r *SelectLockPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	return
}

// Close implements plan.Plan Close interface.
func (r *SelectLockPlan) Close() error {
	return nil
}
