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
	"github.com/pingcap/tidb/kv"
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
	row, err = r.Src.Next(ctx)
	if row == nil || err != nil {
		return nil, errors.Trace(err)
	}
	if len(row.RowKeys) != 0 && r.Lock == coldef.SelectLockForUpdate {
		forupdate.SetForUpdate(ctx)
		txn, err := ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, k := range row.RowKeys {
			err = txn.LockKeys(kv.Key(k.Key))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return
}

// Close implements plan.Plan Close interface.
func (r *SelectLockPlan) Close() error {
	return r.Src.Close()
}
