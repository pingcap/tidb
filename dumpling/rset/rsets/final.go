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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
)

var (
	_ plan.Planner = (*SelectFinalRset)(nil)
)

// SelectFinalRset is record set to select final fields after where/group by/order by/... proccessing, like `select c1, c2 from t where c1 > 1`,
// SelectFinalRset gets c1 and c2 column data value after where condition filter.
type SelectFinalRset struct {
	Src        plan.Plan
	SelectList *plans.SelectList
}

// Plan gets SelectFinalPlan.
func (r *SelectFinalRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.SelectFinalPlan{Src: r.Src, SelectList: r.SelectList}, nil
}
