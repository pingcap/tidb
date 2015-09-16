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
	_ plan.Planner = (*OuterQueryRset)(nil)
)

// OuterQueryRset is to generate OuterQueryPlan,
// so that sub query can fetch value from the table reference from outer query.
type OuterQueryRset struct {
	Src               plan.Plan
	SrcPhase          int
	HiddenFieldOffset int
}

// Plan gets OuterQueryPlan.
func (r *OuterQueryRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.OuterQueryPlan{Src: r.Src, SrcPhase: r.SrcPhase, HiddenFieldOffset: r.HiddenFieldOffset}, nil
}
