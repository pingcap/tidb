// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// ClonePhysicalPlan clones physical plans.
func ClonePhysicalPlan(sctx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, error) {
	cloned := make([]base.PhysicalPlan, 0, len(plans))
	for _, p := range plans {
		c, err := p.Clone(sctx)
		if err != nil {
			return nil, err
		}
		cloned = append(cloned, c)
	}
	return cloned, nil
}

// FlattenTreePlan flattens a plan tree to a list.
func FlattenTreePlan(plan base.PhysicalPlan, plans []base.PhysicalPlan) []base.PhysicalPlan {
	plans = append(plans, plan)
	for _, child := range plan.Children() {
		plans = FlattenTreePlan(child, plans)
	}
	return plans
}

// FlattenPushDownPlan converts a plan tree to a list, whose head is the leaf node like table scan.
func FlattenPushDownPlan(p base.PhysicalPlan) []base.PhysicalPlan {
	plans := make([]base.PhysicalPlan, 0, 5)
	plans = FlattenTreePlan(p, plans)
	for i := range len(plans) / 2 {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}

// AppendChildCandidate appends child candidate to the optimizer trace.
func AppendChildCandidate(origin base.PhysicalPlan, pp base.PhysicalPlan, op *optimizetrace.PhysicalOptimizeOp) {
	candidate := &tracing.CandidatePlanTrace{
		PlanTrace: &tracing.PlanTrace{
			ID:          pp.ID(),
			TP:          pp.TP(),
			ExplainInfo: pp.ExplainInfo(),
			// TODO: trace the cost
		},
	}
	op.AppendCandidate(candidate)
	pp.AppendChildCandidate(op)
	op.GetTracer().Candidates[origin.ID()].AppendChildrenID(pp.ID())
}

// GetTblStats returns the tbl-stats of this plan, which contains all columns before pruning.
func GetTblStats(copTaskPlan base.PhysicalPlan) *statistics.HistColl {
	switch x := copTaskPlan.(type) {
	case *PhysicalTableScan:
		return x.TblColHists
	case *PhysicalIndexScan:
		return x.TblColHists
	default:
		return GetTblStats(copTaskPlan.Children()[0])
	}
}
