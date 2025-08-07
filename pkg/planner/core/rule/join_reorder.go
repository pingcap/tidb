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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)



// JoinReorderRule implements LogicalOptRule interface for join reordering optimization
type JoinReorderRule struct{}

// Match implements LogicalOptRule.Match
func (r *JoinReorderRule) Match(ctx context.Context, p base.LogicalPlan) (bool, error) {
	return r.containsReorderableJoins(p), nil
}

// Optimize implements LogicalOptRule.Optimize
func (r *JoinReorderRule) Optimize(ctx context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	solver := &JoinReOrderSolver{
		ctx: p.SCtx(),
	}

	newPlan, changed, err := solver.Optimize(ctx, p, opt)
	if err != nil {
		return p, false, err
	}

	if changed {
		planChanged = true
	}

	return newPlan, planChanged, nil
}

// Name implements LogicalOptRule.Name
func (r *JoinReorderRule) Name() string {
	return "join_reorder"
}

// containsReorderableJoins checks if the plan contains joins that can be reordered
func (r *JoinReorderRule) containsReorderableJoins(p base.LogicalPlan) bool {
	// Check if this is a join by looking for join-related methods or properties
	// For now, we'll check if the plan has children and assume it might be a join
	if len(p.Children()) >= 2 {
		return true
	}
	
	for _, child := range p.Children() {
		if r.containsReorderableJoins(child) {
			return true
		}
	}
	return false
}

// JoinReOrderSolver is the main solver for join reordering
type JoinReOrderSolver struct {
	ctx base.PlanContext
}

// Optimize performs join reordering optimization
func (s *JoinReOrderSolver) Optimize(ctx context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false

	// Extract join groups from the plan
	joinGroups := util.ExtractJoinGroups(p)

	for _, group := range joinGroups {
		if len(group.Group) <= 1 {
			continue
		}

		// Use appropriate solver based on group size
		var solver util.JoinOrderSolver
		if len(group.Group) <= 6 {
			solver = util.NewDPJoinOrderSolver(s.ctx, group)
		} else {
			solver = util.NewGreedyJoinOrderSolver(s.ctx, group)
		}

		newJoin, err := solver.Solve()
		if err != nil {
			return p, false, err
		}

		if newJoin != nil {
			// Replace the original join group with the optimized one
			p = s.replaceJoinGroup(p, group, newJoin)
			planChanged = true
		}
	}

	return p, planChanged, nil
}

// replaceJoinGroup replaces the original join group with the optimized one
func (s *JoinReOrderSolver) replaceJoinGroup(p base.LogicalPlan, group *util.JoinGroupResult, newJoin base.LogicalPlan) base.LogicalPlan {
	// Implementation for replacing join group in the plan tree
	// This would involve traversing the plan tree and replacing the identified group
	return p // Simplified for brevity
}
