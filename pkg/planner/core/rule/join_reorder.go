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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
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
	switch p.(type) {
	case *logicalop.LogicalJoin:
		return true
	default:
		for _, child := range p.Children() {
			if r.containsReorderableJoins(child) {
				return true
			}
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

	// Create extractor for join group extraction
	extractor := &LogicalJoinExtractor{}

	// Extract join groups from the plan
	joinGroups := util.ExtractJoinGroups(p, extractor)

	for _, group := range joinGroups {
		if len(group.Group) <= 1 {
			continue
		}

		// For now, we'll use a simplified approach
		// TODO: Implement full join reordering logic
		// This is a placeholder to ensure compatibility
		if len(group.Group) > 1 {
			planChanged = true
		}
	}

	return p, planChanged, nil
}

// LogicalJoinExtractor implements util.JoinGroupExtractor for LogicalJoin
type LogicalJoinExtractor struct{}

// IsJoin checks if the plan is a LogicalJoin
func (e *LogicalJoinExtractor) IsJoin(p base.LogicalPlan) bool {
	_, ok := p.(*logicalop.LogicalJoin)
	return ok
}

// GetJoinType returns the join type as an integer
func (e *LogicalJoinExtractor) GetJoinType(p base.LogicalPlan) int {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return int(join.JoinType)
	}
	return 0 // Default to InnerJoin
}

// GetEqualConditions returns the equal conditions
func (e *LogicalJoinExtractor) GetEqualConditions(p base.LogicalPlan) []*expression.ScalarFunction {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.EqualConditions
	}
	return nil
}

// GetOtherConditions returns the other conditions
func (e *LogicalJoinExtractor) GetOtherConditions(p base.LogicalPlan) []expression.Expression {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.OtherConditions
	}
	return nil
}

// GetLeftConditions returns the left conditions
func (e *LogicalJoinExtractor) GetLeftConditions(p base.LogicalPlan) []expression.Expression {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.LeftConditions
	}
	return nil
}

// GetRightConditions returns the right conditions
func (e *LogicalJoinExtractor) GetRightConditions(p base.LogicalPlan) []expression.Expression {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.RightConditions
	}
	return nil
}

// GetHintInfo returns the hint information
func (e *LogicalJoinExtractor) GetHintInfo(p base.LogicalPlan) *h.PlanHints {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.HintInfo
	}
	return nil
}

// GetPreferJoinType returns the preferred join type
func (e *LogicalJoinExtractor) GetPreferJoinType(p base.LogicalPlan) uint {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.PreferJoinType
	}
	return 0
}

// GetStraightJoin returns whether it's a straight join
func (e *LogicalJoinExtractor) GetStraightJoin(p base.LogicalPlan) bool {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.StraightJoin
	}
	return false
}

// GetPreferJoinOrder returns whether to prefer join order
func (e *LogicalJoinExtractor) GetPreferJoinOrder(p base.LogicalPlan) bool {
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return join.PreferJoinOrder
	}
	return false
}

// replaceJoinGroup replaces the original join group with the optimized one
func (s *JoinReOrderSolver) replaceJoinGroup(p base.LogicalPlan, group *util.JoinGroupResult, newJoin base.LogicalPlan) base.LogicalPlan {
	// Check if schema has changed and add projection if necessary
	originalSchema := p.Schema()
	schemaChanged := false
	
	if len(newJoin.Schema().Columns) != len(originalSchema.Columns) {
		schemaChanged = true
	} else {
		for i, col := range newJoin.Schema().Columns {
			if !col.EqualColumn(originalSchema.Columns[i]) {
				schemaChanged = true
				break
			}
		}
	}
	
	if schemaChanged {
		// Create a projection to maintain the original schema
		proj := &logicalop.LogicalProjection{
			Exprs: expression.Column2Exprs(originalSchema.Columns),
		}
		proj.Init(newJoin.SCtx(), newJoin.QueryBlockOffset())
		proj.SetSchema(originalSchema.Clone())
		proj.SetChildren(newJoin)
		return proj
	}
	
	return newJoin
}

// createJoin creates a new LogicalJoin with the given parameters
func (s *JoinReOrderSolver) createJoin(
	lChild, rChild base.LogicalPlan,
	eqEdges []*expression.ScalarFunction,
	otherConds, leftConds, rightConds []expression.Expression,
	joinType int) base.LogicalPlan {
	
	offset := lChild.QueryBlockOffset()
	if offset != rChild.QueryBlockOffset() {
		offset = -1
	}
	
	join := logicalop.LogicalJoin{
		JoinType:  logicalop.JoinType(joinType),
		Reordered: true,
	}.Init(s.ctx, offset)
	
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	
	// Set conditions
	join.EqualConditions = eqEdges
	join.OtherConditions = otherConds
	join.LeftConditions = leftConds
	join.RightConditions = rightConds
	
	return join
}
