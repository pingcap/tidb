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

	// Extract join groups from the plan
	joinGroups := s.extractJoinGroups(p)

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

// replaceJoinGroup replaces the original join group with the optimized one
func (s *JoinReOrderSolver) replaceJoinGroup(p base.LogicalPlan, group *JoinGroupResult, newJoin base.LogicalPlan) base.LogicalPlan {
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

// JoinGroupResult represents the result of extracting a join group
type JoinGroupResult struct {
	Group              []base.LogicalPlan
	EqEdges            []*expression.ScalarFunction
	OtherConds         []expression.Expression
	JoinTypes          []*JoinTypeWithExtMsg
	JoinOrderHintInfo  []*h.PlanHints
	JoinMethodHintInfo map[int]*JoinMethodHint
	HasOuterJoin       bool
}

// JoinTypeWithExtMsg represents join type with extended message
type JoinTypeWithExtMsg struct {
	JoinType int
	ExtMsg   string
}

// JoinMethodHint represents join method hint information
type JoinMethodHint struct {
	Method string
	Tables []string
}

// extractJoinGroups extracts all join groups from the given plan
func (s *JoinReOrderSolver) extractJoinGroups(p base.LogicalPlan) []*JoinGroupResult {
	var groups []*JoinGroupResult
	s.extractJoinGroupsRecursive(p, &groups)
	return groups
}

// extractJoinGroupsRecursive recursively extracts join groups
func (s *JoinReOrderSolver) extractJoinGroupsRecursive(p base.LogicalPlan, groups *[]*JoinGroupResult) {
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		group := s.extractJoinGroup(p)
		if group != nil && len(group.Group) > 1 {
			*groups = append(*groups, group)
		}
	default:
		for _, child := range p.Children() {
			s.extractJoinGroupsRecursive(child, groups)
		}
	}
}

// extractJoinGroup extracts a single join group starting from the given join node
func (s *JoinReOrderSolver) extractJoinGroup(p base.LogicalPlan) *JoinGroupResult {
	join, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return nil
	}

	group := &JoinGroupResult{
		Group:              make([]base.LogicalPlan, 0),
		EqEdges:            make([]*expression.ScalarFunction, 0),
		OtherConds:         make([]expression.Expression, 0),
		JoinTypes:          make([]*JoinTypeWithExtMsg, 0),
		JoinOrderHintInfo:  make([]*h.PlanHints, 0),
		JoinMethodHintInfo: make(map[int]*JoinMethodHint),
		HasOuterJoin:       false,
	}

	s.extractJoinGroupRecursive(p, group)
	return group
}

// extractJoinGroupRecursive recursively extracts join information
func (s *JoinReOrderSolver) extractJoinGroupRecursive(p base.LogicalPlan, group *JoinGroupResult) {
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		// Check if this is an outer join
		if x.JoinType != logicalop.InnerJoin {
			group.HasOuterJoin = true
		}

		// Add join type information
		joinType := &JoinTypeWithExtMsg{
			JoinType: int(x.JoinType),
			ExtMsg:   "",
		}
		group.JoinTypes = append(group.JoinTypes, joinType)

		// Extract equal conditions and other conditions
		for _, cond := range x.EqualConditions {
			group.EqEdges = append(group.EqEdges, cond)
		}
		for _, cond := range x.OtherConditions {
			group.OtherConds = append(group.OtherConds, cond)
		}

		// Recursively process children
		s.extractJoinGroupRecursive(x.Children()[0], group)
		s.extractJoinGroupRecursive(x.Children()[1], group)

	default:
		// This is a leaf node (table or other operator)
		group.Group = append(group.Group, p)
	}
}
