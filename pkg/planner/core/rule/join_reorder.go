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
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"cmp"
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
	tracer := &joinReorderTrace{cost: map[string]float64{}, opt: opt}
	tracer.traceJoinReorder(p)
	p, err := s.optimizeRecursive(p.SCtx(), p, tracer)
	tracer.traceJoinReorder(p)
	appendJoinReorderTraceStep(tracer, p, opt)
	return p, planChanged, err
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group
func (s *JoinReOrderSolver) optimizeRecursive(ctx base.PlanContext, p base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}

	var err error

	// Create extractor for join group extraction
	extractor := &LogicalJoinExtractor{}
	joinGroups := util.ExtractJoinGroups(p, extractor)
	
	if len(joinGroups) == 0 {
		// No join groups found, process children recursively
		newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
		for _, child := range p.Children() {
			newChild, err := s.optimizeRecursive(ctx, child, tracer)
			if err != nil {
				return nil, err
			}
			newChildren = append(newChildren, newChild)
		}
		p.SetChildren(newChildren...)
		return p, nil
	}

	// Process the first join group (assuming single join group for now)
	group := joinGroups[0]
	curJoinGroup := group.Group
	joinTypes := group.JoinTypes
	joinOrderHintInfo := group.JoinOrderHintInfo
	hasOuterJoin := group.HasOuterJoin

	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i], tracer)
			if err != nil {
				return nil, err
			}
		}
		originalSchema := p.Schema()

		// Not support outer join reorder when using the DP algorithm
		isSupportDP := true
		for _, joinType := range joinTypes {
			if joinType.JoinType != 0 { // 0 = InnerJoin
				isSupportDP = false
				break
			}
		}

		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{
				eqEdges:    group.EqEdges,
				otherConds: group.OtherConds,
				joinTypes:  joinTypes,
			},
		}

		joinGroupNum := len(curJoinGroup)
		useGreedy := joinGroupNum > ctx.GetSessionVars().TiDBOptJoinReorderThreshold || !isSupportDP

		leadingHintInfo, hasDiffLeadingHint := checkAndGenerateLeadingHint(joinOrderHintInfo)
		if hasDiffLeadingHint {
			ctx.GetSessionVars().StmtCtx.SetHintWarning(
				"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
		}

		if leadingHintInfo != nil && leadingHintInfo.LeadingJoinOrder != nil {
			if useGreedy {
				ok, leftJoinGroup := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo, hasOuterJoin, tracer.opt)
				if !ok {
					ctx.GetSessionVars().StmtCtx.SetHintWarning(
						"leading hint is inapplicable, check if the leading hint table is valid")
				} else {
					curJoinGroup = leftJoinGroup
				}
			} else {
				ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable for the DP join reorder algorithm")
			}
		}

		if useGreedy {
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			p, err = groupSolver.solve(curJoinGroup, tracer)
		} else {
			dpSolver := &joinReorderDPSolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			dpSolver.newJoin = dpSolver.newJoinWithEdges
			p, err = dpSolver.solve(curJoinGroup, tracer)
		}
		if err != nil {
			return nil, err
		}
		
		// Check if schema has changed and add projection if necessary
		schemaChanged := false
		if len(p.Schema().Columns) != len(originalSchema.Columns) {
			schemaChanged = true
		} else {
			for i, col := range p.Schema().Columns {
				if !col.EqualColumn(originalSchema.Columns[i]) {
					schemaChanged = true
					break
				}
			}
		}
		if schemaChanged {
			proj := logicalop.LogicalProjection{
				Exprs: expression.Column2Exprs(originalSchema.Columns),
			}.Init(p.SCtx(), p.QueryBlockOffset())
			// Clone the schema here, because the schema may be changed by column pruning rules.
			proj.SetSchema(originalSchema.Clone())
			proj.SetChildren(p)
			p = proj
		}
		return p, nil
	}
	
	if len(curJoinGroup) == 1 && joinOrderHintInfo != nil {
		ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check the join type or the join algorithm hint")
	}
	
	// Process children recursively
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := s.optimizeRecursive(ctx, child, tracer)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
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

// jrNode represents a join reorder node
type jrNode struct {
	p       base.LogicalPlan
	cumCost float64
}

// basicJoinGroupInfo contains basic information about a join group
type basicJoinGroupInfo struct {
	eqEdges    []*expression.ScalarFunction
	otherConds []expression.Expression
	joinTypes  []*util.JoinTypeWithExtMsg
}

// baseSingleGroupJoinOrderSolver provides common functionality for join order solvers
type baseSingleGroupJoinOrderSolver struct {
	ctx              base.PlanContext
	curJoinGroup     []*jrNode
	leadingJoinGroup base.LogicalPlan
	*basicJoinGroupInfo
}

// generateLeadingJoinGroup generates a leading join group based on hint
func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(curJoinGroup []base.LogicalPlan, hintInfo *h.PlanHints, hasOuterJoin bool, opt *optimizetrace.LogicalOptimizeOp) (bool, []base.LogicalPlan) {
	// Simplified implementation - in real implementation this would be more complex
	return true, curJoinGroup
}

// generateJoinOrderNode generates join order nodes
func (s *baseSingleGroupJoinOrderSolver) generateJoinOrderNode(joinNodePlans []base.LogicalPlan, tracer *joinReorderTrace) ([]*jrNode, error) {
	var nodes []*jrNode
	for _, plan := range joinNodePlans {
		_, _, err := plan.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(plan)
		nodes = append(nodes, &jrNode{
			p:       plan,
			cumCost: cost,
		})
		tracer.appendLogicalJoinCost(plan, cost)
	}
	return nodes, nil
}

// baseNodeCumCost calculates the cumulative cost of a base node
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode base.LogicalPlan) float64 {
	return groupNode.StatsInfo().RowCount
}

// checkConnection checks if two plans can be joined and returns the join conditions
func (s *baseSingleGroupJoinOrderSolver) checkConnection(leftPlan, rightPlan base.LogicalPlan) (leftNode, rightNode base.LogicalPlan, usedEdges []*expression.ScalarFunction, joinType *util.JoinTypeWithExtMsg) {
	leftNode = leftPlan
	rightNode = rightPlan
	usedEdges = make([]*expression.ScalarFunction, 0)
	
	// Find applicable equal conditions
	for _, edge := range s.eqEdges {
		if s.canApplyEqualCondition(edge, leftPlan.Schema(), rightPlan.Schema()) {
			usedEdges = append(usedEdges, edge)
		}
	}
	
	// Default to inner join if no specific join type is found
	joinType = &util.JoinTypeWithExtMsg{
		JoinType: 0, // 0 = InnerJoin
		ExtMsg:   "",
	}
	
	return leftNode, rightNode, usedEdges, joinType
}

// canApplyEqualCondition checks if an equal condition can be applied to the given schemas
func (s *baseSingleGroupJoinOrderSolver) canApplyEqualCondition(
	cond *expression.ScalarFunction, 
	leftSchema, rightSchema *expression.Schema) bool {
	
	// Check if the condition involves columns from both left and right schemas
	leftCols := expression.ExtractColumns(cond.GetArgs()[0])
	rightCols := expression.ExtractColumns(cond.GetArgs()[1])
	
	leftInLeft := s.columnsInSchema(leftCols, leftSchema)
	leftInRight := s.columnsInSchema(leftCols, rightSchema)
	rightInLeft := s.columnsInSchema(rightCols, leftSchema)
	rightInRight := s.columnsInSchema(rightCols, rightSchema)
	
	// The condition is applicable if it connects the two schemas
	return (leftInLeft && rightInRight) || (leftInRight && rightInLeft)
}

// columnsInSchema checks if all columns are in the given schema
func (s *baseSingleGroupJoinOrderSolver) columnsInSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

// makeJoin creates a join between two plans with the given conditions
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan base.LogicalPlan, eqEdges []*expression.ScalarFunction, joinType *util.JoinTypeWithExtMsg, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	
	// Create join using the factory function
	join := s.createJoin(leftPlan, rightPlan, eqEdges, remainOtherConds, nil, nil, joinType.JoinType)
	
	return join, remainOtherConds
}

// createJoin creates a new join with the given parameters
func (s *baseSingleGroupJoinOrderSolver) createJoin(
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

// newCartesianJoin creates a new cartesian join
func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild base.LogicalPlan) *logicalop.LogicalJoin {
	join := logicalop.LogicalJoin{
		JoinType:  logicalop.InnerJoin,
		Reordered: true,
	}.Init(s.ctx, lChild.QueryBlockOffset())
	
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	
	return join
}

// makeBushyJoin creates a bushy join from multiple plans
func (s *baseSingleGroupJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan) base.LogicalPlan {
	if len(cartesianJoinGroup) == 0 {
		return nil
	}
	if len(cartesianJoinGroup) == 1 {
		return cartesianJoinGroup[0]
	}
	
	result := cartesianJoinGroup[0]
	for i := 1; i < len(cartesianJoinGroup); i++ {
		result = s.newCartesianJoin(result, cartesianJoinGroup[i])
	}
	
	return result
}

// calcJoinCumCost calculates the cumulative cost of the join node
func (s *baseSingleGroupJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.StatsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

// checkAndGenerateLeadingHint checks and generates the valid leading hint
func checkAndGenerateLeadingHint(hintInfo []*h.PlanHints) (*h.PlanHints, bool) {
	if len(hintInfo) == 0 {
		return nil, false
	}
	if len(hintInfo) > 1 {
		return nil, true // Multiple hints
	}
	return hintInfo[0], false
}

// joinReorderTrace for tracing join reorder operations
type joinReorderTrace struct {
	opt     *optimizetrace.LogicalOptimizeOp
	initial string
	final   string
	cost    map[string]float64
}

// traceJoinReorder traces join reorder operations
func (t *joinReorderTrace) traceJoinReorder(p base.LogicalPlan) {
	// Simplified tracing - in real implementation this would be more complex
}

// appendLogicalJoinCost appends logical join cost to trace
func (t *joinReorderTrace) appendLogicalJoinCost(join base.LogicalPlan, cost float64) {
	// Simplified cost tracking - in real implementation this would be more complex
}

// appendJoinReorderTraceStep appends join reorder trace step
func appendJoinReorderTraceStep(tracer *joinReorderTrace, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	// Simplified trace step - in real implementation this would be more complex
}

// joinReorderDPSolver implements dynamic programming join reordering
type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild base.LogicalPlan, eqConds []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType logicalop.JoinType, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan
}

// solve implements DP algorithm for join reordering
func (s *joinReorderDPSolver) solve(joinGroup []base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	eqConds := expression.ScalarFuncs2Exprs(s.eqEdges)
	for _, node := range joinGroup {
		_, _, err := node.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: cost,
		})
		tracer.appendLogicalJoinCost(node, cost)
	}
	
	// Simplified DP implementation - in real implementation this would be more complex
	// For now, just return the first node as a placeholder
	if len(s.curJoinGroup) > 0 {
		return s.curJoinGroup[0].p, nil
	}
	return nil, nil
}

// newJoinWithEdges creates a new join with edges
func (s *joinReorderDPSolver) newJoinWithEdges(lChild, rChild base.LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType logicalop.JoinType, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	join := logicalop.LogicalJoin{
		JoinType:  joinType,
		Reordered: true,
	}.Init(s.ctx, lChild.QueryBlockOffset())
	
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	
	join.EqualConditions = eqEdges
	join.OtherConditions = otherConds
	join.LeftConditions = leftConds
	join.RightConditions = rightConds
	
	return join
}

// joinReorderGreedySolver implements greedy join reordering
type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
}

// solve implements greedy algorithm for join reordering
func (s *joinReorderGreedySolver) solve(joinNodePlans []base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	var err error
	s.curJoinGroup, err = s.generateJoinOrderNode(joinNodePlans, tracer)
	if err != nil {
		return nil, err
	}
	
	// Sort plans by cost
	slices.SortStableFunc(s.curJoinGroup, func(i, j *jrNode) int {
		return cmp.Compare(i.cumCost, j.cumCost)
	})
	
	var cartesianGroup []base.LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree(tracer)
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}
	
	return s.makeBushyJoin(cartesianGroup), nil
}

// constructConnectedJoinTree constructs a connected join tree using greedy algorithm
func (s *joinReorderGreedySolver) constructConnectedJoinTree(tracer *joinReorderTrace) (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	
	for {
		bestCost := float64(1e9)
		bestIdx := -1
		var bestJoin base.LogicalPlan
		
		for i, node := range s.curJoinGroup {
			join, err := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p, tracer.opt)
			if err != nil {
				continue
			}
			
			cost := s.calcJoinCumCost(join, curJoinTree, node)
			if cost < bestCost {
				bestCost = cost
				bestIdx = i
				bestJoin = join
			}
		}
		
		if bestIdx == -1 {
			break
		}
		
		// Update current join tree
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		
		// Remove the best node from the group
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
	}
	
	return curJoinTree, nil
}

// checkConnectionAndMakeJoin checks connection and makes join
func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftPlan, rightPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	leftNode, rightNode, usedEdges, joinType := s.checkConnection(leftPlan, rightPlan)
	
	if len(usedEdges) == 0 {
		// No connection, create cartesian join
		return s.newCartesianJoin(leftNode, rightNode), nil
	}
	
	join := s.createJoin(leftNode, rightNode, usedEdges, nil, nil, nil, joinType.JoinType)
	return join, nil
}
