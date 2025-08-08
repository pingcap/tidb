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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/tracing"
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
	var leadingJoinGroup []base.LogicalPlan
	leftJoinGroup := make([]base.LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)
	var queryBlockNames []ast.HintTable
	if p := s.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}
	for _, hintTbl := range hintInfo.LeadingJoinOrder {
		match := false
		for i, joinGroup := range leftJoinGroup {
			tableAlias := util.ExtractTableAlias(joinGroup, joinGroup.QueryBlockOffset())
			if tableAlias == nil {
				continue
			}
			if (hintTbl.DBName.L == tableAlias.DBName.L || hintTbl.DBName.L == "*") && hintTbl.TblName.L == tableAlias.TblName.L && hintTbl.SelectOffset == tableAlias.SelectOffset {
				match = true
				leadingJoinGroup = append(leadingJoinGroup, joinGroup)
				leftJoinGroup = slices.Delete(leftJoinGroup, i, i+1)
				break
			}
		}
		if match {
			continue
		}

		// consider query block alias: select /*+ leading(t1, t2) */ * from (select ...) t1, t2 ...
		groupIdx := -1
		for i, joinGroup := range leftJoinGroup {
			blockOffset := joinGroup.QueryBlockOffset()
			if blockOffset > 1 && blockOffset < len(queryBlockNames) {
				blockName := queryBlockNames[blockOffset]
				if hintTbl.DBName.L == blockName.DBName.L && hintTbl.TblName.L == blockName.TableName.L {
					// this can happen when multiple join groups are from the same block, for example:
					//   select /*+ leading(tx) */ * from (select * from t1, t2 ...) tx, ...
					// `tx` is split to 2 join groups `t1` and `t2`, and they have the same block offset.
					// TODO: currently we skip this case for simplification, we can support it in the future.
					if groupIdx != -1 {
						groupIdx = -1
						break
					}
					groupIdx = i
				}
			}
		}
		if groupIdx != -1 {
			leadingJoinGroup = append(leadingJoinGroup, leftJoinGroup[groupIdx])
			leftJoinGroup = slices.Delete(leftJoinGroup, groupIdx, groupIdx+1)
		}
	}
	if len(leadingJoinGroup) != len(hintInfo.LeadingJoinOrder) || leadingJoinGroup == nil {
		return false, nil
	}
	leadingJoin := leadingJoinGroup[0]
	leadingJoinGroup = leadingJoinGroup[1:]
	for len(leadingJoinGroup) > 0 {
		var usedEdges []*expression.ScalarFunction
		var joinType *util.JoinTypeWithExtMsg
		leadingJoin, leadingJoinGroup[0], usedEdges, joinType = s.checkConnection(leadingJoin, leadingJoinGroup[0])
		if hasOuterJoin && usedEdges == nil {
			// If the joinGroups contain the outer join, we disable the cartesian product.
			return false, nil
		}
		leadingJoin, s.otherConds = s.makeJoin(leadingJoin, leadingJoinGroup[0], usedEdges, joinType, opt)
		leadingJoinGroup = leadingJoinGroup[1:]
	}
	s.leadingJoinGroup = leadingJoin
	return true, leftJoinGroup
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
	if t == nil || t.opt == nil || t.opt.TracerIsNil() {
		return
	}
	if len(t.initial) > 0 {
		t.final = allJoinOrderToString(extractJoinAndDataSource(p.BuildPlanTrace()))
		return
	}
	t.initial = allJoinOrderToString(extractJoinAndDataSource(p.BuildPlanTrace()))
}

// appendLogicalJoinCost appends logical join cost to trace
func (t *joinReorderTrace) appendLogicalJoinCost(join base.LogicalPlan, cost float64) {
	if t == nil || t.opt == nil || t.opt.TracerIsNil() {
		return
	}
	joinMapKey := allJoinOrderToString(extractJoinAndDataSource(join.BuildPlanTrace()))
	t.cost[joinMapKey] = cost
}

// appendJoinReorderTraceStep appends join reorder trace step
func appendJoinReorderTraceStep(tracer *joinReorderTrace, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	if len(tracer.initial) < 1 || len(tracer.final) < 1 {
		return
	}
	action := func() string {
		return fmt.Sprintf("join order becomes %v from original %v", tracer.final, tracer.initial)
	}
	reason := func() string {
		buffer := bytes.NewBufferString("join cost during reorder: [")
		var joins []string
		for join := range tracer.cost {
			joins = append(joins, join)
		}
		slices.Sort(joins)
		for i, join := range joins {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "[%s, cost:%v]", join, tracer.cost[join])
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	opt.AppendStepToCurrent(plan.ID(), plan.TP(), reason, action)
}

// allJoinOrderToString converts plan traces to string
func allJoinOrderToString(tt []*tracing.PlanTrace) string {
	if len(tt) == 1 {
		return joinOrderToString(tt[0])
	}
	buffer := bytes.NewBufferString("[")
	for i, t := range tt {
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(joinOrderToString(t))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// joinOrderToString let Join(DataSource, DataSource) become '(t1*t2)'
func joinOrderToString(t *tracing.PlanTrace) string {
	if t.TP == "Join" {
		buffer := bytes.NewBufferString("(")
		for i, child := range t.Children {
			if i > 0 {
				buffer.WriteString("*")
			}
			buffer.WriteString(joinOrderToString(child))
		}
		buffer.WriteString(")")
		return buffer.String()
	} else if t.TP == "DataSource" {
		return t.ExplainInfo[6:]
	}
	return ""
}

// extractJoinAndDataSource will only keep join and dataSource operator and remove other operators.
// For example: Proj->Join->(Proj->DataSource, DataSource) will become Join->(DataSource, DataSource)
func extractJoinAndDataSource(t *tracing.PlanTrace) []*tracing.PlanTrace {
	roots := findRoots(t)
	if len(roots) < 1 {
		return nil
	}
	rr := make([]*tracing.PlanTrace, 0, len(roots))
	for _, root := range roots {
		simplify(root)
		rr = append(rr, root)
	}
	return rr
}

// simplify only keeps Join and DataSource operators, and discard other operators.
func simplify(node *tracing.PlanTrace) {
	if len(node.Children) < 1 {
		return
	}
	for valid := false; !valid; {
		valid = true
		newChildren := make([]*tracing.PlanTrace, 0)
		for _, child := range node.Children {
			if child.TP != "DataSource" && child.TP != "Join" {
				newChildren = append(newChildren, child.Children...)
				valid = false
			} else {
				newChildren = append(newChildren, child)
			}
		}
		node.Children = newChildren
	}
	for _, child := range node.Children {
		simplify(child)
	}
}

// findRoots finds root nodes in the plan trace
func findRoots(t *tracing.PlanTrace) []*tracing.PlanTrace {
	if t.TP == "Join" || t.TP == "DataSource" {
		return []*tracing.PlanTrace{t}
	}
	r := make([]*tracing.PlanTrace, 0, 5)
	for _, child := range t.Children {
		r = append(r, findRoots(child)...)
	}
	return r
}

// joinGroupEqEdge represents an edge in the join group
type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

// joinGroupNonEqEdge represents a non-equal edge in the join group
type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
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
	adjacents := make([][]int, len(s.curJoinGroup))
	totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}
	// Build Graph for join group
	for _, cond := range eqConds {
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx, err := findNodeIndexInGroup(joinGroup, lCol)
		if err != nil {
			return nil, err
		}
		rIdx, err := findNodeIndexInGroup(joinGroup, rCol)
		if err != nil {
			return nil, err
		}
		addEqEdge(lIdx, rIdx, sf)
	}
	totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		cols := expression.ExtractColumns(cond)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			idx, err := findNodeIndexInGroup(joinGroup, col)
			if err != nil {
				return nil, err
			}
			ids = append(ids, idx)
			mask |= 1 << uint(idx)
		}
		totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
			nodeIDs:    ids,
			nodeIDMask: mask,
			expr:       cond,
		})
	}
	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	joins := make([]base.LogicalPlan, 0, len(joinGroup))
	// BFS the tree.
	for i := range joinGroup {
		if visited[i] {
			continue
		}
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		nodeIDMask := uint(0)
		for _, nodeID := range visitID2NodeID {
			nodeIDMask |= 1 << uint(nodeID)
		}
		var subNonEqEdges []joinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			// If this edge is not the subset of the current sub graph.
			if totalNonEqEdges[i].nodeIDMask&nodeIDMask != totalNonEqEdges[i].nodeIDMask {
				continue
			}
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].nodeIDs {
				newMask |= 1 << uint(nodeID2VisitID[nodeID])
			}
			totalNonEqEdges[i].nodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = slices.Delete(totalNonEqEdges, i, i+1)
		}
		// Do DP on each sub graph.
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEqEdges, subNonEqEdges, tracer)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		remainedOtherConds = append(remainedOtherConds, edge.expr)
	}
	// Build bushy tree for cartesian joins.
	return s.makeBushyJoin(joins, remainedOtherConds, tracer.opt), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (s *joinReorderDPSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VisitID []int) []int {
	queue := make([]int, 0, len(adjacents))
	queue = append(queue, startNode)
	visited[startNode] = true
	visitID := 0
	visitID2NodeID := make([]int, 0, len(adjacents))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		nodeID2VisitID[cur] = visitID
		visitID2NodeID = append(visitID2NodeID, cur)
		visitID++
		for _, next := range adjacents[cur] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}
	return visitID2NodeID
}

// dpGraph implements the dynamic programming algorithm for join reordering
func (s *joinReorderDPSolver) dpGraph(visitID2NodeID, nodeID2VisitID []int, _ []base.LogicalPlan,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	nodeCnt := uint(len(visitID2NodeID))
	bestPlan := make([]*jrNode, 1<<nodeCnt)
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := range nodeCnt {
		bestPlan[1<<i] = s.curJoinGroup[visitID2NodeID[i]]
	}
	// Enumerate the nodeBitmap from small to big, make sure that S1 must be enumerated before S2 if S1 belongs to S2.
	for nodeBitmap := uint(1); nodeBitmap < (1 << nodeCnt); nodeBitmap++ {
		if bits.OnesCount(nodeBitmap) == 1 {
			continue
		}
		// This loop can iterate all its subset.
		for sub := (nodeBitmap - 1) & nodeBitmap; sub > 0; sub = (sub - 1) & nodeBitmap {
			remain := nodeBitmap ^ sub
			if sub > remain {
				continue
			}
			// If this subset is not connected skip it.
			if bestPlan[sub] == nil || bestPlan[remain] == nil {
				continue
			}
			// Get the edge connecting the two parts.
			usedEdges, otherConds := s.nodesAreConnected(sub, remain, nodeID2VisitID, totalEqEdges, totalNonEqEdges)
			// Here we only check equal condition currently.
			if len(usedEdges) == 0 {
				continue
			}
			join, err := s.newJoinWithEdge(bestPlan[sub].p, bestPlan[remain].p, usedEdges, otherConds, tracer.opt)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(join, bestPlan[sub], bestPlan[remain])
			tracer.appendLogicalJoinCost(join, curCost)
			if bestPlan[nodeBitmap] == nil {
				bestPlan[nodeBitmap] = &jrNode{
					p:       join,
					cumCost: curCost,
				}
			} else if bestPlan[nodeBitmap].cumCost > curCost {
				bestPlan[nodeBitmap].p = join
				bestPlan[nodeBitmap].cumCost = curCost
			}
		}
	}
	return bestPlan[(1<<nodeCnt)-1].p, nil
}

// nodesAreConnected checks if two node sets are connected
func (s *joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) ([]joinGroupEqEdge, []expression.Expression) {
	var usedEqEdges []joinGroupEqEdge
	for _, edge := range totalEqEdges {
		lIdx := uint(oldPos2NewPos[edge.nodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.nodeIDs[1]])
		if ((leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0) || ((leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0) {
			usedEqEdges = append(usedEqEdges, edge)
		}
	}
	otherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		// If the result is false, means that the current group hasn't covered the columns involved in the expression.
		if edge.nodeIDMask&(leftMask|rightMask) != edge.nodeIDMask {
			continue
		}
		// Check whether this expression is only built from one side of the join.
		if edge.nodeIDMask&leftMask == 0 || edge.nodeIDMask&rightMask == 0 {
			continue
		}
		otherConds = append(otherConds, edge.expr)
	}
	return usedEqEdges, otherConds
}

// newJoinWithEdge creates a new join with edges
func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan base.LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), ast.EQ, edge.edge.GetStaticType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds, nil, nil, logicalop.InnerJoin, opt)
	_, _, err := join.RecursiveDeriveStats(nil)
	return join, err
}

// makeBushyJoin creates a bushy join from multiple plans
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan, otherConds []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]base.LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds, nil, nil, logicalop.InnerJoin, opt))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

// findNodeIndexInGroup finds the index of a node in the group
func findNodeIndexInGroup(group []base.LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
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
	
	var leadingJoinNodes []*jrNode
	if s.leadingJoinGroup != nil {
		// We have a leading hint to let some tables join first. The result is stored in the s.leadingJoinGroup.
		// We generate jrNode separately for it.
		leadingJoinNodes, err = s.generateJoinOrderNode([]base.LogicalPlan{s.leadingJoinGroup}, tracer)
		if err != nil {
			return nil, err
		}
	}
	// Sort plans by cost
	slices.SortStableFunc(s.curJoinGroup, func(i, j *jrNode) int {
		return cmp.Compare(i.cumCost, j.cumCost)
	})

	// joinNodeNum indicates the number of join nodes except leading join nodes in the current join group
	joinNodeNum := len(s.curJoinGroup)
	if leadingJoinNodes != nil {
		// The leadingJoinNodes should be the first element in the s.curJoinGroup.
		// So it can be joined first.
		leadingJoinNodes := append(leadingJoinNodes, s.curJoinGroup...)
		s.curJoinGroup = leadingJoinNodes
	}
	var cartesianGroup []base.LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree(tracer)
		if err != nil {
			return nil, err
		}
		if joinNodeNum > 0 && len(s.curJoinGroup) == joinNodeNum {
			// Getting here means that there is no join condition between the table used in the leading hint and other tables
			// For example: select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a cross join t3
			// We can not let table t3 join first.
			// TODO(hawkingrei): we find the problem in the TestHint.
			// 	`select * from t1, t2, t3 union all select /*+ leading(t3, t2) */ * from t1, t2, t3 union all select * from t1, t2, t3`
			//  this sql should not return the warning. but It will not affect the result. so we will fix it as soon as possible.
			s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table has join conditions with other tables")
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
		bestCost := math.MaxFloat64
		bestIdx, whateverValidOneIdx := -1, -1
		var finalRemainOthers, remainOthersOfWhateverValidOne []expression.Expression
		var bestJoin, whateverValidOne base.LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p, tracer.opt)
			if newJoin == nil {
				continue
			}
			_, _, err := newJoin.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			whateverValidOne = newJoin
			whateverValidOneIdx = i
			remainOthersOfWhateverValidOne = remainOthers
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			tracer.appendLogicalJoinCost(newJoin, curCost)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			if whateverValidOne == nil {
				break
			}
			// This branch is for the unexpected case.
			// We throw assertion in test env. And create a valid join to avoid wrong result in the production env.
			// intest.Assert(false, "Join reorder should find one valid join but failed.")
			bestJoin = whateverValidOne
			bestCost = math.MaxFloat64
			bestIdx = whateverValidOneIdx
			finalRemainOthers = remainOthersOfWhateverValidOne
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = slices.Delete(s.curJoinGroup, bestIdx, bestIdx+1)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

// checkConnectionAndMakeJoin checks connection and makes join
func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftPlan, rightPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, []expression.Expression) {
	leftPlan, rightPlan, usedEdges, joinType := s.checkConnection(leftPlan, rightPlan)
	if len(usedEdges) == 0 {
		return nil, nil
	}
	return s.makeJoin(leftPlan, rightPlan, usedEdges, joinType, opt)
}
