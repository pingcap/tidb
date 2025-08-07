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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// JoinOrderSolver interface for different join order solving algorithms
type JoinOrderSolver interface {
	Solve() (base.LogicalPlan, error)
}

// BaseSingleGroupJoinOrderSolver provides common functionality for join order solvers
type BaseSingleGroupJoinOrderSolver struct {
	ctx                base.PlanContext
	group              *JoinGroupResult
	eqEdges           []*expression.ScalarFunction
	otherConds        []expression.Expression
	joinTypes         []*JoinTypeWithExtMsg
}

// NewBaseSingleGroupJoinOrderSolver creates a new base solver
func NewBaseSingleGroupJoinOrderSolver(ctx base.PlanContext, group *JoinGroupResult) *BaseSingleGroupJoinOrderSolver {
	return &BaseSingleGroupJoinOrderSolver{
		ctx:        ctx,
		group:      group,
		eqEdges:    group.EqEdges,
		otherConds: group.OtherConds,
		joinTypes:  group.JoinTypes,
	}
}

// CheckConnection checks if two plans can be joined and returns the join conditions
func (s *BaseSingleGroupJoinOrderSolver) CheckConnection(leftPlan, rightPlan base.LogicalPlan) (
	leftNode, rightNode base.LogicalPlan, 
	usedEdges []*expression.ScalarFunction, 
	joinType *JoinTypeWithExtMsg) {
	
	leftNode = leftPlan
	rightNode = rightPlan
	usedEdges = make([]*expression.ScalarFunction, 0)
	
	// Get schema information
	leftSchema := leftPlan.Schema()
	rightSchema := rightPlan.Schema()
	
	// Find applicable equal conditions
	for _, edge := range s.eqEdges {
		if s.canApplyEqualCondition(edge, leftSchema, rightSchema) {
			usedEdges = append(usedEdges, edge)
		}
	}
	
	// Default to inner join if no specific join type is found
	joinType = &JoinTypeWithExtMsg{
		JoinType: logicalop.InnerJoin,
		ExtMsg:   "",
	}
	
	return leftNode, rightNode, usedEdges, joinType
}

// canApplyEqualCondition checks if an equal condition can be applied to the given schemas
func (s *BaseSingleGroupJoinOrderSolver) canApplyEqualCondition(
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
func (s *BaseSingleGroupJoinOrderSolver) columnsInSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

// MakeJoin creates a join between two plans with the given conditions  
func (s *BaseSingleGroupJoinOrderSolver) MakeJoin(  
	leftPlan, rightPlan base.LogicalPlan,   
	eqEdges []*expression.ScalarFunction,   
	joinType *JoinTypeWithExtMsg) (base.LogicalPlan, []expression.Expression) {  
	  
	remainOtherConds := make([]expression.Expression, len(s.otherConds))  
	copy(remainOtherConds, s.otherConds)  
	  
	var (  
		otherConds []expression.Expression  
		leftConds  []expression.Expression  
		rightConds []expression.Expression  
	)  
	  
	mergedSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())  
	  
	// Filter conditions based on which schema they belong to  
	remainOtherConds, leftConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {  
		return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())  
	})  
	remainOtherConds, rightConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {  
		return expression.ExprFromSchema(expr, rightPlan.Schema()) && !expression.ExprFromSchema(expr, leftPlan.Schema())  
	})  
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {  
		return expression.ExprFromSchema(expr, mergedSchema)  
	})  
	  
	return s.newJoinWithEdges(leftPlan, rightPlan, eqEdges, otherConds, leftConds, rightConds, joinType.JoinType), remainOtherConds  
}  

// newJoinWithEdges creates a new join with the specified conditions  
func (s *BaseSingleGroupJoinOrderSolver) newJoinWithEdges(  
	lChild, rChild base.LogicalPlan,  
	eqEdges []*expression.ScalarFunction,   
	otherConds, leftConds, rightConds []expression.Expression,   
	joinType logicalop.JoinType) base.LogicalPlan {  
	  
	newJoin := s.newCartesianJoin(lChild, rChild)  
	newJoin.EqualConditions = eqEdges  
	newJoin.OtherConditions = otherConds  
	newJoin.LeftConditions = leftConds  
	newJoin.RightConditions = rightConds  
	newJoin.JoinType = joinType  
	return newJoin  
}  
  
// newCartesianJoin creates a new cartesian join  
func (s *BaseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild base.LogicalPlan) *logicalop.LogicalJoin {  
	offset := lChild.QueryBlockOffset()  
	if offset != rChild.QueryBlockOffset() {  
		offset = -1  
	}  
	join := logicalop.LogicalJoin{  
		JoinType:  logicalop.InnerJoin,  
		Reordered: true,  
	}.Init(s.ctx, offset)  
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))  
	join.SetChildren(lChild, rChild)  
	return join  
}  
  
// BaseNodeCumCost calculates the cumulative cost of a node  
func (s *BaseSingleGroupJoinOrderSolver) BaseNodeCumCost(groupNode base.LogicalPlan) float64 {  
	cost := groupNode.StatsInfo().RowCount  
	for _, child := range groupNode.Children() {  
		cost += s.BaseNodeCumCost(child)  
	}  
	return cost  
} 