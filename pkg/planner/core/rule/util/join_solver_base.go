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
)

// JoinOrderSolver interface for different join order solving algorithms
type JoinOrderSolver interface {
	Solve() (interface{}, error)
}

// BaseSingleGroupJoinOrderSolver provides common functionality for join order solvers
type BaseSingleGroupJoinOrderSolver struct {
	ctx                interface{}
	group              *JoinGroupResult
	eqEdges           []*expression.ScalarFunction
	otherConds        []expression.Expression
	joinTypes         []*JoinTypeWithExtMsg
	createJoin         func(lChild, rChild interface{}, eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType int) interface{}
}

// NewBaseSingleGroupJoinOrderSolver creates a new base solver
func NewBaseSingleGroupJoinOrderSolver(ctx interface{}, group *JoinGroupResult) *BaseSingleGroupJoinOrderSolver {
	return &BaseSingleGroupJoinOrderSolver{
		ctx:        ctx,
		group:      group,
		eqEdges:    group.EqEdges,
		otherConds: group.OtherConds,
		joinTypes:  group.JoinTypes,
	}
}

// CheckConnection checks if two plans can be joined and returns the join conditions
func (s *BaseSingleGroupJoinOrderSolver) CheckConnection(leftPlan, rightPlan interface{}) (
	leftNode, rightNode interface{}, 
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
		JoinType: 0, // 0 = InnerJoin
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
	leftPlan, rightPlan interface{},   
	eqEdges []*expression.ScalarFunction,   
	joinType *JoinTypeWithExtMsg) (interface{}, []expression.Expression) {  
	  
	remainOtherConds := make([]expression.Expression, len(s.otherConds))  
	copy(remainOtherConds, s.otherConds)  
	  
	var (  
		otherConds []expression.Expression  
		leftConds  []expression.Expression  
		rightConds []expression.Expression  
	)  
	  
	// For now, return a simplified implementation
	// TODO: Implement full join creation logic
	return leftPlan, remainOtherConds  
}  

// newJoinWithEdges creates a new join with the specified conditions  
func (s *BaseSingleGroupJoinOrderSolver) newJoinWithEdges(
	lChild, rChild interface{},
	eqEdges []*expression.ScalarFunction,
	otherConds, leftConds, rightConds []expression.Expression,
	joinType int) interface{} {  
	  
	// Create a new join using the factory function
	// This avoids direct import of logicalop package
	return s.createJoin(lChild, rChild, eqEdges, otherConds, leftConds, rightConds, joinType)
}  
  
// newCartesianJoin creates a new cartesian join  
func (s *BaseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild interface{}) interface{} {  
	// Create a cartesian join using the factory function
	return s.createJoin(lChild, rChild, nil, nil, nil, nil, 0) // 0 = InnerJoin
}

// createJoin is a factory function that creates joins
// This will be overridden by the actual implementation in the rule package
func (s *BaseSingleGroupJoinOrderSolver) createJoin(
	lChild, rChild interface{},
	eqEdges []*expression.ScalarFunction,
	otherConds, leftConds, rightConds []expression.Expression,
	joinType int) interface{} {
	
	// Default implementation - return left child as fallback
	// The actual implementation should be provided by the rule package
	return lChild
}

// SetJoinFactory sets the join factory function
func (s *BaseSingleGroupJoinOrderSolver) SetJoinFactory(factory func(
	lChild, rChild interface{},
	eqEdges []*expression.ScalarFunction,
	otherConds, leftConds, rightConds []expression.Expression,
	joinType int) interface{}) {
	s.createJoin = factory
}  
  
// BaseNodeCumCost calculates the cumulative cost of a node  
func (s *BaseSingleGroupJoinOrderSolver) BaseNodeCumCost(groupNode interface{}) float64 {  
	// For now, return a simplified implementation
	// TODO: Implement full cost calculation logic
	return 1.0
} 