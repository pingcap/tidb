// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	plannercore "github.com/pingcap/tidb/planner/core"
)

// Operand is the node of a pattern tree, it represents a logical expression operator.
// Different from logical plan operator which holds the full information about an expression
// operator, Operand only stores the type information.
// An Operand may correspond to a concrete logical plan operator, or it can has special meaning,
// e.g, a placeholder for any logical plan operator.
type Operand int

const (
	OperandAny Operand = iota
	OperandJoin
	OperandAggregation
	OperandProjection
	OperandSelection
	OperandApply
	OperandMaxOneRow
	OperandTableDual
	OperandDataSource
	OperandUnionScan
	OperandUnionAll
	OperandSort
	OperandTopN
	OperandLock
	OperandLimit
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p plannercore.LogicalPlan) (Operand, error) {
	switch x := p.(type) {
	case *plannercore.LogicalJoin:
		return OperandJoin, nil
	case *plannercore.LogicalAggregation:
		return OperandAggregation, nil
	case *plannercore.LogicalProjection:
		return OperandProjection, nil
	case *plannercore.LogicalSelection:
		return OperandSelection, nil
	case *plannercore.LogicalApply:
		return OperandApply, nil
	case *plannercore.LogicalMaxOneRow:
		return OperandMaxOneRow, nil
	case *plannercore.LogicalTableDual:
		return OperandTableDual, nil
	case *plannercore.DataSource:
		return OperandDataSource, nil
	case *plannercore.LogicalUnionScan:
		return OperandUnionScan, nil
	case *plannercore.LogicalUnionAll:
		return OperandUnionAll, nil
	case *plannercore.LogicalSort:
		return OperandSort, nil
	case *plannercore.LogicalTopN:
		return OperandTopN, nil
	case *plannercore.LogicalLock:
		return OperandLock, nil
	case *plannercore.LogicalLimit:
		return OperandLimit, nil
	}
	return OperandUnsupported, plannercore.ErrUnsupportedType.GenWithStack("Unsupported LogicalPlan(%T) for GetOperand", x)
}

// match checks if current Operand matches specified one.
func (o Operand) match(t Operand) bool {
	if o == OperandAny || t == OperandAny {
		return true
	}
	if o == t {
		return true
	}
	return false
}

// Pattern defines the match pattern for a rule.
// It describes a piece of logical expression.
// It's a tree-like structure and each node in the tree is an Operand.
type Pattern struct {
	operand  Operand
	children []*Pattern
}

// BuildPattern builds a Pattern from Operand and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, children ...*Pattern) *Pattern {
	p := &Pattern{operand: operand}
	p.children = children
	return p
}
