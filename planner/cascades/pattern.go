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
	// OperandAny is a placeholder for any Operand.
	OperandAny Operand = iota
	// OperandJoin for LogicalJoin.
	OperandJoin
	// OperandAggregation for LogicalAggregation.
	OperandAggregation
	// OperandProjection for LogicalProjection.
	OperandProjection
	// OperandSelection for LogicalSelection.
	OperandSelection
	// OperandApply for LogicalApply.
	OperandApply
	// OperandMaxOneRow for LogicalMaxOneRow.
	OperandMaxOneRow
	// OperandTableDual for LogicalTableDual.
	OperandTableDual
	// OperandDataSource for DataSource.
	OperandDataSource
	// OperandUnionScan for LogicalUnionScan.
	OperandUnionScan
	// OperandUnionAll for LogicalUnionAll.
	OperandUnionAll
	// OperandSort for LogicalSort.
	OperandSort
	// OperandTopN for LogicalTopN.
	OperandTopN
	// OperandLock for LogicalLock.
	OperandLock
	// OperandLimit for LogicalLimit.
	OperandLimit
	// OperandUnsupported is upper bound of defined Operand yet.
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p plannercore.LogicalPlan) Operand {
	switch p.(type) {
	case *plannercore.LogicalJoin:
		return OperandJoin
	case *plannercore.LogicalAggregation:
		return OperandAggregation
	case *plannercore.LogicalProjection:
		return OperandProjection
	case *plannercore.LogicalSelection:
		return OperandSelection
	case *plannercore.LogicalApply:
		return OperandApply
	case *plannercore.LogicalMaxOneRow:
		return OperandMaxOneRow
	case *plannercore.LogicalTableDual:
		return OperandTableDual
	case *plannercore.DataSource:
		return OperandDataSource
	case *plannercore.LogicalUnionScan:
		return OperandUnionScan
	case *plannercore.LogicalUnionAll:
		return OperandUnionAll
	case *plannercore.LogicalSort:
		return OperandSort
	case *plannercore.LogicalTopN:
		return OperandTopN
	case *plannercore.LogicalLock:
		return OperandLock
	case *plannercore.LogicalLimit:
		return OperandLimit
	default:
		return OperandUnsupported
	}
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

// NewPattern creats a pattern node according to the operand.
func NewPattern(operand Operand) *Pattern {
	return &Pattern{operand: operand}
}

// SetChildren sets the children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	p.children = children
}

// BuildPattern builds a Pattern from Operand and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, children ...*Pattern) *Pattern {
	p := &Pattern{operand: operand}
	p.children = children
	return p
}
