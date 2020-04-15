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

package memo

import (
	plannercore "github.com/pingcap/tidb/v4/planner/core"
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
	// OperandJoin is the operand for LogicalJoin.
	OperandJoin
	// OperandAggregation is the operand for LogicalAggregation.
	OperandAggregation
	// OperandProjection is the operand for LogicalProjection.
	OperandProjection
	// OperandSelection is the operand for LogicalSelection.
	OperandSelection
	// OperandApply is the operand for LogicalApply.
	OperandApply
	// OperandMaxOneRow is the operand for LogicalMaxOneRow.
	OperandMaxOneRow
	// OperandTableDual is the operand for LogicalTableDual.
	OperandTableDual
	// OperandDataSource is the operand for DataSource.
	OperandDataSource
	// OperandUnionScan is the operand for LogicalUnionScan.
	OperandUnionScan
	// OperandUnionAll is the operand for LogicalUnionAll.
	OperandUnionAll
	// OperandSort is the operand for LogicalSort.
	OperandSort
	// OperandTopN is the operand for LogicalTopN.
	OperandTopN
	// OperandLock is the operand for LogicalLock.
	OperandLock
	// OperandLimit is the operand for LogicalLimit.
	OperandLimit
	// OperandTiKVSingleGather is the operand for TiKVSingleGather.
	OperandTiKVSingleGather
	// OperandMemTableScan is the operand for MemTableScan.
	OperandMemTableScan
	// OperandTableScan is the operand for TableScan.
	OperandTableScan
	// OperandIndexScan is the operand for IndexScan.
	OperandIndexScan
	// OperandShow is the operand for Show.
	OperandShow
	// OperandWindow is the operand for window function.
	OperandWindow
	// OperandUnsupported is the operand for unsupported operators.
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p plannercore.LogicalPlan) Operand {
	switch p.(type) {
	case *plannercore.LogicalApply:
		return OperandApply
	case *plannercore.LogicalJoin:
		return OperandJoin
	case *plannercore.LogicalAggregation:
		return OperandAggregation
	case *plannercore.LogicalProjection:
		return OperandProjection
	case *plannercore.LogicalSelection:
		return OperandSelection
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
	case *plannercore.TiKVSingleGather:
		return OperandTiKVSingleGather
	case *plannercore.LogicalTableScan:
		return OperandTableScan
	case *plannercore.LogicalMemTable:
		return OperandMemTableScan
	case *plannercore.LogicalIndexScan:
		return OperandIndexScan
	case *plannercore.LogicalShow:
		return OperandShow
	case *plannercore.LogicalWindow:
		return OperandWindow
	default:
		return OperandUnsupported
	}
}

// Match checks if current Operand matches specified one.
func (o Operand) Match(t Operand) bool {
	if o == OperandAny || t == OperandAny {
		return true
	}
	if o == t {
		return true
	}
	return false
}

// Pattern defines the match pattern for a rule. It's a tree-like structure
// which is a piece of a logical expression. Each node in the Pattern tree is
// defined by an Operand and EngineType pair.
type Pattern struct {
	Operand
	EngineTypeSet
	Children []*Pattern
}

// Match checks whether the EngineTypeSet contains the given EngineType
// and whether the two Operands match.
func (p *Pattern) Match(o Operand, e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand.Match(o)
}

// MatchOperandAny checks whether the pattern's Operand is OperandAny
// and the EngineTypeSet contains the given EngineType.
func (p *Pattern) MatchOperandAny(e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand == OperandAny
}

// NewPattern creates a pattern node according to the Operand and EngineType.
func NewPattern(operand Operand, engineTypeSet EngineTypeSet) *Pattern {
	return &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
}

// SetChildren sets the Children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	p.Children = children
}

// BuildPattern builds a Pattern from Operand, EngineType and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, engineTypeSet EngineTypeSet, children ...*Pattern) *Pattern {
	p := &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
	p.Children = children
	return p
}
