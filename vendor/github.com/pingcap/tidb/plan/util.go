// Copyright 2017 PingCAP, Inc.
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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

// logicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type logicalSchemaProducer struct {
	schema *expression.Schema
	baseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *logicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// physicalSchemaProducer stores the schema for the physical plans who can produce schema directly.
type physicalSchemaProducer struct {
	schema *expression.Schema
	basePhysicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *physicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *physicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// baseSchemaProducer stores the schema for the base plans who can produce schema directly.
type baseSchemaProducer struct {
	schema *expression.Schema
	basePlan
}

// Schema implements the Plan.Schema interface.
func (s *baseSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *baseSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

func buildLogicalJoinSchema(joinType JoinType, join LogicalPlan) *expression.Schema {
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return join.Children()[0].Schema().Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := join.Children()[0].Schema().Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	return expression.MergeSchema(join.Children()[0].Schema(), join.Children()[1].Schema())
}

func buildPhysicalJoinSchema(joinType JoinType, join PhysicalPlan) *expression.Schema {
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return join.Children()[0].Schema().Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := join.Children()[0].Schema().Clone()
		newSchema.Append(join.Schema().Columns[join.Schema().Len()-1])
		return newSchema
	}
	return expression.MergeSchema(join.Children()[0].Schema(), join.Children()[1].Schema())
}
