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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/types"
)

// LogicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type LogicalSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	BaseLogicalPlan
}

// Schema implements the Plan.Schema interface.
func (s *LogicalSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		if len(s.Children()) == 1 {
			// default implementation for plans has only one child: proprgate child schema.
			// multi-children plans are likely to have particular implementation.
			s.schema = s.Children()[0].Schema().Clone()
		} else {
			s.schema = expression.NewSchema()
		}
	}
	return s.schema
}

func (s *LogicalSchemaProducer) OutputNames() types.NameSlice {
	if s.names == nil && len(s.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child `OutputNames`.
		// multi-children plans are likely to have particular implementation.
		s.names = s.Children()[0].OutputNames()
	}
	return s.names
}

func (s *LogicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// SetSchema implements the Plan.SetSchema interface.
func (s *LogicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// SetSchemaAndNames sets the schema and names for the plan.
func (s *LogicalSchemaProducer) SetSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}

// InlineProjection prunes unneeded columns inline a executor.
func (s *LogicalSchemaProducer) InlineProjection(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) {
	prunedColumns := make([]*expression.Column, 0)
	used := expression.GetUsedList(s.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, s.Schema())
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, s.Schema().Columns[i])
			s.schema.Columns = append(s.Schema().Columns[:i], s.Schema().Columns[i+1:]...)
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(s.Self(), prunedColumns, opt)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	p.BaseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)

	// default implementation for plans has only one child: proprgate child keys
	// multi-children plans are likely to have particular implementation.
	if len(childSchema) == 1 {
		for _, key := range childSchema[0].Keys {
			indices := selfSchema.ColumnsIndices(key)
			if indices == nil {
				continue
			}
			newKey := make([]*expression.Column, 0, len(key))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
}
