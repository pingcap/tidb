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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/types"
)

var _ base.HashEquals = &LogicalSchemaProducer{}

// LogicalSchemaProducer stores the schema for the logical plans who can produce schema directly.
type LogicalSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice
	BaseLogicalPlan
}

// Hash64 implements HashEquals interface.
func (s *LogicalSchemaProducer) Hash64(h base.Hasher) {
	// output columns should affect the logical operator's hash.
	// since tidb doesn't maintain the names strictly, we should
	// only use the schema unique id to distinguish them.
	if s.schema != nil {
		h.HashByte(base.NotNilFlag)
		for _, col := range s.schema.Columns {
			col.Hash64(h)
		}
	} else {
		h.HashByte(base.NilFlag)
	}
}

// Equals implement HashEquals interface.
func (s *LogicalSchemaProducer) Equals(other any) bool {
	s2, ok := other.(*LogicalSchemaProducer)
	if !ok {
		return false
	}
	if s == nil {
		return s2 == nil
	}
	if s2 == nil {
		return false
	}
	if s.schema == nil {
		return s2.schema == nil
	}
	if s2.schema == nil {
		return false
	}
	if s.schema.Len() != s2.schema.Len() {
		return false
	}
	for i, col := range s.schema.Columns {
		if !col.Equals(s2.schema.Columns[i]) {
			return false
		}
	}
	return true
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

// OutputNames implements the Plan.OutputNames interface.
func (s *LogicalSchemaProducer) OutputNames() types.NameSlice {
	if s.names == nil && len(s.Children()) == 1 {
		// default implementation for plans has only one child: proprgate child `OutputNames`.
		// multi-children plans are likely to have particular implementation.
		s.names = s.Children()[0].OutputNames()
	}
	return s.names
}

// SetOutputNames sets the output names for the plan.
func (s *LogicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// SetSchema sets the logical schema producer's schema.
func (s *LogicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// SetSchemaAndNames sets the schema and names for the plan.
func (s *LogicalSchemaProducer) SetSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}

// InlineProjection prunes unneeded columns inline an executor.
func (s *LogicalSchemaProducer) InlineProjection(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) {
	prunedColumns := make([]*expression.Column, 0)
	used := expression.GetUsedList(s.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, s.Schema())
	if len(parentUsedCols) == 0 {
		// When this operator output no columns, we return its smallest column for safety.
		minColLen := math.MaxInt
		chosenPos := 0
		for i, col := range s.schema.Columns {
			flen := col.GetType(s.SCtx().GetExprCtx().GetEvalCtx()).GetFlen()
			if flen < minColLen {
				chosenPos = i
				minColLen = flen
			}
		}
		// It should be always true.
		if len(used) > 0 {
			used[chosenPos] = true
		}
	}
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			prunedColumns = append(prunedColumns, s.Schema().Columns[i])
			s.schema.Columns = append(s.Schema().Columns[:i], s.Schema().Columns[i+1:]...)
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(s.Self(), prunedColumns, opt)
}

// BuildKeyInfo implements LogicalPlan.BuildKeyInfo interface.
func (s *LogicalSchemaProducer) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.PKOrUK = nil
	s.BaseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)

	// default implementation for plans has only one child: proprgate child keys
	// multi-children plans are likely to have particular implementation.
	if len(childSchema) == 1 {
		for _, key := range childSchema[0].PKOrUK {
			indices := selfSchema.ColumnsIndices(key)
			if indices == nil {
				continue
			}
			newKey := make([]*expression.Column, 0, len(key))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.Columns[i])
			}
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, newKey)
		}
	}
}
