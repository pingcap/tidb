// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalSchemaProducer stores the schema for the physical plans who can produce schema directly.
type PhysicalSchemaProducer struct {
	schema *expression.Schema
	BasePhysicalPlan
}

// ******************************* start implementation of Plan interface *******************************

// Schema implements the Plan.Schema interface.
func (s *PhysicalSchemaProducer) Schema() *expression.Schema {
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

// SetSchema implements the Plan.SetSchema interface.
func (s *PhysicalSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// ******************************* end implementation of Plan interface *********************************

// *************************** start implementation of PhysicalPlan interface ***************************

// ResolveIndices implements the base.PhysicalPlan.<10th> interface.
func (s *PhysicalSchemaProducer) ResolveIndices() (err error) {
	err = s.BasePhysicalPlan.ResolveIndices()
	return err
}

// MemoryUsage implements the base.PhysicalPlan.<16th> interface.
func (s *PhysicalSchemaProducer) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = s.BasePhysicalPlan.MemoryUsage() + size.SizeOfPointer
	return
}

// *************************** end implementation of PhysicalPlan interface *****************************

// CloneForPlanCacheWithSelf clone physical plan for plan cache with new self as param.
func (s *PhysicalSchemaProducer) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*PhysicalSchemaProducer, bool) {
	cloned := new(PhysicalSchemaProducer)
	cloned.schema = s.schema
	base, ok := s.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, newSelf)
	if !ok {
		return nil, false
	}
	cloned.BasePhysicalPlan = *base
	return cloned, true
}

// CloneWithSelf clone physical plan for basic usage with new self as param.
func (s *PhysicalSchemaProducer) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*PhysicalSchemaProducer, error) {
	base, err := s.BasePhysicalPlan.CloneWithSelf(newCtx, newSelf)
	if err != nil {
		return nil, err
	}
	return &PhysicalSchemaProducer{
		BasePhysicalPlan: *base,
		schema:           s.Schema().Clone(),
	}, nil
}

//******************************** Simple Schema Producer *********************************

// SimpleSchemaProducer stores the schema for the base plans who can produce schema directly.
type SimpleSchemaProducer struct {
	schema *expression.Schema
	names  types.NameSlice `plan-cache-clone:"shallow"`
	baseimpl.Plan
}

// CloneSelfForPlanCache implements the base.Plan interface.
// SimpleSchemaProducer can't override the CloneForPlanCache, otherwise, Execute doesn't impl the plan interface.
// As before, SimpleSchemaProducer will always inherit the embedded Plan's CloneForPlanCache.
func (s *SimpleSchemaProducer) CloneSelfForPlanCache(newCtx base.PlanContext) *SimpleSchemaProducer {
	cloned := new(SimpleSchemaProducer)
	cloned.Plan = *s.Plan.CloneWithNewCtx(newCtx)
	cloned.schema = s.schema
	cloned.names = s.names
	return cloned
}

// OutputNames returns the outputting names of each column.
func (s *SimpleSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

// SetOutputNames sets the outputting name by the given slice.
func (s *SimpleSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// Schema implements the Plan.Schema interface.
func (s *SimpleSchemaProducer) Schema() *expression.Schema {
	if s.schema == nil {
		s.schema = expression.NewSchema()
	}
	return s.schema
}

// SetSchema implements the Plan.SetSchema interface.
func (s *SimpleSchemaProducer) SetSchema(schema *expression.Schema) {
	s.schema = schema
}

// SetSchemaAndNames sets the schema and names for the plan.
func (s *SimpleSchemaProducer) SetSchemaAndNames(schema *expression.Schema, names types.NameSlice) {
	s.schema = schema
	s.names = names
}

// MemoryUsage return the memory usage of SimpleSchemaProducer
func (s *SimpleSchemaProducer) MemoryUsage() (sum int64) {
	if s == nil {
		return
	}

	sum = size.SizeOfPointer + size.SizeOfSlice + int64(cap(s.names))*size.SizeOfPointer + s.Plan.MemoryUsage()
	if s.schema != nil {
		sum += s.schema.MemoryUsage()
	}
	for _, name := range s.names {
		sum += name.MemoryUsage()
	}
	return
}

// ResolveIndices implements Plan interface.
func (*SimpleSchemaProducer) ResolveIndices() (err error) {
	return
}
