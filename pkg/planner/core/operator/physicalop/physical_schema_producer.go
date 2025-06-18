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
