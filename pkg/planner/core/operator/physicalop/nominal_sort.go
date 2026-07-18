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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// NominalSort asks sort properties for its child. It is a fake operator that will not
// appear in final physical operator tree. It will be eliminated or converted to Projection.
type NominalSort struct {
	BasePhysicalPlan

	// These two fields are used to switch ScalarFunctions to Constants. For these
	// NominalSorts, we need to converted to Projections check if the ScalarFunctions
	// are out of bounds. (issue #11653)
	ByItems    []*util.ByItems
	OnlyColumn bool
}

// Init initializes NominalSort.
func (p NominalSort) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *NominalSort {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeSort, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// MemoryUsage return the memory usage of NominalSort
func (p *NominalSort) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.ByItems))*size.SizeOfPointer +
		size.SizeOfBool
	for _, byItem := range p.ByItems {
		sum += byItem.MemoryUsage()
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *NominalSort) ResolveIndices() (err error) {
	return resolveIndicesForSort(&p.BasePhysicalPlan)
}

// Attach2Task implements PhysicalPlan interface.
func (p *NominalSort) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4NominalSort(p, tasks...)
}
