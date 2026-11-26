// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// PhysicalCalcFoundRows streams data and obtains its unlimited count before EOF.
// Both children belong to one statement and one plan-cache entry; no execution
// state or count is retained in the plan. Only child 0 produces client results.
type PhysicalCalcFoundRows struct {
	BasePhysicalPlan
}

// NewPhysicalCalcFoundRows joins independently optimized data and count plans.
func NewPhysicalCalcFoundRows(ctx base.PlanContext, data, count base.PhysicalPlan) *PhysicalCalcFoundRows {
	p := &PhysicalCalcFoundRows{}
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeCalcFoundRows, p, data.QueryBlockOffset())
	p.SetChildren(data, count)
	p.SetStats(data.StatsInfo())
	p.SetOutputNames(data.OutputNames())
	p.SetChildrenReqProps([]*property.PhysicalProperty{
		{TaskTp: property.RootTaskType},
		{TaskTp: property.RootTaskType},
	})
	return p
}

// Clone implements base.PhysicalPlan.
func (p *PhysicalCalcFoundRows) Clone(ctx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalCalcFoundRows)
	*cloned = *p
	basePlan, err := p.BasePhysicalPlan.CloneWithSelf(ctx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalPlan = *basePlan
	return cloned, nil
}
