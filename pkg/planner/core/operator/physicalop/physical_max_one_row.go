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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	BasePhysicalPlan
}

// Init initializes PhysicalMaxOneRow.
func (p PhysicalMaxOneRow) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalMaxOneRow {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeMaxOneRow, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalMaxOneRow) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalMaxOneRow)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalPlan = *base
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalMaxOneRow
func (p *PhysicalMaxOneRow) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.BasePhysicalPlan.MemoryUsage()
}

// ExhaustPhysicalPlans4LogicalMaxOneRow generates PhysicalMaxOneRow plan from LogicalMaxOneRow.
func ExhaustPhysicalPlans4LogicalMaxOneRow(p *logicalop.LogicalMaxOneRow, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if !prop.IsSortItemEmpty() || prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because operator `MaxOneRow` is not supported now.")
		return nil, true, nil
	}
	mor := PhysicalMaxOneRow{}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(),
		&property.PhysicalProperty{
			ExpectedCnt: 2, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	return []base.PhysicalPlan{mor}, true, nil
}
