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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalPartitionUnionAll represents the LogicalUnionAll plan is for partition table.
type LogicalPartitionUnionAll struct {
	LogicalUnionAll `hash64-equals:"true"`
}

// Init initializes LogicalPartitionUnionAll.
func (p LogicalPartitionUnionAll) Init(ctx base.PlanContext, offset int) *LogicalPartitionUnionAll {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypePartitionUnion, &p, offset)
	return &p
}

// *************************** start implementation of LogicalPlan interface ***************************

// PruneColumns implements LogicalPlan interface.
func (p *LogicalPartitionUnionAll) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	prunedPlan, err := p.LogicalUnionAll.PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}

	unionAll, ok := prunedPlan.(*LogicalUnionAll)
	if !ok {
		// Return the transformed plan if it's no longer a LogicalUnionAll
		return prunedPlan, nil
	}

	// Update the wrapped LogicalUnionAll with the pruned result
	p.LogicalUnionAll = *unionAll
	return p, nil
}

// ExhaustPhysicalPlans implements LogicalPlan interface.
func (p *LogicalPartitionUnionAll) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalPartitionUnionAll(p, prop)
}

// *************************** end implementation of LogicalPlan interface ***************************
