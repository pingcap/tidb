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

package core

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx base.PlanContext, offset int) *LogicalJoin {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx base.PlanContext, offset int) *DataSource {
	ds.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeDataSource, &ds, offset)
	return &ds
}

// Init initializes TiKVSingleGather.
func (sg TiKVSingleGather) Init(ctx base.PlanContext, offset int) *TiKVSingleGather {
	sg.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeTiKVSingleGather, &sg, offset)
	return &sg
}

// Init initializes LogicalTableScan.
func (ts LogicalTableScan) Init(ctx base.PlanContext, offset int) *LogicalTableScan {
	ts.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeTableScan, &ts, offset)
	return &ts
}

// Init initializes LogicalIndexScan.
func (is LogicalIndexScan) Init(ctx base.PlanContext, offset int) *LogicalIndexScan {
	is.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeIdxScan, &is, offset)
	return &is
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx base.PlanContext, offset int) *LogicalApply {
	la.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeApply, &la, offset)
	return &la
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx base.PlanContext, qbOffset int) *LogicalSelection {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeSel, &p, qbOffset)
	return &p
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx base.PlanContext, qbOffset int) *LogicalUnionScan {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeUnionScan, &p, qbOffset)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx base.PlanContext, qbOffset int) *LogicalProjection {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeProj, &p, qbOffset)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalExpand) Init(ctx base.PlanContext, offset int) *LogicalExpand {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeExpand, &p, offset)
	return &p
}

// Init initializes LogicalShow.
func (p LogicalShow) Init(ctx base.PlanContext) *LogicalShow {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeShow, &p, 0)
	return &p
}

// Init initializes LogicalShowDDLJobs.
func (p LogicalShowDDLJobs) Init(ctx base.PlanContext) *LogicalShowDDLJobs {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeShowDDLJobs, &p, 0)
	return &p
}

// Init initializes LogicalMemTable.
func (p LogicalMemTable) Init(ctx base.PlanContext, offset int) *LogicalMemTable {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeMemTableScan, &p, offset)
	return &p
}

// Init only assigns type and context.
func (p LogicalCTE) Init(ctx base.PlanContext, offset int) *LogicalCTE {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeCTE, &p, offset)
	return &p
}

// Init only assigns type and context.
func (p LogicalCTETable) Init(ctx base.PlanContext, offset int) *LogicalCTETable {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeCTETable, &p, offset)
	return &p
}

// Init initializes LogicalSequence
func (p LogicalSequence) Init(ctx base.PlanContext, offset int) *LogicalSequence {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, plancodec.TypeSequence, &p, offset)
	return &p
}
