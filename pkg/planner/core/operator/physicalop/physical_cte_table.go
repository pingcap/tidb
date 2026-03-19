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
	"strconv"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalCTETable is for CTE table.
type PhysicalCTETable struct {
	PhysicalSchemaProducer

	IDForStorage int
}

// Init only assigns type and context.
func (p PhysicalCTETable) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTETable {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeCTETable, 0)
	p.SetStats(stats)
	return &p
}

// ExplainInfo overrides the ExplainInfo
func (p *PhysicalCTETable) ExplainInfo() string {
	return "Scan on CTE_" + strconv.Itoa(p.IDForStorage)
}

// MemoryUsage return the memory usage of PhysicalCTETable
func (p *PhysicalCTETable) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt
}

func findBestTask4LogicalCTETable(super base.LogicalPlan, prop *property.PhysicalProperty) (t base.Task, err error) {
	if prop.IndexJoinProp != nil {
		// even enforce hint can not work with this.
		return base.InvalidTask, nil
	}
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalCTETable](super)
	if !prop.IsSortItemEmpty() {
		return base.InvalidTask, nil
	}

	pcteTable := PhysicalCTETable{IDForStorage: p.IDForStorage}.Init(p.SCtx(), p.StatsInfo())
	pcteTable.SetSchema(p.Schema())
	rt := &RootTask{}
	rt.SetPlan(pcteTable)
	t = rt
	return t, nil
}
