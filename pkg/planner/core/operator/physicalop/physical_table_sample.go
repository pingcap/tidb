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
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalTableSample represents a table sample plan.
// It returns the sample rows to its parent operand.
type PhysicalTableSample struct {
	PhysicalSchemaProducer
	TableSampleInfo *tablesampler.TableSampleInfo
	TableInfo       table.Table
	PhysicalTableID int64
	Desc            bool
}

// Init initializes PhysicalTableSample.
func (p PhysicalTableSample) Init(ctx base.PlanContext, offset int) *PhysicalTableSample {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeTableSample, &p, offset)
	p.SetStats(&property.StatsInfo{RowCount: 1})
	return &p
}

// MemoryUsage return the memory usage of PhysicalTableSample
func (p *PhysicalTableSample) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInterface + size.SizeOfBool
	if p.TableSampleInfo != nil {
		sum += p.TableSampleInfo.MemoryUsage()
	}
	return
}
