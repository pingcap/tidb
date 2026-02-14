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
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// PhysicalIndexOnlyJoin combines two indexes: a driver index for ordering
// and a probe index for filtering. Only rows passing both index checks
// are fetched from the table.
type PhysicalIndexOnlyJoin struct {
	PhysicalSchemaProducer

	// DriverPlan is the driver index scan plan (provides ordering).
	DriverPlan base.PhysicalPlan
	// DriverPlans is the flattened driver plans for constructing executor pb.
	DriverPlans []base.PhysicalPlan
	// DriverReader is the properly initialized PhysicalIndexReader for the driver.
	DriverReader *PhysicalIndexReader

	// ProbeIndexPlan is the probe index reader plan template for dataReaderBuilder.
	ProbeIndexPlan base.PhysicalPlan
	// ProbePlans is the flattened probe plans.
	ProbePlans []base.PhysicalPlan
	// ProbeReader is the properly initialized PhysicalIndexReader for the probe.
	ProbeReader *PhysicalIndexReader

	// ProbeRanges is the range template with handle placeholder for probe index.
	ProbeRanges ranger.MutableRanges
	// KeyOff2IdxOff maps handle key offset to probe index column offset.
	KeyOff2IdxOff []int

	// TablePlan is the table scan plan for final row fetch.
	TablePlan base.PhysicalPlan
	// TablePlans is the flattened table plans.
	TablePlans []base.PhysicalPlan

	TableInfo  *model.TableInfo
	ProbeIndex *model.IndexInfo
	HandleCols util.HandleCols
	KeepOrder  bool

	// TableFilters are remaining filters not covered by either index.
	TableFilters []expression.Expression

	// PlanPartInfo is for partition pruning.
	PlanPartInfo *PhysPlanPartInfo
}

// Init initializes PhysicalIndexOnlyJoin.
func (p PhysicalIndexOnlyJoin) Init(ctx base.PlanContext, offset int) *PhysicalIndexOnlyJoin {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexOnlyJoin, &p, offset)
	if p.DriverPlan != nil {
		p.SetStats(p.DriverPlan.StatsInfo())
	}
	p.DriverPlans = FlattenListPushDownPlan(p.DriverPlan)
	p.ProbePlans = FlattenListPushDownPlan(p.ProbeIndexPlan)
	if p.TablePlan != nil {
		p.TablePlans = FlattenListPushDownPlan(p.TablePlan)
		p.SetSchema(p.TablePlan.Schema())
	}
	return &p
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexOnlyJoin) ExplainInfo() string {
	driverIdx := ""
	if len(p.DriverPlans) > 0 {
		if is, ok := p.DriverPlans[0].(*PhysicalIndexScan); ok {
			driverIdx = is.Index.Name.O
		}
	}
	probeIdx := ""
	if p.ProbeIndex != nil {
		probeIdx = p.ProbeIndex.Name.O
	}
	return fmt.Sprintf("type: index_only_join, driver_index: %s, probe_index: %s, keep_order: %v",
		driverIdx, probeIdx, p.KeepOrder)
}

// MemoryUsage returns the total memory usage of PhysicalIndexOnlyJoin.
func (p *PhysicalIndexOnlyJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	sum = p.PhysicalSchemaProducer.MemoryUsage()
	if p.DriverPlan != nil {
		sum += p.DriverPlan.MemoryUsage()
	}
	if p.ProbeIndexPlan != nil {
		sum += p.ProbeIndexPlan.MemoryUsage()
	}
	if p.TablePlan != nil {
		sum += p.TablePlan.MemoryUsage()
	}
	return
}
