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
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// PhysicalIndexMergeReader is the reader using multiple indexes in tidb.
type PhysicalIndexMergeReader struct {
	PhysicalSchemaProducer

	// IsIntersectionType means whether it's intersection type or union type.
	// Intersection type is for expressions connected by `AND` and union type is for `OR`.
	IsIntersectionType bool
	// AccessMVIndex indicates whether this IndexMergeReader access a MVIndex.
	AccessMVIndex bool

	// PushedLimit is used to avoid unnecessary table scan tasks of IndexMergeReader.
	PushedLimit *PushedDownLimit
	// ByItems is used to support sorting the handles returned by PartialPlansRaw.
	ByItems []*util.ByItems

	// PartialPlansRaw are the partial plans that have not been flatted. The type of each element is permitted PhysicalIndexScan or PhysicalTableScan.
	PartialPlansRaw []base.PhysicalPlan
	// TablePlan is a PhysicalTableScan to get the table tuples. Current, it must be not nil.
	TablePlan base.PhysicalPlan
	// PartialPlans flats the PartialPlansRaw to construct executor pb.
	PartialPlans [][]base.PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []base.PhysicalPlan

	// Used by partition table.
	PlanPartInfo *PhysPlanPartInfo

	KeepOrder bool

	HandleCols util.HandleCols
}

// Init initializes PhysicalIndexMergeReader.
func (p PhysicalIndexMergeReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexMergeReader {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexMerge, &p, offset)
	if p.TablePlan != nil {
		p.SetStats(p.TablePlan.StatsInfo())
	} else {
		var totalRowCount float64
		for _, partPlan := range p.PartialPlansRaw {
			totalRowCount += partPlan.StatsCount()
		}
		p.SetStats(p.PartialPlansRaw[0].StatsInfo().ScaleByExpectCnt(ctx.GetSessionVars(), totalRowCount))
		p.StatsInfo().StatsVersion = p.PartialPlansRaw[0].StatsInfo().StatsVersion
	}
	p.PartialPlans = make([][]base.PhysicalPlan, 0, len(p.PartialPlansRaw))
	for _, partialPlan := range p.PartialPlansRaw {
		tempPlans := FlattenListPushDownPlan(partialPlan)
		p.PartialPlans = append(p.PartialPlans, tempPlans)
	}
	if p.TablePlan != nil {
		p.TablePlans = FlattenListPushDownPlan(p.TablePlan)
		p.SetSchema(p.TablePlan.Schema())
		p.HandleCols = p.TablePlans[0].(*PhysicalTableScan).HandleCols
	} else {
		switch p.PartialPlans[0][0].(type) {
		case *PhysicalTableScan:
			p.SetSchema(p.PartialPlans[0][0].Schema())
		default:
			is := p.PartialPlans[0][0].(*PhysicalIndexScan)
			p.SetSchema(is.DataSourceSchema)
		}
	}
	if p.KeepOrder {
		switch x := p.PartialPlans[0][0].(type) {
		case *PhysicalTableScan:
			p.ByItems = x.ByItems
		case *PhysicalIndexScan:
			p.ByItems = x.ByItems
		}
	}
	return &p
}

// GetAvgTableRowSize return the average row size of table plan.
func (p *PhysicalIndexMergeReader) GetAvgTableRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(p.TablePlans[len(p.TablePlans)-1]), p.Schema().Columns, false, false)
}

// GetPartialReaderNetDataSize returns the estimated total response data size of a partial read.
func (p *PhysicalIndexMergeReader) GetPartialReaderNetDataSize(plan base.PhysicalPlan) float64 {
	_, isIdxScan := plan.(*PhysicalIndexScan)
	return plan.StatsCount() * cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(plan), plan.Schema().Columns, isIdxScan, false)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.PartialPlansRaw {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, PartialPlan := range p.PartialPlans {
		for _, child := range PartialPlan {
			corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
		}
	}
	return corCols
}

// GetPlanCostVer1 implements PhysicalPlan cost v1 for IndexMergeReader.
func (p *PhysicalIndexMergeReader) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexMergeReader(p, taskType, option)
}

// GetPlanCostVer2 implements PhysicalPlan cost v2 for IndexMergeReader.
func (p *PhysicalIndexMergeReader) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalIndexMergeReader(p, taskType, option, args...)
}

// MemoryUsage return the memory usage of PhysicalIndexMergeReader
func (p *PhysicalIndexMergeReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.PlanPartInfo.MemoryUsage()
	if p.TablePlan != nil {
		sum += p.TablePlan.MemoryUsage()
	}

	for _, plans := range p.PartialPlans {
		for _, plan := range plans {
			sum += plan.MemoryUsage()
		}
	}
	for _, plan := range p.TablePlans {
		sum += plan.MemoryUsage()
	}
	for _, plan := range p.PartialPlansRaw {
		sum += plan.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexMergeReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	stats.LoadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalIndexMergeReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	ts := p.TablePlans[0].(*PhysicalTableScan)
	asName := ""
	if ts.TableAsName != nil && len(ts.TableAsName.O) > 0 {
		asName = ts.TableAsName.O
	}
	res := GetDynamicAccessPartition(sctx, ts.Table, p.PlanPartInfo, asName)
	if res == nil {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return access.DynamicPartitionAccessObjects{res}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	var str strings.Builder
	if p.IsIntersectionType {
		str.WriteString("type: intersection")
	} else {
		str.WriteString("type: union")
	}
	if p.PushedLimit != nil {
		str.WriteString(", limit embedded(offset:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Offset, 10))
		str.WriteString(", count:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Count, 10))
		str.WriteString(")")
	}
	return str.String()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexMergeReader) ResolveIndices() (err error) {
	err = ResolveIndicesForVirtualColumn(p.TablePlan.Schema().Columns, p.Schema())
	if err != nil {
		return err
	}
	if p.TablePlan != nil {
		err = p.TablePlan.ResolveIndices()
		if err != nil {
			return err
		}
	}
	for i := range p.PartialPlansRaw {
		err = p.PartialPlansRaw[i].ResolveIndices()
		if err != nil {
			return err
		}
	}
	if p.HandleCols != nil && p.KeepOrder {
		p.HandleCols, err = p.HandleCols.ResolveIndices(p.Schema())
		if err != nil {
			return err
		}
	}
	return nil
}
