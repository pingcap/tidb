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
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	PhysicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlan  base.PhysicalPlan
	IndexPlans []base.PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column

	// Used by partition table.
	PlanPartInfo *PhysPlanPartInfo
}

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexReader {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexReader, &p, offset)
	p.SetSchema(nil)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if cloned.IndexPlan, err = p.IndexPlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if cloned.IndexPlans, err = ClonePhysicalPlan(newCtx, p.IndexPlans); err != nil {
		return nil, err
	}
	cloned.OutputColumns = util.CloneCols(p.OutputColumns)
	return cloned, err
}

// SetSchema overrides op.PhysicalPlan SetSchema interface.
func (p *PhysicalIndexReader) SetSchema(_ *expression.Schema) {
	if p.IndexPlan != nil {
		p.IndexPlans = FlattenListPushDownPlan(p.IndexPlan)
		switch p.IndexPlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg, *PhysicalProjection:
			p.PhysicalSchemaProducer.SetSchema(p.IndexPlan.Schema())
		default:
			is := p.IndexPlans[0].(*PhysicalIndexScan)
			p.PhysicalSchemaProducer.SetSchema(is.DataSourceSchema)
		}
		p.OutputColumns = p.Schema().Clone().Columns
	}
}

// SetChildren overrides op.PhysicalPlan SetChildren interface.
func (p *PhysicalIndexReader) SetChildren(children ...base.PhysicalPlan) {
	p.IndexPlan = children[0]
	p.SetSchema(nil)
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.IndexPlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalIndexReader
func (p *PhysicalIndexReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.PlanPartInfo.MemoryUsage()
	if p.IndexPlan != nil {
		p.IndexPlan.MemoryUsage()
	}

	for _, plan := range p.IndexPlans {
		sum += plan.MemoryUsage()
	}
	for _, col := range p.OutputColumns {
		sum += col.MemoryUsage()
	}
	return
}

// GetPlanCostVer1 implements PhysicalPlan cost v1 for IndexMergeReader.
func (p *PhysicalIndexReader) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexReader(p, taskType, option)
}

// GetPlanCostVer2 implements PhysicalPlan cost v2 for IndexMergeReader.
func (p *PhysicalIndexReader) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalIndexReader(p, taskType, option, args...)
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexReader) LoadTableStats(ctx sessionctx.Context) {
	is := p.IndexPlans[0].(*PhysicalIndexScan)
	stats.LoadTableStats(ctx, is.Table, is.PhysicalTableID)
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalIndexReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	is := p.IndexPlans[0].(*PhysicalIndexScan)
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	asName := ""
	if is.TableAsName != nil && len(is.TableAsName.O) > 0 {
		asName = is.TableAsName.O
	}
	res := GetDynamicAccessPartition(sctx, is.Table, p.PlanPartInfo, asName)
	if res == nil {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return access.DynamicPartitionAccessObjects{res}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.IndexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return "index:" + p.IndexPlan.TP()
}

// GetNetDataSize calculates the cost of the plan in network data transfer.
func (p *PhysicalIndexReader) GetNetDataSize() float64 {
	tblStats := GetTblStats(p.IndexPlan)
	rowSize := cardinality.GetAvgRowSize(p.SCtx(), tblStats, p.IndexPlan.Schema().Columns, true, false)
	return p.IndexPlan.StatsCount() * rowSize
}

// GetPhysicalIndexReader returns PhysicalIndexReader for logical TiKVSingleGather.
func GetPhysicalIndexReader(sg *logicalop.TiKVSingleGather, schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexReader {
	reader := PhysicalIndexReader{}.Init(sg.SCtx(), sg.QueryBlockOffset())
	reader.SetStats(stats)
	reader.SetSchema(schema)
	reader.SetChildrenReqProps(props)
	return reader
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	err = p.IndexPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, col := range p.OutputColumns {
		newCol, _, err := col.ResolveIndices(p.IndexPlan.Schema())
		if err != nil {
			// Check if there is duplicate virtual expression column matched.
			sctx := p.SCtx()
			newExprCol, isOK := col.ResolveIndicesByVirtualExpr(sctx.GetExprCtx().GetEvalCtx(), p.IndexPlan.Schema())
			if isOK {
				p.OutputColumns[i] = newExprCol.(*expression.Column)
				continue
			}
			return err
		}
		p.OutputColumns[i] = newCol.(*expression.Column)
	}
	return
}
