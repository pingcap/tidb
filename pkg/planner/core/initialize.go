// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// Init initializes Update.
func (p Update) Init(ctx base.PlanContext) *Update {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeUpdate, 0)
	return &p
}

// Init initializes Delete.
func (p Delete) Init(ctx base.PlanContext) *Delete {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeDelete, 0)
	return &p
}

// Init initializes Insert.
func (p Insert) Init(ctx base.PlanContext) *Insert {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeInsert, 0)
	return &p
}

// Init initializes LoadData.
func (p LoadData) Init(ctx base.PlanContext) *LoadData {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeLoadData, 0)
	return &p
}

// Init initializes ImportInto.
func (p ImportInto) Init(ctx base.PlanContext) *ImportInto {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeImportInto, 0)
	return &p
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx base.PlanContext, offset int) *PhysicalIndexScan {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeIdxScan, &p, offset)
	return &p
}

func initForHash(pp base.PhysicalPlan, ctx base.PlanContext, stats *property.StatsInfo, offset int,
	schema *expression.Schema, props ...*property.PhysicalProperty) base.PhysicalPlan {
	baseAgg := pp.(*physicalop.BasePhysicalAgg)
	p := &PhysicalHashAgg{*baseAgg, ""}
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeHashAgg, p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	p.SetSchema(schema)
	return p
}

func initForStream(pp base.PhysicalPlan, ctx base.PlanContext, stats *property.StatsInfo, offset int,
	schema *expression.Schema, props ...*property.PhysicalProperty) base.PhysicalPlan {
	baseAgg := pp.(*physicalop.BasePhysicalAgg)
	p := &PhysicalStreamAgg{*baseAgg}
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeStreamAgg, p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	p.SetSchema(schema)
	return p
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalApply {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeApply, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexLookUpReader {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.SetSchema(p.tablePlan.Schema())
	return &p
}

// Init initializes PhysicalIndexMergeReader.
func (p PhysicalIndexMergeReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexMergeReader {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeIndexMerge, &p, offset)
	if p.tablePlan != nil {
		p.SetStats(p.tablePlan.StatsInfo())
	} else {
		var totalRowCount float64
		for _, partPlan := range p.partialPlans {
			totalRowCount += partPlan.StatsCount()
		}
		p.SetStats(p.partialPlans[0].StatsInfo().ScaleByExpectCnt(totalRowCount))
		p.StatsInfo().StatsVersion = p.partialPlans[0].StatsInfo().StatsVersion
	}
	p.PartialPlans = make([][]base.PhysicalPlan, 0, len(p.partialPlans))
	for _, partialPlan := range p.partialPlans {
		tempPlans := flattenPushDownPlan(partialPlan)
		p.PartialPlans = append(p.PartialPlans, tempPlans)
	}
	if p.tablePlan != nil {
		p.TablePlans = flattenPushDownPlan(p.tablePlan)
		p.SetSchema(p.tablePlan.Schema())
		p.HandleCols = p.TablePlans[0].(*physicalop.PhysicalTableScan).HandleCols
	} else {
		switch p.PartialPlans[0][0].(type) {
		case *physicalop.PhysicalTableScan:
			p.SetSchema(p.PartialPlans[0][0].Schema())
		default:
			is := p.PartialPlans[0][0].(*PhysicalIndexScan)
			p.SetSchema(is.dataSourceSchema)
		}
	}
	if p.KeepOrder {
		switch x := p.PartialPlans[0][0].(type) {
		case *physicalop.PhysicalTableScan:
			p.ByItems = x.ByItems
		case *PhysicalIndexScan:
			p.ByItems = x.ByItems
		}
	}
	return &p
}

func (p *PhysicalTableReader) adjustReadReqType(ctx base.PlanContext) {
	if p.StoreType == kv.TiFlash {
		_, ok := p.tablePlan.(*PhysicalExchangeSender)
		if ok {
			p.ReadReqType = MPP
			return
		}
		tableScans := p.GetTableScans()
		// When PhysicalTableReader's store type is tiflash, has table scan
		// and all table scans contained are not keepOrder, try to use batch cop.
		if len(tableScans) > 0 {
			for _, tableScan := range tableScans {
				if tableScan.KeepOrder {
					return
				}
			}

			// When allow batch cop is 1, only agg / topN uses batch cop.
			// When allow batch cop is 2, every query uses batch cop.
			switch ctx.GetSessionVars().AllowBatchCop {
			case 1:
				for _, plan := range p.TablePlans {
					switch plan.(type) {
					case *PhysicalHashAgg, *PhysicalStreamAgg, *physicalop.PhysicalTopN:
						p.ReadReqType = BatchCop
						return
					}
				}
			case 2:
				p.ReadReqType = BatchCop
			}
		}
	}
}

// Init initializes PhysicalTableReader.
func (p PhysicalTableReader) Init(ctx base.PlanContext, offset int) *PhysicalTableReader {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeTableReader, &p, offset)
	p.ReadReqType = Cop
	if p.tablePlan == nil {
		return &p
	}
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.SetSchema(p.tablePlan.Schema())
	p.adjustReadReqType(ctx)
	if p.ReadReqType == BatchCop || p.ReadReqType == MPP {
		setMppOrBatchCopForTableScan(p.tablePlan)
	}
	return &p
}

// Init initializes PhysicalTableSample.
func (p PhysicalTableSample) Init(ctx base.PlanContext, offset int) *PhysicalTableSample {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeTableSample, &p, offset)
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

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexReader {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeIndexReader, &p, offset)
	p.SetSchema(nil)
	return &p
}

// Init initializes PhysicalIndexMergeJoin.
func (p PhysicalIndexMergeJoin) Init(ctx base.PlanContext) *PhysicalIndexMergeJoin {
	p.SetTP(plancodec.TypeIndexMergeJoin)
	p.SetID(int(ctx.GetSessionVars().PlanID.Add(1)))
	p.SetSCtx(ctx)
	p.Self = &p
	return &p
}

// Init initializes BatchPointGetPlan.
func (p *BatchPointGetPlan) Init(ctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema, names []*types.FieldName, offset int) *BatchPointGetPlan {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeBatchPointGet, offset)
	p.SetSchema(schema)
	p.SetOutputNames(names)
	p.SetStats(stats)
	p.Columns = ExpandVirtualColumn(p.Columns, p.Schema(), p.TblInfo.Columns)
	return p
}

// Init initializes PointGetPlan.
func (p PointGetPlan) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, _ ...*property.PhysicalProperty) *PointGetPlan {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypePointGet, offset)
	p.SetStats(stats)
	p.Columns = ExpandVirtualColumn(p.Columns, p.schema, p.TblInfo.Columns)
	return &p
}

// Init only assigns type and context.
func (p PhysicalExchangeSender) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalExchangeSender {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeExchangeSender, 0)
	p.SetStats(stats)
	return &p
}

// Init only assigns type and context.
func (p PhysicalExchangeReceiver) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalExchangeReceiver {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeExchangeReceiver, 0)
	p.SetStats(stats)
	return &p
}

func flattenTreePlan(plan base.PhysicalPlan, plans []base.PhysicalPlan) []base.PhysicalPlan {
	plans = append(plans, plan)
	for _, child := range plan.Children() {
		plans = flattenTreePlan(child, plans)
	}
	return plans
}

// flattenPushDownPlan converts a plan tree to a list, whose head is the leaf node like table scan.
func flattenPushDownPlan(p base.PhysicalPlan) []base.PhysicalPlan {
	plans := make([]base.PhysicalPlan, 0, 5)
	plans = flattenTreePlan(p, plans)
	for i := range len(plans) / 2 {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}

// Init only assigns type and context.
func (p PhysicalCTE) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTE {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeCTE, &p, 0)
	p.SetStats(stats)
	return &p
}

// Init only assigns type and context.
func (p PhysicalCTETable) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTETable {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeCTETable, 0)
	p.SetStats(stats)
	return &p
}

// Init initializes FKCheck.
func (p FKCheck) Init(ctx base.PlanContext) *FKCheck {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeForeignKeyCheck, &p, 0)
	p.SetStats(&property.StatsInfo{})
	return &p
}

// Init initializes FKCascade
func (p FKCascade) Init(ctx base.PlanContext) *FKCascade {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeForeignKeyCascade, &p, 0)
	p.SetStats(&property.StatsInfo{})
	return &p
}

// Init initializes PhysicalSequence
func (p PhysicalSequence) Init(ctx base.PlanContext, stats *property.StatsInfo, blockOffset int, props ...*property.PhysicalProperty) *PhysicalSequence {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeSequence, &p, blockOffset)
	p.SetStats(stats)
	p.SetChildrenReqProps(props)
	return &p
}

// Init initializes ScalarSubqueryEvalCtx
func (p ScalarSubqueryEvalCtx) Init(ctx base.PlanContext, offset int) *ScalarSubqueryEvalCtx {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeScalarSubQuery, offset)
	return &p
}
