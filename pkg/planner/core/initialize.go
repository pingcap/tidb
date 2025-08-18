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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
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

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexLookUpReader {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.TablePlans = physicalop.FlattenPushDownPlan(p.tablePlan)
	p.IndexPlans = physicalop.FlattenPushDownPlan(p.indexPlan)
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
		tempPlans := physicalop.FlattenPushDownPlan(partialPlan)
		p.PartialPlans = append(p.PartialPlans, tempPlans)
	}
	if p.tablePlan != nil {
		p.TablePlans = physicalop.FlattenPushDownPlan(p.tablePlan)
		p.SetSchema(p.tablePlan.Schema())
		p.HandleCols = p.TablePlans[0].(*physicalop.PhysicalTableScan).HandleCols
	} else {
		switch p.PartialPlans[0][0].(type) {
		case *physicalop.PhysicalTableScan:
			p.SetSchema(p.PartialPlans[0][0].Schema())
		default:
			is := p.PartialPlans[0][0].(*physicalop.PhysicalIndexScan)
			p.SetSchema(is.DataSourceSchema)
		}
	}
	if p.KeepOrder {
		switch x := p.PartialPlans[0][0].(type) {
		case *physicalop.PhysicalTableScan:
			p.ByItems = x.ByItems
		case *physicalop.PhysicalIndexScan:
			p.ByItems = x.ByItems
		}
	}
	return &p
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

// Init initializes ScalarSubqueryEvalCtx
func (p ScalarSubqueryEvalCtx) Init(ctx base.PlanContext, offset int) *ScalarSubqueryEvalCtx {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeScalarSubQuery, offset)
	return &p
}
