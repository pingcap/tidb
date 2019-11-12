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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/plancodec"
)

// Init initializes LogicalAggregation.
func (la LogicalAggregation) Init(ctx sessionctx.Context) *LogicalAggregation {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeAgg, &la)
	return &la
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx sessionctx.Context) *LogicalJoin {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeJoin, &p)
	return &p
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx sessionctx.Context) *DataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeTableScan, &ds)
	return &ds
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx sessionctx.Context) *LogicalApply {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeApply, &la)
	return &la
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx sessionctx.Context) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSel, &p)
	return &p
}

// Init initializes PhysicalSelection.
func (p PhysicalSelection) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalSelection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSel, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx sessionctx.Context) *LogicalUnionScan {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeUnionScan, &p)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx sessionctx.Context) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeProj, &p)
	return &p
}

// Init initializes PhysicalProjection.
func (p PhysicalProjection) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalProjection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeProj, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx sessionctx.Context) *LogicalUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeUnion, &p)
	return &p
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeUnion, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalSort.
func (ls LogicalSort) Init(ctx sessionctx.Context) *LogicalSort {
	ls.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSort, &ls)
	return &ls
}

// Init initializes PhysicalSort.
func (p PhysicalSort) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSort, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes NominalSort.
func (p NominalSort) Init(ctx sessionctx.Context, props ...*property.PhysicalProperty) *NominalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSort, &p)
	p.childrenReqProps = props
	return &p
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx sessionctx.Context) *LogicalTopN {
	lt.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeTopN, &lt)
	return &lt
}

// Init initializes PhysicalTopN.
func (p PhysicalTopN) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalTopN {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeTopN, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx sessionctx.Context) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeLimit, &p)
	return &p
}

// Init initializes PhysicalLimit.
func (p PhysicalLimit) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLimit {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeLimit, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalTableDual.
func (p LogicalTableDual) Init(ctx sessionctx.Context) *LogicalTableDual {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeDual, &p)
	return &p
}

// Init initializes PhysicalTableDual.
func (p PhysicalTableDual) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalTableDual {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeDual, &p)
	p.stats = stats
	return &p
}

// Init initializes LogicalMaxOneRow.
func (p LogicalMaxOneRow) Init(ctx sessionctx.Context) *LogicalMaxOneRow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeMaxOneRow, &p)
	return &p
}

// Init initializes PhysicalMaxOneRow.
func (p PhysicalMaxOneRow) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalMaxOneRow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMaxOneRow, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalWindow.
func (p LogicalWindow) Init(ctx sessionctx.Context) *LogicalWindow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeWindow, &p)
	return &p
}

// Init initializes PhysicalWindow.
func (p PhysicalWindow) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalWindow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeWindow, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes Update.
func (p Update) Init(ctx sessionctx.Context) *Update {
	p.basePlan = newBasePlan(ctx, plancodec.TypeUpdate)
	return &p
}

// Init initializes Delete.
func (p Delete) Init(ctx sessionctx.Context) *Delete {
	p.basePlan = newBasePlan(ctx, plancodec.TypeDelete)
	return &p
}

// Init initializes Insert.
func (p Insert) Init(ctx sessionctx.Context) *Insert {
	p.basePlan = newBasePlan(ctx, plancodec.TypeInsert)
	return &p
}

// Init initializes Show.
func (p Show) Init(ctx sessionctx.Context) *Show {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeShow, &p)
	// Just use pseudo stats to avoid panic.
	p.stats = &property.StatsInfo{RowCount: 1}
	return &p
}

// Init initializes LogicalLock.
func (p LogicalLock) Init(ctx sessionctx.Context) *LogicalLock {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeLock, &p)
	return &p
}

// Init initializes PhysicalLock.
func (p PhysicalLock) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLock {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeLock, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalTableScan.
func (p PhysicalTableScan) Init(ctx sessionctx.Context) *PhysicalTableScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeTableScan, &p)
	return &p
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx sessionctx.Context) *PhysicalIndexScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIdxScan, &p)
	return &p
}

// Init initializes PhysicalMemTable.
func (p PhysicalMemTable) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalMemTable {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMemTableScan, &p)
	p.stats = stats
	return &p
}

// Init initializes PhysicalHashJoin.
func (p PhysicalHashJoin) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalHashJoin {
	tp := plancodec.TypeHashRightJoin
	if p.InnerChildIdx == 1 {
		tp = plancodec.TypeHashLeftJoin
	}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, tp, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalMergeJoin.
func (p PhysicalMergeJoin) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalMergeJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMergeJoin, &p)
	p.stats = stats
	return &p
}

// Init initializes basePhysicalAgg.
func (base basePhysicalAgg) Init(ctx sessionctx.Context, stats *property.StatsInfo) *basePhysicalAgg {
	base.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeHashAgg, &base)
	base.stats = stats
	return &base
}

func (base basePhysicalAgg) initForHash(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeHashAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (base basePhysicalAgg) initForStream(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeStreamAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalApply {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeApply, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalUnionScan.
func (p PhysicalUnionScan) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalUnionScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeUnionScan, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx sessionctx.Context) *PhysicalIndexLookUpReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

// Init initializes PhysicalTableReader.
func (p PhysicalTableReader) Init(ctx sessionctx.Context) *PhysicalTableReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeTableReader, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx sessionctx.Context) *PhysicalIndexReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexReader, &p)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	switch p.indexPlan.(type) {
	case *PhysicalHashAgg, *PhysicalStreamAgg:
		p.schema = p.indexPlan.Schema()
	default:
		is := p.IndexPlans[0].(*PhysicalIndexScan)
		p.schema = is.dataSourceSchema
	}
	p.OutputColumns = p.schema.Clone().Columns
	return &p
}

// Init initializes PhysicalIndexJoin.
func (p PhysicalIndexJoin) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexJoin, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// flattenPushDownPlan converts a plan tree to a list, whose head is the leaf node like table scan.
func flattenPushDownPlan(p PhysicalPlan) []PhysicalPlan {
	plans := make([]PhysicalPlan, 0, 5)
	for {
		plans = append(plans, p)
		if len(p.Children()) == 0 {
			break
		}
		p = p.Children()[0]
	}
	for i := 0; i < len(plans)/2; i++ {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}
