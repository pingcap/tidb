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
)

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeStreamAgg is the type of StreamAgg.
	TypeStreamAgg = "StreamAgg"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeShow is the type of show.
	TypeShow = "Show"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypeTableScan is the type of TableScan.
	TypeTableScan = "TableScan"
	// TypeMemTableScan is the type of TableScan.
	TypeMemTableScan = "MemTableScan"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeIdxScan is the type of IndexScan.
	TypeIdxScan = "IndexScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashLeftJoin is the type of left hash join.
	TypeHashLeftJoin = "HashLeftJoin"
	// TypeHashRightJoin is the type of right hash join.
	TypeHashRightJoin = "HashRightJoin"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeApply is the type of Apply.
	TypeApply = "Apply"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeDual is the type of TableDual.
	TypeDual = "TableDual"
	// TypeLock is the type of SelectLock.
	TypeLock = "SelectLock"
	// TypeInsert is the type of Insert
	TypeInsert = "Insert"
	// TypeUpdate is the type of Update.
	TypeUpdate = "Update"
	// TypeDelete is the type of Delete.
	TypeDelete = "Delete"
	// TypeIndexLookUp is the type of IndexLookUp.
	TypeIndexLookUp = "IndexLookUp"
	// TypeTableReader is the type of TableReader.
	TypeTableReader = "TableReader"
	// TypeIndexReader is the type of IndexReader.
	TypeIndexReader = "IndexReader"
	// TypeWindow is the type of Window.
	TypeWindow = "Window"
)

// Init initializes LogicalAggregation.
func (la LogicalAggregation) Init(ctx sessionctx.Context) *LogicalAggregation {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeAgg, &la)
	return &la
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx sessionctx.Context) *LogicalJoin {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeJoin, &p)
	return &p
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx sessionctx.Context) *DataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeTableScan, &ds)
	return &ds
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx sessionctx.Context) *LogicalApply {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeApply, &la)
	return &la
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx sessionctx.Context) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeSel, &p)
	return &p
}

// Init initializes PhysicalSelection.
func (p PhysicalSelection) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalSelection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSel, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx sessionctx.Context) *LogicalUnionScan {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeUnionScan, &p)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx sessionctx.Context) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeProj, &p)
	return &p
}

// Init initializes PhysicalProjection.
func (p PhysicalProjection) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalProjection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeProj, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx sessionctx.Context) *LogicalUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeUnion, &p)
	return &p
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeUnion, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalSort.
func (ls LogicalSort) Init(ctx sessionctx.Context) *LogicalSort {
	ls.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeSort, &ls)
	return &ls
}

// Init initializes PhysicalSort.
func (p PhysicalSort) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSort, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes NominalSort.
func (p NominalSort) Init(ctx sessionctx.Context, props ...*property.PhysicalProperty) *NominalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSort, &p)
	p.childrenReqProps = props
	return &p
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx sessionctx.Context) *LogicalTopN {
	lt.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeTopN, &lt)
	return &lt
}

// Init initializes PhysicalTopN.
func (p PhysicalTopN) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalTopN {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTopN, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx sessionctx.Context) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeLimit, &p)
	return &p
}

// Init initializes PhysicalLimit.
func (p PhysicalLimit) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLimit {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeLimit, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalTableDual.
func (p LogicalTableDual) Init(ctx sessionctx.Context) *LogicalTableDual {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeDual, &p)
	return &p
}

// Init initializes PhysicalTableDual.
func (p PhysicalTableDual) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalTableDual {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeDual, &p)
	p.stats = stats
	return &p
}

// Init initializes LogicalMaxOneRow.
func (p LogicalMaxOneRow) Init(ctx sessionctx.Context) *LogicalMaxOneRow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeMaxOneRow, &p)
	return &p
}

// Init initializes PhysicalMaxOneRow.
func (p PhysicalMaxOneRow) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalMaxOneRow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMaxOneRow, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalWindow.
func (p LogicalWindow) Init(ctx sessionctx.Context) *LogicalWindow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeWindow, &p)
	return &p
}

// Init initializes PhysicalWindow.
func (p PhysicalWindow) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalWindow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeWindow, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes Update.
func (p Update) Init(ctx sessionctx.Context) *Update {
	p.basePlan = newBasePlan(ctx, TypeUpdate)
	return &p
}

// Init initializes Delete.
func (p Delete) Init(ctx sessionctx.Context) *Delete {
	p.basePlan = newBasePlan(ctx, TypeDelete)
	return &p
}

// Init initializes Insert.
func (p Insert) Init(ctx sessionctx.Context) *Insert {
	p.basePlan = newBasePlan(ctx, TypeInsert)
	return &p
}

// Init initializes Show.
func (p Show) Init(ctx sessionctx.Context) *Show {
	p.basePlan = newBasePlan(ctx, TypeShow)
	return &p
}

// Init initializes LogicalLock.
func (p LogicalLock) Init(ctx sessionctx.Context) *LogicalLock {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeLock, &p)
	return &p
}

// Init initializes PhysicalLock.
func (p PhysicalLock) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLock {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeLock, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalTableScan.
func (p PhysicalTableScan) Init(ctx sessionctx.Context) *PhysicalTableScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTableScan, &p)
	return &p
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx sessionctx.Context) *PhysicalIndexScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIdxScan, &p)
	return &p
}

// Init initializes PhysicalMemTable.
func (p PhysicalMemTable) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalMemTable {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMemTableScan, &p)
	p.stats = stats
	return &p
}

// Init initializes PhysicalHashJoin.
func (p PhysicalHashJoin) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalHashJoin {
	tp := TypeHashRightJoin
	if p.InnerChildIdx == 1 {
		tp = TypeHashLeftJoin
	}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, tp, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalMergeJoin.
func (p PhysicalMergeJoin) Init(ctx sessionctx.Context, stats *property.StatsInfo) *PhysicalMergeJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMergeJoin, &p)
	p.stats = stats
	return &p
}

// Init initializes basePhysicalAgg.
func (base basePhysicalAgg) Init(ctx sessionctx.Context, stats *property.StatsInfo) *basePhysicalAgg {
	base.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeHashAgg, &base)
	base.stats = stats
	return &base
}

func (base basePhysicalAgg) initForHash(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeHashAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (base basePhysicalAgg) initForStream(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeStreamAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalApply {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeApply, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalUnionScan.
func (p PhysicalUnionScan) Init(ctx sessionctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalUnionScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeUnionScan, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx sessionctx.Context) *PhysicalIndexLookUpReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIndexLookUp, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

// Init initializes PhysicalTableReader.
func (p PhysicalTableReader) Init(ctx sessionctx.Context) *PhysicalTableReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTableReader, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx sessionctx.Context) *PhysicalIndexReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIndexReader, &p)
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
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIndexJoin, &p)
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
