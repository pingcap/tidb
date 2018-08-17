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

package plan

import "github.com/pingcap/tidb/sessionctx"

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
)

func (la LogicalAggregation) init(ctx sessionctx.Context) *LogicalAggregation {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeAgg, &la)
	return &la
}

func (p LogicalJoin) init(ctx sessionctx.Context) *LogicalJoin {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeJoin, &p)
	return &p
}

func (ds DataSource) init(ctx sessionctx.Context) *DataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeTableScan, &ds)
	return &ds
}

func (la LogicalApply) init(ctx sessionctx.Context) *LogicalApply {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeApply, &la)
	return &la
}

func (p LogicalSelection) init(ctx sessionctx.Context) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeSel, &p)
	return &p
}

func (p PhysicalSelection) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalSelection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSel, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p LogicalUnionScan) init(ctx sessionctx.Context) *LogicalUnionScan {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeUnionScan, &p)
	return &p
}

func (p LogicalProjection) init(ctx sessionctx.Context) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeProj, &p)
	return &p
}

func (p PhysicalProjection) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalProjection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeProj, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p LogicalUnionAll) init(ctx sessionctx.Context) *LogicalUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeUnion, &p)
	return &p
}

func (p PhysicalUnionAll) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalUnionAll {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeUnion, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (ls LogicalSort) init(ctx sessionctx.Context) *LogicalSort {
	ls.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeSort, &ls)
	return &ls
}

func (p PhysicalSort) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSort, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p NominalSort) init(ctx sessionctx.Context, props ...*requiredProp) *NominalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeSort, &p)
	p.childrenReqProps = props
	return &p
}

func (lt LogicalTopN) init(ctx sessionctx.Context) *LogicalTopN {
	lt.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeTopN, &lt)
	return &lt
}

func (p PhysicalTopN) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalTopN {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTopN, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p LogicalLimit) init(ctx sessionctx.Context) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeLimit, &p)
	return &p
}

func (p PhysicalLimit) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalLimit {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeLimit, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p LogicalTableDual) init(ctx sessionctx.Context) *LogicalTableDual {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeDual, &p)
	return &p
}

func (p PhysicalTableDual) init(ctx sessionctx.Context, stats *statsInfo) *PhysicalTableDual {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeDual, &p)
	p.stats = stats
	return &p
}

func (p LogicalMaxOneRow) init(ctx sessionctx.Context) *LogicalMaxOneRow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeMaxOneRow, &p)
	return &p
}

func (p PhysicalMaxOneRow) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalMaxOneRow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMaxOneRow, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p Update) init(ctx sessionctx.Context) *Update {
	p.basePlan = newBasePlan(ctx, TypeUpdate)
	return &p
}

func (p Delete) init(ctx sessionctx.Context) *Delete {
	p.basePlan = newBasePlan(ctx, TypeDelete)
	return &p
}

func (p Insert) init(ctx sessionctx.Context) *Insert {
	p.basePlan = newBasePlan(ctx, TypeInsert)
	return &p
}

func (p Show) init(ctx sessionctx.Context) *Show {
	p.basePlan = newBasePlan(ctx, TypeShow)
	return &p
}

func (p LogicalLock) init(ctx sessionctx.Context) *LogicalLock {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, TypeLock, &p)
	return &p
}

func (p PhysicalLock) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalLock {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeLock, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p PhysicalTableScan) init(ctx sessionctx.Context) *PhysicalTableScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTableScan, &p)
	return &p
}

func (p PhysicalIndexScan) init(ctx sessionctx.Context) *PhysicalIndexScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIdxScan, &p)
	return &p
}

func (p PhysicalMemTable) init(ctx sessionctx.Context, stats *statsInfo) *PhysicalMemTable {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMemTableScan, &p)
	p.stats = stats
	return &p
}

func (p PhysicalHashJoin) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalHashJoin {
	tp := TypeHashRightJoin
	if p.InnerChildIdx == 1 {
		tp = TypeHashLeftJoin
	}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, tp, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p PhysicalMergeJoin) init(ctx sessionctx.Context, stats *statsInfo) *PhysicalMergeJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeMergeJoin, &p)
	p.stats = stats
	return &p
}

func (base basePhysicalAgg) init(ctx sessionctx.Context, stats *statsInfo) *basePhysicalAgg {
	base.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeHashAgg, &base)
	base.stats = stats
	return &base
}

func (base basePhysicalAgg) initForHash(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeHashAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (base basePhysicalAgg) initForStream(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeStreamAgg, p)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (p PhysicalApply) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalApply {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeApply, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p PhysicalUnionScan) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalUnionScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeUnionScan, &p)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

func (p PhysicalIndexLookUpReader) init(ctx sessionctx.Context) *PhysicalIndexLookUpReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeIndexLookUp, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalTableReader) init(ctx sessionctx.Context) *PhysicalTableReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, TypeTableReader, &p)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalIndexReader) init(ctx sessionctx.Context) *PhysicalIndexReader {
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

func (p PhysicalIndexJoin) init(ctx sessionctx.Context, stats *statsInfo, props ...*requiredProp) *PhysicalIndexJoin {
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
