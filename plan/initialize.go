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

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
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
	// TypeHashSemiJoin is the type of hash semi join.
	TypeHashSemiJoin = "HashSemiJoin"
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

func (p LogicalAggregation) init(ctx context.Context) *LogicalAggregation {
	p.basePlan = newBasePlan(TypeAgg, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalJoin) init(ctx context.Context) *LogicalJoin {
	p.basePlan = newBasePlan(TypeJoin, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p DataSource) init(ctx context.Context) *DataSource {
	p.basePlan = newBasePlan(TypeTableScan, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalApply) init(ctx context.Context) *LogicalApply {
	p.basePlan = newBasePlan(TypeApply, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalSelection) init(ctx context.Context) *LogicalSelection {
	p.basePlan = newBasePlan(TypeSel, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalSelection) init(ctx context.Context) *PhysicalSelection {
	p.basePlan = newBasePlan(TypeSel, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalUnionScan) init(ctx context.Context) *LogicalUnionScan {
	p.basePlan = newBasePlan(TypeUnionScan, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalProjection) init(ctx context.Context) *LogicalProjection {
	p.basePlan = newBasePlan(TypeProj, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalProjection) init(ctx context.Context) *PhysicalProjection {
	p.basePlan = newBasePlan(TypeProj, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalUnionAll) init(ctx context.Context) *LogicalUnionAll {
	p.basePlan = newBasePlan(TypeUnion, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalUnionAll) init(ctx context.Context) *PhysicalUnionAll {
	p.basePlan = newBasePlan(TypeUnion, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalSort) init(ctx context.Context) *LogicalSort {
	p.basePlan = newBasePlan(TypeSort, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalSort) init(ctx context.Context) *PhysicalSort {
	p.basePlan = newBasePlan(TypeSort, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalTopN) init(ctx context.Context) *LogicalTopN {
	p.basePlan = newBasePlan(TypeTopN, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalTopN) init(ctx context.Context) *PhysicalTopN {
	p.basePlan = newBasePlan(TypeTopN, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalLimit) init(ctx context.Context) *LogicalLimit {
	p.basePlan = newBasePlan(TypeLimit, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalLimit) init(ctx context.Context) *PhysicalLimit {
	p.basePlan = newBasePlan(TypeLimit, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalTableDual) init(ctx context.Context) *LogicalTableDual {
	p.basePlan = newBasePlan(TypeDual, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalTableDual) init(ctx context.Context) *PhysicalTableDual {
	p.basePlan = newBasePlan(TypeDual, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalExists) init(ctx context.Context) *LogicalExists {
	p.basePlan = newBasePlan(TypeExists, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalExists) init(ctx context.Context) *PhysicalExists {
	p.basePlan = newBasePlan(TypeExists, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalMaxOneRow) init(ctx context.Context) *LogicalMaxOneRow {
	p.basePlan = newBasePlan(TypeMaxOneRow, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalMaxOneRow) init(ctx context.Context) *PhysicalMaxOneRow {
	p.basePlan = newBasePlan(TypeMaxOneRow, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Update) init(ctx context.Context) *Update {
	p.basePlan = *newBasePlan(TypeUpdate, ctx, &p)
	return &p
}

func (p Delete) init(ctx context.Context) *Delete {
	p.basePlan = *newBasePlan(TypeDelete, ctx, &p)
	return &p
}

func (p Insert) init(ctx context.Context) *Insert {
	p.basePlan = *newBasePlan(TypeInsert, ctx, &p)
	return &p
}

func (p Show) init(ctx context.Context) *Show {
	p.basePlan = *newBasePlan(TypeShow, ctx, &p)
	return &p
}

func (p LogicalLock) init(ctx context.Context) *LogicalLock {
	p.basePlan = newBasePlan(TypeLock, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p PhysicalLock) init(ctx context.Context) *PhysicalLock {
	p.basePlan = newBasePlan(TypeLock, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalTableScan) init(ctx context.Context) *PhysicalTableScan {
	p.basePlan = newBasePlan(TypeTableScan, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalIndexScan) init(ctx context.Context) *PhysicalIndexScan {
	p.basePlan = newBasePlan(TypeIdxScan, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalMemTable) init(ctx context.Context) *PhysicalMemTable {
	p.basePlan = newBasePlan(TypeMemTableScan, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalHashJoin) init(ctx context.Context) *PhysicalHashJoin {
	tp := TypeHashRightJoin
	if p.SmallChildIdx == 1 {
		tp = TypeHashLeftJoin
	}
	p.basePlan = newBasePlan(tp, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalHashSemiJoin) init(ctx context.Context) *PhysicalHashSemiJoin {
	p.basePlan = newBasePlan(TypeHashSemiJoin, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalMergeJoin) init(ctx context.Context) *PhysicalMergeJoin {
	p.basePlan = newBasePlan(TypeMergeJoin, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (base basePhysicalAgg) initForHash(ctx context.Context) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePlan = newBasePlan(TypeHashAgg, ctx, p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return p
}

func (base basePhysicalAgg) initForStream(ctx context.Context, keys []*expression.Column, inputCnt float64) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{
		basePhysicalAgg: base,
		propKeys:        keys,
		inputCount:      inputCnt,
	}
	p.basePlan = newBasePlan(TypeStreamAgg, ctx, p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return p
}

func (p PhysicalApply) init(ctx context.Context) *PhysicalApply {
	p.basePlan = newBasePlan(TypeApply, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalUnionScan) init(ctx context.Context) *PhysicalUnionScan {
	p.basePlan = newBasePlan(TypeUnionScan, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalIndexLookUpReader) init(ctx context.Context) *PhysicalIndexLookUpReader {
	p.basePlan = newBasePlan(TypeIndexLookUp, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalTableReader) init(ctx context.Context) *PhysicalTableReader {
	p.basePlan = newBasePlan(TypeTableReader, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalIndexReader) init(ctx context.Context) *PhysicalIndexReader {
	p.basePlan = newBasePlan(TypeIndexReader, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	if _, ok := p.indexPlan.(*PhysicalHashAgg); ok {
		p.schema = p.indexPlan.Schema()
	} else {
		is := p.IndexPlans[0].(*PhysicalIndexScan)
		p.schema = is.dataSourceSchema
	}
	p.OutputColumns = p.schema.Clone().Columns
	return &p
}

func (p PhysicalIndexJoin) init(ctx context.Context, children ...Plan) *PhysicalIndexJoin {
	p.basePlan = newBasePlan(TypeIndexJoin, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.children = children
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
		p = p.Children()[0].(PhysicalPlan)
	}
	for i := 0; i < len(plans)/2; i++ {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}
