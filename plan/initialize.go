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
	// TypeUpate is the type of Update.
	TypeUpate = "Update"
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

func (p Selection) init(ctx context.Context) *Selection {
	p.basePlan = newBasePlan(TypeSel, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p LogicalUnionScan) init(ctx context.Context) *LogicalUnionScan {
	p.basePlan = newBasePlan(TypeUnionScan, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p Projection) init(ctx context.Context) *Projection {
	p.basePlan = newBasePlan(TypeProj, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Union) init(ctx context.Context) *Union {
	p.basePlan = newBasePlan(TypeUnion, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Sort) init(ctx context.Context) *Sort {
	p.basePlan = newBasePlan(TypeSort, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p TopN) init(ctx context.Context) *TopN {
	p.basePlan = newBasePlan(TypeTopN, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Limit) init(ctx context.Context) *Limit {
	p.basePlan = newBasePlan(TypeLimit, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p TableDual) init(ctx context.Context) *TableDual {
	p.basePlan = newBasePlan(TypeDual, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Exists) init(ctx context.Context) *Exists {
	p.basePlan = newBasePlan(TypeExists, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p MaxOneRow) init(ctx context.Context) *MaxOneRow {
	p.basePlan = newBasePlan(TypeMaxOneRow, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Update) init(ctx context.Context) *Update {
	p.basePlan = newBasePlan(TypeUpate, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Delete) init(ctx context.Context) *Delete {
	p.basePlan = newBasePlan(TypeDelete, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Insert) init(ctx context.Context) *Insert {
	p.basePlan = newBasePlan(TypeInsert, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Show) init(ctx context.Context) *Show {
	p.basePlan = newBasePlan(TypeShow, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p SelectLock) init(ctx context.Context) *SelectLock {
	p.basePlan = newBasePlan(TypeLock, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
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

func (p PhysicalAggregation) init(ctx context.Context) *PhysicalAggregation {
	tp := TypeHashAgg
	if p.AggType == StreamedAgg {
		tp = TypeStreamAgg
	}
	p.basePlan = newBasePlan(tp, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
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
	p.NeedColHandle = p.IndexPlans[0].(*PhysicalIndexScan).NeedColHandle
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalTableReader) init(ctx context.Context) *PhysicalTableReader {
	p.basePlan = newBasePlan(TypeTableReader, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.NeedColHandle = p.TablePlans[0].(*PhysicalTableScan).NeedColHandle
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalIndexReader) init(ctx context.Context) *PhysicalIndexReader {
	p.basePlan = newBasePlan(TypeIndexReader, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.NeedColHandle = p.IndexPlans[0].(*PhysicalIndexScan).NeedColHandle
	if _, ok := p.indexPlan.(*PhysicalAggregation); ok {
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
	p.schema = expression.MergeSchema(p.children[0].Schema(), p.children[1].Schema())
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
