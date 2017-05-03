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
	// TypeCache is the type of cache.
	TypeCache = "Cache"
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

func (p LogicalAggregation) init(allocator *idAllocator, ctx context.Context) *LogicalAggregation {
	p.basePlan = newBasePlan(TypeAgg, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalJoin) init(allocator *idAllocator, ctx context.Context) *LogicalJoin {
	p.basePlan = newBasePlan(TypeJoin, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p DataSource) init(allocator *idAllocator, ctx context.Context) *DataSource {
	p.basePlan = newBasePlan(TypeTableScan, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p LogicalApply) init(allocator *idAllocator, ctx context.Context) *LogicalApply {
	p.basePlan = newBasePlan(TypeApply, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p Selection) init(allocator *idAllocator, ctx context.Context) *Selection {
	p.basePlan = newBasePlan(TypeSel, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Projection) init(allocator *idAllocator, ctx context.Context) *Projection {
	p.basePlan = newBasePlan(TypeProj, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Union) init(allocator *idAllocator, ctx context.Context) *Union {
	p.basePlan = newBasePlan(TypeUnion, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Sort) init(allocator *idAllocator, ctx context.Context) *Sort {
	p.basePlan = newBasePlan(TypeSort, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Limit) init(allocator *idAllocator, ctx context.Context) *Limit {
	p.basePlan = newBasePlan(TypeLimit, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p TableDual) init(allocator *idAllocator, ctx context.Context) *TableDual {
	p.basePlan = newBasePlan(TypeDual, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Exists) init(allocator *idAllocator, ctx context.Context) *Exists {
	p.basePlan = newBasePlan(TypeExists, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p MaxOneRow) init(allocator *idAllocator, ctx context.Context) *MaxOneRow {
	p.basePlan = newBasePlan(TypeMaxOneRow, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Update) init(allocator *idAllocator, ctx context.Context) *Update {
	p.basePlan = newBasePlan(TypeUpate, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Delete) init(allocator *idAllocator, ctx context.Context) *Delete {
	p.basePlan = newBasePlan(TypeDelete, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Insert) init(allocator *idAllocator, ctx context.Context) *Insert {
	p.basePlan = newBasePlan(TypeInsert, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Show) init(allocator *idAllocator, ctx context.Context) *Show {
	p.basePlan = newBasePlan(TypeShow, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p SelectLock) init(allocator *idAllocator, ctx context.Context) *SelectLock {
	p.basePlan = newBasePlan(TypeLock, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalTableScan) init(allocator *idAllocator, ctx context.Context) *PhysicalTableScan {
	p.basePlan = newBasePlan(TypeTableScan, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalIndexScan) init(allocator *idAllocator, ctx context.Context) *PhysicalIndexScan {
	p.basePlan = newBasePlan(TypeIdxScan, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalMemTable) init(allocator *idAllocator, ctx context.Context) *PhysicalMemTable {
	p.basePlan = newBasePlan(TypeMemTableScan, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalHashJoin) init(allocator *idAllocator, ctx context.Context) *PhysicalHashJoin {
	tp := TypeHashRightJoin
	if p.SmallTable == 1 {
		tp = TypeHashLeftJoin
	}
	p.basePlan = newBasePlan(tp, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalHashSemiJoin) init(allocator *idAllocator, ctx context.Context) *PhysicalHashSemiJoin {
	p.basePlan = newBasePlan(TypeHashSemiJoin, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalMergeJoin) init(allocator *idAllocator, ctx context.Context) *PhysicalMergeJoin {
	p.basePlan = newBasePlan(TypeMergeJoin, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalAggregation) init(allocator *idAllocator, ctx context.Context) *PhysicalAggregation {
	tp := TypeHashAgg
	if p.AggType == StreamedAgg {
		tp = TypeStreamAgg
	}
	p.basePlan = newBasePlan(tp, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalApply) init(allocator *idAllocator, ctx context.Context) *PhysicalApply {
	p.basePlan = newBasePlan(TypeApply, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p Cache) init(allocator *idAllocator, ctx context.Context) *Cache {
	p.basePlan = newBasePlan(TypeCache, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalUnionScan) init(allocator *idAllocator, ctx context.Context) *PhysicalUnionScan {
	p.basePlan = newBasePlan(TypeUnionScan, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}

func (p PhysicalIndexLookUpReader) init(allocator *idAllocator, ctx context.Context) *PhysicalIndexLookUpReader {
	p.basePlan = newBasePlan(TypeIndexLookUp, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	return &p
}

func (p PhysicalTableReader) init(allocator *idAllocator, ctx context.Context) *PhysicalTableReader {
	p.basePlan = newBasePlan(TypeTableReader, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
	p.schema = p.tablePlan.Schema()
	return &p
}

func (p PhysicalIndexReader) init(allocator *idAllocator, ctx context.Context) *PhysicalIndexReader {
	p.basePlan = newBasePlan(TypeIndexReader, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	if _, ok := p.indexPlan.(*PhysicalAggregation); ok {
		p.schema = p.indexPlan.Schema()
	} else {
		// The IndexScan running in KV Layer will read all columns from storage. But TiDB Only needs some of them.
		// So their schemas are different, we need to resolve indices again.
		schemaInKV := p.indexPlan.Schema()
		is := p.IndexPlans[0].(*PhysicalIndexScan)
		p.schema = is.dataSourceSchema
		for _, col := range p.schema.Columns {
			col.ResolveIndices(schemaInKV)
		}
	}
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
