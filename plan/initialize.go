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

import "github.com/pingcap/tidb/context"

func (p Aggregation) init(allocator *idAllocator, ctx context.Context) *Aggregation {
	p.basePlan = newBasePlan(TypeAgg, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p Join) init(allocator *idAllocator, ctx context.Context) *Join {
	p.basePlan = newBasePlan(TypeJoin, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p DataSource) init(allocator *idAllocator, ctx context.Context) *DataSource {
	p.basePlan = newBasePlan(TypeTableScan, allocator, ctx, &p)
	p.baseLogicalPlan = newBaseLogicalPlan(p.basePlan)
	return &p
}

func (p Apply) init(allocator *idAllocator, ctx context.Context) *Apply {
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

func (p Analyze) init(allocator *idAllocator, ctx context.Context) *Analyze {
	p.basePlan = newBasePlan(TypeAnalyze, allocator, ctx, &p)
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

func (p PhysicalDummyScan) init(allocator *idAllocator, ctx context.Context) *PhysicalDummyScan {
	p.basePlan = newBasePlan(TypeDummy, allocator, ctx, &p)
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
	p.basePlan = newBasePlan(TypeCache, allocator, ctx, &p)
	p.basePhysicalPlan = newBasePhysicalPlan(p.basePlan)
	return &p
}
