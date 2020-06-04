// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

var (
	_ PhysicalPlan = &PhysicalSelection{}
	_ PhysicalPlan = &PhysicalProjection{}
	_ PhysicalPlan = &PhysicalTopN{}
	_ PhysicalPlan = &PhysicalMaxOneRow{}
	_ PhysicalPlan = &PhysicalTableDual{}
	_ PhysicalPlan = &PhysicalUnionAll{}
	_ PhysicalPlan = &PhysicalSort{}
	_ PhysicalPlan = &NominalSort{}
	_ PhysicalPlan = &PhysicalLock{}
	_ PhysicalPlan = &PhysicalLimit{}
	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &PhysicalTableScan{}
	_ PhysicalPlan = &PhysicalTableReader{}
	_ PhysicalPlan = &PhysicalIndexReader{}
	_ PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ PhysicalPlan = &PhysicalHashAgg{}
	_ PhysicalPlan = &PhysicalStreamAgg{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalIndexJoin{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
	_ PhysicalPlan = &PhysicalWindow{}
)

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	physicalSchemaProducer

	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	tablePlan  PhysicalPlan
}

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	indexPlan  PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column
}

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	indexPlan  PhysicalPlan
	tablePlan  PhysicalPlan

	// PushedLimit is used to avoid unnecessary table scan tasks of IndexLookUpReader.
	PushedLimit *PushedDownLimit
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	Table      *model.TableInfo
	Index      *model.IndexInfo
	IdxCols    []*expression.Column
	IdxColLens []int
	Ranges     []*ranger.Range
	Columns    []*model.ColumnInfo
	DBName     model.CIStr
	Desc       bool
	KeepOrder  bool
	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	TableAsName *model.CIStr

	// dataSourceSchema is the original schema of DataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	rangeInfo string

	// The index scan may be on a partition.
	isPartition     bool
	physicalTableID int64

	GenExprs map[model.TableColumnID]expression.Expression
}

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	physicalSchemaProducer

	DBName      model.CIStr
	Table       *model.TableInfo
	Columns     []*model.ColumnInfo
	TableAsName *model.CIStr
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	filterCondition []expression.Expression

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  model.CIStr
	Desc    bool
	Ranges  []*ranger.Range
	pkCol   *expression.Column

	TableAsName *model.CIStr

	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	// The table scan may be a partition, rather than a real table.
	isPartition     bool
	physicalTableID int64

	rangeDecidedBy []*expression.Column

	// HandleIdx is the index of handle, which is only used for admin check table.
	HandleIdx int
}

// IsPartition returns true and partition ID if it's actually a partition.
func (ts *PhysicalTableScan) IsPartition() (bool, int64) {
	return ts.isPartition, ts.physicalTableID
}

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	physicalSchemaProducer

	Exprs                []expression.Expression
	CalculateNoDelay     bool
	AvoidColumnEvaluator bool
}

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	basePhysicalPlan

	ByItems []*util.ByItems
	Offset  uint64
	Count   uint64
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	OuterSchema   []*expression.CorrelatedColumn
	rightChOffset int
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
	// InnerChildIdx indicates which child is to build the hash table.
	// For inner join, the smaller one will be chosen.
	// For outer join or semi join, it's exactly the inner one.
	InnerChildIdx int
	Concurrency   uint

	DefaultValues []types.Datum
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	physicalSchemaProducer

	JoinType        JoinType
	OuterJoinKeys   []*expression.Column
	InnerJoinKeys   []*expression.Column
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs
	OuterIndex      int
	outerSchema     *expression.Schema
	innerPlan       PhysicalPlan

	DefaultValues []types.Datum

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges []*ranger.Range
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxColLens stores the length of each index column.
	IdxColLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b=10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *ColWithCmpFuncManager
}

// PhysicalMergeJoin represents merge join for inner/ outer join.
type PhysicalMergeJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	CompareFuncs    []expression.CompareFunc
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	DefaultValues []types.Datum

	LeftKeys  []*expression.Column
	RightKeys []*expression.Column
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalLock is the physical operator of lock, which is used for `select ... for update` clause.
type PhysicalLock struct {
	basePhysicalPlan

	Lock ast.SelectLockType

	PartitionedTable []table.PartitionedTable
}

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	basePhysicalPlan

	Offset uint64
	Count  uint64
}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	physicalSchemaProducer
}

// AggregationType stands for the mode of aggregation plan.
type AggregationType int

const (
	// StreamedAgg supposes its input is sorted by group by key.
	StreamedAgg AggregationType = iota
	// FinalAgg supposes its input is partial results.
	FinalAgg
	// CompleteAgg supposes its input is original results.
	CompleteAgg
)

// String implements fmt.Stringer interface.
func (at AggregationType) String() string {
	switch at {
	case StreamedAgg:
		return "stream"
	case FinalAgg:
		return "final"
	case CompleteAgg:
		return "complete"
	}
	return "unsupported aggregation type"
}

type basePhysicalAgg struct {
	physicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
}

func (p *basePhysicalAgg) hasDistinctFunc() bool {
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			return true
		}
	}
	return false
}

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	basePhysicalAgg
}

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	basePhysicalAgg
}

// PhysicalSort is the physical operator of sort, which implements a memory sort.
type PhysicalSort struct {
	basePhysicalPlan

	ByItems []*util.ByItems
}

// NominalSort asks sort properties for its child. It is a fake operator that will not
// appear in final physical operator tree.
type NominalSort struct {
	basePhysicalPlan
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	basePhysicalPlan

	Conditions []expression.Expression
}

// IsPartition returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartition() (bool, int64) {
	return p.isPartition, p.physicalTableID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *stmtctx.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPoint(sc)
}

// PhysicalSelection represents a filter.
type PhysicalSelection struct {
	basePhysicalPlan

	Conditions []expression.Expression
}

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	basePhysicalPlan
}

// PhysicalTableDual is the physical operator of dual.
type PhysicalTableDual struct {
	physicalSchemaProducer

	RowCount int
	// placeHolder indicates if this dual plan is a place holder in query optimization
	// for data sources like `Show`, if true, the dual plan would be substituted by
	// `Show` in the final plan.
	placeHolder bool
}

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	physicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.tablePlan, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.stats.StatsVersion
	case *PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.stats.StatsVersion
	}

	return statsInfos
}
