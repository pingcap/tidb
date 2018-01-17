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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

var (
	_ PhysicalPlan = &PhysicalSelection{}
	_ PhysicalPlan = &PhysicalProjection{}
	_ PhysicalPlan = &PhysicalTopN{}
	_ PhysicalPlan = &PhysicalExists{}
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

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	indexPlan  PhysicalPlan
	tablePlan  PhysicalPlan
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	filterCondition []expression.Expression

	Table     *model.TableInfo
	Index     *model.IndexInfo
	Ranges    []*ranger.NewRange
	Columns   []*model.ColumnInfo
	DBName    model.CIStr
	Desc      bool
	KeepOrder bool
	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	// accessInAndEqCount is counter of all conditions in AccessCondition[accessEqualCount:accessInAndEqCount].
	accessInAndEqCount int
	// accessEqualCount is counter of all conditions in AccessCondition[:accessEqualCount].
	accessEqualCount int

	TableAsName *model.CIStr

	// dataSourceSchema is the original schema of DataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema

	// HistVersion is the version of the histogram when the query was issued.
	// It is used for query feedback.
	HistVersion uint64
}

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	physicalSchemaProducer

	DBName      model.CIStr
	Table       *model.TableInfo
	Columns     []*model.ColumnInfo
	Ranges      []ranger.IntColumnRange
	TableAsName *model.CIStr
}

func needCount(af *aggregation.AggFuncDesc) bool {
	return af.Name == ast.AggFuncCount || af.Name == ast.AggFuncAvg
}

func needValue(af *aggregation.AggFuncDesc) bool {
	return af.Name == ast.AggFuncSum || af.Name == ast.AggFuncAvg || af.Name == ast.AggFuncFirstRow ||
		af.Name == ast.AggFuncMax || af.Name == ast.AggFuncMin || af.Name == ast.AggFuncGroupConcat ||
		af.Name == ast.AggFuncBitOr || af.Name == ast.AggFuncBitAnd || af.Name == ast.AggFuncBitXor
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
	Ranges  []*ranger.NewRange
	pkCol   *expression.Column

	TableAsName *model.CIStr

	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool

	// HistVersion is the version of the histogram when the query was issued.
	// It is used for query feedback.
	HistVersion uint64
}

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	physicalSchemaProducer

	Exprs            []expression.Expression
	CalculateNoDelay bool
}

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	basePhysicalPlan

	ByItems []*ByItems
	Offset  uint64
	Count   uint64

	// partial is true if this topn is generated by push-down optimization.
	partial bool
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	physicalSchemaProducer

	PhysicalJoin *PhysicalHashJoin
	OuterSchema  []*expression.CorrelatedColumn

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
	SmallChildIdx   int
	Concurrency     int

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
	Ranges []*ranger.NewRange
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
}

// PhysicalMergeJoin represents merge join for inner/ outer join.
type PhysicalMergeJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	DefaultValues []types.Datum

	leftKeys  []*expression.Column
	rightKeys []*expression.Column
}

// PhysicalLock is the physical operator of lock, which is used for `select ... for update` clause.
type PhysicalLock struct {
	basePhysicalPlan

	Lock ast.SelectLockType
}

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	basePhysicalPlan

	Offset uint64
	Count  uint64

	// partial is true if this topn is generated by push-down optimization.
	partial bool
}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	basePhysicalPlan
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

	ByItems []*ByItems
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

// PhysicalExists is the physical operator of Exists.
type PhysicalExists struct {
	physicalSchemaProducer
}

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	basePhysicalPlan
}

// PhysicalTableDual is the physical operator of dual.
type PhysicalTableDual struct {
	physicalSchemaProducer

	RowCount int
}
