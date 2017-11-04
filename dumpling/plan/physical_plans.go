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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
)

var (
	_ PhysicalPlan = &Selection{}
	_ PhysicalPlan = &Projection{}
	_ PhysicalPlan = &Exists{}
	_ PhysicalPlan = &MaxOneRow{}
	_ PhysicalPlan = &TableDual{}
	_ PhysicalPlan = &Union{}
	_ PhysicalPlan = &Sort{}
	_ PhysicalPlan = &Update{}
	_ PhysicalPlan = &Delete{}
	_ PhysicalPlan = &SelectLock{}
	_ PhysicalPlan = &Limit{}
	_ PhysicalPlan = &Show{}
	_ PhysicalPlan = &Insert{}
	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &PhysicalTableScan{}
	_ PhysicalPlan = &PhysicalTableReader{}
	_ PhysicalPlan = &PhysicalIndexReader{}
	_ PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ PhysicalPlan = &PhysicalAggregation{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalIndexJoin{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalHashSemiJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
)

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	*basePlan
	basePhysicalPlan

	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	tablePlan  PhysicalPlan

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	*basePlan
	basePhysicalPlan

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	indexPlan  PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	*basePlan
	basePhysicalPlan

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	indexPlan  PhysicalPlan
	tablePlan  PhysicalPlan

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexLookUpReader) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalTableSource

	Table      *model.TableInfo
	Index      *model.IndexInfo
	Ranges     []*types.IndexRange
	Columns    []*model.ColumnInfo
	DBName     model.CIStr
	Desc       bool
	OutOfOrder bool
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
}

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	*basePlan
	basePhysicalPlan

	DBName      model.CIStr
	Table       *model.TableInfo
	Columns     []*model.ColumnInfo
	Ranges      []types.IntColumnRange
	TableAsName *model.CIStr

	// NeedColHandle is used in execution phase.
	NeedColHandle bool
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalMemTable) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

type physicalTableSource struct {
	*basePlan
	basePhysicalPlan

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	// filterCondition is only used by new planner.
	filterCondition []expression.Expression

	// NeedColHandle is used in execution phase.
	NeedColHandle bool

	// TODO: This should be removed after old planner was removed.
	unionScanSchema *expression.Schema
}

func needCount(af aggregation.Aggregation) bool {
	return af.GetName() == ast.AggFuncCount || af.GetName() == ast.AggFuncAvg
}

func needValue(af aggregation.Aggregation) bool {
	return af.GetName() == ast.AggFuncSum || af.GetName() == ast.AggFuncAvg || af.GetName() == ast.AggFuncFirstRow ||
		af.GetName() == ast.AggFuncMax || af.GetName() == ast.AggFuncMin || af.GetName() == ast.AggFuncGroupConcat
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	physicalTableSource

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  model.CIStr
	Desc    bool
	Ranges  []types.IntColumnRange
	pkCol   *expression.Column

	TableAsName *model.CIStr

	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	*basePlan
	basePhysicalPlan

	PhysicalJoin PhysicalPlan
	OuterSchema  []*expression.CorrelatedColumn

	rightChOffset int
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	*basePlan
	basePhysicalPlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
	SmallTable      int
	Concurrency     int

	DefaultValues []types.Datum
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	*basePlan
	basePhysicalPlan

	JoinType        JoinType
	OuterJoinKeys   []*expression.Column
	InnerJoinKeys   []*expression.Column
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs
	outerIndex      int
	KeepOrder       bool
	outerSchema     *expression.Schema
	innerPlan       PhysicalPlan

	DefaultValues []types.Datum
}

// PhysicalMergeJoin represents merge join for inner/ outer join.
type PhysicalMergeJoin struct {
	*basePlan
	basePhysicalPlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	DefaultValues []types.Datum

	leftKeys  []*expression.Column
	rightKeys []*expression.Column
}

// PhysicalHashSemiJoin represents hash join for semi join.
type PhysicalHashSemiJoin struct {
	*basePlan
	basePhysicalPlan

	WithAux bool
	Anti    bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression

	rightChOffset int
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

// PhysicalAggregation is Aggregation's physical plan.
type PhysicalAggregation struct {
	*basePlan
	basePhysicalPlan

	HasGby       bool
	AggType      AggregationType
	AggFuncs     []aggregation.Aggregation
	GroupByItems []expression.Expression

	propKeys   []*expression.Column
	inputCount float64 // inputCount is the input count of this plan.
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	*basePlan
	basePhysicalPlan

	NeedColHandle bool
	Conditions    []expression.Expression
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// SourceSchema returns the original schema of DataSource
func (p *PhysicalIndexScan) SourceSchema() *expression.Schema {
	return p.dataSourceSchema
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *variable.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPoint(sc)
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalApply) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashSemiJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalMergeJoin) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Selection) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Projection) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Exists) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *MaxOneRow) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Insert) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Limit) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Union) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Sort) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *TopN) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *TableDual) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *SelectLock) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalAggregation) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Update) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Delete) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Show) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.baseLogicalPlan = newBaseLogicalPlan(np.basePlan)
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalUnionScan) Copy() PhysicalPlan {
	np := *p
	np.basePlan = p.basePlan.copy()
	np.basePhysicalPlan = newBasePhysicalPlan(np.basePlan)
	return &np
}

func buildSchema(p PhysicalPlan) {
	switch x := p.(type) {
	case *Limit, *TopN, *Sort, *Selection, *MaxOneRow, *SelectLock:
		p.SetSchema(p.Children()[0].Schema())
	case *PhysicalHashJoin:
		p.SetSchema(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()))
	case *PhysicalIndexJoin:
		if x.JoinType == SemiJoin || x.JoinType == AntiSemiJoin {
			x.SetSchema(x.children[0].Schema().Clone())
		} else if x.JoinType == LeftOuterSemiJoin || x.JoinType == AntiLeftOuterSemiJoin {
			auxCol := x.schema.Columns[x.Schema().Len()-1]
			x.SetSchema(x.children[0].Schema().Clone())
			x.schema.Append(auxCol)
		} else {
			p.SetSchema(expression.MergeSchema(p.Children()[x.outerIndex].Schema(), p.Children()[1-x.outerIndex].Schema()))
		}
	case *PhysicalMergeJoin:
		if x.JoinType == SemiJoin || x.JoinType == AntiSemiJoin {
			x.SetSchema(x.children[0].Schema().Clone())
		} else if x.JoinType == LeftOuterSemiJoin || x.JoinType == AntiLeftOuterSemiJoin {
			auxCol := x.schema.Columns[x.Schema().Len()-1]
			x.SetSchema(x.children[0].Schema().Clone())
			x.schema.Append(auxCol)
		} else {
			p.SetSchema(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()))
		}
	case *PhysicalApply:
		buildSchema(x.PhysicalJoin)
		x.schema = x.PhysicalJoin.Schema()
	case *PhysicalHashSemiJoin:
		if x.WithAux {
			auxCol := x.schema.Columns[x.Schema().Len()-1]
			x.SetSchema(x.children[0].Schema().Clone())
			x.schema.Append(auxCol)
		} else {
			x.SetSchema(x.children[0].Schema().Clone())
		}
	case *Union:
		panic("Union shouldn't rebuild schema")
	}
}

// rebuildSchema rebuilds the schema for physical plans, because new planner may change indexjoin's schema.
func rebuildSchema(p PhysicalPlan) bool {
	needRebuild := false
	for _, ch := range p.Children() {
		needRebuild = needRebuild || rebuildSchema(ch.(PhysicalPlan))
	}
	if needRebuild {
		buildSchema(p)
	}
	switch p.(type) {
	case *PhysicalIndexJoin, *PhysicalHashJoin, *PhysicalMergeJoin:
		needRebuild = true
	case *Projection, *PhysicalAggregation:
		needRebuild = false
	}
	return needRebuild
}
