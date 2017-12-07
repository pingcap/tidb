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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
)

var (
	_ LogicalPlan = &LogicalJoin{}
	_ LogicalPlan = &LogicalAggregation{}
	_ LogicalPlan = &LogicalProjection{}
	_ LogicalPlan = &LogicalSelection{}
	_ LogicalPlan = &LogicalApply{}
	_ LogicalPlan = &LogicalExists{}
	_ LogicalPlan = &LogicalMaxOneRow{}
	_ LogicalPlan = &LogicalTableDual{}
	_ LogicalPlan = &DataSource{}
	_ LogicalPlan = &LogicalUnionAll{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalLock{}
	_ LogicalPlan = &LogicalLimit{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if row a in table A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if row a in table A does not match any row in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	case SemiJoin:
		return "semi join"
	case AntiSemiJoin:
		return "anti semi join"
	case LeftOuterSemiJoin:
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		return "anti left outer semi join"
	}
	return "unsupported join type"
}

const (
	preferLeftAsOuter = 1 << iota
	preferRightAsOuter
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	*basePlan
	baseLogicalPlan

	JoinType        JoinType
	reordered       bool
	cartesianJoin   bool
	preferINLJ      int
	preferMergeJoin bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	LeftJoinKeys    []*expression.Column
	RightJoinKeys   []*expression.Column
	leftProperties  [][]*expression.Column
	rightProperties [][]*expression.Column

	// DefaultValues is only used for outer join, which stands for the default values when the outer table cannot find join partner
	// instead of null padding.
	DefaultValues []types.Datum

	// redundantSchema contains columns which are eliminated in join.
	// For select * from a join b using (c); a.c will in output schema, and b.c will in redundantSchema.
	redundantSchema *expression.Schema
}

func (p *LogicalJoin) columnSubstitute(schema *expression.Schema, exprs []expression.Expression) {
	for i, fun := range p.EqualConditions {
		p.EqualConditions[i] = expression.ColumnSubstitute(fun, schema, exprs).(*expression.ScalarFunction)
	}
	for i, fun := range p.LeftConditions {
		p.LeftConditions[i] = expression.ColumnSubstitute(fun, schema, exprs)
	}
	for i, fun := range p.RightConditions {
		p.RightConditions[i] = expression.ColumnSubstitute(fun, schema, exprs)
	}
	for i, fun := range p.OtherConditions {
		p.OtherConditions[i] = expression.ColumnSubstitute(fun, schema, exprs)
	}
}

func (p *LogicalJoin) attachOnConds(onConds []expression.Expression) {
	eq, left, right, other := extractOnCondition(onConds, p.children[0].(LogicalPlan), p.children[1].(LogicalPlan))
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

func (p *LogicalJoin) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, extractCorColumns(fun)...)
	}
	return corCols
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	*basePlan
	baseLogicalPlan

	Exprs []expression.Expression

	// calculateGenCols indicates the projection is for calculating generated columns.
	// In *UPDATE*, we should know this to tell different projections.
	calculateGenCols bool
}

func (p *LogicalProjection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range p.Exprs {
		corCols = append(corCols, extractCorColumns(expr)...)
	}
	return corCols
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	*basePlan
	baseLogicalPlan

	AggFuncs     []aggregation.Aggregation
	GroupByItems []expression.Expression
	// groupByCols stores the columns that are group-by items.
	groupByCols []*expression.Column

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.
}

func (p *LogicalAggregation) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, expr := range p.GroupByItems {
		corCols = append(corCols, extractCorColumns(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.GetArgs() {
			corCols = append(corCols, extractCorColumns(arg)...)
		}
	}
	return corCols
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	*basePlan
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

func (p *LogicalSelection) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, cond := range p.Conditions {
		corCols = append(corCols, extractCorColumns(cond)...)
	}
	return corCols
}

// LogicalApply gets one row from outer executor and gets one row from inner executor according to outer row.
type LogicalApply struct {
	LogicalJoin

	corCols []*expression.CorrelatedColumn
}

func (p *LogicalApply) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.LogicalJoin.extractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if p.children[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

// LogicalExists checks if a query returns result.
type LogicalExists struct {
	*basePlan
	baseLogicalPlan
}

// LogicalMaxOneRow checks if a query returns no more than one row.
type LogicalMaxOneRow struct {
	*basePlan
	baseLogicalPlan
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	*basePlan
	baseLogicalPlan

	RowCount int
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	*basePlan
	baseLogicalPlan

	conditions []expression.Expression
}

// DataSource represents a tablescan without condition push down.
type DataSource struct {
	*basePlan
	baseLogicalPlan

	indexHints []*ast.IndexHint
	tableInfo  *model.TableInfo
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr

	LimitCount *int64

	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression

	statisticTable *statistics.Table

	// availableIndices is used for storing result of avalableIndices function.
	availableIndices *avalableIndices
}

type avalableIndices struct {
	indices          []*model.IndexInfo
	includeTableScan bool
}

func (p *DataSource) getPKIsHandleCol() *expression.Column {
	if !p.tableInfo.PKIsHandle {
		return nil
	}
	for i, col := range p.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			return p.schema.Columns[i]
		}
	}
	return nil
}

// TableInfo returns the *TableInfo of data source.
func (p *DataSource) TableInfo() *model.TableInfo {
	return p.tableInfo
}

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	*basePlan
	baseLogicalPlan
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	*basePlan
	baseLogicalPlan

	ByItems []*ByItems
}

func (p *LogicalSort) extractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.baseLogicalPlan.extractCorrelatedCols()
	for _, item := range p.ByItems {
		corCols = append(corCols, extractCorColumns(item.Expr)...)
	}
	return corCols
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	*basePlan
	baseLogicalPlan

	ByItems []*ByItems
	Offset  uint64
	Count   uint64

	// partial is true if this topn is generated by push-down optimization.
	partial bool
}

// isLimit checks if TopN is a limit plan.
func (t *LogicalTopN) isLimit() bool {
	return len(t.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	*basePlan
	baseLogicalPlan

	Offset uint64
	Count  uint64

	// partial is true if this topn is generated by push-down optimization.
	partial bool
}

// LogicalLock represents a select lock plan.
type LogicalLock struct {
	*basePlan
	baseLogicalPlan

	Lock ast.SelectLockType
}
