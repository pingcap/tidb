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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan/statistics"
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
	// SemiJoinWithAux means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
	SemiJoinWithAux
)

// Join is the logical join plan.
type Join struct {
	baseLogicalPlan

	JoinType      JoinType
	anti          bool
	reordered     bool
	cartesianJoin bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}

// Projection represents a select fields plan.
type Projection struct {
	baseLogicalPlan
	Exprs []expression.Expression
}

// Aggregation represents an aggregate plan.
type Aggregation struct {
	baseLogicalPlan
	// TODO: implement hash aggregation and streamed aggregation
	AggFuncs     []expression.AggregationFunction
	GroupByItems []expression.Expression
}

// Selection means a filter.
type Selection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

// Apply gets one row from outer executor and gets one row from inner executor according to outer row.
type Apply struct {
	baseLogicalPlan

	InnerPlan   LogicalPlan
	OuterSchema expression.Schema
	Checker     *ApplyConditionChecker
	// outerColumns is the columns that not belong to this plan.
	outerColumns []*expression.Column
}

// Exists checks if a query returns result.
type Exists struct {
	baseLogicalPlan
}

// MaxOneRow checks if a query returns no more than one row.
type MaxOneRow struct {
	baseLogicalPlan
}

// TableDual represents a dual table plan.
type TableDual struct {
	baseLogicalPlan
}

// DataSource represents a tablescan without condition push down.
type DataSource struct {
	baseLogicalPlan

	table   *ast.TableName
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  *model.CIStr
	Desc    bool
	ctx     context.Context

	TableAsName *model.CIStr

	LimitCount *int64

	statisticTable *statistics.Table
}

// Trim trims child's rows.
type Trim struct {
	baseLogicalPlan
}

// Union represents Union plan.
type Union struct {
	baseLogicalPlan
}

// Sort stands for the order by plan.
type Sort struct {
	baseLogicalPlan

	ByItems []*ByItems

	ExecLimit *Limit
}

// Update represents Update plan.
type Update struct {
	baseLogicalPlan

	SelectPlan  Plan
	OrderedList []*expression.Assignment
}

// Delete represents a delete plan.
type Delete struct {
	baseLogicalPlan

	SelectPlan   Plan
	Tables       []*ast.TableName
	IsMultiTable bool
}

// AddChild for parent.
func addChild(parent Plan, child Plan) {
	if child == nil || parent == nil {
		return
	}
	child.AddParent(parent)
	parent.AddChild(child)
}

// InsertPlan means inserting plan between two plans.
func InsertPlan(parent Plan, child Plan, insert Plan) error {
	err := child.ReplaceParent(parent, insert)
	if err != nil {
		return errors.Trace(err)
	}
	err = parent.ReplaceChild(child, insert)
	if err != nil {
		return errors.Trace(err)
	}
	insert.AddChild(child)
	insert.AddParent(parent)
	return nil
}

// RemovePlan means removing a plan.
func RemovePlan(p Plan) error {
	parents := p.GetParents()
	children := p.GetChildren()
	if len(parents) > 1 || len(children) != 1 {
		return SystemInternalErrorType.Gen("can't remove this plan")
	}
	if len(parents) == 0 {
		child := children[0]
		child.SetParents()
		return nil
	}
	parent, child := parents[0], children[0]
	err := parent.ReplaceChild(p, child)
	if err != nil {
		return errors.Trace(err)
	}
	err = child.ReplaceParent(p, parent)
	return errors.Trace(err)
}
