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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// CrossJoin means Cartesian Product, but not used now.
	CrossJoin JoinType = iota
	// InnerJoin means inner join.
	InnerJoin
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

	JoinType JoinType
	anti     bool

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

// NewTableDual represents a dual table plan.
type NewTableDual struct {
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

	TableAsName *model.CIStr

	LimitCount *int64
}

// Trim trims child's rows.
type Trim struct {
	baseLogicalPlan
}

// NewUnion represents Union plan.
type NewUnion struct {
	baseLogicalPlan

	Selects []LogicalPlan
}

// NewSort stands for the order by plan.
type NewSort struct {
	baseLogicalPlan

	ByItems []*ByItems

	ExecLimit *Limit
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
