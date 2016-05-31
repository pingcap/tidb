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
	// TODO: support semi join.
)

// Join is the logical join plan.
type Join struct {
	basePlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}

// Projection represents a select fields plan.
type Projection struct {
	basePlan
	exprs []expression.Expression
}

// Aggregation represents an aggregate plan.
type Aggregation struct {
	basePlan
	AggFuncs     []expression.AggregationFunction
	GroupByItems []expression.Expression
}

// Selection means a filter.
type Selection struct {
	basePlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

// NewTableScan represents a tablescan without condition push down.
type NewTableScan struct {
	basePlan

	Table  *model.TableInfo
	Desc   bool
	Ranges []TableRange

	// RefAccess indicates it references a previous joined table, used in explain.
	RefAccess bool

	// AccessConditions can be used to build index range.
	AccessConditions []expression.Expression

	// FilterConditions can be used to filter result.
	FilterConditions []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

func (ts *NewTableScan) attachCondition(conditions []expression.Expression) {
	for _, con := range conditions {
		//TODO: implement refiner for expression.
		ts.FilterConditions = append(ts.FilterConditions, con)
	}
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
	if len(parents) != 1 || len(children) != 1 {
		return SystemInternalErrorType.Gen("can't remove this plan")
	}
	parent, child := parents[0], children[0]
	err := parent.ReplaceChild(p, child)
	if err != nil {
		return errors.Trace(err)
	}
	err = child.ReplaceParent(p, parent)
	return errors.Trace(err)
}
