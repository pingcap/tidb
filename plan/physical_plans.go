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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	basePhysicalPlan

	Table      *model.TableInfo
	Index      *model.IndexInfo
	Ranges     []*IndexRange
	Columns    []*model.ColumnInfo
	DBName     *model.CIStr
	Desc       bool
	OutOfOrder bool

	accessEqualCount int
	AccessCondition  []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	baseLogicalPlan

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  *model.CIStr
	Desc    bool
	Ranges  []TableRange

	AccessCondition []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	basePhysicalPlan

	InnerPlan   PhysicalPlan
	OuterSchema expression.Schema
	Checker     *ApplyConditionChecker
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	basePhysicalPlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}

// PhysicalHashSemiJoin represents hash join for semi join.
type PhysicalHashSemiJoin struct {
	basePhysicalPlan

	WithAux bool
	Anti    bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}
