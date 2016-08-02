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
	basePlan

	Table      *model.TableInfo
	Index      *model.IndexInfo
	Ranges     []*IndexRange
	Columns    []*model.ColumnInfo
	DBName     *model.CIStr
	Desc       bool
	OutOfOrder bool
	DoubleRead bool

	accessEqualCount int
	AccessCondition  []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	basePlan

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  *model.CIStr
	Desc    bool
	Ranges  []TableRange
	pkCol   *expression.Column

	AccessCondition []expression.Expression

	TableAsName *model.CIStr

	LimitCount *int64
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	basePlan

	InnerPlan   PhysicalPlan
	OuterSchema expression.Schema
	Checker     *ApplyConditionChecker
}

// PhysicalHashJoin represents hash join for inner/ outer join.
type PhysicalHashJoin struct {
	basePlan

	JoinType JoinType

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
	SmallTable      int
}

// PhysicalHashSemiJoin represents hash join for semi join.
type PhysicalHashSemiJoin struct {
	basePlan

	WithAux bool
	Anti    bool

	EqualConditions []*expression.ScalarFunction
	LeftConditions  []expression.Expression
	RightConditions []expression.Expression
	OtherConditions []expression.Expression
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalIndexScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalTableScan) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalApply) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashSemiJoin) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *PhysicalHashJoin) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Distinct) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Selection) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Projection) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Exists) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *MaxOneRow) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Insert) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Limit) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *NewUnion) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *NewSort) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *NewTableDual) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Trim) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *SelectLock) Copy() PhysicalPlan {
	np := *p
	return &np
}

// Copy implements the PhysicalPlan Copy interface.
func (p *Aggregation) Copy() PhysicalPlan {
	np := *p
	return &np
}
