package plan

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

// PhysicalIndexScan represents a index scan plan.
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
