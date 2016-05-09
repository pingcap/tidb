// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

// TableRange represents a range of row handle.
type TableRange struct {
	LowVal  int64
	HighVal int64
}

// TableDual represents a dual table plan.
type TableDual struct {
	basePlan

	HasAgg bool
	// FilterConditions can be used to filter result.
	FilterConditions []ast.ExprNode
}

// TableScan represents a table scan plan.
type TableScan struct {
	basePlan

	Table  *model.TableInfo
	Desc   bool
	Ranges []TableRange

	// RefAccess indicates it references a previous joined table, used in explain.
	RefAccess bool

	// AccessConditions can be used to build index range.
	AccessConditions []ast.ExprNode

	// FilterConditions can be used to filter result.
	FilterConditions []ast.ExprNode

	// TableName is used to distinguish the same table selected multiple times in different place,
	// like 'select * from t where exists(select 1 from t as x where t.c < x.c)'
	TableName *ast.TableName

	TableAsName *model.CIStr

	LimitCount *int64
}

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	basePlan
}

// CheckTable is for checking table data.
type CheckTable struct {
	basePlan

	Tables []*ast.TableName
}

// IndexRange represents an index range to be scanned.
type IndexRange struct {
	LowVal      []types.Datum
	LowExclude  bool
	HighVal     []types.Datum
	HighExclude bool
}

// IsPoint returns if the index range is a point.
func (ir *IndexRange) IsPoint() bool {
	if len(ir.LowVal) != len(ir.HighVal) {
		return false
	}
	for i := range ir.LowVal {
		a := ir.LowVal[i]
		b := ir.HighVal[i]
		if a.Kind() == types.KindMinNotNull || b.Kind() == types.KindMaxValue {
			return false
		}
		cmp, err := a.CompareDatum(b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}
	}
	return !ir.LowExclude && !ir.HighExclude
}

// IndexScan represents an index scan plan.
type IndexScan struct {
	basePlan

	// The index used.
	Index *model.IndexInfo

	// The table to lookup.
	Table *model.TableInfo

	// Ordered and non-overlapping ranges to be scanned.
	Ranges []*IndexRange

	// Desc indicates whether the index should be scanned in descending order.
	Desc bool

	// RefAccess indicates it references a previous joined table, used in explain.
	RefAccess bool

	// AccessConditions can be used to build index range.
	AccessConditions []ast.ExprNode

	// Number of leading equal access condition.
	// The offset of each equal condition correspond to the offset of index column.
	// For example, an index has column (a, b, c), condition is 'a = 0 and b = 0 and c > 0'
	// AccessEqualCount would be 2.
	AccessEqualCount int

	// FilterConditions can be used to filter result.
	FilterConditions []ast.ExprNode

	// OutOfOrder indicates if the index scan can return out of order.
	OutOfOrder bool

	// NoLimit indicates that this plan need fetch all the rows.
	NoLimit bool

	// TableName is used to distinguish the same table selected multiple times in different place,
	// like 'select * from t where exists(select 1 from t as x where t.c < x.c)'
	TableName *ast.TableName

	TableAsName *model.CIStr

	LimitCount *int64
}

// JoinOuter represents outer join plan.
type JoinOuter struct {
	basePlan

	Outer Plan
	Inner Plan
}

// JoinInner represents inner join plan.
type JoinInner struct {
	basePlan

	Inners     []Plan
	Conditions []ast.ExprNode
}

func (p *JoinInner) String() string {
	return fmt.Sprintf("JoinInner()")
}

// SelectLock represents a select lock plan.
type SelectLock struct {
	basePlan

	Lock ast.SelectLockType
}

// SetLimit implements Plan SetLimit interface.
func (p *SelectLock) SetLimit(limit float64) {
	p.limit = limit
	p.GetChildByIndex(0).SetLimit(p.limit)
}

// SelectFields represents a select fields plan.
type SelectFields struct {
	basePlan
}

// SetLimit implements Plan SetLimit interface.
func (p *SelectFields) SetLimit(limit float64) {
	p.limit = limit
	if p.GetChildByIndex(0) != nil {
		p.GetChildByIndex(0).SetLimit(limit)
	}
}

// Sort represents a sorting plan.
type Sort struct {
	basePlan

	ByItems []*ast.ByItem

	ExecLimit *Limit
}

// SetLimit implements Plan SetLimit interface.
// It set the Src limit only if it is bypassed.
// Bypass has to be determined before this get called.
func (p *Sort) SetLimit(limit float64) {
	p.limit = limit
}

// Limit represents offset and limit plan.
type Limit struct {
	basePlan

	Offset uint64
	Count  uint64
}

// SetLimit implements Plan SetLimit interface.
// As Limit itself determine the real limit,
// We just ignore the input, and set the real limit.
func (p *Limit) SetLimit(limit float64) {
	p.limit = float64(p.Offset + p.Count)
	p.GetChildByIndex(0).SetLimit(p.limit)
}

// Union represents Union plan.
type Union struct {
	basePlan

	Selects []Plan
}

// Distinct represents Distinct plan.
type Distinct struct {
	basePlan
}

// SetLimit implements Plan SetLimit interface.
func (p *Distinct) SetLimit(limit float64) {
	p.limit = limit
	if p.GetChildByIndex(0) != nil {
		p.GetChildByIndex(0).SetLimit(limit)
	}
}

// Prepare represents prepare plan.
type Prepare struct {
	basePlan

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	basePlan

	Name      string
	UsingVars []ast.ExprNode
	ID        uint32
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	basePlan

	Name string
}

// Aggregate represents a select fields plan.
type Aggregate struct {
	basePlan
	AggFuncs     []*ast.AggregateFuncExpr
	GroupByItems []*ast.ByItem
}

// SetLimit implements Plan SetLimit interface.
func (p *Aggregate) SetLimit(limit float64) {
	p.limit = limit
	if p.GetChildByIndex(0) != nil {
		p.GetChildByIndex(0).SetLimit(limit)
	}
}

// Having represents a having plan.
// The having plan should after aggregate plan.
type Having struct {
	basePlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []ast.ExprNode
}

// SetLimit implements Plan SetLimit interface.
func (p *Having) SetLimit(limit float64) {
	p.limit = limit
	// We assume 50% of the GetChildByIndex(0) row is filtered out.
	p.GetChildByIndex(0).SetLimit(limit * 2)
}

// Update represents an update plan.
type Update struct {
	basePlan

	OrderedList []*ast.Assignment // OrderedList has the same offset as TablePlan's result fields.
	SelectPlan  Plan
}

// Delete represents a delete plan.
type Delete struct {
	basePlan

	SelectPlan   Plan
	Tables       []*ast.TableName
	IsMultiTable bool
}

// Filter represents a plan that filter GetChildByIndex(0)plan result.
type Filter struct {
	basePlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []ast.ExprNode
}

// SetLimit implements Plan SetLimit interface.
func (p *Filter) SetLimit(limit float64) {
	p.limit = limit
	// We assume 50% of the GetChildByIndex(0) row is filtered out.
	p.GetChildByIndex(0).SetLimit(limit * 2)
}

// Show represents a show plan.
type Show struct {
	basePlan

	Tp     ast.ShowStmtType // Databases/Tables/Columns/....
	DBName string
	Table  *ast.TableName  // Used for showing columns.
	Column *ast.ColumnName // Used for `desc table column`.
	Flag   int             // Some flag parsed from sql, such as FULL.
	Full   bool
	User   string // Used for show grants.

	// Used by show variables
	GlobalScope bool
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	basePlan

	Statement ast.StmtNode
}

// Insert represents an insert plan.
type Insert struct {
	basePlan

	Table       *ast.TableRefsClause
	Columns     []*ast.ColumnName
	Lists       [][]ast.ExprNode
	Setlist     []*ast.Assignment
	OnDuplicate []*ast.Assignment
	SelectPlan  Plan

	IsReplace bool
	Priority  int
}

// DDL represents a DDL statement plan.
type DDL struct {
	basePlan

	Statement ast.DDLNode
}

// Explain represents a explain plan.
type Explain struct {
	basePlan

	StmtPlan Plan
}
