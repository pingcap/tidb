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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	basePlan
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	basePlan

	Tables []*ast.TableName
}

// SelectLock represents a select lock plan.
type SelectLock struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Lock ast.SelectLockType
}

// Limit represents offset and limit plan.
type Limit struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Offset uint64
	Count  uint64
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
	UsingVars []expression.Expression
	ExecID    uint32
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	basePlan

	Name string
}

// Show represents a show plan.
type Show struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

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

// Set represents a plan for set stmt.
type Set struct {
	basePlan

	VarAssigns []*expression.VarAssignment
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	basePlan

	Statement ast.StmtNode
}

// Insert represents an insert plan.
type Insert struct {
	*basePlan
	baseLogicalPlan
	basePhysicalPlan

	Table       table.Table
	tableSchema *expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	Setlist     []*expression.Assignment
	OnDuplicate []*expression.Assignment

	IsReplace bool
	Priority  int
	Ignore    bool
}

// AnalyzePKTask is used for analyze pk. Used only when pk is handle.
type AnalyzePKTask struct {
	TableInfo *model.TableInfo
	PKInfo    *model.ColumnInfo
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	TableInfo *model.TableInfo
	ColsInfo  []*model.ColumnInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	TableInfo *model.TableInfo
	IndexInfo *model.IndexInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	basePlan

	PkTasks  []AnalyzePKTask
	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
}

// LoadData represents a loaddata plan.
type LoadData struct {
	basePlan

	IsLocal    bool
	Path       string
	Table      *ast.TableName
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause
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
