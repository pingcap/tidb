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
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

// TableRange represents a range of row handle.
type TableRange struct {
	LowVal  int64
	HighVal int64
}

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	basePlan
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
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
func (ir *IndexRange) IsPoint(sc *variable.StatementContext) bool {
	if len(ir.LowVal) != len(ir.HighVal) {
		return false
	}
	for i := range ir.LowVal {
		a := ir.LowVal[i]
		b := ir.HighVal[i]
		if a.Kind() == types.KindMinNotNull || b.Kind() == types.KindMaxValue {
			return false
		}
		cmp, err := a.CompareDatum(sc, b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}
	}
	return !ir.LowExclude && !ir.HighExclude
}

func (ir *IndexRange) String() string {
	lowStrs := make([]string, 0, len(ir.LowVal))
	for _, d := range ir.LowVal {
		if d.Kind() == types.KindMinNotNull {
			lowStrs = append(lowStrs, "-inf")
		} else {
			lowStrs = append(lowStrs, fmt.Sprintf("%v", d.GetValue()))
		}
	}
	highStrs := make([]string, 0, len(ir.LowVal))
	for _, d := range ir.HighVal {
		if d.Kind() == types.KindMaxValue {
			highStrs = append(highStrs, "+inf")
		} else {
			highStrs = append(highStrs, fmt.Sprintf("%v", d.GetValue()))
		}
	}
	l, r := "[", "]"
	if ir.LowExclude {
		l = "("
	}
	if ir.HighExclude {
		r = ")"
	}
	return l + strings.Join(lowStrs, " ") + "," + strings.Join(highStrs, " ") + r
}

// SelectLock represents a select lock plan.
type SelectLock struct {
	baseLogicalPlan

	Lock ast.SelectLockType
}

// Limit represents offset and limit plan.
type Limit struct {
	baseLogicalPlan

	Offset uint64
	Count  uint64
}

// Distinct represents Distinct plan.
type Distinct struct {
	baseLogicalPlan
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
	ID        uint32
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	basePlan

	Name string
}

// Show represents a show plan.
type Show struct {
	baseLogicalPlan

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
	baseLogicalPlan

	Table       table.Table
	tableSchema expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	Setlist     []*expression.Assignment
	OnDuplicate []*expression.Assignment

	IsReplace bool
	Priority  int
	Ignore    bool
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
