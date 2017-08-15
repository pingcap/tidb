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
	"encoding/json"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/types"
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
	User   *auth.UserIdentity // Used for show grants.

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
	Priority  mysql.PriorityEnum
	Ignore    bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	TableInfo *model.TableInfo
	PKInfo    *model.ColumnInfo
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

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
}

// LoadData represents a loaddata plan.
type LoadData struct {
	basePlan

	IsLocal    bool
	Path       string
	Table      *ast.TableName
	Columns    []*ast.ColumnName
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

	StmtPlan       Plan
	Rows           [][]types.Datum
	explainedPlans map[string]bool
}

func (e *Explain) prepareExplainInfo(p Plan, parent Plan) error {
	for _, child := range p.Children() {
		err := e.prepareExplainInfo(child, p)
		if err != nil {
			return errors.Trace(err)
		}
	}
	explain, err := json.MarshalIndent(p, "", "    ")
	if err != nil {
		return errors.Trace(err)
	}
	parentStr := ""
	if parent != nil {
		parentStr = parent.ID()
	}
	row := types.MakeDatums(p.ID(), string(explain), parentStr)
	e.Rows = append(e.Rows, row)
	return nil
}

// prepareExplainInfo4DAGTask generates the following information for every plan:
// ["id", "parents", "task", "operator info"].
func (e *Explain) prepareExplainInfo4DAGTask(p PhysicalPlan, taskType string) {
	parents := p.Parents()
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ID())
	}
	childrenIDs := make([]string, 0, len(p.Children()))
	for _, ch := range p.Children() {
		childrenIDs = append(childrenIDs, ch.ID())
	}
	parentInfo := strings.Join(parentIDs, ",")
	childrenInfo := strings.Join(childrenIDs, ",")
	operatorInfo := p.ExplainInfo()
	count := p.statsProfile().count
	row := types.MakeDatums(p.ID(), parentInfo, childrenInfo, taskType, operatorInfo, count)
	e.Rows = append(e.Rows, row)
}

// prepareCopTaskInfo generates explain information for cop-tasks.
// Only PhysicalTableReader, PhysicalIndexReader and PhysicalIndexLookUpReader have cop-tasks currently.
func (e *Explain) prepareCopTaskInfo(plans []PhysicalPlan) {
	for _, p := range plans {
		e.prepareExplainInfo4DAGTask(p, "cop")
	}
}

// prepareRootTaskInfo generates explain information for root-tasks.
func (e *Explain) prepareRootTaskInfo(p PhysicalPlan) {
	e.explainedPlans[p.ID()] = true
	for _, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		e.prepareRootTaskInfo(child.(PhysicalPlan))
	}
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		e.prepareCopTaskInfo(copPlan.TablePlans)
	case *PhysicalIndexReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
	case *PhysicalIndexLookUpReader:
		e.prepareCopTaskInfo(copPlan.IndexPlans)
		e.prepareCopTaskInfo(copPlan.TablePlans)
	}
	e.prepareExplainInfo4DAGTask(p, "root")
}
