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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/ranger"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	basePlan
}

// ShowDDLJobs is for showing DDL job list.
type ShowDDLJobs struct {
	basePlan
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	basePlan

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	basePlan

	JobIDs []int64
}

// Prepare represents prepare plan.
type Prepare struct {
	basePlan

	Name    string
	SQLText string
}

// Prepared represents a prepared statement.
type Prepared struct {
	Stmt          ast.StmtNode
	Params        []*ast.ParamMarkerExpr
	SchemaVersion int64
	UseCache      bool
}

// Execute represents prepare plan.
type Execute struct {
	basePlan

	Name      string
	UsingVars []expression.Expression
	ExecID    uint32
	Stmt      ast.StmtNode
	Plan      Plan
}

func (e *Execute) optimizePreparedPlan(ctx context.Context, is infoschema.InfoSchema) error {
	vars := ctx.GetSessionVars()
	if e.Name != "" {
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	v := vars.PreparedStmts[e.ExecID]
	if v == nil {
		return errors.Trace(ErrStmtNotFound)
	}
	prepared := v.(*Prepared)

	if len(prepared.Params) != len(e.UsingVars) {
		return errors.Trace(ErrWrongParamCount)
	}

	if cap(vars.PreparedParams) < len(e.UsingVars) {
		vars.PreparedParams = make([]interface{}, len(e.UsingVars))
	}
	for i, usingVar := range e.UsingVars {
		val, err := usingVar.Eval(nil)
		if err != nil {
			return errors.Trace(err)
		}
		prepared.Params[i].SetDatum(val)
		vars.PreparedParams[i] = val
	}
	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := Preprocess(ctx, prepared.Stmt, is, true)
		if err != nil {
			return ErrSchemaChanged.Gen("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	p, err := e.getPhysicalPlan(ctx, is, prepared)
	if err != nil {
		return errors.Trace(err)
	}
	e.Stmt = prepared.Stmt
	e.Plan = p
	return nil
}

func (e *Execute) getPhysicalPlan(ctx context.Context, is infoschema.InfoSchema, prepared *Prepared) (Plan, error) {
	var cacheKey kvcache.Key
	sessionVars := ctx.GetSessionVars()
	sessionVars.StmtCtx.UseCache = prepared.UseCache
	if prepared.UseCache {
		cacheKey = NewPSTMTPlanCacheKey(sessionVars, e.ExecID, prepared.SchemaVersion)
		if cacheValue, exists := ctx.PreparedPlanCache().Get(cacheKey); exists {
			plan := cacheValue.(*PSTMTPlanCacheValue).Plan
			err := e.rebuildRange(plan)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return plan, nil
		}
	}
	p, err := Optimize(ctx, prepared.Stmt, is)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if prepared.UseCache {
		ctx.PreparedPlanCache().Put(cacheKey, NewPSTMTPlanCacheValue(p))
	}
	return p, err
}

func (e *Execute) rebuildRange(p Plan) error {
	sc := p.context().GetSessionVars().StmtCtx
	switch x := p.(type) {
	case *PhysicalTableReader:
		ts := x.TablePlans[0].(*PhysicalTableScan)
		cols := expression.ColumnInfos2ColumnsWithDBName(ts.DBName, ts.Table.Name, ts.Columns)
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				pkCol = expression.ColInfo2Col(cols, pkColInfo)
			}
		}
		newRanges := ranger.FullIntRange()
		if pkCol != nil {
			ranges, err := ranger.BuildRange(sc, ts.AccessCondition, ranger.IntRangeType, []*expression.Column{pkCol}, nil)
			if err != nil {
				return errors.Trace(err)
			}
			newRanges = ranger.Ranges2IntRanges(ranges)
		}
		ts.Ranges = newRanges
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		var err error
		is.Ranges, err = e.buildRangeForIndexScan(sc, is)
		if err != nil {
			return errors.Trace(err)
		}
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		var err error
		is.Ranges, err = e.buildRangeForIndexScan(sc, is)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		var err error
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *Execute) buildRangeForIndexScan(sc *stmtctx.StatementContext, is *PhysicalIndexScan) ([]*ranger.IndexRange, error) {
	cols := expression.ColumnInfos2ColumnsWithDBName(is.DBName, is.Table.Name, is.Columns)
	idxCols, colLengths := expression.IndexInfo2Cols(cols, is.Index)
	newRanges := ranger.FullIndexRange()
	if len(idxCols) > 0 {
		ranges, err := ranger.BuildRange(sc, is.AccessCondition, ranger.IndexRangeType, idxCols, colLengths)
		if err != nil {
			return nil, errors.Trace(err)
		}
		newRanges = ranger.Ranges2IndexRanges(ranges)
	}
	return newRanges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	basePlan

	Name string
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
	User   *auth.UserIdentity // Used for show grants.

	Conditions []expression.Expression

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

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	basePlan

	Table       table.Table
	tableSchema *expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	Setlist     []*expression.Assignment
	OnDuplicate []*expression.Assignment

	IsReplace bool
	Priority  mysql.PriorityEnum
	IgnoreErr bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	GenCols InsertGeneratedColumns

	SelectPlan PhysicalPlan
}

// Update represents Update plan.
type Update struct {
	basePlan

	OrderedList []*expression.Assignment
	IgnoreErr   bool

	SelectPlan PhysicalPlan
}

// Delete represents a delete plan.
type Delete struct {
	basePlan

	Tables       []*ast.TableName
	IsMultiTable bool

	SelectPlan PhysicalPlan
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

	GenCols InsertGeneratedColumns
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
	Rows           [][]string
	explainedPlans map[int]bool
}

// prepareExplainInfo4DAGTask generates the following information for every plan:
// ["id", "parents", "task", "operator info"].
func (e *Explain) prepareExplainInfo4DAGTask(p PhysicalPlan, taskType string, parentID string) {
	childrenIDs := make([]string, 0, len(p.Children()))
	for _, ch := range p.Children() {
		childrenIDs = append(childrenIDs, ch.ExplainID())
	}
	childrenInfo := strings.Join(childrenIDs, ",")
	operatorInfo := p.ExplainInfo()
	count := string(strconv.AppendFloat([]byte{}, p.StatsInfo().count, 'f', -1, 64))
	row := []string{p.ExplainID(), parentID, childrenInfo, taskType, operatorInfo, count}
	e.Rows = append(e.Rows, row)
}

// prepareCopTaskInfo generates explain information for cop-tasks.
// Only PhysicalTableReader, PhysicalIndexReader and PhysicalIndexLookUpReader have cop-tasks currently.
func (e *Explain) prepareCopTaskInfo(plans []PhysicalPlan) {
	for i, p := range plans {
		var parentID string
		if i+1 < len(plans) {
			parentID = plans[i+1].ExplainID()
		}
		e.prepareExplainInfo4DAGTask(p, "cop", parentID)
	}
}

// prepareRootTaskInfo generates explain information for root-tasks.
func (e *Explain) prepareRootTaskInfo(p PhysicalPlan, parentID string) {
	e.explainedPlans[p.ID()] = true
	for _, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		e.prepareRootTaskInfo(child.(PhysicalPlan), p.ExplainID())
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
	e.prepareExplainInfo4DAGTask(p, "root", parentID)
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	buffer.WriteString(fmt.Sprintf("\ndigraph %s {\n", p.ExplainID()))
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString(fmt.Sprintln("}"))

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("subgraph cluster%v{\n", p.ID()))
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	buffer.WriteString(fmt.Sprintf("label = \"%s\"\n", taskTp))

	if len(p.Children()) == 0 {
		buffer.WriteString(fmt.Sprintf("\"%s\"\n}\n", p.ExplainID()))
		return
	}

	copTasks := []Plan{}
	pipelines := []string{}

	for planQueue := []Plan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		}
		for _, child := range curPlan.Children() {
			buffer.WriteString(fmt.Sprintf("\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID()))
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop.(PhysicalPlan), "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}
