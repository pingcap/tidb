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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/ranger"
)

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowDDLJobs is for showing DDL job list.
type ShowDDLJobs struct {
	baseSchemaProducer

	JobNumber int64
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CheckIndex is used for checking index data, built from the 'admin check index' statement.
type CheckIndex struct {
	baseSchemaProducer

	IndexLookUpReader *PhysicalIndexLookUpReader
	DBName            string
	IdxName           string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

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
	baseSchemaProducer

	Name      string
	UsingVars []expression.Expression
	ExecID    uint32
	Stmt      ast.StmtNode
	Plan      Plan
}

func (e *Execute) optimizePreparedPlan(ctx sessionctx.Context, is infoschema.InfoSchema) error {
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
		val, err := usingVar.Eval(chunk.Row{})
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

func (e *Execute) getPhysicalPlan(ctx sessionctx.Context, is infoschema.InfoSchema, prepared *Prepared) (Plan, error) {
	var cacheKey kvcache.Key
	sessionVars := ctx.GetSessionVars()
	sessionVars.StmtCtx.UseCache = prepared.UseCache
	if prepared.UseCache {
		cacheKey = NewPSTMTPlanCacheKey(sessionVars, e.ExecID, prepared.SchemaVersion)
		if cacheValue, exists := ctx.PreparedPlanCache().Get(cacheKey); exists {
			metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
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
	sctx := p.context()
	sc := p.context().GetSessionVars().StmtCtx
	switch x := p.(type) {
	case *PhysicalTableReader:
		ts := x.TablePlans[0].(*PhysicalTableScan)
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			}
		}
		if pkCol != nil {
			var err error
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sc, pkCol.RetType)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			ts.Ranges = ranger.FullIntRange(false)
		}
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		var err error
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return errors.Trace(err)
		}
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		var err error
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return errors.Trace(err)
		}
	case PhysicalPlan:
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

func (e *Execute) buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) ([]*ranger.Range, error) {
	idxCols, colLengths := expression.IndexInfo2Cols(is.schema.Columns, is.Index)
	ranges := ranger.FullRange()
	if len(idxCols) > 0 {
		var err error
		ranges, _, _, _, err = ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, idxCols, colLengths)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return ranges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Show represents a show plan.
type Show struct {
	baseSchemaProducer

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
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

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
	baseSchemaProducer

	Table       table.Table
	tableSchema *expression.Schema
	Columns     []*ast.ColumnName
	Lists       [][]expression.Expression
	SetList     []*expression.Assignment

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	GenCols InsertGeneratedColumns

	SelectPlan PhysicalPlan
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	SelectPlan PhysicalPlan
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	Tables       []*ast.TableName
	IsMultiTable bool

	SelectPlan PhysicalPlan
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	PhysicalTableID int64
	PKInfo          *model.ColumnInfo
	ColsInfo        []*model.ColumnInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	// PhysicalTableID is the id for a partition or a table.
	PhysicalTableID int64
	IndexInfo       *model.IndexInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal    bool
	Path       string
	Table      *ast.TableName
	Columns    []*ast.ColumnName
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause

	GenCols InsertGeneratedColumns
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	StmtPlan       Plan
	Rows           [][]string
	explainedPlans map[int]bool
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p PhysicalPlan, taskType, indent string, isLastChild bool) {
	e.prepareOperatorInfo(p, taskType, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := e.getIndent4Child(indent, isLastChild)
	for i, child := range p.Children() {
		if e.explainedPlans[child.ID()] {
			continue
		}
		e.explainPlanInRowFormat(child.(PhysicalPlan), taskType, childIndent, i == len(p.Children())-1)
	}

	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		e.explainPlanInRowFormat(copPlan.tablePlan, "cop", childIndent, true)
	case *PhysicalIndexReader:
		e.explainPlanInRowFormat(copPlan.indexPlan, "cop", childIndent, true)
	case *PhysicalIndexLookUpReader:
		e.explainPlanInRowFormat(copPlan.indexPlan, "cop", childIndent, false)
		e.explainPlanInRowFormat(copPlan.tablePlan, "cop", childIndent, true)
	}
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, task type, operator info, and the estemated row count.
func (e *Explain) prepareOperatorInfo(p PhysicalPlan, taskType string, indent string, isLastChild bool) {
	operatorInfo := p.ExplainInfo()
	count := string(strconv.AppendFloat([]byte{}, p.statsInfo().count, 'f', 2, 64))
	row := []string{e.prettyIdentifier(p.ExplainID(), indent, isLastChild), count, taskType, operatorInfo}
	e.Rows = append(e.Rows, row)
}

const (
	// treeBody indicates the current operator sub-tree is not finished, still
	// has child operators to be attached on.
	treeBody = '│'
	// treeMiddleNode indicates this operator is not the last child of the
	// current sub-tree rooted by its parent.
	treeMiddleNode = '├'
	// treeLastNode indicates this operator is the last child of the current
	// sub-tree rooted by its parent.
	treeLastNode = '└'
	// treeGap is used to represent the gap between the branches of the tree.
	treeGap = ' '
	// treeNodeIdentifier is used to replace the treeGap once we need to attach
	// a node to a sub-tree.
	treeNodeIdentifier = '─'
)

func (e *Explain) prettyIdentifier(id, indent string, isLastChild bool) string {
	if len(indent) == 0 {
		return id
	}

	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		if indentBytes[i] != treeBody {
			continue
		}

		// Here we attach a new node to the current sub-tree by changing
		// the closest treeBody to a:
		// 1. treeLastNode, if this operator is the last child.
		// 2. treeMiddleNode, if this operator is not the last child..
		if isLastChild {
			indentBytes[i] = treeLastNode
		} else {
			indentBytes[i] = treeMiddleNode
		}
		break
	}

	// Replace the treeGap between the treeBody and the node to a
	// treeNodeIdentifier.
	indentBytes[len(indentBytes)-1] = treeNodeIdentifier
	return string(indentBytes) + id
}

func (e *Explain) getIndent4Child(indent string, isLastChild bool) string {
	if !isLastChild {
		return string(append([]rune(indent), treeBody, treeGap))
	}

	// If the current node is the last node of the current operator tree, we
	// need to end this sub-tree by changing the closest treeBody to a treeGap.
	indentBytes := []rune(indent)
	for i := len(indentBytes) - 1; i >= 0; i-- {
		if indentBytes[i] == treeBody {
			indentBytes[i] = treeGap
			break
		}
	}

	return string(append(indentBytes, treeBody, treeGap))
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

	var copTasks []PhysicalPlan
	var pipelines []string

	for planQueue := []PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
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
