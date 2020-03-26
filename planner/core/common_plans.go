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

package core

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/texttree"
)

var planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowDDLJobs is for showing DDL job list.
type ShowDDLJobs struct {
	baseSchemaProducer

	JobNumber int64
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	DBName             string
	Table              table.Table
	IndexInfos         []*model.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
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

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptRuleBlacklist reloads the data from opt_rule_blacklist table.
type ReloadOptRuleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsAction indicate action will be taken on plugins.
type AdminPluginsAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates tidb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	Action  AdminPluginsAction
	Plugins []string
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name      string
	UsingVars []expression.Expression
	ExecID    uint32
	Stmt      ast.StmtNode
	StmtType  string
	Plan      Plan
}

// OptimizePreparedPlan optimizes the prepared statement.
func (e *Execute) OptimizePreparedPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema) error {
	vars := sctx.GetSessionVars()
	if e.Name != "" {
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	prepared, ok := vars.PreparedStmts[e.ExecID]
	if !ok {
		return errors.Trace(ErrStmtNotFound)
	}
	vars.StmtCtx.StmtType = prepared.StmtType

	if len(prepared.Params) != len(e.UsingVars) {
		return errors.Trace(ErrWrongParamCount)
	}

	for i, usingVar := range e.UsingVars {
		val, err := usingVar.Eval(chunk.Row{})
		if err != nil {
			return err
		}
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Datum = val
		param.InExecute = true
		vars.PreparedParams = append(vars.PreparedParams, val)
	}
	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := Preprocess(sctx, prepared.Stmt, is, InPrepare)
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	p, err := e.getPhysicalPlan(ctx, sctx, is, prepared)
	if err != nil {
		return err
	}
	e.Stmt = prepared.Stmt
	e.Plan = p
	return nil
}

func (e *Execute) getPhysicalPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, prepared *ast.Prepared) (Plan, error) {
	var cacheKey kvcache.Key
	sessionVars := sctx.GetSessionVars()
	sessionVars.StmtCtx.UseCache = prepared.UseCache
	if prepared.UseCache {
		cacheKey = NewPSTMTPlanCacheKey(sessionVars, e.ExecID, prepared.SchemaVersion)
		if cacheValue, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
			if metrics.ResettablePlanCacheCounterFortTest {
				metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
			} else {
				planCacheCounter.Inc()
			}
			plan := cacheValue.(*PSTMTPlanCacheValue).Plan
			err := e.rebuildRange(plan)
			if err != nil {
				return nil, err
			}
			return plan, nil
		}
	}
	p, err := OptimizeAstNode(ctx, sctx, prepared.Stmt, is)
	if err != nil {
		return nil, err
	}
<<<<<<< HEAD
	_, isTableDual := p.(*PhysicalTableDual)
	if !isTableDual && prepared.UseCache {
		sctx.PreparedPlanCache().Put(cacheKey, NewPSTMTPlanCacheValue(p))
=======
	e.names = names
	e.Plan = p
	isRange := e.isRangePartition(p)
	_, isTableDual := p.(*PhysicalTableDual)
	if !isTableDual && prepared.UseCache && !isRange {
		cached := NewPSTMTPlanCacheValue(p, names)
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
		sctx.PreparedPlanCache().Put(cacheKey, cached)
	}
	return err
}

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point update"
func (e *Execute) tryCachePointPlan(ctx context.Context, sctx sessionctx.Context,
	preparedStmt *CachedPrepareStmt, is infoschema.InfoSchema, p Plan) error {
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
	)
	switch p.(type) {
	case *PointGetPlan:
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx, p)
		names = p.OutputNames()
		if err != nil {
			return err
		}
	case *Update:
		ok, err = IsPointUpdateByAutoCommit(sctx, p)
		if err != nil {
			return err
		}
		if ok {
			// make constant expression store paramMarker
			sctx.GetSessionVars().StmtCtx.PointExec = true
			p, names, err = OptimizeAstNode(ctx, sctx, prepared.Stmt, is)
		}
	}
	if ok {
		// just cache point plan now
		prepared.CachedPlan = p
		prepared.CachedNames = names
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
>>>>>>> ec156a6... plan: do not cache plan for query on range partition table (#15697)
	}
	return p, err
}

func (e *Execute) rebuildRange(p Plan) error {
	sctx := p.context()
	sc := p.context().GetSessionVars().StmtCtx
	var err error
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
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sc, pkCol.RetType)
			if err != nil {
				return err
			}
<<<<<<< HEAD
=======
			if ts.Table.Partition != nil && ts.Table.Partition.Type == model.PartitionTypeHash {
				pID, err := rebuildNewTableIDFromTable(e.ctx, ts, sc, pkCol)
				if err != nil {
					return err
				}
				if pID != -1 {
					ts.physicalTableID = pID
				}
			}
>>>>>>> ec156a6... plan: do not cache plan for query on range partition table (#15697)
		} else {
			ts.Ranges = ranger.FullIntRange(false)
		}
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
<<<<<<< HEAD
=======
		if is.Table.Partition != nil && is.Table.Partition.Type == model.PartitionTypeHash {
			pID, err := rebuildNewTableIDFromIndex(e.ctx, is, sc)
			if err != nil {
				return err
			}
			if pID != -1 {
				is.physicalTableID = pID
			}
		}
>>>>>>> ec156a6... plan: do not cache plan for query on range partition table (#15697)
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
<<<<<<< HEAD
=======
		if is.Table.Partition != nil && is.Table.Partition.Type == model.PartitionTypeHash {
			pID, err := rebuildNewTableIDFromIndex(e.ctx, is, sc)
			if err != nil {
				return err
			}
			if pID != -1 {
				is.physicalTableID = pID
				tblScan := x.TablePlans[0].(*PhysicalTableScan)
				tblScan.physicalTableID = pID
			}
		}
>>>>>>> ec156a6... plan: do not cache plan for query on range partition table (#15697)
	case *PointGetPlan:
		if x.HandleParam != nil {
			x.Handle, err = x.HandleParam.Datum.ToInt64(sc)
			if err != nil {
				return err
			}
			return nil
		}
		for i, param := range x.IndexValueParams {
			if param != nil {
				x.IndexValues[i] = param.Datum
			}
		}
		return nil
	case PhysicalPlan:
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *Update:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	}
	return nil
}

func checkRangePartitionInfo(pi *model.PartitionInfo) bool {
	if pi != nil && pi.Type == model.PartitionTypeRange {
		return true
	}
	return false
}

// Prepare plan cache is not support query plan on range partition table.
func (e *Execute) isRangePartition(p Plan) bool {
	isRange := false
	switch x := p.(type) {
	case *PhysicalTableReader:
		ts := x.TablePlans[0].(*PhysicalTableScan)
		return checkRangePartitionInfo(ts.Table.Partition)
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		return checkRangePartitionInfo(is.Table.Partition)
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		return checkRangePartitionInfo(is.Table.Partition)
	case PhysicalPlan:
		for _, child := range x.Children() {
			if e.isRangePartition(child) {
				isRange = true
			}
		}
	}
	return isRange
}

func (e *Execute) buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) ([]*ranger.Range, error) {
	idxCols, colLengths := expression.IndexInfo2Cols(is.schema.Columns, is.Index)
	if len(idxCols) == 0 {
		return ranger.FullRange(), nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, idxCols, colLengths)
	if err != nil {
		return nil, err
	}
	return res.Ranges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Show represents a show plan.
type Show struct {
	physicalSchemaProducer

	Tp          ast.ShowStmtType // Databases/Tables/Columns/....
	DBName      string
	Table       *ast.TableName  // Used for showing columns.
	Column      *ast.ColumnName // Used for `desc table column`.
	IndexName   model.CIStr
	Flag        int // Some flag parsed from sql, such as FULL.
	Full        bool
	User        *auth.UserIdentity   // Used for show grants.
	Roles       []*auth.RoleIdentity // Used for show grants.
	IfNotExists bool                 // Used for `show create database if not exists`

	GlobalScope bool // Used by show variables
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SQLBindOpType repreents the SQL bind type
type SQLBindOpType int

const (
	// OpSQLBindCreate represents the operation to create a SQL bind.
	OpSQLBindCreate SQLBindOpType = iota
	// OpSQLBindDrop represents the operation to drop a SQL bind.
	OpSQLBindDrop
)

// SQLBindPlan represents a plan for SQL bind.
type SQLBindPlan struct {
	baseSchemaProducer

	SQLBindOp    SQLBindOpType
	NormdOrigSQL string
	BindSQL      string
	IsGlobal     bool
	BindStmt     ast.StmtNode
	Db           string
	Charset      string
	Collation    string
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

// analyzeInfo is used to store the database name, table name and partition name of analyze task.
type analyzeInfo struct {
	DBName        string
	TableName     string
	PartitionName string
	// PhysicalTableID is the id for a partition or a table.
	PhysicalTableID int64
	Incremental     bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	PKInfo   *model.ColumnInfo
	ColsInfo []*model.ColumnInfo
	TblInfo  *model.TableInfo
	analyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	analyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks      []AnalyzeColumnsTask
	IdxTasks      []AnalyzeIndexTask
	MaxNumBuckets uint64
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal     bool
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Table       *ast.TableName
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines uint64

	GenCols InsertGeneratedColumns
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	TableInfo      *model.TableInfo
	PartitionNames []model.CIStr
	IndexInfo      *model.IndexInfo
	Lower          []types.Datum
	Upper          []types.Datum
	Num            int
	ValueLists     [][]types.Datum
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
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
	Format         string
	Analyze        bool
	ExecStmt       ast.StmtNode
	ExecPlan       Plan
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		retFields := []string{"id", "count", "task", "operator info"}
		if e.Analyze {
			retFields = append(retFields, "execution info", "memory")
		}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		e.SetSchema(schema)
	case ast.ExplainFormatDOT:
		retFields := []string{"dot contents"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		e.SetSchema(schema)
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.StmtPlan == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		e.explainedPlans = map[int]bool{}
		e.explainPlanInRowFormat(e.StmtPlan.(PhysicalPlan), "root", "", true)
	case ast.ExplainFormatDOT:
		e.prepareDotInfo(e.StmtPlan.(PhysicalPlan))
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p PhysicalPlan, taskType, indent string, isLastChild bool) {
	e.prepareOperatorInfo(p, taskType, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := texttree.Indent4Child(indent, isLastChild)
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
	count := string(strconv.AppendFloat([]byte{}, p.statsInfo().RowCount, 'f', 2, 64))
	explainID := p.ExplainID().String()
	row := []string{texttree.PrettyIdentifier(explainID, indent, isLastChild), count, taskType, operatorInfo}
	if e.Analyze {
		runtimeStatsColl := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
		// So check copTaskExecDetail first and print the real cop task information if it's not empty.
		if runtimeStatsColl.ExistsCopStats(explainID) {
			row = append(row, runtimeStatsColl.GetCopStats(explainID).String())
		} else if runtimeStatsColl.ExistsRootStats(explainID) {
			row = append(row, runtimeStatsColl.GetRootStats(explainID).String())
		} else {
			row = append(row, "time:0ns, loops:0, rows:0")
		}

		tracker := e.ctx.GetSessionVars().StmtCtx.MemTracker.SearchTracker(p.ExplainID().String())
		if tracker != nil {
			row = append(row, tracker.BytesToString(tracker.MaxConsumed()))
		} else {
			row = append(row, "N/A")
		}
	}
	e.Rows = append(e.Rows, row)
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
