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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/texttree"
)

var planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
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

	Name          string
	UsingVars     []expression.Expression
	PrepareParams []types.Datum
	ExecID        uint32
	Stmt          ast.StmtNode
	StmtType      string
	Plan          Plan
}

// OptimizePreparedPlan optimizes the prepared statement.
func (e *Execute) OptimizePreparedPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema) error {
	vars := sctx.GetSessionVars()
	if e.Name != "" {
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	preparedPointer, ok := vars.PreparedStmts[e.ExecID]
	if !ok {
		return errors.Trace(ErrStmtNotFound)
	}
	preparedObj, ok := preparedPointer.(*CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	vars.StmtCtx.StmtType = prepared.StmtType

	paramLen := len(e.PrepareParams)
	if paramLen > 0 {
		// for binary protocol execute, argument is placed in vars.PrepareParams
		if len(prepared.Params) != paramLen {
			return errors.Trace(ErrWrongParamCount)
		}
		vars.PreparedParams = e.PrepareParams
		for i, val := range vars.PreparedParams {
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Datum = val
			param.InExecute = true
		}
	} else {
		// for `execute stmt using @a, @b, @c`, using value in e.UsingVars
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
	}

	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schema version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schema version like prepared plan cache key
		prepared.CachedPlan = nil
		preparedObj.Executor = nil
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		err := Preprocess(sctx, prepared.Stmt, is, InPrepare)
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	err := e.getPhysicalPlan(ctx, sctx, is, preparedObj)
	if err != nil {
		return err
	}
	e.Stmt = prepared.Stmt
	return nil
}

func (e *Execute) checkPreparedPriv(ctx context.Context, sctx sessionctx.Context,
	preparedObj *CachedPrepareStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, preparedObj.VisitInfos); err != nil {
			return err
		}
	}
	err := CheckTableLock(sctx, is, preparedObj.VisitInfos)
	return err
}

func (e *Execute) getPhysicalPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, preparedStmt *CachedPrepareStmt) error {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	prepared := preparedStmt.PreparedAst
	if prepared.CachedPlan != nil {
		// Rewriting the expression in the select.where condition  will convert its
		// type from "paramMarker" to "Constant".When Point Select queries are executed,
		// the expression in the where condition will not be evaluated,
		// so you don't need to consider whether prepared.useCache is enabled.
		plan := prepared.CachedPlan.(Plan)
		names := prepared.CachedNames.(types.NameSlice)
		err := e.rebuildRange(plan)
		if err != nil {
			return err
		}
		if metrics.ResettablePlanCacheCounterFortTest {
			metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
		} else {
			planCacheCounter.Inc()
		}
		e.names = names
		e.Plan = plan
		stmtCtx.PointExec = true
		return nil
	}
	var cacheKey kvcache.Key
	stmtCtx.UseCache = prepared.UseCache
	if prepared.UseCache {
		cacheKey = NewPSTMTPlanCacheKey(sctx.GetSessionVars(), e.ExecID, prepared.SchemaVersion)
		if cacheValue, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
			if err := e.checkPreparedPriv(ctx, sctx, preparedStmt, is); err != nil {
				return err
			}
			if metrics.ResettablePlanCacheCounterFortTest {
				metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
			} else {
				planCacheCounter.Inc()
			}
			cachedVal := cacheValue.(*PSTMTPlanCacheValue)
			err := e.rebuildRange(cachedVal.Plan)
			if err != nil {
				return err
			}
			e.names = cachedVal.OutPutNames
			e.Plan = cachedVal.Plan
			stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
			return nil
		}
	}
	p, names, err := OptimizeAstNode(ctx, sctx, prepared.Stmt, is)
	if err != nil {
		return err
	}
	err = e.tryCachePointPlan(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return err
	}
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
	}
	return err
}

func (e *Execute) rebuildRange(p Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetSessionVars().StmtCtx
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
			if ts.Table.Partition != nil && ts.Table.Partition.Type == model.PartitionTypeHash {
				pID, err := rebuildNewTableIDFromTable(e.ctx, ts, sc, pkCol)
				if err != nil {
					return err
				}
				if pID != -1 {
					ts.physicalTableID = pID
				}
			}
		} else {
			ts.Ranges = ranger.FullIntRange(false)
		}
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
		if is.Table.Partition != nil && is.Table.Partition.Type == model.PartitionTypeHash {
			pID, err := rebuildNewTableIDFromIndex(e.ctx, is, sc)
			if err != nil {
				return err
			}
			if pID != -1 {
				is.physicalTableID = pID
			}
		}
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
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
	case *PointGetPlan:
		if x.HandleParam != nil {
			x.Handle, err = x.HandleParam.Datum.ToInt64(sc)
			if err != nil {
				return err
			}
			if x.PartitionInfo != nil {
				num := x.TblInfo.Partition.Num
				pos := math.Abs(x.Handle) % int64(num)
				x.PartitionInfo = &x.TblInfo.Partition.Definitions[pos]
			}
			return nil
		}
		for i, param := range x.IndexValueParams {
			if param != nil {
				x.IndexValues[i] = param.Datum
			}
		}
		if x.PartitionInfo != nil {
			val := x.IndexValues[x.partitionColumnPos].GetInt64()
			partitionID := val % int64(x.TblInfo.Partition.Num)
			x.PartitionInfo = &x.TblInfo.Partition.Definitions[partitionID]
		}
		return nil
	case *BatchPointGetPlan:
		for i, param := range x.HandleParams {
			if param != nil {
				x.Handles[i], err = param.Datum.ToInt64(sc)
				if err != nil {
					return err
				}
			}
		}
		for i, params := range x.IndexValueParams {
			if len(params) < 1 {
				continue
			}
			for j, param := range params {
				if param != nil {
					x.IndexValues[i][j] = param.Datum
				}
			}
		}
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
	if len(is.IdxCols) == 0 {
		return ranger.FullRange(), nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxCols, is.IdxColLens)
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
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpEvolveBindings is used to evolve plan binding.
	OpEvolveBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
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

	Table         table.Table
	tableSchema   *expression.Schema
	tableColNames types.NameSlice
	Columns       []*ast.ColumnName
	Lists         [][]expression.Expression
	SetList       []*expression.Assignment

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema
	names4OnDuplicate  types.NameSlice

	GenCols InsertGeneratedColumns

	SelectPlan PhysicalPlan

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreConstant bool

	SelectPlan PhysicalPlan

	TblColPosInfos TblColPosInfoSlice
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	IsMultiTable bool

	SelectPlan PhysicalPlan

	TblColPosInfos TblColPosInfoSlice
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

	ColTasks []AnalyzeColumnsTask
	IdxTasks []AnalyzeIndexTask
	Opts     map[ast.AnalyzeOptionType]uint64
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

// IndexAdvise represents a index advise plan.
type IndexAdvise struct {
	baseSchemaProducer

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
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

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetPlan Plan
	IntoOpt    *ast.SelectIntoOption
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan Plan
	Format     string
	Analyze    bool
	ExecStmt   ast.StmtNode

	Rows           [][]string
	explainedPlans map[int]bool
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)

	switch {
	case format == ast.ExplainFormatROW && !e.Analyze:
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == ast.ExplainFormatROW && e.Analyze:
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == ast.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == ast.ExplainFormatHint:
		fieldNames = []string{"hint"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetPlan == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		e.explainedPlans = map[int]bool{}
		err := e.explainPlanInRowFormat(e.TargetPlan, "root", "", "", true)
		if err != nil {
			return err
		}
	case ast.ExplainFormatDOT:
		e.prepareDotInfo(e.TargetPlan.(PhysicalPlan))
	case ast.ExplainFormatHint:
		e.Rows = append(e.Rows, []string{GenHintsFromPhysicalPlan(e.TargetPlan)})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p Plan, taskType, driverSide, indent string, isLastChild bool) (err error) {
	e.prepareOperatorInfo(p, taskType, driverSide, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := texttree.Indent4Child(indent, isLastChild)

	if physPlan, ok := p.(PhysicalPlan); ok {
		// indicate driven side and driving side of 'join' and 'apply'
		// See issue https://github.com/pingcap/tidb/issues/14602.
		driverSideInfo := make([]string, len(physPlan.Children()))
		buildSide := -1

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				buildSide = plan.InnerChildIdx ^ 1
			} else {
				buildSide = plan.InnerChildIdx
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				buildSide = 0
			} else {
				buildSide = 1
			}
		case *PhysicalIndexJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexMergeJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexHashJoin:
			buildSide = plan.InnerChildIdx ^ 1
		}

		if buildSide != -1 {
			driverSideInfo[0], driverSideInfo[1] = "(Build)", "(Probe)"
		} else {
			buildSide = 0
		}

		// Always put the Build above the Probe.
		for i := range physPlan.Children() {
			pchild := &physPlan.Children()[i^buildSide]
			if e.explainedPlans[(*pchild).ID()] {
				continue
			}
			err = e.explainPlanInRowFormat(*pchild, taskType, driverSideInfo[i], childIndent, i == len(physPlan.Children())-1)
			if err != nil {
				return
			}
		}
	}

	switch x := p.(type) {
	case *PhysicalTableReader:
		var storeType string
		switch x.StoreType {
		case kv.TiKV, kv.TiFlash, kv.TiDB:
			// expected do nothing
		default:
			return errors.Errorf("the store type %v is unknown", x.StoreType)
		}
		storeType = x.StoreType.Name()
		err = e.explainPlanInRowFormat(x.tablePlan, "cop["+storeType+"]", "", childIndent, true)
	case *PhysicalIndexReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop[tikv]", "", childIndent, true)
	case *PhysicalIndexLookUpReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop[tikv]", "(Build)", childIndent, false)
		err = e.explainPlanInRowFormat(x.tablePlan, "cop[tikv]", "(Probe)", childIndent, true)
	case *PhysicalIndexMergeReader:
		for i, pchild := range x.partialPlans {
			if x.tablePlan != nil || i < len(x.partialPlans)-1 {
				err = e.explainPlanInRowFormat(pchild, "cop[tikv]", "(Build)", childIndent, false)
			} else {
				err = e.explainPlanInRowFormat(pchild, "cop[tikv]", "(Probe)", childIndent, true)
			}
		}
		if x.tablePlan != nil {
			err = e.explainPlanInRowFormat(x.tablePlan, "cop[tikv]", "(Probe)", childIndent, true)
		}
	case *Insert:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Update:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Delete:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Execute:
		if x.Plan != nil {
			err = e.explainPlanInRowFormat(x.Plan, "root", "", indent, true)
		}
	}
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func (e *Explain) prepareOperatorInfo(p Plan, taskType, driverSide, indent string, isLastChild bool) {
	if p.ExplainID().String() == "_0" {
		return
	}

	id := texttree.PrettyIdentifier(p.ExplainID().String()+driverSide, indent, isLastChild)

	estRows := "N/A"
	if si := p.statsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	var accessObject, operatorInfo string
	if plan, ok := p.(dataAccesser); ok {
		accessObject = plan.AccessObject()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		operatorInfo = p.ExplainInfo()
	}

	var row []string
	if e.Analyze {
		runtimeStatsColl := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		explainID := p.ExplainID().String()
		var actRows, analyzeInfo string

		// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
		// So check copTaskEkxecDetail first and print the real cop task information if it's not empty.
		if runtimeStatsColl.ExistsCopStats(explainID) {
			copstats := runtimeStatsColl.GetCopStats(explainID)
			analyzeInfo = copstats.String()
			actRows = fmt.Sprint(copstats.GetActRows())
		} else if runtimeStatsColl.ExistsRootStats(explainID) {
			rootstats := runtimeStatsColl.GetRootStats(explainID)
			analyzeInfo = rootstats.String()
			actRows = fmt.Sprint(rootstats.GetActRows())
		} else {
			analyzeInfo = "time:0ns, loops:0"
		}
		switch p.(type) {
		case *PhysicalTableReader, *PhysicalIndexReader, *PhysicalIndexLookUpReader:
			if s := runtimeStatsColl.GetReaderStats(explainID); s != nil && len(s.String()) > 0 {
				analyzeInfo += ", " + s.String()
			}
		}

		memoryInfo := "N/A"
		memTracker := e.ctx.GetSessionVars().StmtCtx.MemTracker.SearchTracker(p.ExplainID().String())
		if memTracker != nil {
			memoryInfo = memTracker.BytesToString(memTracker.MaxConsumed())
		}

		diskInfo := "N/A"
		diskTracker := e.ctx.GetSessionVars().StmtCtx.DiskTracker.SearchTracker(p.ExplainID().String())
		if diskTracker != nil {
			diskInfo = diskTracker.BytesToString(diskTracker.MaxConsumed())
		}

		row = []string{id, estRows, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo}
	} else {
		row = []string{id, estRows, taskType, accessObject, operatorInfo}
	}
	e.Rows = append(e.Rows, row)
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
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
		case *PhysicalIndexMergeReader:
			for i := 0; i < len(copPlan.partialPlans); i++ {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.partialPlans[i].ExplainID()))
				copTasks = append(copTasks, copPlan.partialPlans[i])
			}
			if copPlan.tablePlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
				copTasks = append(copTasks, copPlan.tablePlan)
			}
		}
		for _, child := range curPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID())
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

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p Plan) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx), nil
	case *PhysicalTableReader:
		tableScan := v.TablePlans[0].(*PhysicalTableScan)
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint(ctx.GetSessionVars().StmtCtx), nil
	case *PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		return v.IndexInfo == nil, nil
	default:
		return false, nil
	}
}

// IsAutoCommitTxn checks if session is in autocommit mode and not InTxn
func IsAutoCommitTxn(ctx sessionctx.Context) bool {
	return ctx.GetSessionVars().IsAutocommit() && !ctx.GetSessionVars().InTxn()
}

// IsPointUpdateByAutoCommit checks if plan p is point update and is in autocommit context
func IsPointUpdateByAutoCommit(ctx sessionctx.Context, p Plan) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	updPlan, ok := p.(*Update)
	if !ok {
		return false, nil
	}
	if _, isFastSel := updPlan.SelectPlan.(*PointGetPlan); isFastSel {
		return true, nil
	}
	return false, nil
}

func buildSchemaAndNameFromIndex(cols []*expression.Column, dbName model.CIStr, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (*expression.Schema, types.NameSlice) {
	schema := expression.NewSchema(cols...)
	idxCols := idxInfo.Columns
	names := make([]*types.FieldName, 0, len(idxCols))
	tblName := tblInfo.Name
	for _, col := range idxCols {
		names = append(names, &types.FieldName{
			OrigTblName: tblName,
			OrigColName: col.Name,
			DBName:      dbName,
			TblName:     tblName,
			ColName:     col.Name,
		})
	}
	return schema, names
}

func buildSchemaAndNameFromPKCol(pkCol *expression.Column, dbName model.CIStr, tblInfo *model.TableInfo) (*expression.Schema, types.NameSlice) {
	schema := expression.NewSchema([]*expression.Column{pkCol}...)
	names := make([]*types.FieldName, 0, 1)
	tblName := tblInfo.Name
	col := tblInfo.GetPkColInfo()
	names = append(names, &types.FieldName{
		OrigTblName: tblName,
		OrigColName: col.Name,
		DBName:      dbName,
		TblName:     tblName,
		ColName:     col.Name,
	})
	return schema, names
}

func locateHashPartition(ctx sessionctx.Context, expr expression.Expression, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	ret, isNull, err := expr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	if ret < 0 {
		ret = 0 - ret
	}
	return int(ret % int64(pi.Num)), nil
}

func getPhysicalTableIDForPartition(ctx sessionctx.Context, pi *model.PartitionInfo, schema *expression.Schema, names types.NameSlice, val []types.Datum) (int64, error) {
	expr, err := expression.ParseSimpleExprsWithNames(ctx, pi.Expr, schema, names)
	if err != nil {
		return 0, err
	}
	pos, err := locateHashPartition(ctx, expr[0], pi, val)
	if err != nil {
		return 0, err
	}
	pID := pi.Definitions[pos].ID
	return pID, nil
}

func rebuildNewTableIDFromIndex(ctx sessionctx.Context, is *PhysicalIndexScan, sc *stmtctx.StatementContext) (int64, error) {
	pi := is.Table.Partition
	if pi.Type == model.PartitionTypeHash && len(is.Ranges) == 1 && is.Ranges[0].IsPoint(sc) {
		schema, names := buildSchemaAndNameFromIndex(is.IdxCols, is.DBName, is.Table, is.Index)
		pID, err := getPhysicalTableIDForPartition(ctx, pi, schema, names, is.Ranges[0].LowVal)
		if err != nil {
			return -1, err
		}
		return pID, nil
	}
	return -1, nil
}

func rebuildNewTableIDFromTable(ctx sessionctx.Context, ts *PhysicalTableScan, sc *stmtctx.StatementContext, pkCol *expression.Column) (int64, error) {
	pi := ts.Table.Partition
	if pi.Type == model.PartitionTypeHash && len(ts.Ranges) == 1 && ts.Ranges[0].IsPoint(sc) {
		schema, names := buildSchemaAndNameFromPKCol(pkCol, ts.DBName, ts.Table)
		pID, err := getPhysicalTableIDForPartition(ctx, pi, schema, names, ts.Ranges[0].LowVal)
		if err != nil {
			return -1, err
		}
		return pID, nil
	}
	return -1, nil
}
