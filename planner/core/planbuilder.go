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
	"fmt"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/schemautil"
)

type visitInfo struct {
	privilege mysql.PrivilegeType
	db        string
	table     string
	column    string
	err       error
}

type tableHintInfo struct {
	indexNestedLoopJoinTables []model.CIStr
	sortMergeJoinTables       []model.CIStr
	hashJoinTables            []model.CIStr
}

func (info *tableHintInfo) ifPreferMergeJoin(tableNames ...*model.CIStr) bool {
	return info.matchTableName(tableNames, info.sortMergeJoinTables)
}

func (info *tableHintInfo) ifPreferHashJoin(tableNames ...*model.CIStr) bool {
	return info.matchTableName(tableNames, info.hashJoinTables)
}

func (info *tableHintInfo) ifPreferINLJ(tableNames ...*model.CIStr) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables)
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (info *tableHintInfo) matchTableName(tables []*model.CIStr, tablesInHints []model.CIStr) bool {
	for _, tableName := range tables {
		if tableName == nil {
			continue
		}
		for _, curEntry := range tablesInHints {
			if curEntry.L == tableName.L {
				return true
			}
		}
	}
	return false
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	windowClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
	windowClause:        "field list", // For window functions that in field list.
}

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	inUpdateStmt bool
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo     []visitInfo
	tableHintInfo []tableHintInfo
	optFlag       uint64

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	windowSpecs map[string]ast.WindowSpec
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	return b.visitInfo
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	return &PlanBuilder{
		ctx:       sctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(node ast.Node) (Plan, error) {
	b.optFlag = flagPrunColumns
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(x)
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}, nil
	case *ast.DeleteStmt:
		return b.buildDelete(x)
	case *ast.ExecuteStmt:
		return b.buildExecute(x)
	case *ast.ExplainStmt:
		return b.buildExplain(x)
	case *ast.TraceStmt:
		return b.buildTrace(x)
	case *ast.InsertStmt:
		return b.buildInsert(x)
	case *ast.LoadDataStmt:
		return b.buildLoadData(x)
	case *ast.LoadStatsStmt:
		return b.buildLoadStats(x), nil
	case *ast.PrepareStmt:
		return b.buildPrepare(x), nil
	case *ast.SelectStmt:
		return b.buildSelect(x)
	case *ast.UnionStmt:
		return b.buildUnion(x)
	case *ast.UpdateStmt:
		return b.buildUpdate(x)
	case *ast.ShowStmt:
		return b.buildShow(x)
	case *ast.DoStmt:
		return b.buildDo(x)
	case *ast.SetStmt:
		return b.buildSet(x)
	case *ast.AnalyzeTableStmt:
		return b.buildAnalyze(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DDLNode:
		return b.buildDDL(x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildExecute(v *ast.ExecuteStmt) (Plan, error) {
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		newExpr, _, err := b.rewrite(expr, nil, nil, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vars = append(vars, newExpr)
	}
	exe := &Execute{Name: v.Name, UsingVars: vars, ExecID: v.ExecID}
	return exe, nil
}

func (b *PlanBuilder) buildDo(v *ast.DoStmt) (Plan, error) {
	var p LogicalPlan
	dual := LogicalTableDual{RowCount: 1}.Init(b.ctx)
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(v.Exprs))...)
	for _, astExpr := range v.Exprs {
		expr, np, err := b.rewrite(astExpr, p, nil, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p = np
		proj.Exprs = append(proj.Exprs, expr)
		schema.Append(&expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  expr.GetType(),
		})
	}
	proj.SetChildren(p)
	proj.self = proj
	proj.SetSchema(schema)
	proj.calculateNoDelay = true
	return proj, nil
}

func (b *PlanBuilder) buildSet(v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		if vars.IsGlobal {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Convert column name expression to string value expression.
				vars.Value = ast.NewValueExpr(cn.Name.Name.O)
			}
			mockTablePlan := LogicalTableDual{}.Init(b.ctx)
			var err error
			assign.Expr, _, err = b.rewrite(vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.(*driver.ValueExpr).Datum,
				RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (b *PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
	for _, f := range sel.Fields.Fields {
		if ast.HasWindowFlag(f.Expr) {
			return true
		}
	}
	return false
}

func getPathByIndexName(paths []*accessPath, idxName model.CIStr, tblInfo *model.TableInfo) *accessPath {
	var tablePath *accessPath
	for _, path := range paths {
		if path.isTablePath {
			tablePath = path
			continue
		}
		if path.index.Name.L == idxName.L {
			return path
		}
	}
	if isPrimaryIndexHint(idxName) && tblInfo.PKIsHandle {
		return tablePath
	}
	return nil
}

func isPrimaryIndexHint(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func getPossibleAccessPaths(indexHints []*ast.IndexHint, tblInfo *model.TableInfo) ([]*accessPath, error) {
	publicPaths := make([]*accessPath, 0, len(tblInfo.Indices)+1)
	publicPaths = append(publicPaths, &accessPath{isTablePath: true})
	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			publicPaths = append(publicPaths, &accessPath{index: index})
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*accessPath, 0, len(publicPaths))
	ignored := make([]*accessPath, 0, len(publicPaths))
	for _, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		hasScanHint = true
		for _, idxName := range hint.IndexNames {
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				return nil, ErrKeyDoesNotExist.GenWithStackByArgs(idxName, tblInfo.Name)
			}
			if hint.HintType == ast.HintIgnore {
				// Collect all the ignored index hints.
				ignored = append(ignored, path)
				continue
			}
			// Currently we don't distinguish between "FORCE" and "USE" because
			// our cost estimation is not reliable.
			hasUseOrForce = true
			path.forced = true
			available = append(available, path)
		}
	}

	if !hasScanHint || !hasUseOrForce {
		available = publicPaths
	}

	available = removeIgnoredPaths(available, ignored, tblInfo)

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use table scan.
	if len(available) == 0 {
		available = append(available, &accessPath{isTablePath: true})
	}
	return available, nil
}

func removeIgnoredPaths(paths, ignoredPaths []*accessPath, tblInfo *model.TableInfo) []*accessPath {
	if len(ignoredPaths) == 0 {
		return paths
	}
	remainedPaths := make([]*accessPath, 0, len(paths))
	for _, path := range paths {
		if path.isTablePath || getPathByIndexName(ignoredPaths, path.index.Name, tblInfo) == nil {
			remainedPaths = append(remainedPaths, path)
		}
	}
	return remainedPaths
}

func (b *PlanBuilder) buildSelectLock(src LogicalPlan, lock ast.SelectLockType) *LogicalLock {
	selectLock := LogicalLock{Lock: lock}.Init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock
}

func (b *PlanBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		if v, ok := b.ctx.GetSessionVars().Users[x.SQLVar.Name]; ok {
			p.SQLText = v
		} else {
			p.SQLText = "NULL"
		}
	} else {
		p.SQLText = x.SQLText
	}
	return p
}

func (b *PlanBuilder) buildCheckIndex(dbName model.CIStr, as *ast.AdminStmt) (Plan, error) {
	tblName := as.Tables[0]
	tbl, err := b.is.TableByName(dbName, tblName.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tblInfo := tbl.Meta()

	// get index information
	var idx *model.IndexInfo
	for _, index := range tblInfo.Indices {
		if index.Name.L == strings.ToLower(as.Index) {
			idx = index
			break
		}
	}
	if idx == nil {
		return nil, errors.Errorf("index %s do not exist", as.Index)
	}
	if idx.State != model.StatePublic {
		return nil, errors.Errorf("index %s state %s isn't public", as.Index, idx.State)
	}

	id := 1
	columns := make([]*model.ColumnInfo, 0, len(idx.Columns))
	schema := expression.NewSchema(make([]*expression.Column, 0, len(idx.Columns))...)
	for _, idxCol := range idx.Columns {
		for _, col := range tblInfo.Columns {
			if idxCol.Name.L == col.Name.L {
				columns = append(columns, col)
				schema.Append(&expression.Column{
					ColName:  col.Name,
					UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  &col.FieldType,
				})
			}
		}
	}
	is := PhysicalIndexScan{
		Table:            tblInfo,
		TableAsName:      &tblName.Name,
		DBName:           dbName,
		Columns:          columns,
		Index:            idx,
		dataSourceSchema: schema,
		Ranges:           ranger.FullRange(),
		KeepOrder:        false,
	}.Init(b.ctx)
	is.stats = &property.StatsInfo{}
	cop := &copTask{indexPlan: is}
	// It's double read case.
	ts := PhysicalTableScan{Columns: columns, Table: is.Table}.Init(b.ctx)
	ts.SetSchema(is.dataSourceSchema)
	cop.tablePlan = ts
	is.initSchema(id, idx, true)
	t := finishCopTask(b.ctx, cop)

	rootT := t.(*rootTask)
	return rootT.p, nil
}

func (b *PlanBuilder) buildAdmin(as *ast.AdminStmt) (Plan, error) {
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckTable:
		ret, err = b.buildAdminCheckTable(as)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case ast.AdminCheckIndex:
		dbName := as.Tables[0].Schema
		readerPlan, err := b.buildCheckIndex(dbName, as)
		if err != nil {
			return ret, errors.Trace(err)
		}

		ret = &CheckIndex{
			DBName:            dbName.L,
			IdxName:           as.Index,
			IndexLookUpReader: readerPlan.(*PhysicalIndexLookUpReader),
		}
	case ast.AdminRecoverIndex:
		p := &RecoverIndex{Table: as.Tables[0], IndexName: as.Index}
		p.SetSchema(buildRecoverIndexFields())
		ret = p
	case ast.AdminCleanupIndex:
		p := &CleanupIndex{Table: as.Tables[0], IndexName: as.Index}
		p.SetSchema(buildCleanupIndexFields())
		ret = p
	case ast.AdminChecksumTable:
		p := &ChecksumTable{Tables: as.Tables}
		p.SetSchema(buildChecksumTableSchema())
		ret = p
	case ast.AdminShowNextRowID:
		p := &ShowNextRowID{TableName: as.Tables[0]}
		p.SetSchema(buildShowNextRowID())
		ret = p
	case ast.AdminShowDDL:
		p := &ShowDDL{}
		p.SetSchema(buildShowDDLFields())
		ret = p
	case ast.AdminShowDDLJobs:
		p := &ShowDDLJobs{JobNumber: as.JobNumber}
		p.SetSchema(buildShowDDLJobsFields())
		ret = p
	case ast.AdminCancelDDLJobs:
		p := &CancelDDLJobs{JobIDs: as.JobIDs}
		p.SetSchema(buildCancelDDLJobsFields())
		ret = p
	case ast.AdminCheckIndexRange:
		schema, err := b.buildCheckIndexSchema(as.Tables[0], as.Index)
		if err != nil {
			return nil, errors.Trace(err)
		}

		p := &CheckIndexRange{Table: as.Tables[0], IndexName: as.Index, HandleRanges: as.HandleRanges}
		p.SetSchema(schema)
		ret = p
	case ast.AdminShowDDLJobQueries:
		p := &ShowDDLJobQueries{JobIDs: as.JobIDs}
		p.SetSchema(buildShowDDLJobQueriesFields())
		ret = p
	case ast.AdminShowSlow:
		p := &ShowSlow{ShowSlow: as.ShowSlow}
		p.SetSchema(buildShowSlowSchema())
		ret = p
	case ast.AdminRestoreTable:
		if len(as.JobIDs) > 0 {
			ret = &RestoreTable{JobID: as.JobIDs[0]}
		} else if len(as.Tables) > 0 {
			ret = &RestoreTable{Table: as.Tables[0], JobNum: as.JobNumber}
		} else {
			return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
		}
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return ret, nil
}

func (b *PlanBuilder) buildAdminCheckTable(as *ast.AdminStmt) (*CheckTable, error) {
	p := &CheckTable{Tables: as.Tables}
	p.GenExprs = make(map[model.TableColumnID]expression.Expression, len(p.Tables))

	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	for _, tbl := range p.Tables {
		tableInfo := tbl.TableInfo
		schema := expression.TableInfo2SchemaWithDBName(b.ctx, tbl.Schema, tableInfo)
		table, ok := b.is.TableByID(tableInfo.ID)
		if !ok {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tbl.DBInfo.Name.O, tableInfo.Name.O)
		}

		mockTablePlan.SetSchema(schema)

		// Calculate generated columns.
		columns := table.Cols()
		for _, column := range columns {
			if !column.IsGenerated() {
				continue
			}
			columnName := &ast.ColumnName{Name: column.Name}
			columnName.SetText(column.Name.O)

			colExpr, _, err := mockTablePlan.findColumn(columnName)
			if err != nil {
				return nil, errors.Trace(err)
			}

			expr, _, err := b.rewrite(column.GeneratedExpr, mockTablePlan, nil, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
			expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
			p.GenExprs[model.TableColumnID{TableID: tableInfo.ID, ColumnID: column.ColumnInfo.ID}] = expr
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildCheckIndexSchema(tn *ast.TableName, indexName string) (*expression.Schema, error) {
	schema := expression.NewSchema()
	indexName = strings.ToLower(indexName)
	indicesInfo := tn.TableInfo.Indices
	cols := tn.TableInfo.Cols()
	for _, idxInfo := range indicesInfo {
		if idxInfo.Name.L != indexName {
			continue
		}
		for _, idxCol := range idxInfo.Columns {
			col := cols[idxCol.Offset]
			schema.Append(&expression.Column{
				ColName:  idxCol.Name,
				TblName:  tn.Name,
				DBName:   tn.Schema,
				RetType:  &col.FieldType,
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				ID:       col.ID})
		}
		schema.Append(&expression.Column{
			ColName:  model.NewCIStr("extra_handle"),
			TblName:  tn.Name,
			DBName:   tn.Schema,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       -1,
		})
	}
	if schema.Len() == 0 {
		return nil, errors.Errorf("index %s not found", indexName)
	}
	return schema, nil
}

// getColsInfo returns the info of index columns, normal columns and primary key.
func getColsInfo(tn *ast.TableName) (indicesInfo []*model.IndexInfo, colsInfo []*model.ColumnInfo, pkCol *model.ColumnInfo) {
	tbl := tn.TableInfo
	// idxNames contains all the normal columns that can be analyzed more effectively, because those columns occur as index
	// columns or primary key columns with integer type.
	var idxNames []string
	if tbl.PKIsHandle {
		for _, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				idxNames = append(idxNames, col.Name.L)
				pkCol = col
			}
		}
	}
	for _, idx := range tn.TableInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
			if len(idx.Columns) == 1 {
				idxNames = append(idxNames, idx.Columns[0].Name.L)
			}
		}
	}
	for _, col := range tbl.Columns {
		isIndexCol := false
		for _, idx := range idxNames {
			if idx == col.Name.L {
				isIndexCol = true
				break
			}
		}
		if !isIndexCol {
			colsInfo = append(colsInfo, col)
		}
	}
	return
}

func getPhysicalIDs(tblInfo *model.TableInfo, partitionNames []model.CIStr) ([]int64, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if len(partitionNames) != 0 {
			return nil, errors.Trace(ddl.ErrPartitionMgmtOnNonpartitioned)
		}
		return []int64{tblInfo.ID}, nil
	}
	if len(partitionNames) == 0 {
		ids := make([]int64, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
		return ids, nil
	}
	ids := make([]int64, 0, len(partitionNames))
	for _, name := range partitionNames {
		found := false
		for _, def := range pi.Definitions {
			if def.Name.L == name.L {
				found = true
				ids = append(ids, def.ID)
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("can not found the specified partition name %s in the table definition", name.O)
		}
	}
	return ids, nil
}

func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	for _, tbl := range as.TableNames {
		if tbl.TableInfo.IsView() {
			return nil, errors.Errorf("analyze %s is not supported now.", tbl.Name.O)
		}
		idxInfo, colInfo, pkInfo := getColsInfo(tbl)
		physicalIDs, err := getPhysicalIDs(tbl.TableInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		for _, idx := range idxInfo {
			for _, id := range physicalIDs {
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{PhysicalTableID: id, IndexInfo: idx})
			}
		}
		if len(colInfo) > 0 || pkInfo != nil {
			for _, id := range physicalIDs {
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PhysicalTableID: id, PKInfo: pkInfo, ColsInfo: colInfo})
			}
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, err := getPhysicalIDs(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idxName := range as.IndexNames {
		idx := schemautil.FindIndexByName(idxName.L, tblInfo.Indices)
		if idx == nil || idx.State != model.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		for _, id := range physicalIDs {
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{PhysicalTableID: id, IndexInfo: idx})
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, err := getPhysicalIDs(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			for _, id := range physicalIDs {
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{PhysicalTableID: id, IndexInfo: idx})
			}
		}
	}
	return p, nil
}

const (
	defaultMaxNumBuckets = 256
	numBucketsLimit      = 1024
)

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	for _, tbl := range as.TableNames {
		user := b.ctx.GetSessionVars().User
		var insertErr, selectErr error
		if user != nil {
			insertErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
			selectErr = ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tbl.Schema.O, tbl.Name.O, "", insertErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, tbl.Schema.O, tbl.Name.O, "", selectErr)
	}
	if as.MaxNumBuckets == 0 {
		as.MaxNumBuckets = defaultMaxNumBuckets
	} else {
		as.MaxNumBuckets = mathutil.MinUint64(as.MaxNumBuckets, numBucketsLimit)
	}
	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as)
		}
		return b.buildAnalyzeIndex(as)
	}
	return b.buildAnalyzeTable(as)
}

func buildShowNextRowID() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 4)...)
	schema.Append(buildColumn("", "DB_NAME", mysql.TypeVarchar, mysql.MaxDatabaseNameLength))
	schema.Append(buildColumn("", "TABLE_NAME", mysql.TypeVarchar, mysql.MaxTableNameLength))
	schema.Append(buildColumn("", "COLUMN_NAME", mysql.TypeVarchar, mysql.MaxColumnNameLength))
	schema.Append(buildColumn("", "NEXT_GLOBAL_ROW_ID", mysql.TypeLonglong, 4))
	return schema
}

func buildShowDDLFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 4)...)
	schema.Append(buildColumn("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "OWNER_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "OWNER_ADDRESS", mysql.TypeVarchar, 32))
	schema.Append(buildColumn("", "RUNNING_JOBS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "SELF_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "QUERY", mysql.TypeVarchar, 256))

	return schema
}

func buildRecoverIndexFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 2)...)
	schema.Append(buildColumn("", "ADDED_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "SCAN_COUNT", mysql.TypeLonglong, 4))
	return schema
}

func buildCleanupIndexFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 1)...)
	schema.Append(buildColumn("", "REMOVED_COUNT", mysql.TypeLonglong, 4))
	return schema
}

func buildShowDDLJobsFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 10)...)
	schema.Append(buildColumn("", "JOB_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "DB_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "TABLE_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "JOB_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_STATE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "TABLE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "ROW_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "START_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "STATE", mysql.TypeVarchar, 64))
	return schema
}

func buildShowDDLJobQueriesFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 1)...)
	schema.Append(buildColumn("", "QUERY", mysql.TypeVarchar, 256))
	return schema
}

func buildShowSlowSchema() *expression.Schema {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	tinySize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	timestampSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTimestamp)
	durationSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDuration)

	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn("", "SQL", mysql.TypeVarchar, 4096))
	schema.Append(buildColumn("", "START", mysql.TypeTimestamp, timestampSize))
	schema.Append(buildColumn("", "DURATION", mysql.TypeDuration, durationSize))
	schema.Append(buildColumn("", "DETAILS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "SUCC", mysql.TypeTiny, tinySize))
	schema.Append(buildColumn("", "CONN_ID", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumn("", "TRANSACTION_TS", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumn("", "USER", mysql.TypeVarchar, 32))
	schema.Append(buildColumn("", "DB", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "TABLE_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "INDEX_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "INTERNAL", mysql.TypeTiny, tinySize))
	return schema
}

func buildCancelDDLJobsFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 2)...)
	schema.Append(buildColumn("", "JOB_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "RESULT", mysql.TypeVarchar, 128))

	return schema
}

func buildColumn(tableName, name string, tp byte, size int) *expression.Column {
	cs, cl := types.DefaultCharsetForType(tp)
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = charset.CharsetUTF8MB4
		cl = charset.CollationUTF8MB4
		flag = 0
	}

	fieldType := &types.FieldType{
		Charset: cs,
		Collate: cl,
		Tp:      tp,
		Flen:    size,
		Flag:    flag,
	}
	return &expression.Column{
		ColName: model.NewCIStr(name),
		TblName: model.NewCIStr(tableName),
		DBName:  model.NewCIStr(infoschema.Name),
		RetType: fieldType,
	}
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *PlanBuilder) buildShow(show *ast.ShowStmt) (Plan, error) {
	p := Show{
		Tp:          show.Tp,
		DBName:      show.DBName,
		Table:       show.Table,
		Column:      show.Column,
		Flag:        show.Flag,
		Full:        show.Full,
		User:        show.User,
		IfNotExists: show.IfNotExists,
		GlobalScope: show.GlobalScope,
	}.Init(b.ctx)
	switch showTp := show.Tp; showTp {
	case ast.ShowProcedureStatus:
		p.SetSchema(buildShowProcedureSchema())
	case ast.ShowTriggers:
		p.SetSchema(buildShowTriggerSchema())
	case ast.ShowEvents:
		p.SetSchema(buildShowEventsSchema())
	case ast.ShowWarnings, ast.ShowErrors:
		p.SetSchema(buildShowWarningsSchema())
	default:
		isView := false
		switch showTp {
		case ast.ShowTables, ast.ShowTableStatus:
			if p.DBName == "" {
				return nil, ErrNoDB
			}
		case ast.ShowCreateTable:
			user := b.ctx.GetSessionVars().User
			var err error
			if user != nil {
				err = ErrTableaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask, show.Table.Schema.L, show.Table.Name.L, "", err)
			if table, err := b.is.TableByName(show.Table.Schema, show.Table.Name); err == nil {
				isView = table.Meta().IsView()
			}
		}
		p.SetSchema(buildShowSchema(show, isView))
	}
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(p.schema)
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.Schema().Columns[0].ColName},
		}
		expr, _, err := b.rewrite(show.Pattern, mockTablePlan, nil, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.Conditions = append(p.Conditions, expr)
	}
	if show.Where != nil {
		conds := splitWhere(show.Where)
		for _, cond := range conds {
			expr, _, err := b.rewrite(cond, mockTablePlan, nil, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			p.Conditions = append(p.Conditions, expr)
		}
		err := p.ResolveIndices()
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		b.visitInfo = collectVisitInfoFromGrantStmt(b.visitInfo, raw)
	case *ast.RevokeStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.KillStmt:
		// If you have the SUPER privilege, you can kill all threads and statements.
		// Otherwise, you can kill only your own threads and statements.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			processList := sm.ShowProcessList()
			if pi, ok := processList[raw.ConnectionID]; ok {
				loginUser := b.ctx.GetSessionVars().User
				if pi.User != loginUser.Username {
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
				}
			}
		}
	case *ast.UseStmt:
		if raw.DBName == "" {
			return nil, ErrNoDB
		}
	}
	return p, nil
}

func collectVisitInfoFromGrantStmt(vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)

	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		if item.Priv == mysql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = mysql.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = mysql.AllDBPrivs
			case ast.GrantLevelTable:
				allPrivs = mysql.AllTablePrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "", nil)
	}

	return vi
}

func (b *PlanBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	value, err := table.GetColDefaultValue(b.ctx, col.ToInfo())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

func (b *PlanBuilder) findDefaultValue(cols []*table.Column, name *ast.ColumnName) (*expression.Constant, error) {
	for _, col := range cols {
		if col.Name.L == name.Name.L {
			return b.getDefaultValue(col)
		}
	}
	return nil, ErrUnknownColumn.GenWithStackByArgs(name.Name.O, "field_list")
}

// resolveGeneratedColumns resolves generated columns with their generation
// expressions respectively. onDups indicates which columns are in on-duplicate list.
func (b *PlanBuilder) resolveGeneratedColumns(columns []*table.Column, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedColumns, err error) {
	for _, column := range columns {
		if !column.IsGenerated() {
			continue
		}
		columnName := &ast.ColumnName{Name: column.Name}
		columnName.SetText(column.Name.O)

		colExpr, _, err := mockPlan.findColumn(columnName)
		if err != nil {
			return igc, errors.Trace(err)
		}

		expr, _, err := b.rewrite(column.GeneratedExpr, mockPlan, nil, true)
		if err != nil {
			return igc, errors.Trace(err)
		}
		expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())

		igc.Columns = append(igc.Columns, columnName)
		igc.Exprs = append(igc.Exprs, expr)
		if onDups == nil {
			continue
		}
		for dep := range column.Dependences {
			if _, ok := onDups[dep]; ok {
				assign := &expression.Assignment{Col: colExpr, Expr: expr}
				igc.OnDuplicates = append(igc.OnDuplicates, assign)
				break
			}
		}
	}
	return igc, nil
}

func (b *PlanBuilder) buildInsert(insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	tableInfo := tn.TableInfo
	if tableInfo.IsView() {
		err := errors.Errorf("insert into view %s is not supported now.", tableInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into view %s is not supported now.", tableInfo.Name.O)
		}
		return nil, err
	}
	// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, tn.Schema, tableInfo)
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		return nil, errors.Errorf("Can't get table %s.", tableInfo.Name.O)
	}

	insertPlan := Insert{
		Table:       tableInPlan,
		Columns:     insert.Columns,
		tableSchema: schema,
		IsReplace:   insert.IsReplace,
	}.Init(b.ctx)

	b.visitInfo = append(b.visitInfo, visitInfo{
		privilege: mysql.InsertPriv,
		db:        tn.DBInfo.Name.L,
		table:     tableInfo.Name.L,
		err:       nil,
	})

	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(insertPlan.tableSchema)

	checkRefColumn := func(n ast.Node) ast.Node {
		if insertPlan.NeedFillDefaultValue {
			return n
		}
		switch n.(type) {
		case *ast.ColumnName, *ast.ColumnNameExpr:
			insertPlan.NeedFillDefaultValue = true
		}
		return n
	}

	if len(insert.Setlist) > 0 {
		// Branch for `INSERT ... SET ...`.
		err := b.buildSetValuesOfInsert(insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else if len(insert.Lists) > 0 {
		// Branch for `INSERT ... VALUES ...`.
		err := b.buildValuesListOfInsert(insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// Branch for `INSERT ... SELECT ...`.
		err := b.buildSelectPlanOfInsert(insert, insertPlan)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	mockTablePlan.SetSchema(insertPlan.Schema4OnDuplicate)
	columnByName := make(map[string]*table.Column, len(insertPlan.Table.Cols()))
	for _, col := range insertPlan.Table.Cols() {
		columnByName[col.Name.L] = col
	}
	onDupColSet, dupCols, err := insertPlan.validateOnDup(insert.OnDuplicate, columnByName, tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, assign := range insert.OnDuplicate {
		// Construct the function which calculates the assign value of the column.
		expr, err1 := b.rewriteInsertOnDuplicateUpdate(assign.Expr, mockTablePlan, insertPlan)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		insertPlan.OnDuplicate = append(insertPlan.OnDuplicate, &expression.Assignment{
			Col:  dupCols[i],
			Expr: expr,
		})
	}

	// Calculate generated columns.
	mockTablePlan.schema = insertPlan.tableSchema
	insertPlan.GenCols, err = b.resolveGeneratedColumns(insertPlan.Table.Cols(), onDupColSet, mockTablePlan)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = insertPlan.ResolveIndices()
	return insertPlan, err
}

func (p *Insert) validateOnDup(onDup []*ast.Assignment, colMap map[string]*table.Column, tblInfo *model.TableInfo) (map[string]struct{}, []*expression.Column, error) {
	onDupColSet := make(map[string]struct{}, len(onDup))
	dupCols := make([]*expression.Column, 0, len(onDup))
	for _, assign := range onDup {
		// Check whether the column to be updated exists in the source table.
		col, err := p.tableSchema.FindColumn(assign.Column)
		if err != nil {
			return nil, nil, errors.Trace(err)
		} else if col == nil {
			return nil, nil, ErrUnknownColumn.GenWithStackByArgs(assign.Column.OrigColName(), "field list")
		}

		// Check whether the column to be updated is the generated column.
		column := colMap[assign.Column.Name.L]
		if column.IsGenerated() {
			return nil, nil, ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}
		onDupColSet[column.Name.L] = struct{}{}
		dupCols = append(dupCols, col)
	}
	return onDupColSet, dupCols, nil
}

func (b *PlanBuilder) getAffectCols(insertStmt *ast.InsertStmt, insertPlan *Insert) (affectedValuesCols []*table.Column, err error) {
	if len(insertStmt.Columns) > 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name (col_name [, col_name] ...) {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name (col_name [, col_name] ...) SELECT ...`.
		colName := make([]string, 0, len(insertStmt.Columns))
		for _, col := range insertStmt.Columns {
			colName = append(colName, col.Name.O)
		}
		affectedValuesCols, err = table.FindCols(insertPlan.Table.Cols(), colName, insertPlan.Table.Meta().PKIsHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}

	} else if len(insertStmt.Setlist) == 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesCols = insertPlan.Table.Cols()
	}
	return affectedValuesCols, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	tableInfo := insertPlan.Table.Meta()
	colNames := make([]string, 0, len(insert.Setlist))
	exprCols := make([]*expression.Column, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		exprCol, err := insertPlan.tableSchema.FindColumn(assign.Column)
		if err != nil {
			return errors.Trace(err)
		}
		if exprCol == nil {
			return errors.Errorf("Can't find column %s", assign.Column)
		}
		colNames = append(colNames, assign.Column.Name.L)
		exprCols = append(exprCols, exprCol)
	}

	// Check whether the column to be updated is the generated column.
	tCols, err := table.FindCols(insertPlan.Table.Cols(), colNames, tableInfo.PKIsHandle)
	if err != nil {
		return errors.Trace(err)
	}
	for _, tCol := range tCols {
		if tCol.IsGenerated() {
			return ErrBadGeneratedColumn.GenWithStackByArgs(tCol.Name.O, tableInfo.Name.O)
		}
	}

	for i, assign := range insert.Setlist {
		expr, _, err := b.rewriteWithPreprocess(assign.Expr, mockTablePlan, nil, true, checkRefColumn)
		if err != nil {
			return errors.Trace(err)
		}
		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			Col:  exprCols[i],
			Expr: expr,
		})
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildValuesListOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return errors.Trace(err)
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
		// No generated column is allowed.
		for _, col := range affectedValuesCols {
			if col.IsGenerated() {
				return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
			}
		}
	}

	totalTableCols := insertPlan.Table.Cols()
	for i, valuesItem := range insert.Lists {
		// The length of all the value_list should be the same.
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		if i > 0 && len(insert.Lists[i-1]) != len(insert.Lists[i]) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
		}
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			var expr expression.Expression
			var err error
			switch x := valueItem.(type) {
			case *ast.DefaultExpr:
				if x.Name != nil {
					expr, err = b.findDefaultValue(totalTableCols, x.Name)
				} else {
					expr, err = b.getDefaultValue(affectedValuesCols[j])
				}
			case *driver.ValueExpr:
				expr = &expression.Constant{
					Value:   x.Datum,
					RetType: &x.Type,
				}
			default:
				expr, _, err = b.rewriteWithPreprocess(valueItem, mockTablePlan, nil, true, checkRefColumn)
			}
			if err != nil {
				return errors.Trace(err)
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildSelectPlanOfInsert(insert *ast.InsertStmt, insertPlan *Insert) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return errors.Trace(err)
	}
	selectPlan, err := b.Build(insert.Select)
	if err != nil {
		return errors.Trace(err)
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	if selectPlan.Schema().Len() != len(affectedValuesCols) {
		return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
	}

	// Check to guarantee that there's no generated column.
	// This check should be done after the above one to make its behavior compatible with MySQL.
	// For example, table t has two columns, namely a and b, and b is a generated column.
	// "insert into t (b) select * from t" will raise an error that the column count is not matched.
	// "insert into t select * from t" will raise an error that there's a generated column in the column list.
	// If we do this check before the above one, "insert into t (b) select * from t" will raise an error
	// that there's a generated column in the column list.
	for _, col := range affectedValuesCols {
		if col.IsGenerated() {
			return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
		}
	}

	insertPlan.SelectPlan, err = DoOptimize(b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return errors.Trace(err)
	}

	// schema4NewRow is the schema for the newly created data record based on
	// the result of the select statement.
	schema4NewRow := expression.NewSchema(make([]*expression.Column, len(insertPlan.Table.Cols()))...)
	for i, selCol := range insertPlan.SelectPlan.Schema().Columns {
		ordinal := affectedValuesCols[i].Offset
		schema4NewRow.Columns[ordinal] = &expression.Column{}
		*schema4NewRow.Columns[ordinal] = *selCol

		schema4NewRow.Columns[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.Columns[ordinal].RetType = affectedValuesCols[i].FieldType
	}
	for i := range schema4NewRow.Columns {
		if schema4NewRow.Columns[i] == nil {
			schema4NewRow.Columns[i] = &expression.Column{UniqueID: insertPlan.ctx.GetSessionVars().AllocPlanColumnID()}
		}
	}
	insertPlan.Schema4OnDuplicate = expression.MergeSchema(insertPlan.tableSchema, schema4NewRow)
	return nil
}

func (b *PlanBuilder) buildLoadData(ld *ast.LoadDataStmt) (Plan, error) {
	p := &LoadData{
		IsLocal:     ld.IsLocal,
		Path:        ld.Path,
		Table:       ld.Table,
		Columns:     ld.Columns,
		FieldsInfo:  ld.FieldsInfo,
		LinesInfo:   ld.LinesInfo,
		IgnoreLines: ld.IgnoreLines,
	}
	tableInfo := p.Table.TableInfo
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(db, tableInfo.Name.O)
	}
	schema := expression.TableInfo2Schema(b.ctx, tableInfo)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(schema)

	var err error
	p.GenCols, err = b.resolveGeneratedColumns(tableInPlan.Cols(), nil, mockTablePlan)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

func (b *PlanBuilder) buildLoadStats(ld *ast.LoadStatsStmt) Plan {
	p := &LoadStats{Path: ld.Path}
	return p
}

func (b *PlanBuilder) buildDDL(node ast.DDLNode) (Plan, error) {
	switch v := node.(type) {
	case *ast.AlterTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
			err:       nil,
		})
	case *ast.CreateDatabaseStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.CreatePriv,
			db:        v.Name,
			err:       nil,
		})
	case *ast.CreateIndexStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.IndexPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
			err:       nil,
		})
	case *ast.CreateTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.CreatePriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
			err:       nil,
		})
		if v.ReferTable != nil {
			b.visitInfo = append(b.visitInfo, visitInfo{
				privilege: mysql.SelectPriv,
				db:        v.ReferTable.Schema.L,
				table:     v.ReferTable.Name.L,
				err:       nil,
			})
		}
	case *ast.CreateViewStmt:
		plan, err := b.Build(v.Select)
		if err != nil {
			return nil, err
		}
		schema := plan.Schema()
		if v.Cols != nil && len(v.Cols) != schema.Len() {
			return nil, ddl.ErrViewWrongList
		}
		// we use fieldList to store schema.Columns temporary
		var fieldList = make([]*ast.SelectField, schema.Len())
		for i, col := range schema.Columns {
			fieldList[i] = &ast.SelectField{AsName: col.ColName}
		}
		v.Select.(*ast.SelectStmt).Fields.Fields = fieldList
		if _, ok := plan.(LogicalPlan); ok {
			b.visitInfo = append(b.visitInfo, visitInfo{
				// TODO: We should check CreateViewPriv instead of CreatePriv.
				// See https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_create-view.
				privilege: mysql.CreatePriv,
				db:        v.ViewName.Schema.L,
				table:     v.ViewName.Name.L,
				err:       nil,
			})
		}
	case *ast.DropDatabaseStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.DropPriv,
			db:        v.Name,
			err:       nil,
		})
	case *ast.DropIndexStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.IndexPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
			err:       nil,
		})
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			b.visitInfo = append(b.visitInfo, visitInfo{
				privilege: mysql.DropPriv,
				db:        tableVal.Schema.L,
				table:     tableVal.Name.L,
				err:       nil,
			})
		}
	case *ast.TruncateTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.DeletePriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
			err:       nil,
		})
	case *ast.RenameTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.OldTable.Schema.L,
			table:     v.OldTable.Name.L,
			err:       nil,
		})
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.NewTable.Schema.L,
			table:     v.NewTable.Name.L,
			err:       nil,
		})
	}

	p := &DDL{Statement: node}
	return p, nil
}

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (Plan, error) {
	if _, ok := trace.Stmt.(*ast.SelectStmt); !ok && trace.Format == "row" {
		return nil, errors.New("trace only supports select query when format is row")
	}

	p := &Trace{StmtNode: trace.Stmt, Format: trace.Format}

	switch trace.Format {
	case "row":
		retFields := []string{"operation", "duration", "spanID"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		schema.Append(buildColumn("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumn("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumn("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema)
	case "json":
		retFields := []string{"json"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		schema.Append(buildColumn("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema)
	default:
		return nil, errors.New("trace format should be one of 'row' or 'json'")
	}
	return p, nil
}

func (b *PlanBuilder) buildExplain(explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(show)
	}
	targetPlan, err := OptimizeAstNode(b.ctx, explain.Stmt, b.is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pp, ok := targetPlan.(PhysicalPlan)
	if !ok {
		switch x := targetPlan.(type) {
		case *Delete:
			pp = x.SelectPlan
		case *Update:
			pp = x.SelectPlan
		case *Insert:
			if x.SelectPlan != nil {
				pp = x.SelectPlan
			}
		}
		if pp == nil {
			return nil, ErrUnsupportedType.GenWithStackByArgs(targetPlan)
		}
	}
	p := &Explain{StmtPlan: pp, Analyze: explain.Analyze, Format: explain.Format, ExecStmt: explain.Stmt, ExecPlan: targetPlan}
	p.ctx = b.ctx
	err = p.prepareSchema()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func buildShowProcedureSchema() *expression.Schema {
	tblName := "ROUTINES"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowTriggerSchema() *expression.Schema {
	tblName := "TRIGGERS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumn(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowEventsSchema() *expression.Schema {
	tblName := "EVENTS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 15)...)
	schema.Append(buildColumn(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumn(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumn(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumn(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumn(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema
}

func buildShowWarningsSchema() *expression.Schema {
	tblName := "WARNINGS"
	schema := expression.NewSchema(make([]*expression.Column, 0, 3)...)
	schema.Append(buildColumn(tblName, "Level", mysql.TypeVarchar, 64))
	schema.Append(buildColumn(tblName, "Code", mysql.TypeLong, 19))
	schema.Append(buildColumn(tblName, "Message", mysql.TypeVarchar, 64))
	return schema
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool) (schema *expression.Schema) {
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowDatabases:
		names = []string{"Database"}
	case ast.ShowTables:
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Table_type")
		}
	case ast.ShowTableStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowWarnings, ast.ShowErrors:
		names = []string{"Level", "Code", "Message"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar}
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCreateTable:
		if !isView {
			names = []string{"Table", "Create Table"}
		} else {
			names = []string{"View", "Create View", "character_set_client", "collation_connection"}
		}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowGrants:
		if s.User != nil {
			names = []string{fmt.Sprintf("Grants for %s", s.User)}
		} else {
			// Don't know the name yet, so just say "user"
			names = []string{"Grants for User"}
		}
	case ast.ShowIndex:
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPlugins:
		names = []string{"Name", "Status", "Type", "Library", "License", "Version"}
		ftypes = []byte{
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
		}
	case ast.ShowProcessList:
		names = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Table_name", "Partition_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowProfiles: // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{mysql.TypeLong, mysql.TypeDouble, mysql.TypeVarchar}
	case ast.ShowMasterStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPrivileges:
		names = []string{"Privilege", "Context", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	}

	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	for i := range names {
		col := &expression.Column{
			ColName: model.NewCIStr(names[i]),
		}
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schema.Append(col)
	}
	return schema
}

func buildChecksumTableSchema() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 5)...)
	schema.Append(buildColumn("", "Db_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn("", "Table_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumn("", "Checksum_crc64_xor", mysql.TypeLonglong, 22))
	schema.Append(buildColumn("", "Total_kvs", mysql.TypeLonglong, 22))
	schema.Append(buildColumn("", "Total_bytes", mysql.TypeLonglong, 22))
	return schema
}
