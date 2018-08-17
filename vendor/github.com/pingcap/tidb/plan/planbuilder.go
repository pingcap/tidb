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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

type visitInfo struct {
	privilege mysql.PrivilegeType
	db        string
	table     string
	column    string
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
	groupByClause
	showStatement
)

var clauseMsg = map[clauseCode]string{
	unknowClause:  "",
	fieldList:     "field list",
	havingClause:  "having clause",
	onClause:      "on clause",
	orderByClause: "order clause",
	whereClause:   "where clause",
	groupByClause: "group statement",
	showStatement: "show statement",
}

// planBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type planBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	inUpdateStmt bool
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// Collect the visit information for privilege check.
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
}

func (b *planBuilder) build(node ast.Node) (Plan, error) {
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
		return b.buildSimple(node.(ast.StmtNode)), nil
	case ast.DDLNode:
		return b.buildDDL(x), nil
	}
	return nil, ErrUnsupportedType.Gen("Unsupported type %T", node)
}

func (b *planBuilder) buildExecute(v *ast.ExecuteStmt) (Plan, error) {
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

func (b *planBuilder) buildDo(v *ast.DoStmt) (Plan, error) {
	dual := LogicalTableDual{RowCount: 1}.init(b.ctx)

	p := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(v.Exprs))...)
	for _, astExpr := range v.Exprs {
		expr, _, err := b.rewrite(astExpr, dual, nil, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.Exprs = append(p.Exprs, expr)
		schema.Append(&expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  expr.GetType(),
		})
	}
	p.SetChildren(dual)
	p.self = p
	p.SetSchema(schema)
	p.calculateNoDelay = true
	return p, nil
}

func (b *planBuilder) buildSet(v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
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
			mockTablePlan := LogicalTableDual{}.init(b.ctx)
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
				Value:   vars.ExtendValue.Datum,
				RetType: &vars.ExtendValue.Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

// Detect aggregate function or groupby clause.
func (b *planBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
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
				return nil, ErrKeyDoesNotExist.GenByArgs(idxName, tblInfo.Name)
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

func findIndexByName(indices []*model.IndexInfo, name model.CIStr) *model.IndexInfo {
	for _, idx := range indices {
		if idx.Name.L == name.L {
			return idx
		}
	}
	return nil
}

func (b *planBuilder) buildSelectLock(src LogicalPlan, lock ast.SelectLockType) *LogicalLock {
	selectLock := LogicalLock{Lock: lock}.init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock
}

func (b *planBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		p.SQLText, _ = x.SQLVar.GetValue().(string)
	} else {
		p.SQLText = x.SQLText
	}
	return p
}

func (b *planBuilder) buildCheckIndex(dbName model.CIStr, as *ast.AdminStmt) (Plan, error) {
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
	}.init(b.ctx)
	is.stats = &statsInfo{}
	cop := &copTask{indexPlan: is}
	// It's double read case.
	ts := PhysicalTableScan{Columns: columns, Table: is.Table}.init(b.ctx)
	ts.SetSchema(is.dataSourceSchema)
	cop.tablePlan = ts
	is.initSchema(id, idx, true)
	t := finishCopTask(b.ctx, cop)

	rootT := t.(*rootTask)
	return rootT.p, nil
}

func (b *planBuilder) buildAdmin(as *ast.AdminStmt) (Plan, error) {
	var ret Plan

	switch as.Tp {
	case ast.AdminCheckTable:
		p := &CheckTable{Tables: as.Tables}
		ret = p
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
	default:
		return nil, ErrUnsupportedType.Gen("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}
	return ret, nil
}

func (b *planBuilder) buildCheckIndexSchema(tn *ast.TableName, indexName string) (*expression.Schema, error) {
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

func getPhysicalIDs(tblInfo *model.TableInfo) []int64 {
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		ids := make([]int64, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
		return ids
	}
	return []int64{tblInfo.ID}
}

func (b *planBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt) Plan {
	p := &Analyze{}
	for _, tbl := range as.TableNames {
		idxInfo, colInfo, pkInfo := getColsInfo(tbl)
		physicalIDs := getPhysicalIDs(tbl.TableInfo)
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
	return p
}

func (b *planBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs := getPhysicalIDs(tblInfo)
	for _, idxName := range as.IndexNames {
		idx := findIndexByName(tblInfo.Indices, idxName)
		if idx == nil || idx.State != model.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenByArgs(idxName.O, tblInfo.Name.O)
		}
		for _, id := range physicalIDs {
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{PhysicalTableID: id, IndexInfo: idx})
		}
	}
	return p, nil
}

func (b *planBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt) Plan {
	p := &Analyze{}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs := getPhysicalIDs(tblInfo)
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			for _, id := range physicalIDs {
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{PhysicalTableID: id, IndexInfo: idx})
			}
		}
	}
	return p
}

func (b *planBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as), nil
		}
		return b.buildAnalyzeIndex(as)
	}
	return b.buildAnalyzeTable(as), nil
}

func buildShowDDLFields() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 4)...)
	schema.Append(buildColumn("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "OWNER", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "RUNNING_JOBS", mysql.TypeVarchar, 256))
	schema.Append(buildColumn("", "SELF_ID", mysql.TypeVarchar, 64))

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
		cs = mysql.DefaultCharset
		cl = mysql.DefaultCollationName
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

func (b *planBuilder) buildShow(show *ast.ShowStmt) (Plan, error) {
	p := Show{
		Tp:          show.Tp,
		DBName:      show.DBName,
		Table:       show.Table,
		Column:      show.Column,
		Flag:        show.Flag,
		Full:        show.Full,
		User:        show.User,
		GlobalScope: show.GlobalScope,
	}.init(b.ctx)
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
		switch showTp {
		case ast.ShowTables, ast.ShowTableStatus:
			if p.DBName == "" {
				return nil, ErrNoDB
			}
		case ast.ShowCreateTable:
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask, show.Table.Schema.L, show.Table.Name.L, "")
		}
		p.SetSchema(buildShowSchema(show))
	}
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	mockTablePlan := LogicalTableDual{}.init(b.ctx)
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
		p.ResolveIndices()
	}
	return p, nil
}

func (b *planBuilder) buildSimple(node ast.StmtNode) Plan {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "")
	case *ast.GrantStmt:
		b.visitInfo = collectVisitInfoFromGrantStmt(b.visitInfo, raw)
	case *ast.SetPwdStmt, *ast.RevokeStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "")
	case *ast.KillStmt:
		// If you have the SUPER privilege, you can kill all threads and statements.
		// Otherwise, you can kill only your own threads and statements.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			processList := sm.ShowProcessList()
			if pi, ok := processList[raw.ConnectionID]; ok {
				loginUser := b.ctx.GetSessionVars().User
				if pi.User != loginUser.Username {
					b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "")
				}
			}
		}
	}
	return p
}

func collectVisitInfoFromGrantStmt(vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "")

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
		vi = appendVisitInfo(vi, item.Priv, dbName, tableName, "")
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, tableName, "")
	}

	return vi
}

func (b *planBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	value, err := table.GetColDefaultValue(b.ctx, col.ToInfo())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

func (b *planBuilder) findDefaultValue(cols []*table.Column, name *ast.ColumnName) (*expression.Constant, error) {
	for _, col := range cols {
		if col.Name.L == name.Name.L {
			return b.getDefaultValue(col)
		}
	}
	return nil, ErrUnknownColumn.GenByArgs(name.Name.O, "field_list")
}

// resolveGeneratedColumns resolves generated columns with their generation
// expressions respectively. onDups indicates which columns are in on-duplicate list.
func (b *planBuilder) resolveGeneratedColumns(columns []*table.Column, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedColumns, err error) {
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

func (b *planBuilder) buildInsert(insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenByArgs()
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenByArgs()
	}
	tableInfo := tn.TableInfo
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
	}.init(b.ctx)

	b.visitInfo = append(b.visitInfo, visitInfo{
		privilege: mysql.InsertPriv,
		db:        tn.DBInfo.Name.L,
		table:     tableInfo.Name.L,
	})

	mockTablePlan := LogicalTableDual{}.init(b.ctx)
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

	insertPlan.ResolveIndices()
	return insertPlan, nil
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
			return nil, nil, ErrUnknownColumn.GenByArgs(assign.Column.OrigColName(), "field list")
		}

		// Check whether the column to be updated is the generated column.
		column := colMap[assign.Column.Name.L]
		if column.IsGenerated() {
			return nil, nil, ErrBadGeneratedColumn.GenByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}
		onDupColSet[column.Name.L] = struct{}{}
		dupCols = append(dupCols, col)
	}
	return onDupColSet, dupCols, nil
}

func (b *planBuilder) getAffectCols(insertStmt *ast.InsertStmt, insertPlan *Insert) (affectedValuesCols []*table.Column, err error) {
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

func (b *planBuilder) buildSetValuesOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
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
			return ErrBadGeneratedColumn.GenByArgs(tCol.Name.O, tableInfo.Name.O)
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

func (b *planBuilder) buildValuesListOfInsert(insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return errors.Trace(err)
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			return ErrWrongValueCountOnRow.GenByArgs(1)
		}
		// No generated column is allowed.
		for _, col := range affectedValuesCols {
			if col.IsGenerated() {
				return ErrBadGeneratedColumn.GenByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
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
			return ErrWrongValueCountOnRow.GenByArgs(i + 1)
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
			case *ast.ValueExpr:
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

func (b *planBuilder) buildSelectPlanOfInsert(insert *ast.InsertStmt, insertPlan *Insert) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return errors.Trace(err)
	}
	selectPlan, err := b.build(insert.Select)
	if err != nil {
		return errors.Trace(err)
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	if selectPlan.Schema().Len() != len(affectedValuesCols) {
		return ErrWrongValueCountOnRow.GenByArgs(1)
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
			return ErrBadGeneratedColumn.GenByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
		}
	}

	insertPlan.SelectPlan, err = doOptimize(b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return errors.Trace(err)
	}

	insertPlan.Schema4OnDuplicate = expression.MergeSchema(insertPlan.tableSchema, insertPlan.SelectPlan.Schema())
	return nil
}

func (b *planBuilder) buildLoadData(ld *ast.LoadDataStmt) (Plan, error) {
	p := &LoadData{
		IsLocal:    ld.IsLocal,
		Path:       ld.Path,
		Table:      ld.Table,
		Columns:    ld.Columns,
		FieldsInfo: ld.FieldsInfo,
		LinesInfo:  ld.LinesInfo,
	}
	tableInfo := p.Table.TableInfo
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.GenByArgs(db, tableInfo.Name.O)
	}
	schema := expression.TableInfo2Schema(b.ctx, tableInfo)
	mockTablePlan := LogicalTableDual{}.init(b.ctx)
	mockTablePlan.SetSchema(schema)

	var err error
	p.GenCols, err = b.resolveGeneratedColumns(tableInPlan.Cols(), nil, mockTablePlan)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

func (b *planBuilder) buildLoadStats(ld *ast.LoadStatsStmt) Plan {
	p := &LoadStats{Path: ld.Path}
	return p
}

func (b *planBuilder) buildDDL(node ast.DDLNode) Plan {
	switch v := node.(type) {
	case *ast.AlterTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
		})
	case *ast.CreateDatabaseStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.CreatePriv,
			db:        v.Name,
		})
	case *ast.CreateIndexStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.IndexPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
		})
	case *ast.CreateTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.CreatePriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
		})
		if v.ReferTable != nil {
			b.visitInfo = append(b.visitInfo, visitInfo{
				privilege: mysql.SelectPriv,
				db:        v.ReferTable.Schema.L,
				table:     v.ReferTable.Name.L,
			})
		}
	case *ast.DropDatabaseStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.DropPriv,
			db:        v.Name,
		})
	case *ast.DropIndexStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.IndexPriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
		})
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			b.visitInfo = append(b.visitInfo, visitInfo{
				privilege: mysql.DropPriv,
				db:        tableVal.Schema.L,
				table:     tableVal.Name.L,
			})
		}
	case *ast.TruncateTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.DeletePriv,
			db:        v.Table.Schema.L,
			table:     v.Table.Name.L,
		})
	case *ast.RenameTableStmt:
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.OldTable.Schema.L,
			table:     v.OldTable.Name.L,
		})
		b.visitInfo = append(b.visitInfo, visitInfo{
			privilege: mysql.AlterPriv,
			db:        v.NewTable.Schema.L,
			table:     v.NewTable.Name.L,
		})
	}

	p := &DDL{Statement: node}
	return p
}

func (b *planBuilder) buildExplain(explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(show)
	}
	targetPlan, err := Optimize(b.ctx, explain.Stmt, b.is)
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
			return nil, ErrUnsupportedType.GenByArgs(targetPlan)
		}
	}
	p := &Explain{StmtPlan: pp}
	switch strings.ToLower(explain.Format) {
	case ast.ExplainFormatROW:
		retFields := []string{"id", "count", "task", "operator info"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		p.SetSchema(schema)
		p.explainedPlans = map[int]bool{}
		p.explainPlanInRowFormat(p.StmtPlan.(PhysicalPlan), "root", "", true)
	case ast.ExplainFormatDOT:
		retFields := []string{"dot contents"}
		schema := expression.NewSchema(make([]*expression.Column, 0, len(retFields))...)
		for _, fieldName := range retFields {
			schema.Append(buildColumn("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
		}
		p.SetSchema(schema)
		p.prepareDotInfo(p.StmtPlan.(PhysicalPlan))
	default:
		return nil, errors.Errorf("explain format '%s' is not supported now", explain.Format)
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
func buildShowSchema(s *ast.ShowStmt) (schema *expression.Schema) {
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
			mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
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
		names = []string{"Table", "Create Table"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowGrants:
		names = []string{fmt.Sprintf("Grants for %s", s.User)}
	case ast.ShowIndex:
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPlugins:
		names = []string{"Name", "Status", "Type", "Library", "License"}
		ftypes = []byte{
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
		}
	case ast.ShowProcessList:
		names = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info", "Mem"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString, mysql.TypeLonglong}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Table_name", "Update_time", "Modify_count", "Row_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Table_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Table_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowProfiles: // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{mysql.TypeLong, mysql.TypeDouble, mysql.TypeVarchar}
	case ast.ShowMasterStatus:
		names = []string{"File", "UniqueID", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
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
