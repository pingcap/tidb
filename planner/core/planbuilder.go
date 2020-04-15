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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util"

	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"go.uber.org/zap"
)

type visitInfo struct {
	privilege mysql.PrivilegeType
	db        string
	table     string
	column    string
	err       error
}

type tableHintInfo struct {
	indexNestedLoopJoinTables []hintTableInfo
	sortMergeJoinTables       []hintTableInfo
	hashJoinTables            []hintTableInfo
	indexHintList             []indexHintInfo
	tiflashTables             []hintTableInfo
	tikvTables                []hintTableInfo
	aggHints                  aggHintInfo
}

type hintTableInfo struct {
	name    model.CIStr
	matched bool
}

type indexHintInfo struct {
	tblName   model.CIStr
	indexHint *ast.IndexHint
}

type aggHintInfo struct {
	preferAggType  uint
	preferAggToCop bool
}

func tableNames2HintTableInfo(hintTables []ast.HintTable) []hintTableInfo {
	if len(hintTables) == 0 {
		return nil
	}
	hintTableInfos := make([]hintTableInfo, len(hintTables))
	for i, hintTable := range hintTables {
		hintTableInfos[i] = hintTableInfo{name: hintTable.TableName}
	}
	return hintTableInfos
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

func (info *tableHintInfo) ifPreferTiFlash(tableNames ...*model.CIStr) bool {
	return info.matchTableName(tableNames, info.tiflashTables)
}

func (info *tableHintInfo) ifPreferTiKV(tableNames ...*model.CIStr) bool {
	return info.matchTableName(tableNames, info.tikvTables)
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (info *tableHintInfo) matchTableName(tables []*model.CIStr, hintTables []hintTableInfo) bool {
	hintMatched := false
	for _, tableName := range tables {
		if tableName == nil {
			continue
		}
		for i, curEntry := range hintTables {
			if curEntry.name.L == tableName.L {
				hintTables[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}

func restore2JoinHint(hintType string, hintTables []hintTableInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	for i, table := range hintTables {
		buffer.WriteString(table.name.L)
		if i < len(hintTables)-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func restore2StorageHint(tiflashTables, tikvTables []hintTableInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(HintReadFromStorage))
	buffer.WriteString("(")
	if len(tiflashTables) > 0 {
		buffer.WriteString("tiflash[")
		for i, tableInfo := range tiflashTables {
			buffer.WriteString(tableInfo.name.L)
			if i < len(tiflashTables)-1 {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString("]")
		if len(tikvTables) > 0 {
			buffer.WriteString(", ")
		}
	}
	if len(tikvTables) > 0 {
		buffer.WriteString("tikv[")
		for i, tableInfo := range tikvTables {
			buffer.WriteString(tableInfo.name.L)
			if i < len(tikvTables)-1 {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString("]")
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func extractUnmatchedTables(hintTables []hintTableInfo) []string {
	var tableNames []string
	for _, table := range hintTables {
		if !table.matched {
			tableNames = append(tableNames, table.name.O)
		}
	}
	return tableNames
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
}

type capFlagType = uint64

const (
	_ capFlagType = iota
	// canExpandAST indicates whether the origin AST can be expanded during plan
	// building. ONLY used for `CreateViewStmt` now.
	canExpandAST
	// collectUnderlyingViewName indicates whether to collect the underlying
	// view names of a CreateViewStmt during plan building.
	collectUnderlyingViewName
)

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
	// optFlag indicates the flags of the optimizer rules.
	optFlag uint64
	// capFlag indicates the capability flags.
	capFlag capFlagType

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	windowSpecs map[string]*ast.WindowSpec

	hintProcessor *BlockHintProcessor
	// SelectLock need this information to locate the lock on partitions.
	partitionedTable []table.PartitionedTable
	// CreateView needs this information to check whether exists nested view.
	underlyingViewNames set.StringSet
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	return b.visitInfo
}

// GetDBTableInfo gets the accessed dbs and tables info.
func (b *PlanBuilder) GetDBTableInfo() []stmtctx.TableEntry {
	var tables []stmtctx.TableEntry
	existsFunc := func(tbls []stmtctx.TableEntry, tbl *stmtctx.TableEntry) bool {
		for _, t := range tbls {
			if t == *tbl {
				return true
			}
		}
		return false
	}
	for _, v := range b.visitInfo {
		tbl := &stmtctx.TableEntry{DB: v.db, Table: v.table}
		if !existsFunc(tables, tbl) {
			tables = append(tables, *tbl)
		}
	}
	return tables
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema, processor *BlockHintProcessor) *PlanBuilder {
	return &PlanBuilder{
		ctx:           sctx,
		is:            is,
		colMapper:     make(map[*ast.ColumnNameExpr]int),
		hintProcessor: processor,
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	b.optFlag |= flagPrunColumns
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(ctx, x)
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}, nil
	case *ast.DeleteStmt:
		return b.buildDelete(ctx, x)
	case *ast.ExecuteStmt:
		return b.buildExecute(ctx, x)
	case *ast.ExplainStmt:
		return b.buildExplain(ctx, x)
	case *ast.ExplainForStmt:
		return b.buildExplainFor(x)
	case *ast.TraceStmt:
		return b.buildTrace(x)
	case *ast.InsertStmt:
		return b.buildInsert(ctx, x)
	case *ast.LoadDataStmt:
		return b.buildLoadData(ctx, x)
	case *ast.LoadStatsStmt:
		return b.buildLoadStats(x), nil
	case *ast.PrepareStmt:
		return b.buildPrepare(x), nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.UnionStmt:
		return b.buildUnion(ctx, x)
	case *ast.UpdateStmt:
		return b.buildUpdate(ctx, x)
	case *ast.ShowStmt:
		return b.buildShow(ctx, x)
	case *ast.DoStmt:
		return b.buildDo(ctx, x)
	case *ast.SetStmt:
		return b.buildSet(ctx, x)
	case *ast.AnalyzeTableStmt:
		return b.buildAnalyze(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DDLNode:
		return b.buildDDL(ctx, x)
	case *ast.CreateBindingStmt:
		return b.buildCreateBindPlan(x)
	case *ast.DropBindingStmt:
		return b.buildDropBindPlan(x)
	case *ast.ChangeStmt:
		return b.buildChange(x)
	case *ast.SplitRegionStmt:
		return b.buildSplitRegion(x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildChange(v *ast.ChangeStmt) (Plan, error) {
	exe := &Change{
		ChangeStmt: v,
	}
	return exe, nil
}

func (b *PlanBuilder) buildExecute(ctx context.Context, v *ast.ExecuteStmt) (Plan, error) {
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		newExpr, _, err := b.rewrite(ctx, expr, nil, nil, true)
		if err != nil {
			return nil, err
		}
		vars = append(vars, newExpr)
	}
	exe := &Execute{Name: v.Name, UsingVars: vars, ExecID: v.ExecID}
	return exe, nil
}

func (b *PlanBuilder) buildDo(ctx context.Context, v *ast.DoStmt) (Plan, error) {
	var p LogicalPlan
	dual := LogicalTableDual{RowCount: 1}.Init(b.ctx)
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(v.Exprs))...)
	for _, astExpr := range v.Exprs {
		expr, np, err := b.rewrite(ctx, astExpr, p, nil, true)
		if err != nil {
			return nil, err
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

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
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
			assign.Expr, _, err = b.rewrite(ctx, vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
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

func (b *PlanBuilder) buildDropBindPlan(v *ast.DropBindingStmt) (Plan, error) {
	p := &SQLBindPlan{
		SQLBindOp:    OpSQLBindDrop,
		NormdOrigSQL: parser.Normalize(v.OriginSel.Text()),
		IsGlobal:     v.GlobalScope,
		Db:           getDefaultDB(b.ctx, v.OriginSel),
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func (b *PlanBuilder) buildCreateBindPlan(v *ast.CreateBindingStmt) (Plan, error) {
	charSet, collation := b.ctx.GetSessionVars().GetCharsetInfo()
	p := &SQLBindPlan{
		SQLBindOp:    OpSQLBindCreate,
		NormdOrigSQL: parser.Normalize(v.OriginSel.Text()),
		BindSQL:      v.HintedSel.Text(),
		IsGlobal:     v.GlobalScope,
		BindStmt:     v.HintedSel,
		Db:           getDefaultDB(b.ctx, v.OriginSel),
		Charset:      charSet,
		Collation:    collation,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func getDefaultDB(ctx sessionctx.Context, sel ast.StmtNode) string {
	implicitDB := &implicitDatabase{}
	sel.Accept(implicitDB)
	if implicitDB.hasImplicit {
		return ctx.GetSessionVars().CurrentDB
	}
	return ""
}

type implicitDatabase struct {
	hasImplicit bool
}

func (i *implicitDatabase) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.TableName:
		if x.Schema.L == "" {
			i.hasImplicit = true
		}
		return in, true
	}
	return in, false
}

func (i *implicitDatabase) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
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
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
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
	if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
		return tablePath
	}
	return nil
}

func isPrimaryIndex(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func (b *PlanBuilder) getPossibleAccessPaths(indexHints []*ast.IndexHint, tblInfo *model.TableInfo, dbName, tblName model.CIStr) ([]*accessPath, error) {
	publicPaths := make([]*accessPath, 0, len(tblInfo.Indices)+2)
	publicPaths = append(publicPaths, &accessPath{isTablePath: true, storeType: kv.TiKV})
	if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
		publicPaths = append(publicPaths, &accessPath{isTablePath: true, storeType: kv.TiFlash})
	}
	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			publicPaths = append(publicPaths, &accessPath{index: index})
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*accessPath, 0, len(publicPaths))
	ignored := make([]*accessPath, 0, len(publicPaths))

	// Extract comment-style index hint like /*+ INDEX(t, idx1, idx2) */.
	indexHintsLen := len(indexHints)
	if hints := b.TableHints(); hints != nil {
		for _, hint := range hints.indexHintList {
			if hint.tblName == tblName {
				indexHints = append(indexHints, hint.indexHint)
			}
		}
	}

	_, isolationReadEnginesHasTiKV := b.ctx.GetSessionVars().GetIsolationReadEngines()[kv.TiKV]
	for i, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		hasScanHint = true

		if !isolationReadEnginesHasTiKV {
			if hint.IndexNames != nil {
				engineVals, _ := b.ctx.GetSessionVars().GetSystemVar(variable.TiDBIsolationReadEngines)
				err := errors.New(fmt.Sprintf("TiDB doesn't support index in the isolation read engines(value: '%v')", engineVals))
				if i < indexHintsLen {
					return nil, err
				}
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			}
			continue
		}
		// It is syntactically valid to omit index_list for USE INDEX, which means “use no indexes”.
		// Omitting index_list for FORCE INDEX or IGNORE INDEX is a syntax error.
		// See https://dev.mysql.com/doc/refman/8.0/en/index-hints.html.
		if hint.IndexNames == nil && hint.HintType != ast.HintIgnore {
			if path := getTablePath(publicPaths); path != nil {
				hasUseOrForce = true
				path.forced = true
				available = append(available, path)
			}
		}
		for _, idxName := range hint.IndexNames {
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				err := ErrKeyDoesNotExist.GenWithStackByArgs(idxName, tblInfo.Name)
				// if hint is from comment-style sql hints, we should throw a warning instead of error.
				if i < indexHintsLen {
					return nil, err
				}
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				continue
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

func (b *PlanBuilder) filterPathByIsolationRead(paths []*accessPath, dbName model.CIStr) ([]*accessPath, error) {
	// TODO: filter paths with isolation read locations.
	if dbName.L == mysql.SystemDB || dbName.L == "information_schema" || dbName.L == "performance_schema" {
		return paths, nil
	}
	isolationReadEngines := b.ctx.GetSessionVars().GetIsolationReadEngines()
	availableEngine := map[kv.StoreType]struct{}{}
	var availableEngineStr string
	for i := len(paths) - 1; i >= 0; i-- {
		if _, ok := availableEngine[paths[i].storeType]; !ok {
			availableEngine[paths[i].storeType] = struct{}{}
			if availableEngineStr != "" {
				availableEngineStr += ", "
			}
			availableEngineStr += paths[i].storeType.Name()
		}
		if _, ok := isolationReadEngines[paths[i].storeType]; !ok {
			paths = append(paths[:i], paths[i+1:]...)
		}
	}
	var err error
	if len(paths) == 0 {
		engineVals, _ := b.ctx.GetSessionVars().GetSystemVar(variable.TiDBIsolationReadEngines)
		err = ErrInternal.GenWithStackByArgs(fmt.Sprintf("Can not find access path matching '%v'(value: '%v'). Available values are '%v'.",
			variable.TiDBIsolationReadEngines, engineVals, availableEngineStr))
	}
	return paths, err
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
	selectLock := LogicalLock{Lock: lock, partitionedTable: b.partitionedTable}.Init(b.ctx)
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

func (b *PlanBuilder) buildCheckIndex(ctx context.Context, dbName model.CIStr, as *ast.AdminStmt) (Plan, error) {
	tblName := as.Tables[0]
	tbl, err := b.is.TableByName(dbName, tblName.Name)
	if err != nil {
		return nil, err
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

	return b.buildPhysicalIndexLookUpReader(ctx, dbName, tbl, idx)
}

func (b *PlanBuilder) buildAdmin(ctx context.Context, as *ast.AdminStmt) (Plan, error) {
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckTable:
		ret, err = b.buildAdminCheckTable(ctx, as)
		if err != nil {
			return ret, err
		}
	case ast.AdminCheckIndex:
		dbName := as.Tables[0].Schema
		readerPlan, err := b.buildCheckIndex(ctx, dbName, as)
		if err != nil {
			return ret, err
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
			return nil, err
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
	case ast.AdminReloadExprPushdownBlacklist:
		return &ReloadExprPushdownBlacklist{}, nil
	case ast.AdminReloadOptRuleBlacklist:
		return &ReloadOptRuleBlacklist{}, nil
	case ast.AdminPluginEnable:
		return &AdminPlugins{Action: Enable, Plugins: as.Plugins}, nil
	case ast.AdminPluginDisable:
		return &AdminPlugins{Action: Disable, Plugins: as.Plugins}, nil
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return ret, nil
}

// getGenExprs gets generated expressions map.
func (b *PlanBuilder) getGenExprs(ctx context.Context, dbName model.CIStr, tbl table.Table, idx *model.IndexInfo) (
	map[model.TableColumnID]expression.Expression, error) {
	tblInfo := tbl.Meta()
	genExprsMap := make(map[model.TableColumnID]expression.Expression)
	exprs := make([]expression.Expression, 0, len(tbl.Cols()))
	genExprIdxs := make([]model.TableColumnID, len(tbl.Cols()))
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	mockTablePlan.SetSchema(expression.TableInfo2SchemaWithDBName(b.ctx, dbName, tblInfo))
	for i, colExpr := range mockTablePlan.Schema().Columns {
		col := tbl.Cols()[i]
		var expr expression.Expression
		expr = colExpr
		if col.IsGenerated() && !col.GeneratedStored {
			var err error
			expr, _, err = b.rewrite(ctx, col.GeneratedExpr, mockTablePlan, nil, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
			expr = expression.BuildCastFunction(b.ctx, expr, colExpr.GetType())
			found := false
			for _, column := range idx.Columns {
				if strings.EqualFold(col.Name.L, column.Name.L) {
					found = true
					break
				}
			}
			if found {
				genColumnID := model.TableColumnID{TableID: tblInfo.ID, ColumnID: col.ColumnInfo.ID}
				genExprsMap[genColumnID] = expr
				genExprIdxs[i] = genColumnID
			}
		}
		exprs = append(exprs, expr)
	}
	// Re-iterate expressions to handle those virtual generated columns that refers to the other generated columns.
	for i, expr := range exprs {
		exprs[i] = expression.ColumnSubstitute(expr, mockTablePlan.Schema(), exprs)
		if _, ok := genExprsMap[genExprIdxs[i]]; ok {
			genExprsMap[genExprIdxs[i]] = exprs[i]
		}
	}
	return genExprsMap, nil
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReader(ctx context.Context, dbName model.CIStr, tbl table.Table, idx *model.IndexInfo) (Plan, error) {
	// Get generated columns.
	var genCols []*expression.Column
	pkOffset := -1
	tblInfo := tbl.Meta()
	colsMap := make(map[int64]struct{})
	schema := expression.NewSchema(make([]*expression.Column, 0, len(idx.Columns))...)
	idxReaderCols := make([]*model.ColumnInfo, 0, len(idx.Columns))
	tblReaderCols := make([]*model.ColumnInfo, 0, len(tbl.Cols()))
	genExprsMap, err := b.getGenExprs(ctx, dbName, tbl, idx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, idxCol := range idx.Columns {
		for _, col := range tblInfo.Columns {
			if idxCol.Name.L == col.Name.L {
				idxReaderCols = append(idxReaderCols, col)
				tblReaderCols = append(tblReaderCols, col)
				schema.Append(&expression.Column{
					ColName:  col.Name,
					UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  &col.FieldType})
				colsMap[col.ID] = struct{}{}
				if mysql.HasPriKeyFlag(col.Flag) {
					pkOffset = len(tblReaderCols) - 1
				}
			}
			genColumnID := model.TableColumnID{TableID: tblInfo.ID, ColumnID: col.ID}
			if expr, ok := genExprsMap[genColumnID]; ok {
				cols := expression.ExtractColumns(expr)
				genCols = append(genCols, cols...)
			}
		}
	}
	// Add generated columns to tblSchema and tblReaderCols.
	tblSchema := schema.Clone()
	for _, col := range genCols {
		if _, ok := colsMap[col.ID]; !ok {
			c := table.FindCol(tbl.Cols(), col.ColName.O)
			if c != nil {
				col.Index = len(tblReaderCols)
				tblReaderCols = append(tblReaderCols, c.ColumnInfo)
				tblSchema.Append(&expression.Column{
					ColName:  c.Name,
					UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  &c.FieldType})
				colsMap[c.ID] = struct{}{}
				if mysql.HasPriKeyFlag(c.Flag) {
					pkOffset = len(tblReaderCols) - 1
				}
			}
		}
	}
	if !tbl.Meta().PKIsHandle || pkOffset == -1 {
		tblReaderCols = append(tblReaderCols, model.NewExtraHandleColInfo())
		handleCol := &expression.Column{
			DBName:   dbName,
			TblName:  tblInfo.Name,
			ColName:  model.ExtraHandleName,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       model.ExtraHandleID,
		}
		tblSchema.Append(handleCol)
		pkOffset = len(tblReaderCols) - 1
	}

	is := PhysicalIndexScan{
		Table:            tblInfo,
		TableAsName:      &tblInfo.Name,
		DBName:           dbName,
		Columns:          idxReaderCols,
		Index:            idx,
		dataSourceSchema: schema,
		Ranges:           ranger.FullRange(),
		GenExprs:         genExprsMap,
	}.Init(b.ctx)
	// There is no alternative plan choices, so just use pseudo stats to avoid panic.
	is.stats = &property.StatsInfo{HistColl: &(statistics.PseudoTable(tblInfo)).HistColl}
	// It's double read case.
	ts := PhysicalTableScan{Columns: tblReaderCols, Table: is.Table, TableAsName: &tblInfo.Name}.Init(b.ctx)
	ts.SetSchema(tblSchema)
	if tbl.Meta().GetPartitionInfo() != nil {
		pid := tbl.(table.PhysicalTable).GetPhysicalID()
		is.physicalTableID = pid
		is.isPartition = true
		ts.physicalTableID = pid
		ts.isPartition = true
	}
	cop := &copTask{
		indexPlan:  is,
		tablePlan:  ts,
		tableStats: is.stats,
	}
	ts.HandleIdx = pkOffset
	is.initSchema(idx, true)
	rootT := finishCopTask(b.ctx, cop).(*rootTask)
	return rootT.p, nil
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReaders(ctx context.Context, dbName model.CIStr, tbl table.Table) ([]Plan, []*model.IndexInfo, error) {
	tblInfo := tbl.Meta()
	// get index information
	indexInfos := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	indexLookUpReaders := make([]Plan, 0, len(tblInfo.Indices))
	for _, idx := range tbl.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			logutil.Logger(context.Background()).Info("build physical index lookup reader, the index isn't public",
				zap.String("index", idxInfo.Name.O), zap.Stringer("state", idxInfo.State), zap.String("table", tblInfo.Name.O))
			continue
		}
		indexInfos = append(indexInfos, idxInfo)
		// For partition tables.
		if pi := tbl.Meta().GetPartitionInfo(); pi != nil {
			for _, def := range pi.Definitions {
				t := tbl.(table.PartitionedTable).GetPartition(def.ID)
				reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, t, idxInfo)
				if err != nil {
					return nil, nil, err
				}
				indexLookUpReaders = append(indexLookUpReaders, reader)
			}
			continue
		}
		// For non-partition tables.
		reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, tbl, idxInfo)
		if err != nil {
			return nil, nil, err
		}
		indexLookUpReaders = append(indexLookUpReaders, reader)
	}
	if len(indexLookUpReaders) == 0 {
		return nil, nil, nil
	}
	return indexLookUpReaders, indexInfos, nil
}

func (b *PlanBuilder) buildAdminCheckTable(ctx context.Context, as *ast.AdminStmt) (*CheckTable, error) {
	tbl := as.Tables[0]
	tableInfo := as.Tables[0].TableInfo
	table, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tbl.DBInfo.Name.O, tableInfo.Name.O)
	}
	p := &CheckTable{
		DBName: tbl.Schema.O,
		Table:  table,
	}
	readerPlans, indexInfos, err := b.buildPhysicalIndexLookUpReaders(ctx, tbl.Schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	readers := make([]*PhysicalIndexLookUpReader, 0, len(readerPlans))
	for _, plan := range readerPlans {
		readers = append(readers, plan.(*PhysicalIndexLookUpReader))
	}
	p.IndexInfos = indexInfos
	p.IndexLookUpReaders = readers
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
	for _, col := range tbl.Columns {
		// The virtual column will not store any data in TiKV, so it should be ignored when collect statistics
		if col.IsGenerated() && !col.GeneratedStored {
			continue
		}
		if tbl.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		} else {
			colsInfo = append(colsInfo, col)
		}
	}
	for _, idx := range tn.TableInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	return
}

func getPhysicalIDsAndPartitionNames(tblInfo *model.TableInfo, partitionNames []model.CIStr) ([]int64, []string, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if len(partitionNames) != 0 {
			return nil, nil, errors.Trace(ddl.ErrPartitionMgmtOnNonpartitioned)
		}
		return []int64{tblInfo.ID}, []string{""}, nil
	}
	if len(partitionNames) == 0 {
		ids := make([]int64, 0, len(pi.Definitions))
		names := make([]string, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
			names = append(names, def.Name.O)
		}
		return ids, names, nil
	}
	ids := make([]int64, 0, len(partitionNames))
	names := make([]string, 0, len(partitionNames))
	for _, name := range partitionNames {
		found := false
		for _, def := range pi.Definitions {
			if def.Name.L == name.L {
				found = true
				ids = append(ids, def.ID)
				names = append(names, def.Name.O)
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("can not found the specified partition name %s in the table definition", name.O)
		}
	}
	return ids, names, nil
}

func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	for _, tbl := range as.TableNames {
		if tbl.TableInfo.IsView() {
			return nil, errors.Errorf("analyze %s is not supported now.", tbl.Name.O)
		}
		idxInfo, colInfo, pkInfo := getColsInfo(tbl)
		physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tbl.TableInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		for _, idx := range idxInfo {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{
					IndexInfo:   idx,
					analyzeInfo: info,
					TblInfo:     tbl.TableInfo,
				})
			}
		}
		if len(colInfo) > 0 || pkInfo != nil {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: tbl.Schema.O, TableName: tbl.Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
					PKInfo:      pkInfo,
					ColsInfo:    colInfo,
					analyzeInfo: info,
					TblInfo:     tbl.TableInfo,
				})
			}
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) && tblInfo.PKIsHandle {
			pkCol := tblInfo.GetPkColInfo()
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info, TblInfo: tblInfo})
			}
			continue
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		for i, id := range physicalIDs {
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt) (Plan, error) {
	p := &Analyze{MaxNumBuckets: as.MaxNumBuckets}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
			}
		}
	}
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		for i, id := range physicalIDs {
			info := analyzeInfo{DBName: as.TableNames[0].Schema.O, TableName: as.TableNames[0].Name.O, PartitionName: names[i], PhysicalTableID: id, Incremental: as.Incremental}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{PKInfo: pkCol, analyzeInfo: info})
		}
	}
	return p, nil
}

const (
	defaultMaxNumBuckets = 256
	numBucketsLimit      = 1024
)

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	// If enable fast analyze, the storage must be tikv.Storage.
	if _, isTikvStorage := b.ctx.GetStore().(tikv.Storage); !isTikvStorage && b.ctx.GetSessionVars().EnableFastAnalyze {
		return nil, errors.Errorf("Only support fast analyze in tikv storage.")
	}
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
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn("", "JOB_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "DB_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "TABLE_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "JOB_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_STATE", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCHEMA_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "TABLE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "ROW_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "START_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "END_TIME", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "STATE", mysql.TypeVarchar, 64))
	return schema
}

func buildTableRegionsSchema() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 11)...)
	schema.Append(buildColumn("", "REGION_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "START_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "END_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "LEADER_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "LEADER_STORE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "PEERS", mysql.TypeVarchar, 64))
	schema.Append(buildColumn("", "SCATTERING", mysql.TypeTiny, 1))
	schema.Append(buildColumn("", "WRITTEN_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "READ_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "APPROXIMATE_SIZE(MB)", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "APPROXIMATE_KEYS", mysql.TypeLonglong, 4))
	return schema
}

func buildSplitRegionsSchema() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, 2)...)
	schema.Append(buildColumn("", "TOTAL_SPLIT_REGION", mysql.TypeLonglong, 4))
	schema.Append(buildColumn("", "SCATTER_FINISH_RATIO", mysql.TypeDouble, 8))
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
	schema.Append(buildColumn("", "DIGEST", mysql.TypeVarchar, 64))
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
		DBName:  util.InformationSchemaName,
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

func (b *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (Plan, error) {
	p := Show{
		Tp:          show.Tp,
		DBName:      show.DBName,
		Table:       show.Table,
		Column:      show.Column,
		IndexName:   show.IndexName,
		Flag:        show.Flag,
		Full:        show.Full,
		User:        show.User,
		Roles:       show.Roles,
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
	case ast.ShowRegions:
		p.SetSchema(buildTableRegionsSchema())
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
		case ast.ShowCreateView:
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SHOW VIEW")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		}
		p.SetSchema(buildShowSchema(show, isView))
	}
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	mockTablePlan := LogicalTableDual{placeHolder: true}.Init(b.ctx)
	mockTablePlan.SetSchema(p.schema)
	var err error
	var np LogicalPlan
	np = mockTablePlan
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.Schema().Columns[0].ColName},
		}
		np, err = b.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Where != nil {
		np, err = b.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if np != mockTablePlan {
		fieldsLen := len(mockTablePlan.schema.Columns)
		proj := LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx)
		schema := expression.NewSchema(make([]*expression.Column, 0, fieldsLen)...)
		for _, col := range mockTablePlan.schema.Columns {
			proj.Exprs = append(proj.Exprs, col)
			newCol := col.Clone().(*expression.Column)
			newCol.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
			schema.Append(newCol)
		}
		proj.SetSchema(schema)
		proj.SetChildren(np)
		physical, err := DoOptimize(ctx, b.optFlag|flagEliminateProjection, proj)
		if err != nil {
			return nil, err
		}
		return substitutePlaceHolderDual(physical, p), nil
	}
	return p, nil
}

func substitutePlaceHolderDual(src PhysicalPlan, dst PhysicalPlan) PhysicalPlan {
	if dual, ok := src.(*PhysicalTableDual); ok && dual.placeHolder {
		return dst
	}
	for i, child := range src.Children() {
		newChild := substitutePlaceHolderDual(child, dst)
		src.SetChild(i, newChild)
	}
	return src
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.AlterInstanceStmt:
		err := ErrSpecificAccessDenied.GenWithStack("ALTER INSTANCE")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
	case *ast.AlterUserStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		if b.ctx.GetSessionVars().CurrentDB == "" && raw.Level.DBName == "" {
			if raw.Level.Level == ast.GrantLevelTable {
				return nil, ErrNoDB
			}
		}
		b.visitInfo = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
	case *ast.GrantRoleStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
	case *ast.RevokeStmt:
		b.visitInfo = collectVisitInfoFromRevokeStmt(b.ctx, b.visitInfo, raw)
	case *ast.RevokeRoleStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
	case *ast.KillStmt:
		// If you have the SUPER privilege, you can kill all threads and statements.
		// Otherwise, you can kill only your own threads and statements.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			if pi, ok := sm.GetProcessInfo(raw.ConnectionID); ok {
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
	case *ast.ShutdownStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShutdownPriv, "", "", "", nil)
	}
	return p, nil
}

func collectVisitInfoFromRevokeStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.RevokeStmt) []visitInfo {
	// To use REVOKE, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	if dbName == "" {
		dbName = sctx.GetSessionVars().CurrentDB
	}
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

func collectVisitInfoFromGrantStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	if dbName == "" {
		dbName = sctx.GetSessionVars().CurrentDB
	}
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
		return nil, err
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
func (b *PlanBuilder) resolveGeneratedColumns(ctx context.Context, columns []*table.Column, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedColumns, err error) {
	for _, column := range columns {
		if !column.IsGenerated() {
			continue
		}
		columnName := &ast.ColumnName{Name: column.Name}
		columnName.SetText(column.Name.O)

		colExpr, _, err := mockPlan.findColumn(columnName)
		if err != nil {
			return igc, err
		}

		expr, _, err := b.rewrite(ctx, column.GeneratedExpr, mockPlan, nil, true)
		if err != nil {
			return igc, err
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

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (Plan, error) {
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

	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
			b.ctx.GetSessionVars().User.Username, tableInfo.Name.L)
	}

	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tn.DBInfo.Name.L,
		tableInfo.Name.L, "", authErr)

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
		err := b.buildSetValuesOfInsert(ctx, insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, err
		}
	} else if len(insert.Lists) > 0 {
		// Branch for `INSERT ... VALUES ...`.
		err := b.buildValuesListOfInsert(ctx, insert, insertPlan, mockTablePlan, checkRefColumn)
		if err != nil {
			return nil, err
		}
	} else {
		// Branch for `INSERT ... SELECT ...`.
		err := b.buildSelectPlanOfInsert(ctx, insert, insertPlan)
		if err != nil {
			return nil, err
		}
	}

	mockTablePlan.SetSchema(insertPlan.Schema4OnDuplicate)

	onDupColSet, err := insertPlan.resolveOnDuplicate(insert.OnDuplicate, tableInfo, func(node ast.ExprNode) (expression.Expression, error) {
		return b.rewriteInsertOnDuplicateUpdate(ctx, node, mockTablePlan, insertPlan)
	})
	if err != nil {
		return nil, err
	}

	// Calculate generated columns.
	mockTablePlan.schema = insertPlan.tableSchema
	insertPlan.GenCols, err = b.resolveGeneratedColumns(ctx, insertPlan.Table.Cols(), onDupColSet, mockTablePlan)
	if err != nil {
		return nil, err
	}

	err = insertPlan.ResolveIndices()
	return insertPlan, err
}

func (p *Insert) resolveOnDuplicate(onDup []*ast.Assignment, tblInfo *model.TableInfo, yield func(ast.ExprNode) (expression.Expression, error)) (map[string]struct{}, error) {
	onDupColSet := make(map[string]struct{}, len(onDup))
	colMap := make(map[string]*table.Column, len(p.Table.Cols()))
	for _, col := range p.Table.Cols() {
		colMap[col.Name.L] = col
	}
	for _, assign := range onDup {
		// Check whether the column to be updated exists in the source table.
		col, err := p.tableSchema.FindColumn(assign.Column)
		if err != nil {
			return nil, err
		} else if col == nil {
			return nil, ErrUnknownColumn.GenWithStackByArgs(assign.Column.OrigColName(), "field list")
		}

		// Check whether the column to be updated is the generated column.
		column := colMap[assign.Column.Name.L]
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
		// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
		if column.IsGenerated() {
			if defaultExpr != nil {
				continue
			}
			return nil, ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}

		onDupColSet[column.Name.L] = struct{}{}

		expr, err := yield(assign.Expr)
		if err != nil {
			return nil, err
		}

		p.OnDuplicate = append(p.OnDuplicate, &expression.Assignment{
			Col:  col,
			Expr: expr,
		})
	}
	return onDupColSet, nil
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
			return nil, err
		}

	} else if len(insertStmt.Setlist) == 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesCols = insertPlan.Table.Cols()
	}
	return affectedValuesCols, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	tableInfo := insertPlan.Table.Meta()
	colNames := make([]string, 0, len(insert.Setlist))
	exprCols := make([]*expression.Column, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		exprCol, err := insertPlan.tableSchema.FindColumn(assign.Column)
		if err != nil {
			return err
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
		return err
	}
	generatedColumns := make(map[string]struct{}, len(tCols))
	for _, tCol := range tCols {
		if tCol.IsGenerated() {
			generatedColumns[tCol.Name.L] = struct{}{}
		}
	}

	for i, assign := range insert.Setlist {
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}
		// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
		// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
		if _, ok := generatedColumns[assign.Column.Name.L]; ok {
			if defaultExpr != nil {
				continue
			}
			return ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tableInfo.Name.O)
		}
		expr, _, err := b.rewriteWithPreprocess(ctx, assign.Expr, mockTablePlan, nil, nil, true, checkRefColumn)
		if err != nil {
			return err
		}
		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			Col:  exprCols[i],
			Expr: expr,
		})
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildValuesListOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this table.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.Columns) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesCols) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
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
			var generatedColumnWithDefaultExpr bool
			col := affectedValuesCols[j]
			switch x := valueItem.(type) {
			case *ast.DefaultExpr:
				if col.IsGenerated() {
					if x.Name != nil {
						return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
					}
					generatedColumnWithDefaultExpr = true
					break
				}
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
				expr, _, err = b.rewriteWithPreprocess(ctx, valueItem, mockTablePlan, nil, nil, true, checkRefColumn)
			}
			if err != nil {
				return err
			}
			// insert value into a generated column is not allowed
			if col.IsGenerated() {
				// but there is only one exception:
				// it is allowed to insert the `default` value into a generated column
				if generatedColumnWithDefaultExpr {
					continue
				}
				return ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
			}

			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	return nil
}

func (b *PlanBuilder) buildSelectPlanOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert) error {
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}
	selectPlan, err := b.Build(ctx, insert.Select)
	if err != nil {
		return err
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

	insertPlan.SelectPlan, err = DoOptimize(ctx, b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return err
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

func (b *PlanBuilder) buildLoadData(ctx context.Context, ld *ast.LoadDataStmt) (Plan, error) {
	p := &LoadData{
		IsLocal:     ld.IsLocal,
		OnDuplicate: ld.OnDuplicate,
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
	p.GenCols, err = b.resolveGeneratedColumns(ctx, tableInPlan.Cols(), nil, mockTablePlan)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (b *PlanBuilder) buildLoadStats(ld *ast.LoadStatsStmt) Plan {
	p := &LoadStats{Path: ld.Path}
	return p
}

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (Plan, error) {
	if len(node.IndexName.L) != 0 {
		return b.buildSplitIndexRegion(node)
	}
	return b.buildSplitTableRegion(node)
}

func (b *PlanBuilder) buildSplitIndexRegion(node *ast.SplitRegionStmt) (Plan, error) {
	tblInfo := node.Table.TableInfo
	indexInfo := tblInfo.FindIndexByName(node.IndexName.L)
	if indexInfo == nil {
		return nil, ErrKeyDoesNotExist.GenWithStackByArgs(node.IndexName, tblInfo.Name)
	}
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, node.Table.Schema, tblInfo)
	mockTablePlan.SetSchema(schema)

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
		IndexInfo:      indexInfo,
	}
	p.SetSchema(buildSplitRegionsSchema())
	// Split index regions by user specified value lists.
	if len(node.SplitOpt.ValueLists) > 0 {
		indexValues := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			if len(valuesItem) > len(indexInfo.Columns) {
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
			if err != nil {
				return nil, err
			}
			indexValues = append(indexValues, values)
		}
		p.ValueLists = indexValues
		return p, nil
	}

	// Split index regions by lower, upper value.
	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) ([]types.Datum, error) {
		if len(valuesItem) == 0 {
			return nil, errors.Errorf("Split index `%v` region %s value count should more than 0", indexInfo.Name, name)
		}
		if len(valuesItem) > len(indexInfo.Columns) {
			return nil, errors.Errorf("Split index `%v` region column count doesn't match value count at %v", indexInfo.Name, name)
		}
		return b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
	}
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		return nil, err
	}
	upperValues, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		return nil, err
	}
	p.Lower = lowerValues
	p.Upper = upperValues

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split index region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) convertValue2ColumnType(valuesItem []ast.ExprNode, mockTablePlan LogicalPlan, indexInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]types.Datum, error) {
	values := make([]types.Datum, 0, len(valuesItem))
	for j, valueItem := range valuesItem {
		colOffset := indexInfo.Columns[j].Offset
		value, err := b.convertValue(valueItem, mockTablePlan, tblInfo.Columns[colOffset])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (b *PlanBuilder) convertValue(valueItem ast.ExprNode, mockTablePlan LogicalPlan, col *model.ColumnInfo) (d types.Datum, err error) {
	var expr expression.Expression
	switch x := valueItem.(type) {
	case *driver.ValueExpr:
		expr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	default:
		expr, _, err = b.rewrite(context.TODO(), valueItem, mockTablePlan, nil, true)
		if err != nil {
			return d, err
		}
	}
	constant, ok := expr.(*expression.Constant)
	if !ok {
		return d, errors.New("Expect constant values")
	}
	value, err := constant.Eval(chunk.Row{})
	if err != nil {
		return d, err
	}
	d, err = value.ConvertTo(b.ctx.GetSessionVars().StmtCtx, &col.FieldType)
	if err != nil {
		if !types.ErrTruncated.Equal(err) {
			return d, err
		}
		valStr, err1 := value.ToString()
		if err1 != nil {
			return d, err
		}
		return d, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
	}
	return d, nil
}

func (b *PlanBuilder) buildSplitTableRegion(node *ast.SplitRegionStmt) (Plan, error) {
	tblInfo := node.Table.TableInfo
	var pkCol *model.ColumnInfo
	if tblInfo.PKIsHandle {
		if col := tblInfo.GetPkColInfo(); col != nil {
			pkCol = col
		}
	}
	if pkCol == nil {
		pkCol = model.NewExtraHandleColInfo()
	}
	mockTablePlan := LogicalTableDual{}.Init(b.ctx)
	schema := expression.TableInfo2SchemaWithDBName(b.ctx, node.Table.Schema, tblInfo)
	mockTablePlan.SetSchema(schema)

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	p.SetSchema(buildSplitRegionsSchema())
	if len(node.SplitOpt.ValueLists) > 0 {
		values := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			if len(valuesItem) > 1 {
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			value, err := b.convertValue(valuesItem[0], mockTablePlan, pkCol)
			if err != nil {
				return nil, err
			}
			values = append(values, []types.Datum{value})
		}
		p.ValueLists = values
		return p, nil
	}

	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) (types.Datum, error) {
		if len(valuesItem) != 1 {
			return types.Datum{}, errors.Errorf("Split table region %s value count should be 1", name)
		}
		return b.convertValue(valuesItem[0], mockTablePlan, pkCol)
	}
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		return nil, err
	}
	upperValue, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		return nil, err
	}
	p.Lower = []types.Datum{lowerValues}
	p.Upper = []types.Datum{upperValue}

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split table region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split table region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (Plan, error) {
	var authErr error
	switch v := node.(type) {
	case *ast.AlterDatabaseStmt:
		if v.AlterDefaultDatabase {
			v.Name = b.ctx.GetSessionVars().CurrentDB
		}
		if v.Name == "" {
			return nil, ErrNoDB
		}
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name, "", "", authErr)
	case *ast.AlterTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		for _, spec := range v.Specs {
			if spec.Tp == ast.AlterTableRenameTable {
				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
					v.Table.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, spec.NewTable.Schema.L,
					spec.NewTable.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, spec.NewTable.Schema.L,
					spec.NewTable.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterTableDropPartition {
				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
						b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
					v.Table.Name.L, "", authErr)
			}
		}
	case *ast.CreateDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name,
			"", "", authErr)
	case *ast.CreateIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.CreateTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		if v.ReferTable != nil {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, v.ReferTable.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.ReferTable.Schema.L,
				v.ReferTable.Name.L, "", authErr)
		}
	case *ast.CreateViewStmt:
		b.capFlag |= canExpandAST
		b.capFlag |= collectUnderlyingViewName
		defer func() {
			b.capFlag &= ^canExpandAST
			b.capFlag &= ^collectUnderlyingViewName
		}()
		b.underlyingViewNames = set.NewStringSet()
		plan, err := b.Build(ctx, v.Select)
		if err != nil {
			return nil, err
		}
		if b.underlyingViewNames.Exist(v.ViewName.Schema.L + "." + v.ViewName.Name.L) {
			return nil, ErrNoSuchTable.GenWithStackByArgs(v.ViewName.Schema.O, v.ViewName.Name.O)
		}
		schema := plan.Schema()
		if v.Cols == nil {
			adjustOverlongViewColname(plan.(LogicalPlan))
			v.Cols = make([]model.CIStr, len(schema.Columns))
			for i, col := range schema.Columns {
				v.Cols[i] = col.ColName
			}
		}
		if len(v.Cols) != schema.Len() {
			return nil, ddl.ErrViewWrongList
		}
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE VIEW", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.ViewName.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateViewPriv, v.ViewName.Schema.L,
			v.ViewName.Name.L, "", authErr)
		if v.Definer.CurrentUser && b.ctx.GetSessionVars().User != nil {
			v.Definer = b.ctx.GetSessionVars().User
		}
		if b.ctx.GetSessionVars().User != nil && v.Definer.String() != b.ctx.GetSessionVars().User.String() {
			err = ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "",
				"", "", err)
		}
	case *ast.DropDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username,
				b.ctx.GetSessionVars().User.Hostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Name,
			"", "", authErr)
	case *ast.DropIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEx", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
					b.ctx.GetSessionVars().User.Username, tableVal.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, tableVal.Schema.L,
				tableVal.Name.L, "", authErr)
		}
	case *ast.TruncateTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.RenameTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.OldTable.Schema.L,
			v.OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.OldTable.Schema.L,
			v.OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.NewTable.Schema.L,
			v.NewTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.Hostname,
				b.ctx.GetSessionVars().User.Username, v.NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, v.NewTable.Schema.L,
			v.NewTable.Name.L, "", authErr)
	case *ast.RecoverTableStmt:
		// Recover table command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt:
		// TODO: add Lock Table privilege check.
	case *ast.CleanupTableLockStmt:
		// This command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
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

func (b *PlanBuilder) buildExplainPlan(targetPlan Plan, format string, analyze bool, execStmt ast.StmtNode) (Plan, error) {
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

	p := &Explain{StmtPlan: pp, Analyze: analyze, Format: format, ExecStmt: execStmt, ExecPlan: targetPlan}
	p.ctx = b.ctx
	err := p.prepareSchema()
	if err != nil {
		return nil, err
	}
	return p, nil
}

// buildExplainFor gets *last* (maybe running or finished) query plan from connection #connection id.
// See https://dev.mysql.com/doc/refman/8.0/en/explain-for-connection.html.
func (b *PlanBuilder) buildExplainFor(explainFor *ast.ExplainForStmt) (Plan, error) {
	processInfo, ok := b.ctx.GetSessionManager().GetProcessInfo(explainFor.ConnectionID)
	if !ok {
		return nil, ErrNoSuchThread.GenWithStackByArgs(explainFor.ConnectionID)
	}
	if b.ctx.GetSessionVars() != nil && b.ctx.GetSessionVars().User != nil {
		if b.ctx.GetSessionVars().User.Username != processInfo.User {
			err := ErrAccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username, b.ctx.GetSessionVars().User.Hostname)
			// Different from MySQL's behavior and document.
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
	}

	targetPlan, ok := processInfo.Plan.(Plan)
	if !ok || targetPlan == nil {
		return &Explain{Format: explainFor.Format}, nil
	}

	return b.buildExplainPlan(targetPlan, explainFor.Format, false, nil)
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}
	targetPlan, err := OptimizeAstNode(ctx, b.ctx, explain.Stmt, b.is)
	if err != nil {
		return nil, err
	}

	return b.buildExplainPlan(targetPlan, explain.Format, explain.Analyze, explain.Stmt)
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
	case ast.ShowOpenTables:
		names = []string{"Database", "Table", "In_use", "Name_locked"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeLong}
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
	case ast.ShowCreateUser:
		if s.User != nil {
			names = []string{fmt.Sprintf("CREATE USER for %s", s.User)}
		}
	case ast.ShowCreateView:
		names = []string{"View", "Create View", "character_set_client", "collation_connection"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowDrainerStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
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
	case ast.ShowPumpStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "Update_Time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble}
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
	case ast.ShowBindings:
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time", "State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeDatetime, mysql.TypeVarchar}
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

// adjustOverlongViewColname adjusts the overlong outputNames of a view to
// `new_exp_$off` where `$off` is the offset of the output column, $off starts from 1.
// There is still some MySQL compatible problems.
func adjustOverlongViewColname(plan LogicalPlan) {
	outputCols := plan.Schema().Columns
	for i := range outputCols {
		if outputName := outputCols[i].ColName.L; len(outputName) > mysql.MaxColumnNameLength {
			outputCols[i].ColName = model.NewCIStr(fmt.Sprintf("name_exp_%d", i+1))
		}
	}
}
