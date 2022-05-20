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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	util2 "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	utilparser "github.com/pingcap/tidb/util/parser"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type visitInfo struct {
	privilege        mysql.PrivilegeType
	db               string
	table            string
	column           string
	err              error
	alterWritable    bool
	dynamicPriv      string
	dynamicWithGrant bool
}

type indexNestedLoopJoinTables struct {
	inljTables  []hintTableInfo
	inlhjTables []hintTableInfo
	inlmjTables []hintTableInfo
}

type tableHintInfo struct {
	indexNestedLoopJoinTables
	sortMergeJoinTables []hintTableInfo
	broadcastJoinTables []hintTableInfo
	hashJoinTables      []hintTableInfo
	indexHintList       []indexHintInfo
	tiflashTables       []hintTableInfo
	tikvTables          []hintTableInfo
	aggHints            aggHintInfo
	indexMergeHintList  []indexHintInfo
	timeRangeHint       ast.HintTimeRange
	limitHints          limitHintInfo
	leadingJoinOrder    []hintTableInfo
}

type limitHintInfo struct {
	preferLimitToCop bool
}

type hintTableInfo struct {
	dbName       model.CIStr
	tblName      model.CIStr
	partitions   []model.CIStr
	selectOffset int
	matched      bool
}

type indexHintInfo struct {
	dbName     model.CIStr
	tblName    model.CIStr
	partitions []model.CIStr
	indexHint  *ast.IndexHint
	// Matched indicates whether this index hint
	// has been successfully applied to a DataSource.
	// If an indexHintInfo is not matched after building
	// a Select statement, we will generate a warning for it.
	matched bool
}

func (hint *indexHintInfo) hintTypeString() string {
	switch hint.indexHint.HintType {
	case ast.HintUse:
		return "use_index"
	case ast.HintIgnore:
		return "ignore_index"
	case ast.HintForce:
		return "force_index"
	}
	return ""
}

// indexString formats the indexHint as dbName.tableName[, indexNames].
func (hint *indexHintInfo) indexString() string {
	var indexListString string
	indexList := make([]string, len(hint.indexHint.IndexNames))
	for i := range hint.indexHint.IndexNames {
		indexList[i] = hint.indexHint.IndexNames[i].L
	}
	if len(indexList) > 0 {
		indexListString = fmt.Sprintf(", %s", strings.Join(indexList, ", "))
	}
	return fmt.Sprintf("%s.%s%s", hint.dbName, hint.tblName, indexListString)
}

type aggHintInfo struct {
	preferAggType  uint
	preferAggToCop bool
}

// QueryTimeRange represents a time range specified by TIME_RANGE hint
type QueryTimeRange struct {
	From time.Time
	To   time.Time
}

// Condition returns a WHERE clause base on it's value
func (tr *QueryTimeRange) Condition() string {
	return fmt.Sprintf("where time>='%s' and time<='%s'", tr.From.Format(MetricTableTimeFormat), tr.To.Format(MetricTableTimeFormat))
}

func tableNames2HintTableInfo(ctx sessionctx.Context, hintName string, hintTables []ast.HintTable, p *hint.BlockHintProcessor, currentOffset int) []hintTableInfo {
	if len(hintTables) == 0 {
		return nil
	}
	hintTableInfos := make([]hintTableInfo, 0, len(hintTables))
	defaultDBName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	isInapplicable := false
	for _, hintTable := range hintTables {
		tableInfo := hintTableInfo{
			dbName:       hintTable.DBName,
			tblName:      hintTable.TableName,
			partitions:   hintTable.PartitionList,
			selectOffset: p.GetHintOffset(hintTable.QBName, currentOffset),
		}
		if tableInfo.dbName.L == "" {
			tableInfo.dbName = defaultDBName
		}
		switch hintName {
		case TiDBMergeJoin, HintSMJ, TiDBIndexNestedLoopJoin, HintINLJ, HintINLHJ, HintINLMJ, TiDBHashJoin, HintHJ, HintLeading:
			if len(tableInfo.partitions) > 0 {
				isInapplicable = true
			}
		}
		hintTableInfos = append(hintTableInfos, tableInfo)
	}
	if isInapplicable {
		ctx.GetSessionVars().StmtCtx.AppendWarning(
			errors.New(fmt.Sprintf("Optimizer Hint %s is inapplicable on specified partitions",
				restore2JoinHint(hintName, hintTableInfos))))
		return nil
	}
	return hintTableInfos
}

func (info *tableHintInfo) ifPreferMergeJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.sortMergeJoinTables)
}

func (info *tableHintInfo) ifPreferBroadcastJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.broadcastJoinTables)
}

func (info *tableHintInfo) ifPreferHashJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.hashJoinTables)
}

func (info *tableHintInfo) ifPreferINLJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inljTables)
}

func (info *tableHintInfo) ifPreferINLHJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inlhjTables)
}

func (info *tableHintInfo) ifPreferINLMJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inlmjTables)
}

func (info *tableHintInfo) ifPreferTiFlash(tableName *hintTableInfo) *hintTableInfo {
	if tableName == nil {
		return nil
	}
	for i, tbl := range info.tiflashTables {
		if tableName.dbName.L == tbl.dbName.L && tableName.tblName.L == tbl.tblName.L && tbl.selectOffset == tableName.selectOffset {
			info.tiflashTables[i].matched = true
			return &tbl
		}
	}
	return nil
}

func (info *tableHintInfo) ifPreferTiKV(tableName *hintTableInfo) *hintTableInfo {
	if tableName == nil {
		return nil
	}
	for i, tbl := range info.tikvTables {
		if tableName.dbName.L == tbl.dbName.L && tableName.tblName.L == tbl.tblName.L && tbl.selectOffset == tableName.selectOffset {
			info.tikvTables[i].matched = true
			return &tbl
		}
	}
	return nil
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (info *tableHintInfo) matchTableName(tables []*hintTableInfo, hintTables []hintTableInfo) bool {
	hintMatched := false
	for _, table := range tables {
		for i, curEntry := range hintTables {
			if table == nil {
				continue
			}
			if curEntry.dbName.L == table.dbName.L && curEntry.tblName.L == table.tblName.L && table.selectOffset == curEntry.selectOffset {
				hintTables[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}

func restore2TableHint(hintTables ...hintTableInfo) string {
	buffer := bytes.NewBufferString("")
	for i, table := range hintTables {
		buffer.WriteString(table.tblName.L)
		if len(table.partitions) > 0 {
			buffer.WriteString(" PARTITION(")
			for j, partition := range table.partitions {
				if j > 0 {
					buffer.WriteString(", ")
				}
				buffer.WriteString(partition.L)
			}
			buffer.WriteString(")")
		}
		if i < len(hintTables)-1 {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

func restore2JoinHint(hintType string, hintTables []hintTableInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2TableHint(hintTables...))
	buffer.WriteString(") */")
	return buffer.String()
}

func restore2IndexHint(hintType string, hintIndex indexHintInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2TableHint(hintTableInfo{
		dbName:     hintIndex.dbName,
		tblName:    hintIndex.tblName,
		partitions: hintIndex.partitions,
	}))
	if hintIndex.indexHint != nil && len(hintIndex.indexHint.IndexNames) > 0 {
		for i, indexName := range hintIndex.indexHint.IndexNames {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(" " + indexName.L)
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
		buffer.WriteString(restore2TableHint(tiflashTables...))
		buffer.WriteString("]")
		if len(tikvTables) > 0 {
			buffer.WriteString(", ")
		}
	}
	if len(tikvTables) > 0 {
		buffer.WriteString("tikv[")
		buffer.WriteString(restore2TableHint(tikvTables...))
		buffer.WriteString("]")
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func extractUnmatchedTables(hintTables []hintTableInfo) []string {
	var tableNames []string
	for _, table := range hintTables {
		if !table.matched {
			tableNames = append(tableNames, table.tblName.O)
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
	expressionClause
	windowOrderByClause
	partitionByClause
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
	expressionClause:    "expression",
	windowOrderByClause: "window order by",
	partitionByClause:   "window partition by",
}

type capFlagType = uint64

const (
	_ capFlagType = iota
	// canExpandAST indicates whether the origin AST can be expanded during plan
	// building. ONLY used for `CreateViewStmt` now.
	canExpandAST
	// renameView indicates a view is being renamed, so we cannot use the origin
	// definition of that view.
	renameView
)

type cteInfo struct {
	def *ast.CommonTableExpression
	// nonRecursive is used to decide if a CTE is visible. If a CTE start with `WITH RECURSIVE`, then nonRecursive is false,
	// so it is visible in its definition.
	nonRecursive bool
	// useRecursive is used to record if a subSelect in CTE's definition refer to itself. This help us to identify the seed part and recursive part.
	useRecursive bool
	isBuilding   bool
	// isDistinct indicates if the union between seed part and recursive part is distinct or not.
	isDistinct bool
	// seedLP is the seed part's logical plan.
	seedLP LogicalPlan
	// recurLP is the recursive part's logical plan.
	recurLP LogicalPlan
	// storageID for this CTE.
	storageID int
	// optFlag is the optFlag for the whole CTE.
	optFlag uint64
	// enterSubquery and recursiveRef are used to check "recursive table must be referenced only once, and not in any subquery".
	enterSubquery bool
	recursiveRef  bool
	limitLP       LogicalPlan
	// seedStat is shared between logicalCTE and logicalCTETable.
	seedStat *property.StatsInfo
	// The LogicalCTEs that reference the same table should share the same CteClass.
	cteClass *CTEClass
}

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          sessionctx.Context
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	outerCTEs    []*cteInfo
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

	windowSpecs  map[string]*ast.WindowSpec
	inUpdateStmt bool
	inDeleteStmt bool
	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	// handleHelper records the handle column position for tables. Delete/Update/SelectLock/UnionScan may need this information.
	// It collects the information by the following procedure:
	//   Since we build the plan tree from bottom to top, we maintain a stack to record the current handle information.
	//   If it's a dataSource/tableDual node, we create a new map.
	//   If it's a aggregation, we pop the map and push a nil map since no handle information left.
	//   If it's a union, we pop all children's and push a nil map.
	//   If it's a join, we pop its children's out then merge them and push the new map to stack.
	//   If we meet a subquery, it's clearly that it's a independent problem so we just pop one map out when we finish building the subquery.
	handleHelper *handleColHelper

	hintProcessor *hint.BlockHintProcessor
	// selectOffset is the offsets of current processing select stmts.
	selectOffset []int

	// SelectLock need this information to locate the lock on partitions.
	partitionedTable []table.PartitionedTable
	// buildingViewStack is used to check whether there is a recursive view.
	buildingViewStack set.StringSet
	// renamingViewName is the name of the view which is being renamed.
	renamingViewName string
	// isCreateView indicates whether the query is create view.
	isCreateView bool

	// evalDefaultExpr needs this information to find the corresponding column.
	// It stores the OutputNames before buildProjection.
	allNames [][]*types.FieldName

	// isSampling indicates whether the query is sampling.
	isSampling bool

	// correlatedAggMapper stores columns for correlated aggregates which should be evaluated in outer query.
	correlatedAggMapper map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn

	// isForUpdateRead should be true in either of the following situations
	// 1. use `inside insert`, `update`, `delete` or `select for update` statement
	// 2. isolation level is RC
	isForUpdateRead             bool
	allocIDForCTEStorage        int
	buildingRecursivePartForCTE bool
	buildingCTE                 bool
}

type handleColHelper struct {
	id2HandleMapStack []map[int64][]HandleCols
	stackTail         int
}

func (hch *handleColHelper) resetForReuse() {
	*hch = handleColHelper{
		id2HandleMapStack: hch.id2HandleMapStack[:0],
	}
}

func (hch *handleColHelper) popMap() map[int64][]HandleCols {
	ret := hch.id2HandleMapStack[hch.stackTail-1]
	hch.stackTail--
	hch.id2HandleMapStack = hch.id2HandleMapStack[:hch.stackTail]
	return ret
}

func (hch *handleColHelper) pushMap(m map[int64][]HandleCols) {
	hch.id2HandleMapStack = append(hch.id2HandleMapStack, m)
	hch.stackTail++
}

func (hch *handleColHelper) mergeAndPush(m1, m2 map[int64][]HandleCols) {
	newMap := make(map[int64][]HandleCols, mathutil.Max(len(m1), len(m2)))
	for k, v := range m1 {
		newMap[k] = make([]HandleCols, len(v))
		copy(newMap[k], v)
	}
	for k, v := range m2 {
		if _, ok := newMap[k]; ok {
			newMap[k] = append(newMap[k], v...)
		} else {
			newMap[k] = make([]HandleCols, len(v))
			copy(newMap[k], v)
		}
	}
	hch.pushMap(newMap)
}

func (hch *handleColHelper) tailMap() map[int64][]HandleCols {
	return hch.id2HandleMapStack[hch.stackTail-1]
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	return b.visitInfo
}

// GetIsForUpdateRead gets if the PlanBuilder use forUpdateRead
func (b *PlanBuilder) GetIsForUpdateRead() bool {
	return b.isForUpdateRead
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
	if b.isSampling {
		// Disable logical optimization to avoid the optimizer
		// push down/eliminate operands like Selection, Limit or Sort.
		return 0
	}
	return b.optFlag
}

func (b *PlanBuilder) getSelectOffset() int {
	if len(b.selectOffset) > 0 {
		return b.selectOffset[len(b.selectOffset)-1]
	}
	return -1
}

func (b *PlanBuilder) pushSelectOffset(offset int) {
	b.selectOffset = append(b.selectOffset, offset)
}

func (b *PlanBuilder) popSelectOffset() {
	b.selectOffset = b.selectOffset[:len(b.selectOffset)-1]
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder() *PlanBuilder {
	return &PlanBuilder{
		outerCTEs:           make([]*cteInfo, 0),
		colMapper:           make(map[*ast.ColumnNameExpr]int),
		handleHelper:        &handleColHelper{id2HandleMapStack: make([]map[int64][]HandleCols, 0)},
		correlatedAggMapper: make(map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn),
	}
}

// Init initialize a PlanBuilder.
// Return the original PlannerSelectBlockAsName as well, callers decide if
// PlannerSelectBlockAsName should be restored after using this builder.
// This is The comman code pattern to use it:
// NewPlanBuilder().Init(sctx, is, processor)
func (b *PlanBuilder) Init(sctx sessionctx.Context, is infoschema.InfoSchema, processor *hint.BlockHintProcessor) (*PlanBuilder, []ast.HintTable) {
	savedBlockNames := sctx.GetSessionVars().PlannerSelectBlockAsName
	if processor == nil {
		sctx.GetSessionVars().PlannerSelectBlockAsName = nil
	} else {
		sctx.GetSessionVars().PlannerSelectBlockAsName = make([]ast.HintTable, processor.MaxSelectStmtOffset()+1)
	}

	b.ctx = sctx
	b.is = is
	b.hintProcessor = processor
	b.isForUpdateRead = sctx.GetSessionVars().IsPessimisticReadConsistency()
	return b, savedBlockNames
}

// ResetForReuse reset the plan builder, put it into pool for reuse.
// After reset for reuse, the object should be equal to a object returned by NewPlanBuilder().
func (b *PlanBuilder) ResetForReuse() *PlanBuilder {
	// Save some fields for reuse.
	saveOuterCTEs := b.outerCTEs[:0]
	saveColMapper := b.colMapper
	for k := range saveColMapper {
		delete(saveColMapper, k)
	}
	saveHandleHelper := b.handleHelper
	saveHandleHelper.resetForReuse()

	saveCorrelateAggMapper := b.correlatedAggMapper
	for k := range saveCorrelateAggMapper {
		delete(saveCorrelateAggMapper, k)
	}

	// Reset ALL the fields.
	*b = PlanBuilder{}

	// Reuse part of the fields.
	// It's a bit conservative but easier to get right.
	b.outerCTEs = saveOuterCTEs
	b.colMapper = saveColMapper
	b.handleHelper = saveHandleHelper
	b.correlatedAggMapper = saveCorrelateAggMapper

	// Add more fields if they are safe to be reused.

	return b
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
	case *ast.IndexAdviseStmt:
		return b.buildIndexAdvise(x), nil
	case *ast.PlanReplayerStmt:
		return b.buildPlanReplayer(x), nil
	case *ast.PrepareStmt:
		return b.buildPrepare(x), nil
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil {
			return b.buildSelectInto(ctx, x)
		}
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	case *ast.UpdateStmt:
		return b.buildUpdate(ctx, x)
	case *ast.ShowStmt:
		return b.buildShow(ctx, x)
	case *ast.DoStmt:
		return b.buildDo(ctx, x)
	case *ast.SetStmt:
		return b.buildSet(ctx, x)
	case *ast.SetConfigStmt:
		return b.buildSetConfig(ctx, x)
	case *ast.AnalyzeTableStmt:
		return b.buildAnalyze(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt, *ast.BRIEStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt,
		*ast.RenameUserStmt, *ast.NonTransactionalDeleteStmt:
		return b.buildSimple(ctx, node.(ast.StmtNode))
	case ast.DDLNode:
		return b.buildDDL(ctx, x)
	case *ast.CreateBindingStmt:
		return b.buildCreateBindPlan(x)
	case *ast.DropBindingStmt:
		return b.buildDropBindPlan(x)
	case *ast.SetBindingStmt:
		return b.buildSetBindingStatusPlan(x)
	case *ast.ChangeStmt:
		return b.buildChange(x)
	case *ast.SplitRegionStmt:
		return b.buildSplitRegion(x)
	case *ast.CompactTableStmt:
		return b.buildCompactTable(x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildSetConfig(ctx context.Context, v *ast.SetConfigStmt) (Plan, error) {
	privErr := ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ConfigPriv, "", "", "", privErr)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	expr, _, err := b.rewrite(ctx, v.Value, mockTablePlan, nil, true)
	return &SetConfig{Name: v.Name, Type: v.Type, Instance: v.Instance, Value: expr}, err
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
	exe := &Execute{Name: v.Name, TxtProtoVars: vars, ExecID: v.ExecID}
	if v.BinaryArgs != nil {
		exe.BinProtoVars = v.BinaryArgs.([]types.Datum)
	}
	return exe, nil
}

func (b *PlanBuilder) buildDo(ctx context.Context, v *ast.DoStmt) (Plan, error) {
	var p LogicalPlan
	dual := LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx, b.getSelectOffset())
	proj.names = make([]*types.FieldName, len(v.Exprs))
	schema := expression.NewSchema(make([]*expression.Column, 0, len(v.Exprs))...)

	// Since do statement only contain expression list, and it may contain aggFunc, detecting to build the aggMapper firstly.
	var (
		err      error
		aggFuncs []*ast.AggregateFuncExpr
		totalMap map[*ast.AggregateFuncExpr]int
	)
	hasAgg := b.detectAggInExprNode(v.Exprs)
	needBuildAgg := hasAgg
	if hasAgg {
		if b.buildingRecursivePartForCTE {
			return nil, ErrCTERecursiveForbidsAggregation.GenWithStackByArgs(b.genCTETableNameForError())
		}

		aggFuncs, totalMap = b.extractAggFuncsInExprs(v.Exprs)

		if len(aggFuncs) == 0 {
			needBuildAgg = false
		}
	}
	if needBuildAgg {
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(ctx, p, aggFuncs, nil, nil)
		if err != nil {
			return nil, err
		}
		for agg, idx := range totalMap {
			totalMap[agg] = aggIndexMap[idx]
		}
	}

	for _, astExpr := range v.Exprs {
		expr, np, err := b.rewrite(ctx, astExpr, p, totalMap, true)
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
	proj.CalculateNoDelay = true
	return proj, nil
}

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		if vars.IsGlobal {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or SYSTEM_VARIABLES_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "SYSTEM_VARIABLES_ADMIN", false, err)
		}
		if sem.IsEnabled() && sem.IsInvisibleSysVar(strings.ToLower(vars.Name)) {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_VARIABLES_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "RESTRICTED_VARIABLES_ADMIN", false, err)
		}
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Convert column name expression to string value expression.
				char, col := b.ctx.GetSessionVars().GetCharsetInfo()
				vars.Value = ast.NewValueExpr(cn.Name.Name.O, char, col)
			}
			mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
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
		NormdOrigSQL: parser.Normalize(utilparser.RestoreWithDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, v.OriginNode.Text())),
		IsGlobal:     v.GlobalScope,
		Db:           utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB),
	}
	if v.HintedNode != nil {
		p.BindSQL = utilparser.RestoreWithDefaultDB(v.HintedNode, b.ctx.GetSessionVars().CurrentDB, v.HintedNode.Text())
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func (b *PlanBuilder) buildSetBindingStatusPlan(v *ast.SetBindingStmt) (Plan, error) {
	p := &SQLBindPlan{
		SQLBindOp:    OpSetBindingStatus,
		NormdOrigSQL: parser.Normalize(utilparser.RestoreWithDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, v.OriginNode.Text())),
		Db:           utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB),
	}
	switch v.BindingStatusType {
	case ast.BindingStatusTypeEnabled:
		p.NewStatus = bindinfo.Enabled
	case ast.BindingStatusTypeDisabled:
		p.NewStatus = bindinfo.Disabled
	}
	if v.HintedNode != nil {
		p.BindSQL = utilparser.RestoreWithDefaultDB(v.HintedNode, b.ctx.GetSessionVars().CurrentDB, v.HintedNode.Text())
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func checkHintedSQL(sql, charset, collation, db string) error {
	p := parser.New()
	hintsSet, _, warns, err := hint.ParseHintsSet(p, sql, charset, collation, db)
	if err != nil {
		return err
	}
	hintsStr, err := hintsSet.Restore()
	if err != nil {
		return err
	}
	// For `create global binding for select * from t using select * from t`, we allow it though hintsStr is empty.
	// For `create global binding for select * from t using select /*+ non_exist_hint() */ * from t`,
	// the hint is totally invalid, we escalate warning to error.
	if hintsStr == "" && len(warns) > 0 {
		return warns[0]
	}
	return nil
}

func (b *PlanBuilder) buildCreateBindPlan(v *ast.CreateBindingStmt) (Plan, error) {
	charSet, collation := b.ctx.GetSessionVars().GetCharsetInfo()

	// Because we use HintedNode.Restore instead of HintedNode.Text, so we need do some check here
	// For example, if HintedNode.Text is `select /*+ non_exist_hint() */ * from t` and the current DB is `test`,
	// the HintedNode.Restore will be `select * from test . t`.
	// In other words, illegal hints will be deleted during restore. We can't check hinted SQL after restore.
	// So we need check here.
	if err := checkHintedSQL(v.HintedNode.Text(), charSet, collation, b.ctx.GetSessionVars().CurrentDB); err != nil {
		return nil, err
	}

	p := &SQLBindPlan{
		SQLBindOp:    OpSQLBindCreate,
		NormdOrigSQL: parser.Normalize(utilparser.RestoreWithDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, v.OriginNode.Text())),
		BindSQL:      utilparser.RestoreWithDefaultDB(v.HintedNode, b.ctx.GetSessionVars().CurrentDB, v.HintedNode.Text()),
		IsGlobal:     v.GlobalScope,
		BindStmt:     v.HintedNode,
		Db:           utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB),
		Charset:      charSet,
		Collation:    collation,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

// detectAggInExprNode detects an aggregate function in its exprs.
func (b *PlanBuilder) detectAggInExprNode(exprs []ast.ExprNode) bool {
	for _, expr := range exprs {
		if ast.HasAggFlag(expr) {
			return true
		}
	}
	return false
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if f.WildCard != nil {
			continue
		}
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

func getPathByIndexName(paths []*util.AccessPath, idxName model.CIStr, tblInfo *model.TableInfo) *util.AccessPath {
	var primaryIdxPath *util.AccessPath
	for _, path := range paths {
		if path.StoreType == kv.TiFlash {
			continue
		}
		if path.IsTablePath() {
			primaryIdxPath = path
			continue
		}
		if path.Index.Name.L == idxName.L {
			return path
		}
	}
	if isPrimaryIndex(idxName) && tblInfo.HasClusteredIndex() {
		return primaryIdxPath
	}
	return nil
}

func isPrimaryIndex(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func genTiFlashPath(tblInfo *model.TableInfo) *util.AccessPath {
	tiFlashPath := &util.AccessPath{StoreType: kv.TiFlash}
	fillContentForTablePath(tiFlashPath, tblInfo)
	return tiFlashPath
}

func fillContentForTablePath(tablePath *util.AccessPath, tblInfo *model.TableInfo) {
	if tblInfo.IsCommonHandle {
		tablePath.IsCommonHandlePath = true
		for _, index := range tblInfo.Indices {
			if index.Primary {
				tablePath.Index = index
				break
			}
		}
	} else {
		tablePath.IsIntHandlePath = true
	}
}

// isForUpdateReadSelectLock checks if the lock type need to use forUpdateRead
func isForUpdateReadSelectLock(lock *ast.SelectLockInfo) bool {
	if lock == nil {
		return false
	}
	return lock.LockType == ast.SelectLockForUpdate ||
		lock.LockType == ast.SelectLockForUpdateNoWait ||
		lock.LockType == ast.SelectLockForUpdateWaitN
}

// getLatestIndexInfo gets the index info of latest schema version from given table id,
// it returns nil if the schema version is not changed
func getLatestIndexInfo(ctx sessionctx.Context, id int64, startVer int64) (map[int64]*model.IndexInfo, bool, error) {
	dom := domain.GetDomain(ctx)
	if dom == nil {
		return nil, false, errors.New("domain not found for ctx")
	}
	is := temptable.AttachLocalTemporaryTableInfoSchema(ctx, dom.InfoSchema())
	if is.SchemaMetaVersion() == startVer {
		return nil, false, nil
	}
	latestIndexes := make(map[int64]*model.IndexInfo)

	latestTbl, latestTblExist := is.TableByID(id)
	if latestTblExist {
		latestTblInfo := latestTbl.Meta()
		for _, index := range latestTblInfo.Indices {
			latestIndexes[index.ID] = index
		}
	}
	return latestIndexes, true, nil
}

func getPossibleAccessPaths(ctx sessionctx.Context, tableHints *tableHintInfo, indexHints []*ast.IndexHint, tbl table.Table, dbName, tblName model.CIStr, check bool, startVer int64) ([]*util.AccessPath, error) {
	tblInfo := tbl.Meta()
	publicPaths := make([]*util.AccessPath, 0, len(tblInfo.Indices)+2)
	tp := kv.TiKV
	if tbl.Type().IsClusterTable() {
		tp = kv.TiDB
	}
	tablePath := &util.AccessPath{StoreType: tp}
	fillContentForTablePath(tablePath, tblInfo)
	publicPaths = append(publicPaths, tablePath)

	if tblInfo.TiFlashReplica == nil {
		ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there aren't tiflash replicas of table `" + tblInfo.Name.O + "`.")
	} else if !tblInfo.TiFlashReplica.Available {
		ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because tiflash replicas of table `" + tblInfo.Name.O + "` not ready.")
	} else {
		publicPaths = append(publicPaths, genTiFlashPath(tblInfo))
	}

	optimizerUseInvisibleIndexes := ctx.GetSessionVars().OptimizerUseInvisibleIndexes

	check = check || ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			// Filter out invisible index, because they are not visible for optimizer
			if !optimizerUseInvisibleIndexes && index.Invisible {
				continue
			}
			if tblInfo.IsCommonHandle && index.Primary {
				continue
			}
			if check && latestIndexes == nil {
				latestIndexes, check, err = getLatestIndexInfo(ctx, tblInfo.ID, 0)
				if err != nil {
					return nil, err
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[index.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			publicPaths = append(publicPaths, &util.AccessPath{Index: index})
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*util.AccessPath, 0, len(publicPaths))
	ignored := make([]*util.AccessPath, 0, len(publicPaths))

	// Extract comment-style index hint like /*+ INDEX(t, idx1, idx2) */.
	indexHintsLen := len(indexHints)
	if tableHints != nil {
		for i, hint := range tableHints.indexHintList {
			if hint.dbName.L == dbName.L && hint.tblName.L == tblName.L {
				indexHints = append(indexHints, hint.indexHint)
				tableHints.indexHintList[i].matched = true
			}
		}
	}

	_, isolationReadEnginesHasTiKV := ctx.GetSessionVars().GetIsolationReadEngines()[kv.TiKV]
	for i, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		hasScanHint = true

		if !isolationReadEnginesHasTiKV {
			if hint.IndexNames != nil {
				engineVals, _ := ctx.GetSessionVars().GetSystemVar(variable.TiDBIsolationReadEngines)
				err := errors.New(fmt.Sprintf("TiDB doesn't support index in the isolation read engines(value: '%v')", engineVals))
				if i < indexHintsLen {
					return nil, err
				}
				ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			}
			continue
		}
		// It is syntactically valid to omit index_list for USE INDEX, which means “use no indexes”.
		// Omitting index_list for FORCE INDEX or IGNORE INDEX is a syntax error.
		// See https://dev.mysql.com/doc/refman/8.0/en/index-hints.html.
		if hint.IndexNames == nil && hint.HintType != ast.HintIgnore {
			if path := getTablePath(publicPaths); path != nil {
				hasUseOrForce = true
				path.Forced = true
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
				ctx.GetSessionVars().StmtCtx.AppendWarning(err)
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
			path.Forced = true
			available = append(available, path)
		}
	}

	if !hasScanHint || !hasUseOrForce {
		available = publicPaths
	}

	available = removeIgnoredPaths(available, ignored, tblInfo)
	if staleread.IsStmtStaleness(ctx) {
		// skip tiflash if the statement is for stale read until tiflash support stale read
		available = removeTiflashDuringStaleRead(available)
	}

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use table scan.
	if len(available) == 0 {
		available = append(available, tablePath)
	}
	return available, nil
}

func filterPathByIsolationRead(ctx sessionctx.Context, paths []*util.AccessPath, tblName model.CIStr, dbName model.CIStr) ([]*util.AccessPath, error) {
	// TODO: filter paths with isolation read locations.
	if dbName.L == mysql.SystemDB {
		return paths, nil
	}
	isolationReadEngines := ctx.GetSessionVars().GetIsolationReadEngines()
	availableEngine := map[kv.StoreType]struct{}{}
	var availableEngineStr string
	for i := len(paths) - 1; i >= 0; i-- {
		if _, ok := availableEngine[paths[i].StoreType]; !ok {
			availableEngine[paths[i].StoreType] = struct{}{}
			if availableEngineStr != "" {
				availableEngineStr += ", "
			}
			availableEngineStr += paths[i].StoreType.Name()
		}
		if _, ok := isolationReadEngines[paths[i].StoreType]; !ok && paths[i].StoreType != kv.TiDB {
			paths = append(paths[:i], paths[i+1:]...)
		}
	}
	var err error
	engineVals, _ := ctx.GetSessionVars().GetSystemVar(variable.TiDBIsolationReadEngines)
	if len(paths) == 0 {
		helpMsg := ""
		if engineVals == "tiflash" {
			helpMsg = ". Please check tiflash replica or ensure the query is readonly"
		}
		err = ErrInternal.GenWithStackByArgs(fmt.Sprintf("No access path for table '%s' is found with '%v' = '%v', valid values can be '%s'%s.", tblName.String(),
			variable.TiDBIsolationReadEngines, engineVals, availableEngineStr, helpMsg))
	}
	if _, ok := isolationReadEngines[kv.TiFlash]; !ok {
		ctx.GetSessionVars().RaiseWarningWhenMPPEnforced(
			fmt.Sprintf("MPP mode may be blocked because '%v'(value: '%v') not match, need 'tiflash'.", variable.TiDBIsolationReadEngines, engineVals))
	}
	return paths, err
}

func removeIgnoredPaths(paths, ignoredPaths []*util.AccessPath, tblInfo *model.TableInfo) []*util.AccessPath {
	if len(ignoredPaths) == 0 {
		return paths
	}
	remainedPaths := make([]*util.AccessPath, 0, len(paths))
	for _, path := range paths {
		if path.IsTablePath() || getPathByIndexName(ignoredPaths, path.Index.Name, tblInfo) == nil {
			remainedPaths = append(remainedPaths, path)
		}
	}
	return remainedPaths
}

func removeTiflashDuringStaleRead(paths []*util.AccessPath) []*util.AccessPath {
	n := 0
	for _, path := range paths {
		if path.StoreType != kv.TiFlash {
			paths[n] = path
			n++
		}
	}
	return paths[:n]
}

func (b *PlanBuilder) buildSelectLock(src LogicalPlan, lock *ast.SelectLockInfo) (*LogicalLock, error) {
	var tblID2PhysTblIDCol map[int64]*expression.Column
	if len(b.partitionedTable) > 0 {
		tblID2PhysTblIDCol = make(map[int64]*expression.Column)
		// If a chunk row is read from a partitioned table, which partition the row
		// comes from is unknown. With the existence of Join, the situation could be
		// even worse: SelectLock have to know the `pid` to construct the lock key.
		// To solve the problem, an extra `pid` column is added to the schema, and the
		// DataSource need to return the `pid` information in the chunk row.
		// For dynamic prune mode, it is filled in from the tableID in the key by storage.
		// For static prune mode it is also filled in from the tableID in the key by storage.
		// since it would otherwise be lost in the PartitionUnion executor.
		setExtraPhysTblIDColsOnDataSource(src, tblID2PhysTblIDCol)
	}
	selectLock := LogicalLock{
		Lock:               lock,
		tblID2Handle:       b.handleHelper.tailMap(),
		tblID2PhysTblIDCol: tblID2PhysTblIDCol,
	}.Init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock, nil
}

func setExtraPhysTblIDColsOnDataSource(p LogicalPlan, tblID2PhysTblIDCol map[int64]*expression.Column) {
	switch ds := p.(type) {
	case *DataSource:
		if ds.tableInfo.GetPartitionInfo() == nil {
			return
		}
		tblID2PhysTblIDCol[ds.tableInfo.ID] = ds.AddExtraPhysTblIDColumn()
	default:
		for _, child := range p.Children() {
			setExtraPhysTblIDColsOnDataSource(child, tblID2PhysTblIDCol)
		}
	}
}

func (b *PlanBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.SQLVar != nil {
		if v, ok := b.ctx.GetSessionVars().Users[strings.ToLower(x.SQLVar.Name)]; ok {
			var err error
			p.SQLText, err = v.ToString()
			if err != nil {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				p.SQLText = "NULL"
			}
		} else {
			p.SQLText = "NULL"
		}
	} else {
		p.SQLText = x.SQLText
	}
	return p
}

func (b *PlanBuilder) buildAdmin(ctx context.Context, as *ast.AdminStmt) (Plan, error) {
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckTable, ast.AdminCheckIndex:
		ret, err = b.buildAdminCheckTable(ctx, as)
		if err != nil {
			return ret, err
		}
	case ast.AdminRecoverIndex:
		p := &RecoverIndex{Table: as.Tables[0], IndexName: as.Index}
		p.setSchemaAndNames(buildRecoverIndexFields())
		ret = p
	case ast.AdminCleanupIndex:
		p := &CleanupIndex{Table: as.Tables[0], IndexName: as.Index}
		p.setSchemaAndNames(buildCleanupIndexFields())
		ret = p
	case ast.AdminChecksumTable:
		p := &ChecksumTable{Tables: as.Tables}
		p.setSchemaAndNames(buildChecksumTableSchema())
		ret = p
	case ast.AdminShowNextRowID:
		p := &ShowNextRowID{TableName: as.Tables[0]}
		p.setSchemaAndNames(buildShowNextRowID())
		ret = p
	case ast.AdminShowDDL:
		p := &ShowDDL{}
		p.setSchemaAndNames(buildShowDDLFields())
		ret = p
	case ast.AdminShowDDLJobs:
		p := LogicalShowDDLJobs{JobNumber: as.JobNumber}.Init(b.ctx)
		p.setSchemaAndNames(buildShowDDLJobsFields())
		for _, col := range p.schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		ret = p
		if as.Where != nil {
			ret, err = b.buildSelection(ctx, p, as.Where, nil)
			if err != nil {
				return nil, err
			}
		}
	case ast.AdminCancelDDLJobs:
		p := &CancelDDLJobs{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildCancelDDLJobsFields())
		ret = p
	case ast.AdminCheckIndexRange:
		schema, names, err := b.buildCheckIndexSchema(as.Tables[0], as.Index)
		if err != nil {
			return nil, err
		}

		p := &CheckIndexRange{Table: as.Tables[0], IndexName: as.Index, HandleRanges: as.HandleRanges}
		p.setSchemaAndNames(schema, names)
		ret = p
	case ast.AdminShowDDLJobQueries:
		p := &ShowDDLJobQueries{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildShowDDLJobQueriesFields())
		ret = p
	case ast.AdminShowSlow:
		p := &ShowSlow{ShowSlow: as.ShowSlow}
		p.setSchemaAndNames(buildShowSlowSchema())
		ret = p
	case ast.AdminReloadExprPushdownBlacklist:
		return &ReloadExprPushdownBlacklist{}, nil
	case ast.AdminReloadOptRuleBlacklist:
		return &ReloadOptRuleBlacklist{}, nil
	case ast.AdminPluginEnable:
		return &AdminPlugins{Action: Enable, Plugins: as.Plugins}, nil
	case ast.AdminPluginDisable:
		return &AdminPlugins{Action: Disable, Plugins: as.Plugins}, nil
	case ast.AdminFlushBindings:
		return &SQLBindPlan{SQLBindOp: OpFlushBindings}, nil
	case ast.AdminCaptureBindings:
		return &SQLBindPlan{SQLBindOp: OpCaptureBindings}, nil
	case ast.AdminEvolveBindings:
		var err error
		// The 'baseline evolution' only work in the test environment before the feature is GA.
		if !config.CheckTableBeforeDrop {
			err = errors.Errorf("Cannot enable baseline evolution feature, it is not generally available now")
		}
		return &SQLBindPlan{SQLBindOp: OpEvolveBindings}, err
	case ast.AdminReloadBindings:
		return &SQLBindPlan{SQLBindOp: OpReloadBindings}, nil
	case ast.AdminShowTelemetry:
		p := &AdminShowTelemetry{}
		p.setSchemaAndNames(buildShowTelemetrySchema())
		ret = p
	case ast.AdminResetTelemetryID:
		return &AdminResetTelemetryID{}, nil
	case ast.AdminReloadStatistics:
		return &Simple{Statement: as}, nil
	case ast.AdminFlushPlanCache:
		return &Simple{Statement: as}, nil
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return ret, nil
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReader(ctx context.Context, dbName model.CIStr, tbl table.Table, idx *model.IndexInfo) (Plan, error) {
	tblInfo := tbl.Meta()
	physicalID, isPartition := getPhysicalID(tbl)
	fullExprCols, _, err := expression.TableInfo2SchemaAndNames(b.ctx, dbName, tblInfo)
	if err != nil {
		return nil, err
	}
	extraInfo, extraCol, hasExtraCol := tryGetPkExtraColumn(b.ctx.GetSessionVars(), tblInfo)
	pkHandleInfo, pkHandleCol, hasPkIsHandle := tryGetPkHandleCol(tblInfo, fullExprCols)
	commonInfos, commonCols, hasCommonCols := tryGetCommonHandleCols(tbl, fullExprCols)
	idxColInfos := getIndexColumnInfos(tblInfo, idx)
	idxColSchema := getIndexColsSchema(tblInfo, idx, fullExprCols)
	idxCols, idxColLens := expression.IndexInfo2PrefixCols(idxColInfos, idxColSchema.Columns, idx)

	is := PhysicalIndexScan{
		Table:            tblInfo,
		TableAsName:      &tblInfo.Name,
		DBName:           dbName,
		Columns:          idxColInfos,
		Index:            idx,
		IdxCols:          idxCols,
		IdxColLens:       idxColLens,
		dataSourceSchema: idxColSchema.Clone(),
		Ranges:           ranger.FullRange(),
		physicalTableID:  physicalID,
		isPartition:      isPartition,
		tblColHists:      &(statistics.PseudoTable(tblInfo)).HistColl,
	}.Init(b.ctx, b.getSelectOffset())
	// There is no alternative plan choices, so just use pseudo stats to avoid panic.
	is.stats = &property.StatsInfo{HistColl: &(statistics.PseudoTable(tblInfo)).HistColl}
	if hasCommonCols {
		for _, c := range commonInfos {
			is.Columns = append(is.Columns, c.ColumnInfo)
		}
	}
	is.initSchema(append(is.IdxCols, commonCols...), true)

	// It's double read case.
	ts := PhysicalTableScan{
		Columns:         idxColInfos,
		Table:           tblInfo,
		TableAsName:     &tblInfo.Name,
		physicalTableID: physicalID,
		isPartition:     isPartition,
		tblColHists:     &(statistics.PseudoTable(tblInfo)).HistColl,
	}.Init(b.ctx, b.getSelectOffset())
	ts.SetSchema(idxColSchema)
	ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
	switch {
	case hasExtraCol:
		ts.Columns = append(ts.Columns, extraInfo)
		ts.schema.Append(extraCol)
		ts.HandleIdx = []int{len(ts.Columns) - 1}
	case hasPkIsHandle:
		ts.Columns = append(ts.Columns, pkHandleInfo)
		ts.schema.Append(pkHandleCol)
		ts.HandleIdx = []int{len(ts.Columns) - 1}
	case hasCommonCols:
		ts.HandleIdx = make([]int, 0, len(commonCols))
		for pkOffset, cInfo := range commonInfos {
			found := false
			for i, c := range ts.Columns {
				if c.ID == cInfo.ID {
					found = true
					ts.HandleIdx = append(ts.HandleIdx, i)
					break
				}
			}
			if !found {
				ts.Columns = append(ts.Columns, cInfo.ColumnInfo)
				ts.schema.Append(commonCols[pkOffset])
				ts.HandleIdx = append(ts.HandleIdx, len(ts.Columns)-1)
			}

		}
	}

	cop := &copTask{
		indexPlan:        is,
		tablePlan:        ts,
		tblColHists:      is.stats.HistColl,
		extraHandleCol:   extraCol,
		commonHandleCols: commonCols,
	}
	rootT := cop.convertToRootTask(b.ctx)
	if err := rootT.p.ResolveIndices(); err != nil {
		return nil, err
	}
	return rootT.p, nil
}

func getIndexColumnInfos(tblInfo *model.TableInfo, idx *model.IndexInfo) []*model.ColumnInfo {
	ret := make([]*model.ColumnInfo, len(idx.Columns))
	for i, idxCol := range idx.Columns {
		ret[i] = tblInfo.Columns[idxCol.Offset]
	}
	return ret
}

func getIndexColsSchema(tblInfo *model.TableInfo, idx *model.IndexInfo, allColSchema *expression.Schema) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(idx.Columns))...)
	for _, idxCol := range idx.Columns {
		for i, colInfo := range tblInfo.Columns {
			if colInfo.Name.L == idxCol.Name.L {
				schema.Append(allColSchema.Columns[i])
				break
			}
		}
	}
	return schema
}

func getPhysicalID(t table.Table) (physicalID int64, isPartition bool) {
	tblInfo := t.Meta()
	if tblInfo.GetPartitionInfo() != nil {
		pid := t.(table.PhysicalTable).GetPhysicalID()
		return pid, true
	}
	return tblInfo.ID, false
}

func tryGetPkExtraColumn(sv *variable.SessionVars, tblInfo *model.TableInfo) (*model.ColumnInfo, *expression.Column, bool) {
	if tblInfo.IsCommonHandle || tblInfo.PKIsHandle {
		return nil, nil, false
	}
	info := model.NewExtraHandleColInfo()
	expCol := &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sv.AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
	}
	return info, expCol, true
}

func tryGetCommonHandleCols(t table.Table, allColSchema *expression.Schema) ([]*table.Column, []*expression.Column, bool) {
	tblInfo := t.Meta()
	if !tblInfo.IsCommonHandle {
		return nil, nil, false
	}
	pk := tables.FindPrimaryIndex(tblInfo)
	commonHandleCols, _ := expression.IndexInfo2Cols(tblInfo.Columns, allColSchema.Columns, pk)
	commonHandelColInfos := tables.TryGetCommonPkColumns(t)
	return commonHandelColInfos, commonHandleCols, true
}

func tryGetPkHandleCol(tblInfo *model.TableInfo, allColSchema *expression.Schema) (*model.ColumnInfo, *expression.Column, bool) {
	if !tblInfo.PKIsHandle {
		return nil, nil, false
	}
	for i, c := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(c.GetFlag()) {
			return c, allColSchema.Columns[i], true
		}
	}
	return nil, nil, false
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReaders(ctx context.Context, dbName model.CIStr, tbl table.Table, indices []table.Index) ([]Plan, []*model.IndexInfo, error) {
	tblInfo := tbl.Meta()
	// get index information
	indexInfos := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	indexLookUpReaders := make([]Plan, 0, len(tblInfo.Indices))

	check := b.isForUpdateRead || b.ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && b.ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error

	for _, idx := range indices {
		idxInfo := idx.Meta()
		if tblInfo.IsCommonHandle && idxInfo.Primary {
			// Skip checking clustered index.
			continue
		}
		if idxInfo.State != model.StatePublic {
			logutil.Logger(ctx).Info("build physical index lookup reader, the index isn't public",
				zap.String("index", idxInfo.Name.O),
				zap.Stringer("state", idxInfo.State),
				zap.String("table", tblInfo.Name.O))
			continue
		}
		if check && latestIndexes == nil {
			latestIndexes, check, err = getLatestIndexInfo(b.ctx, tblInfo.ID, b.is.SchemaMetaVersion())
			if err != nil {
				return nil, nil, err
			}
		}
		if check {
			if latestIndex, ok := latestIndexes[idxInfo.ID]; !ok || latestIndex.State != model.StatePublic {
				forUpdateState := model.StateNone
				if ok {
					forUpdateState = latestIndex.State
				}
				logutil.Logger(ctx).Info("build physical index lookup reader, the index isn't public in forUpdateRead",
					zap.String("index", idxInfo.Name.O),
					zap.Stringer("state", idxInfo.State),
					zap.Stringer("forUpdateRead state", forUpdateState),
					zap.String("table", tblInfo.Name.O))
				continue
			}
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
	tblName := as.Tables[0]
	tableInfo := as.Tables[0].TableInfo
	tbl, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tblName.DBInfo.Name.O, tableInfo.Name.O)
	}
	p := &CheckTable{
		DBName: tblName.Schema.O,
		Table:  tbl,
	}
	var readerPlans []Plan
	var indexInfos []*model.IndexInfo
	var err error
	if as.Tp == ast.AdminCheckIndex {
		// get index information
		var idx table.Index
		idxName := strings.ToLower(as.Index)
		for _, index := range tbl.Indices() {
			if index.Meta().Name.L == idxName {
				idx = index
				break
			}
		}
		if idx == nil {
			return nil, errors.Errorf("index %s do not exist", as.Index)
		}
		if idx.Meta().State != model.StatePublic {
			return nil, errors.Errorf("index %s state %s isn't public", as.Index, idx.Meta().State)
		}
		p.CheckIndex = true
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, []table.Index{idx})
	} else {
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, tbl.Indices())
	}
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

func (b *PlanBuilder) buildCheckIndexSchema(tn *ast.TableName, indexName string) (*expression.Schema, types.NameSlice, error) {
	schema := expression.NewSchema()
	var names types.NameSlice
	indexName = strings.ToLower(indexName)
	indicesInfo := tn.TableInfo.Indices
	cols := tn.TableInfo.Cols()
	for _, idxInfo := range indicesInfo {
		if idxInfo.Name.L != indexName {
			continue
		}
		for _, idxCol := range idxInfo.Columns {
			col := cols[idxCol.Offset]
			names = append(names, &types.FieldName{
				ColName: idxCol.Name,
				TblName: tn.Name,
				DBName:  tn.Schema,
			})
			schema.Append(&expression.Column{
				RetType:  &col.FieldType,
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				ID:       col.ID})
		}
		names = append(names, &types.FieldName{
			ColName: model.NewCIStr("extra_handle"),
			TblName: tn.Name,
			DBName:  tn.Schema,
		})
		schema.Append(&expression.Column{
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       -1,
		})
	}
	if schema.Len() == 0 {
		return nil, nil, errors.Errorf("index %s not found", indexName)
	}
	return schema, names, nil
}

// getColsInfo returns the info of index columns, normal columns and primary key.
func getColsInfo(tn *ast.TableName) (indicesInfo []*model.IndexInfo, colsInfo []*model.ColumnInfo) {
	tbl := tn.TableInfo
	for _, col := range tbl.Columns {
		// The virtual column will not store any data in TiKV, so it should be ignored when collect statistics
		if col.IsGenerated() && !col.GeneratedStored {
			continue
		}
		if mysql.HasPriKeyFlag(col.GetFlag()) && tbl.HasClusteredIndex() {
			continue
		}
		colsInfo = append(colsInfo, col)
	}
	for _, idx := range tn.TableInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	return
}

// BuildHandleColsForAnalyze returns HandleCols for ANALYZE.
func BuildHandleColsForAnalyze(ctx sessionctx.Context, tblInfo *model.TableInfo, allColumns bool, colsInfo []*model.ColumnInfo) HandleCols {
	var handleCols HandleCols
	switch {
	case tblInfo.PKIsHandle:
		pkCol := tblInfo.GetPkColInfo()
		var index int
		if allColumns {
			// If all the columns need to be analyzed, we just set index to pkCol.Offset.
			index = pkCol.Offset
		} else {
			// If only a part of the columns need to be analyzed, we need to set index according to colsInfo.
			index = getColOffsetForAnalyze(colsInfo, pkCol.ID)
		}
		handleCols = &IntHandleCols{col: &expression.Column{
			ID:      pkCol.ID,
			RetType: &pkCol.FieldType,
			Index:   index,
		}}
	case tblInfo.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkColLen := len(pkIdx.Columns)
		columns := make([]*expression.Column, pkColLen)
		for i := 0; i < pkColLen; i++ {
			colInfo := tblInfo.Columns[pkIdx.Columns[i].Offset]
			var index int
			if allColumns {
				// If all the columns need to be analyzed, we just set index to colInfo.Offset.
				index = colInfo.Offset
			} else {
				// If only a part of the columns need to be analyzed, we need to set index according to colsInfo.
				index = getColOffsetForAnalyze(colsInfo, colInfo.ID)
			}
			columns[i] = &expression.Column{
				ID:      colInfo.ID,
				RetType: &colInfo.FieldType,
				Index:   index,
			}
		}
		// We don't modify IndexColumn.Offset for CommonHandleCols.idxInfo according to colsInfo. There are two reasons.
		// The first reason is that we use Column.Index of CommonHandleCols.columns, rather than IndexColumn.Offset, to get
		// column value from row samples when calling (*CommonHandleCols).BuildHandleByDatums in (*AnalyzeColumnsExec).buildSamplingStats.
		// The second reason is that in (cb *CommonHandleCols).BuildHandleByDatums, tablecodec.TruncateIndexValues(cb.tblInfo, cb.idxInfo, datumBuf)
		// is called, which asks that IndexColumn.Offset of cb.idxInfo must be according to cb,tblInfo.
		// TODO: find a better way to find handle columns in ANALYZE rather than use Column.Index
		handleCols = &CommonHandleCols{
			tblInfo: tblInfo,
			idxInfo: pkIdx,
			columns: columns,
			sc:      ctx.GetSessionVars().StmtCtx,
		}
	}
	return handleCols
}

// GetPhysicalIDsAndPartitionNames returns physical IDs and names of these partitions.
func GetPhysicalIDsAndPartitionNames(tblInfo *model.TableInfo, partitionNames []model.CIStr) ([]int64, []string, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if len(partitionNames) != 0 {
			return nil, nil, errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
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

type calcOnceMap struct {
	data       map[int64]struct{}
	calculated bool
}

// getMustAnalyzedColumns puts the columns whose statistics must be collected into `cols` if `cols` has not been calculated.
func (b *PlanBuilder) getMustAnalyzedColumns(tbl *ast.TableName, cols *calcOnceMap) (map[int64]struct{}, error) {
	if cols.calculated {
		return cols.data, nil
	}
	tblInfo := tbl.TableInfo
	cols.data = make(map[int64]struct{}, len(tblInfo.Columns))
	if len(tblInfo.Indices) > 0 {
		// Add indexed columns.
		// Some indexed columns are generated columns so we also need to add the columns that make up those generated columns.
		columns, _, err := expression.ColumnInfos2ColumnsAndNames(b.ctx, tbl.Schema, tbl.Name, tblInfo.Columns, tblInfo)
		if err != nil {
			return nil, err
		}
		virtualExprs := make([]expression.Expression, 0, len(tblInfo.Columns))
		for _, idx := range tblInfo.Indices {
			if idx.State != model.StatePublic {
				continue
			}
			for _, idxCol := range idx.Columns {
				colInfo := tblInfo.Columns[idxCol.Offset]
				cols.data[colInfo.ID] = struct{}{}
				if expr := columns[idxCol.Offset].VirtualExpr; expr != nil {
					virtualExprs = append(virtualExprs, expr)
				}
			}
		}
		relatedCols := make([]*expression.Column, 0, len(tblInfo.Columns))
		for len(virtualExprs) > 0 {
			relatedCols = expression.ExtractColumnsFromExpressions(relatedCols, virtualExprs, nil)
			virtualExprs = virtualExprs[:0]
			for _, col := range relatedCols {
				cols.data[col.ID] = struct{}{}
				if col.VirtualExpr != nil {
					virtualExprs = append(virtualExprs, col.VirtualExpr)
				}
			}
			relatedCols = relatedCols[:0]
		}
	}
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		cols.data[pkCol.ID] = struct{}{}
	}
	if b.ctx.GetSessionVars().EnableExtendedStats {
		// Add the columns related to extended stats.
		// TODO: column_ids read from mysql.stats_extended in optimization phase may be different from that in execution phase((*Handle).BuildExtendedStats)
		// if someone inserts data into mysql.stats_extended between the two time points, the new added extended stats may not be computed.
		statsHandle := domain.GetDomain(b.ctx).StatsHandle()
		extendedStatsColIDs, err := statsHandle.CollectColumnsInExtendedStats(tblInfo.ID)
		if err != nil {
			return nil, err
		}
		for _, colID := range extendedStatsColIDs {
			cols.data[colID] = struct{}{}
		}
	}
	cols.calculated = true
	return cols.data, nil
}

func (b *PlanBuilder) getPredicateColumns(tbl *ast.TableName, cols *calcOnceMap) (map[int64]struct{}, error) {
	if cols.calculated {
		return cols.data, nil
	}
	tblInfo := tbl.TableInfo
	cols.data = make(map[int64]struct{}, len(tblInfo.Columns))
	do := domain.GetDomain(b.ctx)
	h := do.StatsHandle()
	colList, err := h.GetPredicateColumns(tblInfo.ID)
	if err != nil {
		return nil, err
	}
	if len(colList) == 0 {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("No predicate column has been collected yet for table %s.%s so all columns are analyzed", tbl.Schema.L, tbl.Name.L))
		for _, colInfo := range tblInfo.Columns {
			cols.data[colInfo.ID] = struct{}{}
		}
	} else {
		for _, id := range colList {
			cols.data[id] = struct{}{}
		}
	}
	cols.calculated = true
	return cols.data, nil
}

func getAnalyzeColumnList(specifiedColumns []model.CIStr, tbl *ast.TableName) ([]*model.ColumnInfo, error) {
	colList := make([]*model.ColumnInfo, 0, len(specifiedColumns))
	for _, colName := range specifiedColumns {
		colInfo := model.FindColumnInfo(tbl.TableInfo.Columns, colName.L)
		if colInfo == nil {
			return nil, ErrAnalyzeMissColumn.GenWithStackByArgs(colName.O, tbl.TableInfo.Name.O)
		}
		colList = append(colList, colInfo)
	}
	return colList, nil
}

// getFullAnalyzeColumnsInfo decides which columns need to be analyzed.
// The first return value is the columns which need to be analyzed and the second return value is the columns which need to
// be record in mysql.analyze_options(only for the case of analyze table t columns c1, .., cn).
func (b *PlanBuilder) getFullAnalyzeColumnsInfo(
	tbl *ast.TableName,
	columnChoice model.ColumnChoice,
	specifiedCols []*model.ColumnInfo,
	predicateCols, mustAnalyzedCols *calcOnceMap,
	mustAllColumns bool,
	warning bool,
) ([]*model.ColumnInfo, []*model.ColumnInfo, error) {
	if mustAllColumns && warning && (columnChoice == model.PredicateColumns || columnChoice == model.ColumnList) {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("Table %s.%s has version 1 statistics so all the columns must be analyzed to overwrite the current statistics", tbl.Schema.L, tbl.Name.L))
	}
	colSet2colList := func(colSet map[int64]struct{}) []*model.ColumnInfo {
		colList := make([]*model.ColumnInfo, 0, len(colSet))
		for _, colInfo := range tbl.TableInfo.Columns {
			if _, ok := colSet[colInfo.ID]; ok {
				colList = append(colList, colInfo)
			}
		}
		return colList
	}
	switch columnChoice {
	case model.DefaultChoice, model.AllColumns:
		return tbl.TableInfo.Columns, nil, nil
	case model.PredicateColumns:
		if mustAllColumns {
			return tbl.TableInfo.Columns, nil, nil
		}
		predicate, err := b.getPredicateColumns(tbl, predicateCols)
		if err != nil {
			return nil, nil, err
		}
		mustAnalyzed, err := b.getMustAnalyzedColumns(tbl, mustAnalyzedCols)
		if err != nil {
			return nil, nil, err
		}
		colSet := make(map[int64]struct{}, len(predicate)+len(mustAnalyzed))
		for colID := range predicate {
			colSet[colID] = struct{}{}
		}
		for colID := range mustAnalyzed {
			colSet[colID] = struct{}{}
		}
		return colSet2colList(colSet), nil, nil
	case model.ColumnList:
		colSet := make(map[int64]struct{}, len(specifiedCols))
		for _, colInfo := range specifiedCols {
			colSet[colInfo.ID] = struct{}{}
		}
		mustAnalyzed, err := b.getMustAnalyzedColumns(tbl, mustAnalyzedCols)
		if err != nil {
			return nil, nil, err
		}
		if warning {
			missing := make(map[int64]struct{}, len(mustAnalyzed))
			for colID := range mustAnalyzed {
				if _, ok := colSet[colID]; !ok {
					missing[colID] = struct{}{}
				}
			}
			if len(missing) > 0 {
				missingNames := make([]string, 0, len(missing))
				for _, col := range tbl.TableInfo.Columns {
					if _, ok := missing[col.ID]; ok {
						missingNames = append(missingNames, col.Name.O)
					}
				}
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("Columns %s are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats", strings.Join(missingNames, ",")))
			}
		}
		for colID := range mustAnalyzed {
			colSet[colID] = struct{}{}
		}
		colList := colSet2colList(colSet)
		if mustAllColumns {
			return tbl.TableInfo.Columns, colList, nil
		}
		return colList, colList, nil
	}
	return nil, nil, nil
}

func getColOffsetForAnalyze(colsInfo []*model.ColumnInfo, colID int64) int {
	for i, col := range colsInfo {
		if colID == col.ID {
			return i
		}
	}
	return -1
}

// getModifiedIndexesInfoForAnalyze returns indexesInfo for ANALYZE.
// 1. If allColumns is true, we just return public indexes in tblInfo.Indices.
// 2. If allColumns is false, colsInfo indicate the columns whose stats need to be collected. colsInfo is a subset of tbl.Columns. For each public index
// in tblInfo.Indices, index.Columns[i].Offset is set according to tblInfo.Columns. Since we decode row samples according to colsInfo rather than tbl.Columns
// in the execution phase of ANALYZE, we need to modify index.Columns[i].Offset according to colInfos.
// TODO: find a better way to find indexed columns in ANALYZE rather than use IndexColumn.Offset
func getModifiedIndexesInfoForAnalyze(tblInfo *model.TableInfo, allColumns bool, colsInfo []*model.ColumnInfo) []*model.IndexInfo {
	idxsInfo := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, originIdx := range tblInfo.Indices {
		if originIdx.State != model.StatePublic {
			continue
		}
		if allColumns {
			// If all the columns need to be analyzed, we don't need to modify IndexColumn.Offset.
			idxsInfo = append(idxsInfo, originIdx)
			continue
		}
		// If only a part of the columns need to be analyzed, we need to set IndexColumn.Offset according to colsInfo.
		idx := originIdx.Clone()
		for i, idxCol := range idx.Columns {
			colID := tblInfo.Columns[idxCol.Offset].ID
			idx.Columns[i].Offset = getColOffsetForAnalyze(colsInfo, colID)
		}
		idxsInfo = append(idxsInfo, idx)
	}
	return idxsInfo
}

func (b *PlanBuilder) buildAnalyzeFullSamplingTask(
	as *ast.AnalyzeTableStmt,
	taskSlice []AnalyzeColumnsTask,
	physicalIDs []int64,
	names []string,
	tbl *ast.TableName,
	version int,
	persistOpts bool,
	rsOptionsMap map[int64]V2AnalyzeOptions,
) ([]AnalyzeColumnsTask, error) {
	if as.Incremental {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The version 2 stats would ignore the INCREMENTAL keyword and do full sampling"))
	}
	astOpts, err := parseAnalyzeOptionsV2(as.AnalyzeOpts)
	if err != nil {
		return nil, err
	}
	astColList, err := getAnalyzeColumnList(as.ColumnNames, tbl)
	if err != nil {
		return nil, err
	}
	var predicateCols, mustAnalyzedCols calcOnceMap
	ver := version
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	// If the statistics of the table is version 1, we must analyze all columns to overwrites all of old statistics.
	mustAllColumns := !statsHandle.CheckAnalyzeVersion(tbl.TableInfo, physicalIDs, &ver)
	astColsInfo, _, err := b.getFullAnalyzeColumnsInfo(tbl, as.ColumnChoice, astColList, &predicateCols, &mustAnalyzedCols, mustAllColumns, true)
	if err != nil {
		return nil, err
	}
	isAnalyzeTable := len(as.PartitionNames) == 0
	optionsMap, colsInfoMap, err := b.genV2AnalyzeOptions(persistOpts, tbl, isAnalyzeTable, physicalIDs, astOpts, as.ColumnChoice, astColList, &predicateCols, &mustAnalyzedCols, mustAllColumns)
	if err != nil {
		return nil, err
	}
	for physicalID, opts := range optionsMap {
		rsOptionsMap[physicalID] = opts
	}
	for i, id := range physicalIDs {
		physicalID := id
		if id == tbl.TableInfo.ID {
			id = -1
		}
		info := AnalyzeInfo{
			DBName:        tbl.Schema.O,
			TableName:     tbl.Name.O,
			PartitionName: names[i],
			TableID:       statistics.AnalyzeTableID{TableID: tbl.TableInfo.ID, PartitionID: id},
			Incremental:   false,
			StatsVersion:  version,
		}
		if optsV2, ok := optionsMap[physicalID]; ok {
			info.V2Options = &optsV2
		}
		execColsInfo := astColsInfo
		if colsInfo, ok := colsInfoMap[physicalID]; ok {
			execColsInfo = colsInfo
		}
		allColumns := len(tbl.TableInfo.Columns) == len(execColsInfo)
		indexes := getModifiedIndexesInfoForAnalyze(tbl.TableInfo, allColumns, execColsInfo)
		handleCols := BuildHandleColsForAnalyze(b.ctx, tbl.TableInfo, allColumns, execColsInfo)
		newTask := AnalyzeColumnsTask{
			HandleCols:  handleCols,
			ColsInfo:    execColsInfo,
			AnalyzeInfo: info,
			TblInfo:     tbl.TableInfo,
			Indexes:     indexes,
		}
		if newTask.HandleCols == nil {
			extraCol := model.NewExtraHandleColInfo()
			// Always place _tidb_rowid at the end of colsInfo, this is corresponding to logics in `analyzeColumnsPushdown`.
			newTask.ColsInfo = append(newTask.ColsInfo, extraCol)
			newTask.HandleCols = &IntHandleCols{col: colInfoToColumn(extraCol, len(newTask.ColsInfo)-1)}
		}
		taskSlice = append(taskSlice, newTask)
	}
	return taskSlice, nil
}

func (b *PlanBuilder) genV2AnalyzeOptions(
	persist bool,
	tbl *ast.TableName,
	isAnalyzeTable bool,
	physicalIDs []int64,
	astOpts map[ast.AnalyzeOptionType]uint64,
	astColChoice model.ColumnChoice,
	astColList []*model.ColumnInfo,
	predicateCols, mustAnalyzedCols *calcOnceMap,
	mustAllColumns bool,
) (map[int64]V2AnalyzeOptions, map[int64][]*model.ColumnInfo, error) {
	optionsMap := make(map[int64]V2AnalyzeOptions, len(physicalIDs))
	colsInfoMap := make(map[int64][]*model.ColumnInfo, len(physicalIDs))
	if !persist {
		return optionsMap, colsInfoMap, nil
	}
	dynamicPrune := variable.PartitionPruneMode(b.ctx.GetSessionVars().PartitionPruneMode.Load()) == variable.Dynamic
	if !isAnalyzeTable && dynamicPrune && (len(astOpts) > 0 || astColChoice != model.DefaultChoice) {
		astOpts = make(map[ast.AnalyzeOptionType]uint64, 0)
		astColChoice = model.DefaultChoice
		astColList = make([]*model.ColumnInfo, 0)
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("Ignore columns and options when analyze partition in dynamic mode"))
	}
	tblSavedOpts, tblSavedColChoice, tblSavedColList, err := b.getSavedAnalyzeOpts(tbl.TableInfo.ID, tbl.TableInfo)
	if err != nil {
		return nil, nil, err
	}
	tblOpts := tblSavedOpts
	tblColChoice := tblSavedColChoice
	tblColList := tblSavedColList
	if isAnalyzeTable {
		tblOpts = mergeAnalyzeOptions(astOpts, tblSavedOpts)
		tblColChoice, tblColList = mergeColumnList(astColChoice, astColList, tblSavedColChoice, tblSavedColList)
	}
	tblFilledOpts := fillAnalyzeOptionsV2(tblOpts)
	tblColsInfo, tblColList, err := b.getFullAnalyzeColumnsInfo(tbl, tblColChoice, tblColList, predicateCols, mustAnalyzedCols, mustAllColumns, false)
	if err != nil {
		return nil, nil, err
	}
	tblAnalyzeOptions := V2AnalyzeOptions{
		PhyTableID:  tbl.TableInfo.ID,
		RawOpts:     tblOpts,
		FilledOpts:  tblFilledOpts,
		ColChoice:   tblColChoice,
		ColumnList:  tblColList,
		IsPartition: false,
	}
	optionsMap[tbl.TableInfo.ID] = tblAnalyzeOptions
	colsInfoMap[tbl.TableInfo.ID] = tblColsInfo
	for _, id := range physicalIDs {
		if id != tbl.TableInfo.ID {
			if dynamicPrune {
				parV2Options := V2AnalyzeOptions{
					PhyTableID:  id,
					RawOpts:     tblOpts,
					FilledOpts:  tblFilledOpts,
					ColChoice:   tblColChoice,
					ColumnList:  tblColList,
					IsPartition: true,
				}
				optionsMap[id] = parV2Options
				colsInfoMap[id] = tblColsInfo
				continue
			}
			parSavedOpts, parSavedColChoice, parSavedColList, err := b.getSavedAnalyzeOpts(id, tbl.TableInfo)
			if err != nil {
				return nil, nil, err
			}
			// merge partition level options with table level options firstly
			savedOpts := mergeAnalyzeOptions(parSavedOpts, tblSavedOpts)
			savedColChoice, savedColList := mergeColumnList(parSavedColChoice, parSavedColList, tblSavedColChoice, tblSavedColList)
			// then merge statement level options
			mergedOpts := mergeAnalyzeOptions(astOpts, savedOpts)
			filledMergedOpts := fillAnalyzeOptionsV2(mergedOpts)
			finalColChoice, mergedColList := mergeColumnList(astColChoice, astColList, savedColChoice, savedColList)
			finalColsInfo, finalColList, err := b.getFullAnalyzeColumnsInfo(tbl, finalColChoice, mergedColList, predicateCols, mustAnalyzedCols, mustAllColumns, false)
			if err != nil {
				return nil, nil, err
			}
			parV2Options := V2AnalyzeOptions{
				PhyTableID: id,
				RawOpts:    mergedOpts,
				FilledOpts: filledMergedOpts,
				ColChoice:  finalColChoice,
				ColumnList: finalColList,
			}
			optionsMap[id] = parV2Options
			colsInfoMap[id] = finalColsInfo
		}
	}
	return optionsMap, colsInfoMap, nil
}

func (b *PlanBuilder) getSavedAnalyzeOpts(physicalID int64, tblInfo *model.TableInfo) (map[ast.AnalyzeOptionType]uint64, model.ColumnChoice, []*model.ColumnInfo, error) {
	analyzeOptions := map[ast.AnalyzeOptionType]uint64{}
	exec := b.ctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, "select sample_num,sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id = %?", physicalID)
	if err != nil {
		return nil, model.DefaultChoice, nil, err
	}
	if len(rows) <= 0 {
		return analyzeOptions, model.DefaultChoice, nil, nil
	}
	row := rows[0]
	sampleNum := row.GetInt64(0)
	if sampleNum > 0 {
		analyzeOptions[ast.AnalyzeOptNumSamples] = uint64(sampleNum)
	}
	sampleRate := row.GetFloat64(1)
	if sampleRate > 0 {
		analyzeOptions[ast.AnalyzeOptSampleRate] = math.Float64bits(sampleRate)
	}
	buckets := row.GetInt64(2)
	if buckets > 0 {
		analyzeOptions[ast.AnalyzeOptNumBuckets] = uint64(buckets)
	}
	topn := row.GetInt64(3)
	if topn >= 0 {
		analyzeOptions[ast.AnalyzeOptNumTopN] = uint64(topn)
	}
	colType := row.GetEnum(4)
	switch colType.Name {
	case "ALL":
		return analyzeOptions, model.AllColumns, tblInfo.Columns, nil
	case "LIST":
		colIDStrs := strings.Split(row.GetString(5), ",")
		colList := make([]*model.ColumnInfo, 0, len(colIDStrs))
		for _, colIDStr := range colIDStrs {
			colID, _ := strconv.ParseInt(colIDStr, 10, 64)
			colInfo := model.FindColumnInfoByID(tblInfo.Columns, colID)
			if colInfo != nil {
				colList = append(colList, colInfo)
			}
		}
		return analyzeOptions, model.ColumnList, colList, nil
	case "PREDICATE":
		return analyzeOptions, model.PredicateColumns, nil, nil
	default:
		return analyzeOptions, model.DefaultChoice, nil, nil
	}
}

func mergeAnalyzeOptions(stmtOpts map[ast.AnalyzeOptionType]uint64, savedOpts map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	merged := map[ast.AnalyzeOptionType]uint64{}
	for optType := range ast.AnalyzeOptionString {
		if stmtOpt, ok := stmtOpts[optType]; ok {
			merged[optType] = stmtOpt
		} else if savedOpt, ok := savedOpts[optType]; ok {
			merged[optType] = savedOpt
		}
	}
	return merged
}

func mergeColumnList(choice1 model.ColumnChoice, list1 []*model.ColumnInfo, choice2 model.ColumnChoice, list2 []*model.ColumnInfo) (model.ColumnChoice, []*model.ColumnInfo) {
	if choice1 != model.DefaultChoice {
		return choice1, list1
	}
	return choice2, list2
}

func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (Plan, error) {
	p := &Analyze{Opts: opts}
	p.OptionsMap = make(map[int64]V2AnalyzeOptions)
	usePersistedOptions := variable.PersistAnalyzeOptions.Load()
	for _, tbl := range as.TableNames {
		if tbl.TableInfo.IsView() {
			return nil, errors.Errorf("analyze view %s is not supported now", tbl.Name.O)
		}
		if tbl.TableInfo.IsSequence() {
			return nil, errors.Errorf("analyze sequence %s is not supported now", tbl.Name.O)
		}
		idxInfo, colInfo := getColsInfo(tbl)
		physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tbl.TableInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		var commonHandleInfo *model.IndexInfo
		// If we want to analyze this table with analyze version 2 but the existing stats is version 1 and stats feedback is enabled,
		// we will switch back to analyze version 1.
		if statistics.FeedbackProbability.Load() > 0 && version == 2 {
			statsHandle := domain.GetDomain(b.ctx).StatsHandle()
			versionIsSame := statsHandle.CheckAnalyzeVersion(tbl.TableInfo, physicalIDs, &version)
			if !versionIsSame {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Use analyze version 1 on table `%s` ", tbl.Name) +
					"because this table already has version 1 statistics and query feedback is also enabled. " +
					"If you want to switch to version 2 statistics, please first disable query feedback by setting feedback-probability to 0.0 in the config file."))
			}
		}
		if version == statistics.Version2 {
			p.ColTasks, err = b.buildAnalyzeFullSamplingTask(as, p.ColTasks, physicalIDs, names, tbl, version, usePersistedOptions, p.OptionsMap)
			if err != nil {
				return nil, err
			}
			continue
		}
		if as.ColumnChoice == model.PredicateColumns {
			return nil, errors.Errorf("Only the analyze version 2 supports analyzing predicate columns")
		}
		if as.ColumnChoice == model.ColumnList {
			return nil, errors.Errorf("Only the analyze version 2 supports analyzing the specified columns")
		}
		for _, idx := range idxInfo {
			// For prefix common handle. We don't use analyze mixed to handle it with columns. Because the full value
			// is read by coprocessor, the prefix index would get wrong stats in this case.
			if idx.Primary && tbl.TableInfo.IsCommonHandle && !idx.HasPrefixIndex() {
				commonHandleInfo = idx
				continue
			}
			for i, id := range physicalIDs {
				if id == tbl.TableInfo.ID {
					id = -1
				}
				info := AnalyzeInfo{
					DBName:        tbl.Schema.O,
					TableName:     tbl.Name.O,
					PartitionName: names[i],
					TableID:       statistics.AnalyzeTableID{TableID: tbl.TableInfo.ID, PartitionID: id},
					Incremental:   as.Incremental,
					StatsVersion:  version,
				}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{
					IndexInfo:   idx,
					AnalyzeInfo: info,
					TblInfo:     tbl.TableInfo,
				})
			}
		}
		handleCols := BuildHandleColsForAnalyze(b.ctx, tbl.TableInfo, true, nil)
		if len(colInfo) > 0 || handleCols != nil {
			for i, id := range physicalIDs {
				if id == tbl.TableInfo.ID {
					id = -1
				}
				info := AnalyzeInfo{
					DBName:        tbl.Schema.O,
					TableName:     tbl.Name.O,
					PartitionName: names[i],
					TableID:       statistics.AnalyzeTableID{TableID: tbl.TableInfo.ID, PartitionID: id},
					Incremental:   as.Incremental,
					StatsVersion:  version,
				}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
					HandleCols:       handleCols,
					CommonHandleInfo: commonHandleInfo,
					ColsInfo:         colInfo,
					AnalyzeInfo:      info,
					TblInfo:          tbl.TableInfo,
				})
			}
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		if b.ctx.GetSessionVars().EnableFastAnalyze {
			return nil, errors.Errorf("Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1")
		}
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The version 2 would collect all statistics not only the selected indexes"))
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) {
			handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
			// Fast analyze use analyze column to solve int handle.
			if handleCols != nil && handleCols.IsInt() && b.ctx.GetSessionVars().EnableFastAnalyze {
				for i, id := range physicalIDs {
					if id == tblInfo.ID {
						id = -1
					}
					info := AnalyzeInfo{
						DBName:        as.TableNames[0].Schema.O,
						TableName:     as.TableNames[0].Name.O,
						PartitionName: names[i], TableID: statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
						Incremental:  as.Incremental,
						StatsVersion: version,
					}
					p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
				}
				continue
			}
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		for i, id := range physicalIDs {
			if id == tblInfo.ID {
				id = -1
			}
			info := AnalyzeInfo{
				DBName:        as.TableNames[0].Schema.O,
				TableName:     as.TableNames[0].Name.O,
				PartitionName: names[i],
				TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
				Incremental:   as.Incremental,
				StatsVersion:  version,
			}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.TableNames[0].TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		if b.ctx.GetSessionVars().EnableFastAnalyze {
			return nil, errors.Errorf("Fast analyze hasn't reached General Availability and only support analyze version 1 currently. But the existing statistics of the table is not version 1")
		}
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The version 2 would collect all statistics not only the selected indexes"))
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			for i, id := range physicalIDs {
				if id == tblInfo.ID {
					id = -1
				}
				info := AnalyzeInfo{
					DBName:        as.TableNames[0].Schema.O,
					TableName:     as.TableNames[0].Name.O,
					PartitionName: names[i],
					TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
					Incremental:   as.Incremental,
					StatsVersion:  version,
				}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo})
			}
		}
	}
	handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
	if handleCols != nil {
		for i, id := range physicalIDs {
			if id == tblInfo.ID {
				id = -1
			}
			info := AnalyzeInfo{
				DBName:        as.TableNames[0].Schema.O,
				TableName:     as.TableNames[0].Name.O,
				PartitionName: names[i],
				TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
				Incremental:   as.Incremental,
				StatsVersion:  version,
			}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

// CMSketchSizeLimit indicates the size limit of CMSketch.
var CMSketchSizeLimit = kv.TxnEntrySizeLimit / binary.MaxVarintLen32

var analyzeOptionLimit = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    1024,
	ast.AnalyzeOptNumTopN:       1024,
	ast.AnalyzeOptCMSketchWidth: CMSketchSizeLimit,
	ast.AnalyzeOptCMSketchDepth: CMSketchSizeLimit,
	ast.AnalyzeOptNumSamples:    500000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(1),
}

var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       20,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    10000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(0),
}

var analyzeOptionDefaultV2 = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       500,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    0,
	ast.AnalyzeOptSampleRate:    math.Float64bits(-1),
}

func parseAnalyzeOptionsV2(opts []ast.AnalyzeOpt) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(nil)
			if err != nil {
				return nil, err
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}
	return optMap, nil
}

func fillAnalyzeOptionsV2(optMap map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	filledMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, defaultVal := range analyzeOptionDefaultV2 {
		if val, ok := optMap[key]; ok {
			filledMap[key] = val
		} else {
			filledMap[key] = defaultVal
		}
	}
	return filledMap
}

func handleAnalyzeOptions(opts []ast.AnalyzeOpt, statsVer int) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	if statsVer == statistics.Version1 {
		for key, val := range analyzeOptionDefault {
			optMap[key] = val
		}
	} else {
		for key, val := range analyzeOptionDefaultV2 {
			optMap[key] = val
		}
	}
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(nil)
			if err != nil {
				return nil, err
			}
			if fVal > 0 && statsVer == statistics.Version1 {
				return nil, errors.Errorf("Version 1's statistics doesn't support the SAMPLERATE option, please set tidb_analyze_version to 2")
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}
	if optMap[ast.AnalyzeOptCMSketchWidth]*optMap[ast.AnalyzeOptCMSketchDepth] > CMSketchSizeLimit {
		return nil, errors.Errorf("cm sketch size(depth * width) should not larger than %d", CMSketchSizeLimit)
	}
	return optMap, nil
}

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeTableStmt) (Plan, error) {
	// If enable fast analyze, the storage must be tikv.Storage.
	if _, isTikvStorage := b.ctx.GetStore().(tikv.Storage); !isTikvStorage && b.ctx.GetSessionVars().EnableFastAnalyze {
		return nil, errors.Errorf("Only support fast analyze in tikv storage")
	}
	statsVersion := b.ctx.GetSessionVars().AnalyzeVersion
	if b.ctx.GetSessionVars().EnableFastAnalyze && statsVersion >= statistics.Version2 {
		return nil, errors.Errorf("Fast analyze hasn't reached General Availability and only support analyze version 1 currently")
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
	opts, err := handleAnalyzeOptions(as.AnalyzeOpts, statsVersion)
	if err != nil {
		return nil, err
	}
	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as, opts, statsVersion)
		}
		return b.buildAnalyzeIndex(as, opts, statsVersion)
	}
	return b.buildAnalyzeTable(as, opts, statsVersion)
}

func buildShowNextRowID() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(4)
	schema.Append(buildColumnWithName("", "DB_NAME", mysql.TypeVarchar, mysql.MaxDatabaseNameLength))
	schema.Append(buildColumnWithName("", "TABLE_NAME", mysql.TypeVarchar, mysql.MaxTableNameLength))
	schema.Append(buildColumnWithName("", "COLUMN_NAME", mysql.TypeVarchar, mysql.MaxColumnNameLength))
	schema.Append(buildColumnWithName("", "NEXT_GLOBAL_ROW_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "ID_TYPE", mysql.TypeVarchar, 15))
	return schema.col2Schema(), schema.names
}

func buildShowDDLFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(6)
	schema.Append(buildColumnWithName("", "SCHEMA_VER", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "OWNER_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "OWNER_ADDRESS", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "RUNNING_JOBS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "SELF_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "QUERY", mysql.TypeVarchar, 256))

	return schema.col2Schema(), schema.names
}

func buildRecoverIndexFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "ADDED_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "SCAN_COUNT", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildCleanupIndexFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "REMOVED_COUNT", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildShowDDLJobsFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(12)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "DB_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "TABLE_NAME", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "JOB_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCHEMA_STATE", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCHEMA_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "TABLE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "ROW_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "CREATE_TIME", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName("", "START_TIME", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName("", "END_TIME", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName("", "STATE", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

func buildTableRegionsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName("", "REGION_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "START_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "END_KEY", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "LEADER_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "LEADER_STORE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "PEERS", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "SCATTERING", mysql.TypeTiny, 1))
	schema.Append(buildColumnWithName("", "WRITTEN_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "READ_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "APPROXIMATE_SIZE(MB)", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "APPROXIMATE_KEYS", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildSplitRegionsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "TOTAL_SPLIT_REGION", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "SCATTER_FINISH_RATIO", mysql.TypeDouble, 8))
	return schema.col2Schema(), schema.names
}

func buildShowDDLJobQueriesFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "QUERY", mysql.TypeVarchar, 256))
	return schema.col2Schema(), schema.names
}

func buildShowSlowSchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	tinySize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	timestampSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTimestamp)
	durationSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDuration)

	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName("", "SQL", mysql.TypeVarchar, 4096))
	schema.Append(buildColumnWithName("", "START", mysql.TypeTimestamp, timestampSize))
	schema.Append(buildColumnWithName("", "DURATION", mysql.TypeDuration, durationSize))
	schema.Append(buildColumnWithName("", "DETAILS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "SUCC", mysql.TypeTiny, tinySize))
	schema.Append(buildColumnWithName("", "CONN_ID", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumnWithName("", "TRANSACTION_TS", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumnWithName("", "USER", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "DB", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "TABLE_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "INDEX_IDS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "INTERNAL", mysql.TypeTiny, tinySize))
	schema.Append(buildColumnWithName("", "DIGEST", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

func buildCancelDDLJobsFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "RESULT", mysql.TypeVarchar, 128))

	return schema.col2Schema(), schema.names
}

func buildBRIESchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	datetimeSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDatetime)

	schema := newColumnsWithNames(5)
	schema.Append(buildColumnWithName("", "Destination", mysql.TypeVarchar, 255))
	schema.Append(buildColumnWithName("", "Size", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumnWithName("", "BackupTS", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumnWithName("", "Queue Time", mysql.TypeDatetime, datetimeSize))
	schema.Append(buildColumnWithName("", "Execution Time", mysql.TypeDatetime, datetimeSize))
	return schema.col2Schema(), schema.names
}

func buildShowTelemetrySchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "TRACKING_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "LAST_STATUS", mysql.TypeString, mysql.MaxBlobWidth))
	schema.Append(buildColumnWithName("", "DATA_PREVIEW", mysql.TypeString, mysql.MaxBlobWidth))
	return schema.col2Schema(), schema.names
}

func buildColumnWithName(tableName, name string, tp byte, size int) (*expression.Column, *types.FieldName) {
	cs, cl := types.DefaultCharsetForType(tp)
	flag := mysql.UnsignedFlag
	if tp == mysql.TypeVarchar || tp == mysql.TypeBlob {
		cs = charset.CharsetUTF8MB4
		cl = charset.CollationUTF8MB4
		flag = 0
	}

	fieldType := &types.FieldType{}
	fieldType.SetType(tp)
	fieldType.SetCharset(cs)
	fieldType.SetCollate(cl)
	fieldType.SetFlen(size)
	fieldType.SetFlag(flag)
	return &expression.Column{
		RetType: fieldType,
	}, &types.FieldName{DBName: util2.InformationSchemaName, TblName: model.NewCIStr(tableName), ColName: model.NewCIStr(name)}
}

type columnsWithNames struct {
	cols  []*expression.Column
	names types.NameSlice
}

func newColumnsWithNames(c int) *columnsWithNames {
	return &columnsWithNames{
		cols:  make([]*expression.Column, 0, 2),
		names: make(types.NameSlice, 0, 2),
	}
}

func (cwn *columnsWithNames) Append(col *expression.Column, name *types.FieldName) {
	cwn.cols = append(cwn.cols, col)
	cwn.names = append(cwn.names, name)
}

func (cwn *columnsWithNames) col2Schema() *expression.Schema {
	return expression.NewSchema(cwn.cols...)
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
	p := LogicalShow{
		ShowContents: ShowContents{
			Tp:          show.Tp,
			DBName:      show.DBName,
			Table:       show.Table,
			Partition:   show.Partition,
			Column:      show.Column,
			IndexName:   show.IndexName,
			Flag:        show.Flag,
			User:        show.User,
			Roles:       show.Roles,
			Full:        show.Full,
			IfNotExists: show.IfNotExists,
			GlobalScope: show.GlobalScope,
			Extended:    show.Extended,
		},
	}.Init(b.ctx)
	isView := false
	isSequence := false

	switch show.Tp {
	case ast.ShowColumns:
		var extractor ShowColumnsTableExtractor
		if extractor.Extract(show) {
			p.Extractor = &extractor
			// avoid to build Selection.
			show.Pattern = nil
		}
	case ast.ShowTables:
		if p.DBName == "" {
			return nil, ErrNoDB
		}
		var extractor ShowTablesTableExtractor
		if extractor.Extract(show) {
			p.Extractor = &extractor
			// Avoid building Selection.
			show.Pattern = nil
		}
	case ast.ShowTableStatus:
		if p.DBName == "" {
			return nil, ErrNoDB
		}
	case ast.ShowCreateTable, ast.ShowCreateSequence, ast.ShowPlacementForTable, ast.ShowPlacementForPartition:
		var err error
		if table, err := b.is.TableByName(show.Table.Schema, show.Table.Name); err == nil {
			isView = table.Meta().IsView()
			isSequence = table.Meta().IsSequence()
		}
		user := b.ctx.GetSessionVars().User
		if isView {
			if user != nil {
				err = ErrTableaccessDenied.GenWithStackByArgs("SHOW VIEW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		} else {
			if user != nil {
				err = ErrTableaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AllPrivMask, show.Table.Schema.L, show.Table.Name.L, "", err)
		}
	case ast.ShowConfig:
		privErr := ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ConfigPriv, "", "", "", privErr)
	case ast.ShowCreateView:
		var err error
		user := b.ctx.GetSessionVars().User
		if user != nil {
			err = ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
		if user != nil {
			err = ErrTableaccessDenied.GenWithStackByArgs("SHOW VIEW", user.AuthUsername, user.AuthHostname, show.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShowViewPriv, show.Table.Schema.L, show.Table.Name.L, "", err)
	case ast.ShowBackups:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or BACKUP_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "BACKUP_ADMIN", false, err)
	case ast.ShowRestores:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESTORE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "RESTORE_ADMIN", false, err)
	case ast.ShowTableNextRowId:
		p := &ShowNextRowID{TableName: show.Table}
		p.setSchemaAndNames(buildShowNextRowID())
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, show.Table.Schema.L, show.Table.Name.L, "", ErrPrivilegeCheckFail)
		return p, nil
	case ast.ShowStatsBuckets, ast.ShowStatsHistograms, ast.ShowStatsMeta, ast.ShowStatsExtended, ast.ShowStatsHealthy, ast.ShowStatsTopN, ast.ShowHistogramsInFlight, ast.ShowColumnStatsUsage:
		user := b.ctx.GetSessionVars().User
		var err error
		if user != nil {
			err = ErrDBaccessDenied.GenWithStackByArgs(user.AuthUsername, user.AuthHostname, mysql.SystemDB)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, mysql.SystemDB, "", "", err)
	case ast.ShowRegions:
		tableInfo, err := b.is.TableByName(show.Table.Schema, show.Table.Name)
		if err != nil {
			return nil, err
		}
		if tableInfo.Meta().TempTableType != model.TempTableNone {
			return nil, ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions")
		}
	}
	if show.Tp == ast.ShowVariables {
		var extractor ShowVariablesExtractor
		if extractor.Extract(show) {
			p.Extractor = &extractor
			// Avoid building Selection.
			show.Pattern = nil
		}
	}
	schema, names := buildShowSchema(show, isView, isSequence)
	p.SetSchema(schema)
	p.names = names
	for _, col := range p.schema.Columns {
		col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
	}
	var err error
	var np LogicalPlan
	np = p
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.OutputNames()[0].ColName},
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
	if np != p {
		b.optFlag |= flagEliminateProjection
		fieldsLen := len(p.schema.Columns)
		proj := LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx, 0)
		schema := expression.NewSchema(make([]*expression.Column, 0, fieldsLen)...)
		for _, col := range p.schema.Columns {
			proj.Exprs = append(proj.Exprs, col)
			newCol := col.Clone().(*expression.Column)
			newCol.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
			schema.Append(newCol)
		}
		proj.SetSchema(schema)
		proj.SetChildren(np)
		proj.SetOutputNames(np.OutputNames())
		np = proj
	}
	if show.Tp == ast.ShowVariables || show.Tp == ast.ShowStatus {
		b.curClause = orderByClause
		orderByCol := np.Schema().Columns[0].Clone().(*expression.Column)
		sort := LogicalSort{
			ByItems: []*util.ByItems{{Expr: orderByCol}},
		}.Init(b.ctx, b.getSelectOffset())
		sort.SetChildren(np)
		np = sort
	}
	return np, nil
}

func (b *PlanBuilder) buildSimple(ctx context.Context, node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.FlushStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("RELOAD")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ReloadPriv, "", "", "", err)
	case *ast.AlterInstanceStmt:
		err := ErrSpecificAccessDenied.GenWithStack("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
	case *ast.RenameUserStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		var err error
		b.visitInfo, err = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
		if err != nil {
			return nil, err
		}
	case *ast.BRIEStmt:
		p.setSchemaAndNames(buildBRIESchema())
		if raw.Kind == ast.BRIEKindRestore {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESTORE_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "RESTORE_ADMIN", false, err)
		} else {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or BACKUP_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "BACKUP_ADMIN", false, err)
		}
	case *ast.GrantRoleStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or ROLE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "ROLE_ADMIN", false, err)
	case *ast.RevokeRoleStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or ROLE_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "ROLE_ADMIN", false, err)
		// Check if any of the users are RESTRICTED
		for _, user := range raw.Users {
			b.visitInfo = appendVisitInfoIsRestrictedUser(b.visitInfo, b.ctx, user, "RESTRICTED_USER_ADMIN")
		}
	case *ast.RevokeStmt:
		var err error
		b.visitInfo, err = collectVisitInfoFromRevokeStmt(b.ctx, b.visitInfo, raw)
		if err != nil {
			return nil, err
		}
	case *ast.KillStmt:
		// All users can kill their own connections regardless.
		// If you have the SUPER privilege, you can kill all threads and statements unless SEM is enabled.
		// In which case you require RESTRICTED_CONNECTION_ADMIN to kill connections that belong to RESTRICTED_USER_ADMIN users.
		sm := b.ctx.GetSessionManager()
		if sm != nil {
			if pi, ok := sm.GetProcessInfo(raw.ConnectionID); ok {
				loginUser := b.ctx.GetSessionVars().User
				if pi.User != loginUser.Username {
					err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or CONNECTION_ADMIN")
					b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "CONNECTION_ADMIN", false, err)
					b.visitInfo = appendVisitInfoIsRestrictedUser(b.visitInfo, b.ctx, &auth.UserIdentity{Username: pi.User, Hostname: pi.Host}, "RESTRICTED_CONNECTION_ADMIN")
				}
			} else if raw.ConnectionID == util2.GetAutoAnalyzeProcID(domain.GetDomain(b.ctx).ServerID) {
				// Only the users with SUPER or CONNECTION_ADMIN privilege can kill auto analyze.
				err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or CONNECTION_ADMIN")
				b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "CONNECTION_ADMIN", false, err)
			}
		}
	case *ast.UseStmt:
		if raw.DBName == "" {
			return nil, ErrNoDB
		}
	case *ast.DropUserStmt:
		// The main privilege checks for DROP USER are currently performed in executor/simple.go
		// because they use complex OR conditions (not supported by visitInfo).
		for _, user := range raw.UserList {
			b.visitInfo = appendVisitInfoIsRestrictedUser(b.visitInfo, b.ctx, user, "RESTRICTED_USER_ADMIN")
		}
	case *ast.SetPwdStmt:
		if raw.User != nil {
			b.visitInfo = appendVisitInfoIsRestrictedUser(b.visitInfo, b.ctx, raw.User, "RESTRICTED_USER_ADMIN")
		}
	case *ast.ShutdownStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ShutdownPriv, "", "", "", nil)
	case *ast.BeginStmt:
		readTS := b.ctx.GetSessionVars().TxnReadTS.PeakTxnReadTS()
		if raw.AsOf != nil {
			startTS, err := staleread.CalculateAsOfTsExpr(b.ctx, raw.AsOf)
			if err != nil {
				return nil, err
			}
			if err := sessionctx.ValidateStaleReadTS(ctx, b.ctx, startTS); err != nil {
				return nil, err
			}
			p.StaleTxnStartTS = startTS
		} else if readTS > 0 {
			p.StaleTxnStartTS = readTS
			// consume read ts here
			b.ctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
		}
	}
	return p, nil
}

func collectVisitInfoFromRevokeStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.RevokeStmt) ([]visitInfo, error) {
	// To use REVOKE, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	// This supports a local revoke SELECT on tablename, but does
	// not add dbName to the visitInfo of a *.* grant.
	if dbName == "" && stmt.Level.Level != ast.GrantLevelGlobal {
		if sctx.GetSessionVars().CurrentDB == "" {
			return nil, ErrNoDB
		}
		dbName = sctx.GetSessionVars().CurrentDB
	}
	var nonDynamicPrivilege bool
	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		if item.Priv == mysql.ExtendedPriv {
			vi = appendDynamicVisitInfo(vi, strings.ToUpper(item.Name), true, nil) // verified in MySQL: requires the dynamic grant option to revoke.
			continue
		}
		nonDynamicPrivilege = true
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
	for _, u := range stmt.Users {
		// For SEM, make sure the users are not restricted
		vi = appendVisitInfoIsRestrictedUser(vi, sctx, u.User, "RESTRICTED_USER_ADMIN")
	}
	if nonDynamicPrivilege {
		// Dynamic privileges use their own GRANT OPTION. If there were any non-dynamic privilege requests,
		// we need to attach the "GLOBAL" version of the GRANT OPTION.
		vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)
	}
	return vi, nil
}

// appendVisitInfoIsRestrictedUser appends additional visitInfo if the user has a
// special privilege called "RESTRICTED_USER_ADMIN". It only applies when SEM is enabled.
func appendVisitInfoIsRestrictedUser(visitInfo []visitInfo, sctx sessionctx.Context, user *auth.UserIdentity, priv string) []visitInfo {
	if !sem.IsEnabled() {
		return visitInfo
	}
	checker := privilege.GetPrivilegeManager(sctx)
	if checker != nil && checker.RequestDynamicVerificationWithUser("RESTRICTED_USER_ADMIN", false, user) {
		err := ErrSpecificAccessDenied.GenWithStackByArgs(priv)
		visitInfo = appendDynamicVisitInfo(visitInfo, priv, false, err)
	}
	return visitInfo
}

func collectVisitInfoFromGrantStmt(sctx sessionctx.Context, vi []visitInfo, stmt *ast.GrantStmt) ([]visitInfo, error) {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	tableName := stmt.Level.TableName
	// This supports a local revoke SELECT on tablename, but does
	// not add dbName to the visitInfo of a *.* grant.
	if dbName == "" && stmt.Level.Level != ast.GrantLevelGlobal {
		if sctx.GetSessionVars().CurrentDB == "" {
			return nil, ErrNoDB
		}
		dbName = sctx.GetSessionVars().CurrentDB
	}
	var nonDynamicPrivilege bool
	var allPrivs []mysql.PrivilegeType
	for _, item := range stmt.Privs {
		if item.Priv == mysql.ExtendedPriv {
			// The observed MySQL behavior is that the error is:
			// ERROR 1227 (42000): Access denied; you need (at least one of) the GRANT OPTION privilege(s) for this operation
			// This is ambiguous, because it doesn't say the GRANT OPTION for which dynamic privilege.

			// In privilege/privileges/cache.go:RequestDynamicVerification SUPER+Grant_Priv will also be accepted here by TiDB, but it is *not* by MySQL.
			// This extension is currently required because:
			// - The visitInfo system does not accept OR conditions. There are many scenarios where SUPER or a DYNAMIC privilege are supported,
			//   this is the one case where SUPER is not intended to be an alternative.
			// - The "ALL" privilege for TiDB does not include all dynamic privileges. This could be fixed by a bootstrap task to assign all SUPER users
			//   with dynamic privileges.

			err := ErrSpecificAccessDenied.GenWithStackByArgs("GRANT OPTION")
			vi = appendDynamicVisitInfo(vi, item.Name, true, err)
			continue
		}
		nonDynamicPrivilege = true
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
	if nonDynamicPrivilege {
		// Dynamic privileges use their own GRANT OPTION. If there were any non-dynamic privilege requests,
		// we need to attach the "GLOBAL" version of the GRANT OPTION.
		vi = appendVisitInfo(vi, mysql.GrantPriv, dbName, tableName, "", nil)
	}
	return vi, nil
}

func (b *PlanBuilder) getDefaultValue(col *table.Column) (*expression.Constant, error) {
	var (
		value types.Datum
		err   error
	)
	if col.DefaultIsExpr && col.DefaultExpr != nil {
		value, err = table.EvalColDefaultExpr(b.ctx, col.ToInfo(), col.DefaultExpr)
	} else {
		value, err = table.GetColDefaultValue(b.ctx, col.ToInfo())
	}
	if err != nil {
		return nil, err
	}
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

// resolveGeneratedColumns resolves generated columns with their generation
// expressions respectively. onDups indicates which columns are in on-duplicate list.
func (b *PlanBuilder) resolveGeneratedColumns(ctx context.Context, columns []*table.Column, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedColumns, err error) {
	for _, column := range columns {
		if !column.IsGenerated() {
			continue
		}
		columnName := &ast.ColumnName{Name: column.Name}
		columnName.SetText(nil, column.Name.O)

		idx, err := expression.FindFieldName(mockPlan.OutputNames(), columnName)
		if err != nil {
			return igc, err
		}
		colExpr := mockPlan.Schema().Columns[idx]

		expr, _, err := b.rewrite(ctx, column.GeneratedExpr, mockPlan, nil, true)
		if err != nil {
			return igc, err
		}

		igc.Columns = append(igc.Columns, columnName)
		igc.Exprs = append(igc.Exprs, expr)
		if onDups == nil {
			continue
		}
		for dep := range column.Dependences {
			if _, ok := onDups[dep]; ok {
				assign := &expression.Assignment{Col: colExpr, ColName: column.Name, Expr: expr}
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
		err := errors.Errorf("insert into view %s is not supported now", tableInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into view %s is not supported now", tableInfo.Name.O)
		}
		return nil, err
	}
	if tableInfo.IsSequence() {
		err := errors.Errorf("insert into sequence %s is not supported now", tableInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into sequence %s is not supported now", tableInfo.Name.O)
		}
		return nil, err
	}
	// Build Schema with DBName otherwise ColumnRef with DBName cannot match any Column in Schema.
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx, tn.Schema, tableInfo)
	if err != nil {
		return nil, err
	}
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		return nil, errors.Errorf("Can't get table %s", tableInfo.Name.O)
	}

	insertPlan := Insert{
		Table:         tableInPlan,
		Columns:       insert.Columns,
		tableSchema:   schema,
		tableColNames: names,
		IsReplace:     insert.IsReplace,
	}.Init(b.ctx)

	if tableInfo.GetPartitionInfo() != nil && len(insert.PartitionNames) != 0 {
		givenPartitionSets := make(map[int64]struct{}, len(insert.PartitionNames))
		// check partition by name.
		for _, name := range insert.PartitionNames {
			id, err := tables.FindPartitionByName(tableInfo, name.L)
			if err != nil {
				return nil, err
			}
			givenPartitionSets[id] = struct{}{}
		}
		pt := tableInPlan.(table.PartitionedTable)
		insertPlan.Table = tables.NewPartitionTableWithGivenSets(pt, givenPartitionSets)
	} else if len(insert.PartitionNames) != 0 {
		return nil, ErrPartitionClauseOnNonpartitioned
	}

	user := b.ctx.GetSessionVars().User
	var authErr error
	if user != nil {
		authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, tableInfo.Name.L)
	}

	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tn.DBInfo.Name.L,
		tableInfo.Name.L, "", authErr)

	// `REPLACE INTO` requires both INSERT + DELETE privilege
	// `ON DUPLICATE KEY UPDATE` requires both INSERT + UPDATE privilege
	var extraPriv mysql.PrivilegeType
	if insert.IsReplace {
		extraPriv = mysql.DeletePriv
	} else if insert.OnDuplicate != nil {
		extraPriv = mysql.UpdatePriv
	}
	if extraPriv != 0 {
		if user != nil {
			cmd := strings.ToUpper(mysql.Priv2Str[extraPriv])
			authErr = ErrTableaccessDenied.GenWithStackByArgs(cmd, user.AuthUsername, user.AuthHostname, tableInfo.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, extraPriv, tn.DBInfo.Name.L, tableInfo.Name.L, "", authErr)
	}

	mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	mockTablePlan.SetSchema(insertPlan.tableSchema)
	mockTablePlan.names = insertPlan.tableColNames

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
	mockTablePlan.names = insertPlan.names4OnDuplicate

	onDupColSet, err := insertPlan.resolveOnDuplicate(insert.OnDuplicate, tableInfo, func(node ast.ExprNode) (expression.Expression, error) {
		return b.rewriteInsertOnDuplicateUpdate(ctx, node, mockTablePlan, insertPlan)
	})
	if err != nil {
		return nil, err
	}

	// Calculate generated columns.
	mockTablePlan.schema = insertPlan.tableSchema
	mockTablePlan.names = insertPlan.tableColNames
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
		idx, err := expression.FindFieldName(p.tableColNames, assign.Column)
		if err != nil {
			return nil, err
		} else if idx < 0 {
			return nil, ErrUnknownColumn.GenWithStackByArgs(assign.Column.OrigColName(), clauseMsg[fieldList])
		}

		column := colMap[assign.Column.Name.L]
		if column.Hidden {
			return nil, ErrUnknownColumn.GenWithStackByArgs(column.Name, clauseMsg[fieldList])
		}
		// Check whether the column to be updated is the generated column.
		// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
		// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
		if column.IsGenerated() {
			if IsDefaultExprSameColumn(p.tableColNames[idx:idx+1], assign.Expr) {
				continue
			}
			return nil, ErrBadGeneratedColumn.GenWithStackByArgs(assign.Column.Name.O, tblInfo.Name.O)
		}
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.Column
		}

		onDupColSet[column.Name.L] = struct{}{}

		expr, err := yield(assign.Expr)
		if err != nil {
			// Throw other error as soon as possible except ErrSubqueryMoreThan1Row which need duplicate in insert in triggered.
			// Refer to https://github.com/pingcap/tidb/issues/29260 for more information.
			if terr, ok := errors.Cause(err).(*terror.Error); !(ok && ErrSubqueryMoreThan1Row.Code() == terr.Code()) {
				return nil, err
			}
		}

		p.OnDuplicate = append(p.OnDuplicate, &expression.Assignment{
			Col:     p.tableSchema.Columns[idx],
			ColName: p.tableColNames[idx].ColName,
			Expr:    expr,
			LazyErr: err,
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
			colName = append(colName, col.Name.L)
		}
		var missingColIdx int
		affectedValuesCols, missingColIdx = table.FindColumns(insertPlan.Table.VisibleCols(), colName, insertPlan.Table.Meta().PKIsHandle)
		if missingColIdx >= 0 {
			return nil, ErrUnknownColumn.GenWithStackByArgs(
				insertStmt.Columns[missingColIdx].Name.O, clauseMsg[fieldList])
		}
	} else if len(insertStmt.Setlist) == 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesCols = insertPlan.Table.VisibleCols()
	}
	return affectedValuesCols, nil
}

func (b PlanBuilder) getInsertColExpr(ctx context.Context, insertPlan *Insert, mockTablePlan *LogicalTableDual, col *table.Column, expr ast.ExprNode, checkRefColumn func(n ast.Node) ast.Node) (outExpr expression.Expression, err error) {
	if col.Hidden {
		return nil, ErrUnknownColumn.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
	}
	switch x := expr.(type) {
	case *ast.DefaultExpr:
		refCol := col
		if x.Name != nil {
			refCol = table.FindColLowerCase(insertPlan.Table.Cols(), x.Name.Name.L)
			if refCol == nil {
				return nil, ErrUnknownColumn.GenWithStackByArgs(x.Name.OrigColName(), clauseMsg[fieldList])
			}
			// Cannot use DEFAULT(generated column) except for the same column
			if col != refCol && (col.IsGenerated() || refCol.IsGenerated()) {
				return nil, ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
			} else if col == refCol && col.IsGenerated() {
				return nil, nil
			}
		} else if col.IsGenerated() {
			// See note in the end of the function. Only default for generated columns are OK.
			return nil, nil
		}
		outExpr, err = b.getDefaultValue(refCol)
	case *driver.ValueExpr:
		outExpr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	case *driver.ParamMarkerExpr:
		outExpr, err = expression.ParamMarkerExpression(b.ctx, x, false)
	default:
		b.curClause = fieldList
		// subquery in insert values should not reference upper scope
		usingPlan := mockTablePlan
		if _, ok := expr.(*ast.SubqueryExpr); ok {
			usingPlan = LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
		}
		var np LogicalPlan
		outExpr, np, err = b.rewriteWithPreprocess(ctx, expr, usingPlan, nil, nil, true, checkRefColumn)
		if np != nil {
			if _, ok := np.(*LogicalTableDual); !ok {
				// See issue#30626 and the related tests in function TestInsertValuesWithSubQuery for more details.
				// This is a TODO and we will support it later.
				return nil, errors.New("Insert's SET operation or VALUES_LIST doesn't support complex subqueries now")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if insertPlan.AllAssignmentsAreConstant {
		_, isConstant := outExpr.(*expression.Constant)
		insertPlan.AllAssignmentsAreConstant = isConstant
	}
	// Note: For INSERT, REPLACE, and UPDATE, if a generated column is inserted into, replaced, or updated explicitly, the only permitted value is DEFAULT.
	// see https://dev.mysql.com/doc/refman/8.0/en/create-table-generated-columns.html
	if col.IsGenerated() {
		return nil, ErrBadGeneratedColumn.GenWithStackByArgs(col.Name.O, insertPlan.Table.Meta().Name.O)
	}
	return outExpr, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockTablePlan *LogicalTableDual, checkRefColumn func(n ast.Node) ast.Node) error {
	tableInfo := insertPlan.Table.Meta()
	colNames := make([]string, 0, len(insert.Setlist))
	exprCols := make([]*expression.Column, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		idx, err := expression.FindFieldName(insertPlan.tableColNames, assign.Column)
		if err != nil {
			return err
		}
		if idx < 0 {
			return errors.Errorf("Can't find column %s", assign.Column)
		}
		colNames = append(colNames, assign.Column.Name.L)
		exprCols = append(exprCols, insertPlan.tableSchema.Columns[idx])
	}

	// Check whether the column to be updated is the generated column.
	tCols, missingColIdx := table.FindColumns(insertPlan.Table.VisibleCols(), colNames, tableInfo.PKIsHandle)
	if missingColIdx >= 0 {
		return ErrUnknownColumn.GenWithStackByArgs(insert.Setlist[missingColIdx].Column.Name.O, clauseMsg[fieldList])
	}

	insertPlan.AllAssignmentsAreConstant = true
	for i, assign := range insert.Setlist {
		expr, err := b.getInsertColExpr(ctx, insertPlan, mockTablePlan, tCols[i], assign.Expr, checkRefColumn)
		if err != nil {
			return err
		}
		if expr == nil {
			continue
		}

		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			Col:     exprCols[i],
			ColName: model.NewCIStr(colNames[i]),
			Expr:    expr,
		})
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	insertPlan.names4OnDuplicate = insertPlan.tableColNames
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

	insertPlan.AllAssignmentsAreConstant = true
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
			expr, err := b.getInsertColExpr(ctx, insertPlan, mockTablePlan, affectedValuesCols[j], valueItem, checkRefColumn)
			if err != nil {
				return err
			}
			if expr == nil {
				continue
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	insertPlan.Schema4OnDuplicate = insertPlan.tableSchema
	insertPlan.names4OnDuplicate = insertPlan.tableColNames
	return nil
}

type colNameInOnDupExtractor struct {
	colNameMap map[types.FieldName]*ast.ColumnNameExpr
}

func (c *colNameInOnDupExtractor) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case *ast.ColumnNameExpr:
		fieldName := types.FieldName{
			DBName:  x.Name.Schema,
			TblName: x.Name.Table,
			ColName: x.Name.Name,
		}
		c.colNameMap[fieldName] = x
		return node, true
	// We don't extract the column names from the sub select.
	case *ast.SelectStmt, *ast.SetOprStmt:
		return node, true
	default:
		return node, false
	}
}

func (c *colNameInOnDupExtractor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func (b *PlanBuilder) buildSelectPlanOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert) error {
	b.isForUpdateRead = true
	affectedValuesCols, err := b.getAffectCols(insert, insertPlan)
	if err != nil {
		return err
	}
	actualColLen := -1
	// For MYSQL, it handles the case that the columns in ON DUPLICATE UPDATE is not the project column of the SELECT clause
	// but just in the table occurs in the SELECT CLAUSE.
	//   e.g. insert into a select x from b ON DUPLICATE KEY UPDATE  a.x=b.y; the `y` is not a column of select's output.
	//        MySQL won't throw error and will execute this SQL successfully.
	// To make compatible with this strange feature, we add the variable `actualColLen` and the following IF branch.
	if len(insert.OnDuplicate) > 0 {
		// If the select has aggregation, it cannot see the columns not in the select field.
		//   e.g. insert into a select x from b ON DUPLICATE KEY UPDATE  a.x=b.y; can be executed successfully.
		//        insert into a select x from b group by x ON DUPLICATE KEY UPDATE  a.x=b.y; will report b.y not found.
		if sel, ok := insert.Select.(*ast.SelectStmt); ok && !b.detectSelectAgg(sel) {
			hasWildCard := false
			for _, field := range sel.Fields.Fields {
				if field.WildCard != nil {
					hasWildCard = true
					break
				}
			}
			if !hasWildCard {
				colExtractor := &colNameInOnDupExtractor{colNameMap: make(map[types.FieldName]*ast.ColumnNameExpr)}
				for _, assign := range insert.OnDuplicate {
					assign.Expr.Accept(colExtractor)
				}
				actualColLen = len(sel.Fields.Fields)
				for _, colName := range colExtractor.colNameMap {
					// If we found the name from the INSERT's table columns, we don't try to find it in select field anymore.
					if insertPlan.tableColNames.FindAstColName(colName.Name) {
						continue
					}
					found := false
					for _, field := range sel.Fields.Fields {
						if colField, ok := field.Expr.(*ast.ColumnNameExpr); ok &&
							(colName.Name.Schema.L == "" || colField.Name.Schema.L == "" || colName.Name.Schema.L == colField.Name.Schema.L) &&
							(colName.Name.Table.L == "" || colField.Name.Table.L == "" || colName.Name.Table.L == colField.Name.Table.L) &&
							colName.Name.Name.L == colField.Name.Name.L {
							found = true
							break
						}
					}
					if found {
						continue
					}
					sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{Expr: colName, Offset: len(sel.Fields.Fields)})
				}
			}
		}
	}
	selectPlan, err := b.Build(ctx, insert.Select)
	if err != nil {
		return err
	}

	// Check to guarantee that the length of the row returned by select is equal to that of affectedValuesCols.
	if (actualColLen == -1 && selectPlan.Schema().Len() != len(affectedValuesCols)) || (actualColLen != -1 && actualColLen != len(affectedValuesCols)) {
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

	names := selectPlan.OutputNames()
	insertPlan.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return err
	}

	if actualColLen == -1 {
		actualColLen = selectPlan.Schema().Len()
	}
	insertPlan.RowLen = actualColLen
	// schema4NewRow is the schema for the newly created data record based on
	// the result of the select statement.
	schema4NewRow := expression.NewSchema(make([]*expression.Column, len(insertPlan.Table.Cols()))...)
	names4NewRow := make(types.NameSlice, len(insertPlan.Table.Cols()))
	// TODO: don't clone it.
	for i := 0; i < actualColLen; i++ {
		selCol := insertPlan.SelectPlan.Schema().Columns[i]
		ordinal := affectedValuesCols[i].Offset
		schema4NewRow.Columns[ordinal] = &expression.Column{}
		*schema4NewRow.Columns[ordinal] = *selCol

		schema4NewRow.Columns[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.Columns[ordinal].RetType = affectedValuesCols[i].FieldType

		names4NewRow[ordinal] = names[i]
	}
	for i := range schema4NewRow.Columns {
		if schema4NewRow.Columns[i] == nil {
			schema4NewRow.Columns[i] = &expression.Column{UniqueID: insertPlan.ctx.GetSessionVars().AllocPlanColumnID()}
			names4NewRow[i] = types.EmptyName
		}
	}
	insertPlan.Schema4OnDuplicate = expression.NewSchema(insertPlan.tableSchema.Columns...)
	insertPlan.Schema4OnDuplicate.Append(insertPlan.SelectPlan.Schema().Columns[actualColLen:]...)
	insertPlan.Schema4OnDuplicate.Append(schema4NewRow.Columns...)
	insertPlan.names4OnDuplicate = append(insertPlan.tableColNames.Shallow(), names[actualColLen:]...)
	insertPlan.names4OnDuplicate = append(insertPlan.names4OnDuplicate, names4NewRow...)
	return nil
}

func (b *PlanBuilder) buildLoadData(ctx context.Context, ld *ast.LoadDataStmt) (Plan, error) {
	p := LoadData{
		IsLocal:            ld.IsLocal,
		OnDuplicate:        ld.OnDuplicate,
		Path:               ld.Path,
		Table:              ld.Table,
		Columns:            ld.Columns,
		FieldsInfo:         ld.FieldsInfo,
		LinesInfo:          ld.LinesInfo,
		IgnoreLines:        ld.IgnoreLines,
		ColumnAssignments:  ld.ColumnAssignments,
		ColumnsAndUserVars: ld.ColumnsAndUserVars,
	}.Init(b.ctx)
	user := b.ctx.GetSessionVars().User
	var insertErr, deleteErr error
	if user != nil {
		insertErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		deleteErr = ErrTableaccessDenied.GenWithStackByArgs("DELETE", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, p.Table.Schema.O, p.Table.Name.O, "", insertErr)
	if p.OnDuplicate == ast.OnDuplicateKeyHandlingReplace {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, p.Table.Schema.O, p.Table.Name.O, "", deleteErr)
	}
	tableInfo := p.Table.TableInfo
	tableInPlan, ok := b.is.TableByID(tableInfo.ID)
	if !ok {
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(db, tableInfo.Name.O)
	}
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx, model.NewCIStr(""), tableInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	mockTablePlan.SetSchema(schema)
	mockTablePlan.names = names

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

func (b *PlanBuilder) buildIndexAdvise(node *ast.IndexAdviseStmt) Plan {
	p := &IndexAdvise{
		IsLocal:     node.IsLocal,
		Path:        node.Path,
		MaxMinutes:  node.MaxMinutes,
		MaxIndexNum: node.MaxIndexNum,
		LinesInfo:   node.LinesInfo,
	}
	return p
}

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (Plan, error) {
	if node.Table.TableInfo.TempTableType != model.TempTableNone {
		return nil, ErrOptOnTemporaryTable.GenWithStackByArgs("split table")
	}
	if node.SplitSyntaxOpt != nil && node.SplitSyntaxOpt.HasPartition && node.Table.TableInfo.Partition == nil {
		return nil, ErrPartitionClauseOnNonpartitioned
	}
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
	mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx, node.Table.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.names = names

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
		IndexInfo:      indexInfo,
	}
	p.names = names
	p.setSchemaAndNames(buildSplitRegionsSchema())
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
		if !types.ErrTruncated.Equal(err) && !types.ErrTruncatedWrongVal.Equal(err) && !types.ErrBadNumber.Equal(err) {
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
	handleColInfos := buildHandleColumnInfos(tblInfo)
	mockTablePlan := LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx, node.Table.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.names = names

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	p.setSchemaAndNames(buildSplitRegionsSchema())
	if len(node.SplitOpt.ValueLists) > 0 {
		values := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			data, err := convertValueListToData(valuesItem, handleColInfos, i, b, mockTablePlan)
			if err != nil {
				return nil, err
			}
			values = append(values, data)
		}
		p.ValueLists = values
		return p, nil
	}

	p.Lower, err = convertValueListToData(node.SplitOpt.Lower, handleColInfos, lowerBound, b, mockTablePlan)
	if err != nil {
		return nil, err
	}
	p.Upper, err = convertValueListToData(node.SplitOpt.Upper, handleColInfos, upperBound, b, mockTablePlan)
	if err != nil {
		return nil, err
	}

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split table region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split table region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func buildHandleColumnInfos(tblInfo *model.TableInfo) []*model.ColumnInfo {
	switch {
	case tblInfo.PKIsHandle:
		if col := tblInfo.GetPkColInfo(); col != nil {
			return []*model.ColumnInfo{col}
		}
	case tblInfo.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkCols := make([]*model.ColumnInfo, 0, len(pkIdx.Columns))
		cols := tblInfo.Columns
		for _, idxCol := range pkIdx.Columns {
			pkCols = append(pkCols, cols[idxCol.Offset])
		}
		return pkCols
	default:
		return []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}
	return nil
}

const (
	lowerBound int = -1
	upperBound int = -2
)

func convertValueListToData(valueList []ast.ExprNode, handleColInfos []*model.ColumnInfo, rowIdx int,
	b *PlanBuilder, mockTablePlan *LogicalTableDual) ([]types.Datum, error) {
	if len(valueList) != len(handleColInfos) {
		var err error
		switch rowIdx {
		case lowerBound:
			err = errors.Errorf("Split table region lower value count should be %d", len(handleColInfos))
		case upperBound:
			err = errors.Errorf("Split table region upper value count should be %d", len(handleColInfos))
		default:
			err = ErrWrongValueCountOnRow.GenWithStackByArgs(rowIdx)
		}
		return nil, err
	}
	data := make([]types.Datum, 0, len(handleColInfos))
	for i, v := range valueList {
		convertedDatum, err := b.convertValue(v, mockTablePlan, handleColInfos[i])
		if err != nil {
			return nil, err
		}
		if convertedDatum.IsNull() {
			return nil, ErrBadNull.GenWithStackByArgs(handleColInfos[i].Name.O)
		}
		data = append(data, convertedDatum)
	}
	return data, nil
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
			authErr = ErrDBaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name, "", "", authErr)
	case *ast.AlterTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		dbName := v.Table.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetSessionVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, dbName,
			v.Table.Name.L, "", authErr)
		for _, spec := range v.Specs {
			if spec.Tp == ast.AlterTableRenameTable || spec.Tp == ast.AlterTableExchangePartition {
				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, dbName,
					v.Table.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, dbName,
					spec.NewTable.Name.L, "", authErr)

				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, spec.NewTable.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, dbName,
					spec.NewTable.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterTableDropPartition {
				if b.ctx.GetSessionVars().User != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
						b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
					v.Table.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterTableWriteable {
				b.visitInfo[0].alterWritable = true
			} else if spec.Tp == ast.AlterTableAddStatistics {
				var selectErr, insertErr error
				user := b.ctx.GetSessionVars().User
				if user != nil {
					selectErr = ErrTableaccessDenied.GenWithStackByArgs("ADD STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, v.Table.Name.L)
					insertErr = ErrTableaccessDenied.GenWithStackByArgs("ADD STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, "stats_extended")
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.Table.Schema.L,
					v.Table.Name.L, "", selectErr)
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, mysql.SystemDB,
					"stats_extended", "", insertErr)
			} else if spec.Tp == ast.AlterTableDropStatistics {
				user := b.ctx.GetSessionVars().User
				if user != nil {
					authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP STATS_EXTENDED", user.AuthUsername,
						user.AuthHostname, "stats_extended")
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, mysql.SystemDB,
					"stats_extended", "", authErr)
			}
		}
	case *ast.AlterSequenceStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.Name.Schema.L,
			v.Name.Name.L, "", authErr)
	case *ast.CreateDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name,
			"", "", authErr)
	case *ast.CreateIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.CreateTableStmt:
		if v.TemporaryKeyword != ast.TemporaryNone {
			for _, cons := range v.Constraints {
				if cons.Tp == ast.ConstraintForeignKey {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		if b.ctx.GetSessionVars().User != nil {
			// This is tricky here: we always need the visitInfo because it's not only used in privilege checks, and we
			// must pass the table name. However, the privilege check is towards the database. We'll deal with it later.
			if v.TemporaryKeyword == ast.TemporaryLocal {
				authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Schema.L)
			} else {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
			}
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
		if v.ReferTable != nil {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, v.ReferTable.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, v.ReferTable.Schema.L,
				v.ReferTable.Name.L, "", authErr)
		}
	case *ast.CreateViewStmt:
		b.isCreateView = true
		b.capFlag |= canExpandAST | renameView
		b.renamingViewName = v.ViewName.Schema.L + "." + v.ViewName.Name.L
		defer func() {
			b.capFlag &= ^canExpandAST
			b.capFlag &= ^renameView
			b.isCreateView = false
		}()

		if stmt := findStmtAsViewSchema(v); stmt != nil {
			stmt.AsViewSchema = true
		}

		plan, err := b.Build(ctx, v.Select)
		if err != nil {
			return nil, err
		}
		schema := plan.Schema()
		names := plan.OutputNames()
		if v.Cols == nil {
			adjustOverlongViewColname(plan.(LogicalPlan))
			v.Cols = make([]model.CIStr, len(schema.Columns))
			for i, name := range names {
				v.Cols[i] = name.ColName
			}
		}
		if len(v.Cols) != schema.Len() {
			return nil, dbterror.ErrViewWrongList
		}
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE VIEW", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.ViewName.Name.L)
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
	case *ast.CreateSequenceStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.Name.Schema.L,
			v.Name.Name.L, "", authErr)
	case *ast.DropDatabaseStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Name,
			"", "", authErr)
	case *ast.DropIndexStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.IndexPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.DropTableStmt:
		for _, tableVal := range v.Tables {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, tableVal.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, tableVal.Schema.L,
				tableVal.Name.L, "", authErr)
		}
	case *ast.DropSequenceStmt:
		for _, sequence := range v.Sequences {
			if b.ctx.GetSessionVars().User != nil {
				authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
					b.ctx.GetSessionVars().User.AuthHostname, sequence.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, sequence.Schema.L,
				sequence.Name.L, "", authErr)
		}
	case *ast.TruncateTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.Table.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.Table.Schema.L,
			v.Table.Name.L, "", authErr)
	case *ast.RenameTableStmt:
		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, v.TableToTables[0].OldTable.Schema.L,
			v.TableToTables[0].OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].OldTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DropPriv, v.TableToTables[0].OldTable.Schema.L,
			v.TableToTables[0].OldTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.CreatePriv, v.TableToTables[0].NewTable.Schema.L,
			v.TableToTables[0].NewTable.Name.L, "", authErr)

		if b.ctx.GetSessionVars().User != nil {
			authErr = ErrTableaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetSessionVars().User.AuthUsername,
				b.ctx.GetSessionVars().User.AuthHostname, v.TableToTables[0].NewTable.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, v.TableToTables[0].NewTable.Schema.L,
			v.TableToTables[0].NewTable.Name.L, "", authErr)
	case *ast.RecoverTableStmt, *ast.FlashBackTableStmt:
		// Recover table command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt:
		// TODO: add Lock Table privilege check.
	case *ast.CleanupTableLockStmt:
		// This command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.RepairTableStmt:
		// Repair table command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	case *ast.DropPlacementPolicyStmt, *ast.CreatePlacementPolicyStmt, *ast.AlterPlacementPolicyStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or PLACEMENT_ADMIN")
		b.visitInfo = appendDynamicVisitInfo(b.visitInfo, "PLACEMENT_ADMIN", false, err)
	}
	p := &DDL{Statement: node}
	return p, nil
}

const (
	// TraceFormatRow indicates row tracing format.
	TraceFormatRow = "row"
	// TraceFormatJSON indicates json tracing format.
	TraceFormatJSON = "json"
	// TraceFormatLog indicates log tracing format.
	TraceFormatLog = "log"

	// TracePlanTargetEstimation indicates CE trace target for optimizer trace.
	TracePlanTargetEstimation = "estimation"
)

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (Plan, error) {
	p := &Trace{
		StmtNode:             trace.Stmt,
		Format:               trace.Format,
		OptimizerTrace:       trace.TracePlan,
		OptimizerTraceTarget: trace.TracePlanTarget,
	}
	// TODO: forbid trace plan if the statement isn't select read-only statement
	if trace.TracePlan {
		if trace.TracePlanTarget != "" && trace.TracePlanTarget != TracePlanTargetEstimation {
			return nil, errors.New("trace plan target should only be 'estimation'")
		}
		if trace.TracePlanTarget == TracePlanTargetEstimation {
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "CE_trace", mysql.TypeVarchar, mysql.MaxBlobWidth))
			p.SetSchema(schema.col2Schema())
			p.names = schema.names
		} else {
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "Dump_link", mysql.TypeVarchar, 128))
			p.SetSchema(schema.col2Schema())
			p.names = schema.names
		}
		return p, nil
	}
	switch trace.Format {
	case TraceFormatRow:
		schema := newColumnsWithNames(3)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.names = schema.names
	case TraceFormatJSON:
		schema := newColumnsWithNames(1)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.names = schema.names
	case TraceFormatLog:
		schema := newColumnsWithNames(4)
		schema.Append(buildColumnWithName("", "time", mysql.TypeTimestamp, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "event", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "tags", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "spanName", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.names = schema.names
	default:
		return nil, errors.New("trace format should be one of 'row', 'log' or 'json'")
	}
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan Plan, format string, explainRows [][]string, analyze bool, execStmt ast.StmtNode, runtimeStats *execdetails.RuntimeStatsColl) (Plan, error) {
	p := &Explain{
		TargetPlan:       targetPlan,
		Format:           format,
		Analyze:          analyze,
		ExecStmt:         execStmt,
		ExplainRows:      explainRows,
		RuntimeStatsColl: runtimeStats,
	}
	p.ctx = b.ctx
	return p, p.prepareSchema()
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
	var explainRows [][]string
	if explainFor.Format == types.ExplainFormatROW {
		explainRows = processInfo.PlanExplainRows
	}
	return b.buildExplainPlan(targetPlan, explainFor.Format, explainRows, false, nil, processInfo.RuntimeStatsColl)
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}
	targetPlan, _, err := OptimizeAstNode(ctx, b.ctx, explain.Stmt, b.is)
	if err != nil {
		return nil, err
	}

	return b.buildExplainPlan(targetPlan, explain.Format, nil, explain.Analyze, explain.Stmt, nil)
}

func (b *PlanBuilder) buildSelectInto(ctx context.Context, sel *ast.SelectStmt) (Plan, error) {
	if sem.IsEnabled() {
		return nil, ErrNotSupportedWithSem.GenWithStackByArgs("SELECT INTO")
	}
	selectIntoInfo := sel.SelectIntoOpt
	sel.SelectIntoOpt = nil
	targetPlan, _, err := OptimizeAstNode(ctx, b.ctx, sel, b.is)
	if err != nil {
		return nil, err
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.FilePriv, "", "", "", ErrSpecificAccessDenied.GenWithStackByArgs("FILE"))
	return &SelectInto{
		TargetPlan: targetPlan,
		IntoOpt:    selectIntoInfo,
	}, nil
}

func buildShowProcedureSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "ROUTINES"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowTriggerSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "TRIGGERS"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowEventsSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "EVENTS"
	schema := newColumnsWithNames(15)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowWarningsSchema() (*expression.Schema, types.NameSlice) {
	tblName := "WARNINGS"
	schema := newColumnsWithNames(3)
	schema.Append(buildColumnWithName(tblName, "Level", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName(tblName, "Code", mysql.TypeLong, 19))
	schema.Append(buildColumnWithName(tblName, "Message", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool, isSequence bool) (schema *expression.Schema, outputNames []*types.FieldName) {
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowProcedureStatus:
		return buildShowProcedureSchema()
	case ast.ShowTriggers:
		return buildShowTriggerSchema()
	case ast.ShowEvents:
		return buildShowEventsSchema()
	case ast.ShowWarnings, ast.ShowErrors:
		return buildShowWarningsSchema()
	case ast.ShowRegions:
		return buildTableRegionsSchema()
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowConfig:
		names = []string{"Type", "Instance", "Name", "Value"}
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
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCreateTable, ast.ShowCreateSequence:
		if isSequence {
			names = []string{"Sequence", "Create Sequence"}
		} else if isView {
			names = []string{"View", "Create View", "character_set_client", "collation_connection"}
		} else {
			names = []string{"Table", "Create Table"}
		}
	case ast.ShowCreatePlacementPolicy:
		names = []string{"Policy", "Create Policy"}
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
			"Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression", "Clustered"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
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
	case ast.ShowStatsExtended:
		names = []string{"Db_name", "Table_name", "Stats_name", "Column_names", "Stats_type", "Stats_val", "Last_update_version"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound", "Ndv"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsTopN:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Value", "Count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Table_name", "Partition_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowHistogramsInFlight:
		names = []string{"HistogramsInFlight"}
		ftypes = []byte{mysql.TypeLonglong}
	case ast.ShowColumnStatsUsage:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Last_used_at", "Last_analyzed_at"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime}
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
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation", "Source"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindingCacheStatus:
		names = []string{"bindings_in_cache", "bindings_in_table", "memory_usage", "memory_quota"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time", "End_time", "State", "Fail_reason", "Instance", "Process_ID"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowBuiltins:
		names = []string{"Supported_builtin_functions"}
		ftypes = []byte{mysql.TypeVarchar}
	case ast.ShowBackups, ast.ShowRestores:
		names = []string{"Destination", "State", "Progress", "Queue_time", "Execution_time", "Finish_time", "Connection", "Message"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDouble, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowPlacementLabels:
		names = []string{"Key", "Values"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeJSON}
	case ast.ShowPlacement, ast.ShowPlacementForDatabase, ast.ShowPlacementForTable, ast.ShowPlacementForPartition:
		names = []string{"Target", "Placement", "Scheduling_State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	}

	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: model.NewCIStr(names[i])})
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		col.RetType = fieldType
		schema.Append(col)
	}
	return
}

func (b *PlanBuilder) buildPlanReplayer(pc *ast.PlanReplayerStmt) Plan {
	p := &PlanReplayer{ExecStmt: pc.Stmt, Analyze: pc.Analyze, Load: pc.Load, File: pc.File}
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "File_token", mysql.TypeVarchar, 128))
	p.SetSchema(schema.col2Schema())
	p.names = schema.names
	return p
}

func buildChecksumTableSchema() (*expression.Schema, []*types.FieldName) {
	schema := newColumnsWithNames(5)
	schema.Append(buildColumnWithName("", "Db_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName("", "Table_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName("", "Checksum_crc64_xor", mysql.TypeLonglong, 22))
	schema.Append(buildColumnWithName("", "Total_kvs", mysql.TypeLonglong, 22))
	schema.Append(buildColumnWithName("", "Total_bytes", mysql.TypeLonglong, 22))
	return schema.col2Schema(), schema.names
}

// adjustOverlongViewColname adjusts the overlong outputNames of a view to
// `new_exp_$off` where `$off` is the offset of the output column, $off starts from 1.
// There is still some MySQL compatible problems.
func adjustOverlongViewColname(plan LogicalPlan) {
	outputNames := plan.OutputNames()
	for i := range outputNames {
		if outputName := outputNames[i].ColName.L; len(outputName) > mysql.MaxColumnNameLength {
			outputNames[i].ColName = model.NewCIStr(fmt.Sprintf("name_exp_%d", i+1))
		}
	}
}

// findStmtAsViewSchema finds the first SelectStmt as the schema for the view
func findStmtAsViewSchema(stmt ast.Node) *ast.SelectStmt {
	switch x := stmt.(type) {
	case *ast.CreateViewStmt:
		return findStmtAsViewSchema(x.Select)
	case *ast.SetOprStmt:
		return findStmtAsViewSchema(x.SelectList)
	case *ast.SetOprSelectList:
		return findStmtAsViewSchema(x.Selects[0])
	case *ast.SelectStmt:
		return x
	}
	return nil
}

// buildCompactTable builds a plan for the "ALTER TABLE [NAME] COMPACT ..." statement.
func (b *PlanBuilder) buildCompactTable(node *ast.CompactTableStmt) (Plan, error) {
	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		authErr = ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
			b.ctx.GetSessionVars().User.AuthHostname, node.Table.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, node.Table.Schema.L,
		node.Table.Name.L, "", authErr)

	tblInfo := node.Table.TableInfo
	p := &CompactTable{
		ReplicaKind: node.ReplicaKind,
		TableInfo:   tblInfo,
	}
	return p, nil
}
