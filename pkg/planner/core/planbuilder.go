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
	"context"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/domainmisc"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/oracle"
)

type visitInfo struct {
	privilege     mysql.PrivilegeType
	db            string
	table         string
	column        string
	err           error
	alterWritable bool
	// if multiple privileges is provided, user should
	// have at least one privilege to pass the check.
	dynamicPrivs     []string
	dynamicWithGrant bool
}

func (v *visitInfo) Equals(other *visitInfo) bool {
	return v.privilege == other.privilege &&
		v.db == other.db &&
		v.table == other.table &&
		v.column == other.column &&
		v.err == other.err &&
		v.alterWritable == other.alterWritable &&
		reflect.DeepEqual(v.dynamicPrivs, other.dynamicPrivs) &&
		v.dynamicWithGrant == other.dynamicWithGrant
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
	seedLP base.LogicalPlan
	// recurLP is the recursive part's logical plan.
	recurLP base.LogicalPlan
	// storageID for this CTE.
	storageID int
	// optFlag is the optFlag for the whole CTE.
	optFlag uint64
	// enterSubquery and recursiveRef are used to check "recursive table must be referenced only once, and not in any subquery".
	enterSubquery bool
	recursiveRef  bool
	limitLP       base.LogicalPlan
	// seedStat is shared between logicalCTE and logicalCTETable.
	seedStat *property.StatsInfo
	// The LogicalCTEs that reference the same table should share the same CteClass.
	cteClass *logicalop.CTEClass

	// isInline will determine whether it can be inlined when **CTE is used**
	isInline bool
	// forceInlineByHintOrVar will be true when CTE is hint by merge() or session variable "tidb_opt_force_inline_cte=true"
	forceInlineByHintOrVar bool
	// If CTE contain aggregation, window function, order by, distinct and limit in query (Indirect references to other cte containing those operator in the query are also counted.)
	containRecursiveForbiddenOperator bool
	// Compute in preprocess phase. Record how many consumers the current CTE has
	consumerCount int
}

type subQueryCtx = uint64

const (
	notHandlingSubquery subQueryCtx = iota
	handlingExistsSubquery
	handlingCompareSubquery
	handlingInSubquery
	handlingScalarSubquery
)

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          base.PlanContext
	is           infoschema.InfoSchema
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	outerCTEs    []*cteInfo
	// outerBlockExpand register current Expand OP for rollup syntax in every select query block.
	outerBlockExpand   []*logicalop.LogicalExpand
	currentBlockExpand *logicalop.LogicalExpand
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo     []visitInfo
	tableHintInfo []*hint.PlanHints
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
	//   If we meet a subquery or CTE, it's clearly that it's an independent problem so we just pop one map out when we
	//   finish building the subquery or CTE.
	handleHelper *handleColHelper

	hintProcessor *hint.QBHintHandler
	// qbOffset is the offsets of current processing select stmts.
	qbOffset []int

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
	// Check whether the current building query is a CTE
	isCTE bool
	// CTE table name in lower case, it can be nil
	nameMapCTE map[string]struct{}

	// subQueryCtx and subQueryHintFlags are for handling subquery related hints.
	// Note: "subquery" here only contains subqueries that are handled by the expression rewriter, i.e., [NOT] IN,
	// [NOT] EXISTS, compare + ANY/ALL and scalar subquery. Derived table doesn't belong to this.
	// We need these fields to passing information because:
	//   1. We know what kind of subquery is this only when we're in the outer query block.
	//   2. We know if there are such hints only when we're in the subquery block.
	//   3. These hints are only applicable when they are in a subquery block. And for some hints, they are only
	//     applicable for some kinds of subquery.
	//   4. If a hint is set and is applicable, the corresponding logic is handled in the outer query block.
	// Example SQL: select * from t where exists(select /*+ SEMI_JOIN_REWRITE() */ 1 from t1 where t.a=t1.a)

	// subQueryCtx indicates if we are handling a subquery, and what kind of subquery is it.
	subQueryCtx subQueryCtx
	// subQueryHintFlags stores subquery related hints that are set and applicable in the query block.
	// It's for returning information to buildSubquery().
	subQueryHintFlags uint64 // TODO: move this field to hint.PlanHints

	// disableSubQueryPreprocessing indicates whether to pre-process uncorrelated sub-queries in rewriting stage.
	disableSubQueryPreprocessing bool

	// noDecorrelate indicates whether decorrelation should be disabled for correlated aggregates in subqueries
	noDecorrelate bool

	// allowBuildCastArray indicates whether allow cast(... as ... array).
	allowBuildCastArray bool
	// resolveCtx is set when calling Build, it's only effective in the current Build call.
	resolveCtx *resolve.Context
}

type handleColHelper struct {
	id2HandleMapStack []map[int64][]util.HandleCols
	stackTail         int
}

func (hch *handleColHelper) resetForReuse() {
	*hch = handleColHelper{
		id2HandleMapStack: hch.id2HandleMapStack[:0],
	}
}

func (hch *handleColHelper) popMap() map[int64][]util.HandleCols {
	ret := hch.id2HandleMapStack[hch.stackTail-1]
	hch.stackTail--
	hch.id2HandleMapStack = hch.id2HandleMapStack[:hch.stackTail]
	return ret
}

func (hch *handleColHelper) pushMap(m map[int64][]util.HandleCols) {
	hch.id2HandleMapStack = append(hch.id2HandleMapStack, m)
	hch.stackTail++
}

func (hch *handleColHelper) mergeAndPush(m1, m2 map[int64][]util.HandleCols) {
	newMap := make(map[int64][]util.HandleCols, max(len(m1), len(m2)))
	for k, v := range m1 {
		newMap[k] = make([]util.HandleCols, len(v))
		copy(newMap[k], v)
	}
	for k, v := range m2 {
		if _, ok := newMap[k]; ok {
			newMap[k] = append(newMap[k], v...)
		} else {
			newMap[k] = make([]util.HandleCols, len(v))
			copy(newMap[k], v)
		}
	}
	hch.pushMap(newMap)
}

func (hch *handleColHelper) tailMap() map[int64][]util.HandleCols {
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
func GetDBTableInfo(visitInfo []visitInfo) []stmtctx.TableEntry {
	var tables []stmtctx.TableEntry
	existsFunc := func(tbls []stmtctx.TableEntry, tbl *stmtctx.TableEntry) bool {
		return slices.Contains(tbls, *tbl)
	}
	for _, v := range visitInfo {
		if v.db == "" && v.table == "" {
			// when v.db == "" and v.table == "", it means this visitInfo is for dynamic privilege,
			// so it is not related to any database or table.
			continue
		}

		tbl := &stmtctx.TableEntry{DB: v.db, Table: v.table}
		if !existsFunc(tables, tbl) {
			tables = append(tables, *tbl)
		}
	}
	return tables
}

// GetOptFlag gets the OptFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	if b.isSampling {
		// Disable logical optimization to avoid the optimizer
		// push down/eliminate operands like Selection, Limit or Sort.
		return 0
	}
	return b.optFlag
}

func (b *PlanBuilder) getSelectOffset() int {
	if len(b.qbOffset) > 0 {
		return b.qbOffset[len(b.qbOffset)-1]
	}
	return -1
}

func (b *PlanBuilder) pushSelectOffset(offset int) {
	b.qbOffset = append(b.qbOffset, offset)
}

func (b *PlanBuilder) popSelectOffset() {
	b.qbOffset = b.qbOffset[:len(b.qbOffset)-1]
}

// PlanBuilderOpt is used to adjust the plan builder.
type PlanBuilderOpt interface {
	Apply(builder *PlanBuilder)
}

// PlanBuilderOptNoExecution means the plan builder should not run any executor during Build().
type PlanBuilderOptNoExecution struct{}

// Apply implements the interface PlanBuilderOpt.
func (PlanBuilderOptNoExecution) Apply(builder *PlanBuilder) {
	builder.disableSubQueryPreprocessing = true
}

// PlanBuilderOptAllowCastArray means the plan builder should allow build cast(... as ... array).
type PlanBuilderOptAllowCastArray struct{}

// Apply implements the interface PlanBuilderOpt.
func (PlanBuilderOptAllowCastArray) Apply(builder *PlanBuilder) {
	builder.allowBuildCastArray = true
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(opts ...PlanBuilderOpt) *PlanBuilder {
	builder := &PlanBuilder{
		outerCTEs:           make([]*cteInfo, 0),
		colMapper:           make(map[*ast.ColumnNameExpr]int),
		handleHelper:        &handleColHelper{id2HandleMapStack: make([]map[int64][]util.HandleCols, 0)},
		correlatedAggMapper: make(map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn),
	}
	for _, opt := range opts {
		opt.Apply(builder)
	}
	return builder
}

// Init initialize a PlanBuilder.
// Return the original PlannerSelectBlockAsName as well, callers decide if
// PlannerSelectBlockAsName should be restored after using this builder.
// This is The comman code pattern to use it:
// NewPlanBuilder().Init(sctx, is, processor)
func (b *PlanBuilder) Init(sctx base.PlanContext, is infoschema.InfoSchema, processor *hint.QBHintHandler) (*PlanBuilder, []ast.HintTable) {
	savedBlockNames := sctx.GetSessionVars().PlannerSelectBlockAsName.Load()
	if processor == nil {
		sctx.GetSessionVars().PlannerSelectBlockAsName.Store(&[]ast.HintTable{})
	} else {
		newPlannerSelectBlockAsName := make([]ast.HintTable, processor.MaxSelectStmtOffset()+1)
		sctx.GetSessionVars().PlannerSelectBlockAsName.Store(&newPlannerSelectBlockAsName)
	}

	b.ctx = sctx
	b.is = is
	b.hintProcessor = processor
	b.isForUpdateRead = sctx.GetSessionVars().IsPessimisticReadConsistency()
	b.noDecorrelate = sctx.GetSessionVars().EnableNoDecorrelateInSelect
	if savedBlockNames == nil {
		return b, nil
	}
	return b, *savedBlockNames
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
	b.noDecorrelate = false

	// Add more fields if they are safe to be reused.

	return b
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node *resolve.NodeW) (base.Plan, error) {
	err := b.checkSEMStmt(node.Node)
	if err != nil {
		return nil, err
	}

	// Build might be called recursively, right now they all share the same resolve
	// context, so it's ok to override it.
	b.resolveCtx = node.GetResolveContext()
	b.optFlag |= rule.FlagPruneColumns
	switch x := node.Node.(type) {
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
	case *ast.ImportIntoStmt:
		return b.buildImportInto(ctx, x)
	case *ast.LoadDataStmt:
		return b.buildLoadData(ctx, x)
	case *ast.LoadStatsStmt:
		return b.buildLoadStats(x), nil
	case *ast.RefreshStatsStmt:
		return b.buildRefreshStats(x)
	case *ast.LockStatsStmt:
		return b.buildLockStats(x), nil
	case *ast.UnlockStatsStmt:
		return b.buildUnlockStats(x), nil
	case *ast.PlanReplayerStmt:
		return b.buildPlanReplayer(x), nil
	case *ast.TrafficStmt:
		return b.buildTraffic(x), nil
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
		*ast.BeginStmt, *ast.CommitStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.AlterRangeStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt,
		*ast.RenameUserStmt, *ast.NonTransactionalDMLStmt, *ast.SetSessionStatesStmt, *ast.SetResourceGroupStmt, *ast.CancelDistributionJobStmt,
		*ast.ImportIntoActionStmt, *ast.CalibrateResourceStmt, *ast.AddQueryWatchStmt, *ast.DropQueryWatchStmt, *ast.DropProcedureStmt:
		return b.buildSimple(ctx, node.Node.(ast.StmtNode))
	case ast.DDLNode:
		if b.ctx.IsCrossKS() {
			return nil, errors.New("DDL is not supported in cross keyspace session")
		}
		return b.buildDDL(ctx, x)
	case *ast.CreateBindingStmt:
		return b.buildCreateBindPlan(x)
	case *ast.DropBindingStmt:
		return b.buildDropBindPlan(x)
	case *ast.SetBindingStmt:
		return b.buildSetBindingStatusPlan(x)
	case *ast.SplitRegionStmt:
		return b.buildSplitRegion(x)
	case *ast.DistributeTableStmt:
		return b.buildDistributeTable(x)
	case *ast.CompactTableStmt:
		return b.buildCompactTable(x)
	case *ast.RecommendIndexStmt:
		return b.buildRecommendIndex(x)
	}
	return nil, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported type %T", node.Node)
}

func (b *PlanBuilder) buildSetConfig(ctx context.Context, v *ast.SetConfigStmt) (base.Plan, error) {
	privErr := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.ConfigPriv, "", "", "", privErr)
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	if _, ok := v.Value.(*ast.DefaultExpr); ok {
		return nil, errors.New("Unknown DEFAULT for SET CONFIG")
	}
	expr, _, err := b.rewrite(ctx, v.Value, mockTablePlan, nil, true)
	return &SetConfig{Name: v.Name, Type: v.Type, Instance: v.Instance, Value: expr}, err
}

func (b *PlanBuilder) buildExecute(ctx context.Context, v *ast.ExecuteStmt) (base.Plan, error) {
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		newExpr, _, err := b.rewrite(ctx, expr, nil, nil, true)
		if err != nil {
			return nil, err
		}
		vars = append(vars, newExpr)
	}

	prepStmt, err := GetPreparedStmt(v, b.ctx.GetSessionVars())
	if err != nil {
		return nil, err
	}
	exe := &Execute{Name: v.Name, Params: vars, PrepStmt: prepStmt}
	if v.BinaryArgs != nil {
		exe.Params = v.BinaryArgs.([]expression.Expression)
	}
	return exe, nil
}

func (b *PlanBuilder) buildDo(ctx context.Context, v *ast.DoStmt) (base.Plan, error) {
	var p base.LogicalPlan
	dual := logicalop.LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := logicalop.LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx, b.getSelectOffset())
	proj.SetOutputNames(make([]*types.FieldName, len(v.Exprs)))
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
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.GenWithStackByArgs(b.genCTETableNameForError())
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
			RetType:  expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		})
	}
	proj.SetChildren(p)
	proj.SetSelf(proj)
	proj.SetSchema(schema)
	proj.CalculateNoDelay = true
	return proj, nil
}

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (base.Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		if vars.IsGlobal || vars.IsInstance {
			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or SYSTEM_VARIABLES_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"SYSTEM_VARIABLES_ADMIN"}, false, err)
		}
		varName := strings.ToLower(vars.Name)
		if sem.IsEnabled() && sem.IsInvisibleSysVar(varName) {
			err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("RESTRICTED_VARIABLES_ADMIN")
			b.visitInfo = appendDynamicVisitInfo(b.visitInfo, []string{"RESTRICTED_VARIABLES_ADMIN"}, false, err)
		}
		if kerneltype.IsNextGen() && vardef.IsReadOnlyVarInNextGen(varName) {
			return nil, variable.ErrNotSupportedInNextGen.GenWithStackByArgs(fmt.Sprintf("setting %s", varName))
		}
		assign := &expression.VarAssignment{
			Name:       vars.Name,
			IsGlobal:   vars.IsGlobal,
			IsInstance: vars.IsInstance,
			IsSystem:   vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Convert column name expression to string value expression.
				char, col := b.ctx.GetSessionVars().GetCharsetInfo()
				vars.Value = ast.NewValueExpr(cn.Name.Name.O, char, col)
			}
			// The mocked plan need one output for the complex cases.
			// See the following IF branch.
			mockTablePlan := logicalop.LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
			var err error
			var possiblePlan base.LogicalPlan
			assign.Expr, possiblePlan, err = b.rewrite(ctx, vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
			// It's possible that the subquery of the SET_VAR is a complex one so we need to get the result by evaluating the plan.
			if _, ok := possiblePlan.(*logicalop.LogicalTableDual); !ok {
				physicalPlan, _, err := DoOptimize(ctx, b.ctx, b.optFlag, possiblePlan)
				if err != nil {
					return nil, err
				}
				row, err := EvalSubqueryFirstRow(ctx, physicalPlan, b.is, b.ctx)
				if err != nil {
					return nil, err
				}
				constant := &expression.Constant{
					Value:   row[0],
					RetType: assign.Expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
				}
				assign.Expr = constant
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

// detectAggInExprNode detects an aggregate function in its exprs.
func (*PlanBuilder) detectAggInExprNode(exprs []ast.ExprNode) bool {
	return slices.ContainsFunc(exprs, ast.HasAggFlag)
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (*PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
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

func (*PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
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

func getPathByIndexName(paths []*util.AccessPath, idxName ast.CIStr, tblInfo *model.TableInfo) *util.AccessPath {
	var indexPrefixPath *util.AccessPath
	prefixMatches := 0
	for _, path := range paths {
		// Only accept tikv's primary key table path.
		if path.IsTiKVTablePath() && isPrimaryIndex(idxName) && tblInfo.HasClusteredIndex() {
			return path
		}
		// If it's not a tikv table path and the index is nil, it could not be any index path.
		if path.Index == nil {
			continue
		}
		if path.Index.Name.L == idxName.L {
			return path
		}
		if strings.HasPrefix(path.Index.Name.L, idxName.L) {
			indexPrefixPath = path
			prefixMatches++
		}
	}

	// Return only unique prefix matches
	if prefixMatches == 1 {
		return indexPrefixPath
	}
	return nil
}

func isPrimaryIndex(indexName ast.CIStr) bool {
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

func isTiKVIndexByName(idxName ast.CIStr, indexInfo *model.IndexInfo, tblInfo *model.TableInfo) bool {
	// when the PKIsHandle of table is true, the primary key is not in the indices list.
	if idxName.L == "primary" && tblInfo.PKIsHandle {
		return true
	}
	return indexInfo != nil && !indexInfo.IsColumnarIndex()
}

func checkIndexLookUpPushDownSupported(ctx base.PlanContext, tblInfo *model.TableInfo, index *model.IndexInfo, suppressWarning bool) bool {
	unSupportedReason := ""
	sessionVars := ctx.GetSessionVars()
	if tblInfo.IsCommonHandle && tblInfo.CommonHandleVersion < 1 {
		unSupportedReason = "common handle table with old encoding version is not supported"
	} else if index.Global {
		unSupportedReason = "the global index in partition table is not supported"
	} else if tblInfo.TempTableType != model.TempTableNone {
		unSupportedReason = "temporary table is not supported"
	} else if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		unSupportedReason = "cached table is not supported"
	} else if index.MVIndex {
		unSupportedReason = "multi-valued index is not supported"
	} else if !sessionVars.IsIsolation(ast.RepeatableRead) {
		unSupportedReason = "transaction isolation level is not REPEATABLE-READ"
	} else if sessionVars.GetReplicaRead() != kv.ReplicaReadLeader {
		unSupportedReason = "only leader read is supported"
	} else if sessionVars.TxnCtx.IsStaleness {
		unSupportedReason = "stale read is not supported"
	} else if sessionVars.SnapshotTS != 0 {
		unSupportedReason = "historical read is not supported"
	}

	if unSupportedReason != "" {
		if !suppressWarning {
			ctx.GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf("hint INDEX_LOOKUP_PUSHDOWN is inapplicable, %s", unSupportedReason))
		}
		return false
	}
	return true
}

func checkAutoForceIndexLookUpPushDown(ctx base.PlanContext, tblInfo *model.TableInfo, index *model.IndexInfo) bool {
	policy := ctx.GetSessionVars().IndexLookUpPushDownPolicy
	switch policy {
	case vardef.IndexLookUpPushDownPolicyForce:
	case vardef.IndexLookUpPushDownPolicyAffinityForce:
		if tblInfo.Affinity == nil {
			// No affinity info, should not auto push down the index look up.
			return false
		}
	default:
		return false
	}
	return checkIndexLookUpPushDownSupported(ctx, tblInfo, index, true)
}

func getPossibleAccessPaths(ctx base.PlanContext, tableHints *hint.PlanHints, indexHints []*ast.IndexHint, tbl table.Table, dbName, tblName ast.CIStr, check bool, hasFlagPartitionProcessor bool) ([]*util.AccessPath, error) {
	tblInfo := tbl.Meta()
	publicPaths := make([]*util.AccessPath, 0, len(tblInfo.Indices)+2)
	tp := kv.TiKV
	if tbl.Type().IsClusterTable() {
		tp = kv.TiDB
	}
	tablePath := &util.AccessPath{StoreType: tp}
	fillContentForTablePath(tablePath, tblInfo)
	publicPaths = append(publicPaths, tablePath)

	// consider hypo TiFlash replicas
	isHypoTiFlashReplica := false
	if ctx.GetSessionVars().StmtCtx.InExplainStmt && ctx.GetSessionVars().HypoTiFlashReplicas != nil {
		hypoReplicas := ctx.GetSessionVars().HypoTiFlashReplicas
		originalTableName := tblInfo.Name.L
		if hypoReplicas[dbName.L] != nil {
			if _, ok := hypoReplicas[dbName.L][originalTableName]; ok {
				isHypoTiFlashReplica = true
			}
		}
	}

	if tblInfo.TiFlashReplica == nil {
		if isHypoTiFlashReplica {
			publicPaths = append(publicPaths, genTiFlashPath(tblInfo))
		} else {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there aren't tiflash replicas of table `" + tblInfo.Name.O + "`.")
		}
	} else if !tblInfo.TiFlashReplica.Available {
		if isHypoTiFlashReplica {
			publicPaths = append(publicPaths, genTiFlashPath(tblInfo))
		} else {
			ctx.GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because tiflash replicas of table `" + tblInfo.Name.O + "` not ready.")
		}
	} else {
		publicPaths = append(publicPaths, genTiFlashPath(tblInfo))
	}

	optimizerUseInvisibleIndexes := ctx.GetSessionVars().OptimizerUseInvisibleIndexes

	check = check || ctx.GetSessionVars().IsIsolation(ast.ReadCommitted)
	check = check && ctx.GetSessionVars().ConnectionID > 0
	var latestIndexes map[int64]*model.IndexInfo
	var err error
	// Inverted Index can not be used as access path index.
	invertedIndexes := make(map[string]struct{})

	// When NO_INDEX_LOOKUP_PUSHDOWN hint is specified, we should set `forceNoIndexLookUpPushDown = true` to avoid
	// using index look up push down even if other hint or system variable `tidb_index_lookup_pushdown_policy`
	// tries to enable it.
	forceNoIndexLookUpPushDown := false
	if len(tableHints.NoIndexLookUpPushDown) > 0 {
		for _, h := range tableHints.NoIndexLookUpPushDown {
			if h.Match(&hint.HintedTable{
				DBName:  dbName,
				TblName: tblName,
			}) {
				h.Matched = true
				forceNoIndexLookUpPushDown = true
				break
			}
		}
	}

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
				latestIndexes, check, err = domainmisc.GetLatestIndexInfo(ctx, tblInfo.ID, 0)
				if err != nil {
					return nil, err
				}
			}
			if check {
				if latestIndex, ok := latestIndexes[index.ID]; !ok || latestIndex.State != model.StatePublic {
					continue
				}
			}
			if index.InvertedInfo != nil {
				invertedIndexes[index.Name.L] = struct{}{}
				continue
			}
			if index.IsColumnarIndex() {
				// Because the value of `TiFlashReplica.Available` changes as the user modify replica, it is not ideal if the state of index changes accordingly.
				// So the current way to use the columnar indexes is to require the TiFlash Replica to be available.
				if !tblInfo.TiFlashReplica.Available {
					continue
				}
				path := genTiFlashPath(tblInfo)
				path.Index = index
				publicPaths = append(publicPaths, path)
				continue
			}
			path := &util.AccessPath{Index: index}
			if !forceNoIndexLookUpPushDown && checkAutoForceIndexLookUpPushDown(ctx, tblInfo, index) {
				path.IndexLookUpPushDownBy = util.IndexLookUpPushDownBySysVar
			}
			publicPaths = append(publicPaths, path)
		}
	}

	// consider hypo-indexes
	hypoIndexes := ctx.GetSessionVars().HypoIndexes // session level hypo-indexes
	if ctx.GetSessionVars().StmtCtx.InExplainStmt && hypoIndexes != nil {
		originalTableName := tblInfo.Name.L
		if hypoIndexes[dbName.L] != nil && hypoIndexes[dbName.L][originalTableName] != nil {
			for _, index := range hypoIndexes[dbName.L][originalTableName] {
				publicPaths = append(publicPaths, &util.AccessPath{Index: index})
			}
		}
	}
	if len(ctx.GetSessionVars().StmtCtx.HintedHypoIndexes) > 0 { // statement-level hypo-indexes from hints
		if !ctx.GetSessionVars().StmtCtx.InExplainStmt {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("can only use HYPO_INDEX hint in explain-statements"))
		} else {
			hintedHypoIndexes := ctx.GetSessionVars().StmtCtx.HintedHypoIndexes
			originalTableName := tblInfo.Name.L
			if hintedHypoIndexes[dbName.L] != nil && hintedHypoIndexes[dbName.L][originalTableName] != nil {
				for _, index := range hintedHypoIndexes[dbName.L][originalTableName] {
					publicPaths = append(publicPaths, &util.AccessPath{Index: index})
				}
			}
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*util.AccessPath, 0, len(publicPaths))
	ignored := make([]*util.AccessPath, 0, len(publicPaths))

	// Extract comment-style index hint like /*+ INDEX(t, idx1, idx2) */.
	indexHintsLen := len(indexHints)
	var indexLookUpPushDownHints map[int]struct{}
	if tableHints != nil {
		for i, hint := range tableHints.IndexHintList {
			if hint.Match(dbName, tblName) {
				indexHints = append(indexHints, hint.IndexHint)
				tableHints.IndexHintList[i].Matched = true
				if hint.ShouldPushDownIndexLookUp() {
					if indexLookUpPushDownHints == nil {
						indexLookUpPushDownHints = make(map[int]struct{}, 1)
					}
					indexLookUpPushDownHints[len(indexHints)-1] = struct{}{}
				}
			}
		}
	}

	_, isolationReadEnginesHasTiKV := ctx.GetSessionVars().GetIsolationReadEngines()[kv.TiKV]
	for i, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}

		hasScanHint = true

		// It is syntactically valid to omit index_list for USE INDEX, which means "use no indexes".
		// Omitting index_list for FORCE INDEX or IGNORE INDEX is a syntax error.
		// See https://dev.mysql.com/doc/refman/8.0/en/index-hints.html.
		if !isolationReadEnginesHasTiKV && hint.IndexNames == nil {
			continue
		}
		if hint.IndexNames == nil && hint.HintType != ast.HintIgnore {
			if path := getTablePath(publicPaths); path != nil {
				hasUseOrForce = true
				path.Forced = true
				available = append(available, path)
			}
		}

		for _, idxName := range hint.IndexNames {
			if _, ok := invertedIndexes[idxName.L]; ok {
				continue
			}
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				err := plannererrors.ErrKeyDoesNotExist.FastGenByArgs(idxName, tblInfo.Name)
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
			if isTiKVIndexByName(idxName, path.Index, tblInfo) && !isolationReadEnginesHasTiKV {
				engineVals, _ := ctx.GetSessionVars().GetSystemVar(vardef.TiDBIsolationReadEngines)
				err := fmt.Errorf("TiDB doesn't support index '%v' in the isolation read engines(value: '%v')", idxName, engineVals)
				if i < indexHintsLen {
					return nil, err
				}
				ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				continue
			}
			// Currently we don't distinguish between "FORCE" and "USE" because
			// our cost estimation is not reliable.
			hasUseOrForce = true
			path.Forced = true
			if hint.HintType == ast.HintOrderIndex {
				path.ForceKeepOrder = true
			}
			if hint.HintType == ast.HintNoOrderIndex {
				path.ForceNoKeepOrder = true
			}
			if i >= indexHintsLen {
				// Currently we only support to hint the index look up push down for comment-style sql hints.
				// So only i >= indexHintsLen may have the hints here.
				if _, ok := indexLookUpPushDownHints[i]; ok {
					if forceNoIndexLookUpPushDown {
						ctx.GetSessionVars().StmtCtx.SetHintWarning(
							"hint INDEX_LOOKUP_PUSHDOWN cannot be inapplicable, NO_INDEX_LOOKUP_PUSHDOWN is specified",
						)
						continue
					}

					if !checkIndexLookUpPushDownSupported(ctx, tblInfo, path.Index, false) {
						continue
					}
					path.IndexLookUpPushDownBy = util.IndexLookUpPushDownByHint
				}
			}
			available = append(available, path)
		}
	}

	if !hasScanHint || !hasUseOrForce {
		available = publicPaths
	}

	available = removeIgnoredPaths(available, ignored, tblInfo)

	// global index must not use partition pruning optimization, as LogicalPartitionAll not suitable for global index.
	// ignore global index if flagPartitionProcessor exists.
	if hasFlagPartitionProcessor {
		available = removeGlobalIndexPaths(available)
	}

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use table scan.
	if len(available) == 0 {
		available = append(available, tablePath)
	}

	// If all available paths are Multi-Valued Index, Partial index or other index that need to check its usability in later phase,
	// it's possible that all these path are inapplicable,
	// so that the table paths are still added here to avoid failing to find any physical plan.
	allUndeterminedPath := true
	for _, availablePath := range available {
		if !availablePath.IsUndetermined() {
			allUndeterminedPath = false
			break
		}
	}
	if allUndeterminedPath {
		available = append(available, tablePath)
	}

	return available, nil
}

func removeIgnoredPaths(paths, ignoredPaths []*util.AccessPath, tblInfo *model.TableInfo) []*util.AccessPath {
	if len(ignoredPaths) == 0 {
		return paths
	}
	remainedPaths := make([]*util.AccessPath, 0, len(paths))
	for _, path := range paths {
		if path.IsTiKVTablePath() || path.IsTiFlashSimpleTablePath() || getPathByIndexName(ignoredPaths, path.Index.Name, tblInfo) == nil {
			remainedPaths = append(remainedPaths, path)
		}
	}
	return remainedPaths
}

func removeGlobalIndexPaths(paths []*util.AccessPath) []*util.AccessPath {
	i := 0
	for _, path := range paths {
		if path.Index != nil && path.Index.Global {
			continue
		}
		paths[i] = path
		i++
	}
	return paths[:i]
}


// BuildHandleColsForAnalyze returns HandleCols for ANALYZE.

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
	schema.Append(buildColumnWithName("", "COMMENTS", mysql.TypeVarchar, 65535))
	return schema.col2Schema(), schema.names
}

func buildTableDistributionSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(14)
	schema.Append(buildColumnWithName("", "PARTITION_NAME", mysql.TypeVarchar, 4))
	schema.Append(buildColumnWithName("", "STORE_ID", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "STORE_TYPE", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "REGION_LEADER_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_PEER_COUNT", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_WRITE_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_WRITE_KEYS", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_WRITE_QUERY", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_LEADER_READ_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_LEADER_READ_KEYS", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_LEADER_READ_QUERY", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_PEER_READ_BYTES", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_PEER_READ_KEYS", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "REGION_PEER_READ_QUERY", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildTableRegionsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(13)
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
	schema.Append(buildColumnWithName("", "SCHEDULING_CONSTRAINTS", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "SCHEDULING_STATE", mysql.TypeVarchar, 16))
	return schema.col2Schema(), schema.names
}

func buildSplitRegionsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "TOTAL_SPLIT_REGION", mysql.TypeLonglong, 4))
	schema.Append(buildColumnWithName("", "SCATTER_FINISH_RATIO", mysql.TypeDouble, 8))
	return schema.col2Schema(), schema.names
}

func buildDistributeTableSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeLonglong, 4))
	return schema.col2Schema(), schema.names
}

func buildShowDDLJobQueriesFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "QUERY", mysql.TypeVarchar, 256))
	return schema.col2Schema(), schema.names
}

func buildShowDDLJobQueriesWithRangeFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeVarchar, 64))
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
	schema.Append(buildColumnWithName("", "SESSION_ALIAS", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

func buildCommandOnDDLJobsFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(2)
	schema.Append(buildColumnWithName("", "JOB_ID", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName("", "RESULT", mysql.TypeVarchar, 128))

	return schema.col2Schema(), schema.names
}

func buildCancelDDLJobsFields() (*expression.Schema, types.NameSlice) {
	return buildCommandOnDDLJobsFields()
}

func buildPauseDDLJobsFields() (*expression.Schema, types.NameSlice) {
	return buildCommandOnDDLJobsFields()
}

func buildResumeDDLJobsFields() (*expression.Schema, types.NameSlice) {
	return buildCommandOnDDLJobsFields()
}

func buildAdminShowBDRRoleFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "BDR_ROLE", mysql.TypeString, 1))
	return schema.col2Schema(), schema.names
}

func buildShowBackupMetaSchema() (*expression.Schema, types.NameSlice) {
	names := []string{"Database", "Table", "Total_kvs", "Total_bytes", "Time_range_start", "Time_range_end"}
	ftypes := []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDatetime, mysql.TypeDatetime}
	schema := newColumnsWithNames(len(names))
	for i := range names {
		fLen, _ := mysql.GetDefaultFieldLengthAndDecimal(ftypes[i])
		if ftypes[i] == mysql.TypeVarchar {
			// the default varchar length is `5`, which might be too short for us.
			fLen = 255
		}
		schema.Append(buildColumnWithName("", names[i], ftypes[i], fLen))
	}
	return schema.col2Schema(), schema.names
}

func buildShowBackupQuerySchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "Query", mysql.TypeVarchar, 4096))
	return schema.col2Schema(), schema.names
}

func buildBackupRestoreSchema(kind ast.BRIEKind) (*expression.Schema, types.NameSlice) {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	datetimeSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDatetime)

	schema := newColumnsWithNames(5)
	schema.Append(buildColumnWithName("", "Destination", mysql.TypeVarchar, 255))
	schema.Append(buildColumnWithName("", "Size", mysql.TypeLonglong, longlongSize))
	schema.Append(buildColumnWithName("", "BackupTS", mysql.TypeLonglong, longlongSize))
	if kind == ast.BRIEKindRestore {
		schema.Append(buildColumnWithName("", "Cluster TS", mysql.TypeLonglong, longlongSize))
	}
	schema.Append(buildColumnWithName("", "Queue Time", mysql.TypeDatetime, datetimeSize))
	schema.Append(buildColumnWithName("", "Execution Time", mysql.TypeDatetime, datetimeSize))
	return schema.col2Schema(), schema.names
}

func buildBRIESchema(kind ast.BRIEKind) (*expression.Schema, types.NameSlice) {
	switch kind {
	case ast.BRIEKindShowBackupMeta:
		return buildShowBackupMetaSchema()
	case ast.BRIEKindShowQuery:
		return buildShowBackupQuerySchema()
	case ast.BRIEKindBackup, ast.BRIEKindRestore:
		return buildBackupRestoreSchema(kind)
	default:
		s := newColumnsWithNames(0)
		return s.col2Schema(), s.names
	}
}

func buildCalibrateResourceSchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "QUOTA", mysql.TypeLonglong, longlongSize))

	return schema.col2Schema(), schema.names
}

func buildAddQueryWatchSchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	cols := newColumnsWithNames(1)
	cols.Append(buildColumnWithName("", "WATCH_ID", mysql.TypeLonglong, longlongSize))

	return cols.col2Schema(), cols.names
}

func buildShowTrafficJobsSchema() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(8)
	schema.Append(buildColumnWithName("", "START_TIME", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName("", "END_TIME", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName("", "INSTANCE", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "TYPE", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "PROGRESS", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "STATUS", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName("", "FAIL_REASON", mysql.TypeVarchar, 256))
	schema.Append(buildColumnWithName("", "PARAMS", mysql.TypeVarchar, 4096))

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
	}, &types.FieldName{DBName: metadef.InformationSchemaName, TblName: ast.NewCIStr(tableName), ColName: ast.NewCIStr(name)}
}

type columnsWithNames struct {
	cols  []*expression.Column
	names types.NameSlice
}

func newColumnsWithNames(_ int) *columnsWithNames {
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
	var flags []uint

	switch s.Tp {
	case ast.ShowBinlogStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowProcedureStatus, ast.ShowFunctionStatus:
		return buildShowProcedureSchema()
	case ast.ShowTriggers:
		return buildShowTriggerSchema()
	case ast.ShowEvents:
		return buildShowEventsSchema()
	case ast.ShowWarnings, ast.ShowErrors:
		if s.CountWarningsOrErrors {
			names = []string{"Count"}
			ftypes = []byte{mysql.TypeLong}
			break
		}
		return buildShowWarningsSchema()
	case ast.ShowRegions:
		return buildTableRegionsSchema()
	case ast.ShowDistributions:
		return buildTableDistributionSchema()
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowConfig:
		names = []string{"Type", "Instance", "Name", "Value"}
	case ast.ShowDatabases:
		fieldDB := "Database"
		if patternName := extractPatternLikeOrIlikeName(s.Pattern); patternName != "" {
			fieldDB = fmt.Sprintf("%s (%s)", fieldDB, patternName)
		}
		names = []string{fieldDB}
	case ast.ShowOpenTables:
		names = []string{"Database", "Table", "In_use", "Name_locked"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeLong}
	case ast.ShowTables:
		fieldTable := fmt.Sprintf("Tables_in_%s", s.DBName)
		if patternName := extractPatternLikeOrIlikeName(s.Pattern); patternName != "" {
			fieldTable = fmt.Sprintf("%s (%s)", fieldTable, patternName)
		}
		names = []string{fieldTable}
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
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
		flags = []uint{0, 0, mysql.UnsignedFlag | mysql.NotNullFlag, 0, 0, 0, 0}
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
	case ast.ShowCreateResourceGroup:
		names = []string{"Resource_Group", "Create Resource Group"}
	case ast.ShowCreateUser:
		if s.User != nil {
			names = []string{fmt.Sprintf("CREATE USER for %s", s.User)}
		}
	case ast.ShowCreateView:
		names = []string{"View", "Create View", "character_set_client", "collation_connection"}
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
			"Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression", "Clustered", "Global"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
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
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count", "Last_analyze_time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDatetime}
	case ast.ShowStatsExtended:
		names = []string{"Db_name", "Table_name", "Stats_name", "Column_names", "Stats_type", "Stats_val", "Last_update_version"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation",
			"Load_status", "Total_mem_usage", "Hist_mem_usage", "Topn_mem_usage", "Cms_mem_usage"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble,
			mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong}
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
	case ast.ShowStatsLocked:
		names = []string{"Db_name", "Table_name", "Partition_name", "Status"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
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
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation", "Source", "Sql_digest", "Plan_digest"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindingCacheStatus:
		names = []string{"bindings_in_cache", "bindings_in_table", "memory_usage", "memory_quota"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time",
			"End_time", "State", "Fail_reason", "Instance", "Process_ID", "Remaining_seconds", "Progress", "Estimated_total_rows"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeDouble, mysql.TypeLonglong}
	case ast.ShowBuiltins:
		names = []string{"Supported_builtin_functions"}
		ftypes = []byte{mysql.TypeVarchar}
	case ast.ShowBackups, ast.ShowRestores:
		names = []string{"Id", "Destination", "State", "Progress", "Queue_time", "Execution_time", "Finish_time", "Connection", "Message"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDouble, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowPlacementLabels:
		names = []string{"Key", "Values"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeJSON}
	case ast.ShowPlacement, ast.ShowPlacementForDatabase, ast.ShowPlacementForTable, ast.ShowPlacementForPartition:
		names = []string{"Target", "Placement", "Scheduling_State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowSessionStates:
		names = []string{"Session_states", "Session_token"}
		ftypes = []byte{mysql.TypeJSON, mysql.TypeJSON}
	case ast.ShowImportJobs:
		names = importIntoSchemaNames
		ftypes = ImportIntoSchemaFTypes
	case ast.ShowImportGroups:
		names = showImportGroupsNames
		ftypes = showImportGroupsFTypes
	case ast.ShowDistributionJobs:
		names = distributionJobsSchemaNames
		ftypes = distributionJobsSchedulerFTypes
	case ast.ShowAffinity:
		names = []string{"Db_name", "Table_name", "Partition_name", "Leader_store_id", "Voter_store_ids", "Status", "Region_count", "Affinity_region_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong}
	}
	return convert2OutputSchemasAndNames(names, ftypes, flags)
}

func convert2OutputSchemasAndNames(names []string, ftypes []byte, flags []uint) (schema *expression.Schema, outputNames []*types.FieldName) {
	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: ast.NewCIStr(names[i])})
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
		if len(flags) > 0 {
			fieldType.SetFlag(flags[i])
		}
		col.RetType = fieldType
		schema.Append(col)
	}
	return
}

func (b *PlanBuilder) buildPlanReplayer(pc *ast.PlanReplayerStmt) base.Plan {
	p := &PlanReplayer{ExecStmt: pc.Stmt, StmtList: pc.StmtList, Analyze: pc.Analyze, Load: pc.Load, File: pc.File,
		Capture: pc.Capture, Remove: pc.Remove, SQLDigest: pc.SQLDigest, PlanDigest: pc.PlanDigest}

	if pc.HistoricalStatsInfo != nil {
		p.HistoricalStatsTS = calcTSForPlanReplayer(b.ctx, pc.HistoricalStatsInfo.TsExpr)
	}

	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "File_token", mysql.TypeVarchar, 128))
	p.SetSchema(schema.col2Schema())
	p.SetOutputNames(schema.names)
	return p
}

func calcTSForPlanReplayer(sctx base.PlanContext, tsExpr ast.ExprNode) uint64 {
	tsVal, err := evalAstExprWithPlanCtx(sctx, tsExpr)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	// mustn't be NULL
	if tsVal.IsNull() {
		return 0
	}

	// first, treat it as a TSO
	tpLonglong := types.NewFieldType(mysql.TypeLonglong)
	tpLonglong.SetFlag(mysql.UnsignedFlag)
	// We need a strict check, which means no truncate or any other warnings/errors, or it will wrongly try to parse
	// a date/time string into a TSO.
	// To achieve this, we create a new type context without re-using the one in statement context.
	tso, err := tsVal.ConvertTo(types.DefaultStmtNoWarningContext.WithLocation(sctx.GetSessionVars().Location()), tpLonglong)
	if err == nil {
		return tso.GetUint64()
	}

	// if failed, treat it as a date/time
	// this part is similar to CalculateAsOfTsExpr
	tpDateTime := types.NewFieldType(mysql.TypeDatetime)
	tpDateTime.SetDecimal(6)
	timestamp, err := tsVal.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), tpDateTime)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	goTime, err := timestamp.GetMysqlTime().GoTime(sctx.GetSessionVars().Location())
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	return oracle.GoTimeToTS(goTime)
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
func adjustOverlongViewColname(plan base.LogicalPlan) {
	outputNames := plan.OutputNames()
	for i := range outputNames {
		if outputName := outputNames[i].ColName.L; len(outputName) > mysql.MaxColumnNameLength {
			outputNames[i].ColName = ast.NewCIStr(fmt.Sprintf("name_exp_%d", i+1))
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

func (b *PlanBuilder) buildTraffic(pc *ast.TrafficStmt) base.Plan {
	tf := &Traffic{
		OpType:  pc.OpType,
		Options: pc.Options,
		Dir:     pc.Dir,
	}
	var errMsg string
	var privs []string
	switch pc.OpType {
	case ast.TrafficOpCapture:
		errMsg = "SUPER or TRAFFIC_CAPTURE_ADMIN"
		privs = []string{"TRAFFIC_CAPTURE_ADMIN"}
	case ast.TrafficOpReplay:
		errMsg = "SUPER or TRAFFIC_REPLAY_ADMIN"
		privs = []string{"TRAFFIC_REPLAY_ADMIN"}
	case ast.TrafficOpShow:
		tf.SetSchemaAndNames(buildShowTrafficJobsSchema())
		fallthrough
	case ast.TrafficOpCancel:
		errMsg = "SUPER, TRAFFIC_CAPTURE_ADMIN or TRAFFIC_REPLAY_ADMIN"
		privs = []string{"TRAFFIC_CAPTURE_ADMIN", "TRAFFIC_REPLAY_ADMIN"}
	}
	err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(errMsg)
	b.visitInfo = appendDynamicVisitInfo(b.visitInfo, privs, false, err)
	return tf
}

// buildCompactTable builds a plan for the "ALTER TABLE [NAME] COMPACT ..." statement.
func (b *PlanBuilder) buildCompactTable(node *ast.CompactTableStmt) (base.Plan, error) {
	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
			b.ctx.GetSessionVars().User.AuthHostname, node.Table.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, node.Table.Schema.L,
		node.Table.Name.L, "", authErr)

	tnW := b.resolveCtx.GetTableName(node.Table)
	tblInfo := tnW.TableInfo
	p := &CompactTable{
		ReplicaKind:    node.ReplicaKind,
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	return p, nil
}

func (*PlanBuilder) buildRecommendIndex(v *ast.RecommendIndexStmt) (base.Plan, error) {
	p := &RecommendIndexPlan{
		Action:   v.Action,
		SQL:      v.SQL,
		AdviseID: v.ID,
		Options:  v.Options,
	}

	switch v.Action {
	case "run":
		schema := newColumnsWithNames(7)
		schema.Append(buildColumnWithName("", "database", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "table", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "index_name", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "index_columns", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "est_index_size", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "reason", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "top_impacted_query", mysql.TypeBlob, -1))
		schema.Append(buildColumnWithName("", "create_index_statement", mysql.TypeBlob, -1))
		p.SetSchemaAndNames(schema.col2Schema(), schema.names)
	case "set":
		if len(p.Options) == 0 {
			return nil, fmt.Errorf("option is empty")
		}
	case "show":
		schema := newColumnsWithNames(3)
		schema.Append(buildColumnWithName("", "option", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "value", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "description", mysql.TypeVarchar, 256))
		p.SetSchemaAndNames(schema.col2Schema(), schema.names)
	default:
		return nil, fmt.Errorf("unsupported action %s", v.Action)
	}
	return p, nil
}

func extractPatternLikeOrIlikeName(patternLike *ast.PatternLikeOrIlikeExpr) string {
	if patternLike == nil {
		return ""
	}
	if v, ok := patternLike.Pattern.(*driver.ValueExpr); ok {
		return v.GetString()
	}
	return ""
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath() {
			return path
		}
	}
	return nil
}

func (b *PlanBuilder) buildAdminAlterDDLJob(ctx context.Context, as *ast.AdminStmt) (_ base.Plan, err error) {
	options := make([]*AlterDDLJobOpt, 0, len(as.AlterJobOptions))
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	for _, opt := range as.AlterJobOptions {
		_, ok := allowedAlterDDLJobParams[opt.Name]
		if !ok {
			return nil, fmt.Errorf("unsupported admin alter ddl jobs config: %s", opt.Name)
		}
		alterDDLJobOpt := AlterDDLJobOpt{Name: opt.Name}
		if opt.Value != nil {
			alterDDLJobOpt.Value, _, err = b.rewrite(ctx, opt.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		}
		if err = checkAlterDDLJobOptValue(&alterDDLJobOpt); err != nil {
			return nil, err
		}
		options = append(options, &alterDDLJobOpt)
	}
	p := &AlterDDLJob{
		JobID:   as.JobNumber,
		Options: options,
	}
	return p, nil
}

// check if the config value is valid.
func checkAlterDDLJobOptValue(opt *AlterDDLJobOpt) error {
	switch opt.Name {
	case AlterDDLJobThread:
		thread, err := GetThreadOrBatchSizeFromExpression(opt)
		if err != nil {
			return err
		}
		if thread < 1 || thread > vardef.MaxConfigurableConcurrency {
			return fmt.Errorf("the value %v for %s is out of range [1, %v]",
				thread, opt.Name, vardef.MaxConfigurableConcurrency)
		}
	case AlterDDLJobBatchSize:
		batchSize, err := GetThreadOrBatchSizeFromExpression(opt)
		if err != nil {
			return err
		}
		bs := int32(batchSize)
		if bs < vardef.MinDDLReorgBatchSize || bs > vardef.MaxDDLReorgBatchSize {
			return fmt.Errorf("the value %v for %s is out of range [%v, %v]",
				bs, opt.Name, vardef.MinDDLReorgBatchSize, vardef.MaxDDLReorgBatchSize)
		}
	case AlterDDLJobMaxWriteSpeed:
		speed, err := GetMaxWriteSpeedFromExpression(opt)
		if err != nil {
			return err
		}
		if speed < 0 || speed > units.PiB {
			return fmt.Errorf("the value %s for %s is out of range [%v, %v]",
				strconv.FormatInt(speed, 10), opt.Name, 0, units.PiB)
		}
	}
	return nil
}

// for nextgen import-into with SEM, we disallow user to set S3 external ID explicitly,
// and we will use the keyspace name as the S3 external ID.
func checkNextGenS3PathWithSem(u *url.URL) error {
	values := u.Query()
	for k := range values {
		lowerK := strings.ToLower(k)
		if lowerK == s3like.S3ExternalID {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO with explicit external ID")
		}
	}

	return nil
}

// GetThreadOrBatchSizeFromExpression gets the numeric value of the thread or batch size from the expression.
func GetThreadOrBatchSizeFromExpression(opt *AlterDDLJobOpt) (int64, error) {
	v := opt.Value.(*expression.Constant)
	switch v.RetType.EvalType() {
	case types.ETInt:
		return v.Value.GetInt64(), nil
	default:
		return 0, fmt.Errorf("the value for %s is invalid, only integer is allowed", opt.Name)
	}
}

// GetMaxWriteSpeedFromExpression gets the numeric value of the max write speed from the expression.
func GetMaxWriteSpeedFromExpression(opt *AlterDDLJobOpt) (maxWriteSpeed int64, err error) {
	v := opt.Value.(*expression.Constant)
	switch v.RetType.EvalType() {
	case types.ETString:
		speedStr := v.Value.GetString()
		maxWriteSpeed, err = units.RAMInBytes(speedStr)
		if err != nil {
			return 0, errors.Annotate(err, "parse max_write_speed value error")
		}
	case types.ETInt:
		maxWriteSpeed = v.Value.GetInt64()
	default:
		return 0, fmt.Errorf("the value %v for %s is invalid", v.Value.GetValue(), opt.Name)
	}
	return maxWriteSpeed, nil
}

func (b *PlanBuilder) checkSEMStmt(stmt ast.Node) error {
	if !semv2.IsEnabled() {
		return nil
	}

	stmtNode, ok := stmt.(ast.StmtNode)
	intest.Assert(ok, "node.Node should be ast.StmtNode, but got %T", stmt)
	if !semv2.IsRestrictedSQL(stmtNode) {
		return nil
	}

	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	checker := privilege.GetPrivilegeManager(b.ctx)
	hasPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_SQL_ADMIN", false)

	if hasPriv {
		return nil
	}

	return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs(stmtNode.Text())
}
