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
	"cmp"
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lock"
	tablelock "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (base.Plan, types.NameSlice, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

// IsReadOnly check whether the ast.Node is a read only statement.
var IsReadOnly func(node ast.Node, vars *variable.SessionVars) bool

// Note: The order of flags is same as the order of optRule in the list.
// Do not mess up the order.
const (
	flagGcSubstitute uint64 = 1 << iota
	flagPrunColumns
	flagStabilizeResults
	flagBuildKeyInfo
	flagDecorrelate
	flagSemiJoinRewrite
	flagEliminateAgg
	flagSkewDistinctAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagConstantPropagation
	flagConvertOuterToInnerJoin
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagCollectPredicateColumnsPoint
	flagPushDownAgg
	flagDeriveTopNFromWindow
	flagPredicateSimplification
	flagPushDownTopN
	flagSyncWaitStatsLoadPoint
	flagJoinReOrder
	flagPrunColumnsAgain
	flagPushDownSequence
	flagResolveExpand
)

var optRuleList = []logicalOptRule{
	&gcSubstituter{},
	&columnPruner{},
	&resultReorder{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&semiJoinRewriter{},
	&aggregationEliminator{},
	&skewDistinctAggRewriter{},
	&projectionEliminator{},
	&maxMinEliminator{},
	&constantPropagationSolver{},
	&convertOuterToInnerJoin{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&collectPredicateColumnsPoint{},
	&aggregationPushDownSolver{},
	&deriveTopNFromWindow{},
	&predicateSimplification{},
	&pushDownTopNOptimizer{},
	&syncWaitStatsLoadPoint{},
	&joinReOrderSolver{},
	&columnPruner{}, // column pruning again at last, note it will mess up the results of buildKeySolver
	&pushDownSequenceSolver{},
	&resolveExpand{},
}

// Interaction Rule List
/* The interaction rule will be trigger when it satisfies following conditions:
1. The related rule has been trigger and changed the plan
2. The interaction rule is enabled
*/
var optInteractionRuleList = map[logicalOptRule]logicalOptRule{}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	/* Return Parameters:
	1. base.LogicalPlan: The optimized base.LogicalPlan after rule is applied
	2. bool: Used to judge whether the plan is changed or not by logical rule.
		 If the plan is changed, it will return true.
		 The default value is false. It means that no interaction rule will be triggered.
	3. error: If there is error during the rule optimizer, it will be thrown
	*/
	optimize(context.Context, base.LogicalPlan, *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error)
	name() string
}

// BuildLogicalPlanForTest builds a logical plan for testing purpose from ast.Node.
func BuildLogicalPlanForTest(ctx context.Context, sctx sessionctx.Context, node ast.Node, infoSchema infoschema.InfoSchema) (base.Plan, error) {
	sctx.GetSessionVars().PlanID.Store(0)
	sctx.GetSessionVars().PlanColumnID.Store(0)
	builder, _ := NewPlanBuilder().Init(sctx.GetPlanCtx(), infoSchema, utilhint.NewQBHintHandler(nil))
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	if logic, ok := p.(base.LogicalPlan); ok {
		RecheckCTE(logic)
	}
	return p, err
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		if v.privilege == mysql.ExtendedPriv {
			if !pm.RequestDynamicVerification(activeRoles, v.dynamicPriv, v.dynamicWithGrant) {
				if v.err == nil {
					return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(v.dynamicPriv)
				}
				return v.err
			}
		} else if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			if v.err == nil {
				return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(v.privilege.String())
			}
			return v.err
		}
	}
	return nil
}

// VisitInfo4PrivCheck generates privilege check infos because privilege check of local temporary tables is different
// with normal tables. `CREATE` statement needs `CREATE TEMPORARY TABLE` privilege from the database, and subsequent
// statements do not need any privileges.
func VisitInfo4PrivCheck(is infoschema.InfoSchema, node ast.Node, vs []visitInfo) (privVisitInfo []visitInfo) {
	if node == nil {
		return vs
	}

	switch stmt := node.(type) {
	case *ast.CreateTableStmt:
		privVisitInfo = make([]visitInfo, 0, len(vs))
		for _, v := range vs {
			if v.privilege == mysql.CreatePriv {
				if stmt.TemporaryKeyword == ast.TemporaryLocal {
					// `CREATE TEMPORARY TABLE` privilege is required from the database, not the table.
					newVisitInfo := v
					newVisitInfo.privilege = mysql.CreateTMPTablePriv
					newVisitInfo.table = ""
					privVisitInfo = append(privVisitInfo, newVisitInfo)
				} else {
					// If both the normal table and temporary table already exist, we need to check the privilege.
					privVisitInfo = append(privVisitInfo, v)
				}
			} else {
				// `CREATE TABLE LIKE tmp` or `CREATE TABLE FROM SELECT tmp` in the future.
				if needCheckTmpTablePriv(is, v) {
					privVisitInfo = append(privVisitInfo, v)
				}
			}
		}
	case *ast.DropTableStmt:
		// Dropping a local temporary table doesn't need any privileges.
		if stmt.IsView {
			privVisitInfo = vs
		} else {
			privVisitInfo = make([]visitInfo, 0, len(vs))
			if stmt.TemporaryKeyword != ast.TemporaryLocal {
				for _, v := range vs {
					if needCheckTmpTablePriv(is, v) {
						privVisitInfo = append(privVisitInfo, v)
					}
				}
			}
		}
	case *ast.GrantStmt, *ast.DropSequenceStmt, *ast.DropPlacementPolicyStmt:
		// Some statements ignore local temporary tables, so they should check the privileges on normal tables.
		privVisitInfo = vs
	default:
		privVisitInfo = make([]visitInfo, 0, len(vs))
		for _, v := range vs {
			if needCheckTmpTablePriv(is, v) {
				privVisitInfo = append(privVisitInfo, v)
			}
		}
	}
	return
}

func needCheckTmpTablePriv(is infoschema.InfoSchema, v visitInfo) bool {
	if v.db != "" && v.table != "" {
		// Other statements on local temporary tables except `CREATE` do not check any privileges.
		tb, err := is.TableByName(model.NewCIStr(v.db), model.NewCIStr(v.table))
		// If the table doesn't exist, we do not report errors to avoid leaking the existence of the table.
		if err == nil && tb.Meta().TempTableType == model.TempTableLocal {
			return false
		}
	}
	return true
}

// CheckTableLock checks the table lock.
func CheckTableLock(ctx tablelock.TableLockReadContext, is infoschema.InfoSchema, vs []visitInfo) error {
	if !config.TableLockEnabled() {
		return nil
	}

	checker := lock.NewChecker(ctx, is)
	for i := range vs {
		err := checker.CheckTableLock(vs[i].db, vs[i].table, vs[i].privilege, vs[i].alterWritable)
		// if table with lock-write table dropped, we can access other table, such as `rename` operation
		if err == lock.ErrLockedTableDropped {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func checkStableResultMode(sctx base.PlanContext) bool {
	s := sctx.GetSessionVars()
	st := s.StmtCtx
	return s.EnableStableResultMode && (!st.InInsertStmt && !st.InUpdateStmt && !st.InDeleteStmt && !st.InLoadDataStmt)
}

// doOptimize optimizes a logical plan into a physical plan,
// while also returning the optimized logical plan, the final physical plan, and the cost of the final plan.
// The returned logical plan is necessary for generating plans for Common Table Expressions (CTEs).
func doOptimize(
	ctx context.Context,
	sctx base.PlanContext,
	flag uint64,
	logic base.LogicalPlan,
) (base.LogicalPlan, base.PhysicalPlan, float64, error) {
	sessVars := sctx.GetSessionVars()
	flag = adjustOptimizationFlags(flag, logic)
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, nil, 0, err
	}

	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, nil, 0, errors.Trace(plannererrors.ErrCartesianProductUnsupported)
	}
	planCounter := base.PlanCounterTp(sessVars.StmtCtx.StmtHints.ForceNthPlan)
	if planCounter == 0 {
		planCounter = -1
	}
	physical, cost, err := physicalOptimize(logic, &planCounter)
	if err != nil {
		return nil, nil, 0, err
	}
	finalPlan := postOptimize(ctx, sctx, physical)

	if sessVars.StmtCtx.EnableOptimizerCETrace {
		refineCETrace(sctx)
	}
	if sessVars.StmtCtx.EnableOptimizeTrace {
		sessVars.StmtCtx.OptimizeTracer.RecordFinalPlan(finalPlan.BuildPlanTrace())
	}
	return logic, finalPlan, cost, nil
}

func adjustOptimizationFlags(flag uint64, logic base.LogicalPlan) uint64 {
	// If there is something after flagPrunColumns, do flagPrunColumnsAgain.
	if flag&flagPrunColumns > 0 && flag-flagPrunColumns > flagPrunColumns {
		flag |= flagPrunColumnsAgain
	}
	if checkStableResultMode(logic.SCtx()) {
		flag |= flagStabilizeResults
	}
	if logic.SCtx().GetSessionVars().StmtCtx.StraightJoinOrder {
		// When we use the straight Join Order hint, we should disable the join reorder optimization.
		flag &= ^flagJoinReOrder
	}
	flag |= flagCollectPredicateColumnsPoint
	flag |= flagSyncWaitStatsLoadPoint
	if !logic.SCtx().GetSessionVars().StmtCtx.UseDynamicPruneMode {
		flag |= flagPartitionProcessor // apply partition pruning under static mode
	}
	return flag
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(
	ctx context.Context,
	sctx base.PlanContext,
	flag uint64,
	logic base.LogicalPlan,
) (base.PhysicalPlan, float64, error) {
	sessVars := sctx.GetSessionVars()
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	_, finalPlan, cost, err := doOptimize(ctx, sctx, flag, logic)
	return finalPlan, cost, err
}

// refineCETrace will adjust the content of CETrace.
// Currently, it will (1) deduplicate trace records, (2) sort the trace records (to make it easier in the tests) and (3) fill in the table name.
func refineCETrace(sctx base.PlanContext) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	stmtCtx.OptimizerCETrace = tracing.DedupCETrace(stmtCtx.OptimizerCETrace)
	slices.SortFunc(stmtCtx.OptimizerCETrace, func(i, j *tracing.CETraceRecord) int {
		if i == nil && j != nil {
			return -1
		}
		if i == nil || j == nil {
			return 1
		}

		if c := cmp.Compare(i.TableID, j.TableID); c != 0 {
			return c
		}
		if c := cmp.Compare(i.Type, j.Type); c != 0 {
			return c
		}
		if c := cmp.Compare(i.Expr, j.Expr); c != 0 {
			return c
		}
		return cmp.Compare(i.RowCount, j.RowCount)
	})
	traceRecords := stmtCtx.OptimizerCETrace
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	for _, rec := range traceRecords {
		tbl, _ := infoschema.FindTableByTblOrPartID(is, rec.TableID)
		if tbl != nil {
			rec.TableName = tbl.Meta().Name.O
			continue
		}
		logutil.BgLogger().Warn("Failed to find table in infoschema", zap.String("category", "OptimizerTrace"),
			zap.Int64("table id", rec.TableID))
	}
}

// mergeContinuousSelections merge continuous selections which may occur after changing plans.
func mergeContinuousSelections(p base.PhysicalPlan) {
	if sel, ok := p.(*PhysicalSelection); ok {
		for {
			childSel := sel.children[0]
			tmp, ok := childSel.(*PhysicalSelection)
			if !ok {
				break
			}
			sel.Conditions = append(sel.Conditions, tmp.Conditions...)
			sel.SetChild(0, tmp.children[0])
		}
	}
	for _, child := range p.Children() {
		mergeContinuousSelections(child)
	}
	// merge continuous selections in a coprocessor task of tiflash
	tableReader, isTableReader := p.(*PhysicalTableReader)
	if isTableReader && tableReader.StoreType == kv.TiFlash {
		mergeContinuousSelections(tableReader.tablePlan)
		tableReader.TablePlans = flattenPushDownPlan(tableReader.tablePlan)
	}
}

func postOptimize(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan) base.PhysicalPlan {
	// some cases from update optimize will require avoiding projection elimination.
	// see comments ahead of call of DoOptimize in function of buildUpdate().
	plan = eliminatePhysicalProjection(plan)
	plan = InjectExtraProjection(plan)
	mergeContinuousSelections(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	plan = enableParallelApply(sctx, plan)
	handleFineGrainedShuffle(ctx, sctx, plan)
	propagateProbeParents(plan, nil)
	countStarRewrite(plan)
	disableReuseChunkIfNeeded(sctx, plan)
	tryEnableLateMaterialization(sctx, plan)
	generateRuntimeFilter(sctx, plan)
	return plan
}

func generateRuntimeFilter(sctx base.PlanContext, plan base.PhysicalPlan) {
	if !sctx.GetSessionVars().IsRuntimeFilterEnabled() || sctx.GetSessionVars().InRestrictedSQL {
		return
	}
	logutil.BgLogger().Debug("Start runtime filter generator")
	rfGenerator := &RuntimeFilterGenerator{
		rfIDGenerator:      &util.IDGenerator{},
		columnUniqueIDToRF: map[int64][]*RuntimeFilter{},
		parentPhysicalPlan: plan,
	}
	startRFGenerator := time.Now()
	rfGenerator.GenerateRuntimeFilter(plan)
	logutil.BgLogger().Debug("Finish runtime filter generator",
		zap.Duration("Cost", time.Since(startRFGenerator)))
}

// tryEnableLateMaterialization tries to push down some filter conditions to the table scan operator
// @brief: push down some filter conditions to the table scan operator
// @param: sctx: session context
// @param: plan: the physical plan to be pruned
// @note: this optimization is only applied when the TiFlash is used.
// @note: the following conditions should be satisfied:
//   - Only the filter conditions with high selectivity should be pushed down.
//   - The filter conditions which contain heavy cost functions should not be pushed down.
//   - Filter conditions that apply to the same column are either pushed down or not pushed down at all.
func tryEnableLateMaterialization(sctx base.PlanContext, plan base.PhysicalPlan) {
	// check if EnableLateMaterialization is set
	if sctx.GetSessionVars().EnableLateMaterialization && !sctx.GetSessionVars().TiFlashFastScan {
		predicatePushDownToTableScan(sctx, plan)
	}
	if sctx.GetSessionVars().EnableLateMaterialization && sctx.GetSessionVars().TiFlashFastScan {
		sc := sctx.GetSessionVars().StmtCtx
		sc.AppendWarning(errors.NewNoStackError("FastScan is not compatible with late materialization, late materialization is disabled"))
	}
}

/*
*
The countStarRewriter is used to rewrite

	count(*) -> count(not null column)

**Only for TiFlash**
Attention:
Since count(*) is directly translated into count(1) during grammar parsing,
the rewritten pattern actually matches count(constant)

Pattern:
PhysicalAggregation: count(constant)

	    |
	TableFullScan: TiFlash

Optimize:
Table

	<k1 bool not null, k2 int null, k3 bigint not null>

Query: select count(*) from table
ColumnPruningRule: datasource pick row_id
countStarRewrite: datasource pick k1 instead of row_id

	rewrite count(*) -> count(k1)

Rewritten Query: select count(k1) from table
*/
func countStarRewrite(plan base.PhysicalPlan) {
	countStarRewriteInternal(plan)
	if tableReader, ok := plan.(*PhysicalTableReader); ok {
		countStarRewrite(tableReader.tablePlan)
	} else {
		for _, child := range plan.Children() {
			countStarRewrite(child)
		}
	}
}

func countStarRewriteInternal(plan base.PhysicalPlan) {
	// match pattern any agg(count(constant)) -> tablefullscan(tiflash)
	var physicalAgg *basePhysicalAgg
	switch x := plan.(type) {
	case *PhysicalHashAgg:
		physicalAgg = x.getPointer()
	case *PhysicalStreamAgg:
		physicalAgg = x.getPointer()
	default:
		return
	}
	if len(physicalAgg.GroupByItems) > 0 || len(physicalAgg.children) != 1 {
		return
	}
	for _, aggFunc := range physicalAgg.AggFuncs {
		if aggFunc.Name != "count" || len(aggFunc.Args) != 1 || aggFunc.HasDistinct {
			return
		}
		if _, ok := aggFunc.Args[0].(*expression.Constant); !ok {
			return
		}
	}
	physicalTableScan, ok := physicalAgg.Children()[0].(*PhysicalTableScan)
	if !ok || !physicalTableScan.isFullScan() || physicalTableScan.StoreType != kv.TiFlash || len(physicalTableScan.schema.Columns) != 1 {
		return
	}
	// rewrite datasource and agg args
	rewriteTableScanAndAggArgs(physicalTableScan, physicalAgg.AggFuncs)
}

// rewriteTableScanAndAggArgs Pick the narrowest and not null column from table
// If there is no not null column in Data Source, the row_id or pk column will be retained
func rewriteTableScanAndAggArgs(physicalTableScan *PhysicalTableScan, aggFuncs []*aggregation.AggFuncDesc) {
	var resultColumnInfo *model.ColumnInfo
	var resultColumn *expression.Column

	resultColumnInfo = physicalTableScan.Columns[0]
	resultColumn = physicalTableScan.schema.Columns[0]
	// prefer not null column from table
	for _, columnInfo := range physicalTableScan.Table.Columns {
		if columnInfo.FieldType.IsVarLengthType() {
			continue
		}
		if mysql.HasNotNullFlag(columnInfo.GetFlag()) {
			if columnInfo.GetFlen() < resultColumnInfo.GetFlen() {
				resultColumnInfo = columnInfo
				resultColumn = &expression.Column{
					UniqueID: physicalTableScan.SCtx().GetSessionVars().AllocPlanColumnID(),
					ID:       resultColumnInfo.ID,
					RetType:  resultColumnInfo.FieldType.Clone(),
					OrigName: fmt.Sprintf("%s.%s.%s", physicalTableScan.DBName.L, physicalTableScan.Table.Name.L, resultColumnInfo.Name),
				}
			}
		}
	}
	// table scan (row_id) -> (not null column)
	physicalTableScan.Columns[0] = resultColumnInfo
	physicalTableScan.schema.Columns[0] = resultColumn
	// agg arg count(1) -> count(not null column)
	arg := resultColumn.Clone()
	for _, aggFunc := range aggFuncs {
		constExpr, ok := aggFunc.Args[0].(*expression.Constant)
		if !ok {
			return
		}
		// count(null) shouldn't be rewritten
		if constExpr.Value.IsNull() {
			continue
		}
		aggFunc.Args[0] = arg
	}
}

// Only for MPP(Window<-[Sort]<-ExchangeReceiver<-ExchangeSender).
// TiFlashFineGrainedShuffleStreamCount:
// < 0: fine grained shuffle is disabled.
// > 0: use TiFlashFineGrainedShuffleStreamCount as stream count.
// == 0: use TiFlashMaxThreads as stream count when it's greater than 0. Otherwise set status as uninitialized.
func handleFineGrainedShuffle(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan) {
	streamCount := sctx.GetSessionVars().TiFlashFineGrainedShuffleStreamCount
	if streamCount < 0 {
		return
	}
	if streamCount == 0 {
		if sctx.GetSessionVars().TiFlashMaxThreads > 0 {
			streamCount = sctx.GetSessionVars().TiFlashMaxThreads
		}
	}
	// use two separate cluster info to avoid grpc calls cost
	tiflashServerCountInfo := tiflashClusterInfo{unInitialized, 0}
	streamCountInfo := tiflashClusterInfo{unInitialized, 0}
	if streamCount != 0 {
		streamCountInfo.itemStatus = initialized
		streamCountInfo.itemValue = uint64(streamCount)
	}
	setupFineGrainedShuffle(ctx, sctx, &streamCountInfo, &tiflashServerCountInfo, plan)
}

func setupFineGrainedShuffle(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, plan base.PhysicalPlan) {
	if tableReader, ok := plan.(*PhysicalTableReader); ok {
		if _, isExchangeSender := tableReader.tablePlan.(*PhysicalExchangeSender); isExchangeSender {
			helper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: make([]*basePhysicalPlan, 1)}
			setupFineGrainedShuffleInternal(ctx, sctx, tableReader.tablePlan, &helper, streamCountInfo, tiflashServerCountInfo)
		}
	} else {
		for _, child := range plan.Children() {
			setupFineGrainedShuffle(ctx, sctx, streamCountInfo, tiflashServerCountInfo, child)
		}
	}
}

type shuffleTarget uint8

const (
	unknown shuffleTarget = iota
	window
	joinBuild
	hashAgg
)

type fineGrainedShuffleHelper struct {
	shuffleTarget shuffleTarget
	plans         []*basePhysicalPlan
	joinKeysCount int
}

type tiflashClusterInfoStatus uint8

const (
	unInitialized tiflashClusterInfoStatus = iota
	initialized
	failed
)

type tiflashClusterInfo struct {
	itemStatus tiflashClusterInfoStatus
	itemValue  uint64
}

func (h *fineGrainedShuffleHelper) clear() {
	h.shuffleTarget = unknown
	h.plans = h.plans[:0]
	h.joinKeysCount = 0
}

func (h *fineGrainedShuffleHelper) updateTarget(t shuffleTarget, p *basePhysicalPlan) {
	h.shuffleTarget = t
	h.plans = append(h.plans, p)
}

// calculateTiFlashStreamCountUsingMinLogicalCores uses minimal logical cpu cores among tiflash servers, and divide by 2
// return false, 0 if any err happens
func calculateTiFlashStreamCountUsingMinLogicalCores(ctx context.Context, sctx base.PlanContext, serversInfo []infoschema.ServerInfo) (bool, uint64) {
	failpoint.Inject("mockTiFlashStreamCountUsingMinLogicalCores", func(val failpoint.Value) {
		intVal, err := strconv.Atoi(val.(string))
		if err == nil {
			failpoint.Return(true, uint64(intVal))
		} else {
			failpoint.Return(false, 0)
		}
	})
	rows, err := infoschema.FetchClusterServerInfoWithoutPrivilegeCheck(ctx, sctx.GetSessionVars(), serversInfo, diagnosticspb.ServerInfoType_HardwareInfo, false)
	if err != nil {
		return false, 0
	}
	var initialMaxCores uint64 = 10000
	var minLogicalCores = initialMaxCores // set to a large enough value here
	for _, row := range rows {
		if row[4].GetString() == "cpu-logical-cores" {
			logicalCpus, err := strconv.Atoi(row[5].GetString())
			if err == nil && logicalCpus > 0 {
				minLogicalCores = min(minLogicalCores, uint64(logicalCpus))
			}
		}
	}
	// No need to check len(serersInfo) == serverCount here, since missing some servers' info won't affect the correctness
	if minLogicalCores > 1 && minLogicalCores != initialMaxCores {
		if runtime.GOARCH == "amd64" {
			// In most x86-64 platforms, `Thread(s) per core` is 2
			return true, minLogicalCores / 2
		}
		// ARM cpus don't implement Hyper-threading.
		return true, minLogicalCores
		// Other platforms are too rare to consider
	}

	return false, 0
}

func checkFineGrainedShuffleForJoinAgg(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, exchangeColCount int, splitLimit uint64) (applyFlag bool, streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		streamCount = 4 // assume 8c node in cluster as minimal, stream count is 8 / 2 = 4
	case initialized:
		streamCount = (*streamCountInfo).itemValue
	case failed:
		return false, 0 // probably won't reach this path
	}

	var tiflashServerCount uint64
	switch (*tiflashServerCountInfo).itemStatus {
	case unInitialized:
		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
		if err != nil {
			(*tiflashServerCountInfo).itemStatus = failed
			(*tiflashServerCountInfo).itemValue = 0
			if (*streamCountInfo).itemStatus == unInitialized {
				setDefaultStreamCount(streamCountInfo)
			}
			return false, 0
		}
		tiflashServerCount = uint64(len(serversInfo))
		(*tiflashServerCountInfo).itemStatus = initialized
		(*tiflashServerCountInfo).itemValue = tiflashServerCount
	case initialized:
		tiflashServerCount = (*tiflashServerCountInfo).itemValue
	case failed:
		return false, 0
	}

	// if already exceeds splitLimit, no need to fetch actual logical cores
	if tiflashServerCount*uint64(exchangeColCount)*streamCount > splitLimit {
		return false, 0
	}

	// if streamCount already initialized, and can pass splitLimit check
	if (*streamCountInfo).itemStatus == initialized {
		return true, streamCount
	}

	serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
	if err != nil {
		(*tiflashServerCountInfo).itemStatus = failed
		(*tiflashServerCountInfo).itemValue = 0
		return false, 0
	}
	flag, temStreamCount := calculateTiFlashStreamCountUsingMinLogicalCores(ctx, sctx, serversInfo)
	if !flag {
		setDefaultStreamCount(streamCountInfo)
		(*tiflashServerCountInfo).itemStatus = failed
		return false, 0
	}
	streamCount = temStreamCount
	(*streamCountInfo).itemStatus = initialized
	(*streamCountInfo).itemValue = streamCount
	applyFlag = tiflashServerCount*uint64(exchangeColCount)*streamCount <= splitLimit
	return applyFlag, streamCount
}

func inferFineGrainedShuffleStreamCountForWindow(ctx context.Context, sctx base.PlanContext, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) (streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		if (*tiflashServerCountInfo).itemStatus == failed {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			break
		}

		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx.GetStore())
		if err != nil {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			(*tiflashServerCountInfo).itemStatus = failed
			break
		}

		if (*tiflashServerCountInfo).itemStatus == unInitialized {
			(*tiflashServerCountInfo).itemStatus = initialized
			(*tiflashServerCountInfo).itemValue = uint64(len(serversInfo))
		}

		flag, temStreamCount := calculateTiFlashStreamCountUsingMinLogicalCores(ctx, sctx, serversInfo)
		if !flag {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			(*tiflashServerCountInfo).itemStatus = failed
			break
		}
		streamCount = temStreamCount
		(*streamCountInfo).itemStatus = initialized
		(*streamCountInfo).itemValue = streamCount
	case initialized:
		streamCount = (*streamCountInfo).itemValue
	case failed:
		setDefaultStreamCount(streamCountInfo)
		streamCount = (*streamCountInfo).itemValue
	}
	return streamCount
}

func setDefaultStreamCount(streamCountInfo *tiflashClusterInfo) {
	(*streamCountInfo).itemStatus = initialized
	(*streamCountInfo).itemValue = variable.DefStreamCountWhenMaxThreadsNotSet
}

func setupFineGrainedShuffleInternal(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan, helper *fineGrainedShuffleHelper, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) {
	switch x := plan.(type) {
	case *PhysicalWindow:
		// Do not clear the plans because window executor will keep the data partition.
		// For non hash partition window function, there will be a passthrough ExchangeSender to collect data,
		// which will break data partition.
		helper.updateTarget(window, &x.basePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalSort:
		if x.IsPartialSort {
			// Partial sort will keep the data partition.
			helper.plans = append(helper.plans, &x.basePhysicalPlan)
		} else {
			// Global sort will break the data partition.
			helper.clear()
		}
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalSelection:
		helper.plans = append(helper.plans, &x.basePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalProjection:
		helper.plans = append(helper.plans, &x.basePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalExchangeReceiver:
		helper.plans = append(helper.plans, &x.basePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalHashAgg:
		// Todo: allow hash aggregation's output still benefits from fine grained shuffle
		aggHelper := fineGrainedShuffleHelper{shuffleTarget: hashAgg, plans: []*basePhysicalPlan{}}
		aggHelper.plans = append(aggHelper.plans, &x.basePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], &aggHelper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalHashJoin:
		child0 := x.children[0]
		child1 := x.children[1]
		buildChild := child0
		probChild := child1
		joinKeys := x.LeftJoinKeys
		if x.InnerChildIdx != 0 {
			// Child1 is build side.
			buildChild = child1
			joinKeys = x.RightJoinKeys
			probChild = child0
		}
		if len(joinKeys) > 0 { // Not cross join
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: joinBuild, plans: []*basePhysicalPlan{}}
			buildHelper.plans = append(buildHelper.plans, &x.basePhysicalPlan)
			buildHelper.joinKeysCount = len(joinKeys)
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		} else {
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*basePhysicalPlan{}}
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		}
		// don't apply fine grained shuffle for probe side
		helper.clear()
		setupFineGrainedShuffleInternal(ctx, sctx, probChild, helper, streamCountInfo, tiflashServerCountInfo)
	case *PhysicalExchangeSender:
		if x.ExchangeType == tipb.ExchangeType_Hash {
			// Set up stream count for all plans based on shuffle target type.
			var exchangeColCount = x.Schema().Len()
			switch helper.shuffleTarget {
			case window:
				streamCount := inferFineGrainedShuffleStreamCountForWindow(ctx, sctx, streamCountInfo, tiflashServerCountInfo)
				x.TiFlashFineGrainedShuffleStreamCount = streamCount
				for _, p := range helper.plans {
					p.TiFlashFineGrainedShuffleStreamCount = streamCount
				}
			case hashAgg:
				applyFlag, streamCount := checkFineGrainedShuffleForJoinAgg(ctx, sctx, streamCountInfo, tiflashServerCountInfo, exchangeColCount, 1200) // 1200: performance test result
				if applyFlag {
					x.TiFlashFineGrainedShuffleStreamCount = streamCount
					for _, p := range helper.plans {
						p.TiFlashFineGrainedShuffleStreamCount = streamCount
					}
				}
			case joinBuild:
				// Support hashJoin only when shuffle hash keys equals to join keys due to tiflash implementations
				if len(x.HashCols) != helper.joinKeysCount {
					break
				}
				applyFlag, streamCount := checkFineGrainedShuffleForJoinAgg(ctx, sctx, streamCountInfo, tiflashServerCountInfo, exchangeColCount, 600) // 600: performance test result
				if applyFlag {
					x.TiFlashFineGrainedShuffleStreamCount = streamCount
					for _, p := range helper.plans {
						p.TiFlashFineGrainedShuffleStreamCount = streamCount
					}
				}
			}
		}
		// exchange sender will break the data partition.
		helper.clear()
		setupFineGrainedShuffleInternal(ctx, sctx, x.children[0], helper, streamCountInfo, tiflashServerCountInfo)
	default:
		for _, child := range x.Children() {
			childHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*basePhysicalPlan{}}
			setupFineGrainedShuffleInternal(ctx, sctx, child, &childHelper, streamCountInfo, tiflashServerCountInfo)
		}
	}
}

// propagateProbeParents doesn't affect the execution plan, it only sets the probeParents field of a PhysicalPlan.
// It's for handling the inconsistency between row count in the statsInfo and the recorded actual row count. Please
// see comments in PhysicalPlan for details.
func propagateProbeParents(plan base.PhysicalPlan, probeParents []base.PhysicalPlan) {
	plan.SetProbeParents(probeParents)
	switch x := plan.(type) {
	case *PhysicalApply, *PhysicalIndexJoin, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin:
		if join, ok := plan.(interface{ getInnerChildIdx() int }); ok {
			propagateProbeParents(plan.Children()[1-join.getInnerChildIdx()], probeParents)

			// The core logic of this method:
			// Record every Apply and Index Join we met, record it in a slice, and set it in their inner children.
			newParents := make([]base.PhysicalPlan, len(probeParents), len(probeParents)+1)
			copy(newParents, probeParents)
			newParents = append(newParents, plan)
			propagateProbeParents(plan.Children()[join.getInnerChildIdx()], newParents)
		}
	case *PhysicalTableReader:
		propagateProbeParents(x.tablePlan, probeParents)
	case *PhysicalIndexReader:
		propagateProbeParents(x.indexPlan, probeParents)
	case *PhysicalIndexLookUpReader:
		propagateProbeParents(x.indexPlan, probeParents)
		propagateProbeParents(x.tablePlan, probeParents)
	case *PhysicalIndexMergeReader:
		for _, pchild := range x.partialPlans {
			propagateProbeParents(pchild, probeParents)
		}
		propagateProbeParents(x.tablePlan, probeParents)
	default:
		for _, child := range plan.Children() {
			propagateProbeParents(child, probeParents)
		}
	}
}

func enableParallelApply(sctx base.PlanContext, plan base.PhysicalPlan) base.PhysicalPlan {
	if !sctx.GetSessionVars().EnableParallelApply {
		return plan
	}
	// the parallel apply has three limitation:
	// 1. the parallel implementation now cannot keep order;
	// 2. the inner child has to support clone;
	// 3. if one Apply is in the inner side of another Apply, it cannot be parallel, for example:
	//		The topology of 3 Apply operators are A1(A2, A3), which means A2 is the outer child of A1
	//		while A3 is the inner child. Then A1 and A2 can be parallel and A3 cannot.
	if apply, ok := plan.(*PhysicalApply); ok {
		outerIdx := 1 - apply.InnerChildIdx
		noOrder := len(apply.GetChildReqProps(outerIdx).SortItems) == 0 // limitation 1
		_, err := SafeClone(apply.Children()[apply.InnerChildIdx])
		supportClone := err == nil // limitation 2
		if noOrder && supportClone {
			apply.Concurrency = sctx.GetSessionVars().ExecutorConcurrency
		} else {
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Some apply operators can not be executed in parallel: %v", err))
			} else {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("Some apply operators can not be executed in parallel"))
			}
		}
		// because of the limitation 3, we cannot parallelize Apply operators in this Apply's inner size,
		// so we only invoke recursively for its outer child.
		apply.SetChild(outerIdx, enableParallelApply(sctx, apply.Children()[outerIdx]))
		return apply
	}
	for i, child := range plan.Children() {
		plan.SetChild(i, enableParallelApply(sctx, child))
	}
	return plan
}

// LogicalOptimizeTest is just exported for test.
func LogicalOptimizeTest(ctx context.Context, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, error) {
	return logicalOptimize(ctx, flag, logic)
}

func logicalOptimize(ctx context.Context, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, error) {
	if logic.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(logic.SCtx())
		defer debugtrace.LeaveContextCommon(logic.SCtx())
	}
	opt := optimizetrace.DefaultLogicalOptimizeOption()
	vars := logic.SCtx().GetSessionVars()
	if vars.StmtCtx.EnableOptimizeTrace {
		vars.StmtCtx.OptimizeTracer = &tracing.OptimizeTracer{}
		tracer := &tracing.LogicalOptimizeTracer{
			Steps: make([]*tracing.LogicalRuleOptimizeTracer, 0),
		}
		opt = opt.WithEnableOptimizeTracer(tracer)
		defer func() {
			vars.StmtCtx.OptimizeTracer.Logical = tracer
		}()
	}
	var err error
	var againRuleList []logicalOptRule
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		opt.AppendBeforeRuleOptimize(i, rule.name(), logic.BuildPlanTrace)
		var planChanged bool
		logic, planChanged, err = rule.optimize(ctx, logic, opt)
		if err != nil {
			return nil, err
		}
		// Compute interaction rules that should be optimized again
		interactionRule, ok := optInteractionRuleList[rule]
		if planChanged && ok && isLogicalRuleDisabled(interactionRule) {
			againRuleList = append(againRuleList, interactionRule)
		}
	}

	// Trigger the interaction rule
	for i, rule := range againRuleList {
		opt.AppendBeforeRuleOptimize(i, rule.name(), logic.BuildPlanTrace)
		logic, _, err = rule.optimize(ctx, logic, opt)
		if err != nil {
			return nil, err
		}
	}

	opt.RecordFinalLogicalPlan(logic.BuildPlanTrace)
	return logic, err
}

func isLogicalRuleDisabled(r logicalOptRule) bool {
	disabled := DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(r.name())
	return disabled
}

func physicalOptimize(logic base.LogicalPlan, planCounter *base.PlanCounterTp) (plan base.PhysicalPlan, cost float64, err error) {
	if logic.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(logic.SCtx())
		defer debugtrace.LeaveContextCommon(logic.SCtx())
	}
	if _, err := logic.RecursiveDeriveStats(nil); err != nil {
		return nil, 0, err
	}

	preparePossibleProperties(logic)

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	opt := optimizetrace.DefaultPhysicalOptimizeOption()
	stmtCtx := logic.SCtx().GetSessionVars().StmtCtx
	if stmtCtx.EnableOptimizeTrace {
		tracer := &tracing.PhysicalOptimizeTracer{
			PhysicalPlanCostDetails: make(map[string]*tracing.PhysicalPlanCostDetail),
			Candidates:              make(map[int]*tracing.CandidatePlanTrace),
		}
		opt = opt.WithEnableOptimizeTracer(tracer)
		defer func() {
			r := recover()
			if r != nil {
				panic(r) /* pass panic to upper function to handle */
			}
			if err == nil {
				tracer.RecordFinalPlanTrace(plan.BuildPlanTrace())
				stmtCtx.OptimizeTracer.Physical = tracer
			}
		}()
	}

	logic.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS = 0
	t, _, err := logic.FindBestTask(prop, planCounter, opt)
	if err != nil {
		return nil, 0, err
	}
	if *planCounter > 0 {
		logic.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("The parameter of nth_plan() is out of range"))
	}
	if t.Invalid() {
		errMsg := "Can't find a proper physical plan for this query"
		if config.GetGlobalConfig().DisaggregatedTiFlash && !logic.SCtx().GetSessionVars().IsMPPAllowed() {
			errMsg += ": cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch"
		}
		return nil, 0, plannererrors.ErrInternal.GenWithStackByArgs(errMsg)
	}

	if err = t.Plan().ResolveIndices(); err != nil {
		return nil, 0, err
	}
	cost, err = getPlanCost(t.Plan(), property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
	return t.Plan(), cost, err
}

// eliminateUnionScanAndLock set lock property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx base.PlanContext, p base.PhysicalPlan) base.PhysicalPlan {
	var pointGet *PointGetPlan
	var batchPointGet *BatchPointGetPlan
	var physLock *PhysicalLock
	var unionScan *PhysicalUnionScan
	iteratePhysicalPlan(p, func(p base.PhysicalPlan) bool {
		if len(p.Children()) > 1 {
			return false
		}
		switch x := p.(type) {
		case *PointGetPlan:
			pointGet = x
		case *BatchPointGetPlan:
			batchPointGet = x
		case *PhysicalLock:
			physLock = x
		case *PhysicalUnionScan:
			unionScan = x
		}
		return true
	})
	if pointGet == nil && batchPointGet == nil {
		return p
	}
	if physLock == nil && unionScan == nil {
		return p
	}
	if physLock != nil {
		lock, waitTime := getLockWaitTime(sctx, physLock.Lock)
		if !lock {
			return p
		}
		if pointGet != nil {
			pointGet.Lock = lock
			pointGet.LockWaitTime = waitTime
		} else {
			batchPointGet.Lock = lock
			batchPointGet.LockWaitTime = waitTime
		}
	}
	return transformPhysicalPlan(p, func(p base.PhysicalPlan) base.PhysicalPlan {
		if p == physLock {
			return p.Children()[0]
		}
		if p == unionScan {
			return p.Children()[0]
		}
		return p
	})
}

func iteratePhysicalPlan(p base.PhysicalPlan, f func(p base.PhysicalPlan) bool) {
	if !f(p) {
		return
	}
	for _, child := range p.Children() {
		iteratePhysicalPlan(child, f)
	}
}

func transformPhysicalPlan(p base.PhysicalPlan, f func(p base.PhysicalPlan) base.PhysicalPlan) base.PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = transformPhysicalPlan(child, f)
	}
	return f(p)
}

func existsCartesianProduct(p base.LogicalPlan) bool {
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.Children() {
		if existsCartesianProduct(child) {
			return true
		}
	}
	return false
}

// DefaultDisabledLogicalRulesList indicates the logical rules which should be banned.
var DefaultDisabledLogicalRulesList *atomic.Value

func disableReuseChunkIfNeeded(sctx base.PlanContext, plan base.PhysicalPlan) {
	if !sctx.GetSessionVars().IsAllocValid() {
		return
	}

	if checkOverlongColType(sctx, plan) {
		return
	}

	for _, child := range plan.Children() {
		disableReuseChunkIfNeeded(sctx, child)
	}
}

// checkOverlongColType Check if read field type is long field.
func checkOverlongColType(sctx base.PlanContext, plan base.PhysicalPlan) bool {
	if plan == nil {
		return false
	}
	switch plan.(type) {
	case *PhysicalTableReader, *PhysicalIndexReader,
		*PhysicalIndexLookUpReader, *PhysicalIndexMergeReader, *PointGetPlan:
		if existsOverlongType(plan.Schema()) {
			sctx.GetSessionVars().ClearAlloc(nil, false)
			return true
		}
	}
	return false
}

// existsOverlongType Check if exists long type column.
func existsOverlongType(schema *expression.Schema) bool {
	if schema == nil {
		return false
	}
	for _, column := range schema.Columns {
		switch column.RetType.GetType() {
		case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
			mysql.TypeBlob, mysql.TypeJSON:
			return true
		case mysql.TypeVarString, mysql.TypeVarchar:
			// if the column is varchar and the length of
			// the column is defined to be more than 1000,
			// the column is considered a large type and
			// disable chunk_reuse.
			if column.RetType.GetFlen() > 1000 {
				return true
			}
		}
	}
	return false
}
