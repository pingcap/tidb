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
	"math"
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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades"
	"github.com/pingcap/tidb/pkg/planner/cascades/impl"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (base.Plan, types.NameSlice, error)

// OptimizeAstNodeNoCache bypasses the plan cache and generates a physical plan directly.
var OptimizeAstNodeNoCache func(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (base.Plan, types.NameSlice, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const initialMaxCores uint64 = 10000

var (
	// the old ref of optRuleList for downgrading to old optimizing routine.
	logicalRuleList = optRuleList
	// the new normalizeRuleList is special for prev-phase of memo, which is for always-good rules.
	normalizeRuleList = optRuleList
	// note this two list will differ when some trade-off rules is moved out of norm phase for cascades.
)

var optRuleList = []base.LogicalOptRule{
	&GcSubstituter{},
	&rule.ColumnPruner{},
	&ResultReorder{},
	&rule.BuildKeySolver{},
	&DecorrelateSolver{},
	&SemiJoinRewriter{},
	&AggregationEliminator{},
	&SkewDistinctAggRewriter{},
	&ProjectionEliminator{},
	&MaxMinEliminator{},
	&rule.ConstantPropagationSolver{},
	&ConvertOuterToInnerJoin{},
	&PPDSolver{},
	&OuterJoinEliminator{},
	&rule.PartitionProcessor{},
	&rule.CollectPredicateColumnsPoint{},
	&AggregationPushDownSolver{},
	&DeriveTopNFromWindow{},
	&rule.PredicateSimplification{},
	&PushDownTopNOptimizer{},
	&rule.SyncWaitStatsLoadPoint{},
	&JoinReOrderSolver{},
	&rule.ColumnPruner{}, // column pruning again at last, note it will mess up the results of buildKeySolver
	&PushDownSequenceSolver{},
	&EliminateUnionAllDualItem{},
	&EmptySelectionEliminator{},
	&ResolveExpand{},
}

// Interaction Rule List
/* The interaction rule will be trigger when it satisfies following conditions:
1. The related rule has been trigger and changed the plan
2. The interaction rule is enabled
*/
var optInteractionRuleList = map[base.LogicalOptRule]base.LogicalOptRule{}

// BuildLogicalPlanForTest builds a logical plan for testing purpose from ast.Node.
func BuildLogicalPlanForTest(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, infoSchema infoschema.InfoSchema) (base.Plan, error) {
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
			hasPriv := false
			for _, priv := range v.dynamicPrivs {
				hasPriv = hasPriv || pm.RequestDynamicVerification(activeRoles, priv, v.dynamicWithGrant)
				if hasPriv {
					break
				}
			}
			if !hasPriv {
				if v.err == nil {
					return plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs(v.dynamicPrivs)
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
func VisitInfo4PrivCheck(ctx context.Context, is infoschema.InfoSchema, node ast.Node, vs []visitInfo) (privVisitInfo []visitInfo) {
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
				if needCheckTmpTablePriv(ctx, is, v) {
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
					if needCheckTmpTablePriv(ctx, is, v) {
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
			if needCheckTmpTablePriv(ctx, is, v) {
				privVisitInfo = append(privVisitInfo, v)
			}
		}
	}
	return
}

func needCheckTmpTablePriv(ctx context.Context, is infoschema.InfoSchema, v visitInfo) bool {
	if v.db != "" && v.table != "" {
		// Other statements on local temporary tables except `CREATE` do not check any privileges.
		tb, err := is.TableByName(ctx, ast.NewCIStr(v.db), ast.NewCIStr(v.table))
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

// CheckTableMode checks if the table is accessible by table mode, only TableModeNormal can be accessed.
func CheckTableMode(node *resolve.NodeW) error {
	// First make exceptions for stmt that only visit table meta;
	// For example, `describe <table_name>` and `show create table <table_name>`;
	switch node.Node.(type) {
	case *ast.ShowStmt, *ast.ExplainStmt:
	default:
		// Special handling to the `ADMIN CHECKSUM TABLE`, as `IMPORT INTO` will
		// executes this statement during post checksum to verify data.
		// TODO: only allow `ADMIN CHECKSUM TABLE` from import into task
		adminStmt, ok := node.Node.(*ast.AdminStmt)
		if ok && adminStmt.Tp == ast.AdminChecksumTable {
			return nil
		}
		for _, tblNameW := range node.GetResolveContext().GetTableNames() {
			if err := dbutil.CheckTableModeIsNormal(tblNameW.Name, tblNameW.TableInfo.Mode); err != nil {
				return err
			}
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
func doOptimize(ctx context.Context, sctx base.PlanContext, flag uint64, logic base.LogicalPlan) (
	base.LogicalPlan, base.PhysicalPlan, float64, error) {
	if sctx.GetSessionVars().GetSessionVars().EnableCascadesPlanner {
		return CascadesOptimize(ctx, sctx, flag, logic)
	}
	return VolcanoOptimize(ctx, sctx, flag, logic)
}

// CascadesOptimize includes: normalization, cascadesOptimize, and physicalOptimize.
func CascadesOptimize(ctx context.Context, sctx base.PlanContext, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, base.PhysicalPlan, float64, error) {
	flag = adjustOptimizationFlags(flag, logic)
	logic, err := normalizeOptimize(ctx, flag, logic)
	if err != nil {
		return nil, nil, 0, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, nil, 0, errors.Trace(plannererrors.ErrCartesianProductUnsupported)
	}
	logic.ExtractFD()

	var cas *cascades.Optimizer
	if cas, err = cascades.NewOptimizer(logic); err == nil {
		defer cas.Destroy()
		err = cas.Execute()
	}
	if err != nil {
		return nil, nil, 0, err
	}
	var (
		cost     float64
		physical base.PhysicalPlan
	)
	physical, cost, err = impl.ImplementMemoAndCost(cas.GetMemo().GetRootGroup())
	if err != nil {
		return nil, nil, 0, err
	}

	finalPlan := postOptimize(ctx, sctx, physical)
	return logic, finalPlan, cost, nil
}

// VolcanoOptimize includes: logicalOptimize, physicalOptimize
func VolcanoOptimize(ctx context.Context, sctx base.PlanContext, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, base.PhysicalPlan, float64, error) {
	flag = adjustOptimizationFlags(flag, logic)
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, nil, 0, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, nil, 0, errors.Trace(plannererrors.ErrCartesianProductUnsupported)
	}
	failpoint.Inject("ConsumeVolcanoOptimizePanic", nil)
	physical, cost, err := physicalOptimize(logic)
	if err != nil {
		return nil, nil, 0, err
	}
	finalPlan := postOptimize(ctx, sctx, physical)

	if err = finalPlan.ResolveIndices(); err != nil {
		return nil, nil, 0, err
	}

	return logic, finalPlan, cost, nil
}

func adjustOptimizationFlags(flag uint64, logic base.LogicalPlan) uint64 {
	// If there is something after flagPrunColumns, do FlagPruneColumnsAgain.
	if flag&rule.FlagPruneColumns > 0 && flag-rule.FlagPruneColumns > rule.FlagPruneColumns {
		flag |= rule.FlagPruneColumnsAgain
	}
	if checkStableResultMode(logic.SCtx()) {
		flag |= rule.FlagStabilizeResults
	}
	if logic.SCtx().GetSessionVars().StmtCtx.StraightJoinOrder {
		// When we use the straight Join Order hint, we should disable the join reorder optimization.
		flag &= ^rule.FlagJoinReOrder
	}
	// InternalSQLScanUserTable is for ttl scan.
	if !logic.SCtx().GetSessionVars().InRestrictedSQL || logic.SCtx().GetSessionVars().InternalSQLScanUserTable {
		flag |= rule.FlagCollectPredicateColumnsPoint
		flag |= rule.FlagSyncWaitStatsLoadPoint
	}
	if !logic.SCtx().GetSessionVars().StmtCtx.UseDynamicPruneMode {
		flag |= rule.FlagPartitionProcessor // apply partition pruning under static mode
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
	_, finalPlan, cost, err := doOptimize(ctx, sctx, flag, logic)
	return finalPlan, cost, err
}

// mergeContinuousSelections merge continuous selections which may occur after changing plans.
func mergeContinuousSelections(p base.PhysicalPlan) {
	if sel, ok := p.(*physicalop.PhysicalSelection); ok {
		for {
			childSel := sel.Children()[0]
			tmp, ok := childSel.(*physicalop.PhysicalSelection)
			if !ok {
				break
			}
			sel.Conditions = append(sel.Conditions, tmp.Conditions...)
			sel.SetChild(0, tmp.Children()[0])
		}
	}
	for _, child := range p.Children() {
		mergeContinuousSelections(child)
	}
	// merge continuous selections in a coprocessor task of tiflash
	tableReader, isTableReader := p.(*physicalop.PhysicalTableReader)
	if isTableReader && tableReader.StoreType == kv.TiFlash {
		mergeContinuousSelections(tableReader.TablePlan)
		tableReader.TablePlans = physicalop.FlattenListPushDownPlan(tableReader.TablePlan)
	}
}

func postOptimize(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan) base.PhysicalPlan {
	// some cases from update optimize will require avoiding projection elimination.
	// see comments ahead of call of DoOptimize in function of buildUpdate().
	plan = eliminatePhysicalProjection(plan)
	plan = InjectExtraProjection(plan)
	mergeContinuousSelections(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	plan = avoidColumnEvaluatorForProjBelowUnion(plan)
	plan = enableParallelApply(sctx, plan)
	handleFineGrainedShuffle(ctx, sctx, plan)
	propagateProbeParents(plan, nil)
	countStarRewrite(plan)
	disableReuseChunkIfNeeded(sctx, plan)
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
		columnUniqueIDToRF: map[int64][]*physicalop.RuntimeFilter{},
		parentPhysicalPlan: plan,
	}
	startRFGenerator := time.Now()
	rfGenerator.GenerateRuntimeFilter(plan)
	logutil.BgLogger().Debug("Finish runtime filter generator",
		zap.Duration("Cost", time.Since(startRFGenerator)))
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
	if tableReader, ok := plan.(*physicalop.PhysicalTableReader); ok {
		countStarRewrite(tableReader.TablePlan)
	} else {
		for _, child := range plan.Children() {
			countStarRewrite(child)
		}
	}
}

func countStarRewriteInternal(plan base.PhysicalPlan) {
	// match pattern any agg(count(constant)) -> tablefullscan(tiflash)
	var physicalAgg *physicalop.BasePhysicalAgg
	switch x := plan.(type) {
	case *physicalop.PhysicalHashAgg:
		physicalAgg = x.GetPointer()
	case *physicalop.PhysicalStreamAgg:
		physicalAgg = x.GetPointer()
	default:
		return
	}
	if len(physicalAgg.GroupByItems) > 0 || len(physicalAgg.Children()) != 1 {
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
	physicalTableScan, ok := physicalAgg.Children()[0].(*physicalop.PhysicalTableScan)
	if !ok || !physicalTableScan.IsFullScan() || physicalTableScan.StoreType != kv.TiFlash || len(physicalTableScan.Schema().Columns) != 1 {
		return
	}
	// rewrite datasource and agg args
	rewriteTableScanAndAggArgs(physicalTableScan, physicalAgg.AggFuncs)
}

// rewriteTableScanAndAggArgs Pick the narrowest and not null column from table
// If there is no not null column in Data Source, the row_id or pk column will be retained
func rewriteTableScanAndAggArgs(physicalTableScan *physicalop.PhysicalTableScan, aggFuncs []*aggregation.AggFuncDesc) {
	var resultColumnInfo *model.ColumnInfo
	var resultColumn *expression.Column

	resultColumnInfo = physicalTableScan.Columns[0]
	resultColumn = physicalTableScan.Schema().Columns[0]
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
	physicalTableScan.Schema().Columns[0] = resultColumn
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
	if tableReader, ok := plan.(*physicalop.PhysicalTableReader); ok {
		if _, isExchangeSender := tableReader.TablePlan.(*physicalop.PhysicalExchangeSender); isExchangeSender {
			helper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: make([]*physicalop.BasePhysicalPlan, 1)}
			setupFineGrainedShuffleInternal(ctx, sctx, tableReader.TablePlan, &helper, streamCountInfo, tiflashServerCountInfo)
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
	plans         []*physicalop.BasePhysicalPlan
	joinKeys      []*expression.Column
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
	h.joinKeys = nil
}

func (h *fineGrainedShuffleHelper) updateTarget(t shuffleTarget, p *physicalop.BasePhysicalPlan) {
	h.shuffleTarget = t
	h.plans = append(h.plans, p)
}

func getTiFlashServerMinLogicalCores(ctx context.Context, sctx base.PlanContext, serversInfo []infoschema.ServerInfo) (bool, uint64) {
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
	return true, minLogicalCores
}

// calculateTiFlashStreamCountUsingMinLogicalCores uses minimal logical cpu cores among tiflash servers
// return false, 0 if any err happens
func calculateTiFlashStreamCountUsingMinLogicalCores(ctx context.Context, sctx base.PlanContext, serversInfo []infoschema.ServerInfo) (bool, uint64) {
	valid, minLogicalCores := getTiFlashServerMinLogicalCores(ctx, sctx, serversInfo)
	if !valid {
		return false, 0
	}
	if minLogicalCores != initialMaxCores {
		// use logical core number as the stream count, the same as TiFlash's default max_threads: https://github.com/pingcap/tiflash/blob/v7.5.0/dbms/src/Interpreters/SettingsCommon.h#L166
		return true, minLogicalCores
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
	(*streamCountInfo).itemValue = vardef.DefStreamCountWhenMaxThreadsNotSet
}

func setupFineGrainedShuffleInternal(ctx context.Context, sctx base.PlanContext, plan base.PhysicalPlan, helper *fineGrainedShuffleHelper, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) {
	switch x := plan.(type) {
	case *physicalop.PhysicalWindow:
		// Do not clear the plans because window executor will keep the data partition.
		// For non hash partition window function, there will be a passthrough ExchangeSender to collect data,
		// which will break data partition.
		helper.updateTarget(window, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalSort:
		if x.IsPartialSort {
			// Partial sort will keep the data partition.
			helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		} else {
			// Global sort will break the data partition.
			helper.clear()
		}
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalSelection:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalProjection:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalExchangeReceiver:
		helper.plans = append(helper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalHashAgg:
		// Todo: allow hash aggregation's output still benefits from fine grained shuffle
		aggHelper := fineGrainedShuffleHelper{shuffleTarget: hashAgg, plans: []*physicalop.BasePhysicalPlan{}}
		aggHelper.plans = append(aggHelper.plans, &x.BasePhysicalPlan)
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], &aggHelper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalHashJoin:
		child0 := x.Children()[0]
		child1 := x.Children()[1]
		buildChild := child0
		probChild := child1
		joinKeys := x.LeftJoinKeys
		if x.InnerChildIdx != 0 {
			// Child1 is build side.
			buildChild = child1
			joinKeys = x.RightJoinKeys
			probChild = child0
		}

		if len(joinKeys) > 0 && !x.CanTiFlashUseHashJoinV2(sctx) { // Not cross join and can not use hash join v2 in tiflash
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: joinBuild, plans: []*physicalop.BasePhysicalPlan{}}
			buildHelper.plans = append(buildHelper.plans, &x.BasePhysicalPlan)
			buildHelper.joinKeys = joinKeys
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		} else {
			buildHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*physicalop.BasePhysicalPlan{}}
			setupFineGrainedShuffleInternal(ctx, sctx, buildChild, &buildHelper, streamCountInfo, tiflashServerCountInfo)
		}
		// don't apply fine grained shuffle for probe side
		helper.clear()
		setupFineGrainedShuffleInternal(ctx, sctx, probChild, helper, streamCountInfo, tiflashServerCountInfo)
	case *physicalop.PhysicalExchangeSender:
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
				if len(x.HashCols) != len(helper.joinKeys) {
					break
				}
				// Check the shuffle key should be equal to joinKey, otherwise the shuffle hash code may not be equal to
				// actual join hash code due to type cast
				applyFlag := true
				for i, joinKey := range helper.joinKeys {
					if !x.HashCols[i].Col.EqualColumn(joinKey) {
						applyFlag = false
						break
					}
				}
				if !applyFlag {
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
		setupFineGrainedShuffleInternal(ctx, sctx, x.Children()[0], helper, streamCountInfo, tiflashServerCountInfo)
	default:
		for _, child := range x.Children() {
			childHelper := fineGrainedShuffleHelper{shuffleTarget: unknown, plans: []*physicalop.BasePhysicalPlan{}}
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
	case *physicalop.PhysicalApply, *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin,
		*physicalop.PhysicalIndexMergeJoin:
		if join, ok := plan.(interface{ GetInnerChildIdx() int }); ok {
			propagateProbeParents(plan.Children()[1-join.GetInnerChildIdx()], probeParents)

			// The core logic of this method:
			// Record every Apply and Index Join we met, record it in a slice, and set it in their inner children.
			newParents := make([]base.PhysicalPlan, len(probeParents), len(probeParents)+1)
			copy(newParents, probeParents)
			newParents = append(newParents, plan)
			propagateProbeParents(plan.Children()[join.GetInnerChildIdx()], newParents)
		}
	case *physicalop.PhysicalTableReader:
		propagateProbeParents(x.TablePlan, probeParents)
	case *physicalop.PhysicalIndexReader:
		propagateProbeParents(x.IndexPlan, probeParents)
	case *physicalop.PhysicalIndexLookUpReader:
		propagateProbeParents(x.IndexPlan, probeParents)
		propagateProbeParents(x.TablePlan, probeParents)
	case *physicalop.PhysicalIndexMergeReader:
		for _, pchild := range x.PartialPlansRaw {
			propagateProbeParents(pchild, probeParents)
		}
		propagateProbeParents(x.TablePlan, probeParents)
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
	if apply, ok := plan.(*physicalop.PhysicalApply); ok {
		outerIdx := 1 - apply.InnerChildIdx
		noOrder := len(apply.GetChildReqProps(outerIdx).SortItems) == 0 // limitation 1
		_, err := physicalop.SafeClone(sctx, apply.Children()[apply.InnerChildIdx])
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

func normalizeOptimize(ctx context.Context, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, error) {
	var err error
	// todo: the normalization rule driven way will be changed as stack-driven.
	for i, rule := range normalizeRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		logic, _, err = rule.Optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}
	return logic, err
}

func logicalOptimize(ctx context.Context, flag uint64, logic base.LogicalPlan) (base.LogicalPlan, error) {
	var err error
	var againRuleList []base.LogicalOptRule
	for i, rule := range logicalRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		var planChanged bool
		logic, planChanged, err = rule.Optimize(ctx, logic)
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
	for _, rule := range againRuleList {
		logic, _, err = rule.Optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}

	return logic, err
}

func isLogicalRuleDisabled(r base.LogicalOptRule) bool {
	disabled := DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(r.Name())
	return disabled
}

func physicalOptimize(logic base.LogicalPlan) (plan base.PhysicalPlan, cost float64, err error) {
	if _, _, err := logic.RecursiveDeriveStats(nil); err != nil {
		return nil, 0, err
	}

	preparePossibleProperties(logic)

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	logic.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS = 0
	t, err := physicalop.FindBestTask(logic, prop)
	if err != nil {
		return nil, 0, err
	}
	if t.Invalid() {
		errMsg := "Can't find a proper physical plan for this query"
		if config.GetGlobalConfig().DisaggregatedTiFlash && !logic.SCtx().GetSessionVars().IsMPPAllowed() {
			errMsg += ": cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch"
		}
		return nil, 0, plannererrors.ErrInternal.GenWithStackByArgs(errMsg)
	}

	// collect the warnings from task.
	logic.SCtx().GetSessionVars().StmtCtx.AppendWarnings(t.(*physicalop.RootTask).Warnings.GetWarnings())

	cost, err = getPlanCost(t.Plan(), property.RootTaskType, costusage.NewDefaultPlanCostOption())
	return t.Plan(), cost, err
}

// avoidColumnEvaluatorForProjBelowUnion sets AvoidColumnEvaluator to false for the projection operator which is a child of Union operator.
func avoidColumnEvaluatorForProjBelowUnion(p base.PhysicalPlan) base.PhysicalPlan {
	iteratePhysicalPlan(p, func(p base.PhysicalPlan) bool {
		x, ok := p.(*physicalop.PhysicalUnionAll)
		if ok {
			for _, child := range x.Children() {
				if proj, ok := child.(*physicalop.PhysicalProjection); ok {
					proj.AvoidColumnEvaluator = true
				}
			}
		}
		return true
	})
	return p
}

// eliminateUnionScanAndLock set lock property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx base.PlanContext, p base.PhysicalPlan) base.PhysicalPlan {
	var pointGet *physicalop.PointGetPlan
	var batchPointGet *physicalop.BatchPointGetPlan
	var physLock *physicalop.PhysicalLock
	var unionScan *physicalop.PhysicalUnionScan
	iteratePhysicalPlan(p, func(p base.PhysicalPlan) bool {
		if len(p.Children()) > 1 {
			return false
		}
		switch x := p.(type) {
		case *physicalop.PointGetPlan:
			pointGet = x
		case *physicalop.BatchPointGetPlan:
			batchPointGet = x
		case *physicalop.PhysicalLock:
			physLock = x
		case *physicalop.PhysicalUnionScan:
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
		if lock {
			if pointGet != nil {
				pointGet.Lock = lock
				pointGet.LockWaitTime = waitTime
			} else {
				batchPointGet.Lock = lock
				batchPointGet.LockWaitTime = waitTime
			}
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
	if join, ok := p.(*logicalop.LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == base.InnerJoin || join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin
	}
	return slices.ContainsFunc(p.Children(), existsCartesianProduct)
}

// DefaultDisabledLogicalRulesList indicates the logical rules which should be banned.
var DefaultDisabledLogicalRulesList *atomic.Value

func disableReuseChunkIfNeeded(sctx base.PlanContext, plan base.PhysicalPlan) {
	if !sctx.GetSessionVars().IsAllocValid() {
		return
	}
	if disableReuseChunk, continueIterating := checkOverlongColType(sctx, plan); disableReuseChunk || !continueIterating {
		return
	}
	for _, child := range plan.Children() {
		disableReuseChunkIfNeeded(sctx, child)
	}
}

// checkOverlongColType Check if read field type is long field.
func checkOverlongColType(sctx base.PlanContext, plan base.PhysicalPlan) (skipReuseChunk bool, continueIterating bool) {
	if plan == nil {
		return false, false
	}
	switch plan.(type) {
	case *physicalop.PhysicalTableReader, *physicalop.PhysicalIndexReader,
		*physicalop.PhysicalIndexLookUpReader, *physicalop.PhysicalIndexMergeReader:
		if existsOverlongType(plan.Schema(), false) {
			sctx.GetSessionVars().ClearAlloc(nil, false)
			return true, false
		}
	case *physicalop.PointGetPlan:
		if existsOverlongType(plan.Schema(), true) {
			sctx.GetSessionVars().ClearAlloc(nil, false)
			return true, false
		}
	default:
		// Other physical operators do not read data, so we can continue to iterate.
		return false, true
	}
	// PhysicalReader and PointGet is at the root, their children are nil or on the tikv/tiflash side.
	// So we can stop iterating.
	return false, false
}

var (
	// MaxMemoryLimitForOverlongType is the memory limit for overlong type column check.
	// Why is it not 128 ?
	// Because many customers allocate a portion of memory to their management programs,
	// the actual amount of usable memory does not align to 128GB.
	// TODO: We are also lacking test data for instances with less than 128GB of memory, so we need to plan the rules here.
	// TODO: internal sql can force to use chunk reuse if we ensure the memory usage is safe.
	// TODO: We can consider the limit/Topn in the future.
	MaxMemoryLimitForOverlongType = 120 * size.GB
	maxFlenForOverlongType        = mysql.MaxBlobWidth * 2
)

// existsOverlongType Check if exists long type column.
// If pointGet is true, we will check the total Flen of all columns, if it exceeds maxFlenForOverlongType,
// we will disable chunk reuse.
// For a point get, there is only one row, so we can easily estimate the size.
// However, for a non-point get, there may be many rows, and it is impossible to determine the memory size used.
// Therefore, we can only forcibly skip the reuse chunk.
func existsOverlongType(schema *expression.Schema, pointGet bool) bool {
	if schema == nil {
		return false
	}
	totalFlen := 0
	for _, column := range schema.Columns {
		switch column.RetType.GetType() {
		case mysql.TypeLongBlob,
			mysql.TypeBlob, mysql.TypeJSON, mysql.TypeTiDBVectorFloat32:
			return true
		case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob:
			// if the column is varchar and the length of
			// the column is defined to be more than 1000,
			// the column is considered a large type and
			// disable chunk_reuse.
			if column.RetType.GetFlen() <= 1000 {
				continue
			}
			if pointGet {
				totalFlen += column.RetType.GetFlen()
				if checkOverlongTypeForPointGet(totalFlen) {
					return true
				}
				continue
			}
			return true
		}
	}
	return false
}

func checkOverlongTypeForPointGet(totalFlen int) bool {
	totalMemory, err := memory.MemTotal()
	if err != nil || totalMemory <= 0 {
		return true
	}
	if totalMemory >= MaxMemoryLimitForOverlongType {
		if totalFlen <= maxFlenForOverlongType {
			return false
		}
	}
	return true
}
