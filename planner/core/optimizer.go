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
	"runtime"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/lock"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	utilhint "github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, types.NameSlice, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

// IsReadOnly check whether the ast.Node is a read only statement.
var IsReadOnly func(node ast.Node, vars *variable.SessionVars) bool

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
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagCollectPredicateColumnsPoint
	flagPushDownAgg
	flagDeriveTopNFromWindow
	flagPushDownTopN
	flagSyncWaitStatsLoadPoint
	flagJoinReOrder
	flagPrunColumnsAgain
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
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&collectPredicateColumnsPoint{},
	&aggregationPushDownSolver{},
	&deriveTopNFromWindow{},
	&pushDownTopNOptimizer{},
	&syncWaitStatsLoadPoint{},
	&joinReOrderSolver{},
	&columnPruner{}, // column pruning again at last, note it will mess up the results of buildKeySolver
}

type logicalOptimizeOp struct {
	// tracer is goring to track optimize steps during rule optimizing
	tracer *tracing.LogicalOptimizeTracer
}

func defaultLogicalOptimizeOption() *logicalOptimizeOp {
	return &logicalOptimizeOp{}
}

func (op *logicalOptimizeOp) withEnableOptimizeTracer(tracer *tracing.LogicalOptimizeTracer) *logicalOptimizeOp {
	op.tracer = tracer
	return op
}

func (op *logicalOptimizeOp) appendBeforeRuleOptimize(index int, name string, before LogicalPlan) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.AppendRuleTracerBeforeRuleOptimize(index, name, before.buildPlanTrace())
}

func (op *logicalOptimizeOp) appendStepToCurrent(id int, tp string, reason, action func() string) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.AppendRuleTracerStepToCurrent(id, tp, reason(), action())
}

func (op *logicalOptimizeOp) recordFinalLogicalPlan(final LogicalPlan) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.RecordFinalLogicalPlan(final.buildPlanTrace())
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(context.Context, LogicalPlan, *logicalOptimizeOp) (LogicalPlan, error)
	name() string
}

// BuildLogicalPlanForTest builds a logical plan for testing purpose from ast.Node.
func BuildLogicalPlanForTest(ctx context.Context, sctx sessionctx.Context, node ast.Node, infoSchema infoschema.InfoSchema) (Plan, types.NameSlice, error) {
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder, _ := NewPlanBuilder().Init(sctx, infoSchema, &utilhint.BlockHintProcessor{})
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}
	return p, p.OutputNames(), err
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		if v.privilege == mysql.ExtendedPriv {
			if !pm.RequestDynamicVerification(activeRoles, v.dynamicPriv, v.dynamicWithGrant) {
				if v.err == nil {
					return ErrPrivilegeCheckFail.GenWithStackByArgs(v.dynamicPriv)
				}
				return v.err
			}
		} else if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			if v.err == nil {
				return ErrPrivilegeCheckFail.GenWithStackByArgs(v.privilege.String())
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
func CheckTableLock(ctx sessionctx.Context, is infoschema.InfoSchema, vs []visitInfo) error {
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

func checkStableResultMode(sctx sessionctx.Context) bool {
	s := sctx.GetSessionVars()
	st := s.StmtCtx
	return s.EnableStableResultMode && (!st.InInsertStmt && !st.InUpdateStmt && !st.InDeleteStmt && !st.InLoadDataStmt)
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(ctx context.Context, sctx sessionctx.Context, flag uint64, logic LogicalPlan) (PhysicalPlan, float64, error) {
	// if there is something after flagPrunColumns, do flagPrunColumnsAgain
	if flag&flagPrunColumns > 0 && flag-flagPrunColumns > flagPrunColumns {
		flag |= flagPrunColumnsAgain
	}
	if checkStableResultMode(sctx) {
		flag |= flagStabilizeResults
	}
	if sctx.GetSessionVars().StmtCtx.StraightJoinOrder {
		// When we use the straight Join Order hint, we should disable the join reorder optimization.
		flag &= ^flagJoinReOrder
	}
	flag |= flagCollectPredicateColumnsPoint
	flag |= flagSyncWaitStatsLoadPoint
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, 0, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, 0, errors.Trace(ErrCartesianProductUnsupported)
	}
	planCounter := PlanCounterTp(sctx.GetSessionVars().StmtCtx.StmtHints.ForceNthPlan)
	if planCounter == 0 {
		planCounter = -1
	}
	physical, cost, err := physicalOptimize(logic, &planCounter)
	if err != nil {
		return nil, 0, err
	}
	finalPlan, err := postOptimize(ctx, sctx, physical)
	if err != nil {
		return nil, 0, err
	}

	if sctx.GetSessionVars().StmtCtx.EnableOptimizerCETrace {
		refineCETrace(sctx)
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace {
		sctx.GetSessionVars().StmtCtx.OptimizeTracer.RecordFinalPlan(finalPlan.buildPlanTrace())
	}
	return finalPlan, cost, nil
}

// refineCETrace will adjust the content of CETrace.
// Currently, it will (1) deduplicate trace records, (2) sort the trace records (to make it easier in the tests) and (3) fill in the table name.
func refineCETrace(sctx sessionctx.Context) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	stmtCtx.OptimizerCETrace = tracing.DedupCETrace(stmtCtx.OptimizerCETrace)
	slices.SortFunc(stmtCtx.OptimizerCETrace, func(i, j *tracing.CETraceRecord) bool {
		if i == nil && j != nil {
			return true
		}
		if i == nil || j == nil {
			return false
		}

		if i.TableID != j.TableID {
			return i.TableID < j.TableID
		}
		if i.Type != j.Type {
			return i.Type < j.Type
		}
		if i.Expr != j.Expr {
			return i.Expr < j.Expr
		}
		return i.RowCount < j.RowCount
	})
	traceRecords := stmtCtx.OptimizerCETrace
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	for _, rec := range traceRecords {
		tbl, ok := is.TableByID(rec.TableID)
		if ok {
			rec.TableName = tbl.Meta().Name.O
			continue
		}
		tbl, _, _ = is.FindTableByPartitionID(rec.TableID)
		if tbl != nil {
			rec.TableName = tbl.Meta().Name.O
			continue
		}
		logutil.BgLogger().Warn("[OptimizerTrace] Failed to find table in infoschema",
			zap.Int64("table id", rec.TableID))
	}
}

// mergeContinuousSelections merge continuous selections which may occur after changing plans.
func mergeContinuousSelections(p PhysicalPlan) {
	if sel, ok := p.(*PhysicalSelection); ok {
		for {
			childSel := sel.children[0]
			if tmp, ok := childSel.(*PhysicalSelection); ok {
				sel.Conditions = append(sel.Conditions, tmp.Conditions...)
				sel.SetChild(0, tmp.children[0])
			} else {
				break
			}
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

func postOptimize(ctx context.Context, sctx sessionctx.Context, plan PhysicalPlan) (PhysicalPlan, error) {
	// some cases from update optimize will require avoiding projection elimination.
	// see comments ahead of call of DoOptimize in function of buildUpdate().
	err := prunePhysicalColumns(sctx, plan)
	if err != nil {
		return nil, err
	}
	plan = eliminatePhysicalProjection(plan)
	plan = InjectExtraProjection(plan)
	mergeContinuousSelections(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	plan = enableParallelApply(sctx, plan)
	handleFineGrainedShuffle(ctx, sctx, plan)
	propagateProbeParents(plan, nil)
	countStarRewrite(plan)
	disableReuseChunkIfNeeded(sctx, plan)
	return plan, nil
}

// prunePhysicalColumns currently only work for MPP(HashJoin<-Exchange).
// Here add projection instead of pruning columns directly for safety considerations.
// And projection is cheap here for it saves the network cost and work in memory.
func prunePhysicalColumns(sctx sessionctx.Context, plan PhysicalPlan) error {
	if tableReader, ok := plan.(*PhysicalTableReader); ok {
		if _, isExchangeSender := tableReader.tablePlan.(*PhysicalExchangeSender); isExchangeSender {
			err := prunePhysicalColumnsInternal(sctx, tableReader.tablePlan)
			if err != nil {
				return err
			}
		}
	} else {
		for _, child := range plan.Children() {
			return prunePhysicalColumns(sctx, child)
		}
	}
	return nil
}

func (p *PhysicalHashJoin) extractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, neCond := range p.NAEqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(neCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	lChild := p.children[0]
	rChild := p.children[1]
	for _, col := range parentUsedCols {
		if lChild.Schema().Contains(col) {
			leftCols = append(leftCols, col)
		} else if rChild.Schema().Contains(col) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

func prunePhysicalColumnForHashJoinChild(sctx sessionctx.Context, hashJoin *PhysicalHashJoin, joinUsedCols []*expression.Column, sender *PhysicalExchangeSender) error {
	var err error
	joinUsed := expression.GetUsedList(joinUsedCols, sender.Schema())
	hashCols := make([]*expression.Column, len(sender.HashCols))
	for i, mppCol := range sender.HashCols {
		hashCols[i] = mppCol.Col
	}
	hashUsed := expression.GetUsedList(hashCols, sender.Schema())

	needPrune := false
	usedExprs := make([]expression.Expression, len(sender.Schema().Columns))
	prunedSchema := sender.Schema().Clone()
	for i := len(joinUsed) - 1; i >= 0; i-- {
		usedExprs[i] = sender.Schema().Columns[i]
		if !joinUsed[i] && !hashUsed[i] {
			needPrune = true
			usedExprs = append(usedExprs[:i], usedExprs[i+1:]...)
			prunedSchema.Columns = append(prunedSchema.Columns[:i], prunedSchema.Columns[i+1:]...)
		}
	}

	if needPrune && len(sender.children) > 0 {
		ch := sender.children[0]
		proj := PhysicalProjection{
			Exprs: usedExprs,
		}.Init(sctx, ch.statsInfo(), ch.SelectBlockOffset())

		proj.SetSchema(prunedSchema)
		proj.SetChildren(ch)
		sender.children[0] = proj

		// Resolve Indices from bottom to up
		err = proj.ResolveIndicesItself()
		if err != nil {
			return err
		}
		err = sender.ResolveIndicesItself()
		if err != nil {
			return err
		}
		err = hashJoin.ResolveIndicesItself()
		if err != nil {
			return err
		}
	}
	return err
}

func prunePhysicalColumnsInternal(sctx sessionctx.Context, plan PhysicalPlan) error {
	var err error
	switch x := plan.(type) {
	case *PhysicalHashJoin:
		schemaColumns := x.Schema().Clone().Columns
		leftCols, rightCols := x.extractUsedCols(schemaColumns)
		matchPattern := false
		for i := 0; i <= 1; i++ {
			// Pattern: HashJoin <- ExchangeReceiver <- ExchangeSender
			matchPattern = false
			var exchangeSender *PhysicalExchangeSender
			if receiver, ok := x.children[i].(*PhysicalExchangeReceiver); ok {
				exchangeSender, matchPattern = receiver.children[0].(*PhysicalExchangeSender)
			}

			if matchPattern {
				if i == 0 {
					err = prunePhysicalColumnForHashJoinChild(sctx, x, leftCols, exchangeSender)
				} else {
					err = prunePhysicalColumnForHashJoinChild(sctx, x, rightCols, exchangeSender)
				}
				if err != nil {
					return nil
				}
			}

			/// recursively travel the physical plan
			err = prunePhysicalColumnsInternal(sctx, x.children[i])
			if err != nil {
				return nil
			}
		}
	default:
		for _, child := range x.Children() {
			err = prunePhysicalColumnsInternal(sctx, child)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
func countStarRewrite(plan PhysicalPlan) {
	countStarRewriteInternal(plan)
	if tableReader, ok := plan.(*PhysicalTableReader); ok {
		countStarRewrite(tableReader.tablePlan)
	} else {
		for _, child := range plan.Children() {
			countStarRewrite(child)
		}
	}
}

func countStarRewriteInternal(plan PhysicalPlan) {
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
					UniqueID: physicalTableScan.ctx.GetSessionVars().AllocPlanColumnID(),
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
func handleFineGrainedShuffle(ctx context.Context, sctx sessionctx.Context, plan PhysicalPlan) {
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

func setupFineGrainedShuffle(ctx context.Context, sctx sessionctx.Context, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, plan PhysicalPlan) {
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
func calculateTiFlashStreamCountUsingMinLogicalCores(ctx context.Context, sctx sessionctx.Context, serversInfo []infoschema.ServerInfo) (bool, uint64) {
	failpoint.Inject("mockTiFlashStreamCountUsingMinLogicalCores", func(val failpoint.Value) {
		intVal, err := strconv.Atoi(val.(string))
		if err == nil {
			failpoint.Return(true, uint64(intVal))
		} else {
			failpoint.Return(false, 0)
		}
	})
	rows, err := infoschema.FetchClusterServerInfoWithoutPrivilegeCheck(ctx, sctx, serversInfo, diagnosticspb.ServerInfoType_HardwareInfo, false)
	if err != nil {
		return false, 0
	}
	var initialMaxCores uint64 = 10000
	var minLogicalCores uint64 = initialMaxCores // set to a large enough value here
	for _, row := range rows {
		if row[4].GetString() == "cpu-logical-cores" {
			logicalCpus, err := strconv.Atoi(row[5].GetString())
			if err == nil && logicalCpus > 0 {
				minLogicalCores = mathutil.Min(minLogicalCores, uint64(logicalCpus))
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

func checkFineGrainedShuffleForJoinAgg(ctx context.Context, sctx sessionctx.Context, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo, exchangeColCount int, splitLimit uint64) (applyFlag bool, streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		streamCount = 4 // assume 8c node in cluster as minimal, stream count is 8 / 2 = 4
	case initialized:
		streamCount = (*streamCountInfo).itemValue
	case failed:
		return false, 0 // probably won't reach this path
	}

	var tiflashServerCount uint64 = 0
	switch (*tiflashServerCountInfo).itemStatus {
	case unInitialized:
		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx)
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

	serversInfo, err := infoschema.GetTiFlashServerInfo(sctx)
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

func inferFineGrainedShuffleStreamCountForWindow(ctx context.Context, sctx sessionctx.Context, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) (streamCount uint64) {
	switch (*streamCountInfo).itemStatus {
	case unInitialized:
		if (*tiflashServerCountInfo).itemStatus == failed {
			setDefaultStreamCount(streamCountInfo)
			streamCount = (*streamCountInfo).itemValue
			break
		}

		serversInfo, err := infoschema.GetTiFlashServerInfo(sctx)
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

func setupFineGrainedShuffleInternal(ctx context.Context, sctx sessionctx.Context, plan PhysicalPlan, helper *fineGrainedShuffleHelper, streamCountInfo *tiflashClusterInfo, tiflashServerCountInfo *tiflashClusterInfo) {
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
func propagateProbeParents(plan PhysicalPlan, probeParents []PhysicalPlan) {
	plan.setProbeParents(probeParents)
	switch x := plan.(type) {
	case *PhysicalApply, *PhysicalIndexJoin, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin:
		if join, ok := plan.(interface{ getInnerChildIdx() int }); ok {
			propagateProbeParents(plan.Children()[1-join.getInnerChildIdx()], probeParents)

			// The core logic of this method:
			// Record every Apply and Index Join we met, record it in a slice, and set it in their inner children.
			newParents := make([]PhysicalPlan, len(probeParents), len(probeParents)+1)
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

func enableParallelApply(sctx sessionctx.Context, plan PhysicalPlan) PhysicalPlan {
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
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("Some apply operators can not be executed in parallel"))
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
func LogicalOptimizeTest(ctx context.Context, flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	return logicalOptimize(ctx, flag, logic)
}

func logicalOptimize(ctx context.Context, flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	opt := defaultLogicalOptimizeOption()
	vars := logic.SCtx().GetSessionVars()
	if vars.StmtCtx.EnableOptimizeTrace {
		vars.StmtCtx.OptimizeTracer = &tracing.OptimizeTracer{}
		tracer := &tracing.LogicalOptimizeTracer{
			Steps: make([]*tracing.LogicalRuleOptimizeTracer, 0),
		}
		opt = opt.withEnableOptimizeTracer(tracer)
		defer func() {
			vars.StmtCtx.OptimizeTracer.Logical = tracer
		}()
	}
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalRuleDisabled(rule) {
			continue
		}
		opt.appendBeforeRuleOptimize(i, rule.name(), logic)
		logic, err = rule.optimize(ctx, logic, opt)
		if err != nil {
			return nil, err
		}
	}
	opt.recordFinalLogicalPlan(logic)
	return logic, err
}

func isLogicalRuleDisabled(r logicalOptRule) bool {
	disabled := DefaultDisabledLogicalRulesList.Load().(set.StringSet).Exist(r.name())
	return disabled
}

func physicalOptimize(logic LogicalPlan, planCounter *PlanCounterTp) (plan PhysicalPlan, cost float64, err error) {
	if _, err := logic.recursiveDeriveStats(nil); err != nil {
		return nil, 0, err
	}

	preparePossibleProperties(logic)

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	opt := defaultPhysicalOptimizeOption()
	stmtCtx := logic.SCtx().GetSessionVars().StmtCtx
	if stmtCtx.EnableOptimizeTrace {
		tracer := &tracing.PhysicalOptimizeTracer{
			PhysicalPlanCostDetails: make(map[int]*tracing.PhysicalPlanCostDetail),
			Candidates:              make(map[int]*tracing.CandidatePlanTrace),
		}
		opt = opt.withEnableOptimizeTracer(tracer)
		defer func() {
			if err == nil {
				tracer.RecordFinalPlanTrace(plan.buildPlanTrace())
				stmtCtx.OptimizeTracer.Physical = tracer
			}
		}()
	}

	logic.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS = 0
	t, _, err := logic.findBestTask(prop, planCounter, opt)
	if err != nil {
		return nil, 0, err
	}
	if *planCounter > 0 {
		logic.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("The parameter of nth_plan() is out of range"))
	}
	if t.invalid() {
		return nil, 0, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	if err = t.plan().ResolveIndices(); err != nil {
		return nil, 0, err
	}
	cost, err = getPlanCost(t.plan(), property.RootTaskType, NewDefaultPlanCostOption())
	return t.plan(), cost, err
}

// eliminateUnionScanAndLock set lock property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx sessionctx.Context, p PhysicalPlan) PhysicalPlan {
	var pointGet *PointGetPlan
	var batchPointGet *BatchPointGetPlan
	var physLock *PhysicalLock
	var unionScan *PhysicalUnionScan
	iteratePhysicalPlan(p, func(p PhysicalPlan) bool {
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
	return transformPhysicalPlan(p, func(p PhysicalPlan) PhysicalPlan {
		if p == physLock {
			return p.Children()[0]
		}
		if p == unionScan {
			return p.Children()[0]
		}
		return p
	})
}

func iteratePhysicalPlan(p PhysicalPlan, f func(p PhysicalPlan) bool) {
	if !f(p) {
		return
	}
	for _, child := range p.Children() {
		iteratePhysicalPlan(child, f)
	}
}

func transformPhysicalPlan(p PhysicalPlan, f func(p PhysicalPlan) PhysicalPlan) PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = transformPhysicalPlan(child, f)
	}
	return f(p)
}

func existsCartesianProduct(p LogicalPlan) bool {
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

func init() {
	expression.EvalAstExpr = evalAstExpr
	expression.RewriteAstExpr = rewriteAstExpr
	DefaultDisabledLogicalRulesList = new(atomic.Value)
	DefaultDisabledLogicalRulesList.Store(set.NewStringSet())
}

func disableReuseChunkIfNeeded(sctx sessionctx.Context, plan PhysicalPlan) {
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
func checkOverlongColType(sctx sessionctx.Context, plan PhysicalPlan) bool {
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
