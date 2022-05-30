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
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
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
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagCollectPredicateColumnsPoint
	flagPushDownAgg
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
	&aggregationEliminator{},
	&projectionEliminator{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&collectPredicateColumnsPoint{},
	&aggregationPushDownSolver{},
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
	finalPlan := postOptimize(sctx, physical)

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
	sort.Slice(stmtCtx.OptimizerCETrace, func(i, j int) bool {
		if stmtCtx.OptimizerCETrace[i] == nil && stmtCtx.OptimizerCETrace[j] != nil {
			return true
		}
		if stmtCtx.OptimizerCETrace[i] == nil || stmtCtx.OptimizerCETrace[j] == nil {
			return false
		}

		if stmtCtx.OptimizerCETrace[i].TableID != stmtCtx.OptimizerCETrace[j].TableID {
			return stmtCtx.OptimizerCETrace[i].TableID < stmtCtx.OptimizerCETrace[j].TableID
		}
		if stmtCtx.OptimizerCETrace[i].Type != stmtCtx.OptimizerCETrace[j].Type {
			return stmtCtx.OptimizerCETrace[i].Type < stmtCtx.OptimizerCETrace[j].Type
		}
		if stmtCtx.OptimizerCETrace[i].Expr != stmtCtx.OptimizerCETrace[j].Expr {
			return stmtCtx.OptimizerCETrace[i].Expr < stmtCtx.OptimizerCETrace[j].Expr
		}
		return stmtCtx.OptimizerCETrace[i].RowCount < stmtCtx.OptimizerCETrace[j].RowCount
	})
	traceRecords := stmtCtx.OptimizerCETrace
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	for _, rec := range traceRecords {
		tbl, ok := is.TableByID(rec.TableID)
		if !ok {
			logutil.BgLogger().Warn("[OptimizerTrace] Failed to find table in infoschema",
				zap.Int64("table id", rec.TableID))
		}
		rec.TableName = tbl.Meta().Name.O
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

func postOptimize(sctx sessionctx.Context, plan PhysicalPlan) PhysicalPlan {
	plan = eliminatePhysicalProjection(plan)
	plan = InjectExtraProjection(plan)
	mergeContinuousSelections(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	plan = enableParallelApply(sctx, plan)
	checkPlanCacheable(sctx, plan)
	return plan
}

// checkPlanCacheable used to check whether a plan can be cached. Plans that
// meet the following characteristics cannot be cached:
// 1. Use the TiFlash engine.
// Todo: make more careful check here.
func checkPlanCacheable(sctx sessionctx.Context, plan PhysicalPlan) {
	if sctx.GetSessionVars().StmtCtx.UseCache && useTiFlash(plan) {
		sctx.GetSessionVars().StmtCtx.SkipPlanCache = true
	}
}

// useTiFlash used to check whether the plan use the TiFlash engine.
func useTiFlash(p PhysicalPlan) bool {
	switch x := p.(type) {
	case *PhysicalTableReader:
		switch x.StoreType {
		case kv.TiFlash:
			return true
		default:
			return false
		}
	default:
		if len(p.Children()) > 0 {
			for _, plan := range p.Children() {
				return useTiFlash(plan)
			}
		}
	}
	return false
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
		tracer := &tracing.PhysicalOptimizeTracer{State: make(map[string]map[string]*tracing.PlanTrace)}
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

	err = t.plan().ResolveIndices()
	return t.plan(), t.cost(), err
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
