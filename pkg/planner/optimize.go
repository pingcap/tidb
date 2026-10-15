// Copyright 2018 PingCAP, Inc.
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

package planner

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cascades"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.SessionVars) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		prepareStmt, err := core.GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(prepareStmt.PreparedAst.Stmt)
	}
	return ast.IsReadOnly(node)
}

// getPlanFromNonPreparedPlanCache tries to get an available cached plan from the NonPrepared Plan Cache for this stmt.
func getPlanFromNonPreparedPlanCache(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode, is infoschema.InfoSchema) (p base.Plan, ns types.NameSlice, ok bool, err error) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	_, isExplain := stmt.(*ast.ExplainStmt)
	if !sctx.GetSessionVars().EnableNonPreparedPlanCache || // disabled
		stmtCtx.InPreparedPlanBuilding || // already in cached plan rebuilding phase
		stmtCtx.EnableOptimizerCETrace || stmtCtx.EnableOptimizeTrace || // in trace
		stmtCtx.InRestrictedSQL || // is internal SQL
		isExplain || // explain external
		!sctx.GetSessionVars().DisableTxnAutoRetry || // txn-auto-retry
		sctx.GetSessionVars().InMultiStmts || // in multi-stmt
		(stmtCtx.InExplainStmt && stmtCtx.ExplainFormat != types.ExplainFormatPlanCache) { // in explain internal
		return nil, nil, false, nil
	}
	ok, reason := core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)
	if !ok {
		if !isExplain && stmtCtx.InExplainStmt && stmtCtx.ExplainFormat == types.ExplainFormatPlanCache {
			stmtCtx.AppendWarning(errors.NewNoStackErrorf("skip non-prepared plan-cache: %s", reason))
		}
		return nil, nil, false, nil
	}

	paramSQL, paramsVals, err := core.GetParamSQLFromAST(stmt)
	if err != nil {
		return nil, nil, false, err
	}
	if intest.InTest && ctx.Value(core.PlanCacheKeyTestIssue43667{}) != nil { // update the AST in the middle of the process
		ctx.Value(core.PlanCacheKeyTestIssue43667{}).(func(stmt ast.StmtNode))(stmt)
	}
	val := sctx.GetSessionVars().GetNonPreparedPlanCacheStmt(paramSQL)
	paramExprs := core.Params2Expressions(paramsVals)

	if val == nil {
		// Create a new AST upon this parameterized SQL instead of using the original AST.
		// Keep the original AST unchanged to avoid any side effect.
		paramStmt, err := core.ParseParameterizedSQL(sctx, paramSQL)
		if err != nil {
			// This can happen rarely, cannot parse the parameterized(restored) SQL successfully, skip the plan cache in this case.
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
			return nil, nil, false, nil
		}
		// GeneratePlanCacheStmtWithAST may evaluate these parameters so set their values into SCtx in advance.
		if err := core.SetParameterValuesIntoSCtx(sctx.GetPlanCtx(), true, nil, paramExprs); err != nil {
			return nil, nil, false, err
		}
		cachedStmt, _, _, err := core.GeneratePlanCacheStmtWithAST(ctx, sctx, false, paramSQL, paramStmt, is)
		if err != nil {
			return nil, nil, false, err
		}
		sctx.GetSessionVars().AddNonPreparedPlanCacheStmt(paramSQL, cachedStmt)
		val = cachedStmt
	}
	cachedStmt := val.(*core.PlanCacheStmt)

	cachedPlan, names, err := core.GetPlanFromPlanCache(ctx, sctx, true, is, cachedStmt, paramExprs)
	if err != nil {
		return nil, nil, false, err
	}

	if intest.InTest && ctx.Value(core.PlanCacheKeyTestIssue47133{}) != nil {
		ctx.Value(core.PlanCacheKeyTestIssue47133{}).(func(names []*types.FieldName))(names)
	}

	return cachedPlan, names, true, nil
}

// Optimize does optimization and creates a Plan.
func Optimize(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (plan base.Plan, slice types.NameSlice, retErr error) {
	defer func() {
		tracing.StartRegion(ctx, "planner.Optimize").End()
		if r := recover(); r != nil {
			retErr = tidbutil.GetRecoverError(r)
		}
	}()
	sessVars := sctx.GetSessionVars()
	pctx := sctx.GetPlanCtx()
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(pctx)
		defer debugtrace.LeaveContextCommon(pctx)
	}

	if !sessVars.InRestrictedSQL && (variable.RestrictedReadOnly.Load() || variable.VarTiDBSuperReadOnly.Load()) {
		allowed, err := allowInReadOnlyMode(pctx, node.Node)
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			return nil, nil, errors.Trace(plannererrors.ErrSQLInReadOnlyMode)
		}
	}

	defer func() {
		// Override the resource group if the hint is set.
		if retErr == nil && sessVars.StmtCtx.StmtHints.HasResourceGroup {
			if variable.EnableResourceControl.Load() {
				hasPriv := true
				// only check dynamic privilege when strict-mode is enabled.
				if variable.EnableResourceControlStrictMode.Load() {
					checker := privilege.GetPrivilegeManager(sctx)
					if checker != nil {
						hasRgAdminPriv := checker.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "RESOURCE_GROUP_ADMIN", false)
						hasRgUserPriv := checker.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "RESOURCE_GROUP_USER", false)
						hasPriv = hasRgAdminPriv || hasRgUserPriv
					}
				}
				if hasPriv {
					sessVars.StmtCtx.ResourceGroupName = sessVars.StmtCtx.StmtHints.ResourceGroup
					// if we are in a txn, should update the txn resource name to let the txn
					// commit with the hint resource group.
					if txn, err := sctx.Txn(false); err == nil && txn != nil && txn.Valid() {
						kv.SetTxnResourceGroup(txn, sessVars.StmtCtx.ResourceGroupName)
					}
				} else {
					err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER or RESOURCE_GROUP_ADMIN or RESOURCE_GROUP_USER")
					sessVars.StmtCtx.AppendWarning(err)
				}
			} else {
				err := infoschema.ErrResourceGroupSupportDisabled
				sessVars.StmtCtx.AppendWarning(err)
			}
		}

		if retErr == nil {
			// Handle SetVars hints for cached plans.
			for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
				oldV, err := sessVars.SetSystemVarWithOldValAsRet(name, val)
				if err != nil {
					sessVars.StmtCtx.AppendWarning(err)
				}
				sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
			}
		}
	}()

	// Handle the execute statement. This calls into the prepared plan cache.
	if _, ok := node.Node.(*ast.ExecuteStmt); ok {
		p, names, err := OptimizeExecStmt(ctx, sctx, node, is)
		return p, names, err
	}

	tableHints := hint.ExtractTableHintsFromStmtNode(node.Node, sessVars.StmtCtx)
	originStmtHints, _, warns := hint.ParseStmtHints(tableHints,
		setVarHintChecker, hypoIndexChecker(ctx, is),
		sessVars.CurrentDB, byte(kv.ReplicaReadFollower))
	sessVars.StmtCtx.StmtHints = originStmtHints
	for _, warn := range warns {
		sessVars.StmtCtx.AppendWarning(warn)
	}

	warns = warns[:0]
	for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
		oldV, err := sessVars.SetSystemVarWithOldValAsRet(name, val)
		if err != nil {
			sessVars.StmtCtx.AppendWarning(err)
		}
		sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
	}

	// This should be handled after `set_var`
	if sessVars.SQLMode.HasStrictMode() && !IsReadOnly(node.Node, sessVars) {
		sessVars.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode = true
		_, hasTiFlashAccess := sessVars.IsolationReadEngines[kv.TiFlash]
		if hasTiFlashAccess {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
		}
		defer func() {
			sessVars.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode = false
			if hasTiFlashAccess {
				sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
			}
		}()
	}

	if _, isolationReadContainTiKV := sessVars.IsolationReadEngines[kv.TiKV]; isolationReadContainTiKV {
		var fp base.Plan
		if fpv, ok := sctx.Value(core.PointPlanKey).(core.PointPlanVal); ok {
			// point plan is already tried in a multi-statement query.
			fp = fpv.Plan
		} else {
			fp = core.TryFastPlan(pctx, node)
		}
		if fp != nil {
			return fp, fp.OutputNames(), nil
		}
	}
	if err := pctx.AdviseTxnWarmup(); err != nil {
		return nil, nil, err
	}

	enableUseBinding := sessVars.UsePlanBaselines
	stmtNode, isStmtNode := node.Node.(ast.StmtNode)
	binding, match, scope := bindinfo.MatchSQLBinding(sctx, stmtNode)
	var bindings bindinfo.Bindings
	if match {
		bindings = []bindinfo.Binding{binding}
	}

	useBinding := enableUseBinding && isStmtNode && match
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		failpoint.Inject("SetBindingTimeToZero", func(val failpoint.Value) {
			if val.(bool) && bindings != nil {
				bindings = bindings.Copy()
				for i := range bindings {
					bindings[i].CreateTime = types.ZeroTime
					bindings[i].UpdateTime = types.ZeroTime
				}
			}
		})
		debugtrace.RecordAnyValuesWithNames(pctx,
			"Used binding", useBinding,
			"Enable binding", enableUseBinding,
			"IsStmtNode", isStmtNode,
			"Matched", match,
			"Scope", scope,
			"Matched bindings", bindings,
		)
	}
	if isStmtNode {
		// add the extra Limit after matching the bind record
		stmtNode = core.TryAddExtraLimit(sctx, stmtNode)
		node = node.CloneWithNewNode(stmtNode)
	}

	// try to get Plan from the NonPrepared Plan Cache
	if sessVars.EnableNonPreparedPlanCache &&
		isStmtNode &&
		!useBinding { // TODO: support binding
		cachedPlan, names, ok, err := getPlanFromNonPreparedPlanCache(ctx, sctx, stmtNode, is)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return cachedPlan, names, nil
		}
	}

	var (
		names                      types.NameSlice
		bestPlan, bestPlanFromBind base.Plan
		chosenBinding              bindinfo.Binding
		err                        error
	)
	if useBinding {
		minCost := math.MaxFloat64
		var bindStmtHints hint.StmtHints
		originHints := hint.CollectHint(stmtNode)
		// bindings must be not nil when coming here, try to find the best binding.
		for _, binding := range bindings {
			if !binding.IsBindingEnabled() {
				continue
			}
			if sessVars.StmtCtx.EnableOptimizerDebugTrace {
				core.DebugTraceTryBinding(pctx, binding.Hint)
			}
			hint.BindHint(stmtNode, binding.Hint)
			curStmtHints, _, curWarns := hint.ParseStmtHints(binding.Hint.GetStmtHints(),
				setVarHintChecker, hypoIndexChecker(ctx, is),
				sessVars.CurrentDB, byte(kv.ReplicaReadFollower))
			sessVars.StmtCtx.StmtHints = curStmtHints
			// update session var by hint /set_var/
			for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
				oldV, err := sessVars.SetSystemVarWithOldValAsRet(name, val)
				if err != nil {
					sessVars.StmtCtx.AppendWarning(err)
				}
				sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
			}
			plan, curNames, cost, err := optimize(ctx, pctx, node, is)
			if err != nil {
				binding.Status = bindinfo.Invalid
				handleInvalidBinding(ctx, pctx, scope, binding)
				continue
			}
			if cost < minCost {
				bindStmtHints, warns, minCost, names, bestPlanFromBind, chosenBinding = curStmtHints, curWarns, cost, curNames, plan, binding
			}
		}
		if bestPlanFromBind == nil {
			sessVars.StmtCtx.AppendWarning(errors.NewNoStackError("no plan generated from bindings"))
		} else {
			bestPlan = bestPlanFromBind
			sessVars.StmtCtx.StmtHints = bindStmtHints
			for _, warn := range warns {
				sessVars.StmtCtx.AppendWarning(warn)
			}
			sessVars.StmtCtx.BindSQL = chosenBinding.BindSQL
			sessVars.FoundInBinding = true
			if sessVars.StmtCtx.InVerboseExplain {
				sessVars.StmtCtx.AppendNote(errors.NewNoStackErrorf("Using the bindSQL: %v", chosenBinding.BindSQL))
			} else {
				sessVars.StmtCtx.AppendExtraNote(errors.NewNoStackErrorf("Using the bindSQL: %v", chosenBinding.BindSQL))
			}
			if len(tableHints) > 0 {
				sessVars.StmtCtx.AppendWarning(errors.NewNoStackErrorf("The system ignores the hints in the current query and uses the hints specified in the bindSQL: %v", chosenBinding.BindSQL))
			}
		}
		// Restore the hint to avoid changing the stmt node.
		hint.BindHint(stmtNode, originHints)
	}

	if sessVars.StmtCtx.EnableOptimizerDebugTrace && bestPlanFromBind != nil {
		core.DebugTraceBestBinding(pctx, chosenBinding.Hint)
	}
	// No plan found from the bindings, or the bindings are ignored.
	if bestPlan == nil {
		sessVars.StmtCtx.StmtHints = originStmtHints
		bestPlan, names, _, err = optimize(ctx, pctx, node, is)
		if err != nil {
			return nil, nil, err
		}
	}

	// Add a baseline evolution task if:
	// 1. the returned plan is from bindings;
	// 2. the query is a select statement;
	// 3. the original binding contains no read_from_storage hint;
	// 4. the plan when ignoring bindings contains no tiflash hint;
	// 5. the pending verified binding has not been added already;
	savedStmtHints := sessVars.StmtCtx.StmtHints
	defer func() {
		sessVars.StmtCtx.StmtHints = savedStmtHints
	}()
	if sessVars.EvolvePlanBaselines && bestPlanFromBind != nil &&
		sessVars.SelectLimit == math.MaxUint64 { // do not evolve this query if sql_select_limit is enabled
		// Check bestPlanFromBind firstly to avoid nil stmtNode.
		if _, ok := stmtNode.(*ast.SelectStmt); ok && !bindings[0].Hint.ContainTableHint(hint.HintReadFromStorage) {
			sessVars.StmtCtx.StmtHints = originStmtHints
			defPlan, _, _, err := optimize(ctx, pctx, node, is)
			if err != nil {
				// Ignore this evolution task.
				return bestPlan, names, nil
			}
			defPlanHints := core.GenHintsFromPhysicalPlan(defPlan)
			for _, h := range defPlanHints {
				if h.HintName.String() == hint.HintReadFromStorage {
					return bestPlan, names, nil
				}
			}
		}
	}

	return bestPlan, names, nil
}

// OptimizeForForeignKeyCascade does optimization and creates a Plan for foreign key cascade.
// Compare to Optimize, OptimizeForForeignKeyCascade only build plan by StmtNode,
// doesn't consider plan cache and plan binding, also doesn't do privilege check.
func OptimizeForForeignKeyCascade(ctx context.Context, sctx planctx.PlanContext, node *resolve.NodeW, is infoschema.InfoSchema) (base.Plan, error) {
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	hintProcessor := hint.NewQBHintHandler(sctx.GetSessionVars().StmtCtx)
	builder.Init(sctx, is, hintProcessor)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	if err := core.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, err
	}
	return p, nil
}

func allowInReadOnlyMode(sctx planctx.PlanContext, node ast.Node) (bool, error) {
	pm := privilege.GetPrivilegeManager(sctx)
	if pm == nil {
		return true, nil
	}
	roles := sctx.GetSessionVars().ActiveRoles
	// allow replication thread
	// NOTE: it is required, whether SEM is enabled or not, only user with explicit RESTRICTED_REPLICA_WRITER_ADMIN granted can ignore the restriction, so we need to surpass the case that if SEM is not enabled, SUPER will has all privileges
	if pm.HasExplicitlyGrantedDynamicPrivilege(roles, "RESTRICTED_REPLICA_WRITER_ADMIN", false) {
		return true, nil
	}

	switch node.(type) {
	// allow change variables (otherwise can't unset read-only mode)
	case *ast.SetStmt,
		// allow analyze table
		*ast.AnalyzeTableStmt,
		*ast.UseStmt,
		*ast.ShowStmt,
		*ast.CreateBindingStmt,
		*ast.DropBindingStmt,
		*ast.PrepareStmt,
		*ast.BeginStmt,
		*ast.RollbackStmt:
		return true, nil
	case *ast.CommitStmt:
		txn, err := sctx.Txn(true)
		if err != nil {
			return false, err
		}
		if !txn.IsReadOnly() {
			return false, txn.Rollback()
		}
		return true, nil
	}

	vars := sctx.GetSessionVars()
	return IsReadOnly(node, vars), nil
}

var planBuilderPool = sync.Pool{
	New: func() any {
		return core.NewPlanBuilder()
	},
}

type logicalPlanBuildCtx struct {
	stmtCtxState                stmtctx.LogicalPlanBuildState
	plannerSelectBlockAsName    *[]ast.HintTable
	plannerSelectBlockAliasInfo *[]hint.SelectBlockAlias
	mapScalarSubQ               []any
	mapHashCode2UniqueID        map[string]int
	rewritePhaseInfo            variable.RewritePhaseInfo
	// planID and planColumnID are the per-statement id allocators. buildLogicalPlan
	// resets both to 0 at the start of every round, so each round re-allocates from
	// scratch. We snapshot them here so that after picking the cost winner we can
	// restore the winning round's high-water marks; otherwise, when the first round
	// wins but the alternative round ran last and allocated fewer ids, the counters
	// would be left below the max UniqueID that is live in the returned plan.
	planID       int32
	planColumnID int64
}

type logicalPlanCandidate struct {
	plan           base.PhysicalPlan
	names          types.NameSlice
	cost           float64
	logicalPlanCtx logicalPlanBuildCtx
}

type logicalPlanRoundState struct {
	checked                   bool
	optimizeStarted           bool
	beginOpt                  time.Time
	captureBestLogicalPlanCtx bool
	best                      logicalPlanCandidate
}

func (s *logicalPlanRoundState) considerCandidate(
	sessVars *variable.SessionVars,
	plan base.PhysicalPlan,
	names types.NameSlice,
	cost float64,
) {
	if s.best.plan != nil && cost >= s.best.cost {
		return
	}
	s.best = logicalPlanCandidate{
		plan:  plan,
		names: names,
		cost:  cost,
	}
	if s.captureBestLogicalPlanCtx {
		s.best.logicalPlanCtx = saveLogicalPlanBuildCtx(sessVars)
	}
}

// resetLogicalPlanBuildRoundState resets planner state that is allocated
// independently by every logical-plan build round. Keep this function and the
// corresponding fields in logicalPlanBuildCtx and stmtctx.LogicalPlanBuildState
// in sync: every reset field whose winning value can outlive a round must also
// be saved and restored there.
func resetLogicalPlanBuildRoundState(sessVars *variable.SessionVars) {
	sessVars.PlanID.Store(0)
	sessVars.PlanColumnID.Store(0)
	sessVars.MapScalarSubQ = nil
	sessVars.MapHashCode2UniqueID4ExtendedCol = nil
	sessVars.RewritePhaseInfo.Reset()
	// These fields historically accumulated across optimize() calls. Reset them
	// only when candidate isolation is enabled, so the feature does not silently
	// change the feature-off path.
	if sessVars.EnableAlternativeLogicalPlans {
		sessVars.StmtCtx.ResetUsedStatsInfo()
		sessVars.StmtCtx.ResetIndexForce()
	}
}

func saveLogicalPlanBuildCtx(sessVars *variable.SessionVars) logicalPlanBuildCtx {
	// Note: SessionVars.FoundInPlanCache and FoundInBinding are intentionally not
	// snapshotted here. They are written only outside a build round -- by the
	// plan-cache path (core/plan_cache.go) and by binding selection in Optimize,
	// both of which run before/around optimize() rather than while building a
	// candidate -- so the alternative rounds never mutate them. If that ever
	// changes, add them here so a discarded round cannot leak a wrong
	// @@last_plan_from_cache / @@last_plan_from_binding.
	// SessionVars.UserVars is also intentionally not snapshotted. Logical-plan
	// building only sets user-variable types derived from the unchanged AST; each
	// round writes the same name and type idempotently and never changes values.
	// StatementContext.RelatedTableIDs and the transaction-scoped MDL table map
	// are intentionally not snapshotted either. Both rounds build from the same
	// AST before their optimization flags diverge, so they visit the same tables
	// and update these sets idempotently. Keeping their union is conservative: an
	// extra related table only adds a schema fence, while an acquired MDL must not
	// be rolled back as ordinary planner metadata. Revisit this choice if build
	// behavior ever starts differing between rounds.
	return logicalPlanBuildCtx{
		stmtCtxState:                sessVars.StmtCtx.SaveLogicalPlanBuildState(),
		plannerSelectBlockAsName:    sessVars.PlannerSelectBlockAsName.Load(),
		plannerSelectBlockAliasInfo: sessVars.PlannerSelectBlockAliasInfo.Load(),
		// buildLogicalPlan sets MapScalarSubQ = nil at the start of every round, so
		// each round appends into its own fresh backing array and no two rounds share
		// storage. Restoring the slice header therefore re-selects the winning round's
		// array without any risk of one round overwriting another's entries -- the
		// safety comes from that per-round reset, not from the append-only access
		// pattern alone. Extended-column IDs are mutated through a map reference, so
		// that map must be cloned.
		mapScalarSubQ:        sessVars.MapScalarSubQ,
		mapHashCode2UniqueID: maps.Clone(sessVars.MapHashCode2UniqueID4ExtendedCol),
		rewritePhaseInfo:     sessVars.RewritePhaseInfo,
		planID:               sessVars.PlanID.Load(),
		planColumnID:         sessVars.PlanColumnID.Load(),
	}
}

func restoreLogicalPlanBuildCtx(sessVars *variable.SessionVars, logicalPlanCtx logicalPlanBuildCtx) {
	sessVars.StmtCtx.RestoreLogicalPlanBuildState(logicalPlanCtx.stmtCtxState)
	sessVars.PlannerSelectBlockAsName.Store(logicalPlanCtx.plannerSelectBlockAsName)
	sessVars.PlannerSelectBlockAliasInfo.Store(logicalPlanCtx.plannerSelectBlockAliasInfo)
	sessVars.MapScalarSubQ = logicalPlanCtx.mapScalarSubQ
	sessVars.MapHashCode2UniqueID4ExtendedCol = maps.Clone(logicalPlanCtx.mapHashCode2UniqueID)
	sessVars.RewritePhaseInfo = logicalPlanCtx.rewritePhaseInfo
	sessVars.PlanID.Store(logicalPlanCtx.planID)
	sessVars.PlanColumnID.Store(logicalPlanCtx.planColumnID)
}

func buildAndOptimizeLogicalPlanRound(
	ctx context.Context,
	sctx planctx.PlanContext,
	node *resolve.NodeW,
	is infoschema.InfoSchema,
	hintProcessor *hint.QBHintHandler,
	roundState *logicalPlanRoundState,
	optFlagAdjust func(uint64) uint64,
) (base.Plan, types.NameSlice, bool, error) {
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	viewHintsHandled := false
	defer func() {
		if !viewHintsHandled {
			builder.HandleUnusedViewHints()
		}
	}()

	builder.Init(sctx, is, hintProcessor)

	p, err := buildLogicalPlan(ctx, sctx, node, builder)
	if err != nil {
		return nil, nil, false, err
	}
	// optFlagAdjust is non-nil only for the speculative alternative round; use it
	// as the round discriminator so the failpoint fires only there.
	if optFlagAdjust != nil {
		failpoint.Inject("mockAlternativeLogicalPlanRoundError", func(val failpoint.Value) {
			if errorType, _ := val.(string); errorType == "memory" {
				failpoint.Return(nil, nil, false, exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(1))
			}
			failpoint.Return(nil, nil, false, errors.New("mock alternative logical plan round error"))
		})
	}
	names := p.OutputNames()

	if !roundState.checked {
		// Keep privilege and lock checks fail-fast. These depend on visitInfo
		// produced by the logical build, but not on the later cost winner.
		if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
			visitInfo := core.VisitInfo4PrivCheck(ctx, is, node.Node, builder.GetVisitInfo())
			if err := core.CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, visitInfo); err != nil {
				return nil, nil, false, err
			}
			if err := core.CheckLBACColumnAccess(sctx, pm, builder.GetLBACVisitInfo()); err != nil {
				return nil, nil, false, err
			}
		}

		if err := core.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
			return nil, nil, false, err
		}

		if err := core.CheckTableMode(node); err != nil {
			return nil, nil, false, err
		}
		roundState.checked = true
	}

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(base.LogicalPlan)
	if !isLogicalPlan {
		return p, names, true, nil
	}

	core.RecheckCTE(logic)

	if !roundState.optimizeStarted {
		roundState.optimizeStarted = true
		roundState.beginOpt = time.Now()
	}

	optFlag := builder.GetOptFlag()
	if optFlagAdjust != nil {
		optFlag = optFlagAdjust(optFlag)
	}

	var (
		finalPlan base.PhysicalPlan
		cost      float64
		optErr    error
	)
	if sctx.GetSessionVars().GetEnableCascadesPlanner() {
		finalPlan, cost, optErr = cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
	} else {
		finalPlan, cost, optErr = core.DoOptimize(ctx, sctx, optFlag, logic)
	}
	if optErr != nil {
		return nil, nil, false, optErr
	}

	builder.HandleUnusedViewHints()
	viewHintsHandled = true
	roundState.considerCandidate(sctx.GetSessionVars(), finalPlan, names, cost)
	return p, names, false, nil
}

// optimizeCnt is a global variable only used for test.
var optimizeCnt int

func shouldTryNonDecorrelationRound(sessVars *variable.SessionVars) bool {
	return sessVars.EnableAlternativeLogicalPlans &&
		sessVars.StmtCtx.NeedAlternativeLogicalPlanRound()
}

func shouldTryCorrelateRound(sessVars *variable.SessionVars) bool {
	return sessVars.EnableAlternativeLogicalPlans &&
		sessVars.StmtCtx.AlternativeLogicalPlanPreferCorrelate
}

// alternativeRound describes one alternative logical-plan round.
// adjustFlag adjusts the optimization flags for the round.
// enabled returns true when the round should be attempted.
// setup optionally modifies session state and returns the cleanup callback.
type alternativeRound struct {
	name       string
	adjustFlag func(uint64) uint64
	enabled    func(*variable.SessionVars) bool
	setup      func(*variable.SessionVars) func()
}

var alternativeRounds = [...]alternativeRound{
	{
		name:       "non-decorrelate",
		adjustFlag: func(flag uint64) uint64 { return flag &^ rule.FlagDecorrelate },
		enabled:    shouldTryNonDecorrelationRound,
	},
	{
		name:       "correlate",
		adjustFlag: func(flag uint64) uint64 { return flag | rule.FlagCorrelate },
		enabled:    shouldTryCorrelateRound,
		setup: func(sv *variable.SessionVars) func() {
			original := sv.EnableCorrelateSubquery
			sv.EnableCorrelateSubquery = true
			return func() {
				sv.EnableCorrelateSubquery = original
			}
		},
	},
}

func isAlternativeLogicalPlanFallbackBlockedError(err error) bool {
	// Keep this list in sync with sqlkiller.getKillError: any kill signal that
	// can surface during the alternative round must abort the statement rather
	// than be masked by the fallback.
	cause := errors.Cause(err)
	return cause == context.Canceled ||
		cause == context.DeadlineExceeded ||
		exeerrors.ErrQueryInterrupted.Equal(err) ||
		exeerrors.ErrMaxExecTimeExceeded.Equal(err) ||
		exeerrors.ErrMemoryExceedForQuery.Equal(err) ||
		exeerrors.ErrMemoryExceedForInstance.Equal(err) ||
		exeerrors.ErrResourceGroupQueryRunawayInterrupted.Equal(err)
}

func fallbackToBestPlanAfterAlternativeLogicalPlanError(
	ctx context.Context,
	sessVars *variable.SessionVars,
	err error,
	best logicalPlanCandidate,
) (base.Plan, types.NameSlice, float64, bool) {
	if best.plan == nil {
		return nil, nil, 0, false
	}
	if ctx.Err() != nil || isAlternativeLogicalPlanFallbackBlockedError(err) {
		return nil, nil, 0, false
	}
	restoreLogicalPlanBuildCtx(sessVars, best.logicalPlanCtx)
	logutil.BgLogger().Warn("alternative logical plan round failed, fallback to the best plan from previous round", zap.Error(err))
	return best.plan, best.names, best.cost, true
}

func optimize(ctx context.Context, sctx planctx.PlanContext, node *resolve.NodeW, is infoschema.InfoSchema) (base.Plan, types.NameSlice, float64, error) {
	failpoint.Inject("checkOptimizeCountOne", func(val failpoint.Value) {
		// only count the optimization for SQL with specified text
		if testSQL, ok := val.(string); ok && testSQL == node.Node.OriginalText() {
			optimizeCnt++
			if optimizeCnt > 1 {
				failpoint.Return(nil, nil, 0, errors.New("gofail wrong optimizerCnt error"))
			}
		}
	})
	failpoint.Inject("mockHighLoadForOptimize", func() {
		sqlPrefixes := []string{"select"}
		topsql.MockHighCPULoad(sctx.GetSessionVars().StmtCtx.OriginalSQL, sqlPrefixes, 10)
	})
	sessVars := sctx.GetSessionVars()
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	var roundState logicalPlanRoundState
	// DurationOptimization starts when the first logical candidate enters physical
	// optimization. Alternative logical builds after that point are included; the
	// first logical build before optimization starts is intentionally excluded.
	defer func() {
		if roundState.optimizeStarted {
			sessVars.DurationOptimization = time.Since(roundState.beginOpt)
		}
	}()

	// Build the logical plan from the raw AST. The hint processor only keeps
	// AST-derived metadata; per-build state is allocated inside PlanBuilder.
	hintProcessor := hint.NewQBHintHandler(sctx.GetSessionVars().StmtCtx)
	node.Node.Accept(hintProcessor)

	// build multi logical plan from raw AST.
	var (
		alternativeLogicalPlansEnabled = sessVars.EnableAlternativeLogicalPlans
		alternativeRoundTried          bool
	)
	var initialLogicalPlanCtx logicalPlanBuildCtx
	if alternativeLogicalPlansEnabled {
		initialLogicalPlanCtx = saveLogicalPlanBuildCtx(sessVars)
		sessVars.StmtCtx.ResetAlternativeLogicalPlanSignals()
	}

	p, names, nonLogical, err := buildAndOptimizeLogicalPlanRound(
		ctx,
		sctx,
		node,
		is,
		hintProcessor,
		&roundState,
		nil,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	if nonLogical {
		// keep compatible with the old.
		return p, names, 0, nil
	}

	// Pre-compute which rounds are enabled based on the signals from the first
	// (default) build. This prevents signal leakage: alternative rounds rebuild
	// the plan and may set AlternativeLogicalPlan* signals as a side effect,
	// which are not reset by restoreLogicalPlanBuildCtx. Evaluating enabled()
	// upfront ensures each round's eligibility is determined solely by the
	// original build's signals.
	enabledRounds := make([]alternativeRound, 0, len(alternativeRounds))
	for _, round := range alternativeRounds {
		if round.enabled(sessVars) {
			enabledRounds = append(enabledRounds, round)
		}
	}
	if len(enabledRounds) > 0 {
		alternativeRoundTried = true
		// The live state still belongs to the first-round winner. Capture it only
		// after deciding to run alternative rounds, so statements that do not need
		// another round avoid an otherwise unused full state snapshot.
		roundState.best.logicalPlanCtx = saveLogicalPlanBuildCtx(sessVars)
		roundState.captureBestLogicalPlanCtx = true
	}
	for _, round := range enabledRounds {
		restoreLogicalPlanBuildCtx(sessVars, initialLogicalPlanCtx)
		failpoint.Inject("failIfAlternativeLogicalPlanRoundTriggered", func(val failpoint.Value) {
			switch value := val.(type) {
			case bool:
				if value {
					failpoint.Return(nil, nil, 0, errors.New("unexpected alternative logical plan round"))
				}
			case string:
				if value == node.Node.OriginalText() {
					failpoint.Return(nil, nil, 0, errors.New("unexpected alternative logical plan round"))
				}
			}
		})

		// Use a closure so that defer-based cleanup runs at the end of each
		// iteration, not at function exit. This ensures session state (e.g.
		// EnableCorrelateSubquery) is restored even if the round panics.
		func() {
			if round.setup != nil {
				if cleanup := round.setup(sessVars); cleanup != nil {
					defer cleanup()
				}
			}
			p, names, nonLogical, err = buildAndOptimizeLogicalPlanRound(
				ctx,
				sctx,
				node,
				is,
				hintProcessor,
				&roundState,
				round.adjustFlag,
			)
		}()
		if err != nil {
			if _, _, _, ok := fallbackToBestPlanAfterAlternativeLogicalPlanError(ctx, sessVars, err, roundState.best); ok {
				continue
			}
			return nil, nil, 0, err
		}
		if nonLogical {
			return p, names, 0, nil
		}
	}
	if roundState.best.plan == nil {
		return nil, nil, 0, errors.New("failed to build logical plan")
	}
	if alternativeRoundTried {
		restoreLogicalPlanBuildCtx(sessVars, roundState.best.logicalPlanCtx)
	}
	return roundState.best.plan, roundState.best.names, roundState.best.cost, nil
}

// OptimizeExecStmt to handle the "execute" statement
func OptimizeExecStmt(ctx context.Context, sctx sessionctx.Context,
	execAst *resolve.NodeW, is infoschema.InfoSchema) (base.Plan, types.NameSlice, error) {
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	pctx := sctx.GetPlanCtx()
	builder.Init(pctx, is, nil)

	p, err := buildLogicalPlan(ctx, pctx, execAst, builder)
	if err != nil {
		return nil, nil, err
	}
	exec, ok := p.(*core.Execute)
	if !ok {
		return nil, nil, errors.Errorf("invalid result plan type, should be Execute")
	}
	plan, names, err := core.GetPlanFromPlanCache(ctx, sctx, false, is, exec.PrepStmt, exec.Params)
	if err != nil {
		return nil, nil, err
	}
	exec.Plan = plan
	exec.SetOutputNames(names)
	exec.Stmt = exec.PrepStmt.PreparedAst.Stmt
	return exec, names, nil
}

func buildLogicalPlan(ctx context.Context, sctx planctx.PlanContext, node *resolve.NodeW, builder *core.PlanBuilder) (base.Plan, error) {
	resetLogicalPlanBuildRoundState(sctx.GetSessionVars())

	failpoint.Inject("mockRandomPlanID", func() {
		sctx.GetSessionVars().PlanID.Store(rand.Int31n(1000)) // nolint:gosec
	})

	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	sctx.GetSessionVars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)
	if exec, ok := p.(*core.Execute); ok && exec.PrepStmt != nil {
		sctx.GetSessionVars().StmtCtx.Tables = core.GetDBTableInfo(exec.PrepStmt.VisitInfos)
	} else {
		sctx.GetSessionVars().StmtCtx.Tables = core.GetDBTableInfo(builder.GetVisitInfo())
	}
	return p, nil
}

func handleInvalidBinding(ctx context.Context, sctx planctx.PlanContext, level string, binding bindinfo.Binding) {
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
	err := sessionHandle.DropSessionBinding([]string{binding.SQLDigest})
	if err != nil {
		logutil.Logger(ctx).Info("drop session bindings failed")
	}
	if level == metrics.ScopeSession {
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	globalHandle.AddInvalidGlobalBinding(binding)
}

// setVarHintChecker checks whether the variable name in set_var hint is valid.
func setVarHintChecker(varName, hint string) (ok bool, warning error) {
	sysVar := variable.GetSysVar(varName)
	if sysVar == nil { // no such a variable
		return false, plannererrors.ErrUnresolvedHintName.FastGenByArgs(varName, hint)
	}
	// This variable selects both the plan-cache identity and PointUpdate
	// conversion semantics. Applying it after cache lookup would split one
	// execution across two modes, so SET_VAR and binding hints are deliberately
	// unsupported and ignored. Keep warning+ignore explicit rather than relying
	// on mutable SysVar hint metadata.
	if strings.EqualFold(varName, variable.TiDBEnablePlanCacheDMLParamTypeMatch) {
		return false, plannererrors.ErrNotHintUpdatable.FastGenByArgs(varName)
	}
	if !sysVar.IsHintUpdatableVerified {
		warning = plannererrors.ErrNotHintUpdatable.FastGenByArgs(varName)
	}
	return true, warning
}

func hypoIndexChecker(ctx context.Context, is infoschema.InfoSchema) func(db, tbl, col model.CIStr) (colOffset int, err error) {
	return func(db, tbl, col model.CIStr) (colOffset int, err error) {
		t, err := is.TableByName(ctx, db, tbl)
		if err != nil {
			return 0, errors.NewNoStackErrorf("table '%v.%v' doesn't exist", db, tbl)
		}
		for i, tblCol := range t.Cols() {
			if tblCol.Name.L == col.L {
				return i, nil
			}
		}
		return 0, errors.NewNoStackErrorf("can't find column %v in table %v.%v", col, db, tbl)
	}
}

// queryPlanCost returns the plan cost of this node, which is mainly for the Index Advisor.
func queryPlanCost(sctx sessionctx.Context, stmt ast.StmtNode) (float64, error) {
	nodeW := resolve.NewNodeW(stmt)
	plan, _, err := Optimize(context.Background(), sctx, nodeW, sctx.GetDomainInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return 0, err
	}
	pp, ok := plan.(base.PhysicalPlan)
	if !ok {
		return 0, errors.Errorf("plan is not a physical plan: %T", plan)
	}
	return core.GetPlanCost(pp, property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
}

func init() {
	core.OptimizeAstNode = Optimize
	core.IsReadOnly = IsReadOnly
	indexadvisor.QueryPlanCostHook = queryPlanCost
	bindinfo.GetGlobalBindingHandle = func(sctx sessionctx.Context) bindinfo.GlobalBindingHandle {
		return domain.GetDomain(sctx).BindHandle()
	}
}
