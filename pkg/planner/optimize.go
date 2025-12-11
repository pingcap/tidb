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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/topsql"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// getPlanFromNonPreparedPlanCache tries to get an available cached plan from the NonPrepared Plan Cache for this stmt.
func getPlanFromNonPreparedPlanCache(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (p base.Plan, ns types.NameSlice, ok bool, err error) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	stmt, isStmtNode := node.Node.(ast.StmtNode)
	_, isExplain := stmt.(*ast.ExplainStmt)
	if !sctx.GetSessionVars().EnableNonPreparedPlanCache || // disabled
		!isStmtNode ||
		stmtCtx.EnableOptimizeTrace || // in trace
		stmtCtx.InRestrictedSQL || // is internal SQL
		isExplain || // explain external
		!sctx.GetSessionVars().DisableTxnAutoRetry || // txn-auto-retry
		sctx.GetSessionVars().InMultiStmts { // in multi-stmt
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
	defer tracing.StartRegion(ctx, "planner.Optimize").End()
	sessVars := sctx.GetSessionVars()
	pctx := sctx.GetPlanCtx()

	if !sessVars.InRestrictedSQL && (vardef.RestrictedReadOnly.Load() || vardef.VarTiDBSuperReadOnly.Load()) {
		allowed, err := allowInReadOnlyMode(pctx, node.Node)
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			return nil, nil, errors.Trace(plannererrors.ErrSQLInReadOnlyMode)
		}
	}

	defer func() {
		if retErr == nil {
			// Override the resource group if the hint is set.
			// resource group name is case-insensitive. so we need to convert it to lower case.
			lowerRgName := strings.ToLower(sessVars.StmtCtx.StmtHints.ResourceGroup)
			if sessVars.StmtCtx.StmtHints.HasResourceGroup {
				if vardef.EnableResourceControl.Load() {
					hasPriv := true
					// only check dynamic privilege when strict-mode is enabled.
					if vardef.EnableResourceControlStrictMode.Load() {
						checker := privilege.GetPrivilegeManager(sctx)
						if checker != nil {
							hasRgAdminPriv := checker.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "RESOURCE_GROUP_ADMIN", false)
							hasRgUserPriv := checker.RequestDynamicVerification(sctx.GetSessionVars().ActiveRoles, "RESOURCE_GROUP_USER", false)
							hasPriv = hasRgAdminPriv || hasRgUserPriv
						}
					}
					if hasPriv {
						sessVars.StmtCtx.ResourceGroupName = lowerRgName
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

			// Handle SetVars hints for cached plans.
			for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
				oldV, err := sessVars.SetSystemVarWithOldStateAsRet(name, val)
				if err != nil {
					sessVars.StmtCtx.AppendWarning(err)
				}
				sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
			}
		}
	}()

	// Handle the execute statement.  This calls into the prepared plan cache.
	if _, ok := node.Node.(*ast.ExecuteStmt); ok {
		p, names, err := OptimizeExecStmt(ctx, sctx, node, is)
		return p, names, err
	}

	return optimizeCache(ctx, sctx, node, is)
}

func optimizeCache(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (plan base.Plan, slice types.NameSlice, retErr error) {
	// Call into the non-prepared plan cache.
	cachedPlan, names, ok, err := getPlanFromNonPreparedPlanCache(ctx, sctx, node, is)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return cachedPlan, names, nil
	}

	return optimizeNoCache(ctx, sctx, node, is)
}

func optimizeNoCache(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, is infoschema.InfoSchema) (plan base.Plan, slice types.NameSlice, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = tidbutil.GetRecoverError(r)
		}
	}()
	pctx := sctx.GetPlanCtx()
	sessVars := sctx.GetSessionVars()

	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(pctx)
		defer debugtrace.LeaveContextCommon(pctx)
	}

	tableHints := hint.ExtractTableHintsFromStmtNode(node.Node, sessVars.StmtCtx)
	originStmtHints, _, warns := hint.ParseStmtHints(tableHints,
		setVarHintChecker, hypoIndexChecker(ctx, is),
		sessVars.CurrentDB, byte(kv.ReplicaReadFollower))
	sessVars.StmtCtx.StmtHints = originStmtHints
	for _, warn := range warns {
		sessVars.StmtCtx.AppendWarning(warn)
	}

	if sessVars.StmtCtx.StmtHints.IgnorePlanCache {
		sessVars.StmtCtx.SetSkipPlanCache("ignore_plan_cache hint used in SQL query")
	}

	for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
		oldV, err := sessVars.SetSystemVarWithOldStateAsRet(name, val)
		if err != nil {
			sessVars.StmtCtx.AppendWarning(err)
		}
		sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
	}

	// XXX: this should be handled after any bindings are setup.
	if sessVars.SQLMode.HasStrictMode() && !core.IsReadOnly(node.Node, sessVars) {
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

	enableUseBinding := sessVars.UsePlanBaselines
	stmtNode, isStmtNode := node.Node.(ast.StmtNode)
	binding, match, _ := bindinfo.MatchSQLBinding(sctx, stmtNode)

	useBinding := enableUseBinding && isStmtNode && match
	if isStmtNode {
		// add the extra Limit after matching the bind record
		stmtNode = core.TryAddExtraLimit(sctx, stmtNode)
		node = node.CloneWithNewNode(stmtNode)
	}

	var (
		names                      types.NameSlice
		bestPlan, bestPlanFromBind base.Plan
		chosenBinding              *bindinfo.Binding
		err                        error
	)
	if useBinding {
		var bindStmtHints hint.StmtHints
		originHints := hint.CollectHint(stmtNode)
		var warns []error
		if binding != nil && binding.IsBindingEnabled() {
			if sessVars.StmtCtx.EnableOptimizerDebugTrace {
				core.DebugTraceTryBinding(pctx, binding.Hint)
			}
			hint.BindHint(stmtNode, binding.Hint)
			curStmtHints, _, curWarns := hint.ParseStmtHints(binding.Hint.GetStmtHints(),
				setVarHintChecker, hypoIndexChecker(ctx, is),
				sessVars.CurrentDB, byte(kv.ReplicaReadFollower))
			sessVars.StmtCtx.StmtHints = curStmtHints

			if sessVars.StmtCtx.StmtHints.IgnorePlanCache {
				sessVars.StmtCtx.SetSkipPlanCache("ignore_plan_cache hint used in SQL binding")
			}

			for name, val := range sessVars.StmtCtx.StmtHints.SetVars {
				oldV, err := sessVars.SetSystemVarWithOldStateAsRet(name, val)
				if err != nil {
					sessVars.StmtCtx.AppendWarning(err)
				}
				sessVars.StmtCtx.AddSetVarHintRestore(name, oldV)
			}

			plan, curNames, _, err := optimize(ctx, pctx, node, is)
			if err != nil {
				sessVars.StmtCtx.AppendWarning(errors.Errorf("binding %s failed: %v", binding.BindSQL, err))
			}
			bindStmtHints, warns, names, bestPlanFromBind, chosenBinding = curStmtHints, curWarns, curNames, plan, binding
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
			if originStmtHints.QueryHasHints {
				sessVars.StmtCtx.AppendWarning(errors.NewNoStackErrorf("The system ignores the hints in the current query and uses the hints specified in the bindSQL: %v", chosenBinding.BindSQL))
			}
		}
		// Restore the hint to avoid changing the stmt node.
		hint.BindHint(stmtNode, originHints)
	}

	// postpone Warmup because binding may change the behaviour, like pipelined DML
	if err = pctx.AdviseTxnWarmup(); err != nil {
		return nil, nil, err
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
		if _, ok := stmtNode.(*ast.SelectStmt); ok && !binding.Hint.ContainTableHint(hint.HintReadFromStorage) {
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
	// Passing false allows global variables updates in read-only mode.
	return core.IsReadOnlyInternal(node, vars, false), nil
}

var planBuilderPool = sync.Pool{
	New: func() any {
		return core.NewPlanBuilder()
	},
}

// optimizeCnt is a global variable only used for test.
var optimizeCnt int

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

	// build logical plan
	hintProcessor := hint.NewQBHintHandler(sctx.GetSessionVars().StmtCtx)
	node.Node.Accept(hintProcessor)
	defer hintProcessor.HandleUnusedViewHints()
	builder := planBuilderPool.Get().(*core.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())
	builder.Init(sctx, is, hintProcessor)
	p, err := buildLogicalPlan(ctx, sctx, node, builder)
	if err != nil {
		return nil, nil, 0, err
	}

	activeRoles := sessVars.ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := core.VisitInfo4PrivCheck(ctx, is, node.Node, builder.GetVisitInfo())
		if err := core.CheckPrivilege(activeRoles, pm, visitInfo); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := core.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	if err := core.CheckTableMode(node); err != nil {
		return nil, nil, 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(base.LogicalPlan)
	if !isLogicalPlan {
		return p, names, 0, nil
	}

	core.RecheckCTE(logic)

	beginOpt := time.Now()
	finalPlan, cost, err := core.DoOptimize(ctx, sctx, builder.GetOptFlag(), logic)
	// TODO: capture plan replayer here if it matches sql and plan digest

	sessVars.DurationOptimization = time.Since(beginOpt)
	return finalPlan, names, cost, err
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
	sctx.GetSessionVars().PlanID.Store(0)
	sctx.GetSessionVars().PlanColumnID.Store(0)
	sctx.GetSessionVars().MapScalarSubQ = nil
	sctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = nil

	failpoint.Inject("mockRandomPlanID", func() {
		sctx.GetSessionVars().PlanID.Store(rand.Int31n(1000)) // nolint:gosec
	})

	// reset fields about rewrite
	sctx.GetSessionVars().RewritePhaseInfo.Reset()
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

// setVarHintChecker checks whether the variable name in set_var hint is valid.
func setVarHintChecker(varName, hint string) (ok bool, warning error) {
	sysVar := variable.GetSysVar(varName)
	if sysVar == nil { // no such a variable
		return false, plannererrors.ErrUnresolvedHintName.FastGenByArgs(varName, hint)
	}
	if !sysVar.IsHintUpdatableVerified {
		warning = plannererrors.ErrNotHintUpdatable.FastGenByArgs(varName)
	}
	return true, warning
}

func hypoIndexChecker(ctx context.Context, is infoschema.InfoSchema) func(db, tbl, col ast.CIStr) (colOffset int, err error) {
	return func(db, tbl, col ast.CIStr) (colOffset int, err error) {
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
	plan, _, err := Optimize(context.Background(), sctx, nodeW, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return 0, err
	}
	pp, ok := plan.(base.PhysicalPlan)
	if !ok {
		return 0, errors.Errorf("plan is not a physical plan: %T", plan)
	}
	return core.GetPlanCost(pp, property.RootTaskType, costusage.NewDefaultPlanCostOption())
}

func calculatePlanDigestFunc(sctx sessionctx.Context, stmt ast.StmtNode) (planDigest string, err error) {
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(stmt)
	err = core.Preprocess(
		context.Background(),
		sctx,
		nodeW,
		core.WithPreprocessorReturn(ret),
		core.InitTxnContextProvider,
	)
	if err != nil {
		return "", err
	}

	p, _, err := Optimize(context.Background(), sctx, nodeW, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return "", err
	}
	flat := core.FlattenPhysicalPlan(p, false)
	_, digest := core.NormalizeFlatPlan(flat)
	return digest.String(), nil
}

func recordRelevantOptVarsAndFixes(sctx sessionctx.Context, stmt ast.StmtNode) (varNames []string, fixIDs []uint64, err error) {
	sctx.GetSessionVars().ResetRelevantOptVarsAndFixes(true)
	defer sctx.GetSessionVars().ResetRelevantOptVarsAndFixes(false)
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(stmt)
	err = core.Preprocess(
		context.Background(),
		sctx,
		nodeW,
		core.WithPreprocessorReturn(ret),
		core.InitTxnContextProvider,
	)
	if err != nil {
		return nil, nil, err
	}

	_, _, err = Optimize(context.Background(), sctx, nodeW, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return nil, nil, err
	}

	for varName := range sctx.GetSessionVars().RelevantOptVars {
		varNames = append(varNames, varName)
	}
	sort.Strings(varNames)

	for fixID := range sctx.GetSessionVars().RelevantOptFixes {
		fixIDs = append(fixIDs, fixID)
	}
	sort.Slice(fixIDs, func(i, j int) bool {
		return fixIDs[i] < fixIDs[j]
	})
	return
}

func genBriefPlanWithSCtx(sctx sessionctx.Context, stmt ast.StmtNode) (planDigest, planHintStr string, planText [][]string, err error) {
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(stmt)
	if err = core.Preprocess(context.Background(), sctx, nodeW,
		core.WithPreprocessorReturn(ret), core.InitTxnContextProvider,
	); err != nil {
		return "", "", nil, err
	}

	p, _, err := Optimize(context.Background(), sctx, nodeW, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return "", "", nil, err
	}
	flat := core.FlattenPhysicalPlan(p, false)
	_, digest := core.NormalizeFlatPlan(flat)
	sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix = true // ignore operatorID to make the output simpler
	plan := core.ExplainFlatPlanInRowFormat(flat, types.ExplainFormatBrief, false, nil)
	hints := core.GenHintsFromFlatPlan(flat)

	return digest.String(), hint.RestoreOptimizerHints(hints), plan, nil
}

func planIDFunc(plan any) (planID int, ok bool) {
	if p, ok := plan.(base.Plan); ok {
		return p.ID(), true
	}
	return 0, false
}

func init() {
	core.OptimizeAstNode = optimizeCache
	core.OptimizeAstNodeNoCache = optimizeNoCache
	indexadvisor.QueryPlanCostHook = queryPlanCost
	bindinfo.GetBindingHandle = func(sctx sessionctx.Context) bindinfo.BindingHandle {
		dom := domain.GetDomain(sctx)
		if dom == nil {
			return nil
		}
		return dom.BindingHandle()
	}
	bindinfo.CalculatePlanDigest = calculatePlanDigestFunc
	bindinfo.RecordRelevantOptVarsAndFixes = recordRelevantOptVarsAndFixes
	bindinfo.GenBriefPlanWithSCtx = genBriefPlanWithSCtx
	stmtctx.PlanIDFunc = planIDFunc
}
