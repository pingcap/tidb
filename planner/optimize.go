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
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"context"
	"fmt"
	"math"
	"runtime/trace"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	utilparser "github.com/pingcap/tidb/util/parser"
	"go.uber.org/zap"
)

// GetPreparedStmt extract the prepared statement from the execute statement.
func GetPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (*plannercore.CachedPrepareStmt, error) {
	var ok bool
	execID := stmt.ExecID
	if stmt.Name != "" {
		if execID, ok = vars.PreparedStmtNameToID[stmt.Name]; !ok {
			return nil, plannercore.ErrStmtNotFound
		}
	}
	if preparedPointer, ok := vars.PreparedStmts[execID]; ok {
		preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
		if !ok {
			return nil, errors.Errorf("invalid CachedPrepareStmt type")
		}
		return preparedObj, nil
	}
	return nil, plannercore.ErrStmtNotFound
}

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.SessionVars) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		prepareStmt, err := GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(prepareStmt.PreparedAst.Stmt)
	}
	return ast.IsReadOnly(node)
}

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error) {
	sessVars := sctx.GetSessionVars()

	// Because for write stmt, TiFlash has a different results when lock the data in point get plan. We ban the TiFlash
	// engine in not read only stmt.
	if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(node, sessVars) {
		delete(sessVars.IsolationReadEngines, kv.TiFlash)
		defer func() {
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}()
	}

	tableHints := hint.ExtractTableHintsFromStmtNode(node, sctx)
	stmtHints, warns := handleStmtHints(tableHints)
	sessVars.StmtCtx.StmtHints = stmtHints
	for _, warn := range warns {
		sctx.GetSessionVars().StmtCtx.AppendWarning(warn)
	}
	warns = warns[:0]
	for name, val := range stmtHints.SetVars {
		err := variable.SetStmtVar(sessVars, name, val)
		if err != nil {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}

	if _, isolationReadContainTiKV := sessVars.IsolationReadEngines[kv.TiKV]; isolationReadContainTiKV {
		var fp plannercore.Plan
		if fpv, ok := sctx.Value(plannercore.PointPlanKey).(plannercore.PointPlanVal); ok {
			// point plan is already tried in a multi-statement query.
			fp = fpv.Plan
		} else {
			fp = plannercore.TryFastPlan(sctx, node)
		}
		if fp != nil {
			if !useMaxTS(sctx, fp) {
				sctx.PrepareTSFuture(ctx)
			}
			return fp, fp.OutputNames(), nil
		}
	}

	sctx.PrepareTSFuture(ctx)

	bestPlan, names, _, err := optimize(ctx, sctx, node, is)
	if err != nil {
		return nil, nil, err
	}
	if !(sessVars.UsePlanBaselines || sessVars.EvolvePlanBaselines) {
		return bestPlan, names, nil
	}
	stmtNode, ok := node.(ast.StmtNode)
	if !ok {
		return bestPlan, names, nil
	}
	bindRecord, scope, err := getBindRecord(sctx, stmtNode)
	if err != nil {
		return nil, nil, err
	}
	if bindRecord == nil {
		return bestPlan, names, nil
	}
	if sctx.GetSessionVars().SelectLimit != math.MaxUint64 {
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so plan binding is not activated"))
		return bestPlan, names, nil
	}
	err = setFoundInBinding(sctx, true)
	if err != nil {
		return nil, nil, err
	}
	bestPlanHint := plannercore.GenHintsFromPhysicalPlan(bestPlan)
	if len(bindRecord.Bindings) > 0 {
		orgBinding := bindRecord.Bindings[0] // the first is the original binding
		for _, tbHint := range tableHints {  // consider table hints which contained by the original binding
			if orgBinding.Hint.ContainTableHint(tbHint.HintName.String()) {
				bestPlanHint = append(bestPlanHint, tbHint)
			}
		}
	}
	bestPlanHintStr := hint.RestoreOptimizerHints(bestPlanHint)

	defer func() {
		sessVars.StmtCtx.StmtHints = stmtHints
		for _, warn := range warns {
			sctx.GetSessionVars().StmtCtx.AppendWarning(warn)
		}
	}()
	binding := bindRecord.FindBinding(bestPlanHintStr)
	// If the best bestPlan is in baselines, just use it.
	if binding != nil && binding.Status == bindinfo.Using {
		if sctx.GetSessionVars().UsePlanBaselines {
			stmtHints, warns = handleStmtHints(binding.Hint.GetFirstTableHints())
		}
		return bestPlan, names, nil
	}
	bestCostAmongHints := math.MaxFloat64
	var bestPlanAmongHints plannercore.Plan
	originHints := hint.CollectHint(stmtNode)
	// Try to find the best binding.
	for _, binding := range bindRecord.Bindings {
		if binding.Status != bindinfo.Using {
			continue
		}
		metrics.BindUsageCounter.WithLabelValues(scope).Inc()
		hint.BindHint(stmtNode, binding.Hint)
		curStmtHints, curWarns := handleStmtHints(binding.Hint.GetFirstTableHints())
		sctx.GetSessionVars().StmtCtx.StmtHints = curStmtHints
		plan, _, cost, err := optimize(ctx, sctx, node, is)
		if err != nil {
			binding.Status = bindinfo.Invalid
			handleInvalidBindRecord(ctx, sctx, scope, bindinfo.BindRecord{
				OriginalSQL: bindRecord.OriginalSQL,
				Db:          bindRecord.Db,
				Bindings:    []bindinfo.Binding{binding},
			})
			continue
		}
		if cost < bestCostAmongHints {
			if sctx.GetSessionVars().UsePlanBaselines {
				stmtHints, warns = curStmtHints, curWarns
			}
			bestCostAmongHints = cost
			bestPlanAmongHints = plan
		}
	}
	// 1. If it is a select query.
	// 2. If there is already a evolution task, we do not need to handle it again.
	// 3. If the origin binding contain `read_from_storage` hint, we should ignore the evolve task.
	// 4. If the best plan contain TiFlash hint, we should ignore the evolve task.
	if _, ok := stmtNode.(*ast.SelectStmt); ok &&
		sctx.GetSessionVars().EvolvePlanBaselines && binding == nil &&
		!originHints.ContainTableHint(plannercore.HintReadFromStorage) &&
		!bindRecord.Bindings[0].Hint.ContainTableHint(plannercore.HintReadFromStorage) {
		handleEvolveTasks(ctx, sctx, bindRecord, stmtNode, bestPlanHintStr)
	}
	// Restore the hint to avoid changing the stmt node.
	hint.BindHint(stmtNode, originHints)
	if sctx.GetSessionVars().UsePlanBaselines && bestPlanAmongHints != nil {
		return bestPlanAmongHints, names, nil
	}
	return bestPlan, names, nil
}

func optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, float64, error) {
	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder, _ := plannercore.NewPlanBuilder(sctx, is, hintProcessor)

	// reset fields about rewrite
	sctx.GetSessionVars().RewritePhaseInfo.Reset()
	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, 0, err
	}
	sctx.GetSessionVars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)

	if execPlan, ok := p.(*plannercore.Execute); ok {
		execID := execPlan.ExecID
		if execPlan.Name != "" {
			execID = sctx.GetSessionVars().PreparedStmtNameToID[execPlan.Name]
		}
		if preparedPointer, ok := sctx.GetSessionVars().PreparedStmts[execID]; ok {
			if preparedObj, ok := preparedPointer.(*core.CachedPrepareStmt); ok && preparedObj.ForUpdateRead {
				is = domain.GetDomain(sctx).InfoSchema()
			}
		}
	}

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := plannercore.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, p.OutputNames(), 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, names, 0, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().GetEnableCascadesPlanner() {
		finalPlan, cost, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, cost, err
	}

	beginOpt := time.Now()
	finalPlan, cost, err := plannercore.DoOptimize(ctx, sctx, builder.GetOptFlag(), logic)
	sctx.GetSessionVars().DurationOptimization = time.Since(beginOpt)
	return finalPlan, names, cost, err
}

func extractSelectAndNormalizeDigest(stmtNode ast.StmtNode, specifiledDB string) (ast.StmtNode, string, string, error) {
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x.Stmt, "", "", nil
		}
		switch x.Stmt.(type) {
		case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
			normalizeSQL := parser.Normalize(utilparser.RestoreWithDefaultDB(x.Stmt, specifiledDB, x.Text()))
			normalizeSQL = plannercore.EraseLastSemicolonInSQL(normalizeSQL)
			hash := parser.DigestNormalized(normalizeSQL)
			return x.Stmt, normalizeSQL, hash.String(), nil
		case *ast.SetOprStmt:
			plannercore.EraseLastSemicolon(x)
			var normalizeExplainSQL string
			if specifiledDB != "" {
				normalizeExplainSQL = parser.Normalize(utilparser.RestoreWithDefaultDB(x, specifiledDB, x.Text()))
			} else {
				normalizeExplainSQL = parser.Normalize(x.Text())
			}
			idx := strings.Index(normalizeExplainSQL, "select")
			parenthesesIdx := strings.Index(normalizeExplainSQL, "(")
			if parenthesesIdx != -1 && parenthesesIdx < idx {
				idx = parenthesesIdx
			}
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestNormalized(normalizeSQL)
			return x.Stmt, normalizeSQL, hash.String(), nil
		}
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		plannercore.EraseLastSemicolon(x)
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x, "", "", nil
		}
		normalizedSQL, hash := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(x, specifiledDB, x.Text()))
		return x, normalizedSQL, hash.String(), nil
	}
	return nil, "", "", nil
}

func getBindRecord(ctx sessionctx.Context, stmt ast.StmtNode) (*bindinfo.BindRecord, string, error) {
	// When the domain is initializing, the bind will be nil.
	if ctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return nil, "", nil
	}
	stmtNode, normalizedSQL, hash, err := extractSelectAndNormalizeDigest(stmt, ctx.GetSessionVars().CurrentDB)
	if err != nil || stmtNode == nil {
		return nil, "", err
	}
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(normalizedSQL, "")
	if bindRecord != nil {
		if bindRecord.HasUsingBinding() {
			return bindRecord, metrics.ScopeSession, nil
		}
		return nil, "", nil
	}
	globalHandle := domain.GetDomain(ctx).BindHandle()
	if globalHandle == nil {
		return nil, "", nil
	}
	bindRecord = globalHandle.GetBindRecord(hash, normalizedSQL, "")
	return bindRecord, metrics.ScopeGlobal, nil
}

func handleInvalidBindRecord(ctx context.Context, sctx sessionctx.Context, level string, bindRecord bindinfo.BindRecord) {
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	err := sessionHandle.DropBindRecord(bindRecord.OriginalSQL, bindRecord.Db, &bindRecord.Bindings[0])
	if err != nil {
		logutil.Logger(ctx).Info("drop session bindings failed")
	}
	if level == metrics.ScopeSession {
		return
	}

	globalHandle := domain.GetDomain(sctx).BindHandle()
	globalHandle.AddDropInvalidBindTask(&bindRecord)
}

func handleEvolveTasks(ctx context.Context, sctx sessionctx.Context, br *bindinfo.BindRecord, stmtNode ast.StmtNode, planHint string) {
	bindSQL := bindinfo.GenerateBindSQL(ctx, stmtNode, planHint, false, br.Db)
	if bindSQL == "" {
		return
	}
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	binding := bindinfo.Binding{
		BindSQL:   bindSQL,
		Status:    bindinfo.PendingVerify,
		Charset:   charset,
		Collation: collation,
		Source:    bindinfo.Evolve,
	}
	globalHandle := domain.GetDomain(sctx).BindHandle()
	globalHandle.AddEvolvePlanTask(br.OriginalSQL, br.Db, binding)
}

// useMaxTS returns true when meets following conditions:
//  1. ctx is auto commit tagged.
//  2. plan is point get by pk.
func useMaxTS(ctx sessionctx.Context, p plannercore.Plan) bool {
	if !plannercore.IsAutoCommitTxn(ctx) {
		return false
	}

	v, ok := p.(*plannercore.PointGetPlan)
	return ok && (v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle))
}

// OptimizeExecStmt to optimize prepare statement protocol "execute" statement
// this is a short path ONLY does things filling prepare related params
// for point select like plan which does not need extra things
func OptimizeExecStmt(ctx context.Context, sctx sessionctx.Context,
	execAst *ast.ExecuteStmt, is infoschema.InfoSchema) (plannercore.Plan, error) {
	defer trace.StartRegion(ctx, "Optimize").End()
	var err error
	builder, _ := plannercore.NewPlanBuilder(sctx, is, nil)
	p, err := builder.Build(ctx, execAst)
	if err != nil {
		return nil, err
	}
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err = execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return execPlan.Plan, err
	}
	err = errors.Errorf("invalid result plan type, should be Execute")
	return nil, err
}

func handleStmtHints(hints []*ast.TableOptimizerHint) (stmtHints stmtctx.StmtHints, warns []error) {
	if len(hints) == 0 {
		return
	}
	var memoryQuotaHint, useToJAHint, useCascadesHint, maxExecutionTime, forceNthPlan *ast.TableOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxExecutionTimeCnt, forceNthPlanCnt int
	setVars := make(map[string]string)
	for _, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			memoryQuotaHint = hint
			memoryQuotaHintCnt++
		case "use_toja":
			useToJAHint = hint
			useToJAHintCnt++
		case "use_cascades":
			useCascadesHint = hint
			useCascadesHintCnt++
		case "no_index_merge":
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			readReplicaHintCnt++
		case "max_execution_time":
			maxExecutionTimeCnt++
			maxExecutionTime = hint
		case "nth_plan":
			forceNthPlanCnt++
			forceNthPlan = hint
		case "set_var":
			setVarHint := hint.HintData.(ast.HintSetVar)

			// Not all session variables are permitted for use with SET_VAR
			sysVar := variable.GetSysVar(setVarHint.VarName)
			if sysVar == nil {
				warns = append(warns, plannercore.ErrUnresolvedHintName.GenWithStackByArgs(setVarHint.VarName, hint.HintName.String()))
				continue
			}
			if !sysVar.IsHintUpdatable {
				warns = append(warns, plannercore.ErrNotHintUpdatable.GenWithStackByArgs(setVarHint.VarName))
				continue
			}
			// If several hints with the same variable name appear in the same statement, the first one is applied and the others are ignored with a warning
			if _, ok := setVars[setVarHint.VarName]; ok {
				msg := fmt.Sprintf("%s(%s=%s)", hint.HintName.String(), setVarHint.VarName, setVarHint.Value)
				warns = append(warns, plannercore.ErrWarnConflictingHint.GenWithStackByArgs(msg))
				continue
			}
			setVars[setVarHint.VarName] = setVarHint.Value
		}
	}
	stmtHints.SetVars = setVars

	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		if memoryQuotaHintCnt > 1 {
			warn := errors.Errorf("MEMORY_QUOTA() s defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// Executor use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			warn := errors.New("The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)")
			warns = append(warns, warn)
		} else {
			stmtHints.HasMemQuotaHint = true
			stmtHints.MemQuotaQuery = memoryQuota
			if memoryQuota == 0 {
				warn := errors.New("Setting the MEMORY_QUOTA to 0 means no memory limit")
				warns = append(warns, warn)
			}
		}
	}
	// Handle USE_TOJA
	if useToJAHintCnt != 0 {
		if useToJAHintCnt > 1 {
			warn := errors.Errorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAndAggHint = true
		stmtHints.AllowInSubqToJoinAndAgg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		if useCascadesHintCnt > 1 {
			warn := errors.Errorf("USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(%v)", useCascadesHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasEnableCascadesPlannerHint = true
		stmtHints.EnableCascadesPlanner = useCascadesHint.HintData.(bool)
	}
	// Handle NO_INDEX_MERGE
	if noIndexMergeHintCnt != 0 {
		if noIndexMergeHintCnt > 1 {
			warn := errors.New("NO_INDEX_MERGE() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.NoIndexMergeHint = true
	}
	// Handle READ_CONSISTENT_REPLICA
	if readReplicaHintCnt != 0 {
		if readReplicaHintCnt > 1 {
			warn := errors.New("READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.HasReplicaReadHint = true
		stmtHints.ReplicaRead = byte(kv.ReplicaReadFollower)
	}
	// Handle MAX_EXECUTION_TIME
	if maxExecutionTimeCnt != 0 {
		if maxExecutionTimeCnt > 1 {
			warn := errors.Errorf("MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(%v)", maxExecutionTime.HintData.(uint64))
			warns = append(warns, warn)
		}
		stmtHints.HasMaxExecutionTime = true
		stmtHints.MaxExecutionTime = maxExecutionTime.HintData.(uint64)
	}
	// Handle NTH_PLAN
	if forceNthPlanCnt != 0 {
		if forceNthPlanCnt > 1 {
			warn := errors.Errorf("NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(%v)", forceNthPlan.HintData.(int64))
			warns = append(warns, warn)
		}
		stmtHints.ForceNthPlan = forceNthPlan.HintData.(int64)
		if stmtHints.ForceNthPlan < 1 {
			stmtHints.ForceNthPlan = -1
			warn := errors.Errorf("the hintdata for NTH_PLAN() is too small, hint ignored.")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthPlan = -1
	}
	return
}

func setFoundInBinding(sctx sessionctx.Context, opt bool) error {
	vars := sctx.GetSessionVars()
	err := vars.SetSystemVar(variable.TiDBFoundInBinding, variable.BoolToOnOff(opt))
	return err
}

func init() {
	plannercore.OptimizeAstNode = Optimize
	plannercore.IsReadOnly = IsReadOnly
}
