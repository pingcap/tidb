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
	"fmt"
	"math"
	"math/rand"
	"runtime/trace"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	utilparser "github.com/pingcap/tidb/util/parser"
	"github.com/pingcap/tidb/util/topsql"
	"go.uber.org/zap"
)

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.SessionVars) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(prepareStmt.PreparedAst.Stmt)
	}
	return ast.IsReadOnly(node)
}

// GetExecuteForUpdateReadIS is used to check whether the statement is `execute` and target statement has a forUpdateRead flag.
// If so, we will return the latest information schema.
func GetExecuteForUpdateReadIS(node ast.Node, sctx sessionctx.Context) infoschema.InfoSchema {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		vars := sctx.GetSessionVars()
		execID := execStmt.ExecID
		if execStmt.Name != "" {
			execID = vars.PreparedStmtNameToID[execStmt.Name]
		}
		if preparedPointer, ok := vars.PreparedStmts[execID]; ok {
			checkSchema := vars.IsIsolation(ast.ReadCommitted)
			if !checkSchema {
				preparedObj, ok := preparedPointer.(*core.CachedPrepareStmt)
				checkSchema = ok && preparedObj.ForUpdateRead
			}
			if checkSchema {
				is := domain.GetDomain(sctx).InfoSchema()
				return temptable.AttachLocalTemporaryTableInfoSchema(sctx, is)
			}
		}
	}
	return nil
}

func matchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode) (bindRecord *bindinfo.BindRecord, scope string, matched bool) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || stmtNode == nil {
		return nil, "", false
	}
	var err error
	bindRecord, scope, err = getBindRecord(sctx, stmtNode)
	if err != nil || bindRecord == nil || len(bindRecord.Bindings) == 0 {
		return nil, "", false
	}
	return bindRecord, scope, true
}

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, error) {
	sessVars := sctx.GetSessionVars()

	if !sctx.GetSessionVars().InRestrictedSQL && variable.RestrictedReadOnly.Load() || variable.VarTiDBSuperReadOnly.Load() {
		allowed, err := allowInReadOnlyMode(sctx, node)
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			return nil, nil, errors.Trace(core.ErrSQLInReadOnlyMode)
		}
	}

	// Because for write stmt, TiFlash has a different results when lock the data in point get plan. We ban the TiFlash
	// engine in not read only stmt.
	if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(node, sessVars) {
		delete(sessVars.IsolationReadEngines, kv.TiFlash)
		defer func() {
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}()
	}

	tableHints := hint.ExtractTableHintsFromStmtNode(node, sctx)
	originStmtHints, originStmtHintsOffs, warns := handleStmtHints(tableHints)
	sessVars.StmtCtx.StmtHints = originStmtHints
	for _, warn := range warns {
		sessVars.StmtCtx.AppendWarning(warn)
	}
	warns = warns[:0]
	for name, val := range originStmtHints.SetVars {
		err := variable.SetStmtVar(sessVars, name, val)
		if err != nil {
			sessVars.StmtCtx.AppendWarning(err)
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

	useBinding := sessVars.UsePlanBaselines
	stmtNode, ok := node.(ast.StmtNode)
	if !ok {
		useBinding = false
	}
	bindRecord, scope, match := matchSQLBinding(sctx, stmtNode)
	if !match {
		useBinding = false
	}
	if ok {
		// add the extra Limit after matching the bind record
		stmtNode = plannercore.TryAddExtraLimit(sctx, stmtNode)
		node = stmtNode
	}

	var (
		names                      types.NameSlice
		bestPlan, bestPlanFromBind plannercore.Plan
		chosenBinding              bindinfo.Binding
		err                        error
	)
	if useBinding {
		minCost := math.MaxFloat64
		var bindStmtHints stmtctx.StmtHints
		originHints := hint.CollectHint(stmtNode)
		// bindRecord must be not nil when coming here, try to find the best binding.
		for _, binding := range bindRecord.Bindings {
			if !binding.IsBindingEnabled() {
				continue
			}
			metrics.BindUsageCounter.WithLabelValues(scope).Inc()
			hint.BindHint(stmtNode, binding.Hint)
			curStmtHints, _, curWarns := handleStmtHints(binding.Hint.GetFirstTableHints())
			sessVars.StmtCtx.StmtHints = curStmtHints
			plan, curNames, cost, err := optimize(ctx, sctx, node, is)
			if err != nil {
				binding.Status = bindinfo.Invalid
				handleInvalidBindRecord(ctx, sctx, scope, bindinfo.BindRecord{
					OriginalSQL: bindRecord.OriginalSQL,
					Db:          bindRecord.Db,
					Bindings:    []bindinfo.Binding{binding},
				})
				continue
			}
			if cost < minCost {
				bindStmtHints, warns, minCost, names, bestPlanFromBind, chosenBinding = curStmtHints, curWarns, cost, curNames, plan, binding
			}
		}
		if bestPlanFromBind == nil {
			sessVars.StmtCtx.AppendWarning(errors.New("no plan generated from bindings"))
		} else {
			bestPlan = bestPlanFromBind
			sessVars.StmtCtx.StmtHints = bindStmtHints
			for _, warn := range warns {
				sessVars.StmtCtx.AppendWarning(warn)
			}
			if err := setFoundInBinding(sctx, true, chosenBinding.BindSQL); err != nil {
				logutil.BgLogger().Warn("set tidb_found_in_binding failed", zap.Error(err))
			}
			if sessVars.StmtCtx.InVerboseExplain {
				sessVars.StmtCtx.AppendNote(errors.Errorf("Using the bindSQL: %v", chosenBinding.BindSQL))
			}
		}
		// Restore the hint to avoid changing the stmt node.
		hint.BindHint(stmtNode, originHints)
	}
	// No plan found from the bindings, or the bindings are ignored.
	if bestPlan == nil {
		sessVars.StmtCtx.StmtHints = originStmtHints
		bestPlan, names, _, err = optimize(ctx, sctx, node, is)
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
		if _, ok := stmtNode.(*ast.SelectStmt); ok && !bindRecord.Bindings[0].Hint.ContainTableHint(plannercore.HintReadFromStorage) {
			sessVars.StmtCtx.StmtHints = originStmtHints
			defPlan, _, _, err := optimize(ctx, sctx, node, is)
			if err != nil {
				// Ignore this evolution task.
				return bestPlan, names, nil
			}
			defPlanHints := plannercore.GenHintsFromPhysicalPlan(defPlan)
			for _, hint := range defPlanHints {
				if hint.HintName.String() == plannercore.HintReadFromStorage {
					return bestPlan, names, nil
				}
			}
			// The hints generated from the plan do not contain the statement hints of the query, add them back.
			for _, off := range originStmtHintsOffs {
				defPlanHints = append(defPlanHints, tableHints[off])
			}
			defPlanHintsStr := hint.RestoreOptimizerHints(defPlanHints)
			binding := bindRecord.FindBinding(defPlanHintsStr)
			if binding == nil {
				handleEvolveTasks(ctx, sctx, bindRecord, stmtNode, defPlanHintsStr)
			}
		}
	}

	return bestPlan, names, nil
}

func allowInReadOnlyMode(sctx sessionctx.Context, node ast.Node) (bool, error) {
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
	New: func() interface{} {
		return plannercore.NewPlanBuilder()
	},
}

// optimizeCnt is a global variable only used for test.
var optimizeCnt int

func optimize(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, types.NameSlice, float64, error) {
	failpoint.Inject("checkOptimizeCountOne", func() {
		optimizeCnt++
		if optimizeCnt > 1 {
			failpoint.Return(nil, nil, 0, errors.New("gofail wrong optimizerCnt error"))
		}
	})
	failpoint.Inject("mockHighLoadForOptimize", func() {
		sqlPrefixes := []string{"select"}
		topsql.MockHighCPULoad(sctx.GetSessionVars().StmtCtx.OriginalSQL, sqlPrefixes, 10)
	})

	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	sctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = nil
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)

	failpoint.Inject("mockRandomPlanID", func() {
		sctx.GetSessionVars().PlanID = rand.Intn(1000) // nolint:gosec
	})

	builder := planBuilderPool.Get().(*plannercore.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())

	builder.Init(sctx, is, hintProcessor)

	// reset fields about rewrite
	sctx.GetSessionVars().RewritePhaseInfo.Reset()
	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, 0, err
	}
	sctx.GetSessionVars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := plannercore.VisitInfo4PrivCheck(is, node, builder.GetVisitInfo())
		if err := plannercore.CheckPrivilege(activeRoles, pm, visitInfo); err != nil {
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

// ExtractSelectAndNormalizeDigest extract the select statement and normalize it.
func ExtractSelectAndNormalizeDigest(stmtNode ast.StmtNode, specifiledDB string) (ast.StmtNode, string, string, error) {
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
	stmtNode, normalizedSQL, hash, err := ExtractSelectAndNormalizeDigest(stmt, ctx.GetSessionVars().CurrentDB)
	if err != nil || stmtNode == nil {
		return nil, "", err
	}
	sessionHandle := ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(hash, normalizedSQL, "")
	if bindRecord != nil {
		if bindRecord.HasEnabledBinding() {
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
//  3. not a cache table.
func useMaxTS(ctx sessionctx.Context, p plannercore.Plan) bool {
	if !plannercore.IsAutoCommitTxn(ctx) {
		return false
	}
	v, ok := p.(*plannercore.PointGetPlan)
	if !ok {
		return false
	}
	noSecondRead := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
	if !noSecondRead {
		return false
	}

	if v.TblInfo != nil && (v.TblInfo.TableCacheStatusType != model.TableCacheStatusDisable) {
		return false
	}
	return true
}

// OptimizeExecStmt to optimize prepare statement protocol "execute" statement
// this is a short path ONLY does things filling prepare related params
// for point select like plan which does not need extra things
func OptimizeExecStmt(ctx context.Context, sctx sessionctx.Context,
	execAst *ast.ExecuteStmt, is infoschema.InfoSchema) (plannercore.Plan, error) {
	defer trace.StartRegion(ctx, "Optimize").End()
	var err error

	builder := planBuilderPool.Get().(*plannercore.PlanBuilder)
	defer planBuilderPool.Put(builder.ResetForReuse())

	builder.Init(sctx, is, nil)
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

func handleStmtHints(hints []*ast.TableOptimizerHint) (stmtHints stmtctx.StmtHints, offs []int, warns []error) {
	if len(hints) == 0 {
		return
	}
	hintOffs := make(map[string]int, len(hints))
	var forceNthPlan *ast.TableOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxExecutionTimeCnt, forceNthPlanCnt, straightJoinHintCnt int
	setVars := make(map[string]string)
	setVarsOffs := make([]int, 0, len(hints))
	for i, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			hintOffs[hint.HintName.L] = i
			memoryQuotaHintCnt++
		case "use_toja":
			hintOffs[hint.HintName.L] = i
			useToJAHintCnt++
		case "use_cascades":
			hintOffs[hint.HintName.L] = i
			useCascadesHintCnt++
		case "no_index_merge":
			hintOffs[hint.HintName.L] = i
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			hintOffs[hint.HintName.L] = i
			readReplicaHintCnt++
		case "max_execution_time":
			hintOffs[hint.HintName.L] = i
			maxExecutionTimeCnt++
		case "nth_plan":
			forceNthPlanCnt++
			forceNthPlan = hint
		case "straight_join":
			hintOffs[hint.HintName.L] = i
			straightJoinHintCnt++
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
			setVarsOffs = append(setVarsOffs, i)
		}
	}
	stmtHints.OriginalTableHints = hints
	stmtHints.SetVars = setVars

	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		memoryQuotaHint := hints[hintOffs["memory_quota"]]
		if memoryQuotaHintCnt > 1 {
			warn := errors.Errorf("MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// Executor use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			delete(hintOffs, "memory_quota")
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
		useToJAHint := hints[hintOffs["use_toja"]]
		if useToJAHintCnt > 1 {
			warn := errors.Errorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAndAggHint = true
		stmtHints.AllowInSubqToJoinAndAgg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		useCascadesHint := hints[hintOffs["use_cascades"]]
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
	// Handle straight_join
	if straightJoinHintCnt != 0 {
		if straightJoinHintCnt > 1 {
			warn := errors.New("STRAIGHT_JOIN() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.StraightJoinOrder = true
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
		maxExecutionTime := hints[hintOffs["max_execution_time"]]
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
			warn := errors.Errorf("the hintdata for NTH_PLAN() is too small, hint ignored")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthPlan = -1
	}
	for _, off := range hintOffs {
		offs = append(offs, off)
	}
	offs = append(offs, setVarsOffs...)
	return
}

func setFoundInBinding(sctx sessionctx.Context, opt bool, bindSQL string) error {
	vars := sctx.GetSessionVars()
	vars.StmtCtx.BindSQL = bindSQL
	err := vars.SetSystemVar(variable.TiDBFoundInBinding, variable.BoolToOnOff(opt))
	return err
}

func init() {
	plannercore.OptimizeAstNode = Optimize
	plannercore.IsReadOnly = IsReadOnly
}
