// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
)

// PlanCacheKeyTestIssue43667 is only for test.
type PlanCacheKeyTestIssue43667 struct{}

// PlanCacheKeyTestIssue46760 is only for test.
type PlanCacheKeyTestIssue46760 struct{}

// PlanCacheKeyTestIssue47133 is only for test.
type PlanCacheKeyTestIssue47133 struct{}

// SetParameterValuesIntoSCtx sets these parameters into session context.
func SetParameterValuesIntoSCtx(sctx base.PlanContext, isNonPrep bool, markers []ast.ParamMarkerExpr, params []expression.Expression) error {
	vars := sctx.GetSessionVars()
	vars.PlanCacheParams.Reset()
	for i, usingParam := range params {
		val, err := usingParam.Eval(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return err
		}
		if isGetVarBinaryLiteral(sctx, usingParam) {
			binVal, convErr := val.ToBytes()
			if convErr != nil {
				return convErr
			}
			val.SetBinaryLiteral(binVal)
		}
		if markers != nil {
			param := markers[i].(*driver.ParamMarkerExpr)
			param.Datum = val
			param.InExecute = true
		}
		vars.PlanCacheParams.Append(val)
	}
	if vars.StmtCtx.EnableOptimizerDebugTrace && len(vars.PlanCacheParams.AllParamValues()) > 0 {
		vals := vars.PlanCacheParams.AllParamValues()
		valStrs := make([]string, len(vals))
		for i, val := range vals {
			valStrs[i] = val.String()
		}
		debugtrace.RecordAnyValuesWithNames(sctx, "Parameter datums for EXECUTE", valStrs)
	}
	vars.PlanCacheParams.SetForNonPrepCache(isNonPrep)
	return nil
}

func planCachePreprocess(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema, stmt *PlanCacheStmt, params []expression.Expression) error {
	vars := sctx.GetSessionVars()
	stmtAst := stmt.PreparedAst
	vars.StmtCtx.StmtType = stmtAst.StmtType

	// step 1: check parameter number
	if len(stmt.Params) != len(params) {
		return errors.Trace(plannererrors.ErrWrongParamCount)
	}

	// step 2: set parameter values
	if err := SetParameterValuesIntoSCtx(sctx.GetPlanCtx(), isNonPrepared, stmt.Params, params); err != nil {
		return errors.Trace(err)
	}

	// step 3: add metadata lock and check each table's schema version
	schemaNotMatch := false
	for i := 0; i < len(stmt.dbName); i++ {
		_, ok := is.TableByID(stmt.tbls[i].Meta().ID)
		if !ok {
			tblByName, err := is.TableByName(stmt.dbName[i], stmt.tbls[i].Meta().Name)
			if err != nil {
				return plannererrors.ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
			}
			delete(stmt.RelateVersion, stmt.tbls[i].Meta().ID)
			stmt.tbls[i] = tblByName
			stmt.RelateVersion[tblByName.Meta().ID] = tblByName.Meta().Revision
		}
		newTbl, err := tryLockMDLAndUpdateSchemaIfNecessary(sctx.GetPlanCtx(), stmt.dbName[i], stmt.tbls[i], is)
		if err != nil {
			schemaNotMatch = true
			continue
		}
		if stmt.tbls[i].Meta().Revision != newTbl.Meta().Revision {
			schemaNotMatch = true
		}
		stmt.tbls[i] = newTbl
		stmt.RelateVersion[newTbl.Meta().ID] = newTbl.Meta().Revision
	}

	// step 4: check schema version
	if schemaNotMatch || stmt.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schema version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schema version like prepared plan cache key
		stmt.PointGet.pointPlan = nil
		stmt.PointGet.columnNames = nil
		stmt.PointGet.pointPlanHints = nil
		stmt.PointGet.Executor = nil
		stmt.PointGet.ColumnInfos = nil
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		// Example:
		// When running update in prepared statement's schema version distinguished from the one of execute statement
		// We should reset the tableRefs in the prepared update statements, otherwise, the ast nodes still hold the old
		// tableRefs columnInfo which will cause chaos in logic of trying point get plan. (should ban non-public column)
		ret := &PreprocessorReturn{InfoSchema: is}
		err := Preprocess(ctx, sctx, stmtAst.Stmt, InPrepare, WithPreprocessorReturn(ret))
		if err != nil {
			return plannererrors.ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		stmt.SchemaVersion = is.SchemaMetaVersion()
	}

	// step 5: handle expiration
	// If the lastUpdateTime less than expiredTimeStamp4PC,
	// it means other sessions have executed 'admin flush instance plan_cache'.
	// So we need to clear the current session's plan cache.
	// And update lastUpdateTime to the newest one.
	expiredTimeStamp4PC := domain.GetDomain(sctx).ExpiredTimeStamp4PC()
	if stmt.StmtCacheable && expiredTimeStamp4PC.Compare(vars.LastUpdateTime4PC) > 0 {
		sctx.GetSessionPlanCache().DeleteAll()
		vars.LastUpdateTime4PC = expiredTimeStamp4PC
	}

	return nil
}

// GetPlanFromSessionPlanCache is the entry point of Plan Cache.
// It tries to get a valid cached plan from this session's plan cache.
// If there is no such a plan, it'll call the optimizer to generate a new one.
// isNonPrepared indicates whether to use the non-prepared plan cache or the prepared plan cache.
func GetPlanFromSessionPlanCache(ctx context.Context, sctx sessionctx.Context,
	isNonPrepared bool, is infoschema.InfoSchema, stmt *PlanCacheStmt,
	params []expression.Expression) (plan base.Plan, names []*types.FieldName, err error) {
	if err := planCachePreprocess(ctx, sctx, isNonPrepared, is, stmt, params); err != nil {
		return nil, nil, err
	}

	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	cacheEnabled := false
	if isNonPrepared {
		stmtCtx.SetCacheType(contextutil.SessionNonPrepared)
		cacheEnabled = sctx.GetSessionVars().EnableNonPreparedPlanCache // plan-cache might be disabled after prepare.
	} else {
		stmtCtx.SetCacheType(contextutil.SessionPrepared)
		cacheEnabled = sctx.GetSessionVars().EnablePreparedPlanCache
	}
	if stmt.StmtCacheable && cacheEnabled {
		stmtCtx.EnablePlanCache()
	}
	if stmt.UncacheableReason != "" {
		stmtCtx.WarnSkipPlanCache(stmt.UncacheableReason)
	}

	var bindSQL string
	if stmtCtx.UseCache() {
		var ignoreByBinding bool
		bindSQL, ignoreByBinding = bindinfo.MatchSQLBindingForPlanCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo)
		if ignoreByBinding {
			stmtCtx.SetSkipPlanCache("ignore plan cache by binding")
		}
	}

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64

	if stmtCtx.UseCache() {
		if sctx.GetSessionVars().IsIsolation(ast.ReadCommitted) || stmt.ForUpdateRead {
			// In Rc or ForUpdateRead, we should check if the information schema has been changed since
			// last time. If it changed, we should rebuild the plan. Here, we use a different and more
			// up-to-date schema version which can lead plan cache miss and thus, the plan will be rebuilt.
			latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
		}
		if cacheKey, err = NewPlanCacheKey(sctx.GetSessionVars(), stmt.StmtText,
			stmt.StmtDB, stmt.SchemaVersion, latestSchemaVersion, bindSQL, expression.ExprPushDownBlackListReloadTimeStamp.Load(), stmt.RelateVersion); err != nil {
			return nil, nil, err
		}
	}

	var matchOpts *utilpc.PlanCacheMatchOpts
	if stmtCtx.UseCache() {
		var cacheVal kvcache.Value
		var hit, isPointPlan bool
		if stmt.PointGet.pointPlan != nil { // if it's PointGet Plan, no need to use MatchOpts
			cacheVal = &PlanCacheValue{
				Plan:          stmt.PointGet.pointPlan,
				OutputColumns: stmt.PointGet.columnNames,
				stmtHints:     stmt.PointGet.pointPlanHints,
			}
			isPointPlan, hit = true, true
		} else {
			matchOpts = GetMatchOpts(sctx, is, stmt, params)
			cacheVal, hit = sctx.GetSessionPlanCache().Get(cacheKey, matchOpts)
		}
		if hit {
			if plan, names, ok, err := adjustCachedPlan(sctx, cacheVal.(*PlanCacheValue), isNonPrepared, isPointPlan, cacheKey, bindSQL, is, stmt); err != nil || ok {
				return plan, names, err
			}
		}
	}
	if matchOpts == nil {
		matchOpts = GetMatchOpts(sctx, is, stmt, params)
	}

	return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, latestSchemaVersion, bindSQL, matchOpts)
}

func adjustCachedPlan(sctx sessionctx.Context, cachedVal *PlanCacheValue, isNonPrepared, isPointPlan bool,
	cacheKey kvcache.Key, bindSQL string, is infoschema.InfoSchema, stmt *PlanCacheStmt) (base.Plan,
	[]*types.FieldName, bool, error) {
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	if !isPointPlan { // keep the prior behavior
		if err := checkPreparedPriv(sctx, stmt, is); err != nil {
			return nil, nil, false, err
		}
	}
	for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
		if !unionScan && tableHasDirtyContent(sctx.GetPlanCtx(), tblInfo) {
			// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
			// rebuilding the filters in UnionScan is pretty trivial.
			sctx.GetSessionPlanCache().Delete(cacheKey)
			return nil, nil, false, nil
		}
	}
	if !RebuildPlan4CachedPlan(cachedVal.Plan) {
		return nil, nil, false, nil
	}
	sessVars.FoundInPlanCache = true
	if len(bindSQL) > 0 {
		// When the `len(bindSQL) > 0`, it means we use the binding.
		// So we need to record this.
		sessVars.FoundInBinding = true
	}
	if metrics.ResettablePlanCacheCounterFortTest {
		metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
	} else {
		core_metrics.GetPlanCacheHitCounter(isNonPrepared).Inc()
	}
	stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
	stmtCtx.StmtHints = *cachedVal.stmtHints
	return cachedVal.Plan, cachedVal.OutputColumns, true, nil
}

// generateNewPlan call the optimizer to generate a new plan for current statement
// and try to add it to cache
func generateNewPlan(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema,
	stmt *PlanCacheStmt, cacheKey kvcache.Key, latestSchemaVersion int64, bindSQL string,
	matchOpts *utilpc.PlanCacheMatchOpts) (base.Plan, []*types.FieldName, error) {
	stmtAst := stmt.PreparedAst
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	core_metrics.GetPlanCacheMissCounter(isNonPrepared).Inc()
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = true
	p, names, err := OptimizeAstNode(ctx, sctx, stmtAst.Stmt, is)
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = false
	if err != nil {
		return nil, nil, err
	}

	// check whether this plan is cacheable.
	if stmtCtx.UseCache() {
		if cacheable, reason := isPlanCacheable(sctx.GetPlanCtx(), p, len(matchOpts.ParamTypes), len(matchOpts.LimitOffsetAndCount), matchOpts.HasSubQuery); !cacheable {
			stmtCtx.SetSkipPlanCache(reason)
		}
	}

	// put this plan into the plan cache.
	if stmtCtx.UseCache() {
		// rebuild key to exclude kv.TiFlash when stmt is not read only
		if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmtAst.Stmt, sessVars) {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
			if cacheKey, err = NewPlanCacheKey(sessVars, stmt.StmtText, stmt.StmtDB,
				stmt.SchemaVersion, latestSchemaVersion, bindSQL, expression.ExprPushDownBlackListReloadTimeStamp.Load(), stmt.RelateVersion); err != nil {
				return nil, nil, err
			}
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
		cached := NewPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan, matchOpts, &stmtCtx.StmtHints)
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
		sctx.GetSessionPlanCache().Put(cacheKey, cached, matchOpts)
		if _, ok := p.(*PointGetPlan); ok {
			stmt.PointGet.pointPlan = p
			stmt.PointGet.columnNames = names
			stmt.PointGet.pointPlanHints = stmtCtx.StmtHints.Clone()
		}
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// checkPreparedPriv checks the privilege of the prepared statement
func checkPreparedPriv(sctx sessionctx.Context, stmt *PlanCacheStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := VisitInfo4PrivCheck(is, stmt.PreparedAst.Stmt, stmt.VisitInfos)
		if err := CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, visitInfo); err != nil {
			return err
		}
	}
	err := CheckTableLock(sctx, is, stmt.VisitInfos)
	return err
}

// IsSafeToReusePointGetExecutor checks whether this is a PointGet Plan and safe to reuse its executor.
func IsSafeToReusePointGetExecutor(sctx sessionctx.Context, is infoschema.InfoSchema, stmt *PlanCacheStmt) bool {
	if staleread.IsStmtStaleness(sctx) {
		return false
	}
	// check auto commit
	if !IsAutoCommitTxn(sctx.GetSessionVars()) {
		return false
	}
	if stmt.SchemaVersion != is.SchemaMetaVersion() {
		return false
	}
	return true
}
