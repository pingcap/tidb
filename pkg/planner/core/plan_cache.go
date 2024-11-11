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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// PlanCacheKeyTestIssue43667 is only for test.
type PlanCacheKeyTestIssue43667 struct{}

// PlanCacheKeyTestIssue46760 is only for test.
type PlanCacheKeyTestIssue46760 struct{}

// PlanCacheKeyTestIssue47133 is only for test.
type PlanCacheKeyTestIssue47133 struct{}

// PlanCacheKeyTestClone is only for test.
type PlanCacheKeyTestClone struct{}

// PlanCacheKeyEnableInstancePlanCache is only for test.
type PlanCacheKeyEnableInstancePlanCache struct{}

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
		tbl, ok := is.TableByID(ctx, stmt.tbls[i].Meta().ID)
		if !ok {
			tblByName, err := is.TableByName(context.Background(), stmt.dbName[i], stmt.tbls[i].Meta().Name)
			if err != nil {
				return plannererrors.ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
			}
			delete(stmt.RelateVersion, stmt.tbls[i].Meta().ID)
			stmt.tbls[i] = tblByName
			stmt.RelateVersion[tblByName.Meta().ID] = tblByName.Meta().Revision
		}
		newTbl, err := tryLockMDLAndUpdateSchemaIfNecessary(ctx, sctx.GetPlanCtx(), stmt.dbName[i], stmt.tbls[i], is)
		if err != nil {
			schemaNotMatch = true
			continue
		}
		// The revision of tbl and newTbl may not be the same.
		// Example:
		// The version of stmt.tbls[i] is taken from the prepare statement and is revision v1.
		// When stmt.tbls[i] is locked in MDL, the revision of newTbl is also v1.
		// The revision of tbl is v2. The reason may have other statements trigger "tryLockMDLAndUpdateSchemaIfNecessary" before, leading to tbl revision update.
		if stmt.tbls[i].Meta().Revision != newTbl.Meta().Revision || (tbl != nil && tbl.Meta().Revision != newTbl.Meta().Revision) {
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
		stmt.PointGet.Executor = nil
		stmt.PointGet.ColumnInfos = nil
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		// Example:
		// When running update in prepared statement's schema version distinguished from the one of execute statement
		// We should reset the tableRefs in the prepared update statements, otherwise, the ast nodes still hold the old
		// tableRefs columnInfo which will cause chaos in logic of trying point get plan. (should ban non-public column)
		ret := &PreprocessorReturn{InfoSchema: is}
		nodeW := resolve.NewNodeW(stmtAst.Stmt)
		err := Preprocess(ctx, sctx, nodeW, InPrepare, WithPreprocessorReturn(ret))
		if err != nil {
			return plannererrors.ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		stmt.ResolveCtx = nodeW.GetResolveContext()
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

	// step 6: initialize the tableInfo2UnionScan, which indicates which tables are dirty.
	for _, tbl := range stmt.tbls {
		tblInfo := tbl.Meta()
		if tableHasDirtyContent(sctx.GetPlanCtx(), tblInfo) {
			sctx.GetSessionVars().StmtCtx.TblInfo2UnionScan[tblInfo] = true
		}
	}

	return nil
}

// GetPlanFromPlanCache is the entry point of Plan Cache.
// It tries to get a valid cached plan from plan cache.
// If there is no such a plan, it'll call the optimizer to generate a new one.
// isNonPrepared indicates whether to use the non-prepared plan cache or the prepared plan cache.
func GetPlanFromPlanCache(ctx context.Context, sctx sessionctx.Context,
	isNonPrepared bool, is infoschema.InfoSchema, stmt *PlanCacheStmt,
	params []expression.Expression) (plan base.Plan, names []*types.FieldName, err error) {
	if err := planCachePreprocess(ctx, sctx, isNonPrepared, is, stmt, params); err != nil {
		return nil, nil, err
	}

	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	cacheEnabled := false
	if isNonPrepared {
		stmtCtx.SetCacheType(contextutil.SessionNonPrepared)
		cacheEnabled = sessVars.EnableNonPreparedPlanCache // plan-cache might be disabled after prepare.
	} else {
		stmtCtx.SetCacheType(contextutil.SessionPrepared)
		cacheEnabled = sessVars.EnablePreparedPlanCache
	}
	if stmt.StmtCacheable && cacheEnabled {
		stmtCtx.EnablePlanCache()
	}
	if stmt.UncacheableReason != "" {
		stmtCtx.WarnSkipPlanCache(stmt.UncacheableReason)
	}

	var cacheKey, binding, reason string
	var cacheable bool
	if stmtCtx.UseCache() {
		cacheKey, binding, cacheable, reason, err = NewPlanCacheKey(sctx, stmt)
		if err != nil {
			return nil, nil, err
		}
		if !cacheable {
			stmtCtx.SetSkipPlanCache(reason)
		}
	}

	paramTypes := parseParamTypes(sctx, params)
	if stmtCtx.UseCache() {
		plan, outputCols, stmtHints, hit := lookupPlanCache(ctx, sctx, cacheKey, paramTypes)
		skipPrivCheck := stmt.PointGet.Executor != nil // this case is specially handled
		if hit && instancePlanCacheEnabled(ctx) {
			plan, hit = clonePlanForInstancePlanCache(ctx, sctx, stmt, plan)
		}
		if hit {
			if plan, ok, err := adjustCachedPlan(ctx, sctx, plan, stmtHints, isNonPrepared, skipPrivCheck, binding, is, stmt); err != nil || ok {
				return plan, outputCols, err
			}
		}
	}

	return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, paramTypes)
}

func clonePlanForInstancePlanCache(ctx context.Context, sctx sessionctx.Context,
	stmt *PlanCacheStmt, plan base.Plan) (clonedPlan base.Plan, ok bool) {
	// TODO: add metrics to record the time cost of this clone operation.
	fastPoint := stmt.PointGet.Executor != nil // this case is specially handled
	pointPlan, isPoint := plan.(*PointGetPlan)
	if fastPoint && isPoint { // special optimization for fast point plans
		if stmt.PointGet.FastPlan == nil {
			stmt.PointGet.FastPlan = new(PointGetPlan)
		}
		FastClonePointGetForPlanCache(sctx.GetPlanCtx(), pointPlan, stmt.PointGet.FastPlan)
		clonedPlan = stmt.PointGet.FastPlan
	} else {
		clonedPlan, ok = plan.CloneForPlanCache(sctx.GetPlanCtx())
		if !ok { // clone the value to solve concurrency problem
			return nil, false
		}
	}
	if intest.InTest && ctx.Value(PlanCacheKeyTestClone{}) != nil {
		ctx.Value(PlanCacheKeyTestClone{}).(func(plan, cloned base.Plan))(plan, clonedPlan)
	}
	return clonedPlan, true
}

func instancePlanCacheEnabled(ctx context.Context) bool {
	if intest.InTest && ctx.Value(PlanCacheKeyEnableInstancePlanCache{}) != nil {
		return true
	}
	enableInstancePlanCache := variable.EnableInstancePlanCache.Load()
	return enableInstancePlanCache
}

func lookupPlanCache(ctx context.Context, sctx sessionctx.Context, cacheKey string,
	paramTypes []*types.FieldType) (plan base.Plan, outputCols types.NameSlice, stmtHints *hint.StmtHints, hit bool) {
	useInstanceCache := instancePlanCacheEnabled(ctx)
	defer func(begin time.Time) {
		if hit {
			core_metrics.GetPlanCacheLookupDuration(useInstanceCache).Observe(time.Since(begin).Seconds())
		}
	}(time.Now())
	var v any
	if useInstanceCache {
		v, hit = domain.GetDomain(sctx).GetInstancePlanCache().Get(cacheKey, paramTypes)
	} else {
		v, hit = sctx.GetSessionPlanCache().Get(cacheKey, paramTypes)
	}
	if !hit {
		return nil, nil, nil, false
	}
	pcv := v.(*PlanCacheValue)
	return pcv.Plan, pcv.OutputColumns, pcv.stmtHints, true
}

func adjustCachedPlan(ctx context.Context, sctx sessionctx.Context,
	plan base.Plan, stmtHints *hint.StmtHints, isNonPrepared, skipPrivCheck bool,
	bindSQL string, is infoschema.InfoSchema, stmt *PlanCacheStmt) (
	base.Plan, bool, error) {
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	if !skipPrivCheck { // keep the prior behavior
		if err := checkPreparedPriv(ctx, sctx, stmt, is); err != nil {
			return nil, false, err
		}
	}
	if !RebuildPlan4CachedPlan(plan) {
		return nil, false, nil
	}
	sessVars.FoundInPlanCache = true
	if len(bindSQL) > 0 { // We're using binding, set this to true.
		sessVars.FoundInBinding = true
	}
	if metrics.ResettablePlanCacheCounterFortTest {
		metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
	} else {
		core_metrics.GetPlanCacheHitCounter(isNonPrepared).Inc()
	}
	stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
	stmtCtx.StmtHints = *stmtHints
	return plan, true, nil
}

// generateNewPlan call the optimizer to generate a new plan for current statement
// and try to add it to cache
func generateNewPlan(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema,
	stmt *PlanCacheStmt, cacheKey string, paramTypes []*types.FieldType) (base.Plan, []*types.FieldName, error) {
	stmtAst := stmt.PreparedAst
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	core_metrics.GetPlanCacheMissCounter(isNonPrepared).Inc()
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = true
	nodeW := resolve.NewNodeWWithCtx(stmtAst.Stmt, stmt.ResolveCtx)
	p, names, err := OptimizeAstNode(ctx, sctx, nodeW, is)
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = false
	if err != nil {
		return nil, nil, err
	}

	// check whether this plan is cacheable.
	if stmtCtx.UseCache() {
		if cacheable, reason := isPlanCacheable(sctx.GetPlanCtx(), p, len(paramTypes), len(stmt.limits), stmt.hasSubquery); !cacheable {
			stmtCtx.SetSkipPlanCache(reason)
		}
	}

	// put this plan into the plan cache.
	if stmtCtx.UseCache() {
		cached := NewPlanCacheValue(p, names, paramTypes, &stmtCtx.StmtHints)
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
		if instancePlanCacheEnabled(ctx) {
			domain.GetDomain(sctx).GetInstancePlanCache().Put(cacheKey, cached, paramTypes)
		} else {
			sctx.GetSessionPlanCache().Put(cacheKey, cached, paramTypes)
		}
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// checkPreparedPriv checks the privilege of the prepared statement
func checkPreparedPriv(ctx context.Context, sctx sessionctx.Context, stmt *PlanCacheStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := VisitInfo4PrivCheck(ctx, is, stmt.PreparedAst.Stmt, stmt.VisitInfos)
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
