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
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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

const planCacheHintOnlyNoHintReason = "plan cache strategy is hint_only and use_plan_cache hint is absent"

func containUsePlanCacheHintInPreparedSQLOrBinding(stmt *PlanCacheStmt, matchedBinding *bindinfo.Binding, bindingMatched bool) bool {
	if stmt.HasUsePlanCacheHint {
		return true
	}
	return bindingMatched &&
		matchedBinding != nil &&
		matchedBinding.Hint != nil &&
		matchedBinding.Hint.ContainTableHint(hint.HintUsePlanCache)
}

// SetParameterValuesIntoSCtx sets these parameters into session context.
func SetParameterValuesIntoSCtx(sctx base.PlanContext, isNonPrep bool, markers []ast.ParamMarkerExpr, params []expression.Expression) error {
	vars := sctx.GetSessionVars()
	vars.PlanCacheParams.Reset()
	for i, usingParam := range params {
		var (
			val types.Datum
			err error
		)
		val, err = usingParam.Eval(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
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
	for i := range stmt.dbName {
		tbl, ok := is.TableByID(ctx, stmt.tbls[i].Meta().ID)
		if !ok {
			tblByName, err := is.TableByName(context.Background(), stmt.dbName[i], stmt.tbls[i].Meta().Name)
			if err != nil {
				return plannererrors.ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
			}
			// Table ID is changed, for example, drop & create table, truncate table.
			delete(stmt.RelateVersion, stmt.tbls[i].Meta().ID)
			tbl = tblByName
		}
		// newTbl is the 'should be used' table info for this execution.
		newTbl, err := tryLockMDLAndUpdateSchemaIfNecessary(ctx, sctx.GetPlanCtx(), stmt.dbName[i], tbl, is)
		if err != nil {
			logutil.BgLogger().Warn("meet error during tryLockMDLAndUpdateSchemaIfNecessary", zap.String("table name", tbl.Meta().Name.String()), zap.Error(err))
			// Invalid the cache key related fields to avoid using plan cache.
			stmt.RelateVersion[tbl.Meta().ID] = math.MaxUint64
			schemaNotMatch = true
			continue
		}
		if stmt.tbls[i].Meta().Revision != newTbl.Meta().Revision {
			schemaNotMatch = true
		}
		// Update the cache key related fields.
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
		// The plan shape may change under the new schema (e.g. a unique index was
		// dropped, turning a point get into a stats-dependent scan), so the
		// stats-independent classification must be re-derived from the new plan.
		stmt.statsIndependent = false
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
			if vars.StmtCtx.TblInfo2UnionScan == nil {
				vars.StmtCtx.TblInfo2UnionScan = make(map[*model.TableInfo]bool)
			}
			vars.StmtCtx.TblInfo2UnionScan[tblInfo] = true
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
	var (
		matchedBinding *bindinfo.Binding
		bindingMatched bool
	)
	if isNonPrepared {
		stmtCtx.SetCacheType(contextutil.SessionNonPrepared)
		cacheEnabled = sessVars.EnableNonPreparedPlanCache // plan-cache might be disabled after prepare.
	} else {
		stmtCtx.SetCacheType(contextutil.SessionPrepared)
		cacheEnabled = sessVars.EnablePreparedPlanCache
	}
	if cacheEnabled && stmt.UncacheableReason == "" &&
		sessVars.PlanCacheStrategy == vardef.TiDBPlanCacheStrategyHintOnly &&
		!stmt.HasUsePlanCacheHint {
		if stmt.PreparedAst != nil {
			matchedBinding, bindingMatched, _ = bindinfo.MatchSQLBindingWithCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo)
		}
		if !containUsePlanCacheHintInPreparedSQLOrBinding(stmt, matchedBinding, bindingMatched) {
			cacheEnabled = false
			stmtCtx.WarnSkipPlanCache(planCacheHintOnlyNoHintReason)
		}
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
		cacheKey, binding, cacheable, reason, err = newPlanCacheKeyWithMatchedBinding(sctx, stmt, matchedBinding, bindingMatched)
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
		if !hit && !stmt.statsIndependent && instancePlanCacheEnabled(ctx) && couldBeStatsIndependent(stmt) {
			// Another session may have stored a stats-independent plan for this statement
			// under a statsIndependent=true key in the shared instance plan cache. Probe
			// that key as well: a hit implies the plan is stats-independent, because such
			// keys are only written when the stored plan was classified so (see the
			// statsIndependent byte in newPlanCacheKeyWithMatchedBinding) and key equality
			// means all other planning inputs match. On a miss the flag is re-derived from
			// the new plan in generateNewPlan.
			//
			// The extra key build and lookup are only paid on a miss (where they are
			// dwarfed by the replan that follows), and only when adoption is possible at
			// all: the session-level cache needs no probe since this statement's own flag
			// already tracks everything this session stored, and couldBeStatsIndependent
			// rules out statements that can never classify, so post-ANALYZE misses of
			// ordinary stats-dependent queries don't probe.
			stmt.statsIndependent = true
			probeKey, _, probeCacheable, _, probeErr := newPlanCacheKeyWithMatchedBinding(sctx, stmt, matchedBinding, bindingMatched)
			if probeErr == nil && probeCacheable {
				plan, outputCols, stmtHints, hit = lookupPlanCache(ctx, sctx, probeKey, paramTypes)
			}
			if hit {
				cacheKey = probeKey
			} else {
				stmt.statsIndependent = false
			}
		}
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

	return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, binding, paramTypes)
}

func clonePlanForInstancePlanCache(ctx context.Context, sctx sessionctx.Context,
	stmt *PlanCacheStmt, plan base.Plan) (clonedPlan base.Plan, ok bool) {
	defer func(begin time.Time) {
		if ok {
			core_metrics.GetPlanCacheCloneDuration().Observe(time.Since(begin).Seconds())
		}
	}(time.Now())
	fastPoint := stmt.PointGet.Executor != nil // this case is specially handled
	pointPlan, isPoint := plan.(*physicalop.PointGetPlan)
	if fastPoint && isPoint { // special optimization for fast point plans
		if stmt.PointGet.FastPlan == nil {
			stmt.PointGet.FastPlan = new(physicalop.PointGetPlan)
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
	enableInstancePlanCache := vardef.EnableInstancePlanCache.Load()
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
	sctx.GetSessionVars().PlanCacheValue = pcv
	return pcv.Plan, pcv.OutputColumns, pcv.StmtHints, true
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
	stmt *PlanCacheStmt, cacheKey, binding string, paramTypes []*types.FieldType) (base.Plan, []*types.FieldName, error) {
	stmtAst := stmt.PreparedAst
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	core_metrics.GetPlanCacheMissCounter(isNonPrepared).Inc()
	nodeW := resolve.NewNodeWWithCtx(stmtAst.Stmt, stmt.ResolveCtx)
	if stmt.statsIndependent {
		// The plan is known to be stats-independent from a previous execution, so this
		// replanning (e.g. after cache eviction) cannot be influenced by statistics:
		// don't trigger any sync/async stats loading for it.
		stmtCtx.SkipStatsLoad = true
	}
	p, names, err := OptimizeAstNodeNoCache(ctx, sctx, nodeW, is)
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
		if statsIndependent := isPlanStatsIndependent(p, stmt.hasSubquery); statsIndependent != stmt.statsIndependent {
			// Re-derive the stats-independent classification from the plan we just built,
			// in both directions. A stats-independent plan excludes the stats version from
			// its cache key, so ANALYZE no longer invalidates its entry. The reverse
			// transition matters for correctness: planning inputs that are not part of the
			// cache key (e.g. tidb_opt_fix_control) can turn a formerly stats-independent
			// statement into a stats-dependent plan, which must get the stats version back
			// into its key so ANALYZE invalidates it again. The key computed before
			// optimization used the old classification, so it must be recomputed.
			stmt.statsIndependent = statsIndependent
			var cacheable bool
			cacheKey, binding, cacheable, _, err = newPlanCacheKeyWithMatchedBinding(sctx, stmt, nil, false)
			if err != nil {
				return nil, nil, err
			}
			// The key was cacheable before optimization with the same inputs, so it must
			// still be cacheable here.
			intest.Assert(cacheable)
		}
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		cached := NewPlanCacheValue(sctx, stmt, cacheKey, binding, p, names, paramTypes, &stmtCtx.StmtHints)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
		if instancePlanCacheEnabled(ctx) {
			if cloned, ok := p.CloneForPlanCache(sctx.GetPlanCtx()); ok {
				// Clone this plan before putting it into the cache to avoid read-write DATA RACE. For example,
				// before this session finishes the execution, the next session has started cloning this plan.
				// Time:  | ------------------------------------------------------------------------------- |
				// Sess1: | put plan into cache | ----------- execution (might modify the plan) ----------- |
				// Sess2:                  | start | ------- hit this plan and clone it (DATA RACE) ------- |
				cached.Plan = cloned
				domain.GetDomain(sctx).GetInstancePlanCache().Put(cacheKey, cached, paramTypes)
			}
		} else {
			sctx.GetSessionPlanCache().Put(cacheKey, cached, paramTypes)
		}
		sctx.GetSessionVars().PlanCacheValue = cached
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// couldBeStatsIndependent cheaply rules out statements whose plan can never be classified
// stats-independent, so the probe lookup in GetPlanFromPlanCache is only paid for actual
// candidates. It must stay a superset of isPlanStatsIndependent: PointGet/BatchPointGet
// roots only arise from single-table SELECTs without LIMIT, Insert without SelectPlan only
// from INSERT ... VALUES, and subqueries disqualify classification entirely.
func couldBeStatsIndependent(stmt *PlanCacheStmt) bool {
	if stmt.hasSubquery || len(stmt.limits) > 0 || len(stmt.tables) != 1 {
		return false
	}
	switch x := stmt.PreparedAst.Stmt.(type) {
	case *ast.SelectStmt:
		return true
	case *ast.InsertStmt:
		return x.Select == nil
	}
	return false
}

// isPlanStatsIndependent reports whether this plan's shape can never be affected by
// statistics changes. PointGet and BatchPointGet are chosen by deterministic rules
// (a full match on the primary key or a unique key) rather than by cost, and
// INSERT ... VALUES involves no access-path decision at all. Plans containing
// subqueries are excluded because subquery plans are chosen by cost. For qualifying
// plans the stats version hash is excluded from the plan cache key, so ANALYZE
// doesn't needlessly invalidate their cached entries.
func isPlanStatsIndependent(p base.Plan, hasSubquery bool) bool {
	if hasSubquery {
		return false
	}
	switch x := p.(type) {
	case *physicalop.PointGetPlan, *physicalop.BatchPointGetPlan:
		return true
	case *physicalop.Insert:
		return x.SelectPlan == nil
	}
	return false
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
