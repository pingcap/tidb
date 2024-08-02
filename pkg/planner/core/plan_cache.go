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
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// PlanCacheKeyTestIssue43667 is only for test.
type PlanCacheKeyTestIssue43667 struct{}

// PlanCacheKeyTestIssue46760 is only for test.
type PlanCacheKeyTestIssue46760 struct{}

// PlanCacheKeyTestIssue47133 is only for test.
type PlanCacheKeyTestIssue47133 struct{}

// SetParameterValuesIntoSCtx sets these parameters into session context.
func SetParameterValuesIntoSCtx(sctx PlanContext, isNonPrep bool, markers []ast.ParamMarkerExpr, params []expression.Expression) error {
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
		tbl, ok := is.TableByID(stmt.tbls[i].Meta().ID)
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
		stmt.PointGet.Plan = nil
		stmt.PointGet.Executor = nil
		stmt.PointGet.ColumnInfos = nil
		stmt.PointGet.planCacheKey = nil
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
		stmt.PointGet.Plan = nil
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
	params []expression.Expression) (plan Plan, names []*types.FieldName, err error) {
	if err := planCachePreprocess(ctx, sctx, isNonPrepared, is, stmt, params); err != nil {
		return nil, nil, err
	}

	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	cacheEnabled := false
	if isNonPrepared {
		stmtCtx.CacheType = stmtctx.SessionNonPrepared
		cacheEnabled = sctx.GetSessionVars().EnableNonPreparedPlanCache // plan-cache might be disabled after prepare.
	} else {
		stmtCtx.CacheType = stmtctx.SessionPrepared
		cacheEnabled = sctx.GetSessionVars().EnablePreparedPlanCache
	}
	stmtCtx.UseCache = stmt.StmtCacheable && cacheEnabled
	if stmt.UncacheableReason != "" {
		stmtCtx.ForceSetSkipPlanCache(errors.NewNoStackError(stmt.UncacheableReason))
	}

	var bindSQL string
	if stmtCtx.UseCache {
		var ignoreByBinding bool
		bindSQL, ignoreByBinding = bindinfo.MatchSQLBindingForPlanCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo)
		if ignoreByBinding {
			stmtCtx.SetSkipPlanCache(errors.Errorf("ignore plan cache by binding"))
		}
	}

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64

	if stmtCtx.UseCache {
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

	if stmtCtx.UseCache && stmt.PointGet.Plan != nil && bytes.Equal(stmt.PointGet.planCacheKey.Hash(), cacheKey.Hash()) { // special code path for fast point plan
		if plan, names, ok, err := getCachedPointPlan(stmt, sessVars, stmtCtx); ok {
			return plan, names, err
		}
	}

	matchOpts, err := GetMatchOpts(sctx, is, stmt, params)
	if err != nil {
		return nil, nil, err
	}
	if stmtCtx.UseCache { // for non-point plans
		if plan, names, ok, err := getCachedPlan(sctx, isNonPrepared, cacheKey, bindSQL, is, stmt, matchOpts); err != nil || ok {
			return plan, names, err
		}
	}

	return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, latestSchemaVersion, bindSQL, matchOpts)
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, params []expression.Expression) (paramTypes []*types.FieldType) {
	paramTypes = make([]*types.FieldType, 0, len(params))
	for _, param := range params {
		if c, ok := param.(*expression.Constant); ok { // from binary protocol
			paramTypes = append(paramTypes, c.GetType())
			continue
		}

		// from text protocol, there must be a GetVar function
		name := param.(*expression.ScalarFunction).GetArgs()[0].String()
		tp, ok := sctx.GetSessionVars().GetUserVarType(name)
		if !ok {
			tp = types.NewFieldType(mysql.TypeNull)
		}
		paramTypes = append(paramTypes, tp)
	}
	return
}

func getCachedPointPlan(stmt *PlanCacheStmt, sessVars *variable.SessionVars, stmtCtx *stmtctx.StatementContext) (Plan,
	[]*types.FieldName, bool, error) {
	// short path for point-get plans
	// Rewriting the expression in the select.where condition  will convert its
	// type from "paramMarker" to "Constant".When Point Select queries are executed,
	// the expression in the where condition will not be evaluated,
	// so you don't need to consider whether prepared.useCache is enabled.
	plan := stmt.PointGet.Plan.(Plan)
	names := stmt.PointGet.ColumnNames.(types.NameSlice)
	if !RebuildPlan4CachedPlan(plan) {
		return nil, nil, false, nil
	}
	if metrics.ResettablePlanCacheCounterFortTest {
		metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
	} else {
		// only for prepared plan cache
		core_metrics.GetPlanCacheHitCounter(false).Inc()
	}
	sessVars.FoundInPlanCache = true
	stmtCtx.PointExec = true
	if pointGetPlan, ok := plan.(*PointGetPlan); ok && pointGetPlan != nil && pointGetPlan.stmtHints != nil {
		sessVars.StmtCtx.StmtHints = *pointGetPlan.stmtHints
	}
	return plan, names, true, nil
}

func getCachedPlan(sctx sessionctx.Context, isNonPrepared bool, cacheKey kvcache.Key, bindSQL string,
	is infoschema.InfoSchema, stmt *PlanCacheStmt, matchOpts *utilpc.PlanCacheMatchOpts) (Plan,
	[]*types.FieldName, bool, error) {
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	candidate, exist := sctx.GetSessionPlanCache().Get(cacheKey, matchOpts)
	if !exist {
		return nil, nil, false, nil
	}
	cachedVal := candidate.(*PlanCacheValue)
	if err := CheckPreparedPriv(sctx, stmt, is); err != nil {
		return nil, nil, false, err
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
	return cachedVal.Plan, cachedVal.OutPutNames, true, nil
}

// generateNewPlan call the optimizer to generate a new plan for current statement
// and try to add it to cache
func generateNewPlan(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema,
	stmt *PlanCacheStmt, cacheKey kvcache.Key, latestSchemaVersion int64, bindSQL string,
	matchOpts *utilpc.PlanCacheMatchOpts) (Plan, []*types.FieldName, error) {
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
	err = tryCachePointPlan(ctx, sctx.GetPlanCtx(), stmt, p, names, cacheKey)
	if err != nil {
		return nil, nil, err
	}

	// check whether this plan is cacheable.
	if stmtCtx.UseCache {
		if cacheable, reason := isPlanCacheable(sctx.GetPlanCtx(), p, len(matchOpts.ParamTypes), len(matchOpts.LimitOffsetAndCount), matchOpts.HasSubQuery); !cacheable {
			stmtCtx.SetSkipPlanCache(errors.Errorf(reason))
		}
	}

	// put this plan into the plan cache.
	if stmtCtx.UseCache {
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
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// RebuildPlan4CachedPlan will rebuild this plan under current user parameters.
func RebuildPlan4CachedPlan(p Plan) (ok bool) {
	sc := p.SCtx().GetSessionVars().StmtCtx
	if !sc.UseCache {
		return false // plan-cache is disabled for this query
	}

	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	if err := rebuildRange(p); err != nil {
		sc.AppendWarning(errors.NewNoStackErrorf("skip plan-cache: plan rebuild failed, %s", err.Error()))
		return false // fail to rebuild ranges
	}
	if !sc.UseCache {
		// in this case, the UseCache flag changes from `true` to `false`, then there must be some
		// over-optimized operations were triggered, return `false` for safety here.
		return false
	}
	return true
}

func updateRange(p PhysicalPlan, ranges ranger.Ranges, rangeInfo string) {
	switch x := p.(type) {
	case *PhysicalTableScan:
		x.Ranges = ranges
		x.rangeInfo = rangeInfo
	case *PhysicalIndexScan:
		x.Ranges = ranges
		x.rangeInfo = rangeInfo
	case *PhysicalTableReader:
		updateRange(x.TablePlans[0], ranges, rangeInfo)
	case *PhysicalIndexReader:
		updateRange(x.IndexPlans[0], ranges, rangeInfo)
	case *PhysicalIndexLookUpReader:
		updateRange(x.IndexPlans[0], ranges, rangeInfo)
	}
}

// rebuildRange doesn't set mem limit for building ranges. There are two reasons why we don't restrict range mem usage here.
//  1. The cached plan must be able to build complete ranges under mem limit when it is generated. Hence we can just build
//     ranges from x.AccessConditions. The only difference between the last ranges and new ranges is the change of parameter
//     values, which doesn't cause much change on the mem usage of complete ranges.
//  2. Different parameter values can change the mem usage of complete ranges. If we set range mem limit here, range fallback
//     may heppen and cause correctness problem. For example, a in (?, ?, ?) is the access condition. When the plan is firstly
//     generated, its complete ranges are ['a','a'], ['b','b'], ['c','c'], whose mem usage is under range mem limit 100B.
//     When the cached plan is hit, the complete ranges may become ['aaa','aaa'], ['bbb','bbb'], ['ccc','ccc'], whose mem
//     usage exceeds range mem limit 100B, and range fallback happens and tidb may fetch more rows than users expect.
func rebuildRange(p Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetSessionVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalIndexHashJoin:
		return rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexMergeJoin:
		return rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexJoin:
		if err := x.Ranges.Rebuild(); err != nil {
			return err
		}
		if mutableRange, ok := x.Ranges.(*mutableIndexJoinRange); ok {
			helper := mutableRange.buildHelper
			rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxCols, mutableRange.outerJoinKeys)
			innerPlan := x.Children()[x.InnerChildIdx]
			updateRange(innerPlan, x.Ranges.Range(), rangeInfo)
		}
		for _, child := range x.Children() {
			err = rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *PhysicalTableScan:
		err = buildRangeForTableScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalIndexScan:
		err = buildRangeForIndexScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalTableReader:
		err = rebuildRange(x.TablePlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexReader:
		err = rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		err = rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PointGetPlan:
		if x.TblInfo.GetPartitionInfo() != nil {
			if fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix33031, false) {
				return errors.NewNoStackError("Fix33031 fix-control set and partitioned table in cached Point Get plan")
			}
		}
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != 1 || !isSafeRange(x.AccessConditions, ranges, false, nil) {
					return errors.New("rebuild to get an unsafe range")
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkCol *expression.Column
				var unsignedIntHandle bool
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
					if !x.TblInfo.IsCommonHandle {
						unsignedIntHandle = true
					}
				}
				if pkCol != nil {
					ranges, accessConds, remainingConds, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) != 1 || !isSafeRange(x.AccessConditions, &ranger.DetachRangeResult{
						Ranges:        ranges,
						AccessConds:   accessConds,
						RemainedConds: remainingConds,
					}, unsignedIntHandle, nil) {
						return errors.New("rebuild to get an unsafe range")
					}
					x.Handle = kv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		if x.HandleConstant != nil {
			dVal, err := convertConstant2Datum(sctx, x.HandleConstant, x.handleFieldType)
			if err != nil {
				return err
			}
			iv, err := dVal.ToInt64(sc.TypeCtx())
			if err != nil {
				return err
			}
			x.Handle = kv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexConstants {
			if param != nil {
				dVal, err := convertConstant2Datum(sctx, param, x.ColsFieldType[i])
				if err != nil {
					return err
				}
				x.IndexValues[i] = *dVal
			}
		}
		return nil
	case *BatchPointGetPlan:
		if x.TblInfo.GetPartitionInfo() != nil && fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix33031, false) {
			return errors.NewNoStackError("Fix33031 fix-control set and partitioned table in cached Batch Point Get plan")
		}
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != len(x.IndexValues) || !isSafeRange(x.AccessConditions, ranges, false, nil) {
					return errors.New("rebuild to get an unsafe range")
				}
				for i := range ranges.Ranges {
					copy(x.IndexValues[i], ranges.Ranges[i].LowVal)
				}
			} else {
				var pkCol *expression.Column
				var unsignedIntHandle bool
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
					if !x.TblInfo.IsCommonHandle {
						unsignedIntHandle = true
					}
				}
				if pkCol != nil {
					ranges, accessConds, remainingConds, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) != len(x.Handles) || !isSafeRange(x.AccessConditions, &ranger.DetachRangeResult{
						Ranges:        ranges,
						AccessConds:   accessConds,
						RemainedConds: remainingConds,
					}, unsignedIntHandle, nil) {
						return errors.New("rebuild to get an unsafe range")
					}
					for i := range ranges {
						x.Handles[i] = kv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		if len(x.HandleParams) > 0 {
			if len(x.HandleParams) != len(x.Handles) {
				return errors.New("rebuild to get an unsafe range, Handles length diff")
			}
			for i, param := range x.HandleParams {
				if param != nil {
					dVal, err := convertConstant2Datum(sctx, param, x.HandleType)
					if err != nil {
						return err
					}
					iv, err := dVal.ToInt64(sc.TypeCtx())
					if err != nil {
						return err
					}
					x.Handles[i] = kv.IntHandle(iv)
				}
			}
		}
		if len(x.IndexValueParams) > 0 {
			if len(x.IndexValueParams) != len(x.IndexValues) {
				return errors.New("rebuild to get an unsafe range, IndexValue length diff")
			}
			for i, params := range x.IndexValueParams {
				if len(params) < 1 {
					continue
				}
				for j, param := range params {
					if param != nil {
						dVal, err := convertConstant2Datum(sctx, param, x.IndexColTypes[j])
						if err != nil {
							return err
						}
						x.IndexValues[i][j] = *dVal
					}
				}
			}
		}
	case *PhysicalIndexMergeReader:
		indexMerge := p.(*PhysicalIndexMergeReader)
		for _, partialPlans := range indexMerge.PartialPlans {
			err = rebuildRange(partialPlans[0])
			if err != nil {
				return err
			}
		}
		// We don't need to handle the indexMerge.TablePlans, because the tablePlans
		// only can be (Selection) + TableRowIDScan. There have no range need to rebuild.
	case PhysicalPlan:
		for _, child := range x.Children() {
			err = rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	case *Update:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	}
	return nil
}

func convertConstant2Datum(ctx PlanContext, con *expression.Constant, target *types.FieldType) (*types.Datum, error) {
	val, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	tc := ctx.GetSessionVars().StmtCtx.TypeCtx()
	dVal, err := val.ConvertTo(tc, target)
	if err != nil {
		return nil, err
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(tc, &val, collate.GetCollator(target.GetCollate()))
	if err != nil || cmp != 0 {
		return nil, errors.New("Convert constant to datum is failed, because the constant has changed after the covert")
	}
	return &dVal, nil
}

func buildRangeForTableScan(sctx PlanContext, ts *PhysicalTableScan) (err error) {
	if ts.Table.IsCommonHandle {
		pk := tables.FindPrimaryIndex(ts.Table)
		pkCols := make([]*expression.Column, 0, len(pk.Columns))
		pkColsLen := make([]int, 0, len(pk.Columns))
		for _, colInfo := range pk.Columns {
			if pkCol := expression.ColInfo2Col(ts.schema.Columns, ts.Table.Columns[colInfo.Offset]); pkCol != nil {
				pkCols = append(pkCols, pkCol)
				// We need to consider the prefix index.
				// For example: when we have 'a varchar(50), index idx(a(10))'
				// So we will get 'colInfo.Length = 50' and 'pkCol.RetType.flen = 10'.
				// In 'hasPrefix' function from 'util/ranger/ranger.go' file,
				// we use 'columnLength == types.UnspecifiedLength' to check whether we have prefix index.
				if colInfo.Length != types.UnspecifiedLength && colInfo.Length == pkCol.RetType.GetFlen() {
					pkColsLen = append(pkColsLen, types.UnspecifiedLength)
				} else {
					pkColsLen = append(pkColsLen, colInfo.Length)
				}
			}
		}
		if len(pkCols) > 0 {
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, ts.AccessCondition, pkCols, pkColsLen, 0)
			if err != nil {
				return err
			}
			if !isSafeRange(ts.AccessCondition, res, false, ts.Ranges) {
				return errors.New("rebuild to get an unsafe range")
			}
			ts.Ranges = res.Ranges
		} else {
			if len(ts.AccessCondition) > 0 {
				return errors.New("fail to build ranges, cannot get the primary key column")
			}
			ts.Ranges = ranger.FullRange()
		}
	} else {
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			}
		}
		if pkCol != nil {
			ranges, accessConds, remainingConds, err := ranger.BuildTableRange(ts.AccessCondition, sctx, pkCol.RetType, 0)
			if err != nil {
				return err
			}
			if !isSafeRange(ts.AccessCondition, &ranger.DetachRangeResult{
				Ranges:        ts.Ranges,
				AccessConds:   accessConds,
				RemainedConds: remainingConds,
			}, true, ts.Ranges) {
				return errors.New("rebuild to get an unsafe range")
			}
			ts.Ranges = ranges
		} else {
			if len(ts.AccessCondition) > 0 {
				return errors.New("fail to build ranges, cannot get the primary key column")
			}
			ts.Ranges = ranger.FullIntRange(false)
		}
	}
	return
}

func buildRangeForIndexScan(sctx PlanContext, is *PhysicalIndexScan) (err error) {
	if len(is.IdxCols) == 0 {
		if ranger.HasFullRange(is.Ranges, false) { // the original range is already a full-range.
			is.Ranges = ranger.FullRange()
			return
		}
		return errors.New("unexpected range for PhysicalIndexScan")
	}

	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxCols, is.IdxColLens, 0)
	if err != nil {
		return err
	}
	if !isSafeRange(is.AccessCondition, res, false, is.Ranges) {
		return errors.New("rebuild to get an unsafe range")
	}
	is.Ranges = res.Ranges
	return
}

// checkRebuiltRange checks whether the re-built range is safe.
// To re-use a cached plan, the planner needs to rebuild the access range, but as
// parameters change, some unsafe ranges may occur.
// For example, the first time the planner can build a range `(2, 5)` from `a>2 and a<(?)5`, but if the
// parameter changes to `(?)1`, then it'll get an unsafe range `(empty)`.
// To make plan-cache safer, let the planner abandon the cached plan if it gets an unsafe range here.
func isSafeRange(accessConds []expression.Expression, rebuiltResult *ranger.DetachRangeResult,
	unsignedIntHandle bool, originalRange ranger.Ranges) (safe bool) {
	if len(rebuiltResult.RemainedConds) > 0 || // the ranger generates some other extra conditions
		len(rebuiltResult.AccessConds) != len(accessConds) || // not all access conditions are used
		len(rebuiltResult.Ranges) == 0 { // get an empty range
		return false
	}

	if len(accessConds) > 0 && // if have accessConds, and
		ranger.HasFullRange(rebuiltResult.Ranges, unsignedIntHandle) && // get an full range, and
		originalRange != nil && !ranger.HasFullRange(originalRange, unsignedIntHandle) { // the original range is not a full range
		return false
	}

	return true
}

// CheckPreparedPriv checks the privilege of the prepared statement
func CheckPreparedPriv(sctx sessionctx.Context, stmt *PlanCacheStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := VisitInfo4PrivCheck(is, stmt.PreparedAst.Stmt, stmt.VisitInfos)
		if err := CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, visitInfo); err != nil {
			return err
		}
	}
	err := CheckTableLock(sctx, is, stmt.VisitInfos)
	return err
}

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point update"
func tryCachePointPlan(_ context.Context, sctx PlanContext,
	stmt *PlanCacheStmt, p Plan, names types.NameSlice, cacheKey kvcache.Key) error {
	if !sctx.GetSessionVars().StmtCtx.UseCache {
		return nil
	}
	var (
		ok  bool
		err error
	)

	if plan, _ok := p.(*PointGetPlan); _ok {
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx.GetSessionVars(), p)
		if err != nil {
			return err
		}
		if ok {
			plan.stmtHints = sctx.GetSessionVars().StmtCtx.StmtHints.Clone()
		}
	}

	if ok {
		// just cache point plan now
		stmt.PointGet.Plan = p
		stmt.PointGet.ColumnNames = names
		stmt.PointGet.planCacheKey = cacheKey
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
	}
	return err
}

// IsPointGetPlanShortPathOK check if we can execute using plan cached in prepared structure
// Be careful with the short path, current precondition is ths cached plan satisfying
// IsPointGetWithPKOrUniqueKeyByAutoCommit
func IsPointGetPlanShortPathOK(sctx sessionctx.Context, is infoschema.InfoSchema, stmt *PlanCacheStmt) (bool, error) {
	if stmt.PointGet.Plan == nil || staleread.IsStmtStaleness(sctx) {
		return false, nil
	}
	// check auto commit
	if !IsAutoCommitTxn(sctx.GetSessionVars()) {
		return false, nil
	}
	if stmt.SchemaVersion != is.SchemaMetaVersion() {
		stmt.PointGet.Plan = nil
		stmt.PointGet.ColumnInfos = nil
		stmt.PointGet.planCacheKey = nil
		return false, nil
	}
	// maybe we'd better check cached plan type here, current
	// only point select/update will be cached, see "getPhysicalPlan" func
	var ok bool
	var err error
	switch stmt.PointGet.Plan.(type) {
	case *PointGetPlan:
		ok = true
	case *Update:
		pointUpdate := stmt.PointGet.Plan.(*Update)
		_, ok = pointUpdate.SelectPlan.(*PointGetPlan)
		if !ok {
			err = errors.Errorf("cached update plan not point update")
			stmt.PointGet.Plan = nil
			return false, err
		}
	default:
		ok = false
	}
	return ok, err
}
