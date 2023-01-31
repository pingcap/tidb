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
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

func planCachePreprocess(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema, stmt *PlanCacheStmt, params []expression.Expression) error {
	vars := sctx.GetSessionVars()
	stmtAst := stmt.PreparedAst
	vars.StmtCtx.StmtType = stmtAst.StmtType

	// step 1: check parameter number
	if len(stmtAst.Params) != len(params) {
		return errors.Trace(ErrWrongParamCount)
	}

	// step 2: set parameter values
	for i, usingParam := range params {
		val, err := usingParam.Eval(chunk.Row{})
		if err != nil {
			return err
		}
		param := stmtAst.Params[i].(*driver.ParamMarkerExpr)
		if isGetVarBinaryLiteral(sctx, usingParam) {
			binVal, convErr := val.ToBytes()
			if convErr != nil {
				return convErr
			}
			val.SetBinaryLiteral(binVal)
		}
		param.Datum = val
		param.InExecute = true
		vars.PreparedParams = append(vars.PreparedParams, val)
	}

	// step 3: check schema version
	if stmtAst.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schema version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schema version like prepared plan cache key
		stmtAst.CachedPlan = nil
		stmt.Executor = nil
		stmt.ColumnInfos = nil
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		// Example:
		// When running update in prepared statement's schema version distinguished from the one of execute statement
		// We should reset the tableRefs in the prepared update statements, otherwise, the ast nodes still hold the old
		// tableRefs columnInfo which will cause chaos in logic of trying point get plan. (should ban non-public column)
		ret := &PreprocessorReturn{InfoSchema: is}
		err := Preprocess(ctx, sctx, stmtAst.Stmt, InPrepare, WithPreprocessorReturn(ret))
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		stmtAst.SchemaVersion = is.SchemaMetaVersion()
	}

	// step 4: handle expiration
	// If the lastUpdateTime less than expiredTimeStamp4PC,
	// it means other sessions have executed 'admin flush instance plan_cache'.
	// So we need to clear the current session's plan cache.
	// And update lastUpdateTime to the newest one.
	expiredTimeStamp4PC := domain.GetDomain(sctx).ExpiredTimeStamp4PC()
	if stmt.StmtCacheable && expiredTimeStamp4PC.Compare(vars.LastUpdateTime4PC) > 0 {
		sctx.GetPlanCache(isNonPrepared).DeleteAll()
		stmtAst.CachedPlan = nil
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
	if v := ctx.Value("____GetPlanFromSessionPlanCacheErr"); v != nil { // for testing
		return nil, nil, errors.New("____GetPlanFromSessionPlanCacheErr")
	}

	if err := planCachePreprocess(ctx, sctx, isNonPrepared, is, stmt, params); err != nil {
		return nil, nil, err
	}

	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	stmtAst := stmt.PreparedAst
	stmtCtx.UseCache = stmt.StmtCacheable
	if !stmt.StmtCacheable {
		stmtCtx.SetSkipPlanCache(errors.Errorf("skip plan-cache: %s", stmt.UncacheableReason))
	}

	var bindSQL string
	if stmtCtx.UseCache {
		var ignoreByBinding bool
		bindSQL, ignoreByBinding = GetBindSQL4PlanCache(sctx, stmt)
		if ignoreByBinding {
			stmtCtx.SetSkipPlanCache(errors.Errorf("skip plan-cache: ignore plan cache by binding"))
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
			stmt.StmtDB, stmtAst.SchemaVersion, latestSchemaVersion, bindSQL); err != nil {
			return nil, nil, err
		}
	}

	paramTypes := parseParamTypes(sctx, params)

	if stmtCtx.UseCache && stmtAst.CachedPlan != nil { // for point query plan
		if plan, names, ok, err := getCachedPointPlan(stmtAst, sessVars, stmtCtx); ok {
			return plan, names, err
		}
	}
	limitCountAndOffset, paramErr := ExtractLimitFromAst(stmt.PreparedAst.Stmt, sctx)
	if paramErr != nil {
		return nil, nil, paramErr
	}
	if stmtCtx.UseCache { // for non-point plans
		if plan, names, ok, err := getCachedPlan(sctx, isNonPrepared, cacheKey, bindSQL, is, stmt,
			paramTypes, limitCountAndOffset); err != nil || ok {
			return plan, names, err
		}
	}

	return generateNewPlan(ctx, sctx, isNonPrepared, is, stmt, cacheKey, latestSchemaVersion, paramTypes, bindSQL, limitCountAndOffset)
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, params []expression.Expression) (paramTypes []*types.FieldType) {
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

func getCachedPointPlan(stmt *ast.Prepared, sessVars *variable.SessionVars, stmtCtx *stmtctx.StatementContext) (Plan,
	[]*types.FieldName, bool, error) {
	// short path for point-get plans
	// Rewriting the expression in the select.where condition  will convert its
	// type from "paramMarker" to "Constant".When Point Select queries are executed,
	// the expression in the where condition will not be evaluated,
	// so you don't need to consider whether prepared.useCache is enabled.
	plan := stmt.CachedPlan.(Plan)
	names := stmt.CachedNames.(types.NameSlice)
	err := RebuildPlan4CachedPlan(plan)
	if err != nil {
		logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
		return nil, nil, false, nil
	}
	if metrics.ResettablePlanCacheCounterFortTest {
		metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
	} else {
		planCacheCounter.Inc()
	}
	sessVars.FoundInPlanCache = true
	stmtCtx.PointExec = true
	return plan, names, true, nil
}

func getCachedPlan(sctx sessionctx.Context, isNonPrepared bool, cacheKey kvcache.Key, bindSQL string,
	is infoschema.InfoSchema, stmt *PlanCacheStmt, paramTypes []*types.FieldType, limitParams []uint64) (Plan,
	[]*types.FieldName, bool, error) {
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	candidate, exist := sctx.GetPlanCache(isNonPrepared).Get(cacheKey, paramTypes, limitParams)
	if !exist {
		return nil, nil, false, nil
	}
	cachedVal := candidate.(*PlanCacheValue)
	if err := CheckPreparedPriv(sctx, stmt, is); err != nil {
		return nil, nil, false, err
	}
	for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
		if !unionScan && tableHasDirtyContent(sctx, tblInfo) {
			// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
			// rebuilding the filters in UnionScan is pretty trivial.
			sctx.GetPlanCache(isNonPrepared).Delete(cacheKey)
			return nil, nil, false, nil
		}
	}
	err := RebuildPlan4CachedPlan(cachedVal.Plan)
	if err != nil {
		logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
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
		planCacheCounter.Inc()
	}
	stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
	return cachedVal.Plan, cachedVal.OutPutNames, true, nil
}

// generateNewPlan call the optimizer to generate a new plan for current statement
// and try to add it to cache
func generateNewPlan(ctx context.Context, sctx sessionctx.Context, isNonPrepared bool, is infoschema.InfoSchema,
	stmt *PlanCacheStmt, cacheKey kvcache.Key, latestSchemaVersion int64, paramTypes []*types.FieldType,
	bindSQL string, limitParams []uint64) (Plan, []*types.FieldName, error) {
	stmtAst := stmt.PreparedAst
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	planCacheMissCounter.Inc()
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = true
	p, names, err := OptimizeAstNode(ctx, sctx, stmtAst.Stmt, is)
	sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding = false
	if err != nil {
		return nil, nil, err
	}
	err = tryCachePointPlan(ctx, sctx, stmt, is, p)
	if err != nil {
		return nil, nil, err
	}

	// check whether this plan is cacheable.
	if stmtCtx.UseCache {
		checkPlanCacheability(sctx, p, len(paramTypes), len(limitParams))
	}

	// put this plan into the plan cache.
	if stmtCtx.UseCache {
		// rebuild key to exclude kv.TiFlash when stmt is not read only
		if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmtAst.Stmt, sessVars) {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
			if cacheKey, err = NewPlanCacheKey(sessVars, stmt.StmtText, stmt.StmtDB,
				stmtAst.SchemaVersion, latestSchemaVersion, bindSQL); err != nil {
				return nil, nil, err
			}
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
		cached := NewPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan, paramTypes, limitParams)
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
		sctx.GetPlanCache(isNonPrepared).Put(cacheKey, cached, paramTypes, limitParams)
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// checkPlanCacheability checks whether this plan is cacheable and set to skip plan cache if it's uncacheable.
func checkPlanCacheability(sctx sessionctx.Context, p Plan, paramNum int, limitParamNum int) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	var pp PhysicalPlan
	switch x := p.(type) {
	case *Insert:
		pp = x.SelectPlan
	case *Update:
		pp = x.SelectPlan
	case *Delete:
		pp = x.SelectPlan
	case PhysicalPlan:
		pp = x
	default:
		stmtCtx.SetSkipPlanCache(errors.Errorf("skip plan-cache: unexpected un-cacheable plan %v", p.ExplainID().String()))
		return
	}
	if pp == nil { // simple DML statements
		return
	}

	if useTiFlash(pp) {
		stmtCtx.SetSkipPlanCache(errors.Errorf("skip plan-cache: TiFlash plan is un-cacheable"))
		return
	}

	// We only cache the tableDual plan when the number of parameters are zero.
	if containTableDual(pp) && paramNum > 0 {
		stmtCtx.SetSkipPlanCache(errors.New("skip plan-cache: get a TableDual plan"))
		return
	}

	if accessMVIndexWithIndexMerge(pp) {
		stmtCtx.SetSkipPlanCache(errors.New("skip plan-cache: the plan with IndexMerge accessing Multi-Valued Index is un-cacheable"))
		return
	}

	// before cache the param limit plan, check switch
	if limitParamNum != 0 && !sctx.GetSessionVars().EnablePlanCacheForParamLimit {
		stmtCtx.SetSkipPlanCache(errors.New("skip plan-cache: the switch 'tidb_enable_plan_cache_for_param_limit' is off"))
	}
}

// RebuildPlan4CachedPlan will rebuild this plan under current user parameters.
func RebuildPlan4CachedPlan(p Plan) error {
	sc := p.SCtx().GetSessionVars().StmtCtx
	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	return rebuildRange(p)
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
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) == 0 || len(ranges.AccessConds) != len(x.AccessConditions) {
					return errors.New("failed to rebuild range: the length of the range has changed")
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkCol *expression.Column
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
				}
				if pkCol != nil {
					ranges, _, _, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) == 0 {
						return errors.New("failed to rebuild range: the length of the range has changed")
					}
					x.Handle = kv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		// The code should never run here as long as we're not using point get for partition table.
		// And if we change the logic one day, here work as defensive programming to cache the error.
		if x.PartitionInfo != nil {
			// TODO: relocate the partition after rebuilding range to make PlanCache support PointGet
			return errors.New("point get for partition table can not use plan cache")
		}
		if x.HandleConstant != nil {
			dVal, err := convertConstant2Datum(sc, x.HandleConstant, x.handleFieldType)
			if err != nil {
				return err
			}
			iv, err := dVal.ToInt64(sc)
			if err != nil {
				return err
			}
			x.Handle = kv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexConstants {
			if param != nil {
				dVal, err := convertConstant2Datum(sc, param, x.ColsFieldType[i])
				if err != nil {
					return err
				}
				x.IndexValues[i] = *dVal
			}
		}
		return nil
	case *BatchPointGetPlan:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != len(x.IndexValues) || len(ranges.AccessConds) != len(x.AccessConditions) {
					return errors.New("failed to rebuild range: the length of the range has changed")
				}
				for i := range x.IndexValues {
					copy(x.IndexValues[i], ranges.Ranges[i].LowVal)
				}
			} else {
				var pkCol *expression.Column
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
				}
				if pkCol != nil {
					ranges, _, _, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) != len(x.Handles) {
						return errors.New("failed to rebuild range: the length of the range has changed")
					}
					for i := range ranges {
						x.Handles[i] = kv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		for i, param := range x.HandleParams {
			if param != nil {
				dVal, err := convertConstant2Datum(sc, param, x.HandleType)
				if err != nil {
					return err
				}
				iv, err := dVal.ToInt64(sc)
				if err != nil {
					return err
				}
				x.Handles[i] = kv.IntHandle(iv)
			}
		}
		for i, params := range x.IndexValueParams {
			if len(params) < 1 {
				continue
			}
			for j, param := range params {
				if param != nil {
					dVal, err := convertConstant2Datum(sc, param, x.IndexColTypes[j])
					if err != nil {
						return err
					}
					x.IndexValues[i][j] = *dVal
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

func convertConstant2Datum(sc *stmtctx.StatementContext, con *expression.Constant, target *types.FieldType) (*types.Datum, error) {
	val, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	dVal, err := val.ConvertTo(sc, target)
	if err != nil {
		return nil, err
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(sc, &val, collate.GetCollator(target.GetCollate()))
	if err != nil || cmp != 0 {
		return nil, errors.New("Convert constant to datum is failed, because the constant has changed after the covert")
	}
	return &dVal, nil
}

func buildRangeForTableScan(sctx sessionctx.Context, ts *PhysicalTableScan) (err error) {
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
			if len(res.AccessConds) != len(ts.AccessCondition) {
				return errors.New("rebuild range for cached plan failed")
			}
			ts.Ranges = res.Ranges
		} else {
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
			ts.Ranges, _, _, err = ranger.BuildTableRange(ts.AccessCondition, sctx, pkCol.RetType, 0)
			if err != nil {
				return err
			}
		} else {
			ts.Ranges = ranger.FullIntRange(false)
		}
	}
	return
}

func buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) (err error) {
	if len(is.IdxCols) == 0 {
		is.Ranges = ranger.FullRange()
		return
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxCols, is.IdxColLens, 0)
	if err != nil {
		return err
	}
	if len(res.AccessConds) != len(is.AccessCondition) {
		return errors.New("rebuild range for cached plan failed")
	}
	is.Ranges = res.Ranges
	return
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
func tryCachePointPlan(_ context.Context, sctx sessionctx.Context,
	stmt *PlanCacheStmt, _ infoschema.InfoSchema, p Plan) error {
	if !sctx.GetSessionVars().StmtCtx.UseCache {
		return nil
	}
	var (
		stmtAst = stmt.PreparedAst
		ok      bool
		err     error
		names   types.NameSlice
	)

	if _, _ok := p.(*PointGetPlan); _ok {
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx, p)
		names = p.OutputNames()
		if err != nil {
			return err
		}
	}

	if ok {
		// just cache point plan now
		stmtAst.CachedPlan = p
		stmtAst.CachedNames = names
		stmt.NormalizedPlan, stmt.PlanDigest = NormalizePlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(stmt.NormalizedPlan, stmt.PlanDigest)
	}
	return err
}

func containTableDual(p PhysicalPlan) bool {
	_, isTableDual := p.(*PhysicalTableDual)
	if isTableDual {
		return true
	}
	childContainTableDual := false
	for _, child := range p.Children() {
		childContainTableDual = childContainTableDual || containTableDual(child)
	}
	return childContainTableDual
}

func accessMVIndexWithIndexMerge(p PhysicalPlan) bool {
	if idxMerge, ok := p.(*PhysicalIndexMergeReader); ok {
		if idxMerge.AccessMVIndex {
			return true
		}
	}

	for _, c := range p.Children() {
		if accessMVIndexWithIndexMerge(c) {
			return true
		}
	}
	return false
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

// GetBindSQL4PlanCache used to get the bindSQL for plan cache to build the plan cache key.
func GetBindSQL4PlanCache(sctx sessionctx.Context, stmt *PlanCacheStmt) (string, bool) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	ignore := false
	if !useBinding || stmt.PreparedAst.Stmt == nil || stmt.NormalizedSQL4PC == "" || stmt.SQLDigest4PC == "" {
		return "", ignore
	}
	if sctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return "", ignore
	}
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(stmt.SQLDigest4PC, stmt.NormalizedSQL4PC, "")
	if bindRecord != nil {
		enabledBinding := bindRecord.FindEnabledBinding()
		if enabledBinding != nil {
			ignore = enabledBinding.Hint.ContainTableHint(HintIgnorePlanCache)
			return enabledBinding.BindSQL, ignore
		}
	}
	globalHandle := domain.GetDomain(sctx).BindHandle()
	if globalHandle == nil {
		return "", ignore
	}
	bindRecord = globalHandle.GetBindRecord(stmt.SQLDigest4PC, stmt.NormalizedSQL4PC, "")
	if bindRecord != nil {
		enabledBinding := bindRecord.FindEnabledBinding()
		if enabledBinding != nil {
			ignore = enabledBinding.Hint.ContainTableHint(HintIgnorePlanCache)
			return enabledBinding.BindSQL, ignore
		}
	}
	return "", ignore
}

// IsPointPlanShortPathOK check if we can execute using plan cached in prepared structure
// Be careful with the short path, current precondition is ths cached plan satisfying
// IsPointGetWithPKOrUniqueKeyByAutoCommit
func IsPointPlanShortPathOK(sctx sessionctx.Context, is infoschema.InfoSchema, stmt *PlanCacheStmt) (bool, error) {
	stmtAst := stmt.PreparedAst
	if stmtAst.CachedPlan == nil || staleread.IsStmtStaleness(sctx) {
		return false, nil
	}
	// check auto commit
	if !IsAutoCommitTxn(sctx) {
		return false, nil
	}
	if stmtAst.SchemaVersion != is.SchemaMetaVersion() {
		stmtAst.CachedPlan = nil
		stmt.ColumnInfos = nil
		return false, nil
	}
	// maybe we'd better check cached plan type here, current
	// only point select/update will be cached, see "getPhysicalPlan" func
	var ok bool
	var err error
	switch stmtAst.CachedPlan.(type) {
	case *PointGetPlan:
		ok = true
	case *Update:
		pointUpdate := stmtAst.CachedPlan.(*Update)
		_, ok = pointUpdate.SelectPlan.(*PointGetPlan)
		if !ok {
			err = errors.Errorf("cached update plan not point update")
			stmtAst.CachedPlan = nil
			return false, err
		}
	default:
		ok = false
	}
	return ok, err
}
