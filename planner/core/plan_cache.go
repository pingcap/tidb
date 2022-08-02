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
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// GetPlanFromSessionPlanCache is the entry point of Plan Cache.
// It tries to get a valid cached plan from this session's plan cache.
// If there is no such a plan, it'll call the optimizer to generate a new one.
func GetPlanFromSessionPlanCache(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, preparedStmt *CachedPrepareStmt,
	binProtoVars []types.Datum, txtProtoVars []expression.Expression) (plan Plan, names []*types.FieldName, err error) {
	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	prepared := preparedStmt.PreparedAst
	stmtCtx.UseCache = prepared.UseCache

	var bindSQL string
	var ignorePlanCache = false

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64

	if prepared.UseCache {
		bindSQL, ignorePlanCache = GetBindSQL4PlanCache(sctx, preparedStmt)
		if sctx.GetSessionVars().IsIsolation(ast.ReadCommitted) || preparedStmt.ForUpdateRead {
			// In Rc or ForUpdateRead, we should check if the information schema has been changed since
			// last time. If it changed, we should rebuild the plan. Here, we use a different and more
			// up-to-date schema version which can lead plan cache miss and thus, the plan will be rebuilt.
			latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
		}
		if cacheKey, err = NewPlanCacheKey(sctx.GetSessionVars(), preparedStmt.StmtText,
			preparedStmt.StmtDB, prepared.SchemaVersion, latestSchemaVersion); err != nil {
			return nil, nil, err
		}
	}

	isBinProtocol := len(binProtoVars) > 0
	varsNum, binVarTypes, txtVarTypes := parseParamTypes(sctx, isBinProtocol, binProtoVars, txtProtoVars)

	if prepared.UseCache && prepared.CachedPlan != nil && !ignorePlanCache { // for point query plan
		if plan, names, ok, err := getPointQueryPlan(prepared, sessVars, stmtCtx); ok {
			return plan, names, err
		}
	}

	if prepared.UseCache && !ignorePlanCache { // for general plans
		if plan, names, ok, err := getGeneralPlan(ctx, sctx, cacheKey, bindSQL, is, preparedStmt,
			binVarTypes, txtVarTypes); err != nil || ok {
			return plan, names, err
		}
	}

	return generateNewPlan(ctx, sctx, is, preparedStmt, ignorePlanCache, cacheKey,
		latestSchemaVersion, isBinProtocol, varsNum, binVarTypes, txtVarTypes)
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, isBinProtocol bool, binProtoVars []types.Datum,
	txtProtoVars []expression.Expression) (varsNum int, binVarTypes []byte, txtVarTypes []*types.FieldType) {
	if isBinProtocol { // binary protocol
		varsNum = len(binProtoVars)
		for _, param := range binProtoVars {
			binVarTypes = append(binVarTypes, param.Kind())
		}
	} else { // txt protocol
		varsNum = len(txtProtoVars)
		for _, param := range txtProtoVars {
			name := param.(*expression.ScalarFunction).GetArgs()[0].String()
			tp := sctx.GetSessionVars().UserVarTypes[name]
			if tp == nil {
				tp = types.NewFieldType(mysql.TypeNull)
			}
			txtVarTypes = append(txtVarTypes, tp)
		}
	}
	return
}

func getPointQueryPlan(prepared *ast.Prepared, sessVars *variable.SessionVars, stmtCtx *stmtctx.StatementContext) (Plan,
	[]*types.FieldName, bool, error) {
	// short path for point-get plans
	// Rewriting the expression in the select.where condition  will convert its
	// type from "paramMarker" to "Constant".When Point Select queries are executed,
	// the expression in the where condition will not be evaluated,
	// so you don't need to consider whether prepared.useCache is enabled.
	plan := prepared.CachedPlan.(Plan)
	names := prepared.CachedNames.(types.NameSlice)
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

func getGeneralPlan(ctx context.Context, sctx sessionctx.Context, cacheKey kvcache.Key, bindSQL string,
	is infoschema.InfoSchema, preparedStmt *CachedPrepareStmt, binVarTypes []byte, txtVarTypes []*types.FieldType) (Plan,
	[]*types.FieldName, bool, error) {
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	if cacheValue, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
		if err := checkPreparedPriv(ctx, sctx, preparedStmt, is); err != nil {
			return nil, nil, false, err
		}
		cachedVals := cacheValue.([]*PlanCacheValue)
		for _, cachedVal := range cachedVals {
			if cachedVal.BindSQL != bindSQL {
				// When BindSQL does not match, it means that we have added a new binding,
				// and the original cached plan will be invalid,
				// so the original cached plan can be cleared directly
				sctx.PreparedPlanCache().Delete(cacheKey)
				break
			}
			if !cachedVal.varTypesUnchanged(binVarTypes, txtVarTypes) {
				continue
			}
			planValid := true
			for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
				if !unionScan && tableHasDirtyContent(sctx, tblInfo) {
					planValid = false
					// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
					// rebuilding the filters in UnionScan is pretty trivial.
					sctx.PreparedPlanCache().Delete(cacheKey)
					break
				}
			}
			if planValid {
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
				stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
				return cachedVal.Plan, cachedVal.OutPutNames, true, nil
			}
			break
		}
	}
	return nil, nil, false, nil
}

// generateNewPlan call the optimizer to generate a new plan for current statement
// and try to add it to cache
func generateNewPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, preparedStmt *CachedPrepareStmt,
	ignorePlanCache bool, cacheKey kvcache.Key, latestSchemaVersion int64, isBinProtocol bool, varsNum int, binVarTypes []byte,
	txtVarTypes []*types.FieldType) (Plan, []*types.FieldName, error) {
	prepared := preparedStmt.PreparedAst
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx

	planCacheMissCounter.Inc()
	stmt := prepared.Stmt
	p, names, err := OptimizeAstNode(ctx, sctx, stmt, is)
	if err != nil {
		return nil, nil, err
	}
	err = tryCachePointPlan(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return nil, nil, err
	}

	// We only cache the tableDual plan when the number of vars are zero.
	if containTableDual(p) && varsNum > 0 {
		stmtCtx.SkipPlanCache = true
	}
	if prepared.UseCache && !stmtCtx.SkipPlanCache && !ignorePlanCache {
		// rebuild key to exclude kv.TiFlash when stmt is not read only
		if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmt, sessVars) {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
			if cacheKey, err = NewPlanCacheKey(sessVars, preparedStmt.StmtText, preparedStmt.StmtDB,
				prepared.SchemaVersion, latestSchemaVersion); err != nil {
				return nil, nil, err
			}
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
		cached := NewPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan, isBinProtocol, binVarTypes, txtVarTypes, sessVars.StmtCtx.BindSQL)
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlan(p)
		stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
		if cacheVals, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
			hitVal := false
			for i, cacheVal := range cacheVals.([]*PlanCacheValue) {
				if cacheVal.varTypesUnchanged(binVarTypes, txtVarTypes) {
					hitVal = true
					cacheVals.([]*PlanCacheValue)[i] = cached
					break
				}
			}
			if !hitVal {
				cacheVals = append(cacheVals.([]*PlanCacheValue), cached)
			}
			sctx.PreparedPlanCache().Put(cacheKey, cacheVals)
		} else {
			sctx.PreparedPlanCache().Put(cacheKey, []*PlanCacheValue{cached})
		}
	}
	sessVars.FoundInPlanCache = false
	return p, names, err
}

// RebuildPlan4CachedPlan will rebuild this plan under current user parameters.
func RebuildPlan4CachedPlan(p Plan) error {
	sc := p.SCtx().GetSessionVars().StmtCtx
	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	return rebuildRange(p)
}

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
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens)
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
					ranges, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType)
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
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxCols, x.IdxColLens)
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
					ranges, err := ranger.BuildTableRange(x.AccessConditions, x.ctx, pkCol.RetType)
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
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, ts.AccessCondition, pkCols, pkColsLen)
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
			ts.Ranges, err = ranger.BuildTableRange(ts.AccessCondition, sctx, pkCol.RetType)
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
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxCols, is.IdxColLens)
	if err != nil {
		return err
	}
	if len(res.AccessConds) != len(is.AccessCondition) {
		return errors.New("rebuild range for cached plan failed")
	}
	is.Ranges = res.Ranges
	return
}

func checkPreparedPriv(_ context.Context, sctx sessionctx.Context,
	preparedObj *CachedPrepareStmt, is infoschema.InfoSchema) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		visitInfo := VisitInfo4PrivCheck(is, preparedObj.PreparedAst.Stmt, preparedObj.VisitInfos)
		if err := CheckPrivilege(sctx.GetSessionVars().ActiveRoles, pm, visitInfo); err != nil {
			return err
		}
	}
	err := CheckTableLock(sctx, is, preparedObj.VisitInfos)
	return err
}

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point update"
func tryCachePointPlan(_ context.Context, sctx sessionctx.Context,
	preparedStmt *CachedPrepareStmt, _ infoschema.InfoSchema, p Plan) error {
	if !sctx.GetSessionVars().StmtCtx.UseCache || sctx.GetSessionVars().StmtCtx.SkipPlanCache {
		return nil
	}
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
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
		prepared.CachedPlan = p
		prepared.CachedNames = names
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlan(p)
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
	}
	return err
}

func containTableDual(p Plan) bool {
	_, isTableDual := p.(*PhysicalTableDual)
	if isTableDual {
		return true
	}
	physicalPlan, ok := p.(PhysicalPlan)
	if !ok {
		return false
	}
	childContainTableDual := false
	for _, child := range physicalPlan.Children() {
		childContainTableDual = childContainTableDual || containTableDual(child)
	}
	return childContainTableDual
}
