// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/texttree"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")
var planCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("cache_miss")

// ShowDDL is for showing DDL information.
type ShowDDL struct {
	baseSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDDLJobQueries is for showing DDL job queries sql.
type ShowDDLJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global row ID.
type ShowNextRowID struct {
	baseSchemaProducer
	TableName *ast.TableName
}

// CheckTable is used for checking table data, built from the 'admin check table' statement.
type CheckTable struct {
	baseSchemaProducer

	DBName             string
	Table              table.Table
	IndexInfos         []*model.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
	CheckIndex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string
}

// CheckIndexRange is used for checking index data, output the index values that handle within begin and end.
type CheckIndexRange struct {
	baseSchemaProducer

	Table     *ast.TableName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumTable is used for calculating table checksum, built from the `admin checksum table` statement.
type ChecksumTable struct {
	baseSchemaProducer

	Tables []*ast.TableName
}

// CancelDDLJobs represents a cancel DDL jobs plan.
type CancelDDLJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist table.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptRuleBlacklist reloads the data from opt_rule_blacklist table.
type ReloadOptRuleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsAction indicate action will be taken on plugins.
type AdminPluginsAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates tidb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	Action  AdminPluginsAction
	Plugins []string
}

// AdminShowTelemetry displays telemetry status including tracking ID, status and so on.
type AdminShowTelemetry struct {
	baseSchemaProducer
}

// AdminResetTelemetryID regenerates a new telemetry tracking ID.
type AdminResetTelemetryID struct {
	baseSchemaProducer
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name    string
	SQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name         string
	TxtProtoVars []expression.Expression // parsed variables under text protocol
	BinProtoVars []types.Datum           // parsed variables under binary protocol
	ExecID       uint32
	// Deprecated: SnapshotTS now is only used for asserting after refactoring stale read, it will be removed later.
	SnapshotTS uint64
	// Deprecated: IsStaleness now is only used for asserting after refactoring stale read, it will be removed later.
	IsStaleness bool
	// Deprecated: ReadReplicaScope now is only used for asserting after refactoring stale read, it will be removed later.
	ReadReplicaScope string
	Stmt             ast.StmtNode
	StmtType         string
	Plan             Plan
}

// Check if result of GetVar expr is BinaryLiteral
// Because GetVar use String to represent BinaryLiteral, here we need to convert string back to BinaryLiteral.
func isGetVarBinaryLiteral(sctx sessionctx.Context, expr expression.Expression) (res bool) {
	scalarFunc, ok := expr.(*expression.ScalarFunction)
	if ok && scalarFunc.FuncName.L == ast.GetVar {
		name, isNull, err := scalarFunc.GetArgs()[0].EvalString(sctx, chunk.Row{})
		if err != nil || isNull {
			res = false
		} else if dt, ok2 := sctx.GetSessionVars().Users[name]; ok2 {
			res = (dt.Kind() == types.KindBinaryLiteral)
		}
	}
	return res
}

// OptimizePreparedPlan optimizes the prepared statement.
func (e *Execute) OptimizePreparedPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema) error {
	vars := sctx.GetSessionVars()
	if e.Name != "" {
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	preparedPointer, ok := vars.PreparedStmts[e.ExecID]
	if !ok {
		return errors.Trace(ErrStmtNotFound)
	}
	preparedObj, ok := preparedPointer.(*CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	vars.StmtCtx.StmtType = prepared.StmtType

	paramLen := len(e.BinProtoVars)
	if paramLen > 0 {
		// for binary protocol execute, argument is placed in vars.BinProtoVars
		if len(prepared.Params) != paramLen {
			return errors.Trace(ErrWrongParamCount)
		}
		vars.PreparedParams = e.BinProtoVars
		for i, val := range vars.PreparedParams {
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Datum = val
			param.InExecute = true
		}
	} else {
		// for `execute stmt using @a, @b, @c`, using value in e.TxtProtoVars
		if len(prepared.Params) != len(e.TxtProtoVars) {
			return errors.Trace(ErrWrongParamCount)
		}

		for i, usingVar := range e.TxtProtoVars {
			val, err := usingVar.Eval(chunk.Row{})
			if err != nil {
				return err
			}
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			if isGetVarBinaryLiteral(sctx, usingVar) {
				binVal, convErr := val.ToBytes()
				if convErr != nil {
					return convErr
				}
				val.SetBinaryLiteral(types.BinaryLiteral(binVal))
			}
			param.Datum = val
			param.InExecute = true
			vars.PreparedParams = append(vars.PreparedParams, val)
		}
	}

	// Just setting `e.SnapshotTS`, `e.ReadReplicaScope` and `e.IsStaleness` with the return value of `handleExecuteBuilderOption`
	// for asserting the stale read context after refactoring is exactly the same with the previous logic.
	snapshotTS, readReplicaScope, isStaleness, err := handleExecuteBuilderOption(sctx, preparedObj)
	if err != nil {
		return err
	}
	e.SnapshotTS = snapshotTS
	e.ReadReplicaScope = readReplicaScope
	e.IsStaleness = isStaleness

	failpoint.Inject("assertStaleReadForOptimizePreparedPlan", func() {
		staleread.AssertStmtStaleness(sctx, isStaleness)
		if isStaleness {
			is2, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(snapshotTS)
			if err != nil {
				panic(err)
			}

			if is.SchemaMetaVersion() != is2.SchemaMetaVersion() {
				panic(fmt.Sprintf("%d != %d", is.SchemaMetaVersion(), is2.SchemaMetaVersion()))
			}
		}
	})
	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schema version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schema version like prepared plan cache key
		prepared.CachedPlan = nil
		preparedObj.Executor = nil
		preparedObj.ColumnInfos = nil
		// If the schema version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schema changed.
		// Example:
		// When running update in prepared statement's schema version distinguished from the one of execute statement
		// We should reset the tableRefs in the prepared update statements, otherwise, the ast nodes still hold the old
		// tableRefs columnInfo which will cause chaos in logic of trying point get plan. (should ban non-public column)
		ret := &PreprocessorReturn{InfoSchema: is}
		err := Preprocess(sctx, prepared.Stmt, InPrepare, WithPreprocessorReturn(ret))
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	// If the lastUpdateTime less than expiredTimeStamp4PC,
	// it means other sessions have executed 'admin flush instance plan_cache'.
	// So we need to clear the current session's plan cache.
	// And update lastUpdateTime to the newest one.
	expiredTimeStamp4PC := domain.GetDomain(sctx).ExpiredTimeStamp4PC()
	if prepared.UseCache && expiredTimeStamp4PC.Compare(vars.LastUpdateTime4PC) > 0 {
		sctx.PreparedPlanCache().DeleteAll()
		prepared.CachedPlan = nil
		vars.LastUpdateTime4PC = expiredTimeStamp4PC
	}
	err = e.getPhysicalPlan(ctx, sctx, is, preparedObj)
	if err != nil {
		return err
	}
	e.Stmt = prepared.Stmt
	return nil
}

// Deprecated: it will be removed later. Now it is only used for asserting
func handleExecuteBuilderOption(sctx sessionctx.Context,
	preparedObj *CachedPrepareStmt) (snapshotTS uint64, readReplicaScope string, isStaleness bool, err error) {
	snapshotTS = 0
	readReplicaScope = oracle.GlobalTxnScope
	isStaleness = false
	err = nil
	vars := sctx.GetSessionVars()
	readTS := vars.TxnReadTS.PeakTxnReadTS()
	if readTS > 0 {
		// It means we meet following case:
		// 1. prepare p from 'select * from t as of timestamp now() - x seconds'
		// 1. set transaction read only as of timestamp ts2
		// 2. execute prepare p
		// The execute statement would be refused due to timestamp conflict
		if preparedObj.SnapshotTSEvaluator != nil {
			err = ErrAsOf.FastGenWithCause("as of timestamp can't be set after set transaction read only as of.")
			return
		}
		snapshotTS = vars.TxnReadTS.UseTxnReadTS()
		isStaleness = true
		readReplicaScope = config.GetTxnScopeFromConfig()
		return
	}
	// It means we meet following case:
	// 1. prepare p from 'select * from t as of timestamp ts1'
	// 1. begin
	// 2. execute prepare p
	// The execute statement would be refused due to timestamp conflict
	if preparedObj.SnapshotTSEvaluator != nil {
		if vars.InTxn() {
			err = ErrAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
			return
		}
		// if preparedObj.SnapshotTSEvaluator != nil, it is a stale read SQL:
		// which means its infoschema is specified by the SQL, not the current/latest infoschema
		snapshotTS, err = preparedObj.SnapshotTSEvaluator(sctx)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		isStaleness = true
		readReplicaScope = config.GetTxnScopeFromConfig()
		return
	}
	// It means we meet following case:
	// 1. prepare p from 'select * from t'
	// 1. start transaction read only as of timestamp ts1
	// 2. execute prepare p
	if vars.InTxn() && vars.TxnCtx.IsStaleness {
		isStaleness = true
		snapshotTS = vars.TxnCtx.StartTS
		readReplicaScope = vars.TxnCtx.TxnScope
		return
	}
	return
}

func (e *Execute) checkPreparedPriv(ctx context.Context, sctx sessionctx.Context,
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

func (e *Execute) setFoundInPlanCache(sctx sessionctx.Context, opt bool) error {
	vars := sctx.GetSessionVars()
	err := vars.SetSystemVar(variable.TiDBFoundInPlanCache, variable.BoolToOnOff(opt))
	return err
}

// GetBindSQL4PlanCache used to get the bindSQL for plan cache to build the plan cache key.
func GetBindSQL4PlanCache(sctx sessionctx.Context, preparedStmt *CachedPrepareStmt) string {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || preparedStmt.PreparedAst.Stmt == nil || preparedStmt.NormalizedSQL4PC == "" || preparedStmt.SQLDigest4PC == "" {
		return ""
	}
	if sctx.Value(bindinfo.SessionBindInfoKeyType) == nil {
		return ""
	}
	sessionHandle := sctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindRecord := sessionHandle.GetBindRecord(preparedStmt.SQLDigest4PC, preparedStmt.NormalizedSQL4PC, "")
	if bindRecord != nil {
		enabledBinding := bindRecord.FindEnabledBinding()
		if enabledBinding != nil {
			return enabledBinding.BindSQL
		}
	}
	globalHandle := domain.GetDomain(sctx).BindHandle()
	if globalHandle == nil {
		return ""
	}
	bindRecord = globalHandle.GetBindRecord(preparedStmt.SQLDigest4PC, preparedStmt.NormalizedSQL4PC, "")
	if bindRecord != nil {
		enabledBinding := bindRecord.FindEnabledBinding()
		if enabledBinding != nil {
			return enabledBinding.BindSQL
		}
	}
	return ""
}

func (e *Execute) getPhysicalPlan(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, preparedStmt *CachedPrepareStmt) (err error) {
	var cacheKey kvcache.Key
	sessVars := sctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	prepared := preparedStmt.PreparedAst
	stmtCtx.UseCache = prepared.UseCache

	var bindSQL string
	if prepared.UseCache {
		bindSQL = GetBindSQL4PlanCache(sctx, preparedStmt)
		if cacheKey, err = NewPlanCacheKey(sctx.GetSessionVars(), preparedStmt.StmtText, preparedStmt.StmtDB, prepared.SchemaVersion); err != nil {
			return err
		}
	}

	var varsNum int
	var binVarTypes []byte
	var txtVarTypes []*types.FieldType
	isBinProtocol := len(e.BinProtoVars) > 0
	if isBinProtocol { // binary protocol
		varsNum = len(e.BinProtoVars)
		for _, param := range e.BinProtoVars {
			binVarTypes = append(binVarTypes, param.Kind())
		}
	} else { // txt protocol
		varsNum = len(e.TxtProtoVars)
		for _, param := range e.TxtProtoVars {
			name := param.(*expression.ScalarFunction).GetArgs()[0].String()
			tp := sctx.GetSessionVars().UserVarTypes[name]
			if tp == nil {
				tp = types.NewFieldType(mysql.TypeNull)
			}
			txtVarTypes = append(txtVarTypes, tp)
		}
	}

	if prepared.UseCache && prepared.CachedPlan != nil { // short path for point-get plans
		// Rewriting the expression in the select.where condition  will convert its
		// type from "paramMarker" to "Constant".When Point Select queries are executed,
		// the expression in the where condition will not be evaluated,
		// so you don't need to consider whether prepared.useCache is enabled.
		plan := prepared.CachedPlan.(Plan)
		names := prepared.CachedNames.(types.NameSlice)
		err := e.RebuildPlan(plan)
		if err != nil {
			logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
			goto REBUILD
		}
		if metrics.ResettablePlanCacheCounterFortTest {
			metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
		} else {
			planCacheCounter.Inc()
		}
		err = e.setFoundInPlanCache(sctx, true)
		if err != nil {
			return err
		}
		e.names = names
		e.Plan = plan
		stmtCtx.PointExec = true
		return nil
	}
	if prepared.UseCache { // for general plans
		if cacheValue, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
			if err := e.checkPreparedPriv(ctx, sctx, preparedStmt, is); err != nil {
				return err
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
					err := e.RebuildPlan(cachedVal.Plan)
					if err != nil {
						logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
						goto REBUILD
					}
					err = e.setFoundInPlanCache(sctx, true)
					if err != nil {
						return err
					}
					if len(bindSQL) > 0 {
						// When the `len(bindSQL) > 0`, it means we use the binding.
						// So we need to record this.
						err = sessVars.SetSystemVar(variable.TiDBFoundInBinding, variable.BoolToOnOff(true))
						if err != nil {
							return err
						}
					}
					if metrics.ResettablePlanCacheCounterFortTest {
						metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
					} else {
						planCacheCounter.Inc()
					}
					e.names = cachedVal.OutPutNames
					e.Plan = cachedVal.Plan
					stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
					return nil
				}
				break
			}
		}
	}

REBUILD:
	planCacheMissCounter.Inc()
	stmt := prepared.Stmt
	p, names, err := OptimizeAstNode(ctx, sctx, stmt, is)
	if err != nil {
		return err
	}
	err = e.tryCachePointPlan(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return err
	}
	e.names = names
	e.Plan = p
	// We only cache the tableDual plan when the number of vars are zero.
	if containTableDual(p) && varsNum > 0 {
		stmtCtx.SkipPlanCache = true
	}
	if prepared.UseCache && !stmtCtx.SkipPlanCache {
		// rebuild key to exclude kv.TiFlash when stmt is not read only
		if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmt, sessVars) {
			delete(sessVars.IsolationReadEngines, kv.TiFlash)
			if cacheKey, err = NewPlanCacheKey(sessVars, preparedStmt.StmtText, preparedStmt.StmtDB, prepared.SchemaVersion); err != nil {
				return err
			}
			sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}
		cached := NewPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan, isBinProtocol, binVarTypes, txtVarTypes, sessVars.StmtCtx.BindSQL)
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
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
	err = e.setFoundInPlanCache(sctx, false)
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

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point update"
func (e *Execute) tryCachePointPlan(ctx context.Context, sctx sessionctx.Context,
	preparedStmt *CachedPrepareStmt, is infoschema.InfoSchema, p Plan) error {
	if !sctx.GetSessionVars().StmtCtx.UseCache || sctx.GetSessionVars().StmtCtx.SkipPlanCache {
		return nil
	}
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
	)
	switch p.(type) {
	case *PointGetPlan:
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
		sctx.GetSessionVars().StmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
	}
	return err
}

// RebuildPlan will rebuild this plan under current user parameters.
func (e *Execute) RebuildPlan(p Plan) error {
	sc := p.SCtx().GetSessionVars().StmtCtx
	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	return e.rebuildRange(p)
}

func (e *Execute) rebuildRange(p Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetSessionVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalIndexHashJoin:
		return e.rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexMergeJoin:
		return e.rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexJoin:
		if err := x.Ranges.Rebuild(); err != nil {
			return err
		}
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *PhysicalTableScan:
		err = e.buildRangeForTableScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalIndexScan:
		err = e.buildRangeForIndexScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalTableReader:
		err = e.rebuildRange(x.TablePlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexReader:
		err = e.rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		err = e.rebuildRange(x.IndexPlans[0])
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
			err = e.rebuildRange(partialPlans[0])
			if err != nil {
				return err
			}
		}
		// We don't need to handle the indexMerge.TablePlans, because the tablePlans
		// only can be (Selection) + TableRowIDScan. There have no range need to rebuild.
	case PhysicalPlan:
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *Update:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
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

func (e *Execute) buildRangeForTableScan(sctx sessionctx.Context, ts *PhysicalTableScan) (err error) {
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

func (e *Execute) buildRangeForIndexScan(sctx sessionctx.Context, is *PhysicalIndexScan) (err error) {
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

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	baseSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
}

// SQLBindOpType repreents the SQL bind type
type SQLBindOpType int

const (
	// OpSQLBindCreate represents the operation to create a SQL bind.
	OpSQLBindCreate SQLBindOpType = iota
	// OpSQLBindDrop represents the operation to drop a SQL bind.
	OpSQLBindDrop
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpEvolveBindings is used to evolve plan binding.
	OpEvolveBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
	// OpSetBindingStatus is used to set binding status.
	OpSetBindingStatus
)

// SQLBindPlan represents a plan for SQL bind.
type SQLBindPlan struct {
	baseSchemaProducer

	SQLBindOp    SQLBindOpType
	NormdOrigSQL string
	BindSQL      string
	IsGlobal     bool
	BindStmt     ast.StmtNode
	Db           string
	Charset      string
	Collation    string
	NewStatus    string
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode

	// IsFromRemote indicates whether the statement IS FROM REMOTE TiDB instance in cluster,
	//   and executing in co-processor.
	//   Used for `global kill`. See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
	IsFromRemote bool

	// StaleTxnStartTS is the StartTS that is used to build a staleness transaction by 'START TRANSACTION READ ONLY' statement.
	StaleTxnStartTS uint64
}

// PhysicalSimpleWrapper is a wrapper of `Simple` to implement physical plan interface.
//   Used for simple statements executing in coprocessor.
type PhysicalSimpleWrapper struct {
	basePhysicalPlan
	Inner Simple
}

// InsertGeneratedColumns is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedColumns struct {
	Columns      []*ast.ColumnName
	Exprs        []expression.Expression
	OnDuplicates []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Table         table.Table
	tableSchema   *expression.Schema
	tableColNames types.NameSlice
	Columns       []*ast.ColumnName
	Lists         [][]expression.Expression
	SetList       []*expression.Assignment

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema
	names4OnDuplicate  types.NameSlice

	GenCols InsertGeneratedColumns

	SelectPlan PhysicalPlan

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool

	RowLen int
}

// Update represents Update plan.
type Update struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreConstant bool

	VirtualAssignmentsOffset int

	SelectPlan PhysicalPlan

	TblColPosInfos TblColPosInfoSlice

	// Used when partition sets are given.
	// e.g. update t partition(p0) set a = 1;
	PartitionedTable []table.PartitionedTable

	tblID2Table map[int64]table.Table
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	IsMultiTable bool

	SelectPlan PhysicalPlan

	TblColPosInfos TblColPosInfoSlice
}

// AnalyzeInfo is used to store the database name, table name and partition name of analyze task.
type AnalyzeInfo struct {
	DBName        string
	TableName     string
	PartitionName string
	TableID       statistics.AnalyzeTableID
	Incremental   bool
	StatsVersion  int
	V2Options     *V2AnalyzeOptions
}

// V2AnalyzeOptions is used to hold analyze options information.
type V2AnalyzeOptions struct {
	PhyTableID  int64
	RawOpts     map[ast.AnalyzeOptionType]uint64
	FilledOpts  map[ast.AnalyzeOptionType]uint64
	ColChoice   model.ColumnChoice
	ColumnList  []*model.ColumnInfo
	IsPartition bool
}

// AnalyzeColumnsTask is used for analyze columns.
type AnalyzeColumnsTask struct {
	HandleCols       HandleCols
	CommonHandleInfo *model.IndexInfo
	ColsInfo         []*model.ColumnInfo
	TblInfo          *model.TableInfo
	Indexes          []*model.IndexInfo
	AnalyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *model.IndexInfo
	TblInfo   *model.TableInfo
	AnalyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	ColTasks   []AnalyzeColumnsTask
	IdxTasks   []AnalyzeIndexTask
	Opts       map[ast.AnalyzeOptionType]uint64
	OptionsMap map[int64]V2AnalyzeOptions
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal     bool
	OnDuplicate ast.OnDuplicateKeyHandlingType
	Path        string
	Table       *ast.TableName
	Columns     []*ast.ColumnName
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines uint64

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar

	GenCols InsertGeneratedColumns
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// PlanReplayer represents a plan replayer plan.
type PlanReplayer struct {
	baseSchemaProducer
	ExecStmt ast.StmtNode
	Analyze  bool
	Load     bool
	File     string
}

// IndexAdvise represents a index advise plan.
type IndexAdvise struct {
	baseSchemaProducer

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	TableInfo      *model.TableInfo
	PartitionNames []model.CIStr
	IndexInfo      *model.IndexInfo
	Lower          []types.Datum
	Upper          []types.Datum
	Num            int
	ValueLists     [][]types.Datum
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Table     table.Table
	IndexInfo *model.IndexInfo
}

// CompactTable represents a "ALTER TABLE [NAME] COMPACT ..." plan.
type CompactTable struct {
	baseSchemaProducer

	ReplicaKind ast.CompactReplicaKind
	TableInfo   *model.TableInfo
}

// DDL represents a DDL statement plan.
type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetPlan Plan
	IntoOpt    *ast.SelectIntoOption
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan       Plan
	Format           string
	Analyze          bool
	ExecStmt         ast.StmtNode
	RuntimeStatsColl *execdetails.RuntimeStatsColl

	Rows           [][]string
	ExplainRows    [][]string
	explainedPlans map[int]bool

	ctes []*PhysicalCTE
}

// GetExplainRowsForPlan get explain rows for plan.
func GetExplainRowsForPlan(plan Plan) (rows [][]string) {
	explain := &Explain{
		TargetPlan: plan,
		Format:     types.ExplainFormatROW,
		Analyze:    false,
	}
	if err := explain.RenderResult(); err != nil {
		return rows
	}
	return explain.Rows
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)
	if format == types.ExplainFormatTraditional {
		format = types.ExplainFormatROW
		e.Format = types.ExplainFormatROW
	}
	switch {
	case (format == types.ExplainFormatROW && (!e.Analyze && e.RuntimeStatsColl == nil)) || (format == types.ExplainFormatBrief):
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == types.ExplainFormatVerbose:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "task", "access object", "operator info"}
		}
	case format == types.ExplainFormatROW && (e.Analyze || e.RuntimeStatsColl != nil):
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == types.ExplainFormatHint:
		fieldNames = []string{"hint"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetPlan == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW, types.ExplainFormatBrief, types.ExplainFormatVerbose:
		if e.Rows == nil || e.Analyze {
			e.explainedPlans = map[int]bool{}
			err := e.explainPlanInRowFormat(e.TargetPlan, "root", "", "", true)
			if err != nil {
				return err
			}
			err = e.explainPlanInRowFormatCTE()
			if err != nil {
				return err
			}
		}
	case types.ExplainFormatDOT:
		if physicalPlan, ok := e.TargetPlan.(PhysicalPlan); ok {
			e.prepareDotInfo(physicalPlan)
		}
	case types.ExplainFormatHint:
		hints := GenHintsFromPhysicalPlan(e.TargetPlan)
		hints = append(hints, hint.ExtractTableHintsFromStmtNode(e.ExecStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

func (e *Explain) explainPlanInRowFormatCTE() (err error) {
	explainedCTEPlan := make(map[int]struct{})
	for i := 0; i < len(e.ctes); i++ {
		x := (*CTEDefinition)(e.ctes[i])
		// skip if the CTE has been explained, the same CTE has same IDForStorage
		if _, ok := explainedCTEPlan[x.CTE.IDForStorage]; ok {
			continue
		}
		e.prepareOperatorInfo(x, "root", "", "", true)
		childIndent := texttree.Indent4Child("", true)
		err = e.explainPlanInRowFormat(x.SeedPlan, "root", "(Seed Part)", childIndent, x.RecurPlan == nil)
		if x.RecurPlan != nil {
			err = e.explainPlanInRowFormat(x.RecurPlan, "root", "(Recursive Part)", childIndent, true)
		}
		explainedCTEPlan[x.CTE.IDForStorage] = struct{}{}
	}

	return
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p Plan, taskType, driverSide, indent string, isLastChild bool) (err error) {
	e.prepareOperatorInfo(p, taskType, driverSide, indent, isLastChild)
	e.explainedPlans[p.ID()] = true
	if e.ctx != nil && e.ctx.GetSessionVars() != nil && e.ctx.GetSessionVars().StmtCtx != nil {
		if optimInfo, ok := e.ctx.GetSessionVars().StmtCtx.OptimInfo[p.ID()]; ok {
			e.ctx.GetSessionVars().StmtCtx.AppendNote(errors.New(optimInfo))
		}
	}

	// For every child we create a new sub-tree rooted by it.
	childIndent := texttree.Indent4Child(indent, isLastChild)

	if physPlan, ok := p.(PhysicalPlan); ok {
		// indicate driven side and driving side of 'join' and 'apply'
		// See issue https://github.com/pingcap/tidb/issues/14602.
		driverSideInfo := make([]string, len(physPlan.Children()))
		buildSide := -1

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				buildSide = plan.InnerChildIdx ^ 1
			} else {
				buildSide = plan.InnerChildIdx
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				buildSide = 0
			} else {
				buildSide = 1
			}
		case *PhysicalIndexJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexMergeJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexHashJoin:
			buildSide = plan.InnerChildIdx ^ 1
		}

		if buildSide != -1 {
			driverSideInfo[0], driverSideInfo[1] = "(Build)", "(Probe)"
		} else {
			buildSide = 0
		}

		// Always put the Build above the Probe.
		for i := range physPlan.Children() {
			pchild := &physPlan.Children()[i^buildSide]
			if e.explainedPlans[(*pchild).ID()] {
				continue
			}
			err = e.explainPlanInRowFormat(*pchild, taskType, driverSideInfo[i], childIndent, i == len(physPlan.Children())-1)
			if err != nil {
				return
			}
		}
	}

	switch x := p.(type) {
	case *PhysicalTableReader:
		switch x.StoreType {
		case kv.TiKV, kv.TiFlash, kv.TiDB:
			// expected do nothing
		default:
			return errors.Errorf("the store type %v is unknown", x.StoreType)
		}
		taskName := x.ReadReqType.Name() + "[" + x.StoreType.Name() + "]"
		err = e.explainPlanInRowFormat(x.tablePlan, taskName, "", childIndent, true)
	case *PhysicalIndexReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop[tikv]", "", childIndent, true)
	case *PhysicalIndexLookUpReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "cop[tikv]", "(Build)", childIndent, false)
		if err != nil {
			return
		}
		err = e.explainPlanInRowFormat(x.tablePlan, "cop[tikv]", "(Probe)", childIndent, true)
	case *PhysicalIndexMergeReader:
		for _, pchild := range x.partialPlans {
			err = e.explainPlanInRowFormat(pchild, "cop[tikv]", "(Build)", childIndent, false)
			if err != nil {
				return
			}
		}
		err = e.explainPlanInRowFormat(x.tablePlan, "cop[tikv]", "(Probe)", childIndent, true)
	case *Insert:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Update:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Delete:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Execute:
		if x.Plan != nil {
			err = e.explainPlanInRowFormat(x.Plan, "root", "", indent, true)
		}
	case *PhysicalCTE:
		e.ctes = append(e.ctes, x)
	case *PhysicalShuffleReceiverStub:
		err = e.explainPlanInRowFormat(x.DataSource, "root", "", childIndent, true)
	}
	return
}

func getRuntimeInfo(ctx sessionctx.Context, p Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		if runtimeStatsColl == nil {
			return
		}
	}
	explainID := p.ID()

	// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
	// So check copTaskExecDetail first and print the real cop task information if it's not empty.
	if runtimeStatsColl.ExistsRootStats(explainID) {
		rootStats := runtimeStatsColl.GetRootStats(explainID)
		analyzeInfo = rootStats.String()
		actRows = strconv.FormatInt(rootStats.GetActRows(), 10)
	} else {
		actRows = "0"
	}
	if runtimeStatsColl.ExistsCopStats(explainID) {
		if len(analyzeInfo) > 0 {
			analyzeInfo += ", "
		}
		copStats := runtimeStatsColl.GetCopStats(explainID)
		analyzeInfo += copStats.String()
		actRows = strconv.FormatInt(copStats.GetActRows(), 10)
	}
	memoryInfo = "N/A"
	memTracker := ctx.GetSessionVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	if memTracker != nil {
		memoryInfo = memTracker.FormatBytes(memTracker.MaxConsumed())
	}

	diskInfo = "N/A"
	diskTracker := ctx.GetSessionVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	if diskTracker != nil {
		diskInfo = diskTracker.FormatBytes(diskTracker.MaxConsumed())
	}
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func (e *Explain) prepareOperatorInfo(p Plan, taskType, driverSide, indent string, isLastChild bool) {
	if p.ExplainID().String() == "_0" {
		return
	}

	id := texttree.PrettyIdentifier(p.ExplainID().String()+driverSide, indent, isLastChild)
	estRows, estCost, accessObject, operatorInfo := e.getOperatorInfo(p, id)

	var row []string
	if e.Analyze || e.RuntimeStatsColl != nil {
		row = []string{id, estRows}
		if strings.ToLower(e.Format) == types.ExplainFormatVerbose {
			row = append(row, estCost)
		}
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(e.ctx, p, e.RuntimeStatsColl)
		row = append(row, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo)
	} else {
		row = []string{id, estRows}
		if strings.ToLower(e.Format) == types.ExplainFormatVerbose {
			row = append(row, estCost)
		}
		row = append(row, taskType, accessObject, operatorInfo)
	}
	e.Rows = append(e.Rows, row)
}

func (e *Explain) getOperatorInfo(p Plan, id string) (string, string, string, string) {
	// For `explain for connection` statement, `e.ExplainRows` will be set.
	for _, row := range e.ExplainRows {
		if len(row) < 5 {
			panic("should never happen")
		}
		if row[0] == id {
			return row[1], "N/A", row[3], row[4]
		}
	}
	estRows := "N/A"
	if si := p.statsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}
	estCost := "N/A"
	if pp, ok := p.(PhysicalPlan); ok {
		if p.SCtx().GetSessionVars().EnableNewCostInterface {
			planCost, _ := pp.GetPlanCost(property.RootTaskType, 0)
			estCost = strconv.FormatFloat(planCost, 'f', 2, 64)
		} else {
			estCost = strconv.FormatFloat(pp.Cost(), 'f', 2, 64)
		}
	}
	var accessObject, operatorInfo string
	if plan, ok := p.(dataAccesser); ok {
		accessObject = plan.AccessObject(false)
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(partitionAccesser); ok && e.ctx != nil {
			accessObject = pa.accessObject(e.ctx)
		}
		operatorInfo = p.ExplainInfo()
	}
	return estRows, estCost, accessObject, operatorInfo
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
	}

	var copTasks []PhysicalPlan
	var pipelines []string

	for planQueue := []PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch copPlan := curPlan.(type) {
		case *PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.indexPlan.ExplainID()))
			copTasks = append(copTasks, copPlan.tablePlan)
			copTasks = append(copTasks, copPlan.indexPlan)
		case *PhysicalIndexMergeReader:
			for i := 0; i < len(copPlan.partialPlans); i++ {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.partialPlans[i].ExplainID()))
				copTasks = append(copTasks, copPlan.partialPlans[i])
			}
			if copPlan.tablePlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.tablePlan.ExplainID()))
				copTasks = append(copTasks, copPlan.tablePlan)
			}
		}
		for _, child := range curPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID())
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop, "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. session is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx sessionctx.Context, p Plan) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx), nil
	case *PhysicalTableReader:
		tableScan := v.TablePlans[0].(*PhysicalTableScan)
		isPointRange := len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPointNonNullable(ctx)
		if !isPointRange {
			return false, nil
		}
		pkLength := 1
		if tableScan.Table.IsCommonHandle {
			pkIdx := tables.FindPrimaryIndex(tableScan.Table)
			pkLength = len(pkIdx.Columns)
		}
		return len(tableScan.Ranges[0].LowVal) == pkLength, nil
	case *PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeatable-read
		// and the data and index would be inconsistent!
		// If the PointGetPlan needs to read data from Cache Table, we can't use max uint64,
		// because math.MaxUint64 always make cacheData invalid.
		noSecondRead := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
		if !noSecondRead {
			return false, nil
		}
		if v.TblInfo != nil && (v.TblInfo.TableCacheStatusType != model.TableCacheStatusDisable) {
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}

// IsAutoCommitTxn checks if session is in autocommit mode and not InTxn
// used for fast plan like point get
func IsAutoCommitTxn(ctx sessionctx.Context) bool {
	return ctx.GetSessionVars().IsAutocommit() && !ctx.GetSessionVars().InTxn()
}
