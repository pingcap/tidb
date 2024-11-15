// Copyright 2017 PingCAP, Inc.
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
	"cmp"
	"context"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// MaxCacheableLimitCount is the max limit count for cacheable query.
	MaxCacheableLimitCount = 10000
)

var (
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (*paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
		e.markers = append(e.markers, x)
	}
	return in, true
}

// GeneratePlanCacheStmtWithAST generates the PlanCacheStmt structure for this AST.
// paramSQL is the corresponding parameterized sql like 'select * from t where a<? and b>?'.
// paramStmt is the Node of paramSQL.
func GeneratePlanCacheStmtWithAST(ctx context.Context, sctx sessionctx.Context, isPrepStmt bool,
	paramSQL string, paramStmt ast.StmtNode, is infoschema.InfoSchema) (*PlanCacheStmt, base.Plan, int, error) {
	vars := sctx.GetSessionVars()
	var extractor paramMarkerExtractor
	paramStmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := paramStmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		return nil, nil, 0, plannererrors.ErrPrepareDDL
	}

	switch stmt := paramStmt.(type) {
	case *ast.ImportIntoStmt, *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, *ast.NonTransactionalDMLStmt:
		return nil, nil, 0, plannererrors.ErrUnsupportedPs
	case *ast.SelectStmt:
		if stmt.SelectIntoOpt != nil {
			return nil, nil, 0, plannererrors.ErrUnsupportedPs
		}
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return nil, nil, 0, plannererrors.ErrPsManyParam
	}

	ret := &PreprocessorReturn{InfoSchema: is} // is can be nil, and
	nodeW := resolve.NewNodeW(paramStmt)
	err := Preprocess(ctx, sctx, nodeW, InPrepare, WithPreprocessorReturn(ret))
	if err != nil {
		return nil, nil, 0, err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	slices.SortFunc(extractor.markers, func(i, j ast.ParamMarkerExpr) int {
		return cmp.Compare(i.(*driver.ParamMarkerExpr).Offset, j.(*driver.ParamMarkerExpr).Offset)
	})
	paramCount := len(extractor.markers)
	for i := 0; i < paramCount; i++ {
		extractor.markers[i].SetOrder(i)
	}

	prepared := &ast.Prepared{
		Stmt:     paramStmt,
		StmtType: ast.GetStmtLabel(paramStmt),
	}
	normalizedSQL, digest := parser.NormalizeDigest(prepared.Stmt.Text())

	var (
		cacheable bool
		reason    string
	)
	if (isPrepStmt && !vars.EnablePreparedPlanCache) || // prepared statement
		(!isPrepStmt && !vars.EnableNonPreparedPlanCache) { // non-prepared statement
		cacheable = false
		reason = "plan cache is disabled"
	} else {
		if isPrepStmt {
			cacheable, reason = IsASTCacheable(ctx, sctx.GetPlanCtx(), paramStmt, ret.InfoSchema)
		} else {
			cacheable = true // it is already checked here
		}

		if !cacheable && fixcontrol.GetBoolWithDefault(vars.OptimizerFixControl, fixcontrol.Fix49736, false) {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("force plan-cache: may use risky cached plan: %s", reason))
			cacheable = true
			reason = ""
		}

		if !cacheable {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("skip prepared plan-cache: " + reason))
		}
	}

	// For prepared statements like `prepare st from 'select * from t where a<?'`,
	// parameters are unknown here, so regard them all as NULL.
	// For non-prepared statements, all parameters are already initialized at `ParameterizeAST`, so no need to set NULL.
	if isPrepStmt {
		for i := range extractor.markers {
			param := extractor.markers[i].(*driver.ParamMarkerExpr)
			param.Datum.SetNull()
			param.InExecute = false
		}
	}

	var p base.Plan
	destBuilder, _ := NewPlanBuilder().Init(sctx.GetPlanCtx(), ret.InfoSchema, hint.NewQBHintHandler(nil))
	p, err = destBuilder.Build(ctx, nodeW)
	if err != nil {
		return nil, nil, 0, err
	}

	if cacheable && destBuilder.optFlag&rule.FlagPartitionProcessor > 0 {
		// dynamic prune mode is not used, could be that global statistics not yet available!
		cacheable = false
		reason = "static partition prune mode used"
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("skip prepared plan-cache: " + reason))
	}

	// Collect information for metadata lock.
	dbName := make([]model.CIStr, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	tbls := make([]table.Table, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	relateVersion := make(map[int64]uint64, len(vars.StmtCtx.MDLRelatedTableIDs))
	for id := range vars.StmtCtx.MDLRelatedTableIDs {
		tbl, ok := is.TableByID(ctx, id)
		if !ok {
			logutil.BgLogger().Error("table not found in info schema", zap.Int64("tableID", id))
			return nil, nil, 0, errors.New("table not found in info schema")
		}
		db, ok := is.SchemaByID(tbl.Meta().DBID)
		if !ok {
			logutil.BgLogger().Error("database not found in info schema", zap.Int64("dbID", tbl.Meta().DBID))
			return nil, nil, 0, errors.New("database not found in info schema")
		}
		dbName = append(dbName, db.Name)
		tbls = append(tbls, tbl)
		relateVersion[id] = tbl.Meta().Revision
	}

	preparedObj := &PlanCacheStmt{
		PreparedAst:         prepared,
		ResolveCtx:          nodeW.GetResolveContext(),
		StmtDB:              vars.CurrentDB,
		StmtText:            paramSQL,
		VisitInfos:          destBuilder.GetVisitInfo(),
		NormalizedSQL:       normalizedSQL,
		SQLDigest:           digest,
		ForUpdateRead:       destBuilder.GetIsForUpdateRead(),
		SnapshotTSEvaluator: ret.SnapshotTSEvaluator,
		StmtCacheable:       cacheable,
		UncacheableReason:   reason,
		dbName:              dbName,
		tbls:                tbls,
		SchemaVersion:       ret.InfoSchema.SchemaMetaVersion(),
		RelateVersion:       relateVersion,
		Params:              extractor.markers,
	}

	stmtProcessor := &planCacheStmtProcessor{ctx: ctx, is: is, stmt: preparedObj}
	paramStmt.Accept(stmtProcessor)

	if err = checkPreparedPriv(ctx, sctx, preparedObj, ret.InfoSchema); err != nil {
		return nil, nil, 0, err
	}
	return preparedObj, p, paramCount, nil
}

func hashInt64Uint64Map(b []byte, m map[int64]uint64) []byte {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		v := m[k]
		b = codec.EncodeInt(b, k)
		b = codec.EncodeUint(b, v)
	}
	return b
}

// NewPlanCacheKey creates the plan cache key for this statement.
// Note: lastUpdatedSchemaVersion will only be set in the case of rc or for update read in order to
// differentiate the cache key. In other cases, it will be 0.
// All information that might affect the plan should be considered in this function.
func NewPlanCacheKey(sctx sessionctx.Context, stmt *PlanCacheStmt) (key, binding string, cacheable bool, reason string, err error) {
	binding, ignored := bindinfo.MatchSQLBindingForPlanCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo)
	if ignored {
		return "", binding, false, "ignore plan cache by binding", nil
	}

	// In rc or for update read, we need the latest schema version to decide whether we need to
	// rebuild the plan. So we set this value in rc or for update read. In other cases, let it be 0.
	var latestSchemaVersion int64
	if sctx.GetSessionVars().IsIsolation(ast.ReadCommitted) || stmt.ForUpdateRead {
		// In Rc or ForUpdateRead, we should check if the information schema has been changed since
		// last time. If it changed, we should rebuild the plan. Here, we use a different and more
		// up-to-date schema version which can lead plan cache miss and thus, the plan will be rebuilt.
		latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
	}

	// rebuild key to exclude kv.TiFlash when stmt is not read only
	vars := sctx.GetSessionVars()
	if _, isolationReadContainTiFlash := vars.IsolationReadEngines[kv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(stmt.PreparedAst.Stmt, vars) {
		delete(vars.IsolationReadEngines, kv.TiFlash)
		defer func() {
			vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		}()
	}

	if stmt.StmtText == "" {
		return "", "", false, "", errors.New("no statement text")
	}
	if stmt.SchemaVersion == 0 && !intest.InTest {
		return "", "", false, "", errors.New("Schema version uninitialized")
	}
	stmtDB := stmt.StmtDB
	if stmtDB == "" {
		stmtDB = vars.CurrentDB
	}
	timezoneOffset := 0
	if vars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(vars.TimeZone).Zone()
	}
	connCharset, connCollation := vars.GetCharsetInfo()

	// not allow to share the same plan among different users for safety.
	var userName, hostName string
	if sctx.GetSessionVars().User != nil { // might be nil if in test
		userName = sctx.GetSessionVars().User.AuthUsername
		hostName = sctx.GetSessionVars().User.AuthHostname
	}

	// the user might switch the prune mode dynamically
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()

	hash := make([]byte, 0, len(stmt.StmtText)*2) // TODO: a Pool for this
	hash = append(hash, hack.Slice(userName)...)
	hash = append(hash, hack.Slice(hostName)...)
	hash = append(hash, hack.Slice(stmtDB)...)
	hash = append(hash, hack.Slice(stmt.StmtText)...)
	hash = codec.EncodeInt(hash, stmt.SchemaVersion)
	hash = hashInt64Uint64Map(hash, stmt.RelateVersion)
	hash = append(hash, pruneMode...)
	// Only be set in rc or for update read and leave it default otherwise.
	// In Rc or ForUpdateRead, we should check whether the information schema has been changed when using plan cache.
	// If it changed, we should rebuild the plan. lastUpdatedSchemaVersion help us to decide whether we should rebuild
	// the plan in rc or for update read.
	hash = codec.EncodeInt(hash, latestSchemaVersion)
	hash = codec.EncodeInt(hash, int64(vars.SQLMode))
	hash = codec.EncodeInt(hash, int64(timezoneOffset))
	if _, ok := vars.IsolationReadEngines[kv.TiDB]; ok {
		hash = append(hash, kv.TiDB.Name()...)
	}
	if _, ok := vars.IsolationReadEngines[kv.TiKV]; ok {
		hash = append(hash, kv.TiKV.Name()...)
	}
	if _, ok := vars.IsolationReadEngines[kv.TiFlash]; ok {
		hash = append(hash, kv.TiFlash.Name()...)
	}
	hash = codec.EncodeInt(hash, int64(vars.SelectLimit))
	hash = append(hash, hack.Slice(binding)...)
	hash = append(hash, hack.Slice(connCharset)...)
	hash = append(hash, hack.Slice(connCollation)...)
	hash = append(hash, hack.Slice(strconv.FormatBool(vars.InRestrictedSQL))...)
	hash = append(hash, hack.Slice(strconv.FormatBool(variable.RestrictedReadOnly.Load()))...)
	hash = append(hash, hack.Slice(strconv.FormatBool(variable.VarTiDBSuperReadOnly.Load()))...)
	// expr-pushdown-blacklist can affect query optimization, so we need to consider it in plan cache.
	hash = codec.EncodeInt(hash, expression.ExprPushDownBlackListReloadTimeStamp.Load())

	// whether this query has sub-query
	if stmt.hasSubquery {
		if !vars.EnablePlanCacheForSubquery {
			return "", "", false, "the switch 'tidb_enable_plan_cache_for_subquery' is off", nil
		}
		hash = append(hash, '1')
	} else {
		hash = append(hash, '0')
	}

	// this variable might affect the plan
	hash = append(hash, bool2Byte(vars.ForeignKeyChecks))

	// "limit ?" can affect the cached plan: "limit 1" and "limit 10000" should use different plans.
	if len(stmt.limits) > 0 {
		if !vars.EnablePlanCacheForParamLimit {
			return "", "", false, "the switch 'tidb_enable_plan_cache_for_param_limit' is off", nil
		}
		hash = append(hash, '|')
		for _, node := range stmt.limits {
			for _, valNode := range []ast.ExprNode{node.Count, node.Offset} {
				if valNode == nil {
					continue
				}
				if param, isParam := valNode.(*driver.ParamMarkerExpr); isParam {
					typeExpected, val := CheckParamTypeInt64orUint64(param)
					if !typeExpected {
						return "", "", false, "unexpected value after LIMIT", nil
					}
					if val > MaxCacheableLimitCount {
						return "", "", false, "limit count is too large", nil
					}
					hash = codec.EncodeUint(hash, val)
				}
			}
		}
		hash = append(hash, '|')
	}

	// stats ver can affect cached plan
	if sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats {
		var statsVerHash uint64
		for _, t := range stmt.tables {
			statsVerHash += getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID) // use '+' as the hash function for simplicity
		}
		hash = codec.EncodeUint(hash, statsVerHash)
	}

	// handle dirty tables
	dirtyTables := vars.StmtCtx.TblInfo2UnionScan
	if len(dirtyTables) > 0 {
		dirtyTableIDs := make([]int64, 0, len(dirtyTables)) // TODO: a Pool for this
		for t, dirty := range dirtyTables {
			if !dirty {
				continue
			}
			dirtyTableIDs = append(dirtyTableIDs, t.ID)
		}
		sort.Slice(dirtyTableIDs, func(i, j int) bool { return dirtyTableIDs[i] < dirtyTableIDs[j] })
		for _, id := range dirtyTableIDs {
			hash = codec.EncodeInt(hash, id)
		}
	}

	// txn status
	hash = append(hash, '|')
	hash = append(hash, bool2Byte(vars.InTxn()))
	hash = append(hash, bool2Byte(vars.IsAutocommit()))
	hash = append(hash, bool2Byte(config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()))
	hash = append(hash, bool2Byte(vars.StmtCtx.ForShareLockEnabledByNoop))
	hash = append(hash, bool2Byte(vars.SharedLockPromotion))

	return string(hash), binding, true, "", nil
}

func bool2Byte(flag bool) byte {
	if flag {
		return '1'
	}
	return '0'
}

// PlanCacheValue stores the cached Statement and StmtNode.
type PlanCacheValue struct {
	Plan          base.Plan          // not-read-only, session might update it before reusing
	OutputColumns types.NameSlice    // read-only
	memoryUsage   int64              // read-only
	testKey       int64              // test-only
	paramTypes    []*types.FieldType // read-only, all parameters' types, different parameters may share same plan
	stmtHints     *hint.StmtHints    // read-only, hints which set session variables
}

// unKnownMemoryUsage represent the memory usage of uncounted structure, maybe need implement later
// 100 KiB is approximate consumption of a plan from our internal tests
const unKnownMemoryUsage = int64(50 * size.KB)

// MemoryUsage return the memory usage of PlanCacheValue
func (v *PlanCacheValue) MemoryUsage() (sum int64) {
	if v == nil {
		return
	}

	if v.memoryUsage > 0 {
		return v.memoryUsage
	}
	switch x := v.Plan.(type) {
	case base.PhysicalPlan:
		sum = x.MemoryUsage()
	case *Insert:
		sum = x.MemoryUsage()
	case *Update:
		sum = x.MemoryUsage()
	case *Delete:
		sum = x.MemoryUsage()
	default:
		sum = unKnownMemoryUsage
	}

	sum += size.SizeOfInterface + size.SizeOfSlice*2 + int64(cap(v.OutputColumns))*size.SizeOfPointer +
		size.SizeOfMap + size.SizeOfInt64*2
	if v.paramTypes != nil {
		sum += int64(cap(v.paramTypes)) * size.SizeOfPointer
		for _, ft := range v.paramTypes {
			sum += ft.MemoryUsage()
		}
	}

	for _, name := range v.OutputColumns {
		sum += name.MemoryUsage()
	}
	v.memoryUsage = sum
	return
}

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(plan base.Plan, names []*types.FieldName,
	paramTypes []*types.FieldType, stmtHints *hint.StmtHints) *PlanCacheValue {
	userParamTypes := make([]*types.FieldType, len(paramTypes))
	for i, tp := range paramTypes {
		userParamTypes[i] = tp.Clone()
	}
	return &PlanCacheValue{
		Plan:          plan,
		OutputColumns: names,
		paramTypes:    userParamTypes,
		stmtHints:     stmtHints.Clone(),
	}
}

// planCacheStmtProcessor records all query features which may affect plan selection.
type planCacheStmtProcessor struct {
	ctx  context.Context
	is   infoschema.InfoSchema
	stmt *PlanCacheStmt
}

// Enter implements Visitor interface.
func (f *planCacheStmtProcessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.Limit:
		f.stmt.limits = append(f.stmt.limits, node)
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		f.stmt.hasSubquery = true
	case *ast.TableName:
		t, err := f.is.TableByName(f.ctx, node.Schema, node.Name)
		if err == nil {
			f.stmt.tables = append(f.stmt.tables, t)
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (*planCacheStmtProcessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// PointGetExecutorCache caches the PointGetExecutor to further improve its performance.
// Don't forget to reset this executor when the prior plan is invalid.
type PointGetExecutorCache struct {
	ColumnInfos any
	// Executor is only used for point get scene.
	// Notice that we should only cache the PointGetExecutor that have a snapshot with MaxTS in it.
	// If the current plan is not PointGet or does not use MaxTS optimization, this value should be nil here.
	Executor any

	// FastPlan is only used for instance plan cache.
	// To ensure thread-safe, we have to clone each plan before reusing if using instance plan cache.
	// To reduce the memory allocation and increase performance, we cache the FastPlan here.
	FastPlan *PointGetPlan
}

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
	ResolveCtx  *resolve.Context
	StmtDB      string // which DB the statement will be processed over
	VisitInfos  []visitInfo
	Params      []ast.ParamMarkerExpr

	PointGet PointGetExecutorCache

	// below fields are for PointGet short path
	SchemaVersion int64

	// RelateVersion stores the true cache plan table schema version, since each table schema can be updated separately in transaction.
	RelateVersion map[int64]uint64

	StmtCacheable     bool   // Whether this stmt is cacheable.
	UncacheableReason string // Why this stmt is uncacheable.

	limits      []*ast.Limit
	hasSubquery bool
	tables      []table.Table // to capture table stats changes

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(context.Context, sessionctx.Context) (uint64, error)

	BindingInfo bindinfo.BindingMatchInfo

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string

	// dbName and tbls are used to add metadata lock.
	dbName []model.CIStr
	tbls   []table.Table
}

// GetPreparedStmt extract the prepared statement from the execute statement.
func GetPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (*PlanCacheStmt, error) {
	if stmt.PrepStmt != nil {
		return stmt.PrepStmt.(*PlanCacheStmt), nil
	}
	if stmt.Name != "" {
		prepStmt, err := vars.GetPreparedStmtByName(stmt.Name)
		if err != nil {
			return nil, err
		}
		stmt.PrepStmt = prepStmt
		return prepStmt.(*PlanCacheStmt), nil
	}
	return nil, plannererrors.ErrStmtNotFound
}

// CheckTypesCompatibility4PC compares FieldSlice with []*types.FieldType
// Currently this is only used in plan cache to check whether the types of parameters are compatible.
// If the types of parameters are compatible, we can use the cached plan.
// tpsExpected is types from cached plan
func checkTypesCompatibility4PC(expected, actual any) bool {
	if expected == nil || actual == nil {
		return true // no need to compare types
	}
	tpsExpected := expected.([]*types.FieldType)
	tpsActual := actual.([]*types.FieldType)
	if len(tpsExpected) != len(tpsActual) {
		return false
	}
	for i := range tpsActual {
		// We only use part of logic of `func (ft *FieldType) Equal(other *FieldType)` here because (1) only numeric and
		// string types will show up here, and (2) we don't need flen and decimal to be matched exactly to use plan cache
		tpEqual := (tpsExpected[i].GetType() == tpsActual[i].GetType()) ||
			(tpsExpected[i].GetType() == mysql.TypeVarchar && tpsActual[i].GetType() == mysql.TypeVarString) ||
			(tpsExpected[i].GetType() == mysql.TypeVarString && tpsActual[i].GetType() == mysql.TypeVarchar)
		if !tpEqual || tpsExpected[i].GetCharset() != tpsActual[i].GetCharset() || tpsExpected[i].GetCollate() != tpsActual[i].GetCollate() ||
			(tpsExpected[i].EvalType() == types.ETInt && mysql.HasUnsignedFlag(tpsExpected[i].GetFlag()) != mysql.HasUnsignedFlag(tpsActual[i].GetFlag())) {
			return false
		}
		// When the type is decimal, we should compare the Flen and Decimal.
		// We can only use the plan when both Flen and Decimal should less equal than the cached one.
		// We assume here that there is no correctness problem when the precision of the parameters is less than the precision of the parameters in the cache.
		if tpEqual && tpsExpected[i].GetType() == mysql.TypeNewDecimal && !(tpsExpected[i].GetFlen() >= tpsActual[i].GetFlen() && tpsExpected[i].GetDecimal() >= tpsActual[i].GetDecimal()) {
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCache(sctx base.PlanContext, path *util.AccessPath) bool {
	// PointGet might contain some over-optimized assumptions, like `a>=1 and a<=1` --> `a=1`, but
	// these assumptions may be broken after parameters change.

	if isSafePointGetPath4PlanCacheScenario1(path) {
		return true
	}

	// TODO: enable this fix control switch by default after more test cases are added.
	if sctx != nil && sctx.GetSessionVars() != nil && sctx.GetSessionVars().OptimizerFixControl != nil {
		fixControlOK := fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44830, false)
		if fixControlOK && (isSafePointGetPath4PlanCacheScenario2(path) || isSafePointGetPath4PlanCacheScenario3(path)) {
			return true
		}
	}

	return false
}

func isSafePointGetPath4PlanCacheScenario1(path *util.AccessPath) bool {
	// safe scenario 1: each column corresponds to a single EQ, `a=1 and b=2 and c=3` --> `[1, 2, 3]`
	if len(path.Ranges) <= 0 || path.Ranges[0].Width() != len(path.AccessConds) {
		return false
	}
	for _, accessCond := range path.AccessConds {
		f, ok := accessCond.(*expression.ScalarFunction)
		if !ok || f.FuncName.L != ast.EQ { // column = constant
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCacheScenario2(path *util.AccessPath) bool {
	// safe scenario 2: this Batch or PointGet is simply from a single IN predicate, `key in (...)`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.In {
		return false
	}
	return len(path.Ranges) == len(f.GetArgs())-1 // no duplicated values in this in-list for safety.
}

func isSafePointGetPath4PlanCacheScenario3(path *util.AccessPath) bool {
	// safe scenario 3: this Batch or PointGet is simply from a simple DNF like `key=? or key=? or key=?`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.LogicOr {
		return false
	}

	dnfExprs := expression.FlattenDNFConditions(f)
	if len(path.Ranges) != len(dnfExprs) {
		// no duplicated values in this in-list for safety.
		// e.g. `k=1 or k=2 or k=1` --> [[1, 1], [2, 2]]
		return false
	}

	for _, expr := range dnfExprs {
		f, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch f.FuncName.L {
		case ast.EQ: // (k=1 or k=2) --> [k=1, k=2]
		case ast.LogicAnd: // ((k1=1 and k2=1) or (k1=2 and k2=2)) --> [k1=1 and k2=1, k2=2 and k2=2]
			cnfExprs := expression.FlattenCNFConditions(f)
			if path.Ranges[0].Width() != len(cnfExprs) { // not all key columns are specified
				return false
			}
			for _, expr := range cnfExprs { // k1=1 and k2=1
				f, ok := expr.(*expression.ScalarFunction)
				if !ok || f.FuncName.L != ast.EQ {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, params []expression.Expression) (paramTypes []*types.FieldType) {
	ectx := sctx.GetExprCtx().GetEvalCtx()
	paramTypes = make([]*types.FieldType, 0, len(params))
	for _, param := range params {
		if c, ok := param.(*expression.Constant); ok { // from binary protocol
			paramTypes = append(paramTypes, c.GetType(ectx))
			continue
		}

		// from text protocol, there must be a GetVar function
		name := param.(*expression.ScalarFunction).GetArgs()[0].StringWithCtx(ectx, errors.RedactLogDisable)
		tp, ok := sctx.GetSessionVars().GetUserVarType(name)
		if !ok {
			tp = types.NewFieldType(mysql.TypeNull)
		}
		paramTypes = append(paramTypes, tp)
	}
	return
}
