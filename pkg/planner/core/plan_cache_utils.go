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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/zeropool"
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
	for i := range paramCount {
		extractor.markers[i].SetOrder(i)
	}

	prepared := &ast.Prepared{
		Stmt:     paramStmt,
		StmtType: stmtctx.GetStmtLabel(ctx, paramStmt),
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
	dbName := make([]ast.CIStr, 0, len(vars.StmtCtx.RelatedTableIDs))
	tbls := make([]table.Table, 0, len(vars.StmtCtx.RelatedTableIDs))
	relateVersion := make(map[int64]uint64, len(vars.StmtCtx.RelatedTableIDs))
	for id := range vars.StmtCtx.RelatedTableIDs {
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

// tableIDSlicePool is a pool for int64 slices used in hashInt64Uint64Map.
var tableIDSlicePool = zeropool.New[[]int64](func() []int64 {
	return make([]int64, 0, 8)
})

func hashInt64Uint64Map(b []byte, m map[int64]uint64) []byte {
	n := len(m)

	// Fast path for common cases (covers most scenarios)
	if n == 0 {
		return b
	}
	if n == 1 {
		// Single table: no need for allocation or sorting
		for k, v := range m {
			b = codec.EncodeInt(b, k)
			b = codec.EncodeUint(b, v)
			return b
		}
	}
	if n == 2 {
		// Two tables: direct comparison without array allocation
		var k1, k2 int64
		var v1, v2 uint64
		i := 0
		for k, v := range m {
			if i == 0 {
				k1, v1 = k, v
			} else {
				k2, v2 = k, v
			}
			i++
		}
		// Ensure sorted order
		if k1 > k2 {
			k1, k2 = k2, k1
			v1, v2 = v2, v1
		}
		b = codec.EncodeInt(b, k1)
		b = codec.EncodeUint(b, v1)
		b = codec.EncodeInt(b, k2)
		b = codec.EncodeUint(b, v2)
		return b
	}

	// Slow path for multiple tables
	keys := tableIDSlicePool.Get()[:0]
	defer tableIDSlicePool.Put(keys)

	// Ensure sufficient capacity
	if cap(keys) < n {
		keys = make([]int64, 0, n)
	}

	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

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
	if matchedBinding, matched, _ := bindinfo.MatchSQLBindingWithCache(sctx, stmt.PreparedAst.Stmt, &stmt.BindingInfo); matched {
		// Record the matched binding SQL so the plan cache key reflects the effective hints.
		binding = matchedBinding.BindSQL
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
	if stmt.hasSubquery && !vars.EnablePlanCacheForSubquery {
		return "", "", false, "the switch 'tidb_enable_plan_cache_for_subquery' is off", nil
	}
	if len(stmt.limits) > 0 && !vars.EnablePlanCacheForParamLimit {
		return "", "", false, "the switch 'tidb_enable_plan_cache_for_param_limit' is off", nil
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

	// precalculate the length of the hash buffer, note each time add an element to the buffer, need
	// to update hashLen accordingly
	// basic informations
	hashLen := len(userName) + len(hostName) + len(stmtDB) + len(stmt.StmtText)
	// schemaVersion + relateVersion + pruneMode
	hashLen += 8 + len(stmt.RelateVersion)*16 + len(pruneMode)
	// latestSchemaVersion + sqlMode + timeZoneOffset + isolationReadEngines + selectLimit
	hashLen += 8 + 8 + 8 + 4 /*len(kv.TiDB.Name())*/ + 4 /*len(kv.TiKV.Name())*/ + 7 /*len(kv.TiFlash.Name())*/ + 8
	// binding + connCharset + connCollation + inRestrictedSQL + readOnly + superReadOnly + exprPushdownBlacklistReloadTimeStamp + hasSubquery + foreignKeyChecks
	hashLen += len(binding) + len(connCharset) + len(connCollation) + 3 + 8 + 2
	if len(stmt.limits) > 0 {
		// '|' + each limit count/offset takes 8 bytes + '|'
		hashLen += 2 + len(stmt.limits)*2*8
	}
	if vars.GetSessionVars().PlanCacheInvalidationOnFreshStats {
		// statsVerHash
		hashLen += 8
	}
	// dirty tables
	hashLen += 8 * len(vars.StmtCtx.TblInfo2UnionScan)
	// txn status
	hashLen += 6

	hash := make([]byte, 0, hashLen)
	// hashInitCap is not used, just for test purposes
	hashInitCap := cap(hash)
	hash = append(hash, userName...)
	hash = append(hash, hostName...)
	hash = append(hash, stmtDB...)
	hash = append(hash, stmt.StmtText...)
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
	hash = append(hash, binding...)
	hash = append(hash, connCharset...)
	hash = append(hash, connCollation...)
	hash = append(hash, bool2Byte(vars.InRestrictedSQL))
	hash = append(hash, bool2Byte(vardef.RestrictedReadOnly.Load()))
	hash = append(hash, bool2Byte(vardef.VarTiDBSuperReadOnly.Load()))
	// expr-pushdown-blacklist can affect query optimization, so we need to consider it in plan cache.
	hash = codec.EncodeInt(hash, expression.ExprPushDownBlackListReloadTimeStamp.Load())

	// whether this query has sub-query
	hash = append(hash, bool2Byte(stmt.hasSubquery))

	// this variable might affect the plan
	hash = append(hash, bool2Byte(vars.ForeignKeyChecks))

	// "limit ?" can affect the cached plan: "limit 1" and "limit 10000" should use different plans.
	if len(stmt.limits) > 0 {
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
		// Get int64 slice from pool
		dirtyTableIDs := dirtyTableIDsPool.Get()[:0]
		if cap(dirtyTableIDs) < len(dirtyTables) {
			dirtyTableIDs = make([]int64, 0, len(dirtyTables))
		}
		for t, dirty := range dirtyTables {
			if !dirty {
				continue
			}
			dirtyTableIDs = append(dirtyTableIDs, t.ID)
		}
		slices.Sort(dirtyTableIDs)
		for _, id := range dirtyTableIDs {
			hash = codec.EncodeInt(hash, id)
		}
		// Return slice to pool
		dirtyTableIDsPool.Put(dirtyTableIDs)
	}

	// txn status
	hash = append(hash, '|')
	hash = append(hash, bool2Byte(vars.InTxn()))
	hash = append(hash, bool2Byte(vars.IsAutocommit()))
	hash = append(hash, bool2Byte(config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()))
	hash = append(hash, bool2Byte(vars.StmtCtx.ForShareLockEnabledByNoop))
	hash = append(hash, bool2Byte(vars.SharedLockPromotion))

	if intest.InTest {
		if cap(hash) != hashInitCap {
			panic("unexpected hash buffer realloc in NewPlanCacheKey")
		}
	}

	return string(hack.String(hash)), binding, true, "", nil
}

func bool2Byte(flag bool) byte {
	if flag {
		return '1'
	}
	return '0'
}

