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
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
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
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
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
	err := Preprocess(ctx, sctx, paramStmt, InPrepare, WithPreprocessorReturn(ret))
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
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("skip prepared plan-cache: " + reason))
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
	p, err = destBuilder.Build(ctx, paramStmt)
	if err != nil {
		return nil, nil, 0, err
	}

	if cacheable && destBuilder.optFlag&flagPartitionProcessor > 0 {
		// dynamic prune mode is not used, could be that global statistics not yet available!
		cacheable = false
		reason = "static partition prune mode used"
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("skip prepared plan-cache: " + reason))
	}

	features := new(PlanCacheQueryFeatures)
	paramStmt.Accept(features)

	// Collect information for metadata lock.
	dbName := make([]model.CIStr, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	tbls := make([]table.Table, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	relateVersion := make(map[int64]uint64, len(vars.StmtCtx.MDLRelatedTableIDs))
	for id := range vars.StmtCtx.MDLRelatedTableIDs {
		tbl, ok := is.TableByID(id)
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
		StmtDB:              vars.CurrentDB,
		StmtText:            paramSQL,
		VisitInfos:          destBuilder.GetVisitInfo(),
		NormalizedSQL:       normalizedSQL,
		SQLDigest:           digest,
		ForUpdateRead:       destBuilder.GetIsForUpdateRead(),
		SnapshotTSEvaluator: ret.SnapshotTSEvaluator,
		StmtCacheable:       cacheable,
		UncacheableReason:   reason,
		QueryFeatures:       features,
		dbName:              dbName,
		tbls:                tbls,
		SchemaVersion:       ret.InfoSchema.SchemaMetaVersion(),
		RelateVersion:       relateVersion,
		Params:              extractor.markers,
	}
	if err = checkPreparedPriv(sctx, preparedObj, ret.InfoSchema); err != nil {
		return nil, nil, 0, err
	}
	return preparedObj, p, paramCount, nil
}

// planCacheKey is used to access Plan Cache. We put some variables that do not affect the plan into planCacheKey, such as the sql text.
// Put the parameters that may affect the plan in planCacheValue.
// However, due to some compatibility reasons, we will temporarily keep some system variable-related values in planCacheKey.
// At the same time, because these variables have a small impact on plan, we will move them to PlanCacheValue later if necessary.
// TODO: maintain a sync.pool for this structure.
type planCacheKey struct {
	database      string
	connID        uint64
	stmtText      string
	schemaVersion int64
	tblVersionMap map[int64]uint64

	// Only be set in rc or for update read and leave it default otherwise.
	// In Rc or ForUpdateRead, we should check whether the information schema has been changed when using plan cache.
	// If it changed, we should rebuild the plan. lastUpdatedSchemaVersion help us to decide whether we should rebuild
	// the plan in rc or for update read.
	lastUpdatedSchemaVersion int64
	sqlMode                  mysql.SQLMode
	timezoneOffset           int
	isolationReadEngines     map[kv.StoreType]struct{}
	selectLimit              uint64
	bindSQL                  string
	connCollation            string
	inRestrictedSQL          bool
	restrictedReadOnly       bool
	TiDBSuperReadOnly        bool
	exprBlacklistTS          int64 // expr-pushdown-blacklist can affect query optimization, so we need to consider it in plan cache.

	memoryUsage int64 // Do not include in hash
	hash        []byte
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

// Hash implements Key interface.
func (key *planCacheKey) Hash() []byte {
	if len(key.hash) == 0 {
		if key.hash == nil {
			key.hash = make([]byte, 0, len(key.stmtText)*2)
		}
		key.hash = append(key.hash, hack.Slice(key.database)...)
		key.hash = codec.EncodeInt(key.hash, int64(key.connID))
		key.hash = append(key.hash, hack.Slice(key.stmtText)...)
		key.hash = codec.EncodeInt(key.hash, key.schemaVersion)
		key.hash = hashInt64Uint64Map(key.hash, key.tblVersionMap)
		key.hash = codec.EncodeInt(key.hash, key.lastUpdatedSchemaVersion)
		key.hash = codec.EncodeInt(key.hash, int64(key.sqlMode))
		key.hash = codec.EncodeInt(key.hash, int64(key.timezoneOffset))
		if _, ok := key.isolationReadEngines[kv.TiDB]; ok {
			key.hash = append(key.hash, kv.TiDB.Name()...)
		}
		if _, ok := key.isolationReadEngines[kv.TiKV]; ok {
			key.hash = append(key.hash, kv.TiKV.Name()...)
		}
		if _, ok := key.isolationReadEngines[kv.TiFlash]; ok {
			key.hash = append(key.hash, kv.TiFlash.Name()...)
		}
		key.hash = codec.EncodeInt(key.hash, int64(key.selectLimit))
		key.hash = append(key.hash, hack.Slice(key.bindSQL)...)
		key.hash = append(key.hash, hack.Slice(key.connCollation)...)
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.inRestrictedSQL))...)
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.restrictedReadOnly))...)
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.TiDBSuperReadOnly))...)
		key.hash = codec.EncodeInt(key.hash, key.exprBlacklistTS)
	}
	return key.hash
}

const emptyPlanCacheKeySize = int64(unsafe.Sizeof(planCacheKey{}))

// MemoryUsage return the memory usage of planCacheKey
func (key *planCacheKey) MemoryUsage() (sum int64) {
	if key == nil {
		return
	}

	if key.memoryUsage > 0 {
		return key.memoryUsage
	}
	sum = emptyPlanCacheKeySize + int64(len(key.database)+len(key.stmtText)+len(key.bindSQL)+len(key.connCollation)) +
		int64(len(key.isolationReadEngines))*size.SizeOfUint8 + int64(cap(key.hash))
	key.memoryUsage = sum
	return
}

// SetPstmtIDSchemaVersion implements PstmtCacheKeyMutator interface to change pstmtID and schemaVersion of cacheKey.
// so we can reuse Key instead of new every time.
func SetPstmtIDSchemaVersion(key kvcache.Key, stmtText string, schemaVersion int64, isolationReadEngines map[kv.StoreType]struct{}) {
	psStmtKey, isPsStmtKey := key.(*planCacheKey)
	if !isPsStmtKey {
		return
	}
	psStmtKey.stmtText = stmtText
	psStmtKey.schemaVersion = schemaVersion
	psStmtKey.isolationReadEngines = make(map[kv.StoreType]struct{})
	for k, v := range isolationReadEngines {
		psStmtKey.isolationReadEngines[k] = v
	}
	psStmtKey.hash = psStmtKey.hash[:0]
}

// NewPlanCacheKey creates a new planCacheKey object.
// Note: lastUpdatedSchemaVersion will only be set in the case of rc or for update read in order to
// differentiate the cache key. In other cases, it will be 0.
func NewPlanCacheKey(sessionVars *variable.SessionVars, stmtText, stmtDB string, schemaVersion, lastUpdatedSchemaVersion int64, bindSQL string, exprBlacklistTS int64, relatedSchemaVersion map[int64]uint64) (kvcache.Key, error) {
	if stmtText == "" {
		return nil, errors.New("no statement text")
	}
	if schemaVersion == 0 && !intest.InTest {
		return nil, errors.New("Schema version uninitialized")
	}
	if stmtDB == "" {
		stmtDB = sessionVars.CurrentDB
	}
	timezoneOffset := 0
	if sessionVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(sessionVars.TimeZone).Zone()
	}
	_, connCollation := sessionVars.GetCharsetInfo()

	key := &planCacheKey{
		database:                 stmtDB,
		connID:                   sessionVars.ConnectionID,
		stmtText:                 stmtText,
		schemaVersion:            schemaVersion,
		tblVersionMap:            make(map[int64]uint64),
		lastUpdatedSchemaVersion: lastUpdatedSchemaVersion,
		sqlMode:                  sessionVars.SQLMode,
		timezoneOffset:           timezoneOffset,
		isolationReadEngines:     make(map[kv.StoreType]struct{}),
		selectLimit:              sessionVars.SelectLimit,
		bindSQL:                  bindSQL,
		connCollation:            connCollation,
		inRestrictedSQL:          sessionVars.InRestrictedSQL,
		restrictedReadOnly:       variable.RestrictedReadOnly.Load(),
		TiDBSuperReadOnly:        variable.VarTiDBSuperReadOnly.Load(),
		exprBlacklistTS:          exprBlacklistTS,
	}
	for k, v := range sessionVars.IsolationReadEngines {
		key.isolationReadEngines[k] = v
	}
	for k, v := range relatedSchemaVersion {
		key.tblVersionMap[k] = v
	}
	return key, nil
}

// PlanCacheValue stores the cached Statement and StmtNode.
type PlanCacheValue struct {
	Plan              base.Plan
	OutputColumns     types.NameSlice
	TblInfo2UnionScan map[*model.TableInfo]bool
	memoryUsage       int64

	// matchOpts stores some fields help to choose a suitable plan
	matchOpts *utilpc.PlanCacheMatchOpts
	// stmtHints stores the hints which set session variables, because the hints won't be processed using cached plan.
	stmtHints *hint.StmtHints
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
		size.SizeOfMap + int64(len(v.TblInfo2UnionScan))*(size.SizeOfPointer+size.SizeOfBool) + size.SizeOfInt64*2
	if v.matchOpts != nil {
		sum += int64(cap(v.matchOpts.ParamTypes)) * size.SizeOfPointer
		for _, ft := range v.matchOpts.ParamTypes {
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
func NewPlanCacheValue(plan base.Plan, names []*types.FieldName, srcMap map[*model.TableInfo]bool,
	matchOpts *utilpc.PlanCacheMatchOpts, stmtHints *hint.StmtHints) *PlanCacheValue {
	dstMap := make(map[*model.TableInfo]bool)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	userParamTypes := make([]*types.FieldType, len(matchOpts.ParamTypes))
	for i, tp := range matchOpts.ParamTypes {
		userParamTypes[i] = tp.Clone()
	}
	return &PlanCacheValue{
		Plan:              plan,
		OutputColumns:     names,
		TblInfo2UnionScan: dstMap,
		matchOpts:         matchOpts,
		stmtHints:         stmtHints.Clone(),
	}
}

// PlanCacheQueryFeatures records all query features which may affect plan selection.
type PlanCacheQueryFeatures struct {
	limits      []*ast.Limit
	hasSubquery bool
	tables      []*ast.TableName // to capture table stats changes
}

// Enter implements Visitor interface.
func (f *PlanCacheQueryFeatures) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.Limit:
		f.limits = append(f.limits, node)
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		f.hasSubquery = true
	case *ast.TableName:
		f.tables = append(f.tables, node)
	}
	return in, false
}

// Leave implements Visitor interface.
func (*PlanCacheQueryFeatures) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// PointGetExecutorCache caches the PointGetExecutor to further improve its performance.
// Don't forget to reset this executor when the prior plan is invalid.
type PointGetExecutorCache struct {
	// Special (or tricky) optimization for PointGet Plan.
	// Store the PointGet Plan in PlanCacheStmt directly to bypass the LRU Cache to gain some performance improvement.
	// There is around 3% improvement, BenchmarkPreparedPointGet: 6450 ns/op --> 6250 ns/op.
	pointPlan      base.Plan
	pointPlanHints *hint.StmtHints
	columnNames    types.NameSlice

	ColumnInfos any
	// Executor is only used for point get scene.
	// Notice that we should only cache the PointGetExecutor that have a snapshot with MaxTS in it.
	// If the current plan is not PointGet or does not use MaxTS optimization, this value should be nil here.
	Executor any
}

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
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
	QueryFeatures     *PlanCacheQueryFeatures

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(sessionctx.Context) (uint64, error)

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

// GetMatchOpts get options to fetch plan or generate new plan
// we can add more options here
func GetMatchOpts(sctx sessionctx.Context, is infoschema.InfoSchema, stmt *PlanCacheStmt, params []expression.Expression) *utilpc.PlanCacheMatchOpts {
	var statsVerHash uint64
	var limitOffsetAndCount []uint64

	if stmt.QueryFeatures != nil {
		for _, node := range stmt.QueryFeatures.tables {
			t, err := is.TableByName(node.Schema, node.Name)
			if err != nil { // CTE in this case
				continue
			}
			statsVerHash += getLatestVersionFromStatsTable(sctx, t.Meta(), t.Meta().ID) // use '+' as the hash function for simplicity
		}

		for _, node := range stmt.QueryFeatures.limits {
			if node.Count != nil {
				if count, isParamMarker := node.Count.(*driver.ParamMarkerExpr); isParamMarker {
					typeExpected, val := CheckParamTypeInt64orUint64(count)
					if !typeExpected {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("unexpected value after LIMIT")
						break
					}
					if val > MaxCacheableLimitCount {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("limit count is too large")
						break
					}
					limitOffsetAndCount = append(limitOffsetAndCount, val)
				}
			}
			if node.Offset != nil {
				if offset, isParamMarker := node.Offset.(*driver.ParamMarkerExpr); isParamMarker {
					typeExpected, val := CheckParamTypeInt64orUint64(offset)
					if !typeExpected {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("unexpected value after LIMIT")
						break
					}
					limitOffsetAndCount = append(limitOffsetAndCount, val)
				}
			}
		}
	}

	return &utilpc.PlanCacheMatchOpts{
		LimitOffsetAndCount: limitOffsetAndCount,
		HasSubQuery:         stmt.QueryFeatures.hasSubquery,
		StatsVersionHash:    statsVerHash,
		ParamTypes:          parseParamTypes(sctx, params),
		ForeignKeyChecks:    sctx.GetSessionVars().ForeignKeyChecks,
	}
}

// CheckTypesCompatibility4PC compares FieldSlice with []*types.FieldType
// Currently this is only used in plan cache to check whether the types of parameters are compatible.
// If the types of parameters are compatible, we can use the cached plan.
// tpsExpected is types from cached plan
func checkTypesCompatibility4PC(tpsExpected, tpsActual []*types.FieldType) bool {
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
	paramTypes = make([]*types.FieldType, 0, len(params))
	for _, param := range params {
		if c, ok := param.(*expression.Constant); ok { // from binary protocol
			paramTypes = append(paramTypes, c.GetType(sctx.GetExprCtx().GetEvalCtx()))
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
