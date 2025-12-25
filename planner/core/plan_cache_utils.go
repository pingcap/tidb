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
	"context"
	"math"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/cmp"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/size"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)

	// ExtractSelectAndNormalizeDigest extract the select statement and normalize it.
	ExtractSelectAndNormalizeDigest func(stmtNode ast.StmtNode, specifiledDB string) (ast.StmtNode, string, string, error)
)

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (e *paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
		e.markers = append(e.markers, x)
	}
	return in, true
}

// GeneratePlanCacheStmtWithAST generates the PlanCacheStmt structure for this AST.
func GeneratePlanCacheStmtWithAST(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode, is infoschema.InfoSchema) (*PlanCacheStmt, Plan, int, error) {
	vars := sctx.GetSessionVars()
	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := stmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		return nil, nil, 0, ErrPrepareDDL
	}

	switch x := stmt.(type) {
	case *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, *ast.NonTransactionalDMLStmt:
		return nil, nil, 0, ErrUnsupportedPs
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil {
			return nil, nil, 0, ErrUnsupportedPs
		}
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return nil, nil, 0, ErrPsManyParam
	}

	ret := &PreprocessorReturn{}
	err := Preprocess(ctx, sctx, stmt, InPrepare, WithPreprocessorReturn(ret))
	if err != nil {
		return nil, nil, 0, err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	slices.SortFunc(extractor.markers, func(i, j ast.ParamMarkerExpr) int {
		return cmp.Compare(i.(*driver.ParamMarkerExpr).Offset, j.(*driver.ParamMarkerExpr).Offset)
	})
	ParamCount := len(extractor.markers)
	for i := 0; i < ParamCount; i++ {
		extractor.markers[i].SetOrder(i)
	}

	prepared := &ast.Prepared{
		Stmt:          stmt,
		StmtType:      ast.GetStmtLabel(stmt),
		Params:        extractor.markers,
		SchemaVersion: ret.InfoSchema.SchemaMetaVersion(),
	}
	normalizedSQL, digest := parser.NormalizeDigest(prepared.Stmt.Text())

	var (
		normalizedSQL4PC, digest4PC string
		selectStmtNode              ast.StmtNode
	)
	if !vars.EnablePreparedPlanCache {
		prepared.UseCache = false
	} else {
		cacheable, reason := CacheableWithCtx(sctx, stmt, ret.InfoSchema)
		prepared.UseCache = cacheable
		if !cacheable {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("skip plan-cache: " + reason))
		}
		selectStmtNode, normalizedSQL4PC, digest4PC, err = ExtractSelectAndNormalizeDigest(stmt, vars.CurrentDB)
		if err != nil || selectStmtNode == nil {
			normalizedSQL4PC = ""
			digest4PC = ""
		}
	}

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Datum.SetNull()
		param.InExecute = false
	}

	var p Plan
	destBuilder, _ := NewPlanBuilder().Init(sctx, ret.InfoSchema, &hint.BlockHintProcessor{})
	p, err = destBuilder.Build(ctx, stmt)
	if err != nil {
		return nil, nil, 0, err
	}

	// Collect information for metadata lock.
	dbName := make([]model.CIStr, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	tbls := make([]table.Table, 0, len(vars.StmtCtx.MDLRelatedTableIDs))
	relateVersion := make(map[int64]uint64, len(vars.StmtCtx.MDLRelatedTableIDs))
	for id, dbID := range vars.StmtCtx.MDLRelatedTableIDs {
		tbl, ok := is.TableByID(id)
		if !ok {
			logutil.BgLogger().Error("table not found in info schema", zap.Int64("tableID", id))
			return nil, nil, 0, errors.New("table not found in info schema")
		}
		db, ok := is.SchemaByID(dbID)
		if !ok {
			logutil.BgLogger().Error("database not found in info schema", zap.Int64("dbID", dbID))
			return nil, nil, 0, errors.New("database not found in info schema")
		}
		dbName = append(dbName, db.Name)
		tbls = append(tbls, tbl)
		relateVersion[id] = tbl.Meta().Revision
	}

	preparedObj := &PlanCacheStmt{
		PreparedAst:         prepared,
		StmtDB:              vars.CurrentDB,
		StmtText:            stmt.Text(),
		VisitInfos:          destBuilder.GetVisitInfo(),
		NormalizedSQL:       normalizedSQL,
		SQLDigest:           digest,
		ForUpdateRead:       destBuilder.GetIsForUpdateRead(),
		SnapshotTSEvaluator: ret.SnapshotTSEvaluator,
		NormalizedSQL4PC:    normalizedSQL4PC,
		SQLDigest4PC:        digest4PC,
		dbName:              dbName,
		tbls:                tbls,
		RelateVersion:       relateVersion,
	}
	if err = CheckPreparedPriv(sctx, preparedObj, ret.InfoSchema); err != nil {
		return nil, nil, 0, err
	}
	return preparedObj, p, ParamCount, nil
}

func getValidPlanFromCache(sctx sessionctx.Context, isGeneralPlanCache bool, key kvcache.Key, paramTypes []*types.FieldType) (*PlanCacheValue, bool) {
	cache := sctx.GetPlanCache(isGeneralPlanCache)
	val, exist := cache.Get(key, paramTypes)
	if !exist {
		return nil, exist
	}
	candidate := val.(*PlanCacheValue)
	return candidate, true
}

func putPlanIntoCache(sctx sessionctx.Context, isGeneralPlanCache bool, key kvcache.Key, plan *PlanCacheValue, paramTypes []*types.FieldType) {
	cache := sctx.GetPlanCache(isGeneralPlanCache)
	cache.Put(key, plan, paramTypes)
}

// planCacheKey is used to access Plan Cache. We put some variables that do not affect the plan into planCacheKey, such as the sql text.
// Put the parameters that may affect the plan in planCacheValue.
// However, due to some compatibility reasons, we will temporarily keep some system variable-related values in planCacheKey.
// At the same time, because these variables have a small impact on plan, we will move them to PlanCacheValue later if necessary.
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
	inRestrictedSQL          bool
	restrictedReadOnly       bool
	TiDBSuperReadOnly        bool

	// status related to Txn
	inTxn          bool
	autoCommit     bool
	pessAutoCommit bool

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
	if key == nil {
		return nil
	}
	if len(key.hash) == 0 {
		var (
			dbBytes    = hack.Slice(key.database)
			bufferSize = len(dbBytes) + 8*6 + 3*8
		)
		if key.hash == nil {
			key.hash = make([]byte, 0, bufferSize)
		}
		key.hash = append(key.hash, dbBytes...)
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
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.inRestrictedSQL))...)
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.restrictedReadOnly))...)
		key.hash = append(key.hash, hack.Slice(strconv.FormatBool(key.TiDBSuperReadOnly))...)
		key.hash = append(key.hash, bool2Byte(key.inTxn))
		key.hash = append(key.hash, bool2Byte(key.autoCommit))
		key.hash = append(key.hash, bool2Byte(key.pessAutoCommit))
	}
	return key.hash
}

func bool2Byte(flag bool) byte {
	if flag {
		return '1'
	}
	return '0'
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
	sum = emptyPlanCacheKeySize + int64(len(key.database)+len(key.stmtText)+len(key.bindSQL)) +
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
func NewPlanCacheKey(sessionVars *variable.SessionVars, stmtText, stmtDB string, schemaVersion int64,
	lastUpdatedSchemaVersion int64, bindSQL string, relatedSchemaVersion map[int64]uint64) (kvcache.Key, error) {
	if stmtText == "" {
		return nil, errors.New("no statement text")
	}
	if schemaVersion == 0 {
		return nil, errors.New("Schema version uninitialized")
	}
	if stmtDB == "" {
		stmtDB = sessionVars.CurrentDB
	}
	timezoneOffset := 0
	if sessionVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(sessionVars.TimeZone).Zone()
	}
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
		inRestrictedSQL:          sessionVars.InRestrictedSQL,
		restrictedReadOnly:       variable.RestrictedReadOnly.Load(),
		TiDBSuperReadOnly:        variable.VarTiDBSuperReadOnly.Load(),
		inTxn:                    sessionVars.InTxn(),
		autoCommit:               sessionVars.IsAutocommit(),
		pessAutoCommit:           config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load(),
	}
	for k, v := range sessionVars.IsolationReadEngines {
		key.isolationReadEngines[k] = v
	}
	for k, v := range relatedSchemaVersion {
		key.tblVersionMap[k] = v
	}
	return key, nil
}

// FieldSlice is the slice of the types.FieldType
type FieldSlice []*types.FieldType

// CheckTypesCompatibility4PC compares FieldSlice with []*types.FieldType
// Currently this is only used in plan cache to check whether the types of parameters are compatible.
// If the types of parameters are compatible, we can use the cached plan.
func (s FieldSlice) CheckTypesCompatibility4PC(tps []*types.FieldType) bool {
	if len(s) != len(tps) {
		return false
	}
	for i := range tps {
		// We only use part of logic of `func (ft *FieldType) Equal(other *FieldType)` here because (1) only numeric and
		// string types will show up here, and (2) we don't need flen and decimal to be matched exactly to use plan cache
		tpEqual := (s[i].GetType() == tps[i].GetType()) ||
			(s[i].GetType() == mysql.TypeVarchar && tps[i].GetType() == mysql.TypeVarString) ||
			(s[i].GetType() == mysql.TypeVarString && tps[i].GetType() == mysql.TypeVarchar)
		if !tpEqual || s[i].GetCharset() != tps[i].GetCharset() || s[i].GetCollate() != tps[i].GetCollate() ||
			(s[i].EvalType() == types.ETInt && mysql.HasUnsignedFlag(s[i].GetFlag()) != mysql.HasUnsignedFlag(tps[i].GetFlag())) {
			return false
		}
		// When the type is decimal, we should compare the Flen and Decimal.
		// We can only use the plan when both Flen and Decimal should less equal than the cached one.
		// We assume here that there is no correctness problem when the precision of the parameters is less than the precision of the parameters in the cache.
		if tpEqual && s[i].GetType() == mysql.TypeNewDecimal && !(s[i].GetFlen() >= tps[i].GetFlen() && s[i].GetDecimal() >= tps[i].GetDecimal()) {
			return false
		}
	}
	return true
}

// PlanCacheValue stores the cached Statement and StmtNode.
type PlanCacheValue struct {
	Plan              Plan
	OutPutNames       []*types.FieldName
	TblInfo2UnionScan map[*model.TableInfo]bool
	ParamTypes        FieldSlice
	memoryUsage       int64
	// stmtHints stores the hints which set session variables, because the hints won't be processed using cached plan.
	stmtHints *stmtctx.StmtHints
}

func (v *PlanCacheValue) varTypesUnchanged(txtVarTps []*types.FieldType) bool {
	return v.ParamTypes.CheckTypesCompatibility4PC(txtVarTps)
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
	case PhysicalPlan:
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

	sum += size.SizeOfInterface + size.SizeOfSlice*2 + int64(cap(v.OutPutNames)+cap(v.ParamTypes))*size.SizeOfPointer +
		size.SizeOfMap + int64(len(v.TblInfo2UnionScan))*(size.SizeOfPointer+size.SizeOfBool) + size.SizeOfInt64*2

	for _, name := range v.OutPutNames {
		sum += name.MemoryUsage()
	}
	for _, ft := range v.ParamTypes {
		sum += ft.MemoryUsage()
	}
	v.memoryUsage = sum
	return
}

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(plan Plan, names []*types.FieldName, srcMap map[*model.TableInfo]bool,
	paramTypes []*types.FieldType, stmtHints *stmtctx.StmtHints) *PlanCacheValue {
	dstMap := make(map[*model.TableInfo]bool)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	userParamTypes := make([]*types.FieldType, len(paramTypes))
	for i, tp := range paramTypes {
		userParamTypes[i] = tp.Clone()
	}
	return &PlanCacheValue{
		Plan:              plan,
		OutPutNames:       names,
		TblInfo2UnionScan: dstMap,
		ParamTypes:        userParamTypes,
		stmtHints:         stmtHints.Clone(),
	}
}

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
	StmtDB      string // which DB the statement will be processed over
	VisitInfos  []visitInfo
	ColumnInfos interface{}
	// Executor is only used for point get scene.
	// Notice that we should only cache the PointGetExecutor that have a snapshot with MaxTS in it.
	// If the current plan is not PointGet or does not use MaxTS optimization, this value should be nil here.
	Executor interface{}

	// RelateVersion stores the true cache plan table schema version, since each table schema can be updated separately in transaction.
	RelateVersion map[int64]uint64

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(context.Context, sessionctx.Context) (uint64, error)
	NormalizedSQL4PC    string
	SQLDigest4PC        string

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string

	// dbName and tbls are used to add metadata lock.
	dbName []model.CIStr
	tbls   []table.Table

	// the cache key for point-get statement, have to check whether the cache key changes before reusing this plan for safety.
	planCacheKey kvcache.Key
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
	return nil, ErrStmtNotFound
}

func isSafePointGetPath4PlanCache(sctx sessionctx.Context, path *util.AccessPath) bool {
	// PointGet might contain some over-optimized assumptions, like `a>=1 and a<=1` --> `a=1`, but
	// these assumptions may be broken after parameters change.

	if isSafePointGetPath4PlanCacheScenario1(path) {
		return true
	}

	// TODO: enable this fix control switch by default after more test cases are added.
	if sctx != nil && sctx.GetSessionVars() != nil && sctx.GetSessionVars().OptimizerFixControl != nil {
		v, ok := sctx.GetSessionVars().OptimizerFixControl[variable.TiDBOptFixControl44830]
		if ok && variable.TiDBOptOn(v) && (isSafePointGetPath4PlanCacheScenario2(path) || isSafePointGetPath4PlanCacheScenario3(path)) {
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
