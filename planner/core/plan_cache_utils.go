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
	"strconv"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/size"
	atomic2 "go.uber.org/atomic"
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
// paramSQL is the corresponding parameterized sql like 'select * from t where a<? and b>?'.
// paramStmt is the Node of paramSQL.
func GeneratePlanCacheStmtWithAST(ctx context.Context, sctx sessionctx.Context, paramSQL string, paramStmt ast.StmtNode) (*PlanCacheStmt, Plan, int, error) {
	if v := ctx.Value("____GeneratePlanCacheStmtWithASTErr"); v != nil { // for testing
		return nil, nil, 0, errors.New("____GeneratePlanCacheStmtWithASTErr")
	}

	vars := sctx.GetSessionVars()
	var extractor paramMarkerExtractor
	paramStmt.Accept(&extractor)

	// DDL Statements can not accept parameters
	if _, ok := paramStmt.(ast.DDLNode); ok && len(extractor.markers) > 0 {
		return nil, nil, 0, ErrPrepareDDL
	}

	switch paramStmt.(type) {
	case *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, *ast.NonTransactionalDMLStmt:
		return nil, nil, 0, ErrUnsupportedPs
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return nil, nil, 0, ErrPsManyParam
	}

	ret := &PreprocessorReturn{}
	err := Preprocess(ctx, sctx, paramStmt, InPrepare, WithPreprocessorReturn(ret))
	if err != nil {
		return nil, nil, 0, err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	slices.SortFunc(extractor.markers, func(i, j ast.ParamMarkerExpr) bool {
		return i.(*driver.ParamMarkerExpr).Offset < j.(*driver.ParamMarkerExpr).Offset
	})
	ParamCount := len(extractor.markers)
	for i := 0; i < ParamCount; i++ {
		extractor.markers[i].SetOrder(i)
	}

	prepared := &ast.Prepared{
		Stmt:          paramStmt,
		StmtType:      ast.GetStmtLabel(paramStmt),
		Params:        extractor.markers,
		SchemaVersion: ret.InfoSchema.SchemaMetaVersion(),
	}
	normalizedSQL, digest := parser.NormalizeDigest(prepared.Stmt.Text())

	var (
		normalizedSQL4PC, digest4PC string
		selectStmtNode              ast.StmtNode
		cacheable                   bool
		reason                      string
	)
	if !vars.EnablePreparedPlanCache {
		cacheable = false
		reason = "plan cache is disabled"
	} else {
		cacheable, reason = CacheableWithCtx(sctx, paramStmt, ret.InfoSchema)
		if !cacheable {
			sctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("skip plan-cache: " + reason))
		}
		selectStmtNode, normalizedSQL4PC, digest4PC, err = ExtractSelectAndNormalizeDigest(paramStmt, vars.CurrentDB)
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
	p, err = destBuilder.Build(ctx, paramStmt)
	if err != nil {
		return nil, nil, 0, err
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
		NormalizedSQL4PC:    normalizedSQL4PC,
		SQLDigest4PC:        digest4PC,
		StmtCacheable:       cacheable,
		UncacheableReason:   reason,
	}
	if err = CheckPreparedPriv(sctx, preparedObj, ret.InfoSchema); err != nil {
		return nil, nil, 0, err
	}
	return preparedObj, p, ParamCount, nil
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

	memoryUsage int64 // Do not include in hash
	hash        []byte
}

// Hash implements Key interface.
func (key *planCacheKey) Hash() []byte {
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
	sum = emptyPlanCacheKeySize + int64(len(key.database)+len(key.stmtText)+len(key.bindSQL)) +
		int64(len(key.isolationReadEngines))*size.SizeOfUint8 + int64(cap(key.hash))
	key.memoryUsage = sum
	return
}

type planCacheMatchOpts struct {
	// paramTypes stores all parameters' FieldType, some different parameters may share same plan
	paramTypes FieldSlice
	// limitOffsetAndCount stores all the offset and key parameters extract from limit statement
	// only used for cache and pick plan with parameters in limit
	limitOffsetAndCount []uint64
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
	lastUpdatedSchemaVersion int64, bindSQL string) (kvcache.Key, error) {
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
		lastUpdatedSchemaVersion: lastUpdatedSchemaVersion,
		sqlMode:                  sessionVars.SQLMode,
		timezoneOffset:           timezoneOffset,
		isolationReadEngines:     make(map[kv.StoreType]struct{}),
		selectLimit:              sessionVars.SelectLimit,
		bindSQL:                  bindSQL,
		inRestrictedSQL:          sessionVars.InRestrictedSQL,
		restrictedReadOnly:       variable.RestrictedReadOnly.Load(),
		TiDBSuperReadOnly:        variable.VarTiDBSuperReadOnly.Load(),
	}
	for k, v := range sessionVars.IsolationReadEngines {
		key.isolationReadEngines[k] = v
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
	memoryUsage       int64

	// matchOpts stores some fields help to choose a suitable plan
	matchOpts planCacheMatchOpts
}

func (v *PlanCacheValue) varTypesUnchanged(txtVarTps []*types.FieldType) bool {
	return v.matchOpts.paramTypes.CheckTypesCompatibility4PC(txtVarTps)
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

	sum += size.SizeOfInterface + size.SizeOfSlice*2 + int64(cap(v.OutPutNames)+cap(v.matchOpts.paramTypes))*size.SizeOfPointer +
		size.SizeOfMap + int64(len(v.TblInfo2UnionScan))*(size.SizeOfPointer+size.SizeOfBool) + size.SizeOfInt64*2

	for _, name := range v.OutPutNames {
		sum += name.MemoryUsage()
	}
	for _, ft := range v.matchOpts.paramTypes {
		sum += ft.MemoryUsage()
	}
	v.memoryUsage = sum
	return
}

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(plan Plan, names []*types.FieldName, srcMap map[*model.TableInfo]bool,
	paramTypes []*types.FieldType, limitParams []uint64) *PlanCacheValue {
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
		matchOpts: planCacheMatchOpts{
			paramTypes:          userParamTypes,
			limitOffsetAndCount: limitParams,
		},
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

	StmtCacheable     bool   // Whether this stmt is cacheable.
	UncacheableReason string // Why this stmt is uncacheable.

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(sessionctx.Context) (uint64, error)
	NormalizedSQL4PC    string
	SQLDigest4PC        string

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string
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

type limitExtractor struct {
	cacheable         bool // For safety considerations, check if limit count less than 10000
	offsetAndCount    []uint64
	unCacheableReason string
	paramTypeErr      error
}

// Enter implements Visitor interface.
func (checker *limitExtractor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.Limit:
		if node.Count != nil {
			if count, isParamMarker := node.Count.(*driver.ParamMarkerExpr); isParamMarker {
				typeExpected, val := CheckParamTypeInt64orUint64(count)
				if typeExpected {
					if val > 10000 {
						checker.cacheable = false
						checker.unCacheableReason = "limit count more than 10000"
						return in, true
					}
					checker.offsetAndCount = append(checker.offsetAndCount, val)
				} else {
					checker.paramTypeErr = ErrWrongArguments.GenWithStackByArgs("LIMIT")
					return in, true
				}
			}
		}
		if node.Offset != nil {
			if offset, isParamMarker := node.Offset.(*driver.ParamMarkerExpr); isParamMarker {
				typeExpected, val := CheckParamTypeInt64orUint64(offset)
				if typeExpected {
					checker.offsetAndCount = append(checker.offsetAndCount, val)
				} else {
					checker.paramTypeErr = ErrWrongArguments.GenWithStackByArgs("LIMIT")
					return in, true
				}
			}
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *limitExtractor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}

// ExtractLimitFromAst extract limit offset and count from ast for plan cache key encode
func ExtractLimitFromAst(node ast.Node, sctx sessionctx.Context) ([]uint64, error) {
	if node == nil {
		return nil, nil
	}
	checker := limitExtractor{
		cacheable:      true,
		offsetAndCount: []uint64{},
	}
	node.Accept(&checker)
	if checker.paramTypeErr != nil {
		return nil, checker.paramTypeErr
	}
	if sctx != nil && !checker.cacheable {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.New("skip plan-cache: " + checker.unCacheableReason))
	}
	return checker.offsetAndCount, nil
}
