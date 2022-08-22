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
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/stringutil"
	atomic2 "go.uber.org/atomic"
)

var (
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

func getValidPlanFromCache(sctx sessionctx.Context, isGeneralPlanCache bool, key kvcache.Key, paramTypes []*types.FieldType) (*PlanCacheValue, bool) {
	cache := sctx.GetPlanCache(isGeneralPlanCache)
	val, exist := cache.Get(key)
	if !exist {
		return nil, exist
	}
	candidates := val.([]*PlanCacheValue)
	for _, candidate := range candidates {
		if candidate.varTypesUnchanged(paramTypes) {
			return candidate, true
		}
	}
	return nil, false
}

func putPlanIntoCache(sctx sessionctx.Context, isGeneralPlanCache bool, key kvcache.Key, plan *PlanCacheValue) {
	cache := sctx.GetPlanCache(isGeneralPlanCache)
	val, exist := cache.Get(key)
	if !exist {
		cache.Put(key, []*PlanCacheValue{plan})
		return
	}
	candidates := val.([]*PlanCacheValue)
	for i, candidate := range candidates {
		if candidate.varTypesUnchanged(plan.ParamTypes) {
			// hit an existing cached plan
			candidates[i] = plan
			return
		}
	}
	// add to current candidate list
	// TODO: limit the candidate list length
	candidates = append(candidates, plan)
	cache.Put(key, candidates)
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

	hash []byte
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
			(s[i].GetType() == mysql.TypeVarString && tps[i].GetType() == mysql.TypeVarchar) ||
			// TypeNull should be considered the same as other types.
			(s[i].GetType() == mysql.TypeNull || tps[i].GetType() == mysql.TypeNull)
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
}

func (v *PlanCacheValue) varTypesUnchanged(txtVarTps []*types.FieldType) bool {
	return v.ParamTypes.CheckTypesCompatibility4PC(txtVarTps)
}

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(plan Plan, names []*types.FieldName, srcMap map[*model.TableInfo]bool,
	paramTypes []*types.FieldType) *PlanCacheValue {
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
	}
}

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst         *ast.Prepared
	StmtDB              string // which DB the statement will be processed over
	VisitInfos          []visitInfo
	ColumnInfos         interface{}
	Executor            interface{}
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

// Parameterizer used to parameterize a general statement.
// e.g. 'select * from t where a>23' --> 'select * from t where a>?' + 23
type Parameterizer interface {
	// Parameterize this specific sql, ok indicates whether this sql is supported.
	Parameterize(originSQL string) (paramSQL string, params []expression.Expression, ok bool, err error)
}

// ParameterizerKey is used to get a parameterizer from a ctx, only for test.
const ParameterizerKey = stringutil.StringerStr("parameterizerKey")

// Parameterize parameterizes this sql, used by general plan cache.
func Parameterize(sctx sessionctx.Context, originSQL string) (paramSQL string, params []expression.Expression, ok bool, err error) {
	if v := sctx.Value(ParameterizerKey); v != nil { // for test
		return v.(Parameterizer).Parameterize(originSQL)
	}
	// TODO: implement it
	return "", nil, false, nil
}
