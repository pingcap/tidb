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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	atomic2 "go.uber.org/atomic"
)

var (
	// preparedPlanCacheEnabledValue stores the global config "prepared-plan-cache-enabled".
	// The value is true unless "prepared-plan-cache-enabled" is FALSE in configuration.
	preparedPlanCacheEnabledValue int32 = 1
	// PreparedPlanCacheCapacity stores the global config "prepared-plan-cache-capacity".
	PreparedPlanCacheCapacity uint = 100
	// PreparedPlanCacheMemoryGuardRatio stores the global config "prepared-plan-cache-memory-guard-ratio".
	PreparedPlanCacheMemoryGuardRatio = 0.1
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

const (
	preparedPlanCacheEnabled = 1
	preparedPlanCacheUnable  = 0
)

// SetPreparedPlanCache sets isEnabled to true, then prepared plan cache is enabled.
func SetPreparedPlanCache(isEnabled bool) {
	if isEnabled {
		atomic.StoreInt32(&preparedPlanCacheEnabledValue, preparedPlanCacheEnabled)
	} else {
		atomic.StoreInt32(&preparedPlanCacheEnabledValue, preparedPlanCacheUnable)
	}
}

// PreparedPlanCacheEnabled returns whether the prepared plan cache is enabled.
func PreparedPlanCacheEnabled() bool {
	isEnabled := atomic.LoadInt32(&preparedPlanCacheEnabledValue)
	return isEnabled == preparedPlanCacheEnabled
}

type pstmtPlanCacheKey struct {
	database             string
	connID               uint64
	pstmtID              uint32
	snapshot             uint64
	schemaVersion        int64
	sqlMode              mysql.SQLMode
	timezoneOffset       int
	isolationReadEngines map[kv.StoreType]struct{}
	selectLimit          uint64

	hash []byte
}

// Hash implements Key interface.
func (key *pstmtPlanCacheKey) Hash() []byte {
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
		key.hash = codec.EncodeInt(key.hash, int64(key.pstmtID))
		key.hash = codec.EncodeInt(key.hash, int64(key.snapshot))
		key.hash = codec.EncodeInt(key.hash, key.schemaVersion)
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
	}
	return key.hash
}

// SetPstmtIDSchemaVersion implements PstmtCacheKeyMutator interface to change pstmtID and schemaVersion of cacheKey.
// so we can reuse Key instead of new every time.
func SetPstmtIDSchemaVersion(key kvcache.Key, pstmtID uint32, schemaVersion int64, isolationReadEngines map[kv.StoreType]struct{}) {
	psStmtKey, isPsStmtKey := key.(*pstmtPlanCacheKey)
	if !isPsStmtKey {
		return
	}
	psStmtKey.pstmtID = pstmtID
	psStmtKey.schemaVersion = schemaVersion
	psStmtKey.isolationReadEngines = make(map[kv.StoreType]struct{})
	for k, v := range isolationReadEngines {
		psStmtKey.isolationReadEngines[k] = v
	}
	psStmtKey.hash = psStmtKey.hash[:0]
}

// NewPSTMTPlanCacheKey creates a new pstmtPlanCacheKey object.
func NewPSTMTPlanCacheKey(sessionVars *variable.SessionVars, pstmtID uint32, schemaVersion int64) kvcache.Key {
	timezoneOffset := 0
	if sessionVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(sessionVars.TimeZone).Zone()
	}
	key := &pstmtPlanCacheKey{
		database:             sessionVars.CurrentDB,
		connID:               sessionVars.ConnectionID,
		pstmtID:              pstmtID,
		snapshot:             sessionVars.SnapshotTS,
		schemaVersion:        schemaVersion,
		sqlMode:              sessionVars.SQLMode,
		timezoneOffset:       timezoneOffset,
		isolationReadEngines: make(map[kv.StoreType]struct{}),
		selectLimit:          sessionVars.SelectLimit,
	}
	for k, v := range sessionVars.IsolationReadEngines {
		key.isolationReadEngines[k] = v
	}
	return key
}

// FieldSlice is the slice of the types.FieldType
type FieldSlice []types.FieldType

// Equal compares FieldSlice with []*types.FieldType
func (s FieldSlice) Equal(tps []*types.FieldType) bool {
	if len(s) != len(tps) {
		return false
	}
	for i := range tps {
		if !s[i].Equal(tps[i]) {
			return false
		}
	}
	return true
}

// PSTMTPlanCacheValue stores the cached Statement and StmtNode.
type PSTMTPlanCacheValue struct {
	Plan              Plan
	OutPutNames       []*types.FieldName
	TblInfo2UnionScan map[*model.TableInfo]bool
	UserVarTypes      FieldSlice
}

// NewPSTMTPlanCacheValue creates a SQLCacheValue.
func NewPSTMTPlanCacheValue(plan Plan, names []*types.FieldName, srcMap map[*model.TableInfo]bool, userVarTps []*types.FieldType) *PSTMTPlanCacheValue {
	dstMap := make(map[*model.TableInfo]bool)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	userVarTypes := make([]types.FieldType, len(userVarTps))
	for i, tp := range userVarTps {
		userVarTypes[i] = *tp
	}
	return &PSTMTPlanCacheValue{
		Plan:              plan,
		OutPutNames:       names,
		TblInfo2UnionScan: dstMap,
		UserVarTypes:      userVarTypes,
	}
}

// CachedPrepareStmt store prepared ast from PrepareExec and other related fields
type CachedPrepareStmt struct {
	PreparedAst    *ast.Prepared
	VisitInfos     []visitInfo
	ColumnInfos    interface{}
	Executor       interface{}
	NormalizedSQL  string
	NormalizedPlan string
	SQLDigest      string
	PlanDigest     string
}
