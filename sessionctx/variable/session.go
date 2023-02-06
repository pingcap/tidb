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

package variable

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	ptypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx/sessionstates"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/replayer"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tableutil"
	"github.com/pingcap/tidb/util/timeutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/twmb/murmur3"
	atomic2 "go.uber.org/atomic"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// DefaultResourceGroupName is the default resource group name.
const DefaultResourceGroupName = "default"

var (
	// PreparedStmtCount is exported for test.
	PreparedStmtCount int64
	// enableAdaptiveReplicaRead indicates whether closest adaptive replica read
	// can be enabled. We forces disable replica read when tidb server in missing
	// in regions that contains tikv server to avoid read traffic skew.
	enableAdaptiveReplicaRead uint32 = 1
)

// SetEnableAdaptiveReplicaRead set `enableAdaptiveReplicaRead` with given value.
// return true if the value is changed.
func SetEnableAdaptiveReplicaRead(enabled bool) bool {
	value := uint32(0)
	if enabled {
		value = 1
	}
	return atomic.SwapUint32(&enableAdaptiveReplicaRead, value) != value
}

// IsAdaptiveReplicaReadEnabled returns whether adaptive closest replica read can be enabled.
func IsAdaptiveReplicaReadEnabled() bool {
	return atomic.LoadUint32(&enableAdaptiveReplicaRead) > 0
}

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
	LastRcReadTS           uint64
}

// ReuseChunkPool save Alloc object
type ReuseChunkPool struct {
	mu    sync.Mutex
	Alloc chunk.Allocator
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.autoIncrementIDs.clean()
	r.autoRandomIDs.clean()

	if len(r.DroppedPreparedStmtIDs) > 0 {
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.autoIncrementIDs.resetOffset()
	r.autoRandomIDs.resetOffset()
}

// AddAutoIncrementID adds id to autoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs.autoIDs = append(r.autoIncrementIDs.autoIDs, id)
}

// GetCurrAutoIncrementID gets current autoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, bool) {
	return r.autoIncrementIDs.getCurrent()
}

// AddAutoRandomID adds id to autoRandomIDs.
func (r *RetryInfo) AddAutoRandomID(id int64) {
	r.autoRandomIDs.autoIDs = append(r.autoRandomIDs.autoIDs, id)
}

// GetCurrAutoRandomID gets current AutoRandomID.
func (r *RetryInfo) GetCurrAutoRandomID() (int64, bool) {
	return r.autoRandomIDs.getCurrent()
}

type retryInfoAutoIDs struct {
	currentOffset int
	autoIDs       []int64
}

func (r *retryInfoAutoIDs) resetOffset() {
	r.currentOffset = 0
}

func (r *retryInfoAutoIDs) clean() {
	r.currentOffset = 0
	if len(r.autoIDs) > 0 {
		r.autoIDs = r.autoIDs[:0]
	}
}

func (r *retryInfoAutoIDs) getCurrent() (int64, bool) {
	if r.currentOffset >= len(r.autoIDs) {
		return 0, false
	}
	id := r.autoIDs[r.currentOffset]
	r.currentOffset++
	return id, true
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	TxnCtxNoNeedToRestore
	TxnCtxNeedToRestore
}

// TxnCtxNeedToRestore stores transaction variables which need to be restored when rolling back to a savepoint.
type TxnCtxNeedToRestore struct {
	// TableDeltaMap is used in the schema validator for DDL changes in one table not to block others.
	// It's also used in the statistics updating.
	// Note: for the partitioned table, it stores all the partition IDs.
	TableDeltaMap map[int64]TableDelta

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte

	// CachedTables is not nil if the transaction write on cached table.
	CachedTables map[int64]interface{}
}

// TxnCtxNoNeedToRestore stores transaction variables which do not need to restored when rolling back to a savepoint.
type TxnCtxNoNeedToRestore struct {
	forUpdateTS uint64
	Binlog      interface{}
	InfoSchema  interface{}
	History     interface{}
	StartTS     uint64

	// ShardStep indicates the max size of continuous rowid shard in one transaction.
	ShardStep    int
	shardRemain  int
	currentShard int64

	// unchangedRowKeys is used to store the unchanged rows that needs to lock for pessimistic transaction.
	unchangedRowKeys map[string]struct{}

	PessimisticCacheHit int

	// CreateTime For metrics.
	CreateTime     time.Time
	StatementCount int
	CouldRetry     bool
	IsPessimistic  bool
	// IsStaleness indicates whether the txn is read only staleness txn.
	IsStaleness bool
	// IsExplicit indicates whether the txn is an interactive txn, which is typically started with a BEGIN
	// or START TRANSACTION statement, or by setting autocommit to 0.
	IsExplicit bool
	Isolation  string
	LockExpire uint32
	ForUpdate  uint32
	// TxnScope indicates the value of txn_scope
	TxnScope string

	// Savepoints contains all definitions of the savepoint of a transaction at runtime, the order of the SavepointRecord is the same with the SAVEPOINT statements.
	// It is used for a lookup when running `ROLLBACK TO` statement.
	Savepoints []SavepointRecord

	// TableDeltaMap lock to prevent potential data race
	tdmLock sync.Mutex

	// TemporaryTables is used to store transaction-specific information for global temporary tables.
	// It can also be stored in sessionCtx with local temporary tables, but it's easier to clean this data after transaction ends.
	TemporaryTables map[int64]tableutil.TempTable
	// EnableMDL indicates whether to enable the MDL lock for the transaction.
	EnableMDL bool
	// relatedTableForMDL records the `lock` table for metadata lock. It maps from int64 to int64(version).
	relatedTableForMDL *sync.Map
}

// SavepointRecord indicates a transaction's savepoint record.
type SavepointRecord struct {
	// name is the name of the savepoint
	Name string
	// MemDBCheckpoint is the transaction's memdb checkpoint.
	MemDBCheckpoint *tikv.MemDBCheckpoint
	// TxnCtxSavepoint is the savepoint of TransactionContext
	TxnCtxSavepoint TxnCtxNeedToRestore
}

// GetCurrentShard returns the shard for the next `count` IDs.
func (s *SessionVars) GetCurrentShard(count int) int64 {
	tc := s.TxnCtx
	if s.shardRand == nil {
		s.shardRand = rand.New(rand.NewSource(int64(tc.StartTS))) // #nosec G404
	}
	if tc.shardRemain <= 0 {
		tc.updateShard(s.shardRand)
		tc.shardRemain = tc.ShardStep
	}
	tc.shardRemain -= count
	return tc.currentShard
}

func (tc *TransactionContext) updateShard(shardRand *rand.Rand) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], shardRand.Uint64())
	tc.currentShard = int64(murmur3.Sum32(buf[:]))
}

// AddUnchangedRowKey adds an unchanged row key in update statement for pessimistic lock.
func (tc *TransactionContext) AddUnchangedRowKey(key []byte) {
	if tc.unchangedRowKeys == nil {
		tc.unchangedRowKeys = map[string]struct{}{}
	}
	tc.unchangedRowKeys[string(key)] = struct{}{}
}

// CollectUnchangedRowKeys collects unchanged row keys for pessimistic lock.
func (tc *TransactionContext) CollectUnchangedRowKeys(buf []kv.Key) []kv.Key {
	for key := range tc.unchangedRowKeys {
		buf = append(buf, kv.Key(key))
	}
	tc.unchangedRowKeys = nil
	return buf
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(physicalTableID int64, delta int64, count int64, colSize map[int64]int64) {
	tc.tdmLock.Lock()
	defer tc.tdmLock.Unlock()
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[physicalTableID]
	if item.ColSize == nil && colSize != nil {
		item.ColSize = make(map[int64]int64, len(colSize))
	}
	item.Delta += delta
	item.Count += count
	item.TableID = physicalTableID
	for key, val := range colSize {
		item.ColSize[key] += val
	}
	tc.TableDeltaMap[physicalTableID] = item
}

// GetKeyInPessimisticLockCache gets a key in pessimistic lock cache.
func (tc *TransactionContext) GetKeyInPessimisticLockCache(key kv.Key) (val []byte, ok bool) {
	if tc.pessimisticLockCache == nil {
		return nil, false
	}
	val, ok = tc.pessimisticLockCache[string(key)]
	if ok {
		tc.PessimisticCacheHit++
	}
	return
}

// SetPessimisticLockCache sets a key value pair into pessimistic lock cache.
func (tc *TransactionContext) SetPessimisticLockCache(key kv.Key, val []byte) {
	if tc.pessimisticLockCache == nil {
		tc.pessimisticLockCache = map[string][]byte{}
	}
	tc.pessimisticLockCache[string(key)] = val
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	// tc.InfoSchema = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.Binlog = nil
	tc.History = nil
	tc.tdmLock.Lock()
	tc.TableDeltaMap = nil
	tc.relatedTableForMDL = nil
	tc.tdmLock.Unlock()
	tc.pessimisticLockCache = nil
	tc.IsStaleness = false
	tc.Savepoints = nil
	tc.EnableMDL = false
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.tdmLock.Lock()
	tc.TableDeltaMap = nil
	tc.tdmLock.Unlock()
}

// GetForUpdateTS returns the ts for update.
func (tc *TransactionContext) GetForUpdateTS() uint64 {
	if tc.forUpdateTS > tc.StartTS {
		return tc.forUpdateTS
	}
	return tc.StartTS
}

// SetForUpdateTS sets the ts for update.
func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
	if forUpdateTS > tc.forUpdateTS {
		tc.forUpdateTS = forUpdateTS
	}
}

// GetCurrentSavepoint gets TransactionContext's savepoint.
func (tc *TransactionContext) GetCurrentSavepoint() TxnCtxNeedToRestore {
	tableDeltaMap := make(map[int64]TableDelta, len(tc.TableDeltaMap))
	for k, v := range tc.TableDeltaMap {
		tableDeltaMap[k] = v.Clone()
	}
	pessimisticLockCache := make(map[string][]byte, len(tc.pessimisticLockCache))
	maps.Copy(pessimisticLockCache, tc.pessimisticLockCache)
	cachedTables := make(map[int64]interface{}, len(tc.CachedTables))
	maps.Copy(cachedTables, tc.CachedTables)
	return TxnCtxNeedToRestore{
		TableDeltaMap:        tableDeltaMap,
		pessimisticLockCache: pessimisticLockCache,
		CachedTables:         cachedTables,
	}
}

// RestoreBySavepoint restores TransactionContext to the specify savepoint.
func (tc *TransactionContext) RestoreBySavepoint(savepoint TxnCtxNeedToRestore) {
	tc.TableDeltaMap = savepoint.TableDeltaMap
	tc.pessimisticLockCache = savepoint.pessimisticLockCache
	tc.CachedTables = savepoint.CachedTables
}

// AddSavepoint adds a new savepoint.
func (tc *TransactionContext) AddSavepoint(name string, memdbCheckpoint *tikv.MemDBCheckpoint) {
	name = strings.ToLower(name)
	tc.DeleteSavepoint(name)

	record := SavepointRecord{
		Name:            name,
		MemDBCheckpoint: memdbCheckpoint,
		TxnCtxSavepoint: tc.GetCurrentSavepoint(),
	}
	tc.Savepoints = append(tc.Savepoints, record)
}

// DeleteSavepoint deletes the savepoint, return false indicate the savepoint name doesn't exists.
func (tc *TransactionContext) DeleteSavepoint(name string) bool {
	name = strings.ToLower(name)
	for i, sp := range tc.Savepoints {
		if sp.Name == name {
			tc.Savepoints = append(tc.Savepoints[:i], tc.Savepoints[i+1:]...)
			return true
		}
	}
	return false
}

// ReleaseSavepoint deletes the named savepoint and the later savepoints, return false indicate the named savepoint doesn't exists.
func (tc *TransactionContext) ReleaseSavepoint(name string) bool {
	name = strings.ToLower(name)
	for i, sp := range tc.Savepoints {
		if sp.Name == name {
			tc.Savepoints = append(tc.Savepoints[:i])
			return true
		}
	}
	return false
}

// RollbackToSavepoint rollbacks to the specified savepoint by name.
func (tc *TransactionContext) RollbackToSavepoint(name string) *SavepointRecord {
	name = strings.ToLower(name)
	for idx, sp := range tc.Savepoints {
		if name == sp.Name {
			tc.RestoreBySavepoint(sp.TxnCtxSavepoint)
			tc.Savepoints = tc.Savepoints[:idx+1]
			return &tc.Savepoints[idx]
		}
	}
	return nil
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// TableSnapshot represents a data snapshot of the table contained in `information_schema`.
type TableSnapshot struct {
	Rows [][]types.Datum
	Err  error
}

type txnIsolationLevelOneShotState uint

// RewritePhaseInfo records some information about the rewrite phase
type RewritePhaseInfo struct {
	// DurationRewrite is the duration of rewriting the SQL.
	DurationRewrite time.Duration

	// DurationPreprocessSubQuery is the duration of pre-processing sub-queries.
	DurationPreprocessSubQuery time.Duration

	// PreprocessSubQueries is the number of pre-processed sub-queries.
	PreprocessSubQueries int
}

// Reset resets all fields in RewritePhaseInfo.
func (r *RewritePhaseInfo) Reset() {
	r.DurationRewrite = 0
	r.DurationPreprocessSubQuery = 0
	r.PreprocessSubQueries = 0
}

// TemporaryTableData is a interface to maintain temporary data in session
type TemporaryTableData interface {
	kv.Retriever
	// Staging create a new staging buffer inside the MemBuffer.
	// Subsequent writes will be temporarily stored in this new staging buffer.
	// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
	Staging() kv.StagingHandle
	// Release publish all modifications in the latest staging buffer to upper level.
	Release(kv.StagingHandle)
	// Cleanup cleanups the resources referenced by the StagingHandle.
	// If the changes are not published by `Release`, they will be discarded.
	Cleanup(kv.StagingHandle)
	// GetTableSize get the size of a table
	GetTableSize(tblID int64) int64
	// DeleteTableKey removes the entry for key k from table
	DeleteTableKey(tblID int64, k kv.Key) error
	// SetTableKey sets the entry for k from table
	SetTableKey(tblID int64, k kv.Key, val []byte) error
}

// temporaryTableData is used for store temporary table data in session
type temporaryTableData struct {
	kv.MemBuffer
	tblSize map[int64]int64
}

// NewTemporaryTableData creates a new TemporaryTableData
func NewTemporaryTableData(memBuffer kv.MemBuffer) TemporaryTableData {
	return &temporaryTableData{
		MemBuffer: memBuffer,
		tblSize:   make(map[int64]int64),
	}
}

// GetTableSize get the size of a table
func (d *temporaryTableData) GetTableSize(tblID int64) int64 {
	if tblSize, ok := d.tblSize[tblID]; ok {
		return tblSize
	}
	return 0
}

// DeleteTableKey removes the entry for key k from table
func (d *temporaryTableData) DeleteTableKey(tblID int64, k kv.Key) error {
	bufferSize := d.MemBuffer.Size()
	defer d.updateTblSize(tblID, bufferSize)

	return d.MemBuffer.Delete(k)
}

// SetTableKey sets the entry for k from table
func (d *temporaryTableData) SetTableKey(tblID int64, k kv.Key, val []byte) error {
	bufferSize := d.MemBuffer.Size()
	defer d.updateTblSize(tblID, bufferSize)

	return d.MemBuffer.Set(k, val)
}

func (d *temporaryTableData) updateTblSize(tblID int64, beforeSize int) {
	delta := int64(d.MemBuffer.Size() - beforeSize)
	d.tblSize[tblID] = d.GetTableSize(tblID) + delta
}

const (
	// oneShotDef means default, that is tx_isolation_one_shot not set.
	oneShotDef txnIsolationLevelOneShotState = iota
	// oneShotSet means it's set in current transaction.
	oneShotSet
	// onsShotUse means it should be used in current transaction.
	oneShotUse
)

// ReadConsistencyLevel is the level of read consistency.
type ReadConsistencyLevel string

const (
	// ReadConsistencyStrict means read by strict consistency, default value.
	ReadConsistencyStrict ReadConsistencyLevel = "strict"
	// ReadConsistencyWeak means read can be weak consistency.
	ReadConsistencyWeak ReadConsistencyLevel = "weak"
)

// IsWeak returns true only if it's a weak-consistency read.
func (r ReadConsistencyLevel) IsWeak() bool {
	return r == ReadConsistencyWeak
}

func validateReadConsistencyLevel(val string) error {
	switch v := ReadConsistencyLevel(strings.ToLower(val)); v {
	case ReadConsistencyStrict, ReadConsistencyWeak:
		return nil
	default:
		return ErrWrongTypeForVar.GenWithStackByArgs(TiDBReadConsistency)
	}
}

// SetUserVarVal set user defined variables' value
func (s *SessionVars) SetUserVarVal(name string, dt types.Datum) {
	s.userVars.lock.Lock()
	defer s.userVars.lock.Unlock()
	s.userVars.values[name] = dt
}

// GetUserVarVal get user defined variables' value
func (s *SessionVars) GetUserVarVal(name string) (types.Datum, bool) {
	s.userVars.lock.RLock()
	defer s.userVars.lock.RUnlock()
	dt, ok := s.userVars.values[name]
	return dt, ok
}

// SetUserVarType set user defined variables' type
func (s *SessionVars) SetUserVarType(name string, ft *types.FieldType) {
	s.userVars.lock.Lock()
	defer s.userVars.lock.Unlock()
	s.userVars.types[name] = ft
}

// GetUserVarType get user defined variables' type
func (s *SessionVars) GetUserVarType(name string) (*types.FieldType, bool) {
	s.userVars.lock.RLock()
	defer s.userVars.lock.RUnlock()
	ft, ok := s.userVars.types[name]
	return ft, ok
}

// HookContext contains the necessary variables for executing set/get hook
type HookContext interface {
	GetStore() kv.Storage
}

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency
	MemQuota
	BatchSize
	// DMLBatchSize indicates the number of rows batch-committed for a statement.
	// It will be used when using LOAD DATA or BatchInsert or BatchDelete is on.
	DMLBatchSize        int
	RetryLimit          int64
	DisableTxnAutoRetry bool
	userVars            struct {
		// lock is for user defined variables. values and types is read/write protected.
		lock sync.RWMutex
		// values stores the Datum for user variables
		values map[string]types.Datum
		// types stores the FieldType for user variables, it cannot be inferred from values when values have not been set yet.
		types map[string]*types.FieldType
	}
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// stmtVars variables are temporarily set by SET_VAR hint
	// It only take effect for the duration of a single statement
	stmtVars map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16
	// nonPreparedPlanCacheStmts stores PlanCacheStmts for non-prepared plan cache.
	nonPreparedPlanCacheStmts *kvcache.SimpleLRUCache
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// PreparedParams params for prepared statements
	PreparedParams    PreparedParams
	LastUpdateTime4PC types.Time

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	RetryInfo *RetryInfo
	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext
	// TxnCtxMu is used to protect TxnCtx.
	TxnCtxMu sync.Mutex

	// TxnManager is used to manage txn context in session
	TxnManager interface{}

	// KVVars is the variables for KV storage.
	KVVars *tikvstore.Variables

	// txnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	txnIsolationLevelOneShot struct {
		state txnIsolationLevelOneShotState
		value string
	}

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// MapHashCode2UniqueID4ExtendedCol map the expr's hash code to specified unique ID.
	MapHashCode2UniqueID4ExtendedCol map[string]int

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// Port is the port of the connected socket
	Port string

	// CurrentDB is the default database of this session.
	CurrentDB string

	// CurrentDBChanged indicates if the CurrentDB has been updated, and if it is we should print it into
	// the slow log to make it be compatible with MySQL, https://github.com/pingcap/tidb/issues/17846.
	CurrentDBChanged bool

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

	// TxnReadTS is used for staleness transaction, it provides next staleness transaction startTS.
	TxnReadTS *TxnReadTS

	// SnapshotInfoschema is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	SnapshotInfoschema interface{}

	// BinlogClient is used to write binlog.
	BinlogClient *pumpcli.PumpsClient

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// RefCountOfStmtCtx indicates the reference count of StmtCtx. When the
	// StmtCtx is accessed by other sessions, e.g. oom-alarm-handler/expensive-query-handler, add one first.
	// Note: this variable should be accessed and updated by atomic operations.
	RefCountOfStmtCtx stmtctx.ReferenceCount

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowCartesianBCJ means allow broadcast CARTESIAN join, 0 means not allow, 1 means allow broadcast CARTESIAN join
	// but the table size should under the broadcast threshold, 2 means allow broadcast CARTESIAN join even if the table
	// size exceeds the broadcast threshold
	AllowCartesianBCJ int

	// MPPOuterJoinFixedBuildSide means in MPP plan, always use right(left) table as build side for left(right) out join
	MPPOuterJoinFixedBuildSide bool

	// AllowDistinctAggPushDown can be set true to allow agg with distinct push down to tikv/tiflash.
	AllowDistinctAggPushDown bool

	// EnableSkewDistinctAgg can be set true to allow skew distinct aggregate rewrite
	EnableSkewDistinctAgg bool

	// Enable3StageDistinctAgg indicates whether to allow 3 stage distinct aggregate
	Enable3StageDistinctAgg bool

	// MultiStatementMode permits incorrect client library usage. Not recommended to be turned on.
	MultiStatementMode int

	// AllowWriteRowID variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowBatchCop means if we should send batch coprocessor to TiFlash. Default value is 1, means to use batch cop in case of aggregation and join.
	// Value set to 2 means to force to send batch cop for any query. Value set to 0 means never use batch cop.
	AllowBatchCop int

	// allowMPPExecution means if we should use mpp way to execute query.
	// Default value is `true`, means to be determined by the optimizer.
	// Value set to `false` means never use mpp.
	allowMPPExecution bool

	// HashExchangeWithNewCollation means if we support hash exchange when new collation is enabled.
	// Default value is `true`, means support hash exchange when new collation is enabled.
	// Value set to `false` means not use hash exchange when new collation is enabled.
	HashExchangeWithNewCollation bool

	// enforceMPPExecution means if we should enforce mpp way to execute query.
	// Default value is `false`, means to be determined by variable `allowMPPExecution`.
	// Value set to `true` means enforce use mpp.
	// Note if you want to set `enforceMPPExecution` to `true`, you must set `allowMPPExecution` to `true` first.
	enforceMPPExecution bool

	// TiFlashMaxThreads is the maximum number of threads to execute the request which is pushed down to tiflash.
	// Default value is -1, means it will not be pushed down to tiflash.
	// If the value is bigger than -1, it will be pushed down to tiflash and used to create db context in tiflash.
	TiFlashMaxThreads int64

	// TiDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
	AllowAutoRandExplicitInsert bool

	// BroadcastJoinThresholdSize is used to limit the size of smaller table.
	// It's unit is bytes, if the size of small table is larger than it, we will not use bcj.
	BroadcastJoinThresholdSize int64

	// BroadcastJoinThresholdCount is used to limit the total count of smaller table.
	// If we can't estimate the size of one side of join child, we will check if its row number exceeds this limitation.
	BroadcastJoinThresholdCount int64

	// LimitPushDownThreshold determines if push Limit or TopN down to TiKV forcibly.
	LimitPushDownThreshold int64

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// EnableCorrelationAdjustment is used to indicate if correlation adjustment is enabled.
	EnableCorrelationAdjustment bool

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// cpuFactor is the CPU cost of processing one expression for one row.
	cpuFactor float64
	// copCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	copCPUFactor float64
	// networkFactor is the network cost of transferring 1 byte data.
	networkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash.
	scanFactor float64
	// descScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash in desc order.
	descScanFactor float64
	// seekFactor is the IO cost of seeking the start value of a range in TiKV or TiFlash.
	seekFactor float64
	// memoryFactor is the memory cost of storing one tuple.
	memoryFactor float64
	// diskFactor is the IO cost of reading/writing one byte to temporary disk.
	diskFactor float64
	// concurrencyFactor is the CPU cost of additional one goroutine.
	concurrencyFactor float64

	// enableForceInlineCTE is used to enable/disable force inline CTE.
	enableForceInlineCTE bool

	// CopTiFlashConcurrencyFactor is the concurrency number of computation in tiflash coprocessor.
	CopTiFlashConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// In https://github.com/pingcap/tidb/issues/14164, we can see that MySQL can enter the column that is not in the insert's SELECT's output.
	// We store the extra columns in this variable.
	CurrInsertBatchExtraCols [][]types.Datum

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	/* TiDB system variables */

	// SkipASCIICheck check on input value.
	SkipASCIICheck bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// BatchCommit indicates if we should split the transaction into multiple batches.
	BatchCommit bool

	// IDAllocator is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Table.alloc.
	IDAllocator autoid.Allocator

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int

	// OptimizerEnableNewOnlyFullGroupByCheck enables the new only_full_group_by check which is implemented by maintaining functional dependency.
	OptimizerEnableNewOnlyFullGroupByCheck bool

	// EnableOuterJoinWithJoinReorder enables TiDB to involve the outer join into the join reorder.
	EnableOuterJoinReorder bool

	// OptimizerEnableNAAJ enables TiDB to use null-aware anti join.
	OptimizerEnableNAAJ bool

	// EnableTablePartition enables table partition feature.
	EnableTablePartition string

	// EnableListTablePartition enables list table partition feature.
	EnableListTablePartition bool

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnablePipelinedWindowExec enables executing window functions in a pipelined manner.
	EnablePipelinedWindowExec bool

	// AllowProjectionPushDown enables pushdown projection on TiKV.
	AllowProjectionPushDown bool

	// EnableStrictDoubleTypeCheck enables table field double type check.
	EnableStrictDoubleTypeCheck bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// EnableAutoIncrementInGenerated is used to control whether to allow auto incremented columns in generated columns.
	EnableAutoIncrementInGenerated bool

	// EnablePointGetCache is used to cache value for point get for read only scenario.
	EnablePointGetCache bool

	// PlacementMode the placement mode we use
	//   strict: Check placement settings strictly in ddl operations
	//   ignore: Ignore all placement settings in ddl operations
	PlacementMode string

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	// EnableChunkRPC indicates whether the coprocessor request can use chunk API.
	EnableChunkRPC bool

	writeStmtBufs WriteStmtBufs

	// ConstraintCheckInPlace indicates whether to check the constraint when the SQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current session is doing.
	CommandValue uint32

	// TiDBOptJoinReorderThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	TiDBOptJoinReorderThreshold int

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	SlowQueryFile string

	// EnableFastAnalyze indicates whether to take fast analyze.
	EnableFastAnalyze bool

	// TxnMode indicates should be pessimistic or optimistic.
	TxnMode string

	// LowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds.
	LowResolutionTSO bool

	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the value is 0, timeouts are not enabled.
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	MaxExecutionTime uint64

	// Killed is a flag to indicate that this query is killed.
	Killed uint32

	// ConnectionInfo indicates current connection info used by current session.
	ConnectionInfo *ConnectionInfo

	// NoopFuncsMode allows OFF/ON/WARN values as 0/1/2.
	NoopFuncsMode int

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

	// RewritePhaseInfo records all information about the rewriting phase.
	RewritePhaseInfo

	// DurationOptimization is the duration of optimizing a query.
	DurationOptimization time.Duration

	// DurationWaitTS is the duration of waiting for a snapshot TS
	DurationWaitTS time.Duration

	// PrevStmt is used to store the previous executed statement in the current session.
	PrevStmt fmt.Stringer

	// prevStmtDigest is used to store the digest of the previous statement in the current session.
	prevStmtDigest string

	// AllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	AllowRemoveAutoInc bool

	// UsePlanBaselines indicates whether we will use plan baselines to adjust plan.
	UsePlanBaselines bool

	// EvolvePlanBaselines indicates whether we will evolve the plan baselines.
	EvolvePlanBaselines bool

	// EnableExtendedStats indicates whether we enable the extended statistics feature.
	EnableExtendedStats bool

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAndAgg bool

	// preferRangeScan allows optimizer to always prefer range scan over table scan.
	preferRangeScan bool

	// EnableIndexMerge enables the generation of IndexMergePath.
	enableIndexMerge bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead kv.ReplicaReadType
	// ReplicaClosestReadThreshold is the minimum response body size that a cop request should be sent to the closest replica.
	// this variable only take effect when `tidb_follower_read` = 'closest-adaptive'
	ReplicaClosestReadThreshold int64

	// IsolationReadEngines is used to isolation read, tidb only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[kv.StoreType]struct{}

	mppVersion kv.MppVersion

	mppExchangeCompressionMode kv.ExchangeCompressionMode

	PlannerSelectBlockAsName []ast.HintTable

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	LockWaitTimeout int64

	// MetricSchemaStep indicates the step when query metric schema.
	MetricSchemaStep int64

	// CDCWriteSource indicates the following data is written by TiCDC if it is not 0.
	CDCWriteSource uint64

	// MetricSchemaRangeDuration indicates the step when query metric schema.
	MetricSchemaRangeDuration int64

	// Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
	// All cached snapshots will be released at the end of retrieving
	InspectionTableCache map[string]TableSnapshot

	// RowEncoder is reused in session for encode row data.
	RowEncoder rowcodec.Encoder

	// SequenceState cache all sequence's latest value accessed by lastval() builtins. It's a session scoped
	// variable, and all public methods of SequenceState are currently-safe.
	SequenceState *SequenceState

	// WindowingUseHighPrecision determines whether to compute window operations without loss of precision.
	// see https://dev.mysql.com/doc/refman/8.0/en/window-function-optimization.html for more details.
	WindowingUseHighPrecision bool

	// FoundInPlanCache indicates whether this statement was found in plan cache.
	FoundInPlanCache bool
	// PrevFoundInPlanCache indicates whether the last statement was found in plan cache.
	PrevFoundInPlanCache bool

	// FoundInBinding indicates whether the execution plan is matched with the hints in the binding.
	FoundInBinding bool
	// PrevFoundInBinding indicates whether the last execution plan is matched with the hints in the binding.
	PrevFoundInBinding bool

	// OptimizerUseInvisibleIndexes indicates whether optimizer can use invisible index
	OptimizerUseInvisibleIndexes bool

	// SelectLimit limits the max counts of select statement's output
	SelectLimit uint64

	// EnableClusteredIndex indicates whether to enable clustered index when creating a new table.
	EnableClusteredIndex ClusteredIndexDefMode

	// PresumeKeyNotExists indicates lazy existence checking is enabled.
	PresumeKeyNotExists bool

	// EnableParallelApply indicates that thether to use parallel apply.
	EnableParallelApply bool

	// EnableRedactLog indicates that whether redact log.
	EnableRedactLog bool

	// ShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	ShardAllocateStep int64

	// LastTxnInfo keeps track the info of last committed transaction.
	LastTxnInfo string

	// LastQueryInfo keeps track the info of last query.
	LastQueryInfo sessionstates.QueryInfo

	// LastDDLInfo keeps track the info of last DDL.
	LastDDLInfo sessionstates.LastDDLInfo

	// PartitionPruneMode indicates how and when to prune partitions.
	PartitionPruneMode atomic2.String

	// TxnScope indicates the scope of the transactions. It should be `global` or equal to the value of key `zone` in config.Labels.
	TxnScope kv.TxnScopeVar

	// EnabledRateLimitAction indicates whether enabled ratelimit action during coprocessor
	EnabledRateLimitAction bool

	// EnableAsyncCommit indicates whether to enable the async commit feature.
	EnableAsyncCommit bool

	// Enable1PC indicates whether to enable the one-phase commit feature.
	Enable1PC bool

	// GuaranteeLinearizability indicates whether to guarantee linearizability
	GuaranteeLinearizability bool

	// AnalyzeVersion indicates how TiDB collect and use analyzed statistics.
	AnalyzeVersion int

	// EnableIndexMergeJoin indicates whether to enable index merge join.
	EnableIndexMergeJoin bool

	// TrackAggregateMemoryUsage indicates whether to track the memory usage of aggregate function.
	TrackAggregateMemoryUsage bool

	// TiDBEnableExchangePartition indicates whether to enable exchange partition
	TiDBEnableExchangePartition bool

	// AllowFallbackToTiKV indicates the engine types whose unavailability triggers fallback to TiKV.
	// Now we only support TiFlash.
	AllowFallbackToTiKV map[kv.StoreType]struct{}

	// CTEMaxRecursionDepth indicates The common table expression (CTE) maximum recursion depth.
	// see https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_cte_max_recursion_depth
	CTEMaxRecursionDepth int

	// The temporary table size threshold, which is different from MySQL. See https://github.com/pingcap/tidb/issues/28691.
	TMPTableSize int64

	// EnableStableResultMode if stabilize query results.
	EnableStableResultMode bool

	// EnablePseudoForOutdatedStats if using pseudo for outdated stats
	EnablePseudoForOutdatedStats bool

	// RegardNULLAsPoint if regard NULL as Point
	RegardNULLAsPoint bool

	// LocalTemporaryTables is *infoschema.LocalTemporaryTables, use interface to avoid circle dependency.
	// It's nil if there is no local temporary table.
	LocalTemporaryTables interface{}

	// TemporaryTableData stores committed kv values for temporary table for current session.
	TemporaryTableData TemporaryTableData

	// MPPStoreFailTTL indicates the duration that protect TiDB from sending task to a new recovered TiFlash.
	MPPStoreFailTTL string

	// ReadStaleness indicates the staleness duration for the following query
	ReadStaleness time.Duration

	// cachedStmtCtx is used to optimze the object allocation.
	cachedStmtCtx [2]stmtctx.StatementContext

	// Rng stores the rand_seed1 and rand_seed2 for Rand() function
	Rng *mathutil.MysqlRng

	// EnablePaging indicates whether enable paging in coprocessor requests.
	EnablePaging bool

	// EnableLegacyInstanceScope says if SET SESSION can be used to set an instance
	// scope variable. The default is TRUE.
	EnableLegacyInstanceScope bool

	// ReadConsistency indicates the read consistency requirement.
	ReadConsistency ReadConsistencyLevel

	// StatsLoadSyncWait indicates how long to wait for stats load before timeout.
	StatsLoadSyncWait int64

	// SysdateIsNow indicates whether Sysdate is an alias of Now function
	SysdateIsNow bool
	// EnableMutationChecker indicates whether to check data consistency for mutations
	EnableMutationChecker bool
	// AssertionLevel controls how strict the assertions on data mutations should be.
	AssertionLevel AssertionLevel
	// IgnorePreparedCacheCloseStmt controls if ignore the close-stmt command for prepared statement.
	IgnorePreparedCacheCloseStmt bool
	// EnableNewCostInterface is a internal switch to indicates whether to use the new cost calculation interface.
	EnableNewCostInterface bool
	// CostModelVersion is a internal switch to indicates the Cost Model Version.
	CostModelVersion int
	// IndexJoinDoubleReadPenaltyCostRate indicates whether to add some penalty cost to IndexJoin and how much of it.
	IndexJoinDoubleReadPenaltyCostRate float64

	// BatchPendingTiFlashCount shows the threshold of pending TiFlash tables when batch adding.
	BatchPendingTiFlashCount int
	// RcWriteCheckTS indicates whether some special write statements don't get latest tso from PD at RC
	RcWriteCheckTS bool
	// RemoveOrderbyInSubquery indicates whether to remove ORDER BY in subquery.
	RemoveOrderbyInSubquery bool
	// NonTransactionalIgnoreError indicates whether to ignore error in non-transactional statements.
	// When set to false, returns immediately when it meets the first error.
	NonTransactionalIgnoreError bool

	// MaxAllowedPacket indicates the maximum size of a packet for the MySQL protocol.
	MaxAllowedPacket uint64

	// TiFlash related optimization, only for MPP.
	TiFlashFineGrainedShuffleStreamCount int64
	TiFlashFineGrainedShuffleBatchSize   uint64

	// RequestSourceType is the type of inner request.
	RequestSourceType string

	// MemoryDebugModeMinHeapInUse indicated the minimum heapInUse threshold that triggers the memoryDebugMode.
	MemoryDebugModeMinHeapInUse int64
	// MemoryDebugModeAlarmRatio indicated the allowable bias ratio of memory tracking accuracy check.
	// When `(memory trakced by tidb) * (1+MemoryDebugModeAlarmRatio) < actual heapInUse`, an alarm log will be recorded.
	MemoryDebugModeAlarmRatio int64

	// EnableAnalyzeSnapshot indicates whether to read data on snapshot when collecting statistics.
	// When it is false, ANALYZE reads the latest data.
	// When it is true, ANALYZE reads data on the snapshot at the beginning of ANALYZE.
	EnableAnalyzeSnapshot bool

	// DefaultStrMatchSelectivity adjust the estimation strategy for string matching expressions that can't be estimated by building into range.
	// when > 0: it's the selectivity for the expression.
	// when = 0: try to use TopN to evaluate the like expression to estimate the selectivity.
	DefaultStrMatchSelectivity float64

	// TiFlashFastScan indicates whether use fast scan in TiFlash
	TiFlashFastScan bool

	// PrimaryKeyRequired indicates if sql_require_primary_key sysvar is set
	PrimaryKeyRequired bool

	// EnablePreparedPlanCache indicates whether to enable prepared plan cache.
	EnablePreparedPlanCache bool

	// PreparedPlanCacheSize controls the size of prepared plan cache.
	PreparedPlanCacheSize uint64

	// PreparedPlanCacheMonitor indicates whether to enable prepared plan cache monitor.
	EnablePreparedPlanCacheMemoryMonitor bool

	// EnablePlanCacheForParamLimit controls whether the prepare statement with parameterized limit can be cached
	EnablePlanCacheForParamLimit bool

	// EnableNonPreparedPlanCache indicates whether to enable non-prepared plan cache.
	EnableNonPreparedPlanCache bool

	// NonPreparedPlanCacheSize controls the size of non-prepared plan cache.
	NonPreparedPlanCacheSize uint64

	// ConstraintCheckInPlacePessimistic controls whether to skip the locking of some keys in pessimistic transactions.
	// Postpone the conflict check and constraint check to prewrite or later pessimistic locking requests.
	ConstraintCheckInPlacePessimistic bool

	// EnableTiFlashReadForWriteStmt indicates whether to enable TiFlash to read for write statements.
	EnableTiFlashReadForWriteStmt bool

	// EnableUnsafeSubstitute indicates whether to enable generate column takes unsafe substitute.
	EnableUnsafeSubstitute bool

	// ForeignKeyChecks indicates whether to enable foreign key constraint check.
	ForeignKeyChecks bool

	// RangeMaxSize is the max memory limit for ranges. When the optimizer estimates that the memory usage of complete
	// ranges would exceed the limit, it chooses less accurate ranges such as full range. 0 indicates that there is no
	// memory limit for ranges.
	RangeMaxSize int64

	// LastPlanReplayerToken indicates the last plan replayer token
	LastPlanReplayerToken string

	// AnalyzePartitionConcurrency indicates concurrency for partitions in Analyze
	AnalyzePartitionConcurrency int
	// AnalyzePartitionMergeConcurrency indicates concurrency for merging partition stats
	AnalyzePartitionMergeConcurrency int

	// EnableExternalTSRead indicates whether to enable read through external ts
	EnableExternalTSRead bool

	HookContext

	// MemTracker indicates the memory tracker of current session.
	MemTracker *memory.Tracker
	// MemDBDBFootprint tracks the memory footprint of memdb, and is attached to `MemTracker`
	MemDBFootprint *memory.Tracker
	DiskTracker    *memory.Tracker

	// OptPrefixIndexSingleScan indicates whether to do some optimizations to avoid double scan for prefix index.
	// When set to true, `col is (not) null`(`col` is index prefix column) is regarded as index filter rather than table filter.
	OptPrefixIndexSingleScan bool

	// ChunkPool Several chunks and columns are cached
	ChunkPool ReuseChunkPool
	// EnableReuseCheck indicates  request chunk whether use chunk alloc
	EnableReuseCheck bool

	// preuseChunkAlloc indicates whether pre statement use chunk alloc
	// like select @@last_sql_use_alloc
	preUseChunkAlloc bool

	// EnablePlanReplayerCapture indicates whether enabled plan replayer capture
	EnablePlanReplayerCapture bool

	// EnablePlanReplayedContinuesCapture indicates whether enabled plan replayer continues capture
	EnablePlanReplayedContinuesCapture bool

	// PlanReplayerFinishedTaskKey used to record the finished plan replayer task key in order not to record the
	// duplicate task in plan replayer continues capture
	PlanReplayerFinishedTaskKey map[replayer.PlanReplayerTaskKey]struct{}

	// StoreBatchSize indicates the batch size limit of store batch, set this field to 0 to disable store batch.
	StoreBatchSize int

	// shardRand is used by TxnCtx, for the GetCurrentShard() method.
	shardRand *rand.Rand

	// Resource group name
	ResourceGroupName string

	// ProtectedTSList holds a list of timestamps that should delay GC.
	ProtectedTSList protectedTSList

	// PessimisticTransactionAggressiveLocking controls whether aggressive locking for pessimistic transaction
	// is enabled.
	PessimisticTransactionAggressiveLocking bool
}

// planReplayerSessionFinishedTaskKeyLen is used to control the max size for the finished plan replayer task key in session
// in order to control the used memory
const planReplayerSessionFinishedTaskKeyLen = 128

// AddPlanReplayerFinishedTaskKey record finished task key in session
func (s *SessionVars) AddPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) {
	if len(s.PlanReplayerFinishedTaskKey) >= planReplayerSessionFinishedTaskKeyLen {
		s.initializePlanReplayerFinishedTaskKey()
	}
	s.PlanReplayerFinishedTaskKey[key] = struct{}{}
}

func (s *SessionVars) initializePlanReplayerFinishedTaskKey() {
	s.PlanReplayerFinishedTaskKey = make(map[replayer.PlanReplayerTaskKey]struct{}, planReplayerSessionFinishedTaskKeyLen)
}

// CheckPlanReplayerFinishedTaskKey check whether the key exists
func (s *SessionVars) CheckPlanReplayerFinishedTaskKey(key replayer.PlanReplayerTaskKey) bool {
	if s.PlanReplayerFinishedTaskKey == nil {
		s.initializePlanReplayerFinishedTaskKey()
		return false
	}
	_, ok := s.PlanReplayerFinishedTaskKey[key]
	return ok
}

// IsPlanReplayerCaptureEnabled indicates whether capture or continues capture enabled
func (s *SessionVars) IsPlanReplayerCaptureEnabled() bool {
	return s.EnablePlanReplayerCapture || s.EnablePlanReplayedContinuesCapture
}

// GetNewChunkWithCapacity Attempt to request memory from the chunk pool
// thread safety
func (s *SessionVars) GetNewChunkWithCapacity(fields []*types.FieldType, capacity int, maxCachesize int, pool chunk.Allocator) *chunk.Chunk {
	if pool == nil {
		return chunk.New(fields, capacity, maxCachesize)
	}
	s.ChunkPool.mu.Lock()
	defer s.ChunkPool.mu.Unlock()
	if pool.CheckReuseAllocSize() && (!s.GetUseChunkAlloc()) {
		s.StmtCtx.SetUseChunkAlloc()
	}
	chk := pool.Alloc(fields, capacity, maxCachesize)
	return chk
}

// ExchangeChunkStatus give the status to preUseChunkAlloc
func (s *SessionVars) ExchangeChunkStatus() {
	s.preUseChunkAlloc = s.GetUseChunkAlloc()
}

// GetUseChunkAlloc return useChunkAlloc status
func (s *SessionVars) GetUseChunkAlloc() bool {
	return s.StmtCtx.GetUseChunkAllocStatus()
}

// SetAlloc Attempt to set the buffer pool address
func (s *SessionVars) SetAlloc(alloc chunk.Allocator) {
	if !s.EnableReuseCheck {
		return
	}
	s.ChunkPool.Alloc = alloc
}

// ClearAlloc indicates stop reuse chunk
func (s *SessionVars) ClearAlloc(alloc *chunk.Allocator, b bool) {
	if !b {
		s.ChunkPool.Alloc = nil
		return
	}

	// If an error is reported, re-apply for alloc
	// Prevent the goroutine left before, affecting the execution of the next sql
	// issuse 38918
	s.ChunkPool.mu.Lock()
	s.ChunkPool.Alloc = nil
	s.ChunkPool.mu.Unlock()
	*alloc = chunk.NewAllocator()
}

// GetPreparedStmtByName returns the prepared statement specified by stmtName.
func (s *SessionVars) GetPreparedStmtByName(stmtName string) (interface{}, error) {
	stmtID, ok := s.PreparedStmtNameToID[stmtName]
	if !ok {
		return nil, ErrStmtNotFound
	}
	return s.GetPreparedStmtByID(stmtID)
}

// GetPreparedStmtByID returns the prepared statement specified by stmtID.
func (s *SessionVars) GetPreparedStmtByID(stmtID uint32) (interface{}, error) {
	stmt, ok := s.PreparedStmts[stmtID]
	if !ok {
		return nil, ErrStmtNotFound
	}
	return stmt, nil
}

// InitStatementContext initializes a StatementContext, the object is reused to reduce allocation.
func (s *SessionVars) InitStatementContext() *stmtctx.StatementContext {
	sc := &s.cachedStmtCtx[0]
	if sc == s.StmtCtx {
		sc = &s.cachedStmtCtx[1]
	}
	if s.RefCountOfStmtCtx.TryFreeze() {
		*sc = stmtctx.StatementContext{}
		s.RefCountOfStmtCtx.UnFreeze()
	} else {
		sc = &stmtctx.StatementContext{}
	}
	return sc
}

// IsMPPAllowed returns whether mpp execution is allowed.
func (s *SessionVars) IsMPPAllowed() bool {
	return s.allowMPPExecution
}

// IsMPPEnforced returns whether mpp execution is enforced.
func (s *SessionVars) IsMPPEnforced() bool {
	return s.allowMPPExecution && s.enforceMPPExecution
}

// ChooseMppVersion indicates the mpp-version used to build mpp plan, if mpp-version is unspecified, use the latest version.
func (s *SessionVars) ChooseMppVersion() kv.MppVersion {
	if s.mppVersion == kv.MppVersionUnspecified {
		return kv.GetNewestMppVersion()
	}
	return s.mppVersion
}

// ChooseMppExchangeCompressionMode indicates the data compression method in mpp exchange operator
func (s *SessionVars) ChooseMppExchangeCompressionMode() kv.ExchangeCompressionMode {
	if s.mppExchangeCompressionMode == kv.ExchangeCompressionModeUnspecified {
		// If unspecified, use recommended mode
		return kv.RecommendedExchangeCompressionMode
	}
	return s.mppExchangeCompressionMode
}

// RaiseWarningWhenMPPEnforced will raise a warning when mpp mode is enforced and executing explain statement.
// TODO: Confirm whether this function will be inlined and
// omit the overhead of string construction when calling with false condition.
func (s *SessionVars) RaiseWarningWhenMPPEnforced(warning string) {
	if !s.IsMPPEnforced() {
		return
	}
	if s.StmtCtx.InExplainStmt {
		s.StmtCtx.AppendWarning(errors.New(warning))
	} else {
		s.StmtCtx.AppendExtraWarning(errors.New(warning))
	}
}

// CheckAndGetTxnScope will return the transaction scope we should use in the current session.
func (s *SessionVars) CheckAndGetTxnScope() string {
	if s.InRestrictedSQL || !EnableLocalTxn.Load() {
		return kv.GlobalTxnScope
	}
	if s.TxnScope.GetVarValue() == kv.LocalTxnScope {
		return s.TxnScope.GetTxnScope()
	}
	return kv.GlobalTxnScope
}

// IsDynamicPartitionPruneEnabled indicates whether dynamic partition prune enabled
// Note that: IsDynamicPartitionPruneEnabled only indicates whether dynamic partition prune mode is enabled according to
// session variable, it isn't guaranteed to be used during query due to other conditions checking.
func (s *SessionVars) IsDynamicPartitionPruneEnabled() bool {
	return PartitionPruneMode(s.PartitionPruneMode.Load()) == Dynamic
}

// BuildParserConfig generate parser.ParserConfig for initial parser
func (s *SessionVars) BuildParserConfig() parser.ParserConfig {
	return parser.ParserConfig{
		EnableWindowFunction:        s.EnableWindowFunction,
		EnableStrictDoubleTypeCheck: s.EnableStrictDoubleTypeCheck,
		SkipPositionRecording:       true,
	}
}

// AllocNewPlanID alloc new ID
func (s *SessionVars) AllocNewPlanID() int {
	s.PlanID++
	return s.PlanID
}

const (
	// PlacementModeStrict indicates all placement operations should be checked strictly in ddl
	PlacementModeStrict string = "STRICT"
	// PlacementModeIgnore indicates ignore all placement operations in ddl
	PlacementModeIgnore string = "IGNORE"
)

// PartitionPruneMode presents the prune mode used.
type PartitionPruneMode string

const (
	// Static indicates only prune at plan phase.
	Static PartitionPruneMode = "static"
	// Dynamic indicates only prune at execute phase.
	Dynamic PartitionPruneMode = "dynamic"

	// Don't use out-of-date mode.

	// StaticOnly is out-of-date.
	StaticOnly PartitionPruneMode = "static-only"
	// DynamicOnly is out-of-date.
	DynamicOnly PartitionPruneMode = "dynamic-only"
	// StaticButPrepareDynamic is out-of-date.
	StaticButPrepareDynamic PartitionPruneMode = "static-collect-dynamic"
)

// Valid indicate PruneMode is validated.
func (p PartitionPruneMode) Valid() bool {
	switch p {
	case Static, Dynamic, StaticOnly, DynamicOnly:
		return true
	default:
		return false
	}
}

// Update updates out-of-date PruneMode.
func (p PartitionPruneMode) Update() PartitionPruneMode {
	switch p {
	case StaticOnly, StaticButPrepareDynamic:
		return Static
	case DynamicOnly:
		return Dynamic
	default:
		return p
	}
}

// PreparedParams contains the parameters of the current prepared statement when executing it.
type PreparedParams []types.Datum

func (pps PreparedParams) String() string {
	if len(pps) == 0 {
		return ""
	}
	return " [arguments: " + types.DatumsToStrNoErr(pps) + "]"
}

// ConnectionInfo presents the connection information, which is mainly used by audit logs.
type ConnectionInfo struct {
	ConnectionID      uint64
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerIP          string
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	DB                string
}

const (
	// ConnTypeSocket indicates socket without TLS.
	ConnTypeSocket string = "Socket"
	// ConnTypeUnixSocket indicates Unix Socket.
	ConnTypeUnixSocket string = "UnixSocket"
	// ConnTypeTLS indicates socket with TLS.
	ConnTypeTLS string = "SSL/TLS"
)

// IsSecureTransport checks whether the connection is secure.
func (connInfo *ConnectionInfo) IsSecureTransport() bool {
	switch connInfo.ConnectionType {
	case ConnTypeUnixSocket, ConnTypeTLS:
		return true
	}
	return false
}

// NewSessionVars creates a session vars object.
func NewSessionVars(hctx HookContext) *SessionVars {
	vars := &SessionVars{
		userVars: struct {
			lock   sync.RWMutex
			values map[string]types.Datum
			types  map[string]*types.FieldType
		}{
			values: make(map[string]types.Datum),
			types:  make(map[string]*types.FieldType),
		},
		systems:                       make(map[string]string),
		stmtVars:                      make(map[string]string),
		PreparedStmts:                 make(map[uint32]interface{}),
		PreparedStmtNameToID:          make(map[string]uint32),
		PreparedParams:                make([]types.Datum, 0, 10),
		TxnCtx:                        &TransactionContext{},
		RetryInfo:                     &RetryInfo{},
		ActiveRoles:                   make([]*auth.RoleIdentity, 0, 10),
		StrictSQLMode:                 true,
		AutoIncrementIncrement:        DefAutoIncrementIncrement,
		AutoIncrementOffset:           DefAutoIncrementOffset,
		Status:                        mysql.ServerStatusAutocommit,
		StmtCtx:                       new(stmtctx.StatementContext),
		AllowAggPushDown:              false,
		AllowCartesianBCJ:             DefOptCartesianBCJ,
		MPPOuterJoinFixedBuildSide:    DefOptMPPOuterJoinFixedBuildSide,
		BroadcastJoinThresholdSize:    DefBroadcastJoinThresholdSize,
		BroadcastJoinThresholdCount:   DefBroadcastJoinThresholdSize,
		OptimizerSelectivityLevel:     DefTiDBOptimizerSelectivityLevel,
		EnableOuterJoinReorder:        DefTiDBEnableOuterJoinReorder,
		RetryLimit:                    DefTiDBRetryLimit,
		DisableTxnAutoRetry:           DefTiDBDisableTxnAutoRetry,
		DDLReorgPriority:              kv.PriorityLow,
		allowInSubqToJoinAndAgg:       DefOptInSubqToJoinAndAgg,
		preferRangeScan:               DefOptPreferRangeScan,
		EnableCorrelationAdjustment:   DefOptEnableCorrelationAdjustment,
		LimitPushDownThreshold:        DefOptLimitPushDownThreshold,
		CorrelationThreshold:          DefOptCorrelationThreshold,
		CorrelationExpFactor:          DefOptCorrelationExpFactor,
		cpuFactor:                     DefOptCPUFactor,
		copCPUFactor:                  DefOptCopCPUFactor,
		CopTiFlashConcurrencyFactor:   DefOptTiFlashConcurrencyFactor,
		networkFactor:                 DefOptNetworkFactor,
		scanFactor:                    DefOptScanFactor,
		descScanFactor:                DefOptDescScanFactor,
		seekFactor:                    DefOptSeekFactor,
		memoryFactor:                  DefOptMemoryFactor,
		diskFactor:                    DefOptDiskFactor,
		concurrencyFactor:             DefOptConcurrencyFactor,
		enableForceInlineCTE:          DefOptForceInlineCTE,
		EnableVectorizedExpression:    DefEnableVectorizedExpression,
		CommandValue:                  uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold:   DefTiDBOptJoinReorderThreshold,
		SlowQueryFile:                 config.GetGlobalConfig().Log.SlowQueryFile,
		WaitSplitRegionFinish:         DefTiDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:        DefWaitSplitRegionTimeout,
		enableIndexMerge:              DefTiDBEnableIndexMerge,
		NoopFuncsMode:                 TiDBOptOnOffWarn(DefTiDBEnableNoopFuncs),
		replicaRead:                   kv.ReplicaReadLeader,
		AllowRemoveAutoInc:            DefTiDBAllowRemoveAutoInc,
		UsePlanBaselines:              DefTiDBUsePlanBaselines,
		EvolvePlanBaselines:           DefTiDBEvolvePlanBaselines,
		EnableExtendedStats:           false,
		IsolationReadEngines:          make(map[kv.StoreType]struct{}),
		LockWaitTimeout:               DefInnodbLockWaitTimeout * 1000,
		MetricSchemaStep:              DefTiDBMetricSchemaStep,
		MetricSchemaRangeDuration:     DefTiDBMetricSchemaRangeDuration,
		SequenceState:                 NewSequenceState(),
		WindowingUseHighPrecision:     true,
		PrevFoundInPlanCache:          DefTiDBFoundInPlanCache,
		FoundInPlanCache:              DefTiDBFoundInPlanCache,
		PrevFoundInBinding:            DefTiDBFoundInBinding,
		FoundInBinding:                DefTiDBFoundInBinding,
		SelectLimit:                   math.MaxUint64,
		AllowAutoRandExplicitInsert:   DefTiDBAllowAutoRandExplicitInsert,
		EnableClusteredIndex:          DefTiDBEnableClusteredIndex,
		EnableParallelApply:           DefTiDBEnableParallelApply,
		ShardAllocateStep:             DefTiDBShardAllocateStep,
		PartitionPruneMode:            *atomic2.NewString(DefTiDBPartitionPruneMode),
		TxnScope:                      kv.NewDefaultTxnScopeVar(),
		EnabledRateLimitAction:        DefTiDBEnableRateLimitAction,
		EnableAsyncCommit:             DefTiDBEnableAsyncCommit,
		Enable1PC:                     DefTiDBEnable1PC,
		GuaranteeLinearizability:      DefTiDBGuaranteeLinearizability,
		AnalyzeVersion:                DefTiDBAnalyzeVersion,
		EnableIndexMergeJoin:          DefTiDBEnableIndexMergeJoin,
		AllowFallbackToTiKV:           make(map[kv.StoreType]struct{}),
		CTEMaxRecursionDepth:          DefCTEMaxRecursionDepth,
		TMPTableSize:                  DefTiDBTmpTableMaxSize,
		MPPStoreFailTTL:               DefTiDBMPPStoreFailTTL,
		Rng:                           mathutil.NewWithTime(),
		StatsLoadSyncWait:             StatsLoadSyncWait.Load(),
		EnableLegacyInstanceScope:     DefEnableLegacyInstanceScope,
		RemoveOrderbyInSubquery:       DefTiDBRemoveOrderbyInSubquery,
		EnableSkewDistinctAgg:         DefTiDBSkewDistinctAgg,
		Enable3StageDistinctAgg:       DefTiDB3StageDistinctAgg,
		MaxAllowedPacket:              DefMaxAllowedPacket,
		TiFlashFastScan:               DefTiFlashFastScan,
		EnableTiFlashReadForWriteStmt: DefTiDBEnableTiFlashReadForWriteStmt,
		ForeignKeyChecks:              DefTiDBForeignKeyChecks,
		HookContext:                   hctx,
		EnableReuseCheck:              DefTiDBEnableReusechunk,
		preUseChunkAlloc:              DefTiDBUseAlloc,
		ChunkPool:                     ReuseChunkPool{Alloc: nil},
		mppExchangeCompressionMode:    DefaultExchangeCompressionMode,
		mppVersion:                    kv.MppVersionUnspecified,
	}
	vars.KVVars = tikvstore.NewVariables(&vars.Killed)
	vars.Concurrency = Concurrency{
		indexLookupConcurrency:            DefIndexLookupConcurrency,
		indexSerialScanConcurrency:        DefIndexSerialScanConcurrency,
		indexLookupJoinConcurrency:        DefIndexLookupJoinConcurrency,
		hashJoinConcurrency:               DefTiDBHashJoinConcurrency,
		projectionConcurrency:             DefTiDBProjectionConcurrency,
		distSQLScanConcurrency:            DefDistSQLScanConcurrency,
		hashAggPartialConcurrency:         DefTiDBHashAggPartialConcurrency,
		hashAggFinalConcurrency:           DefTiDBHashAggFinalConcurrency,
		windowConcurrency:                 DefTiDBWindowConcurrency,
		mergeJoinConcurrency:              DefTiDBMergeJoinConcurrency,
		streamAggConcurrency:              DefTiDBStreamAggConcurrency,
		indexMergeIntersectionConcurrency: DefTiDBIndexMergeIntersectionConcurrency,
		ExecutorConcurrency:               DefExecutorConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:      DefTiDBMemQuotaQuery,
		MemQuotaApplyCache: DefTiDBMemQuotaApplyCache,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		InitChunkSize:      DefInitChunkSize,
		MaxChunkSize:       DefMaxChunkSize,
		MinPagingSize:      DefMinPagingSize,
		MaxPagingSize:      DefMaxPagingSize,
	}
	vars.DMLBatchSize = DefDMLBatchSize
	vars.AllowBatchCop = DefTiDBAllowBatchCop
	vars.allowMPPExecution = DefTiDBAllowMPPExecution
	vars.HashExchangeWithNewCollation = DefTiDBHashExchangeWithNewCollation
	vars.enforceMPPExecution = DefTiDBEnforceMPPExecution
	vars.TiFlashMaxThreads = DefTiFlashMaxThreads
	vars.MPPStoreFailTTL = DefTiDBMPPStoreFailTTL
	vars.DiskTracker = disk.NewTracker(memory.LabelForSession, -1)
	vars.MemTracker = memory.NewTracker(memory.LabelForSession, vars.MemQuotaQuery)
	vars.MemTracker.IsRootTrackerOfSess = true

	for _, engine := range config.GetGlobalConfig().IsolationRead.Engines {
		switch engine {
		case kv.TiFlash.Name():
			vars.IsolationReadEngines[kv.TiFlash] = struct{}{}
		case kv.TiKV.Name():
			vars.IsolationReadEngines[kv.TiKV] = struct{}{}
		case kv.TiDB.Name():
			vars.IsolationReadEngines[kv.TiDB] = struct{}{}
		}
	}
	if !EnableLocalTxn.Load() {
		vars.TxnScope = kv.NewGlobalTxnScopeVar()
	}
	vars.systems[CharacterSetConnection], vars.systems[CollationConnection] = charset.GetDefaultCharsetAndCollate()
	return vars
}

// GetAllowInSubqToJoinAndAgg get AllowInSubqToJoinAndAgg from sql hints and SessionVars.allowInSubqToJoinAndAgg.
func (s *SessionVars) GetAllowInSubqToJoinAndAgg() bool {
	if s.StmtCtx.HasAllowInSubqToJoinAndAggHint {
		return s.StmtCtx.AllowInSubqToJoinAndAgg
	}
	return s.allowInSubqToJoinAndAgg
}

// SetAllowInSubqToJoinAndAgg set SessionVars.allowInSubqToJoinAndAgg.
func (s *SessionVars) SetAllowInSubqToJoinAndAgg(val bool) {
	s.allowInSubqToJoinAndAgg = val
}

// GetAllowPreferRangeScan get preferRangeScan from SessionVars.preferRangeScan.
func (s *SessionVars) GetAllowPreferRangeScan() bool {
	return s.preferRangeScan
}

// SetAllowPreferRangeScan set SessionVars.preferRangeScan.
func (s *SessionVars) SetAllowPreferRangeScan(val bool) {
	s.preferRangeScan = val
}

// GetEnableCascadesPlanner get EnableCascadesPlanner from sql hints and SessionVars.EnableCascadesPlanner.
func (s *SessionVars) GetEnableCascadesPlanner() bool {
	if s.StmtCtx.HasEnableCascadesPlannerHint {
		return s.StmtCtx.EnableCascadesPlanner
	}
	return s.EnableCascadesPlanner
}

// SetEnableCascadesPlanner set SessionVars.EnableCascadesPlanner.
func (s *SessionVars) SetEnableCascadesPlanner(val bool) {
	s.EnableCascadesPlanner = val
}

// GetEnableIndexMerge get EnableIndexMerge from SessionVars.enableIndexMerge.
func (s *SessionVars) GetEnableIndexMerge() bool {
	return s.enableIndexMerge
}

// SetEnableIndexMerge set SessionVars.enableIndexMerge.
func (s *SessionVars) SetEnableIndexMerge(val bool) {
	s.enableIndexMerge = val
}

// GetEnablePseudoForOutdatedStats get EnablePseudoForOutdatedStats from SessionVars.EnablePseudoForOutdatedStats.
func (s *SessionVars) GetEnablePseudoForOutdatedStats() bool {
	return s.EnablePseudoForOutdatedStats
}

// SetEnablePseudoForOutdatedStats set SessionVars.EnablePseudoForOutdatedStats.
func (s *SessionVars) SetEnablePseudoForOutdatedStats(val bool) {
	s.EnablePseudoForOutdatedStats = val
}

// GetReplicaRead get ReplicaRead from sql hints and SessionVars.replicaRead.
func (s *SessionVars) GetReplicaRead() kv.ReplicaReadType {
	if s.StmtCtx.HasReplicaReadHint {
		return kv.ReplicaReadType(s.StmtCtx.ReplicaRead)
	}
	// if closest-adaptive is unavailable, fallback to leader read
	if s.replicaRead == kv.ReplicaReadClosestAdaptive && !IsAdaptiveReplicaReadEnabled() {
		return kv.ReplicaReadLeader
	}
	return s.replicaRead
}

// SetReplicaRead set SessionVars.replicaRead.
func (s *SessionVars) SetReplicaRead(val kv.ReplicaReadType) {
	s.replicaRead = val
}

// IsReplicaReadClosestAdaptive returns whether adaptive closest replica can be enabled.
func (s *SessionVars) IsReplicaReadClosestAdaptive() bool {
	return s.replicaRead == kv.ReplicaReadClosestAdaptive && IsAdaptiveReplicaReadEnabled()
}

// GetWriteStmtBufs get pointer of SessionVars.writeStmtBufs.
func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
	return &s.writeStmtBufs
}

// GetSplitRegionTimeout gets split region timeout.
func (s *SessionVars) GetSplitRegionTimeout() time.Duration {
	return time.Duration(s.WaitSplitRegionTimeout) * time.Second
}

// GetIsolationReadEngines gets isolation read engines.
func (s *SessionVars) GetIsolationReadEngines() map[kv.StoreType]struct{} {
	return s.IsolationReadEngines
}

// CleanBuffers cleans the temporary bufs
func (s *SessionVars) CleanBuffers() {
	s.GetWriteStmtBufs().clean()
}

// AllocPlanColumnID allocates column id for plan.
func (s *SessionVars) AllocPlanColumnID() int64 {
	s.PlanColumnID++
	return s.PlanColumnID
}

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	charset = s.systems[CharacterSetConnection]
	collation = s.systems[CollationConnection]
	return
}

// GetParseParams gets the parse parameters from session variables.
func (s *SessionVars) GetParseParams() []parser.ParseParam {
	chs, coll := s.GetCharsetInfo()
	cli, err := s.GetSessionOrGlobalSystemVar(context.Background(), CharacterSetClient)
	if err != nil {
		cli = ""
	}
	return []parser.ParseParam{
		parser.CharsetConnection(chs),
		parser.CollationConnection(coll),
		parser.CharsetClient(cli),
	}
}

// SetStringUserVar set the value and collation for user defined variable.
func (s *SessionVars) SetStringUserVar(name string, strVal string, collation string) {
	name = strings.ToLower(name)
	if len(collation) > 0 {
		s.SetUserVarVal(name, types.NewCollationStringDatum(stringutil.Copy(strVal), collation))
	} else {
		_, collation = s.GetCharsetInfo()
		s.SetUserVarVal(name, types.NewCollationStringDatum(stringutil.Copy(strVal), collation))
	}
}

// UnsetUserVar unset an user defined variable by name.
func (s *SessionVars) UnsetUserVar(varName string) {
	varName = strings.ToLower(varName)
	s.userVars.lock.Lock()
	defer s.userVars.lock.Unlock()
	delete(s.userVars.values, varName)
	delete(s.userVars.types, varName)
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the session server status variable.
// If on is true sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		s.Status |= flag
		return
	}
	s.Status &= ^flag
}

// GetStatusFlag gets the session server status variable, returns true if it is on.
func (s *SessionVars) GetStatusFlag(flag uint16) bool {
	return s.Status&flag > 0
}

// SetInTxn sets whether the session is in transaction.
// It also updates the IsExplicit flag in TxnCtx if val is true.
func (s *SessionVars) SetInTxn(val bool) {
	s.SetStatusFlag(mysql.ServerStatusInTrans, val)
	if val {
		s.TxnCtx.IsExplicit = val
	}
}

// InTxn returns if the session is in transaction.
func (s *SessionVars) InTxn() bool {
	return s.GetStatusFlag(mysql.ServerStatusInTrans)
}

// IsAutocommit returns if the session is set to autocommit.
func (s *SessionVars) IsAutocommit() bool {
	return s.GetStatusFlag(mysql.ServerStatusAutocommit)
}

// IsIsolation if true it means the transaction is at that isolation level.
func (s *SessionVars) IsIsolation(isolation string) bool {
	if s.TxnCtx.Isolation != "" {
		return s.TxnCtx.Isolation == isolation
	}
	if s.txnIsolationLevelOneShot.state == oneShotUse {
		s.TxnCtx.Isolation = s.txnIsolationLevelOneShot.value
	}
	if s.TxnCtx.Isolation == "" {
		s.TxnCtx.Isolation, _ = s.GetSystemVar(TxnIsolation)
	}
	return s.TxnCtx.Isolation == isolation
}

// IsolationLevelForNewTxn returns the isolation level if we want to enter a new transaction
func (s *SessionVars) IsolationLevelForNewTxn() (isolation string) {
	if s.InTxn() {
		if s.txnIsolationLevelOneShot.state == oneShotSet {
			isolation = s.txnIsolationLevelOneShot.value
		}
	} else {
		if s.txnIsolationLevelOneShot.state == oneShotUse {
			isolation = s.txnIsolationLevelOneShot.value
		}
	}

	if isolation == "" {
		isolation, _ = s.GetSystemVar(TxnIsolation)
	}

	return
}

// SetTxnIsolationLevelOneShotStateForNextTxn sets the txnIsolationLevelOneShot.state for next transaction.
func (s *SessionVars) SetTxnIsolationLevelOneShotStateForNextTxn() {
	if isoLevelOneShot := &s.txnIsolationLevelOneShot; isoLevelOneShot.state != oneShotDef {
		switch isoLevelOneShot.state {
		case oneShotSet:
			isoLevelOneShot.state = oneShotUse
		case oneShotUse:
			isoLevelOneShot.state = oneShotDef
			isoLevelOneShot.value = ""
		}
	}
}

// IsPessimisticReadConsistency if true it means the statement is in an read consistency pessimistic transaction.
func (s *SessionVars) IsPessimisticReadConsistency() bool {
	return s.TxnCtx.IsPessimistic && s.IsIsolation(ast.ReadCommitted)
}

// GetNextPreparedStmtID generates and returns the next session scope prepared statement id.
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// SetNextPreparedStmtID sets the next prepared statement id. It's only used in restoring session states.
func (s *SessionVars) SetNextPreparedStmtID(preparedStmtID uint32) {
	s.preparedStmtID = preparedStmtID
}

// Location returns the value of time_zone session variable. If it is nil, then return time.Local.
func (s *SessionVars) Location() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	return loc
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	if name == WarningCount {
		return strconv.Itoa(s.SysWarningCount), true
	} else if name == ErrorCount {
		return strconv.Itoa(int(s.SysErrorCount)), true
	}
	if val, ok := s.stmtVars[name]; ok {
		return val, ok
	}
	val, ok := s.systems[name]
	return val, ok
}

func (s *SessionVars) setDDLReorgPriority(val string) {
	val = strings.ToLower(val)
	switch val {
	case "priority_low":
		s.DDLReorgPriority = kv.PriorityLow
	case "priority_normal":
		s.DDLReorgPriority = kv.PriorityNormal
	case "priority_high":
		s.DDLReorgPriority = kv.PriorityHigh
	default:
		s.DDLReorgPriority = kv.PriorityLow
	}
}

type planCacheStmtKey string

func (k planCacheStmtKey) Hash() []byte {
	return []byte(k)
}

// AddNonPreparedPlanCacheStmt adds this PlanCacheStmt into non-preapred plan-cache stmt cache
func (s *SessionVars) AddNonPreparedPlanCacheStmt(sql string, stmt interface{}) {
	if s.nonPreparedPlanCacheStmts == nil {
		s.nonPreparedPlanCacheStmts = kvcache.NewSimpleLRUCache(uint(s.NonPreparedPlanCacheSize), 0, 0)
	}
	s.nonPreparedPlanCacheStmts.Put(planCacheStmtKey(sql), stmt)
}

// GetNonPreparedPlanCacheStmt gets the PlanCacheStmt.
func (s *SessionVars) GetNonPreparedPlanCacheStmt(sql string) interface{} {
	if s.nonPreparedPlanCacheStmts == nil {
		return nil
	}
	stmt, _ := s.nonPreparedPlanCacheStmts.Get(planCacheStmtKey(sql))
	return stmt
}

// AddPreparedStmt adds prepareStmt to current session and count in global.
func (s *SessionVars) AddPreparedStmt(stmtID uint32, stmt interface{}) error {
	if _, exists := s.PreparedStmts[stmtID]; !exists {
		maxPreparedStmtCount := MaxPreparedStmtCountValue.Load()
		newPreparedStmtCount := atomic.AddInt64(&PreparedStmtCount, 1)
		if maxPreparedStmtCount >= 0 && newPreparedStmtCount > maxPreparedStmtCount {
			atomic.AddInt64(&PreparedStmtCount, -1)
			return ErrMaxPreparedStmtCountReached.GenWithStackByArgs(maxPreparedStmtCount)
		}
		metrics.PreparedStmtGauge.Set(float64(newPreparedStmtCount))
	}
	s.PreparedStmts[stmtID] = stmt
	return nil
}

// RemovePreparedStmt removes preparedStmt from current session and decrease count in global.
func (s *SessionVars) RemovePreparedStmt(stmtID uint32) {
	_, exists := s.PreparedStmts[stmtID]
	if !exists {
		return
	}
	delete(s.PreparedStmts, stmtID)
	afterMinus := atomic.AddInt64(&PreparedStmtCount, -1)
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// WithdrawAllPreparedStmt remove all preparedStmt in current session and decrease count in global.
func (s *SessionVars) WithdrawAllPreparedStmt() {
	psCount := len(s.PreparedStmts)
	if psCount == 0 {
		return
	}
	afterMinus := atomic.AddInt64(&PreparedStmtCount, -int64(psCount))
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// SetStmtVar sets the value of a system variable temporarily
func (s *SessionVars) setStmtVar(name string, val string) error {
	s.stmtVars[name] = val
	return nil
}

// ClearStmtVars clear temporarily system variables.
func (s *SessionVars) ClearStmtVars() {
	s.stmtVars = make(map[string]string)
}

// GetSessionOrGlobalSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func (s *SessionVars) GetSessionOrGlobalSystemVar(ctx context.Context, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sv.HasNoneScope() {
		return sv.Value, nil
	}
	if sv.HasSessionScope() {
		// Populate the value to s.systems if it is not there already.
		// in future should be already loaded on session init
		if sv.GetSession != nil {
			// shortcut to the getter, we won't use the value
			return sv.GetSessionFromHook(s)
		}
		if _, ok := s.systems[sv.Name]; !ok {
			if sv.HasGlobalScope() {
				if val, err := s.GlobalVarsAccessor.GetGlobalSysVar(sv.Name); err == nil {
					s.systems[sv.Name] = val
				}
			} else {
				s.systems[sv.Name] = sv.Value // no global scope, use default
			}
		}
		return sv.GetSessionFromHook(s)
	}
	return sv.GetGlobalFromHook(ctx, s)
}

// GetSessionStatesSystemVar gets the session variable value for session states.
// It's only used for encoding session states when migrating a session.
// The returned boolean indicates whether to keep this value in the session states.
func (s *SessionVars) GetSessionStatesSystemVar(name string) (string, bool, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	// Call GetStateValue first if it exists. Otherwise, call GetSession.
	if sv.GetStateValue != nil {
		return sv.GetStateValue(s)
	}
	if sv.GetSession != nil {
		val, err := sv.GetSessionFromHook(s)
		return val, err == nil, err
	}
	// Only get the cached value. No need to check the global or default value.
	if val, ok := s.systems[sv.Name]; ok {
		return val, true, nil
	}
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func (s *SessionVars) GetGlobalSystemVar(ctx context.Context, name string) (string, error) {
	sv := GetSysVar(name)
	if sv == nil {
		return "", ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.GetGlobalFromHook(ctx, s)
}

// SetStmtVar sets system variable and updates SessionVars states.
func (s *SessionVars) SetStmtVar(name string, value string) error {
	name = strings.ToLower(name)
	sysVar := GetSysVar(name)
	if sysVar == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	sVal, err := sysVar.Validate(s, value, ScopeSession)
	if err != nil {
		return err
	}
	return s.setStmtVar(name, sVal)
}

// SetSystemVar sets the value of a system variable for session scope.
// Values are automatically normalized (i.e. oN / on / 1 => ON)
// and the validation function is run. To set with less validation, see
// SetSystemVarWithRelaxedValidation.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	val, err := sv.Validate(s, val, ScopeSession)
	if err != nil {
		return err
	}
	return sv.SetSessionFromHook(s, val)
}

// SetSystemVarWithoutValidation sets the value of a system variable for session scope.
// Deprecated: Values are NOT normalized or Validated.
func (s *SessionVars) SetSystemVarWithoutValidation(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.SetSessionFromHook(s, val)
}

// SetSystemVarWithRelaxedValidation sets the value of a system variable for session scope.
// Validation functions are called, but scope validation is skipped.
// Errors are not expected to be returned because this could cause upgrade issues.
func (s *SessionVars) SetSystemVarWithRelaxedValidation(name string, val string) error {
	sv := GetSysVar(name)
	if sv == nil {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	val = sv.ValidateWithRelaxedValidation(s, val, ScopeSession)
	return sv.SetSessionFromHook(s, val)
}

// GetReadableTxnMode returns the session variable TxnMode but rewrites it to "OPTIMISTIC" when it's empty.
func (s *SessionVars) GetReadableTxnMode() string {
	txnMode := s.TxnMode
	if txnMode == "" {
		txnMode = ast.Optimistic
	}
	return txnMode
}

// SetPrevStmtDigest sets the digest of the previous statement.
func (s *SessionVars) SetPrevStmtDigest(prevStmtDigest string) {
	s.prevStmtDigest = prevStmtDigest
}

// GetPrevStmtDigest returns the digest of the previous statement.
func (s *SessionVars) GetPrevStmtDigest() string {
	// Because `prevStmt` may be truncated, so it's senseless to normalize it.
	// Even if `prevStmtDigest` is empty but `prevStmt` is not, just return it anyway.
	return s.prevStmtDigest
}

// LazyCheckKeyNotExists returns if we can lazy check key not exists.
func (s *SessionVars) LazyCheckKeyNotExists() bool {
	return s.PresumeKeyNotExists || (s.TxnCtx != nil && s.TxnCtx.IsPessimistic && !s.StmtCtx.DupKeyAsWarning)
}

// GetTemporaryTable returns a TempTable by tableInfo.
func (s *SessionVars) GetTemporaryTable(tblInfo *model.TableInfo) tableutil.TempTable {
	if tblInfo.TempTableType != model.TempTableNone {
		if s.TxnCtx.TemporaryTables == nil {
			s.TxnCtx.TemporaryTables = make(map[int64]tableutil.TempTable)
		}
		tempTables := s.TxnCtx.TemporaryTables
		tempTable, ok := tempTables[tblInfo.ID]
		if !ok {
			tempTable = tableutil.TempTableFromMeta(tblInfo)
			tempTables[tblInfo.ID] = tempTable
		}
		return tempTable
	}

	return nil
}

// EncodeSessionStates saves session states into SessionStates.
func (s *SessionVars) EncodeSessionStates(ctx context.Context, sessionStates *sessionstates.SessionStates) (err error) {
	// Encode user-defined variables.
	s.userVars.lock.RLock()
	sessionStates.UserVars = make(map[string]*types.Datum, len(s.userVars.values))
	sessionStates.UserVarTypes = make(map[string]*ptypes.FieldType, len(s.userVars.types))
	for name, userVar := range s.userVars.values {
		sessionStates.UserVars[name] = userVar.Clone()
	}
	for name, userVarType := range s.userVars.types {
		sessionStates.UserVarTypes[name] = userVarType.Clone()
	}
	s.userVars.lock.RUnlock()

	// Encode other session contexts.
	sessionStates.PreparedStmtID = s.preparedStmtID
	sessionStates.Status = s.Status
	sessionStates.CurrentDB = s.CurrentDB
	sessionStates.LastTxnInfo = s.LastTxnInfo
	if s.LastQueryInfo.StartTS != 0 {
		sessionStates.LastQueryInfo = &s.LastQueryInfo
	}
	if s.LastDDLInfo.SeqNum != 0 {
		sessionStates.LastDDLInfo = &s.LastDDLInfo
	}
	sessionStates.LastFoundRows = s.LastFoundRows
	sessionStates.SequenceLatestValues = s.SequenceState.GetAllStates()
	sessionStates.FoundInPlanCache = s.PrevFoundInPlanCache
	sessionStates.FoundInBinding = s.PrevFoundInBinding

	// Encode StatementContext. We encode it here to avoid circle dependency.
	sessionStates.LastAffectedRows = s.StmtCtx.PrevAffectedRows
	sessionStates.LastInsertID = s.StmtCtx.PrevLastInsertID
	sessionStates.Warnings = s.StmtCtx.GetWarnings()
	return
}

// DecodeSessionStates restores session states from SessionStates.
func (s *SessionVars) DecodeSessionStates(ctx context.Context, sessionStates *sessionstates.SessionStates) (err error) {
	// Decode user-defined variables.
	for name, userVar := range sessionStates.UserVars {
		s.SetUserVarVal(name, *userVar.Clone())
	}
	for name, userVarType := range sessionStates.UserVarTypes {
		s.SetUserVarType(name, userVarType.Clone())
	}

	// Decode other session contexts.
	s.preparedStmtID = sessionStates.PreparedStmtID
	s.Status = sessionStates.Status
	s.CurrentDB = sessionStates.CurrentDB
	s.LastTxnInfo = sessionStates.LastTxnInfo
	if sessionStates.LastQueryInfo != nil {
		s.LastQueryInfo = *sessionStates.LastQueryInfo
	}
	if sessionStates.LastDDLInfo != nil {
		s.LastDDLInfo = *sessionStates.LastDDLInfo
	}
	s.LastFoundRows = sessionStates.LastFoundRows
	s.SequenceState.SetAllStates(sessionStates.SequenceLatestValues)
	s.FoundInPlanCache = sessionStates.FoundInPlanCache
	s.FoundInBinding = sessionStates.FoundInBinding

	// Decode StatementContext.
	s.StmtCtx.SetAffectedRows(uint64(sessionStates.LastAffectedRows))
	s.StmtCtx.PrevLastInsertID = sessionStates.LastInsertID
	s.StmtCtx.SetWarnings(sessionStates.Warnings)
	return
}

// TableDelta stands for the changed count for one table or partition.
type TableDelta struct {
	Delta    int64
	Count    int64
	ColSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
	TableID  int64
}

// Clone returns a cloned TableDelta.
func (td TableDelta) Clone() TableDelta {
	colSize := make(map[int64]int64, len(td.ColSize))
	maps.Copy(colSize, td.ColSize)
	return TableDelta{
		Delta:    td.Delta,
		Count:    td.Count,
		ColSize:  colSize,
		InitTime: td.InitTime,
		TableID:  td.TableID,
	}
}

// ConcurrencyUnset means the value the of the concurrency related variable is unset.
const ConcurrencyUnset = -1

// Concurrency defines concurrency values.
type Concurrency struct {
	// indexLookupConcurrency is the number of concurrent index lookup worker.
	// indexLookupConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupConcurrency int

	// indexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	// indexLookupJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupJoinConcurrency int

	// distSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	distSQLScanConcurrency int

	// hashJoinConcurrency is the number of concurrent hash join outer worker.
	// hashJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	hashJoinConcurrency int

	// projectionConcurrency is the number of concurrent projection worker.
	// projectionConcurrency is deprecated, use ExecutorConcurrency instead.
	projectionConcurrency int

	// hashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	// hashAggPartialConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggPartialConcurrency int

	// hashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	// hashAggFinalConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggFinalConcurrency int

	// windowConcurrency is the number of concurrent window worker.
	// windowConcurrency is deprecated, use ExecutorConcurrency instead.
	windowConcurrency int

	// mergeJoinConcurrency is the number of concurrent merge join worker
	mergeJoinConcurrency int

	// streamAggConcurrency is the number of concurrent stream aggregation worker.
	// streamAggConcurrency is deprecated, use ExecutorConcurrency instead.
	streamAggConcurrency int

	// indexMergeIntersectionConcurrency is the number of indexMergeProcessWorker
	// Only meaningful for dynamic pruned partition table.
	indexMergeIntersectionConcurrency int

	// indexSerialScanConcurrency is the number of concurrent index serial scan worker.
	indexSerialScanConcurrency int

	// ExecutorConcurrency is the number of concurrent worker for all executors.
	ExecutorConcurrency int

	// SourceAddr is the source address of request. Available in coprocessor ONLY.
	SourceAddr net.TCPAddr
}

// SetIndexLookupConcurrency set the number of concurrent index lookup worker.
func (c *Concurrency) SetIndexLookupConcurrency(n int) {
	c.indexLookupConcurrency = n
}

// SetIndexLookupJoinConcurrency set the number of concurrent index lookup join inner worker.
func (c *Concurrency) SetIndexLookupJoinConcurrency(n int) {
	c.indexLookupJoinConcurrency = n
}

// SetDistSQLScanConcurrency set the number of concurrent dist SQL scan worker.
func (c *Concurrency) SetDistSQLScanConcurrency(n int) {
	c.distSQLScanConcurrency = n
}

// SetHashJoinConcurrency set the number of concurrent hash join outer worker.
func (c *Concurrency) SetHashJoinConcurrency(n int) {
	c.hashJoinConcurrency = n
}

// SetProjectionConcurrency set the number of concurrent projection worker.
func (c *Concurrency) SetProjectionConcurrency(n int) {
	c.projectionConcurrency = n
}

// SetHashAggPartialConcurrency set the number of concurrent hash aggregation partial worker.
func (c *Concurrency) SetHashAggPartialConcurrency(n int) {
	c.hashAggPartialConcurrency = n
}

// SetHashAggFinalConcurrency set the number of concurrent hash aggregation final worker.
func (c *Concurrency) SetHashAggFinalConcurrency(n int) {
	c.hashAggFinalConcurrency = n
}

// SetWindowConcurrency set the number of concurrent window worker.
func (c *Concurrency) SetWindowConcurrency(n int) {
	c.windowConcurrency = n
}

// SetMergeJoinConcurrency set the number of concurrent merge join worker.
func (c *Concurrency) SetMergeJoinConcurrency(n int) {
	c.mergeJoinConcurrency = n
}

// SetStreamAggConcurrency set the number of concurrent stream aggregation worker.
func (c *Concurrency) SetStreamAggConcurrency(n int) {
	c.streamAggConcurrency = n
}

// SetIndexMergeIntersectionConcurrency set the number of concurrent intersection process worker.
func (c *Concurrency) SetIndexMergeIntersectionConcurrency(n int) {
	c.indexMergeIntersectionConcurrency = n
}

// SetIndexSerialScanConcurrency set the number of concurrent index serial scan worker.
func (c *Concurrency) SetIndexSerialScanConcurrency(n int) {
	c.indexSerialScanConcurrency = n
}

// IndexLookupConcurrency return the number of concurrent index lookup worker.
func (c *Concurrency) IndexLookupConcurrency() int {
	if c.indexLookupConcurrency != ConcurrencyUnset {
		return c.indexLookupConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexLookupJoinConcurrency return the number of concurrent index lookup join inner worker.
func (c *Concurrency) IndexLookupJoinConcurrency() int {
	if c.indexLookupJoinConcurrency != ConcurrencyUnset {
		return c.indexLookupJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// DistSQLScanConcurrency return the number of concurrent dist SQL scan worker.
func (c *Concurrency) DistSQLScanConcurrency() int {
	return c.distSQLScanConcurrency
}

// HashJoinConcurrency return the number of concurrent hash join outer worker.
func (c *Concurrency) HashJoinConcurrency() int {
	if c.hashJoinConcurrency != ConcurrencyUnset {
		return c.hashJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// ProjectionConcurrency return the number of concurrent projection worker.
func (c *Concurrency) ProjectionConcurrency() int {
	if c.projectionConcurrency != ConcurrencyUnset {
		return c.projectionConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggPartialConcurrency return the number of concurrent hash aggregation partial worker.
func (c *Concurrency) HashAggPartialConcurrency() int {
	if c.hashAggPartialConcurrency != ConcurrencyUnset {
		return c.hashAggPartialConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggFinalConcurrency return the number of concurrent hash aggregation final worker.
func (c *Concurrency) HashAggFinalConcurrency() int {
	if c.hashAggFinalConcurrency != ConcurrencyUnset {
		return c.hashAggFinalConcurrency
	}
	return c.ExecutorConcurrency
}

// WindowConcurrency return the number of concurrent window worker.
func (c *Concurrency) WindowConcurrency() int {
	if c.windowConcurrency != ConcurrencyUnset {
		return c.windowConcurrency
	}
	return c.ExecutorConcurrency
}

// MergeJoinConcurrency return the number of concurrent merge join worker.
func (c *Concurrency) MergeJoinConcurrency() int {
	if c.mergeJoinConcurrency != ConcurrencyUnset {
		return c.mergeJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// StreamAggConcurrency return the number of concurrent stream aggregation worker.
func (c *Concurrency) StreamAggConcurrency() int {
	if c.streamAggConcurrency != ConcurrencyUnset {
		return c.streamAggConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexMergeIntersectionConcurrency return the number of concurrent process worker.
func (c *Concurrency) IndexMergeIntersectionConcurrency() int {
	if c.indexMergeIntersectionConcurrency != ConcurrencyUnset {
		return c.indexMergeIntersectionConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexSerialScanConcurrency return the number of concurrent index serial scan worker.
// This option is not sync with ExecutorConcurrency since it's used by Analyze table.
func (c *Concurrency) IndexSerialScanConcurrency() int {
	return c.indexSerialScanConcurrency
}

// UnionConcurrency return the num of concurrent union worker.
func (c *Concurrency) UnionConcurrency() int {
	return c.ExecutorConcurrency
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64
	// MemQuotaApplyCache defines the memory capacity for apply cache.
	MemQuotaApplyCache int64
}

// BatchSize defines batch size values.
type BatchSize struct {
	// IndexJoinBatchSize is the batch size of a index lookup join.
	IndexJoinBatchSize int

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// InitChunkSize defines init row count of a Chunk during query execution.
	InitChunkSize int

	// MaxChunkSize defines max row count of a Chunk during query execution.
	MaxChunkSize int

	// MinPagingSize defines the min size used by the coprocessor paging protocol.
	MinPagingSize int

	// MinPagingSize defines the max size used by the coprocessor paging protocol.
	MaxPagingSize int
}

const (
	// SlowLogRowPrefixStr is slow log row prefix.
	SlowLogRowPrefixStr = "# "
	// SlowLogSpaceMarkStr is slow log space mark.
	SlowLogSpaceMarkStr = ": "
	// SlowLogSQLSuffixStr is slow log suffix.
	SlowLogSQLSuffixStr = ";"
	// SlowLogTimeStr is slow log field name.
	SlowLogTimeStr = "Time"
	// SlowLogStartPrefixStr is slow log start row prefix.
	SlowLogStartPrefixStr = SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr
	// SlowLogTxnStartTSStr is slow log field name.
	SlowLogTxnStartTSStr = "Txn_start_ts"
	// SlowLogUserAndHostStr is the user and host field name, which is compatible with MySQL.
	SlowLogUserAndHostStr = "User@Host"
	// SlowLogUserStr is slow log field name.
	SlowLogUserStr = "User"
	// SlowLogHostStr only for slow_query table usage.
	SlowLogHostStr = "Host"
	// SlowLogConnIDStr is slow log field name.
	SlowLogConnIDStr = "Conn_ID"
	// SlowLogQueryTimeStr is slow log field name.
	SlowLogQueryTimeStr = "Query_time"
	// SlowLogParseTimeStr is the parse sql time.
	SlowLogParseTimeStr = "Parse_time"
	// SlowLogCompileTimeStr is the compile plan time.
	SlowLogCompileTimeStr = "Compile_time"
	// SlowLogRewriteTimeStr is the rewrite time.
	SlowLogRewriteTimeStr = "Rewrite_time"
	// SlowLogOptimizeTimeStr is the optimization time.
	SlowLogOptimizeTimeStr = "Optimize_time"
	// SlowLogWaitTSTimeStr is the time of waiting TS.
	SlowLogWaitTSTimeStr = "Wait_TS"
	// SlowLogPreprocSubQueriesStr is the number of pre-processed sub-queries.
	SlowLogPreprocSubQueriesStr = "Preproc_subqueries"
	// SlowLogPreProcSubQueryTimeStr is the total time of pre-processing sub-queries.
	SlowLogPreProcSubQueryTimeStr = "Preproc_subqueries_time"
	// SlowLogDBStr is slow log field name.
	SlowLogDBStr = "DB"
	// SlowLogIsInternalStr is slow log field name.
	SlowLogIsInternalStr = "Is_internal"
	// SlowLogIndexNamesStr is slow log field name.
	SlowLogIndexNamesStr = "Index_names"
	// SlowLogDigestStr is slow log field name.
	SlowLogDigestStr = "Digest"
	// SlowLogQuerySQLStr is slow log field name.
	SlowLogQuerySQLStr = "Query" // use for slow log table, slow log will not print this field name but print sql directly.
	// SlowLogStatsInfoStr is plan stats info.
	SlowLogStatsInfoStr = "Stats"
	// SlowLogNumCopTasksStr is the number of cop-tasks.
	SlowLogNumCopTasksStr = "Num_cop_tasks"
	// SlowLogCopProcAvg is the average process time of all cop-tasks.
	SlowLogCopProcAvg = "Cop_proc_avg"
	// SlowLogCopProcP90 is the p90 process time of all cop-tasks.
	SlowLogCopProcP90 = "Cop_proc_p90"
	// SlowLogCopProcMax is the max process time of all cop-tasks.
	SlowLogCopProcMax = "Cop_proc_max"
	// SlowLogCopProcAddr is the address of TiKV where the cop-task which cost max process time run.
	SlowLogCopProcAddr = "Cop_proc_addr"
	// SlowLogCopWaitAvg is the average wait time of all cop-tasks.
	SlowLogCopWaitAvg = "Cop_wait_avg" // #nosec G101
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90" // #nosec G101
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of TiKV where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr" // #nosec G101
	// SlowLogCopBackoffPrefix contains backoff information.
	SlowLogCopBackoffPrefix = "Cop_backoff_"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"
	// SlowLogDiskMax is the nax number bytes of disk used in this statement.
	SlowLogDiskMax = "Disk_max"
	// SlowLogPrepared is used to indicate whether this sql execute in prepare.
	SlowLogPrepared = "Prepared"
	// SlowLogPlanFromCache is used to indicate whether this plan is from plan cache.
	SlowLogPlanFromCache = "Plan_from_cache"
	// SlowLogPlanFromBinding is used to indicate whether this plan is matched with the hints in the binding.
	SlowLogPlanFromBinding = "Plan_from_binding"
	// SlowLogHasMoreResults is used to indicate whether this sql has more following results.
	SlowLogHasMoreResults = "Has_more_results"
	// SlowLogSucc is used to indicate whether this sql execute successfully.
	SlowLogSucc = "Succ"
	// SlowLogPrevStmt is used to show the previous executed statement.
	SlowLogPrevStmt = "Prev_stmt"
	// SlowLogPlan is used to record the query plan.
	SlowLogPlan = "Plan"
	// SlowLogPlanDigest is used to record the query plan digest.
	SlowLogPlanDigest = "Plan_digest"
	// SlowLogBinaryPlan is used to record the binary plan.
	SlowLogBinaryPlan = "Binary_plan"
	// SlowLogPlanPrefix is the prefix of the plan value.
	SlowLogPlanPrefix = ast.TiDBDecodePlan + "('"
	// SlowLogBinaryPlanPrefix is the prefix of the binary plan value.
	SlowLogBinaryPlanPrefix = ast.TiDBDecodeBinaryPlan + "('"
	// SlowLogPlanSuffix is the suffix of the plan value.
	SlowLogPlanSuffix = "')"
	// SlowLogPrevStmtPrefix is the prefix of Prev_stmt in slow log file.
	SlowLogPrevStmtPrefix = SlowLogPrevStmt + SlowLogSpaceMarkStr
	// SlowLogKVTotal is the total time waiting for kv.
	SlowLogKVTotal = "KV_total"
	// SlowLogPDTotal is the total time waiting for pd.
	SlowLogPDTotal = "PD_total"
	// SlowLogBackoffTotal is the total time doing backoff.
	SlowLogBackoffTotal = "Backoff_total"
	// SlowLogWriteSQLRespTotal is the total time used to write response to client.
	SlowLogWriteSQLRespTotal = "Write_sql_response_total"
	// SlowLogExecRetryCount is the execution retry count.
	SlowLogExecRetryCount = "Exec_retry_count"
	// SlowLogExecRetryTime is the execution retry time.
	SlowLogExecRetryTime = "Exec_retry_time"
	// SlowLogBackoffDetail is the detail of backoff.
	SlowLogBackoffDetail = "Backoff_Detail"
	// SlowLogResultRows is the row count of the SQL result.
	SlowLogResultRows = "Result_rows"
	// SlowLogWarnings is the warnings generated during executing the statement.
	// Note that some extra warnings would also be printed through slow log.
	SlowLogWarnings = "Warnings"
	// SlowLogIsExplicitTxn is used to indicate whether this sql execute in explicit transaction or not.
	SlowLogIsExplicitTxn = "IsExplicitTxn"
	// SlowLogIsWriteCacheTable is used to indicate whether writing to the cache table need to wait for the read lock to expire.
	SlowLogIsWriteCacheTable = "IsWriteCacheTable"
	// SlowLogIsSyncStatsFailed is used to indicate whether any failure happen during sync stats
	SlowLogIsSyncStatsFailed = "IsSyncStatsFailed"
)

// GenerateBinaryPlan decides whether we should record binary plan in slow log and stmt summary.
// It's controlled by the global variable `tidb_generate_binary_plan`.
var GenerateBinaryPlan atomic2.Bool

// JSONSQLWarnForSlowLog helps to print the SQLWarn through the slow log in JSON format.
type JSONSQLWarnForSlowLog struct {
	Level   string
	Message string
	// IsExtra means this SQL Warn is expected to be recorded only under some conditions (like in EXPLAIN) and should
	// haven't been recorded as a warning now, but we recorded it anyway to help diagnostics.
	IsExtra bool `json:",omitempty"`
}

// SlowQueryLogItems is a collection of items that should be included in the
// slow query log.
type SlowQueryLogItems struct {
	TxnTS             uint64
	SQL               string
	Digest            string
	TimeTotal         time.Duration
	TimeParse         time.Duration
	TimeCompile       time.Duration
	TimeOptimize      time.Duration
	TimeWaitTS        time.Duration
	IndexNames        string
	StatsInfos        map[string]uint64
	CopTasks          *stmtctx.CopTasksDetails
	ExecDetail        execdetails.ExecDetails
	MemMax            int64
	DiskMax           int64
	Succ              bool
	Prepared          bool
	PlanFromCache     bool
	PlanFromBinding   bool
	HasMoreResults    bool
	PrevStmt          string
	Plan              string
	PlanDigest        string
	BinaryPlan        string
	RewriteInfo       RewritePhaseInfo
	KVTotal           time.Duration
	PDTotal           time.Duration
	BackoffTotal      time.Duration
	WriteSQLRespTotal time.Duration
	ExecRetryCount    uint
	ExecRetryTime     time.Duration
	ResultRows        int64
	IsExplicitTxn     bool
	IsWriteCacheTable bool
	// table -> name -> status
	StatsLoadStatus   map[string]map[string]string
	IsSyncStatsFailed bool
	Warnings          []JSONSQLWarnForSlowLog
}

// SlowLogFormat uses for formatting slow log.
// The slow log output is like below:
// # Time: 2019-04-28T15:24:04.309074+08:00
// # Txn_start_ts: 406315658548871171
// # User@Host: root[root] @ localhost [127.0.0.1]
// # Conn_ID: 6
// # Query_time: 4.895492
// # Process_time: 0.161 Request_count: 1 Total_keys: 100001 Processed_keys: 100000
// # DB: test
// # Index_names: [t1.idx1,t2.idx2]
// # Is_internal: false
// # Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # Stats: t1:1,t2:2
// # Num_cop_tasks: 10
// # Cop_process: Avg_time: 1s P90_time: 2s Max_time: 3s Max_addr: 10.6.131.78
// # Cop_wait: Avg_time: 10ms P90_time: 20ms Max_time: 30ms Max_Addr: 10.6.131.79
// # Memory_max: 4096
// # Disk_max: 65535
// # Succ: true
// # Prev_stmt: begin;
// select * from t_slim;
func (s *SessionVars) SlowLogFormat(logItems *SlowQueryLogItems) string {
	var buf bytes.Buffer

	writeSlowLogItem(&buf, SlowLogTxnStartTSStr, strconv.FormatUint(logItems.TxnTS, 10))
	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, SlowLogUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	if s.ConnectionID != 0 {
		writeSlowLogItem(&buf, SlowLogConnIDStr, strconv.FormatUint(s.ConnectionID, 10))
	}
	if logItems.ExecRetryCount > 0 {
		buf.WriteString(SlowLogRowPrefixStr)
		buf.WriteString(SlowLogExecRetryTime)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.FormatFloat(logItems.ExecRetryTime.Seconds(), 'f', -1, 64))
		buf.WriteString(" ")
		buf.WriteString(SlowLogExecRetryCount)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.Itoa(int(logItems.ExecRetryCount)))
		buf.WriteString("\n")
	}
	writeSlowLogItem(&buf, SlowLogQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v", SlowLogRewriteTimeStr,
		SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	if logItems.RewriteInfo.PreprocessSubQueries > 0 {
		buf.WriteString(fmt.Sprintf(" %v%v%v %v%v%v", SlowLogPreprocSubQueriesStr, SlowLogSpaceMarkStr, logItems.RewriteInfo.PreprocessSubQueries,
			SlowLogPreProcSubQueryTimeStr, SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationPreprocessSubQuery.Seconds(), 'f', -1, 64)))
	}
	buf.WriteString("\n")

	writeSlowLogItem(&buf, SlowLogOptimizeTimeStr, strconv.FormatFloat(logItems.TimeOptimize.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogWaitTSTimeStr, strconv.FormatFloat(logItems.TimeWaitTS.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.String(); len(execDetailStr) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, SlowLogDBStr, strings.ToLower(s.CurrentDB))
	}
	if len(logItems.IndexNames) > 0 {
		writeSlowLogItem(&buf, SlowLogIndexNamesStr, logItems.IndexNames)
	}

	writeSlowLogItem(&buf, SlowLogIsInternalStr, strconv.FormatBool(s.InRestrictedSQL))
	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, SlowLogDigestStr, logItems.Digest)
	}
	if len(logItems.StatsInfos) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + SlowLogStatsInfoStr + SlowLogSpaceMarkStr)
		firstComma := false
		vStr := ""
		for k, v := range logItems.StatsInfos {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
			if v != 0 && len(logItems.StatsLoadStatus[k]) > 0 {
				writeStatsLoadStatusItems(&buf, logItems.StatsLoadStatus[k])
			}
		}
		buf.WriteString("\n")
	}
	if logItems.CopTasks != nil {
		writeSlowLogItem(&buf, SlowLogNumCopTasksStr, strconv.FormatInt(int64(logItems.CopTasks.NumCopTasks), 10))
		if logItems.CopTasks.NumCopTasks > 0 {
			// make the result stable
			backoffs := make([]string, 0, 3)
			for backoff := range logItems.CopTasks.TotBackoffTimes {
				backoffs = append(backoffs, backoff)
			}
			slices.Sort(backoffs)

			if logItems.CopTasks.NumCopTasks == 1 {
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v",
					SlowLogCopProcAvg, SlowLogSpaceMarkStr, logItems.CopTasks.AvgProcessTime.Seconds(),
					SlowLogCopProcAddr, SlowLogSpaceMarkStr, logItems.CopTasks.MaxProcessAddress) + "\n")
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v",
					SlowLogCopWaitAvg, SlowLogSpaceMarkStr, logItems.CopTasks.AvgWaitTime.Seconds(),
					SlowLogCopWaitAddr, SlowLogSpaceMarkStr, logItems.CopTasks.MaxWaitAddress) + "\n")
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTime[backoff].Seconds(),
					))
				}
			} else {
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
					SlowLogCopProcAvg, SlowLogSpaceMarkStr, logItems.CopTasks.AvgProcessTime.Seconds(),
					SlowLogCopProcP90, SlowLogSpaceMarkStr, logItems.CopTasks.P90ProcessTime.Seconds(),
					SlowLogCopProcMax, SlowLogSpaceMarkStr, logItems.CopTasks.MaxProcessTime.Seconds(),
					SlowLogCopProcAddr, SlowLogSpaceMarkStr, logItems.CopTasks.MaxProcessAddress) + "\n")
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
					SlowLogCopWaitAvg, SlowLogSpaceMarkStr, logItems.CopTasks.AvgWaitTime.Seconds(),
					SlowLogCopWaitP90, SlowLogSpaceMarkStr, logItems.CopTasks.P90WaitTime.Seconds(),
					SlowLogCopWaitMax, SlowLogSpaceMarkStr, logItems.CopTasks.MaxWaitTime.Seconds(),
					SlowLogCopWaitAddr, SlowLogSpaceMarkStr, logItems.CopTasks.MaxWaitAddress) + "\n")
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v %v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, logItems.CopTasks.TotBackoffTime[backoff].Seconds(),
						backoffPrefix+"max_time", SlowLogSpaceMarkStr, logItems.CopTasks.MaxBackoffTime[backoff].Seconds(),
						backoffPrefix+"max_addr", SlowLogSpaceMarkStr, logItems.CopTasks.MaxBackoffAddress[backoff],
						backoffPrefix+"avg_time", SlowLogSpaceMarkStr, logItems.CopTasks.AvgBackoffTime[backoff].Seconds(),
						backoffPrefix+"p90_time", SlowLogSpaceMarkStr, logItems.CopTasks.P90BackoffTime[backoff].Seconds(),
					))
				}
			}
		}
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, SlowLogMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, SlowLogDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, SlowLogPrepared, strconv.FormatBool(logItems.Prepared))
	writeSlowLogItem(&buf, SlowLogPlanFromCache, strconv.FormatBool(logItems.PlanFromCache))
	writeSlowLogItem(&buf, SlowLogPlanFromBinding, strconv.FormatBool(logItems.PlanFromBinding))
	writeSlowLogItem(&buf, SlowLogHasMoreResults, strconv.FormatBool(logItems.HasMoreResults))
	writeSlowLogItem(&buf, SlowLogKVTotal, strconv.FormatFloat(logItems.KVTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogPDTotal, strconv.FormatFloat(logItems.PDTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogBackoffTotal, strconv.FormatFloat(logItems.BackoffTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogWriteSQLRespTotal, strconv.FormatFloat(logItems.WriteSQLRespTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogResultRows, strconv.FormatInt(logItems.ResultRows, 10))
	if len(logItems.Warnings) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + SlowLogWarnings + SlowLogSpaceMarkStr)
		jsonEncoder := json.NewEncoder(&buf)
		jsonEncoder.SetEscapeHTML(false)
		// Note that the Encode() will append a '\n' so we don't need to add another.
		err := jsonEncoder.Encode(logItems.Warnings)
		if err != nil {
			buf.WriteString(err.Error())
		}
	}
	writeSlowLogItem(&buf, SlowLogSucc, strconv.FormatBool(logItems.Succ))
	writeSlowLogItem(&buf, SlowLogIsExplicitTxn, strconv.FormatBool(logItems.IsExplicitTxn))
	writeSlowLogItem(&buf, SlowLogIsSyncStatsFailed, strconv.FormatBool(logItems.IsSyncStatsFailed))
	if s.StmtCtx.WaitLockLeaseTime > 0 {
		writeSlowLogItem(&buf, SlowLogIsWriteCacheTable, strconv.FormatBool(logItems.IsWriteCacheTable))
	}
	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, SlowLogPlan, logItems.Plan)
	}
	if len(logItems.PlanDigest) != 0 {
		writeSlowLogItem(&buf, SlowLogPlanDigest, logItems.PlanDigest)
	}
	if len(logItems.BinaryPlan) != 0 {
		writeSlowLogItem(&buf, SlowLogBinaryPlan, logItems.BinaryPlan)
	}
	if logItems.PrevStmt != "" {
		writeSlowLogItem(&buf, SlowLogPrevStmt, logItems.PrevStmt)
	}

	if s.CurrentDBChanged {
		buf.WriteString(fmt.Sprintf("use %s;\n", strings.ToLower(s.CurrentDB)))
		s.CurrentDBChanged = false
	}

	buf.WriteString(logItems.SQL)
	if len(logItems.SQL) == 0 || logItems.SQL[len(logItems.SQL)-1] != ';' {
		buf.WriteString(";")
	}

	return buf.String()
}

func writeStatsLoadStatusItems(buf *bytes.Buffer, loadStatus map[string]string) {
	if len(loadStatus) > 0 {
		buf.WriteString("[")
		firstComma := false
		for name, status := range loadStatus {
			if firstComma {
				buf.WriteString("," + name + ":" + status)
			} else {
				buf.WriteString(name + ":" + status)
				firstComma = true
			}
		}
		buf.WriteString("]")
	}
}

// writeSlowLogItem writes a slow log item in the form of: "# ${key}:${value}"
func writeSlowLogItem(buf *bytes.Buffer, key, value string) {
	buf.WriteString(SlowLogRowPrefixStr + key + SlowLogSpaceMarkStr + value + "\n")
}

// TxnReadTS indicates the value and used situation for tx_read_ts
type TxnReadTS struct {
	readTS uint64
	used   bool
}

// NewTxnReadTS creates TxnReadTS
func NewTxnReadTS(ts uint64) *TxnReadTS {
	return &TxnReadTS{
		readTS: ts,
		used:   false,
	}
}

// UseTxnReadTS returns readTS, and mark used as true
func (t *TxnReadTS) UseTxnReadTS() uint64 {
	if t == nil {
		return 0
	}
	t.used = true
	return t.readTS
}

// SetTxnReadTS update readTS, and refresh used
func (t *TxnReadTS) SetTxnReadTS(ts uint64) {
	if t == nil {
		return
	}
	t.used = false
	t.readTS = ts
}

// PeakTxnReadTS returns readTS
func (t *TxnReadTS) PeakTxnReadTS() uint64 {
	if t == nil {
		return 0
	}
	return t.readTS
}

// CleanupTxnReadTSIfUsed cleans txnReadTS if used
func (s *SessionVars) CleanupTxnReadTSIfUsed() {
	if s.TxnReadTS == nil {
		return
	}
	if s.TxnReadTS.used && s.TxnReadTS.readTS > 0 {
		s.TxnReadTS = NewTxnReadTS(0)
		s.SnapshotInfoschema = nil
	}
}

// GetCPUFactor returns the session variable cpuFactor
func (s *SessionVars) GetCPUFactor() float64 {
	return s.cpuFactor
}

// GetCopCPUFactor returns the session variable copCPUFactor
func (s *SessionVars) GetCopCPUFactor() float64 {
	return s.copCPUFactor
}

// GetMemoryFactor returns the session variable memoryFactor
func (s *SessionVars) GetMemoryFactor() float64 {
	return s.memoryFactor
}

// GetDiskFactor returns the session variable diskFactor
func (s *SessionVars) GetDiskFactor() float64 {
	return s.diskFactor
}

// GetConcurrencyFactor returns the session variable concurrencyFactor
func (s *SessionVars) GetConcurrencyFactor() float64 {
	return s.concurrencyFactor
}

// GetNetworkFactor returns the session variable networkFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetNetworkFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.networkFactor
}

// GetScanFactor returns the session variable scanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.scanFactor
}

// GetDescScanFactor returns the session variable descScanFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetDescScanFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.descScanFactor
}

// GetSeekFactor returns the session variable seekFactor
// returns 0 when tbl is a temporary table.
func (s *SessionVars) GetSeekFactor(tbl *model.TableInfo) float64 {
	if tbl != nil {
		if tbl.TempTableType != model.TempTableNone {
			return 0
		}
	}
	return s.seekFactor
}

// EnableEvalTopNEstimationForStrMatch means if we need to evaluate expression with TopN to improve estimation.
// Currently, it's only for string matching functions (like and regexp).
func (s *SessionVars) EnableEvalTopNEstimationForStrMatch() bool {
	return s.DefaultStrMatchSelectivity == 0
}

// GetStrMatchDefaultSelectivity means the default selectivity for like and regexp.
// Note: 0 is a special value, which means the default selectivity is 0.1 and TopN assisted estimation is enabled.
func (s *SessionVars) GetStrMatchDefaultSelectivity() float64 {
	if s.DefaultStrMatchSelectivity == 0 {
		return 0.1
	}
	return s.DefaultStrMatchSelectivity
}

// GetNegateStrMatchDefaultSelectivity means the default selectivity for not like and not regexp.
// Note:
//
//	  0 is a special value, which means the default selectivity is 0.9 and TopN assisted estimation is enabled.
//	  0.8 (the default value) is also a special value. For backward compatibility, when the variable is set to 0.8, we
//	keep the default selectivity of like/regexp and not like/regexp all 0.8.
func (s *SessionVars) GetNegateStrMatchDefaultSelectivity() float64 {
	if s.DefaultStrMatchSelectivity == DefTiDBDefaultStrMatchSelectivity {
		return DefTiDBDefaultStrMatchSelectivity
	}
	return 1 - s.GetStrMatchDefaultSelectivity()
}

// GetRelatedTableForMDL gets the related table for metadata lock.
func (s *SessionVars) GetRelatedTableForMDL() *sync.Map {
	s.TxnCtx.tdmLock.Lock()
	defer s.TxnCtx.tdmLock.Unlock()
	if s.TxnCtx.relatedTableForMDL == nil {
		s.TxnCtx.relatedTableForMDL = new(sync.Map)
	}
	return s.TxnCtx.relatedTableForMDL
}

// EnableForceInlineCTE returns the session variable enableForceInlineCTE
func (s *SessionVars) EnableForceInlineCTE() bool {
	return s.enableForceInlineCTE
}

// protectedTSList implements util/processinfo#ProtectedTSList
type protectedTSList struct {
	sync.Mutex
	items map[uint64]int
}

// HoldTS holds the timestamp to prevent its data from being GCed.
func (lst *protectedTSList) HoldTS(ts uint64) (unhold func()) {
	lst.Lock()
	if lst.items == nil {
		lst.items = map[uint64]int{}
	}
	lst.items[ts] += 1
	lst.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			lst.Lock()
			if lst.items != nil {
				if lst.items[ts] > 1 {
					lst.items[ts] -= 1
				} else {
					delete(lst.items, ts)
				}
			}
			lst.Unlock()
		})
	}
}

// GetMinProtectedTS returns the minimum protected timestamp that greater than `lowerBound` (0 if no such one).
func (lst *protectedTSList) GetMinProtectedTS(lowerBound uint64) (ts uint64) {
	lst.Lock()
	for k, v := range lst.items {
		if v > 0 && k > lowerBound && (k < ts || ts == 0) {
			ts = k
		}
	}
	lst.Unlock()
	return
}

// Size returns the number of protected timestamps (exported for test).
func (lst *protectedTSList) Size() (size int) {
	lst.Lock()
	size = len(lst.items)
	lst.Unlock()
	return
}
