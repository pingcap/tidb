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
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
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
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	utilMath "github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tableutil"
	"github.com/pingcap/tidb/util/timeutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/twmb/murmur3"
	atomic2 "go.uber.org/atomic"
)

// PreparedStmtCount is exported for test.
var PreparedStmtCount int64

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
	LastRcReadTS           uint64
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
	forUpdateTS uint64
	stmtFuture  oracle.Future
	Binlog      interface{}
	InfoSchema  interface{}
	History     interface{}
	StartTS     uint64

	// ShardStep indicates the max size of continuous rowid shard in one transaction.
	ShardStep    int
	shardRemain  int
	currentShard int64
	shardRand    *rand.Rand

	// TableDeltaMap is used in the schema validator for DDL changes in one table not to block others.
	// It's also used in the statistics updating.
	// Note: for the partitioned table, it stores all the partition IDs.
	TableDeltaMap map[int64]TableDelta

	// unchangedRowKeys is used to store the unchanged rows that needs to lock for pessimistic transaction.
	unchangedRowKeys map[string]struct{}

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte
	PessimisticCacheHit  int

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

	// TableDeltaMap lock to prevent potential data race
	tdmLock sync.Mutex

	// TemporaryTables is used to store transaction-specific information for global temporary tables.
	// It can also be stored in sessionCtx with local temporary tables, but it's easier to clean this data after transaction ends.
	TemporaryTables map[int64]tableutil.TempTable

	// CachedTables is not nil if the transaction write on cached table.
	CachedTables map[int64]interface{}

	// Last ts used by read-consistency read.
	LastRcReadTs uint64
}

// GetShard returns the shard prefix for the next `count` rowids.
func (tc *TransactionContext) GetShard(shardRowIDBits uint64, typeBitsLength uint64, reserveSignBit bool, count int) int64 {
	if shardRowIDBits == 0 {
		return 0
	}
	if tc.shardRand == nil {
		tc.shardRand = rand.New(rand.NewSource(int64(tc.StartTS))) // #nosec G404
	}
	if tc.shardRemain <= 0 {
		tc.updateShard()
		tc.shardRemain = tc.ShardStep
	}
	tc.shardRemain -= count

	var signBitLength uint64
	if reserveSignBit {
		signBitLength = 1
	}
	return (tc.currentShard & (1<<shardRowIDBits - 1)) << (typeBitsLength - shardRowIDBits - signBitLength)
}

func (tc *TransactionContext) updateShard() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], tc.shardRand.Uint64())
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
	tc.tdmLock.Unlock()
	tc.pessimisticLockCache = nil
	tc.IsStaleness = false
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

// SetStmtFutureForRC sets the stmtFuture .
func (tc *TransactionContext) SetStmtFutureForRC(future oracle.Future) {
	tc.stmtFuture = future
}

// GetStmtFutureForRC gets the stmtFuture.
func (tc *TransactionContext) GetStmtFutureForRC() oracle.Future {
	return tc.stmtFuture
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
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]types.Datum
	// UserVarTypes stores the FieldType for user variables, it cannot be inferred from Users when Users have not been set yet.
	// It is read/write protected by UsersLock.
	UserVarTypes map[string]*types.FieldType
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// stmtVars variables are temporarily set by SET_VAR hint
	// It only take effect for the duration of a single statement
	stmtVars map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16
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

	// TxnManager is used to manage txn context in session
	TxnManager interface{}

	// KVVars is the variables for KV storage.
	KVVars *tikvstore.Variables

	// txnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	txnIsolationLevelOneShot struct {
		state txnIsolationLevelOneShotState
		value string
	}

	// mppTaskIDAllocator is used to allocate mpp task id for a session.
	mppTaskIDAllocator struct {
		mu     sync.Mutex
		lastTS uint64
		taskID int64
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

	// CPUFactor is the CPU cost of processing one expression for one row.
	CPUFactor float64
	// CopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	CopCPUFactor float64
	// CopTiFlashConcurrencyFactor is the concurrency number of computation in tiflash coprocessor.
	CopTiFlashConcurrencyFactor float64
	// networkFactor is the network cost of transferring 1 byte data.
	networkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash.
	scanFactor float64
	// descScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash in desc order.
	descScanFactor float64
	// seekFactor is the IO cost of seeking the start value of a range in TiKV or TiFlash.
	seekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

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

	// EnableStrictDoubleTypeCheck enables table field double type check.
	EnableStrictDoubleTypeCheck bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// EnableChangeMultiSchema is used to control whether to enable the multi schema change.
	EnableChangeMultiSchema bool

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

	// ConnectionInfo indicates current connection info used by current session, only be lazy assigned by plugin.
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

	// IsolationReadEngines is used to isolation read, tidb only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[kv.StoreType]struct{}

	PlannerSelectBlockAsName []ast.HintTable

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	LockWaitTimeout int64

	// MetricSchemaStep indicates the step when query metric schema.
	MetricSchemaStep int64
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

	// EnableAmendPessimisticTxn indicates if schema change amend is enabled for pessimistic transactions.
	EnableAmendPessimisticTxn bool

	// LastTxnInfo keeps track the info of last committed transaction.
	LastTxnInfo string

	// LastQueryInfo keeps track the info of last query.
	LastQueryInfo QueryInfo

	// LastDDLInfo keeps track the info of last DDL.
	LastDDLInfo LastDDLInfo

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

	// MPPStoreLastFailTime records the lastest fail time that a TiFlash store failed.
	MPPStoreLastFailTime map[string]time.Time

	// MPPStoreFailTTL indicates the duration that protect TiDB from sending task to a new recovered TiFlash.
	MPPStoreFailTTL string

	// ReadStaleness indicates the staleness duration for the following query
	ReadStaleness time.Duration

	// cached is used to optimze the object allocation.
	cached struct {
		curr int8
		data [2]stmtctx.StatementContext
	}

	// Rng stores the rand_seed1 and rand_seed2 for Rand() function
	Rng *utilMath.MysqlRng

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
	// BatchPendingTiFlashCount shows the threshold of pending TiFlash tables when batch adding.
	BatchPendingTiFlashCount int
	// RcReadCheckTS indicates if ts check optimization is enabled for current session.
	RcReadCheckTS bool
}

// InitStatementContext initializes a StatementContext, the object is reused to reduce allocation.
func (s *SessionVars) InitStatementContext() *stmtctx.StatementContext {
	s.cached.curr = (s.cached.curr + 1) % 2
	s.cached.data[s.cached.curr] = stmtctx.StatementContext{}
	return &s.cached.data[s.cached.curr]
}

// AllocMPPTaskID allocates task id for mpp tasks. It will reset the task id if the query's
// startTs is different.
func (s *SessionVars) AllocMPPTaskID(startTS uint64) int64 {
	s.mppTaskIDAllocator.mu.Lock()
	defer s.mppTaskIDAllocator.mu.Unlock()
	if s.mppTaskIDAllocator.lastTS == startTS {
		s.mppTaskIDAllocator.taskID++
		return s.mppTaskIDAllocator.taskID
	}
	s.mppTaskIDAllocator.lastTS = startTS
	s.mppTaskIDAllocator.taskID = 1
	return 1
}

// IsMPPAllowed returns whether mpp execution is allowed.
func (s *SessionVars) IsMPPAllowed() bool {
	return s.allowMPPExecution
}

// IsMPPEnforced returns whether mpp execution is enforced.
func (s *SessionVars) IsMPPEnforced() bool {
	return s.allowMPPExecution && s.enforceMPPExecution
}

// RaiseWarningWhenMPPEnforced will raise a warning when mpp mode is enforced and executing explain statement.
// TODO: Confirm whether this function will be inlined and
// omit the overhead of string construction when calling with false condition.
func (s *SessionVars) RaiseWarningWhenMPPEnforced(warning string) {
	if s.IsMPPEnforced() && s.StmtCtx.InExplainStmt {
		s.StmtCtx.AppendWarning(errors.New(warning))
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

// UseDynamicPartitionPrune indicates whether use new dynamic partition prune.
func (s *SessionVars) UseDynamicPartitionPrune() bool {
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

// ConnectionInfo present connection used by audit.
type ConnectionInfo struct {
	ConnectionID      uint64
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
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

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{
		Users:                       make(map[string]types.Datum),
		UserVarTypes:                make(map[string]*types.FieldType),
		systems:                     make(map[string]string),
		stmtVars:                    make(map[string]string),
		PreparedStmts:               make(map[uint32]interface{}),
		PreparedStmtNameToID:        make(map[string]uint32),
		PreparedParams:              make([]types.Datum, 0, 10),
		TxnCtx:                      &TransactionContext{},
		RetryInfo:                   &RetryInfo{},
		ActiveRoles:                 make([]*auth.RoleIdentity, 0, 10),
		StrictSQLMode:               true,
		AutoIncrementIncrement:      DefAutoIncrementIncrement,
		AutoIncrementOffset:         DefAutoIncrementOffset,
		Status:                      mysql.ServerStatusAutocommit,
		StmtCtx:                     new(stmtctx.StatementContext),
		AllowAggPushDown:            false,
		AllowCartesianBCJ:           DefOptCartesianBCJ,
		MPPOuterJoinFixedBuildSide:  DefOptMPPOuterJoinFixedBuildSide,
		BroadcastJoinThresholdSize:  DefBroadcastJoinThresholdSize,
		BroadcastJoinThresholdCount: DefBroadcastJoinThresholdSize,
		OptimizerSelectivityLevel:   DefTiDBOptimizerSelectivityLevel,
		RetryLimit:                  DefTiDBRetryLimit,
		DisableTxnAutoRetry:         DefTiDBDisableTxnAutoRetry,
		DDLReorgPriority:            kv.PriorityLow,
		allowInSubqToJoinAndAgg:     DefOptInSubqToJoinAndAgg,
		preferRangeScan:             DefOptPreferRangeScan,
		EnableCorrelationAdjustment: DefOptEnableCorrelationAdjustment,
		LimitPushDownThreshold:      DefOptLimitPushDownThreshold,
		CorrelationThreshold:        DefOptCorrelationThreshold,
		CorrelationExpFactor:        DefOptCorrelationExpFactor,
		CPUFactor:                   DefOptCPUFactor,
		CopCPUFactor:                DefOptCopCPUFactor,
		CopTiFlashConcurrencyFactor: DefOptTiFlashConcurrencyFactor,
		networkFactor:               DefOptNetworkFactor,
		scanFactor:                  DefOptScanFactor,
		descScanFactor:              DefOptDescScanFactor,
		seekFactor:                  DefOptSeekFactor,
		MemoryFactor:                DefOptMemoryFactor,
		DiskFactor:                  DefOptDiskFactor,
		ConcurrencyFactor:           DefOptConcurrencyFactor,
		EnableVectorizedExpression:  DefEnableVectorizedExpression,
		CommandValue:                uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold: DefTiDBOptJoinReorderThreshold,
		SlowQueryFile:               config.GetGlobalConfig().Log.SlowQueryFile,
		WaitSplitRegionFinish:       DefTiDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:      DefWaitSplitRegionTimeout,
		enableIndexMerge:            DefTiDBEnableIndexMerge,
		NoopFuncsMode:               TiDBOptOnOffWarn(DefTiDBEnableNoopFuncs),
		replicaRead:                 kv.ReplicaReadLeader,
		AllowRemoveAutoInc:          DefTiDBAllowRemoveAutoInc,
		UsePlanBaselines:            DefTiDBUsePlanBaselines,
		EvolvePlanBaselines:         DefTiDBEvolvePlanBaselines,
		EnableExtendedStats:         false,
		IsolationReadEngines:        make(map[kv.StoreType]struct{}),
		LockWaitTimeout:             DefInnodbLockWaitTimeout * 1000,
		MetricSchemaStep:            DefTiDBMetricSchemaStep,
		MetricSchemaRangeDuration:   DefTiDBMetricSchemaRangeDuration,
		SequenceState:               NewSequenceState(),
		WindowingUseHighPrecision:   true,
		PrevFoundInPlanCache:        DefTiDBFoundInPlanCache,
		FoundInPlanCache:            DefTiDBFoundInPlanCache,
		PrevFoundInBinding:          DefTiDBFoundInBinding,
		FoundInBinding:              DefTiDBFoundInBinding,
		SelectLimit:                 math.MaxUint64,
		AllowAutoRandExplicitInsert: DefTiDBAllowAutoRandExplicitInsert,
		EnableClusteredIndex:        DefTiDBEnableClusteredIndex,
		EnableParallelApply:         DefTiDBEnableParallelApply,
		ShardAllocateStep:           DefTiDBShardAllocateStep,
		EnableChangeMultiSchema:     DefTiDBChangeMultiSchema,
		EnablePointGetCache:         DefTiDBPointGetCache,
		EnableAmendPessimisticTxn:   DefTiDBEnableAmendPessimisticTxn,
		PartitionPruneMode:          *atomic2.NewString(DefTiDBPartitionPruneMode),
		TxnScope:                    kv.NewDefaultTxnScopeVar(),
		EnabledRateLimitAction:      DefTiDBEnableRateLimitAction,
		EnableAsyncCommit:           DefTiDBEnableAsyncCommit,
		Enable1PC:                   DefTiDBEnable1PC,
		GuaranteeLinearizability:    DefTiDBGuaranteeLinearizability,
		AnalyzeVersion:              DefTiDBAnalyzeVersion,
		EnableIndexMergeJoin:        DefTiDBEnableIndexMergeJoin,
		AllowFallbackToTiKV:         make(map[kv.StoreType]struct{}),
		CTEMaxRecursionDepth:        DefCTEMaxRecursionDepth,
		TMPTableSize:                DefTiDBTmpTableMaxSize,
		MPPStoreLastFailTime:        make(map[string]time.Time),
		MPPStoreFailTTL:             DefTiDBMPPStoreFailTTL,
		Rng:                         utilMath.NewWithTime(),
		StatsLoadSyncWait:           StatsLoadSyncWait.Load(),
		EnableLegacyInstanceScope:   DefEnableLegacyInstanceScope,
	}
	vars.KVVars = tikvstore.NewVariables(&vars.Killed)
	vars.Concurrency = Concurrency{
		indexLookupConcurrency:     DefIndexLookupConcurrency,
		indexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		indexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		hashJoinConcurrency:        DefTiDBHashJoinConcurrency,
		projectionConcurrency:      DefTiDBProjectionConcurrency,
		distSQLScanConcurrency:     DefDistSQLScanConcurrency,
		hashAggPartialConcurrency:  DefTiDBHashAggPartialConcurrency,
		hashAggFinalConcurrency:    DefTiDBHashAggFinalConcurrency,
		windowConcurrency:          DefTiDBWindowConcurrency,
		mergeJoinConcurrency:       DefTiDBMergeJoinConcurrency,
		streamAggConcurrency:       DefTiDBStreamAggConcurrency,
		ExecutorConcurrency:        DefExecutorConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:      config.GetGlobalConfig().MemQuotaQuery,
		MemQuotaApplyCache: DefTiDBMemQuotaApplyCache,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		InitChunkSize:      DefInitChunkSize,
		MaxChunkSize:       DefMaxChunkSize,
	}
	vars.DMLBatchSize = DefDMLBatchSize
	vars.AllowBatchCop = DefTiDBAllowBatchCop
	vars.allowMPPExecution = DefTiDBAllowMPPExecution
	vars.HashExchangeWithNewCollation = DefTiDBHashExchangeWithNewCollation
	vars.enforceMPPExecution = DefTiDBEnforceMPPExecution
	vars.MPPStoreFailTTL = DefTiDBMPPStoreFailTTL

	enableChunkRPC := "0"
	if config.GetGlobalConfig().TiKVClient.EnableChunkRPC {
		enableChunkRPC = "1"
	}
	terror.Log(vars.SetSystemVar(TiDBEnableChunkRPC, enableChunkRPC))
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
	return s.replicaRead
}

// SetReplicaRead set SessionVars.replicaRead.
func (s *SessionVars) SetReplicaRead(val kv.ReplicaReadType) {
	s.replicaRead = val
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
	cli, err := GetSessionOrGlobalSystemVar(s, CharacterSetClient)
	if err != nil {
		cli = ""
	}
	return []parser.ParseParam{
		parser.CharsetConnection(chs),
		parser.CollationConnection(coll),
		parser.CharsetClient(cli),
	}
}

// SetUserVar set the value and collation for user defined variable.
func (s *SessionVars) SetUserVar(varName string, svalue string, collation string) {
	if len(collation) > 0 {
		s.Users[varName] = types.NewCollationStringDatum(stringutil.Copy(svalue), collation)
	} else {
		_, collation = s.GetCharsetInfo()
		s.Users[varName] = types.NewCollationStringDatum(stringutil.Copy(svalue), collation)
	}
}

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the session server status variable.
// If on is ture sets the flag in session status,
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

// AddPreparedStmt adds prepareStmt to current session and count in global.
func (s *SessionVars) AddPreparedStmt(stmtID uint32, stmt interface{}) error {
	if _, exists := s.PreparedStmts[stmtID]; !exists {
		valStr, _ := s.GetSystemVar(MaxPreparedStmtCount)
		maxPreparedStmtCount, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			maxPreparedStmtCount = DefMaxPreparedStmtCount
		}
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
func (s *SessionVars) SetStmtVar(name string, val string) error {
	s.stmtVars[name] = val
	return nil
}

// ClearStmtVars clear temporarily system variables.
func (s *SessionVars) ClearStmtVars() {
	s.stmtVars = make(map[string]string)
}

// SetSystemVar sets the value of a system variable for session scope.
// Validation is expected to be performed before calling this function,
// and the values should been normalized.
// i.e. oN / on / 1 => ON
func (s *SessionVars) SetSystemVar(name string, val string) error {
	sv := GetSysVar(name)
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

// TableDelta stands for the changed count for one table or partition.
type TableDelta struct {
	Delta    int64
	Count    int64
	ColSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
	TableID  int64
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
	// distSQLScanConcurrency is deprecated, use ExecutorConcurrency instead.
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
	// SlowLogPlanPrefix is the prefix of the plan value.
	SlowLogPlanPrefix = ast.TiDBDecodePlan + "('"
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
	// SlowLogIsExplicitTxn is used to indicate whether this sql execute in explicit transaction or not.
	SlowLogIsExplicitTxn = "IsExplicitTxn"
	// SlowLogIsWriteCacheTable is used to indicate whether writing to the cache table need to wait for the read lock to expire.
	SlowLogIsWriteCacheTable = "IsWriteCacheTable"
)

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
			sort.Strings(backoffs)

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
	writeSlowLogItem(&buf, SlowLogSucc, strconv.FormatBool(logItems.Succ))
	writeSlowLogItem(&buf, SlowLogIsExplicitTxn, strconv.FormatBool(logItems.IsExplicitTxn))
	if s.StmtCtx.WaitLockLeaseTime > 0 {
		writeSlowLogItem(&buf, SlowLogIsWriteCacheTable, strconv.FormatBool(logItems.IsWriteCacheTable))
	}
	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, SlowLogPlan, logItems.Plan)
	}
	if len(logItems.PlanDigest) != 0 {
		writeSlowLogItem(&buf, SlowLogPlanDigest, logItems.PlanDigest)
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

// writeSlowLogItem writes a slow log item in the form of: "# ${key}:${value}"
func writeSlowLogItem(buf *bytes.Buffer, key, value string) {
	buf.WriteString(SlowLogRowPrefixStr + key + SlowLogSpaceMarkStr + value + "\n")
}

// QueryInfo represents the information of last executed query. It's used to expose information for test purpose.
type QueryInfo struct {
	TxnScope    string `json:"txn_scope"`
	StartTS     uint64 `json:"start_ts"`
	ForUpdateTS uint64 `json:"for_update_ts"`
	ErrMsg      string `json:"error,omitempty"`
}

// LastDDLInfo represents the information of last DDL. It's used to expose information for test purpose.
type LastDDLInfo struct {
	Query  string `json:"query"`
	SeqNum uint64 `json:"seq_num"`
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

// IsRcCheckTsRetryable checks if the current error is retryable for `RcReadCheckTS` path.
func (s *SessionVars) IsRcCheckTsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// The `RCCheckTS` flag of `stmtCtx` is set.
	return s.RcReadCheckTS && s.StmtCtx.RCCheckTS && errors.ErrorEqual(err, kv.ErrWriteConflict)
}
