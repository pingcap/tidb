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
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/cpuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/v4/config"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/meta/autoid"
	"github.com/pingcap/tidb/v4/metrics"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/collate"
	"github.com/pingcap/tidb/v4/util/execdetails"
	"github.com/pingcap/tidb/v4/util/logutil"
	"github.com/pingcap/tidb/v4/util/rowcodec"
	"github.com/pingcap/tidb/v4/util/storeutil"
	"github.com/pingcap/tidb/v4/util/stringutil"
	"github.com/pingcap/tidb/v4/util/timeutil"
)

var preparedStmtCount int64

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
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
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	return r.autoIncrementIDs.getCurrent()
}

// AddAutoRandomID adds id to autoRandomIDs.
func (r *RetryInfo) AddAutoRandomID(id int64) {
	r.autoRandomIDs.autoIDs = append(r.autoRandomIDs.autoIDs, id)
}

// GetCurrAutoRandomID gets current AutoRandomID.
func (r *RetryInfo) GetCurrAutoRandomID() (int64, error) {
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

func (r *retryInfoAutoIDs) getCurrent() (int64, error) {
	if r.currentOffset >= len(r.autoIDs) {
		return 0, errCantGetValidID
	}
	id := r.autoIDs[r.currentOffset]
	r.currentOffset++
	return id, nil
}

// stmtFuture is used to async get timestamp for statement.
type stmtFuture struct {
	future   oracle.Future
	cachedTS uint64
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	forUpdateTS   uint64
	stmtFuture    oracle.Future
	DirtyDB       interface{}
	Binlog        interface{}
	InfoSchema    interface{}
	History       interface{}
	SchemaVersion int64
	StartTS       uint64
	Shard         *int64
	// TableDeltaMap is used in the schema validator for DDL changes in one table not to block others.
	// It's also used in the statistias updating.
	// Note: for the partitionted table, it stores all the partition IDs.
	TableDeltaMap map[int64]TableDelta

	// unchangedRowKeys is used to store the unchanged rows that needs to lock for pessimistic transaction.
	unchangedRowKeys map[string]struct{}

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte

	// CreateTime For metrics.
	CreateTime     time.Time
	StatementCount int
	ForUpdate      bool
	CouldRetry     bool
	IsPessimistic  bool
	Isolation      string
	LockExpire     uint32
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
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[physicalTableID]
	if item.ColSize == nil && colSize != nil {
		item.ColSize = make(map[int64]int64, len(colSize))
	}
	item.Delta += delta
	item.Count += count
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
	tc.DirtyDB = nil
	tc.Binlog = nil
	tc.History = nil
	tc.TableDeltaMap = nil
	tc.pessimisticLockCache = nil
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.TableDeltaMap = nil
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

const (
	// oneShotDef means default, that is tx_isolation_one_shot not set.
	oneShotDef txnIsolationLevelOneShotState = iota
	// oneShotSet means it's set in current transaction.
	oneShotSet
	// onsShotUse means it should be used in current transaction.
	oneShotUse
)

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	Concurrency
	MemQuota
	BatchSize
	RetryLimit          int64
	DisableTxnAutoRetry bool
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]types.Datum
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
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
	PreparedParams PreparedParams

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	RetryInfo *RetryInfo
	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *kv.Variables

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

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// CurrentDB is the default database of this session.
	CurrentDB string

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this session.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the session is handling restricted SQL execution.
	InRestrictedSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

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

	// AllowDistinctAggPushDown can be set true to allow agg with distinct push down to tikv/tiflash.
	AllowDistinctAggPushDown bool

	// AllowWriteRowID can be set to false to forbid write data to _tidb_rowid.
	// This variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowBatchCop means if we should send batch coprocessor to TiFlash. Default value is 1, means to use batch cop in case of aggregation and join.
	// If value is set to 2 , which means to force to send batch cop for any query. Value is set to 0 means never use batch cop.
	AllowBatchCop int

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// CPUFactor is the CPU cost of processing one expression for one row.
	CPUFactor float64
	// CopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	CopCPUFactor float64
	// NetworkFactor is the network cost of transferring 1 byte data.
	NetworkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash.
	ScanFactor float64
	// DescScanFactor is the IO cost of scanning 1 byte data on TiKV and TiFlash in desc order.
	DescScanFactor float64
	// SeekFactor is the IO cost of seeking the start value of a range in TiKV or TiFlash.
	SeekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	/* TiDB system variables */

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

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DDLReorgPriority is the operation priority of adding indices.
	DDLReorgPriority int

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	// EnableStreaming indicates whether the coprocessor request can use streaming API.
	// TODO: remove this after tidb-server configuration "enable-streaming' removed.
	EnableStreaming bool

	// EnableChunkRPC indicates whether the coprocessor request can use chunk API.
	EnableChunkRPC bool

	writeStmtBufs WriteStmtBufs

	// L2CacheSize indicates the size of CPU L2 cache, using byte as unit.
	L2CacheSize int

	// EnableRadixJoin indicates whether to use radix hash join to execute
	// HashJoin.
	EnableRadixJoin bool

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

	// use noop funcs or not
	EnableNoopFuncs bool

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

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

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAndAgg bool

	// EnableIndexMerge enables the generation of IndexMergePath.
	enableIndexMerge bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead kv.ReplicaReadType

	// IsolationReadEngines is used to isolation read, tidb only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[kv.StoreType]struct{}

	PlannerSelectBlockAsName []ast.HintTable

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	// negative value means nowait, 0 means default behavior, others means actual wait time
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
	ConnectionID      uint32
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
		systems:                     make(map[string]string),
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
		OptimizerSelectivityLevel:   DefTiDBOptimizerSelectivityLevel,
		RetryLimit:                  DefTiDBRetryLimit,
		DisableTxnAutoRetry:         DefTiDBDisableTxnAutoRetry,
		DDLReorgPriority:            kv.PriorityLow,
		allowInSubqToJoinAndAgg:     DefOptInSubqToJoinAndAgg,
		CorrelationThreshold:        DefOptCorrelationThreshold,
		CorrelationExpFactor:        DefOptCorrelationExpFactor,
		CPUFactor:                   DefOptCPUFactor,
		CopCPUFactor:                DefOptCopCPUFactor,
		NetworkFactor:               DefOptNetworkFactor,
		ScanFactor:                  DefOptScanFactor,
		DescScanFactor:              DefOptDescScanFactor,
		SeekFactor:                  DefOptSeekFactor,
		MemoryFactor:                DefOptMemoryFactor,
		DiskFactor:                  DefOptDiskFactor,
		ConcurrencyFactor:           DefOptConcurrencyFactor,
		EnableRadixJoin:             false,
		EnableVectorizedExpression:  DefEnableVectorizedExpression,
		L2CacheSize:                 cpuid.CPU.Cache.L2,
		CommandValue:                uint32(mysql.ComSleep),
		TiDBOptJoinReorderThreshold: DefTiDBOptJoinReorderThreshold,
		SlowQueryFile:               config.GetGlobalConfig().Log.SlowQueryFile,
		WaitSplitRegionFinish:       DefTiDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:      DefWaitSplitRegionTimeout,
		enableIndexMerge:            false,
		EnableNoopFuncs:             DefTiDBEnableNoopFuncs,
		replicaRead:                 kv.ReplicaReadLeader,
		AllowRemoveAutoInc:          DefTiDBAllowRemoveAutoInc,
		UsePlanBaselines:            DefTiDBUsePlanBaselines,
		EvolvePlanBaselines:         DefTiDBEvolvePlanBaselines,
		IsolationReadEngines:        map[kv.StoreType]struct{}{kv.TiKV: {}, kv.TiFlash: {}, kv.TiDB: {}},
		LockWaitTimeout:             DefInnodbLockWaitTimeout * 1000,
		MetricSchemaStep:            DefTiDBMetricSchemaStep,
		MetricSchemaRangeDuration:   DefTiDBMetricSchemaRangeDuration,
		SequenceState:               NewSequenceState(),
		WindowingUseHighPrecision:   true,
	}
	vars.KVVars = kv.NewVariables(&vars.Killed)
	vars.Concurrency = Concurrency{
		IndexLookupConcurrency:     DefIndexLookupConcurrency,
		IndexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		IndexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		HashJoinConcurrency:        DefTiDBHashJoinConcurrency,
		ProjectionConcurrency:      DefTiDBProjectionConcurrency,
		DistSQLScanConcurrency:     DefDistSQLScanConcurrency,
		HashAggPartialConcurrency:  DefTiDBHashAggPartialConcurrency,
		HashAggFinalConcurrency:    DefTiDBHashAggFinalConcurrency,
		WindowConcurrency:          DefTiDBWindowConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery: config.GetGlobalConfig().MemQuotaQuery,

		// The variables below do not take any effect anymore, it's remaining for compatibility.
		// TODO: remove them in v4.1
		MemQuotaHashJoin:          DefTiDBMemQuotaHashJoin,
		MemQuotaMergeJoin:         DefTiDBMemQuotaMergeJoin,
		MemQuotaSort:              DefTiDBMemQuotaSort,
		MemQuotaTopn:              DefTiDBMemQuotaTopn,
		MemQuotaIndexLookupReader: DefTiDBMemQuotaIndexLookupReader,
		MemQuotaIndexLookupJoin:   DefTiDBMemQuotaIndexLookupJoin,
		MemQuotaNestedLoopApply:   DefTiDBMemQuotaNestedLoopApply,
		MemQuotaDistSQL:           DefTiDBMemQuotaDistSQL,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		InitChunkSize:      DefInitChunkSize,
		MaxChunkSize:       DefMaxChunkSize,
		DMLBatchSize:       DefDMLBatchSize,
	}
	var enableStreaming string
	if config.GetGlobalConfig().EnableStreaming {
		enableStreaming = "1"
	} else {
		enableStreaming = "0"
	}
	terror.Log(vars.SetSystemVar(TiDBEnableStreaming, enableStreaming))

	vars.AllowBatchCop = DefTiDBAllowBatchCop

	var enableChunkRPC string
	if config.GetGlobalConfig().TiKVClient.EnableChunkRPC {
		enableChunkRPC = "1"
	} else {
		enableChunkRPC = "0"
	}
	terror.Log(vars.SetSystemVar(TiDBEnableChunkRPC, enableChunkRPC))
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

// SetUserVar set the value and collation for user defined variable.
func (s *SessionVars) SetUserVar(varName string, svalue string, collation string) {
	if len(collation) > 0 {
		s.Users[varName] = types.NewCollationStringDatum(stringutil.Copy(svalue), collation, collate.DefaultLen)
	} else {
		_, collation = s.GetCharsetInfo()
		s.Users[varName] = types.NewCollationStringDatum(stringutil.Copy(svalue), collation, collate.DefaultLen)
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

// InTxn returns if the session is in transaction.
func (s *SessionVars) InTxn() bool {
	return s.GetStatusFlag(mysql.ServerStatusInTrans)
}

// IsAutocommit returns if the session is set to autocommit.
func (s *SessionVars) IsAutocommit() bool {
	return s.GetStatusFlag(mysql.ServerStatusAutocommit)
}

// IsReadConsistencyTxn if true it means the transaction is an read consistency (read committed) transaction.
func (s *SessionVars) IsReadConsistencyTxn() bool {
	if s.TxnCtx.Isolation != "" {
		return s.TxnCtx.Isolation == ast.ReadCommitted
	}
	if s.txnIsolationLevelOneShot.state == oneShotUse {
		s.TxnCtx.Isolation = s.txnIsolationLevelOneShot.value
	}
	if s.TxnCtx.Isolation == "" {
		s.TxnCtx.Isolation, _ = s.GetSystemVar(TxnIsolation)
	}
	return s.TxnCtx.Isolation == ast.ReadCommitted
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
	return s.TxnCtx.IsPessimistic && s.IsReadConsistencyTxn()
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
		newPreparedStmtCount := atomic.AddInt64(&preparedStmtCount, 1)
		if maxPreparedStmtCount >= 0 && newPreparedStmtCount > maxPreparedStmtCount {
			atomic.AddInt64(&preparedStmtCount, -1)
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
	afterMinus := atomic.AddInt64(&preparedStmtCount, -1)
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// WithdrawAllPreparedStmt remove all preparedStmt in current session and decrease count in global.
func (s *SessionVars) WithdrawAllPreparedStmt() {
	psCount := len(s.PreparedStmts)
	if psCount == 0 {
		return
	}
	afterMinus := atomic.AddInt64(&preparedStmtCount, -int64(psCount))
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// SetSystemVar sets the value of a system variable.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	switch name {
	case TxnIsolationOneShot:
		switch val {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			skipIsolationLevelCheck, err := GetSessionSystemVar(s, TiDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(val)
			if err != nil {
				returnErr = err
			}
			if !TiDBOptOn(skipIsolationLevelCheck) || err != nil {
				return returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			if name == "transaction_isolation" {
				s.StmtCtx.AppendWarning(returnErr)
			}
		}
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
	case TimeZone:
		tz, err := parseTimeZone(val)
		if err != nil {
			return err
		}
		s.TimeZone = tz
	case SQLModeVar:
		val = mysql.FormatSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := mysql.GetSQLMode(val)
		if err2 != nil {
			return errors.Trace(err2)
		}
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case TiDBSnapshot:
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
	case AutoCommit:
		isAutocommit := TiDBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case AutoIncrementIncrement:
		// AutoIncrementIncrement is valid in [1, 65535].
		temp := tidbOptPositiveInt32(val, DefAutoIncrementIncrement)
		s.AutoIncrementIncrement = adjustAutoIncrementParameter(temp)
	case AutoIncrementOffset:
		// AutoIncrementOffset is valid in [1, 65535].
		temp := tidbOptPositiveInt32(val, DefAutoIncrementOffset)
		s.AutoIncrementOffset = adjustAutoIncrementParameter(temp)
	case MaxExecutionTime:
		timeoutMS := tidbOptPositiveInt32(val, 0)
		s.MaxExecutionTime = uint64(timeoutMS)
	case InnodbLockWaitTimeout:
		lockWaitSec := tidbOptInt64(val, DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = int64(lockWaitSec * 1000)
	case WindowingUseHighPrecision:
		s.WindowingUseHighPrecision = TiDBOptOn(val)
	case TiDBSkipUTF8Check:
		s.SkipUTF8Check = TiDBOptOn(val)
	case TiDBOptAggPushDown:
		s.AllowAggPushDown = TiDBOptOn(val)
	case TiDBOptDistinctAggPushDown:
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
	case TiDBOptWriteRowID:
		s.AllowWriteRowID = TiDBOptOn(val)
	case TiDBOptInSubqToJoinAndAgg:
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
	case TiDBOptCorrelationThreshold:
		s.CorrelationThreshold = tidbOptFloat64(val, DefOptCorrelationThreshold)
	case TiDBOptCorrelationExpFactor:
		s.CorrelationExpFactor = int(tidbOptInt64(val, DefOptCorrelationExpFactor))
	case TiDBOptCPUFactor:
		s.CPUFactor = tidbOptFloat64(val, DefOptCPUFactor)
	case TiDBOptCopCPUFactor:
		s.CopCPUFactor = tidbOptFloat64(val, DefOptCopCPUFactor)
	case TiDBOptNetworkFactor:
		s.NetworkFactor = tidbOptFloat64(val, DefOptNetworkFactor)
	case TiDBOptScanFactor:
		s.ScanFactor = tidbOptFloat64(val, DefOptScanFactor)
	case TiDBOptDescScanFactor:
		s.DescScanFactor = tidbOptFloat64(val, DefOptDescScanFactor)
	case TiDBOptSeekFactor:
		s.SeekFactor = tidbOptFloat64(val, DefOptSeekFactor)
	case TiDBOptMemoryFactor:
		s.MemoryFactor = tidbOptFloat64(val, DefOptMemoryFactor)
	case TiDBOptDiskFactor:
		s.DiskFactor = tidbOptFloat64(val, DefOptDiskFactor)
	case TiDBOptConcurrencyFactor:
		s.ConcurrencyFactor = tidbOptFloat64(val, DefOptConcurrencyFactor)
	case TiDBIndexLookupConcurrency:
		s.IndexLookupConcurrency = tidbOptPositiveInt32(val, DefIndexLookupConcurrency)
	case TiDBIndexLookupJoinConcurrency:
		s.IndexLookupJoinConcurrency = tidbOptPositiveInt32(val, DefIndexLookupJoinConcurrency)
	case TiDBIndexJoinBatchSize:
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, DefIndexJoinBatchSize)
	case TiDBAllowBatchCop:
		s.AllowBatchCop = int(tidbOptInt64(val, DefTiDBAllowBatchCop))
	case TiDBIndexLookupSize:
		s.IndexLookupSize = tidbOptPositiveInt32(val, DefIndexLookupSize)
	case TiDBHashJoinConcurrency:
		s.HashJoinConcurrency = tidbOptPositiveInt32(val, DefTiDBHashJoinConcurrency)
	case TiDBProjectionConcurrency:
		s.ProjectionConcurrency = tidbOptInt64(val, DefTiDBProjectionConcurrency)
	case TiDBHashAggPartialConcurrency:
		s.HashAggPartialConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggPartialConcurrency)
	case TiDBHashAggFinalConcurrency:
		s.HashAggFinalConcurrency = tidbOptPositiveInt32(val, DefTiDBHashAggFinalConcurrency)
	case TiDBWindowConcurrency:
		s.WindowConcurrency = tidbOptPositiveInt32(val, DefTiDBWindowConcurrency)
	case TiDBDistSQLScanConcurrency:
		s.DistSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
	case TiDBIndexSerialScanConcurrency:
		s.IndexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
	case TiDBBackoffLockFast:
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, kv.DefBackoffLockFast)
	case TiDBBackOffWeight:
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, kv.DefBackOffWeight)
	case TiDBConstraintCheckInPlace:
		s.ConstraintCheckInPlace = TiDBOptOn(val)
	case TiDBBatchInsert:
		s.BatchInsert = TiDBOptOn(val)
	case TiDBBatchDelete:
		s.BatchDelete = TiDBOptOn(val)
	case TiDBBatchCommit:
		s.BatchCommit = TiDBOptOn(val)
	case TiDBDMLBatchSize:
		s.DMLBatchSize = tidbOptPositiveInt32(val, DefDMLBatchSize)
	case TiDBCurrentTS, TiDBConfig:
		return ErrReadOnly
	case TiDBMaxChunkSize:
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
	case TiDBInitChunkSize:
		s.InitChunkSize = tidbOptPositiveInt32(val, DefInitChunkSize)
	case TIDBMemQuotaQuery:
		s.MemQuotaQuery = tidbOptInt64(val, config.GetGlobalConfig().MemQuotaQuery)
	case TIDBMemQuotaHashJoin:
		s.MemQuotaHashJoin = tidbOptInt64(val, DefTiDBMemQuotaHashJoin)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaMergeJoin:
		s.MemQuotaMergeJoin = tidbOptInt64(val, DefTiDBMemQuotaMergeJoin)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaSort:
		s.MemQuotaSort = tidbOptInt64(val, DefTiDBMemQuotaSort)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaTopn:
		s.MemQuotaTopn = tidbOptInt64(val, DefTiDBMemQuotaTopn)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaIndexLookupReader:
		s.MemQuotaIndexLookupReader = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupReader)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaIndexLookupJoin:
		s.MemQuotaIndexLookupJoin = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupJoin)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TIDBMemQuotaNestedLoopApply:
		s.MemQuotaNestedLoopApply = tidbOptInt64(val, DefTiDBMemQuotaNestedLoopApply)
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, TIDBMemQuotaQuery))
	case TiDBGeneralLog:
		atomic.StoreUint32(&ProcessGeneralLog, uint32(tidbOptPositiveInt32(val, DefTiDBGeneralLog)))
	case TiDBPProfSQLCPU:
		EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, DefTiDBPProfSQLCPU)) > 0)
	case TiDBDDLSlowOprThreshold:
		atomic.StoreUint32(&DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, DefTiDBDDLSlowOprThreshold)))
	case TiDBRetryLimit:
		s.RetryLimit = tidbOptInt64(val, DefTiDBRetryLimit)
	case TiDBDisableTxnAutoRetry:
		s.DisableTxnAutoRetry = TiDBOptOn(val)
	case TiDBEnableStreaming:
		s.EnableStreaming = TiDBOptOn(val)
	case TiDBEnableChunkRPC:
		s.EnableChunkRPC = TiDBOptOn(val)
	case TiDBEnableCascadesPlanner:
		s.SetEnableCascadesPlanner(TiDBOptOn(val))
	case TiDBOptimizerSelectivityLevel:
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
	case TiDBEnableTablePartition:
		s.EnableTablePartition = val
	case TiDBDDLReorgPriority:
		s.setDDLReorgPriority(val)
	case TiDBForcePriority:
		atomic.StoreInt32(&ForcePriority, int32(mysql.Str2Priority(val)))
	case TiDBEnableRadixJoin:
		s.EnableRadixJoin = TiDBOptOn(val)
	case TiDBEnableWindowFunction:
		s.EnableWindowFunction = TiDBOptOn(val)
	case TiDBEnableVectorizedExpression:
		s.EnableVectorizedExpression = TiDBOptOn(val)
	case TiDBOptJoinReorderThreshold:
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, DefTiDBOptJoinReorderThreshold)
	case TiDBSlowQueryFile:
		s.SlowQueryFile = val
	case TiDBEnableFastAnalyze:
		s.EnableFastAnalyze = TiDBOptOn(val)
	case TiDBWaitSplitRegionFinish:
		s.WaitSplitRegionFinish = TiDBOptOn(val)
	case TiDBWaitSplitRegionTimeout:
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, DefWaitSplitRegionTimeout))
	case TiDBExpensiveQueryTimeThreshold:
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, DefTiDBExpensiveQueryTimeThreshold)))
	case TiDBTxnMode:
		s.TxnMode = strings.ToUpper(val)
	case TiDBRowFormatVersion:
		formatVersion := int(tidbOptInt64(val, DefTiDBRowFormatV1))
		if formatVersion == DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
	case TiDBLowResolutionTSO:
		s.LowResolutionTSO = TiDBOptOn(val)
	case TiDBEnableIndexMerge:
		s.SetEnableIndexMerge(TiDBOptOn(val))
	case TiDBEnableNoopFuncs:
		s.EnableNoopFuncs = TiDBOptOn(val)
	case TiDBReplicaRead:
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(kv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(kv.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(kv.ReplicaReadLeader)
		}
	case TiDBAllowRemoveAutoInc:
		s.AllowRemoveAutoInc = TiDBOptOn(val)
	// It's a global variable, but it also wants to be cached in server.
	case TiDBMaxDeltaSchemaCount:
		SetMaxDeltaSchemaCount(tidbOptInt64(val, DefTiDBMaxDeltaSchemaCount))
	case TiDBUsePlanBaselines:
		s.UsePlanBaselines = TiDBOptOn(val)
	case TiDBEvolvePlanBaselines:
		s.EvolvePlanBaselines = TiDBOptOn(val)
	case TiDBIsolationReadEngines:
		s.IsolationReadEngines = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case kv.TiKV.Name():
				s.IsolationReadEngines[kv.TiKV] = struct{}{}
			case kv.TiFlash.Name():
				s.IsolationReadEngines[kv.TiFlash] = struct{}{}
			case kv.TiDB.Name():
				s.IsolationReadEngines[kv.TiDB] = struct{}{}
			}
		}
	case TiDBStoreLimit:
		storeutil.StoreLimit.Store(tidbOptInt64(val, DefTiDBStoreLimit))
	case TiDBMetricSchemaStep:
		s.MetricSchemaStep = tidbOptInt64(val, DefTiDBMetricSchemaStep)
	case TiDBMetricSchemaRangeDuration:
		s.MetricSchemaRangeDuration = tidbOptInt64(val, DefTiDBMetricSchemaRangeDuration)
	case CollationConnection, CollationDatabase, CollationServer:
		if _, err := collate.GetCollationByName(val); err != nil {
			return errors.Trace(err)
		}
	case TiDBSlowLogThreshold:
		conf := config.GetGlobalConfig()
		if !conf.EnableDynamicConfig {
			atomic.StoreUint64(&config.GetGlobalConfig().Log.SlowThreshold, uint64(tidbOptInt64(val, logutil.DefaultSlowThreshold)))
		} else {
			s.StmtCtx.AppendWarning(errors.Errorf("cannot update %s when enabling dynamic configs", TiDBSlowLogThreshold))
		}
	case TiDBRecordPlanInSlowLog:
		conf := config.GetGlobalConfig()
		if !conf.EnableDynamicConfig {
			atomic.StoreUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog, uint32(tidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		} else {
			s.StmtCtx.AppendWarning(errors.Errorf("cannot update %s when enabling dynamic configs", TiDBRecordPlanInSlowLog))
		}
	case TiDBEnableSlowLog:
		conf := config.GetGlobalConfig()
		if !conf.EnableDynamicConfig {
			config.GetGlobalConfig().Log.EnableSlowLog = TiDBOptOn(val)
		} else {
			s.StmtCtx.AppendWarning(errors.Errorf("cannot update %s when enabling dynamic configs", TiDBEnableSlowLog))
		}
	case TiDBQueryLogMaxLen:
		conf := config.GetGlobalConfig()
		if !conf.EnableDynamicConfig {
			atomic.StoreUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen, uint64(tidbOptInt64(val, logutil.DefaultQueryLogMaxLen)))
		} else {
			s.StmtCtx.AppendWarning(errors.Errorf("cannot update %s when enabling dynamic configs", TiDBQueryLogMaxLen))
		}
	case TiDBCheckMb4ValueInUTF8:
		conf := config.GetGlobalConfig()
		if !conf.EnableDynamicConfig {
			config.GetGlobalConfig().CheckMb4ValueInUTF8 = TiDBOptOn(val)
		} else {
			s.StmtCtx.AppendWarning(errors.Errorf("cannot update %s when enabling dynamic configs", TiDBCheckMb4ValueInUTF8))
		}
	}
	s.systems[name] = val
	return nil
}

// GetReadableTxnMode returns the session variable TxnMode but rewrites it to "OPTIMISTIC" when it's empty.
func (s *SessionVars) GetReadableTxnMode() string {
	txnMode := s.TxnMode
	if txnMode == "" {
		txnMode = ast.Optimistic
	}
	return txnMode
}

func (s *SessionVars) setTxnMode(val string) error {
	switch strings.ToUpper(val) {
	case ast.Pessimistic:
		s.TxnMode = ast.Pessimistic
	case ast.Optimistic:
		s.TxnMode = ast.Optimistic
	case "":
		s.TxnMode = ""
	default:
		return ErrWrongValueForVar.FastGenByArgs(TiDBTxnMode, val)
	}
	return nil
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

// SetLocalSystemVar sets values of the local variables which in "server" scope.
func SetLocalSystemVar(name string, val string) {
	switch name {
	case TiDBDDLReorgWorkerCount:
		SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)))
	case TiDBDDLReorgBatchSize:
		SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, DefTiDBDDLReorgBatchSize)))
	case TiDBDDLErrorCountLimit:
		SetDDLErrorCountLimit(tidbOptInt64(val, DefTiDBDDLErrorCountLimit))
	}
}

// special session variables.
const (
	SQLModeVar           = "sql_mode"
	CharacterSetResults  = "character_set_results"
	MaxAllowedPacket     = "max_allowed_packet"
	TimeZone             = "time_zone"
	TxnIsolation         = "tx_isolation"
	TransactionIsolation = "transaction_isolation"
	TxnIsolationOneShot  = "tx_isolation_one_shot"
	MaxExecutionTime     = "max_execution_time"
)

// these variables are useless for TiDB, but still need to validate their values for some compatible issues.
// TODO: some more variables need to be added here.
const (
	serverReadOnly = "read_only"
)

var (
	// TxIsolationNames are the valid values of the variable "tx_isolation" or "transaction_isolation".
	TxIsolationNames = map[string]struct{}{
		"READ-UNCOMMITTED": {},
		"READ-COMMITTED":   {},
		"REPEATABLE-READ":  {},
		"SERIALIZABLE":     {},
	}
)

// TableDelta stands for the changed count for one table or partition.
type TableDelta struct {
	Delta    int64
	Count    int64
	ColSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
}

// Concurrency defines concurrency values.
type Concurrency struct {
	// IndexLookupConcurrency is the number of concurrent index lookup worker.
	IndexLookupConcurrency int

	// IndexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	IndexLookupJoinConcurrency int

	// DistSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	DistSQLScanConcurrency int

	// HashJoinConcurrency is the number of concurrent hash join outer worker.
	HashJoinConcurrency int

	// ProjectionConcurrency is the number of concurrent projection worker.
	ProjectionConcurrency int64

	// HashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	HashAggPartialConcurrency int

	// HashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	HashAggFinalConcurrency int

	// WindowConcurrency is the number of concurrent window worker.
	WindowConcurrency int

	// IndexSerialScanConcurrency is the number of concurrent index serial scan worker.
	IndexSerialScanConcurrency int
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64

	// The variables below do not take any effect anymore, it's remaining for compatibility.
	// TODO: remove them in v4.1
	// MemQuotaHashJoin defines the memory quota for a hash join executor.
	MemQuotaHashJoin int64
	// MemQuotaMergeJoin defines the memory quota for a merge join executor.
	MemQuotaMergeJoin int64
	// MemQuotaSort defines the memory quota for a sort executor.
	MemQuotaSort int64
	// MemQuotaTopn defines the memory quota for a top n executor.
	MemQuotaTopn int64
	// MemQuotaIndexLookupReader defines the memory quota for a index lookup reader executor.
	MemQuotaIndexLookupReader int64
	// MemQuotaIndexLookupJoin defines the memory quota for a index lookup join executor.
	MemQuotaIndexLookupJoin int64
	// MemQuotaNestedLoopApply defines the memory quota for a nested loop apply executor.
	MemQuotaNestedLoopApply int64
	// MemQuotaDistSQL defines the memory quota for all operators in DistSQL layer like co-processor and selectResult.
	MemQuotaDistSQL int64
}

// BatchSize defines batch size values.
type BatchSize struct {
	// DMLBatchSize indicates the size of batches for DML.
	// It will be used when BatchInsert or BatchDelete is on.
	DMLBatchSize int

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
	SlowLogCopWaitAvg = "Cop_wait_avg"
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90"
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of TiKV where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr"
	// SlowLogCopBackoffPrefix contains backoff information.
	SlowLogCopBackoffPrefix = "Cop_backoff_"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"
	// SlowLogPrepared is used to indicate whether this sql execute in prepare.
	SlowLogPrepared = "Prepared"
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
)

// SlowQueryLogItems is a collection of items that should be included in the
// slow query log.
type SlowQueryLogItems struct {
	TxnTS          uint64
	SQL            string
	Digest         string
	TimeTotal      time.Duration
	TimeParse      time.Duration
	TimeCompile    time.Duration
	IndexNames     string
	StatsInfos     map[string]uint64
	CopTasks       *stmtctx.CopTasksDetails
	ExecDetail     execdetails.ExecDetails
	MemMax         int64
	Succ           bool
	Prepared       bool
	HasMoreResults bool
	PrevStmt       string
	Plan           string
	PlanDigest     string
}

// SlowLogFormat uses for formatting slow log.
// The slow log output is like below:
// # Time: 2019-04-28T15:24:04.309074+08:00
// # Txn_start_ts: 406315658548871171
// # User: root@127.0.0.1
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
// # Succ: true
// # Prev_stmt: begin;
// select * from t_slim;
func (s *SessionVars) SlowLogFormat(logItems *SlowQueryLogItems) string {
	var buf bytes.Buffer

	writeSlowLogItem(&buf, SlowLogTxnStartTSStr, strconv.FormatUint(logItems.TxnTS, 10))
	if s.User != nil {
		writeSlowLogItem(&buf, SlowLogUserStr, s.User.String())
	}
	if s.ConnectionID != 0 {
		writeSlowLogItem(&buf, SlowLogConnIDStr, strconv.FormatUint(s.ConnectionID, 10))
	}
	writeSlowLogItem(&buf, SlowLogQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.String(); len(execDetailStr) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, SlowLogDBStr, s.CurrentDB)
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

	writeSlowLogItem(&buf, SlowLogPrepared, strconv.FormatBool(logItems.Prepared))
	writeSlowLogItem(&buf, SlowLogHasMoreResults, strconv.FormatBool(logItems.HasMoreResults))
	writeSlowLogItem(&buf, SlowLogSucc, strconv.FormatBool(logItems.Succ))
	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, SlowLogPlan, logItems.Plan)
	}
	if len(logItems.PlanDigest) != 0 {
		writeSlowLogItem(&buf, SlowLogPlanDigest, logItems.PlanDigest)
	}

	if logItems.PrevStmt != "" {
		writeSlowLogItem(&buf, SlowLogPrevStmt, logItems.PrevStmt)
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

// adjustAutoIncrementParameter adjust the increment and offset of AutoIncrement.
// AutoIncrementIncrement / AutoIncrementOffset is valid in [1, 65535].
func adjustAutoIncrementParameter(temp int) int {
	if temp <= 0 {
		return 1
	} else if temp > math.MaxUint16 {
		return math.MaxUint16
	} else {
		return temp
	}
}
