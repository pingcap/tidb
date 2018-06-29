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
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/auth"
)

const (
	codeCantGetValidID terror.ErrCode = 1
	codeCantSetToNull  terror.ErrCode = 2
	codeSnapshotTooOld terror.ErrCode = 3
)

// Error instances.
var (
	errCantGetValidID = terror.ClassVariable.New(codeCantGetValidID, "cannot get valid auto-increment id in retry")
	ErrCantSetToNull  = terror.ClassVariable.New(codeCantSetToNull, "cannot set variable to null")
	ErrSnapshotTooOld = terror.ClassVariable.New(codeSnapshotTooOld, "snapshot is older than GC safe point %s")
)

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	currRetryOff           int
	autoIncrementIDs       []int64
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.currRetryOff = 0
	if len(r.autoIncrementIDs) > 0 {
		r.autoIncrementIDs = r.autoIncrementIDs[:0]
	}
	if len(r.DroppedPreparedStmtIDs) > 0 {
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// AddAutoIncrementID adds id to AutoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs = append(r.autoIncrementIDs, id)
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.currRetryOff = 0
}

// GetCurrAutoIncrementID gets current AutoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	if r.currRetryOff >= len(r.autoIncrementIDs) {
		return 0, errCantGetValidID
	}
	id := r.autoIncrementIDs[r.currRetryOff]
	r.currRetryOff++

	return id, nil
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	ForUpdate     bool
	DirtyDB       interface{}
	Binlog        interface{}
	InfoSchema    interface{}
	Histroy       interface{}
	SchemaVersion int64
	StartTS       uint64
	Shard         *int64
	TableDeltaMap map[int64]TableDelta

	// For metrics.
	CreateTime     time.Time
	StatementCount int
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(tableID int64, delta int64, count int64, colSize map[int64]int64) {
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[tableID]
	if item.ColSize == nil && colSize != nil {
		item.ColSize = make(map[int64]int64)
	}
	item.Delta += delta
	item.Count += count
	for key, val := range colSize {
		item.ColSize[key] += val
	}
	tc.TableDeltaMap[tableID] = item
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.TableDeltaMap = nil
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// BufStore stores temp KVs for a row when executing insert statement.
	// We could reuse a BufStore for multiple rows of a session to reduce memory allocations.
	BufStore *kv.BufferStore
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.BufStore = nil
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

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
	Users map[string]string
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// params for prepared statements
	PreparedParams []interface{}

	// retry information
	RetryInfo *RetryInfo
	// Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *kv.Variables

	// TxnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	TxnIsolationLevelOneShot struct {
		// state 0 means default
		// state 1 means it's set in current transaction.
		// state 2 means it should be used in current transaction.
		State int
		Value string
	}

	// Following variables are special for current session.

	Status           uint16
	PrevLastInsertID uint64 // PrevLastInsertID is the last insert ID of previous statement.
	LastInsertID     uint64 // LastInsertID is the auto-generated ID in the current statement.
	InsertID         uint64 // InsertID is the given insert ID of an auto_increment column.
	// PrevAffectedRows is the affected-rows value(DDL is 0, DML is the number of affected rows).
	PrevAffectedRows int64

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanCacheEnabled stores the global config "plan-cache-enabled", and it will be only updated in tests.
	PlanCacheEnabled bool

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
	BinlogClient interface{}

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowInSubqueryUnFolding can be set to true to fold in subquery
	AllowInSubqueryUnFolding bool

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues interface{}

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	/* TiDB system variables */

	// ImportingData is true when importing data.
	ImportingData bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// IDAllocator is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Table.alloc.
	IDAllocator autoid.Allocator

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in planner.
	OptimizerSelectivityLevel int

	// EnableTablePartition enables table partition feature.
	EnableTablePartition bool

	// EnableStreaming indicates whether the coprocessor request can use streaming API.
	// TODO: remove this after tidb-server configuration "enable-streaming' removed.
	EnableStreaming bool

	writeStmtBufs WriteStmtBufs
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{
		Users:                     make(map[string]string),
		systems:                   make(map[string]string),
		PreparedStmts:             make(map[uint32]interface{}),
		PreparedStmtNameToID:      make(map[string]uint32),
		PreparedParams:            make([]interface{}, 10),
		TxnCtx:                    &TransactionContext{},
		KVVars:                    kv.NewVariables(),
		RetryInfo:                 &RetryInfo{},
		StrictSQLMode:             true,
		Status:                    mysql.ServerStatusAutocommit,
		StmtCtx:                   new(stmtctx.StatementContext),
		AllowAggPushDown:          false,
		OptimizerSelectivityLevel: DefTiDBOptimizerSelectivityLevel,
		RetryLimit:                DefTiDBRetryLimit,
		DisableTxnAutoRetry:       DefTiDBDisableTxnAutoRetry,
	}
	vars.Concurrency = Concurrency{
		IndexLookupConcurrency:     DefIndexLookupConcurrency,
		IndexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		IndexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		HashJoinConcurrency:        DefTiDBHashJoinConcurrency,
		ProjectionConcurrency:      DefTiDBProjectionConcurrency,
		DistSQLScanConcurrency:     DefDistSQLScanConcurrency,
		HashAggPartialConcurrency:  DefTiDBHashAggPartialConcurrency,
		HashAggFinalConcurrency:    DefTiDBHashAggFinalConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:             config.GetGlobalConfig().MemQuotaQuery,
		MemQuotaHashJoin:          DefTiDBMemQuotaHashJoin,
		MemQuotaMergeJoin:         DefTiDBMemQuotaMergeJoin,
		MemQuotaSort:              DefTiDBMemQuotaSort,
		MemQuotaTopn:              DefTiDBMemQuotaTopn,
		MemQuotaIndexLookupReader: DefTiDBMemQuotaIndexLookupReader,
		MemQuotaIndexLookupJoin:   DefTiDBMemQuotaIndexLookupJoin,
		MemQuotaNestedLoopApply:   DefTiDBMemQuotaNestedLoopApply,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		MaxChunkSize:       DefMaxChunkSize,
		DMLBatchSize:       DefDMLBatchSize,
	}
	var enableStreaming string
	if config.GetGlobalConfig().EnableStreaming {
		enableStreaming = "1"
	} else {
		enableStreaming = "0"
	}
	vars.PlanCacheEnabled = config.GetGlobalConfig().PlanCache.Enabled
	terror.Log(vars.SetSystemVar(TiDBEnableStreaming, enableStreaming))
	return vars
}

// GetWriteStmtBufs get pointer of SessionVars.writeStmtBufs.
func (s *SessionVars) GetWriteStmtBufs() *WriteStmtBufs {
	return &s.writeStmtBufs
}

// CleanBuffers cleans the temporary bufs
func (s *SessionVars) CleanBuffers() {
	if !s.ImportingData {
		s.GetWriteStmtBufs().clean()
	}
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

// SetLastInsertID saves the last insert id to the session context.
// TODO: we may store the result for last_insert_id sys var later.
func (s *SessionVars) SetLastInsertID(insertID uint64) {
	s.LastInsertID = insertID
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

// GetNextPreparedStmtID generates and returns the next session scope prepared statement id.
func (s *SessionVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// GetTimeZone returns the value of time_zone session variable.
func (s *SessionVars) GetTimeZone() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = time.Local
	}
	return loc
}

// ResetPrevAffectedRows reset the prev-affected-rows variable.
func (s *SessionVars) ResetPrevAffectedRows() {
	s.PrevAffectedRows = 0
	if s.StmtCtx != nil {
		if s.StmtCtx.InUpdateOrDeleteStmt || s.StmtCtx.InInsertStmt {
			s.PrevAffectedRows = int64(s.StmtCtx.AffectedRows())
		} else if s.StmtCtx.InSelectStmt {
			s.PrevAffectedRows = -1
		}
	}
}

// GetSystemVar gets the string value of a system variable.
func (s *SessionVars) GetSystemVar(name string) (string, bool) {
	val, ok := s.systems[name]
	return val, ok
}

// deleteSystemVar deletes a system variable.
func (s *SessionVars) deleteSystemVar(name string) error {
	if name != CharacterSetResults {
		return ErrCantSetToNull
	}
	delete(s.systems, name)
	return nil
}

// SetSystemVar sets the value of a system variable.
func (s *SessionVars) SetSystemVar(name string, val string) error {
	switch name {
	case TxnIsolationOneShot:
		s.TxnIsolationLevelOneShot.State = 1
		s.TxnIsolationLevelOneShot.Value = val
	case TimeZone:
		tz, err := parseTimeZone(val)
		if err != nil {
			return errors.Trace(err)
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
			return errors.Trace(err)
		}
	case AutocommitVar:
		isAutocommit := TiDBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetStatusFlag(mysql.ServerStatusInTrans, false)
		}
	case TiDBImportingData:
		s.ImportingData = TiDBOptOn(val)
	case TiDBSkipUTF8Check:
		s.SkipUTF8Check = TiDBOptOn(val)
	case TiDBOptAggPushDown:
		s.AllowAggPushDown = TiDBOptOn(val)
	case TiDBOptInSubqUnFolding:
		s.AllowInSubqueryUnFolding = TiDBOptOn(val)
	case TiDBIndexLookupConcurrency:
		s.IndexLookupConcurrency = tidbOptPositiveInt32(val, DefIndexLookupConcurrency)
	case TiDBIndexLookupJoinConcurrency:
		s.IndexLookupJoinConcurrency = tidbOptPositiveInt32(val, DefIndexLookupJoinConcurrency)
	case TiDBIndexJoinBatchSize:
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, DefIndexJoinBatchSize)
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
	case TiDBDistSQLScanConcurrency:
		s.DistSQLScanConcurrency = tidbOptPositiveInt32(val, DefDistSQLScanConcurrency)
	case TiDBIndexSerialScanConcurrency:
		s.IndexSerialScanConcurrency = tidbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
	case TiDBBackoffLockFast:
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, kv.DefBackoffLockFast)
	case TiDBBatchInsert:
		s.BatchInsert = TiDBOptOn(val)
	case TiDBBatchDelete:
		s.BatchDelete = TiDBOptOn(val)
	case TiDBDMLBatchSize:
		s.DMLBatchSize = tidbOptPositiveInt32(val, DefDMLBatchSize)
	case TiDBCurrentTS, TiDBConfig:
		return ErrReadOnly
	case TiDBMaxChunkSize:
		s.MaxChunkSize = tidbOptPositiveInt32(val, DefMaxChunkSize)
	case TIDBMemQuotaQuery:
		s.MemQuotaQuery = tidbOptInt64(val, config.GetGlobalConfig().MemQuotaQuery)
	case TIDBMemQuotaHashJoin:
		s.MemQuotaHashJoin = tidbOptInt64(val, DefTiDBMemQuotaHashJoin)
	case TIDBMemQuotaMergeJoin:
		s.MemQuotaMergeJoin = tidbOptInt64(val, DefTiDBMemQuotaMergeJoin)
	case TIDBMemQuotaSort:
		s.MemQuotaSort = tidbOptInt64(val, DefTiDBMemQuotaSort)
	case TIDBMemQuotaTopn:
		s.MemQuotaTopn = tidbOptInt64(val, DefTiDBMemQuotaTopn)
	case TIDBMemQuotaIndexLookupReader:
		s.MemQuotaIndexLookupReader = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupReader)
	case TIDBMemQuotaIndexLookupJoin:
		s.MemQuotaIndexLookupJoin = tidbOptInt64(val, DefTiDBMemQuotaIndexLookupJoin)
	case TIDBMemQuotaNestedLoopApply:
		s.MemQuotaNestedLoopApply = tidbOptInt64(val, DefTiDBMemQuotaNestedLoopApply)
	case TiDBGeneralLog:
		atomic.StoreUint32(&ProcessGeneralLog, uint32(tidbOptPositiveInt32(val, DefTiDBGeneralLog)))
	case TiDBRetryLimit:
		s.RetryLimit = tidbOptInt64(val, DefTiDBRetryLimit)
	case TiDBDisableTxnAutoRetry:
		s.DisableTxnAutoRetry = TiDBOptOn(val)
	case TiDBEnableStreaming:
		s.EnableStreaming = TiDBOptOn(val)
	case TiDBOptimizerSelectivityLevel:
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, DefTiDBOptimizerSelectivityLevel)
	case TiDBEnableTablePartition:
		s.EnableTablePartition = TiDBOptOn(val)
	case TiDBDDLReorgWorkerCount:
		workerCnt := tidbOptPositiveInt32(val, DefTiDBDDLReorgWorkerCount)
		SetDDLReorgWorkerCounter(int32(workerCnt))
	}
	s.systems[name] = val
	return nil
}

// special session variables.
const (
	SQLModeVar          = "sql_mode"
	AutocommitVar       = "autocommit"
	CharacterSetResults = "character_set_results"
	MaxAllowedPacket    = "max_allowed_packet"
	TimeZone            = "time_zone"
	TxnIsolation        = "tx_isolation"
	TxnIsolationOneShot = "tx_isolation_one_shot"
)

// TableDelta stands for the changed count for one table.
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

	// HashAggPartialConcurrency is the number of concurrent hash aggregation final worker.
	HashAggFinalConcurrency int

	// IndexSerialScanConcurrency is the number of concurrent index serial scan worker.
	IndexSerialScanConcurrency int
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64
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

	// MaxChunkSize defines max row count of a Chunk during query execution.
	MaxChunkSize int
}
