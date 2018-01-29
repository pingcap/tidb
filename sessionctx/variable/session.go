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
	"math"
	"sync"
	"time"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
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
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(tableID int64, delta int64, count int64) {
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[tableID]
	item.Delta += delta
	item.Count += count
	tc.TableDeltaMap[tableID] = item
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.TableDeltaMap = nil
}

// SessionVars is to handle user-defined or global variables in the current session.
type SessionVars struct {
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]string
	// Systems are system variables.
	Systems map[string]string
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32

	// retry information
	RetryInfo *RetryInfo
	// Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// Following variables are special for current session.

	Status           uint16
	PrevLastInsertID uint64 // PrevLastInsertID is the last insert ID of previous statement.
	LastInsertID     uint64 // LastInsertID is the auto-generated ID in the current statement.
	InsertID         uint64 // InsertID is the given insert ID of an auto_increment column.

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

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
	StmtCtx *StatementContext

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

	// SkipConstraintCheck is true when importing data.
	SkipConstraintCheck bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BuildStatsConcurrencyVar is used to control statistics building concurrency.
	BuildStatsConcurrencyVar int

	// IndexJoinBatchSize is the batch size of a index lookup join.
	IndexJoinBatchSize int

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// IndexLookupConcurrency is the number of concurrent index lookup worker.
	IndexLookupConcurrency int

	// DistSQLScanConcurrency is the number of concurrent dist SQL scan worker.
	DistSQLScanConcurrency int

	// IndexSerialScanConcurrency is the number of concurrent index serial scan worker.
	IndexSerialScanConcurrency int

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// DMLBatchSize indicates the size of batches for DML.
	// It will be used when BatchInsert or BatchDelete is on.
	DMLBatchSize int

	// MaxRowCountForINLJ defines max row count that the outer table of index nested loop join could be without force hint.
	MaxRowCountForINLJ int
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	return &SessionVars{
		Users:                      make(map[string]string),
		Systems:                    make(map[string]string),
		PreparedStmts:              make(map[uint32]interface{}),
		PreparedStmtNameToID:       make(map[string]uint32),
		TxnCtx:                     &TransactionContext{},
		RetryInfo:                  &RetryInfo{},
		StrictSQLMode:              true,
		Status:                     mysql.ServerStatusAutocommit,
		StmtCtx:                    new(StatementContext),
		AllowAggPushDown:           false,
		BuildStatsConcurrencyVar:   DefBuildStatsConcurrency,
		IndexJoinBatchSize:         DefIndexJoinBatchSize,
		IndexLookupSize:            DefIndexLookupSize,
		IndexLookupConcurrency:     DefIndexLookupConcurrency,
		IndexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		DistSQLScanConcurrency:     DefDistSQLScanConcurrency,
		MaxRowCountForINLJ:         DefMaxRowCountForINLJ,
		DMLBatchSize:               DefDMLBatchSize,
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
	charset = s.Systems[CharacterSetConnection]
	collation = s.Systems[CollationConnection]
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
	s.Status &= (^flag)
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

// special session variables.
const (
	SQLModeVar          = "sql_mode"
	AutocommitVar       = "autocommit"
	CharacterSetResults = "character_set_results"
	MaxAllowedPacket    = "max_allowed_packet"
	TimeZone            = "time_zone"
	TxnIsolation        = "tx_isolation"
)

// TableDelta stands for the changed count for one table.
type TableDelta struct {
	Delta int64
	Count int64
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution

	InInsertStmt           bool
	InUpdateOrDeleteStmt   bool
	InSelectStmt           bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex
		affectedRows uint64
		foundRows    uint64
		warnings     []error
	}

	// Copied from SessionVars.TimeZone.
	TimeZone     *time.Location
	Priority     mysql.PriorityEnum
	NotFillCache bool
	BatchCheck   bool
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.affectedRows += rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.affectedRows
	sc.mu.Unlock()
	return rows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.foundRows
	sc.mu.Unlock()
	return rows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.foundRows += rows
	sc.mu.Unlock()
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []error {
	sc.mu.Lock()
	warns := make([]error, len(sc.mu.warnings))
	copy(warns, sc.mu.warnings)
	sc.mu.Unlock()
	return warns
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	sc.mu.Lock()
	wc := uint16(len(sc.mu.warnings))
	sc.mu.Unlock()
	return wc
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []error) {
	sc.mu.Lock()
	sc.mu.warnings = warns
	sc.mu.Unlock()
}

// AppendWarning appends a warning.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, warn)
	}
	sc.mu.Unlock()
}

// HandleTruncate ignores or returns the error based on the StatementContext state.
func (sc *StatementContext) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}
	if sc.IgnoreTruncate {
		return nil
	}
	if sc.TruncateAsWarning {
		sc.AppendWarning(err)
		return nil
	}
	return err
}

// HandleOverflow treats ErrOverflow as warnings or returns the error based on the StmtCtx.OverflowAsWarning state.
func (sc *StatementContext) HandleOverflow(err error, warnErr error) error {
	if err == nil {
		return nil
	}

	if sc.OverflowAsWarning {
		sc.AppendWarning(warnErr)
		return nil
	}
	return err
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.mu.Lock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.warnings = nil
	sc.mu.Unlock()
}

// MostRestrictStateContext gets a most restrict StatementContext.
func MostRestrictStateContext() *StatementContext {
	return &StatementContext{
		IgnoreTruncate:    false,
		OverflowAsWarning: false,
		TruncateAsWarning: false,
		TimeZone:          time.UTC,
	}
}
