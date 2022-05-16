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

package stmtctx

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
	// WarnLevelNote represents level "Note" for 'SHOW WARNINGS' syntax.
	WarnLevelNote = "Note"
)

var taskIDAlloc uint64

// AllocateTaskID allocates a new unique ID for a statement execution
func AllocateTaskID() uint64 {
	return atomic.AddUint64(&taskIDAlloc, 1)
}

// SQLWarn relates a sql warning and it's level.
type SQLWarn struct {
	Level string
	Err   error
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution
	StmtHints

	// IsDDLJobInQueue is used to mark whether the DDL job is put into the queue.
	// If IsDDLJobInQueue is true, it means the DDL job is in the queue of storage, and it can be handled by the DDL worker.
	IsDDLJobInQueue        bool
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	InExplainStmt          bool
	InCreateOrAlterStmt    bool
	InPreparedPlanBuilding bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	NoZeroDate             bool
	DupKeyAsWarning        bool
	BadNullAsWarning       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool
	UseCache               bool
	BatchCheck             bool
	InNullRejectCheck      bool
	AllowInvalidDate       bool
	IgnoreNoPartition      bool
	SkipPlanCache          bool
	IgnoreExplainIDSuffix  bool
	SkipUTF8Check          bool
	SkipASCIICheck         bool
	SkipUTF8MB4Check       bool
	// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
	// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
	// in stmtCtx
	IsStaleness bool
	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex

		affectedRows uint64
		foundRows    uint64

		/*
			following variables are ported from 'COPY_INFO' struct of MySQL server source,
			they are used to count rows for INSERT/REPLACE/UPDATE queries:
			  If a row is inserted then the copied variable is incremented.
			  If a row is updated by the INSERT ... ON DUPLICATE KEY UPDATE and the
			     new data differs from the old one then the copied and the updated
			     variables are incremented.
			  The touched variable is incremented if a row was touched by the update part
			     of the INSERT ... ON DUPLICATE KEY UPDATE no matter whether the row
			     was actually changed or not.

			see https://github.com/mysql/mysql-server/blob/d2029238d6d9f648077664e4cdd611e231a6dc14/sql/sql_data_change.h#L60 for more details
		*/
		records uint64
		deleted uint64
		updated uint64
		copied  uint64
		touched uint64

		message        string
		warnings       []SQLWarn
		errorCount     uint16
		execDetails    execdetails.ExecDetails
		allExecDetails []*execdetails.ExecDetails
	}
	// PrevAffectedRows is the affected-rows value(DDL is 0, DML is the number of affected rows).
	PrevAffectedRows int64
	// PrevLastInsertID is the last insert ID of previous statement.
	PrevLastInsertID uint64
	// LastInsertID is the auto-generated ID in the current statement.
	LastInsertID uint64
	// InsertID is the given insert ID of an auto_increment column.
	InsertID uint64

	BaseRowID int64
	MaxRowID  int64

	// Copied from SessionVars.TimeZone.
	TimeZone         *time.Location
	Priority         mysql.PriorityEnum
	NotFillCache     bool
	MemTracker       *memory.Tracker
	DiskTracker      *disk.Tracker
	IsTiFlash        atomic2.Bool
	RuntimeStatsColl *execdetails.RuntimeStatsColl
	TableIDs         []int64
	IndexNames       []string
	StmtType         string
	OriginalSQL      string
	digestMemo       struct {
		sync.Once
		normalized string
		digest     *parser.Digest
	}
	// BindSQL used to construct the key for plan cache. It records the binding used by the stmt.
	// If the binding is not used by the stmt, the value is empty
	BindSQL string
	// planNormalized use for cache the normalized plan, avoid duplicate builds.
	planNormalized        string
	planDigest            *parser.Digest
	encodedPlan           string
	planHint              string
	planHintSet           bool
	Tables                []TableEntry
	PointExec             bool  // for point update cached execution, Constant expression need to set "paramMarker"
	lockWaitStartTime     int64 // LockWaitStartTime stores the pessimistic lock wait start time
	PessimisticLockWaited int32
	LockKeysDuration      int64
	LockKeysCount         int32
	LockTableIDs          map[int64]struct{} // table IDs need to be locked, empty for lock all tables
	TblInfo2UnionScan     map[*model.TableInfo]bool
	TaskID                uint64 // unique ID for an execution of a statement
	TaskMapBakTS          uint64 // counter for

	// stmtCache is used to store some statement-related values.
	stmtCache map[StmtCacheKey]interface{}

	// Map to store all CTE storages of current SQL.
	// Will clean up at the end of the execution.
	CTEStorageMap interface{}

	// If the statement read from table cache, this flag is set.
	ReadFromTableCache bool

	// cache is used to reduce object allocation.
	cache struct {
		execdetails.RuntimeStatsColl
		MemTracker  memory.Tracker
		DiskTracker disk.Tracker
		LogOnExceed [2]memory.LogOnExceed
	}

	// OptimInfo maps Plan.ID() to optimization information when generating Plan.
	OptimInfo map[int]string
	// InVerboseExplain indicates the statement is "explain format='verbose' ...".
	InVerboseExplain bool

	// EnableOptimizeTrace indicates whether enable optimizer trace by 'trace plan statement'
	EnableOptimizeTrace bool
	// OptimizeTracer indicates the tracer for optimize
	OptimizeTracer *tracing.OptimizeTracer
	// EnableOptimizerCETrace indicate if cardinality estimation internal process needs to be traced.
	// CE Trace is currently a submodule of the optimizer trace and is controlled by a separated option.
	EnableOptimizerCETrace bool
	OptimizerCETrace       []*tracing.CETraceRecord

	// WaitLockLeaseTime is the duration of cached table read lease expiration time.
	WaitLockLeaseTime time.Duration

	// KvExecCounter is created from SessionVars.StmtStats to count the number of SQL
	// executions of the kv layer during the current execution of the statement.
	// Its life cycle is limited to this execution, and a new KvExecCounter is
	// always created during each statement execution.
	KvExecCounter *stmtstats.KvExecCounter

	// WeakConsistency is true when read consistency is weak and in a read statement and not in a transaction.
	WeakConsistency bool

	StatsLoad struct {
		// Timeout to wait for sync-load
		Timeout time.Duration
		// NeededColumns stores the columns whose stats are needed for planner.
		NeededColumns []model.TableColumnID
		// ResultCh to receive stats loading results
		ResultCh chan model.TableColumnID
		// Fallback indicates if the planner uses full-loaded stats or fallback all to pseudo/simple.
		Fallback bool
		// LoadStartTime is to record the load start time to calculate latency
		LoadStartTime time.Time
	}

	// SysdateIsNow indicates whether sysdate() is an alias of now() in this statement
	SysdateIsNow bool

	// RCCheckTS indicates the current read-consistency read select statement will use `RCCheckTS` path.
	RCCheckTS bool

	// IsSQLRegistered uses to indicate whether the SQL has been registered for TopSQL.
	IsSQLRegistered atomic2.Bool
	// IsSQLAndPlanRegistered uses to indicate whether the SQL and plan has been registered for TopSQL.
	IsSQLAndPlanRegistered atomic2.Bool
}

// StmtHints are SessionVars related sql hints.
type StmtHints struct {
	// Hint Information
	MemQuotaQuery           int64
	ApplyCacheCapacity      int64
	MaxExecutionTime        uint64
	ReplicaRead             byte
	AllowInSubqToJoinAndAgg bool
	NoIndexMergeHint        bool
	StraightJoinOrder       bool
	// EnableCascadesPlanner is use cascades planner for a single query only.
	EnableCascadesPlanner bool
	// ForceNthPlan indicates the PlanCounterTp number for finding physical plan.
	// -1 for disable.
	ForceNthPlan int64

	// Hint flags
	HasAllowInSubqToJoinAndAggHint bool
	HasMemQuotaHint                bool
	HasReplicaReadHint             bool
	HasMaxExecutionTime            bool
	HasEnableCascadesPlannerHint   bool
	SetVars                        map[string]string

	// the original table hints
	OriginalTableHints []*ast.TableOptimizerHint
}

// TaskMapNeedBackUp indicates that whether we need to back up taskMap during physical optimizing.
func (sh *StmtHints) TaskMapNeedBackUp() bool {
	return sh.ForceNthPlan != -1
}

// StmtCacheKey represents the key type in the StmtCache.
type StmtCacheKey int

const (
	// StmtNowTsCacheKey is a variable for now/current_timestamp calculation/cache of one stmt.
	StmtNowTsCacheKey StmtCacheKey = iota
	// StmtSafeTSCacheKey is a variable for safeTS calculation/cache of one stmt.
	StmtSafeTSCacheKey
)

// GetOrStoreStmtCache gets the cached value of the given key if it exists, otherwise stores the value.
func (sc *StatementContext) GetOrStoreStmtCache(key StmtCacheKey, value interface{}) interface{} {
	if sc.stmtCache == nil {
		sc.stmtCache = make(map[StmtCacheKey]interface{})
	}
	if _, ok := sc.stmtCache[key]; !ok {
		sc.stmtCache[key] = value
	}
	return sc.stmtCache[key]
}

// ResetInStmtCache resets the cache of given key.
func (sc *StatementContext) ResetInStmtCache(key StmtCacheKey) {
	delete(sc.stmtCache, key)
}

// ResetStmtCache resets all cached values.
func (sc *StatementContext) ResetStmtCache() {
	sc.stmtCache = make(map[StmtCacheKey]interface{})
}

// SQLDigest gets normalized and digest for provided sql.
// it will cache result after first calling.
func (sc *StatementContext) SQLDigest() (normalized string, sqlDigest *parser.Digest) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(sc.OriginalSQL)
	})
	return sc.digestMemo.normalized, sc.digestMemo.digest
}

// InitSQLDigest sets the normalized and digest for sql.
func (sc *StatementContext) InitSQLDigest(normalized string, digest *parser.Digest) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = normalized, digest
	})
}

// GetPlanDigest gets the normalized plan and plan digest.
func (sc *StatementContext) GetPlanDigest() (normalized string, planDigest *parser.Digest) {
	return sc.planNormalized, sc.planDigest
}

// GetResourceGroupTagger returns the implementation of tikvrpc.ResourceGroupTagger related to self.
func (sc *StatementContext) GetResourceGroupTagger() tikvrpc.ResourceGroupTagger {
	normalized, digest := sc.SQLDigest()
	planDigest := sc.planDigest
	return func(req *tikvrpc.Request) {
		if req == nil {
			return
		}
		if len(normalized) == 0 {
			return
		}
		req.ResourceGroupTag = resourcegrouptag.EncodeResourceGroupTag(digest, planDigest,
			resourcegrouptag.GetResourceGroupLabelByKey(resourcegrouptag.GetFirstKeyFromRequest(req)))
	}
}

// SetPlanDigest sets the normalized plan and plan digest.
func (sc *StatementContext) SetPlanDigest(normalized string, planDigest *parser.Digest) {
	if planDigest != nil {
		sc.planNormalized, sc.planDigest = normalized, planDigest
	}
}

// GetEncodedPlan gets the encoded plan, it is used to avoid repeated encode.
func (sc *StatementContext) GetEncodedPlan() string {
	return sc.encodedPlan
}

// SetEncodedPlan sets the encoded plan, it is used to avoid repeated encode.
func (sc *StatementContext) SetEncodedPlan(encodedPlan string) {
	sc.encodedPlan = encodedPlan
}

// GetPlanHint gets the hint string generated from the plan.
func (sc *StatementContext) GetPlanHint() (string, bool) {
	return sc.planHint, sc.planHintSet
}

// InitDiskTracker initializes the sc.DiskTracker, use cache to avoid allocation.
func (sc *StatementContext) InitDiskTracker(label int, bytesLimit int64) {
	memory.InitTracker(&sc.cache.DiskTracker, label, bytesLimit, &sc.cache.LogOnExceed[0])
	sc.DiskTracker = &sc.cache.DiskTracker
}

// InitMemTracker initializes the sc.MemTracker, use cache to avoid allocation.
func (sc *StatementContext) InitMemTracker(label int, bytesLimit int64) {
	memory.InitTracker(&sc.cache.MemTracker, label, bytesLimit, &sc.cache.LogOnExceed[1])
	sc.MemTracker = &sc.cache.MemTracker
}

// SetPlanHint sets the hint for the plan.
func (sc *StatementContext) SetPlanHint(hint string) {
	sc.planHintSet = true
	sc.planHint = hint
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.affectedRows += rows
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.affectedRows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.foundRows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.foundRows += rows
}

// RecordRows is used to generate info message
func (sc *StatementContext) RecordRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.records
}

// AddRecordRows adds record rows.
func (sc *StatementContext) AddRecordRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.records += rows
}

// DeletedRows is used to generate info message
func (sc *StatementContext) DeletedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.deleted
}

// AddDeletedRows adds record rows.
func (sc *StatementContext) AddDeletedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.deleted += rows
}

// UpdatedRows is used to generate info message
func (sc *StatementContext) UpdatedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.updated
}

// AddUpdatedRows adds updated rows.
func (sc *StatementContext) AddUpdatedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.updated += rows
}

// CopiedRows is used to generate info message
func (sc *StatementContext) CopiedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.copied
}

// AddCopiedRows adds copied rows.
func (sc *StatementContext) AddCopiedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.copied += rows
}

// TouchedRows is used to generate info message
func (sc *StatementContext) TouchedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.touched
}

// AddTouchedRows adds touched rows.
func (sc *StatementContext) AddTouchedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.touched += rows
}

// GetMessage returns the extra message of the last executed command, if there is no message, it returns empty string
func (sc *StatementContext) GetMessage() string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.message
}

// SetMessage sets the info message generated by some commands
func (sc *StatementContext) SetMessage(msg string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.message = msg
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	warns := make([]SQLWarn, len(sc.mu.warnings))
	copy(warns, sc.mu.warnings)
	return warns
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (sc *StatementContext) TruncateWarnings(start int) []SQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sz := len(sc.mu.warnings) - start
	if sz <= 0 {
		return nil
	}
	ret := make([]SQLWarn, sz)
	copy(ret, sc.mu.warnings[start:])
	sc.mu.warnings = sc.mu.warnings[:start]
	return ret
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return uint16(len(sc.mu.warnings))
}

// NumErrorWarnings gets warning and error count.
func (sc *StatementContext) NumErrorWarnings() (ec uint16, wc int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ec = sc.mu.errorCount
	wc = len(sc.mu.warnings)
	return
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.warnings = warns
	for _, w := range warns {
		if w.Level == WarnLevelError {
			sc.mu.errorCount++
		}
	}
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelWarning, warn})
	}
}

// AppendWarnings appends some warnings.
func (sc *StatementContext) AppendWarnings(warns []SQLWarn) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, warns...)
	}
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelNote, warn})
	}
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, SQLWarn{WarnLevelError, warn})
		sc.mu.errorCount++
	}
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

// resetMuForRetry resets the changed states of sc.mu during execution.
func (sc *StatementContext) resetMuForRetry() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.records = 0
	sc.mu.deleted = 0
	sc.mu.updated = 0
	sc.mu.copied = 0
	sc.mu.touched = 0
	sc.mu.message = ""
	sc.mu.errorCount = 0
	sc.mu.warnings = nil
	sc.mu.execDetails = execdetails.ExecDetails{}
	sc.mu.allExecDetails = make([]*execdetails.ExecDetails, 0, 4)
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.resetMuForRetry()
	sc.MaxRowID = 0
	sc.BaseRowID = 0
	sc.TableIDs = sc.TableIDs[:0]
	sc.IndexNames = sc.IndexNames[:0]
	sc.TaskID = AllocateTaskID()
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (sc *StatementContext) MergeExecDetails(details *execdetails.ExecDetails, commitDetails *util.CommitDetails) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if details != nil {
		sc.mu.execDetails.CopTime += details.CopTime
		sc.mu.execDetails.BackoffTime += details.BackoffTime
		sc.mu.execDetails.RequestCount++
		sc.MergeScanDetail(details.ScanDetail)
		sc.MergeTimeDetail(details.TimeDetail)
		sc.mu.allExecDetails = append(sc.mu.allExecDetails, details)
	}
	if commitDetails != nil {
		if sc.mu.execDetails.CommitDetail == nil {
			sc.mu.execDetails.CommitDetail = commitDetails
		} else {
			sc.mu.execDetails.CommitDetail.Merge(commitDetails)
		}
	}
}

// MergeScanDetail merges scan details into self.
func (sc *StatementContext) MergeScanDetail(scanDetail *util.ScanDetail) {
	// Currently TiFlash cop task does not fill scanDetail, so need to skip it if scanDetail is nil
	if scanDetail == nil {
		return
	}
	if sc.mu.execDetails.ScanDetail == nil {
		sc.mu.execDetails.ScanDetail = &util.ScanDetail{}
	}
	sc.mu.execDetails.ScanDetail.Merge(scanDetail)
}

// MergeTimeDetail merges time details into self.
func (sc *StatementContext) MergeTimeDetail(timeDetail util.TimeDetail) {
	sc.mu.execDetails.TimeDetail.ProcessTime += timeDetail.ProcessTime
	sc.mu.execDetails.TimeDetail.WaitTime += timeDetail.WaitTime
}

// MergeLockKeysExecDetails merges lock keys execution details into self.
func (sc *StatementContext) MergeLockKeysExecDetails(lockKeys *util.LockKeysDetails) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.mu.execDetails.LockKeysDetail == nil {
		sc.mu.execDetails.LockKeysDetail = lockKeys
	} else {
		sc.mu.execDetails.LockKeysDetail.Merge(lockKeys)
	}
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	var details execdetails.ExecDetails
	sc.mu.Lock()
	defer sc.mu.Unlock()
	details = sc.mu.execDetails
	details.LockKeysDuration = time.Duration(atomic.LoadInt64(&sc.LockKeysDuration))
	return details
}

// ShouldClipToZero indicates whether values less than 0 should be clipped to 0 for unsigned integer types.
// This is the case for `insert`, `update`, `alter table`, `create table` and `load data infile` statements, when not in strict SQL mode.
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
func (sc *StatementContext) ShouldClipToZero() bool {
	return sc.InInsertStmt || sc.InLoadDataStmt || sc.InUpdateStmt || sc.InCreateOrAlterStmt || sc.IsDDLJobInQueue
}

// ShouldIgnoreOverflowError indicates whether we should ignore the error when type conversion overflows,
// so we can leave it for further processing like clipping values less than 0 to 0 for unsigned integer types.
func (sc *StatementContext) ShouldIgnoreOverflowError() bool {
	if (sc.InInsertStmt && sc.TruncateAsWarning) || sc.InLoadDataStmt {
		return true
	}
	return false
}

// PushDownFlags converts StatementContext to tipb.SelectRequest.Flags.
func (sc *StatementContext) PushDownFlags() uint64 {
	var flags uint64
	if sc.InInsertStmt {
		flags |= model.FlagInInsertStmt
	} else if sc.InUpdateStmt || sc.InDeleteStmt {
		flags |= model.FlagInUpdateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= model.FlagInSelectStmt
	}
	if sc.IgnoreTruncate {
		flags |= model.FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= model.FlagTruncateAsWarning
	}
	if sc.OverflowAsWarning {
		flags |= model.FlagOverflowAsWarning
	}
	if sc.IgnoreZeroInDate {
		flags |= model.FlagIgnoreZeroInDate
	}
	if sc.DividedByZeroAsWarning {
		flags |= model.FlagDividedByZeroAsWarning
	}
	if sc.InLoadDataStmt {
		flags |= model.FlagInLoadDataStmt
	}
	return flags
}

// CopTasksDetails returns some useful information of cop-tasks during execution.
func (sc *StatementContext) CopTasksDetails() *CopTasksDetails {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := len(sc.mu.allExecDetails)
	d := &CopTasksDetails{
		NumCopTasks:       n,
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}
	if n == 0 {
		return d
	}
	d.AvgProcessTime = sc.mu.execDetails.TimeDetail.ProcessTime / time.Duration(n)
	d.AvgWaitTime = sc.mu.execDetails.TimeDetail.WaitTime / time.Duration(n)

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].TimeDetail.ProcessTime < sc.mu.allExecDetails[j].TimeDetail.ProcessTime
	})
	d.P90ProcessTime = sc.mu.allExecDetails[n*9/10].TimeDetail.ProcessTime
	d.MaxProcessTime = sc.mu.allExecDetails[n-1].TimeDetail.ProcessTime
	d.MaxProcessAddress = sc.mu.allExecDetails[n-1].CalleeAddress

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].TimeDetail.WaitTime < sc.mu.allExecDetails[j].TimeDetail.WaitTime
	})
	d.P90WaitTime = sc.mu.allExecDetails[n*9/10].TimeDetail.WaitTime
	d.MaxWaitTime = sc.mu.allExecDetails[n-1].TimeDetail.WaitTime
	d.MaxWaitAddress = sc.mu.allExecDetails[n-1].CalleeAddress

	// calculate backoff details
	type backoffItem struct {
		callee    string
		sleepTime time.Duration
		times     int
	}
	backoffInfo := make(map[string][]backoffItem)
	for _, ed := range sc.mu.allExecDetails {
		for backoff := range ed.BackoffTimes {
			backoffInfo[backoff] = append(backoffInfo[backoff], backoffItem{
				callee:    ed.CalleeAddress,
				sleepTime: ed.BackoffSleep[backoff],
				times:     ed.BackoffTimes[backoff],
			})
		}
	}
	for backoff, items := range backoffInfo {
		if len(items) == 0 {
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].sleepTime < items[j].sleepTime
		})
		n := len(items)
		d.MaxBackoffAddress[backoff] = items[n-1].callee
		d.MaxBackoffTime[backoff] = items[n-1].sleepTime
		d.P90BackoffTime[backoff] = items[n*9/10].sleepTime

		var totalTime time.Duration
		totalTimes := 0
		for _, it := range items {
			totalTime += it.sleepTime
			totalTimes += it.times
		}
		d.AvgBackoffTime[backoff] = totalTime / time.Duration(n)
		d.TotBackoffTime[backoff] = totalTime
		d.TotBackoffTimes[backoff] = totalTimes
	}
	return d
}

// SetFlagsFromPBFlag set the flag of StatementContext from a `tipb.SelectRequest.Flags`.
func (sc *StatementContext) SetFlagsFromPBFlag(flags uint64) {
	sc.IgnoreTruncate = (flags & model.FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & model.FlagTruncateAsWarning) > 0
	sc.InInsertStmt = (flags & model.FlagInInsertStmt) > 0
	sc.InSelectStmt = (flags & model.FlagInSelectStmt) > 0
	sc.OverflowAsWarning = (flags & model.FlagOverflowAsWarning) > 0
	sc.IgnoreZeroInDate = (flags & model.FlagIgnoreZeroInDate) > 0
	sc.DividedByZeroAsWarning = (flags & model.FlagDividedByZeroAsWarning) > 0
}

// GetLockWaitStartTime returns the statement pessimistic lock wait start time
func (sc *StatementContext) GetLockWaitStartTime() time.Time {
	startTime := atomic.LoadInt64(&sc.lockWaitStartTime)
	if startTime == 0 {
		startTime = time.Now().UnixNano()
		atomic.StoreInt64(&sc.lockWaitStartTime, startTime)
	}
	return time.Unix(0, startTime)
}

// CopTasksDetails collects some useful information of cop-tasks during execution.
type CopTasksDetails struct {
	NumCopTasks int

	AvgProcessTime    time.Duration
	P90ProcessTime    time.Duration
	MaxProcessAddress string
	MaxProcessTime    time.Duration

	AvgWaitTime    time.Duration
	P90WaitTime    time.Duration
	MaxWaitAddress string
	MaxWaitTime    time.Duration

	MaxBackoffTime    map[string]time.Duration
	MaxBackoffAddress map[string]string
	AvgBackoffTime    map[string]time.Duration
	P90BackoffTime    map[string]time.Duration
	TotBackoffTime    map[string]time.Duration
	TotBackoffTimes   map[string]int
}

// ToZapFields wraps the CopTasksDetails as zap.Fileds.
func (d *CopTasksDetails) ToZapFields() (fields []zap.Field) {
	if d.NumCopTasks == 0 {
		return
	}
	fields = make([]zap.Field, 0, 10)
	fields = append(fields, zap.Int("num_cop_tasks", d.NumCopTasks))
	fields = append(fields, zap.String("process_avg_time", strconv.FormatFloat(d.AvgProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_p90_time", strconv.FormatFloat(d.P90ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_time", strconv.FormatFloat(d.MaxProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_addr", d.MaxProcessAddress))
	fields = append(fields, zap.String("wait_avg_time", strconv.FormatFloat(d.AvgWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_p90_time", strconv.FormatFloat(d.P90WaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_time", strconv.FormatFloat(d.MaxWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.MaxWaitAddress))
	return fields
}
