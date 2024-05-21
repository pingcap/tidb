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
	"bytes"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/linter/constructor"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/nocopy"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/tikvrpc"
	atomic2 "go.uber.org/atomic"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/singleflight"
)

var taskIDAlloc uint64

// AllocateTaskID allocates a new unique ID for a statement execution
func AllocateTaskID() uint64 {
	return atomic.AddUint64(&taskIDAlloc, 1)
}

// SQLWarn relates a sql warning and it's level.
type SQLWarn = contextutil.SQLWarn

type jsonSQLWarn struct {
	Level  string        `json:"level"`
	SQLErr *terror.Error `json:"err,omitempty"`
	Msg    string        `json:"msg,omitempty"`
}

// ReferenceCount indicates the reference count of StmtCtx.
type ReferenceCount int32

const (
	// ReferenceCountIsFrozen indicates the current StmtCtx is resetting, it'll refuse all the access from other sessions.
	ReferenceCountIsFrozen int32 = -1
	// ReferenceCountNoReference indicates the current StmtCtx is not accessed by other sessions.
	ReferenceCountNoReference int32 = 0
)

// TryIncrease tries to increase the reference count.
// There is a small chance that TryIncrease returns true while TryFreeze and
// UnFreeze are invoked successfully during the execution of TryIncrease.
func (rf *ReferenceCount) TryIncrease() bool {
	refCnt := atomic.LoadInt32((*int32)(rf))
	for ; refCnt != ReferenceCountIsFrozen && !atomic.CompareAndSwapInt32((*int32)(rf), refCnt, refCnt+1); refCnt = atomic.LoadInt32((*int32)(rf)) {
	}
	return refCnt != ReferenceCountIsFrozen
}

// Decrease decreases the reference count.
func (rf *ReferenceCount) Decrease() {
	for refCnt := atomic.LoadInt32((*int32)(rf)); !atomic.CompareAndSwapInt32((*int32)(rf), refCnt, refCnt-1); refCnt = atomic.LoadInt32((*int32)(rf)) {
	}
}

// TryFreeze tries to freeze the StmtCtx to frozen before resetting the old StmtCtx.
func (rf *ReferenceCount) TryFreeze() bool {
	return atomic.LoadInt32((*int32)(rf)) == ReferenceCountNoReference && atomic.CompareAndSwapInt32((*int32)(rf), ReferenceCountNoReference, ReferenceCountIsFrozen)
}

// UnFreeze unfreeze the frozen StmtCtx thus the other session can access this StmtCtx.
func (rf *ReferenceCount) UnFreeze() {
	atomic.StoreInt32((*int32)(rf), ReferenceCountNoReference)
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// NoCopy indicates that this struct cannot be copied because
	// copying this object will make the copied TypeCtx field to refer a wrong `AppendWarnings` func.
	_ nocopy.NoCopy

	_ constructor.Constructor `ctor:"NewStmtCtxWithTimeZone,Reset"`

	ctxID uint64

	// 	typeCtx is used to indicate how to make the type conversation.
	typeCtx types.Context

	// errCtx is used to indicate how to handle the errors
	errCtx errctx.Context

	// distSQLCtxCache is used to persist all variables and tools needed by the `distsql`
	// this cache is set on `StatementContext` because it has to be updated after each statement.
	distSQLCtxCache struct {
		init sync.Once
		dctx *distsqlctx.DistSQLContext
	}

	// rangerCtxCache is used to persist all variables and tools needed by the `ranger`
	// this cache is set on `StatementContext` because it has to be updated after each statement.
	// `rctx` uses `any` type to avoid cyclic dependency
	rangerCtxCache struct {
		init sync.Once
		rctx any
	}

	// buildPBCtxCache is used to persist all variables and tools needed by the `ToPB`
	// this cache is set on `StatementContext` because it has to be updated after each statement.
	buildPBCtxCache struct {
		init sync.Once
		bctx any
	}

	// Set the following variables before execution
	hint.StmtHints

	// IsDDLJobInQueue is used to mark whether the DDL job is put into the queue.
	// If IsDDLJobInQueue is true, it means the DDL job is in the queue of storage, and it can be handled by the DDL worker.
	IsDDLJobInQueue        bool
	DDLJobID               int64
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	InExplainStmt          bool
	InExplainAnalyzeStmt   bool
	ExplainFormat          string
	InCreateOrAlterStmt    bool
	InSetSessionStatesStmt bool
	InPreparedPlanBuilding bool
	InShowWarning          bool

	contextutil.PlanCacheTracker
	contextutil.RangeFallbackHandler

	BatchCheck            bool
	IgnoreExplainIDSuffix bool
	MultiSchemaInfo       *model.MultiSchemaInfo
	// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
	// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
	// in stmtCtx
	IsStaleness     bool
	InRestrictedSQL bool
	ViewDepth       int32
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

		message string
	}
	WarnHandler contextutil.WarnHandlerExt
	// ExtraWarnHandler record the extra warnings and are only used by the slow log only now.
	// If a warning is expected to be output only under some conditions (like in EXPLAIN or EXPLAIN VERBOSE) but it's
	// not under such conditions now, it is considered as an extra warning.
	// extraWarnings would not be printed through SHOW WARNINGS, but we want to always output them through the slow
	// log to help diagnostics, so we store them here separately.
	ExtraWarnHandler contextutil.WarnHandlerExt

	execdetails.SyncExecDetails

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
	Priority     mysql.PriorityEnum
	NotFillCache bool
	MemTracker   *memory.Tracker
	DiskTracker  *disk.Tracker
	// per statement resource group name
	// hint /* +ResourceGroup(name) */ can change the statement group name
	ResourceGroupName   string
	RunawayChecker      *resourcegroup.RunawayChecker
	IsTiFlash           atomic2.Bool
	RuntimeStatsColl    *execdetails.RuntimeStatsColl
	IndexUsageCollector *indexusage.StmtIndexUsageCollector
	TableIDs            []int64
	IndexNames          []string
	StmtType            string
	OriginalSQL         string
	digestMemo          struct {
		sync.Once
		normalized string
		digest     *parser.Digest
	}
	// BindSQL used to construct the key for plan cache. It records the binding used by the stmt.
	// If the binding is not used by the stmt, the value is empty
	BindSQL string

	// The several fields below are mainly for some diagnostic features, like stmt summary and slow query.
	// We cache the values here to avoid calculating them multiple times.
	// Note:
	//   Avoid accessing these fields directly, use their Setter/Getter methods instead.
	//   Other fields should be the zero value or be consistent with the plan field.
	// TODO: more clearly distinguish between the value is empty and the value has not been set
	planNormalized string
	planDigest     *parser.Digest
	encodedPlan    string
	planHint       string
	planHintSet    bool
	binaryPlan     string
	// To avoid cycle import, we use interface{} for the following two fields.
	// flatPlan should be a *plannercore.FlatPhysicalPlan if it's not nil
	flatPlan any
	// plan should be a plannercore.Plan if it's not nil
	plan any

	Tables                []TableEntry
	lockWaitStartTime     int64 // LockWaitStartTime stores the pessimistic lock wait start time
	PessimisticLockWaited int32
	LockKeysDuration      int64
	LockKeysCount         int32
	LockTableIDs          map[int64]struct{} // table IDs need to be locked, empty for lock all tables
	TblInfo2UnionScan     map[*model.TableInfo]bool
	TaskID                uint64 // unique ID for an execution of a statement
	TaskMapBakTS          uint64 // counter for

	// stmtCache is used to store some statement-related values.
	// add mutex to protect stmtCache concurrent access
	// https://github.com/pingcap/tidb/issues/36159
	stmtCache struct {
		mu   sync.Mutex
		data map[StmtCacheKey]any
	}

	// Map to store all CTE storages of current SQL.
	// Will clean up at the end of the execution.
	CTEStorageMap any

	SetVarHintRestore map[string]string

	// If the statement read from table cache, this flag is set.
	ReadFromTableCache bool

	// cache is used to reduce object allocation.
	cache struct {
		execdetails.RuntimeStatsColl
		MemTracker  memory.Tracker
		DiskTracker disk.Tracker
		LogOnExceed [2]memory.LogOnExceed
	}

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

	EnableOptimizerDebugTrace bool
	OptimizerDebugTrace       any

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
		// NeededItems stores the columns/indices whose stats are needed for planner.
		NeededItems []model.StatsLoadItem
		// ResultCh to receive stats loading results
		ResultCh []<-chan singleflight.Result
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
	// IsReadOnly uses to indicate whether the SQL is read-only.
	IsReadOnly bool
	// usedStatsInfo records version of stats of each table used in the query.
	// It's a map of table physical id -> *UsedStatsInfoForTable
	usedStatsInfo atomic.Pointer[UsedStatsInfo]
	// IsSyncStatsFailed indicates whether any failure happened during sync stats
	IsSyncStatsFailed bool
	// UseDynamicPruneMode indicates whether use UseDynamicPruneMode in query stmt
	UseDynamicPruneMode bool
	// ColRefFromPlan mark the column ref used by assignment in update statement.
	ColRefFromUpdatePlan intset.FastIntSet

	// IsExplainAnalyzeDML is true if the statement is "explain analyze DML executors", before responding the explain
	// results to the client, the transaction should be committed first. See issue #37373 for more details.
	IsExplainAnalyzeDML bool

	// InHandleForeignKeyTrigger indicates currently are handling foreign key trigger.
	InHandleForeignKeyTrigger bool

	// ForeignKeyTriggerCtx is the contain information for foreign key cascade execution.
	ForeignKeyTriggerCtx struct {
		// The SavepointName is use to do rollback when handle foreign key cascade failed.
		SavepointName string
		HasFKCascades bool
	}

	// MPPQueryInfo stores some id and timestamp of current MPP query statement.
	MPPQueryInfo struct {
		QueryID              atomic2.Uint64
		QueryTS              atomic2.Uint64
		AllocatedMPPTaskID   atomic2.Int64
		AllocatedMPPGatherID atomic2.Uint64
	}

	// TableStats stores the visited runtime table stats by table id during query
	TableStats map[int64]any
	// useChunkAlloc indicates whether statement use chunk alloc
	useChunkAlloc bool
	// Check if TiFlash read engine is removed due to strict sql mode.
	TiFlashEngineRemovedDueToStrictSQLMode bool
	// StaleTSOProvider is used to provide stale timestamp oracle for read-only transactions.
	StaleTSOProvider struct {
		sync.Mutex
		value *uint64
		eval  func() (uint64, error)
	}

	// MDLRelatedTableIDs is used to store the table IDs that are related to the current MDL lock.
	MDLRelatedTableIDs map[int64]struct{}
}

// DefaultStmtErrLevels is the default error levels for statement
var DefaultStmtErrLevels = func() (l errctx.LevelMap) {
	l[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
	return
}()

// NewStmtCtx creates a new statement context
func NewStmtCtx() *StatementContext {
	return NewStmtCtxWithTimeZone(time.UTC)
}

// NewStmtCtxWithTimeZone creates a new StatementContext with the given timezone
func NewStmtCtxWithTimeZone(tz *time.Location) *StatementContext {
	intest.AssertNotNil(tz)
	sc := &StatementContext{
		ctxID: contextutil.GenContextID(),
	}
	sc.typeCtx = types.NewContext(types.DefaultStmtFlags, tz, sc)
	sc.errCtx = newErrCtx(sc.typeCtx, DefaultStmtErrLevels, sc)
	sc.PlanCacheTracker = contextutil.NewPlanCacheTracker(sc)
	sc.RangeFallbackHandler = contextutil.NewRangeFallbackHandler(&sc.PlanCacheTracker, sc)
	sc.WarnHandler = contextutil.NewStaticWarnHandler(0)
	sc.ExtraWarnHandler = contextutil.NewStaticWarnHandler(0)
	return sc
}

// Reset resets a statement context
func (sc *StatementContext) Reset() {
	*sc = StatementContext{
		ctxID: contextutil.GenContextID(),
	}
	sc.typeCtx = types.NewContext(types.DefaultStmtFlags, time.UTC, sc)
	sc.errCtx = newErrCtx(sc.typeCtx, DefaultStmtErrLevels, sc)
	sc.PlanCacheTracker = contextutil.NewPlanCacheTracker(sc)
	sc.RangeFallbackHandler = contextutil.NewRangeFallbackHandler(&sc.PlanCacheTracker, sc)
	sc.WarnHandler = contextutil.NewStaticWarnHandler(0)
	sc.ExtraWarnHandler = contextutil.NewStaticWarnHandler(0)
}

// CtxID returns the context id of the statement
func (sc *StatementContext) CtxID() uint64 {
	return sc.ctxID
}

// TimeZone returns the timezone of the type context
func (sc *StatementContext) TimeZone() *time.Location {
	intest.AssertNotNil(sc)
	if sc == nil {
		return time.UTC
	}

	return sc.typeCtx.Location()
}

// SetTimeZone sets the timezone
func (sc *StatementContext) SetTimeZone(tz *time.Location) {
	intest.AssertNotNil(tz)
	sc.typeCtx = sc.typeCtx.WithLocation(tz)
}

// TypeCtx returns the type context
func (sc *StatementContext) TypeCtx() types.Context {
	return sc.typeCtx
}

// ErrCtx returns the error context
func (sc *StatementContext) ErrCtx() errctx.Context {
	return sc.errCtx
}

// SetErrLevels sets the error levels for statement
// The argument otherLevels is used to set the error levels except truncate
func (sc *StatementContext) SetErrLevels(otherLevels errctx.LevelMap) {
	sc.errCtx = newErrCtx(sc.typeCtx, otherLevels, sc)
}

// ErrLevels returns the current `errctx.LevelMap`
func (sc *StatementContext) ErrLevels() errctx.LevelMap {
	return sc.errCtx.LevelMap()
}

// ErrGroupLevel returns the error level for the given error group
func (sc *StatementContext) ErrGroupLevel(group errctx.ErrGroup) errctx.Level {
	return sc.errCtx.LevelForGroup(group)
}

// TypeFlags returns the type flags
func (sc *StatementContext) TypeFlags() types.Flags {
	return sc.typeCtx.Flags()
}

// SetTypeFlags sets the type flags
func (sc *StatementContext) SetTypeFlags(flags types.Flags) {
	sc.typeCtx = sc.typeCtx.WithFlags(flags)
	sc.errCtx = newErrCtx(sc.typeCtx, sc.errCtx.LevelMap(), sc)
}

// HandleTruncate ignores or returns the error based on the TypeContext inside.
// TODO: replace this function with `HandleError`, for `TruncatedError` they should have the same effect.
func (sc *StatementContext) HandleTruncate(err error) error {
	return sc.typeCtx.HandleTruncate(err)
}

// HandleError handles the error based on `ErrCtx()`
func (sc *StatementContext) HandleError(err error) error {
	intest.AssertNotNil(sc)
	if sc == nil {
		return err
	}
	errCtx := sc.ErrCtx()
	return errCtx.HandleError(err)
}

// HandleErrorWithAlias handles the error based on `ErrCtx()`
func (sc *StatementContext) HandleErrorWithAlias(internalErr, err, warnErr error) error {
	intest.AssertNotNil(sc)
	if sc == nil {
		return err
	}
	errCtx := sc.ErrCtx()
	return errCtx.HandleErrorWithAlias(internalErr, err, warnErr)
}

// StmtCacheKey represents the key type in the StmtCache.
type StmtCacheKey int

const (
	// StmtNowTsCacheKey is a variable for now/current_timestamp calculation/cache of one stmt.
	StmtNowTsCacheKey StmtCacheKey = iota
	// StmtSafeTSCacheKey is a variable for safeTS calculation/cache of one stmt.
	StmtSafeTSCacheKey
	// StmtExternalTSCacheKey is a variable for externalTS calculation/cache of one stmt.
	StmtExternalTSCacheKey
)

// GetOrStoreStmtCache gets the cached value of the given key if it exists, otherwise stores the value.
func (sc *StatementContext) GetOrStoreStmtCache(key StmtCacheKey, value any) any {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	if sc.stmtCache.data == nil {
		sc.stmtCache.data = make(map[StmtCacheKey]any)
	}
	if _, ok := sc.stmtCache.data[key]; !ok {
		sc.stmtCache.data[key] = value
	}
	return sc.stmtCache.data[key]
}

// GetOrEvaluateStmtCache gets the cached value of the given key if it exists, otherwise calculate the value.
func (sc *StatementContext) GetOrEvaluateStmtCache(key StmtCacheKey, valueEvaluator func() (any, error)) (any, error) {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	if sc.stmtCache.data == nil {
		sc.stmtCache.data = make(map[StmtCacheKey]any)
	}
	if _, ok := sc.stmtCache.data[key]; !ok {
		value, err := valueEvaluator()
		if err != nil {
			return nil, err
		}
		sc.stmtCache.data[key] = value
	}
	return sc.stmtCache.data[key], nil
}

// ResetInStmtCache resets the cache of given key.
func (sc *StatementContext) ResetInStmtCache(key StmtCacheKey) {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	delete(sc.stmtCache.data, key)
}

// ResetStmtCache resets all cached values.
func (sc *StatementContext) ResetStmtCache() {
	sc.stmtCache.mu.Lock()
	defer sc.stmtCache.mu.Unlock()
	sc.stmtCache.data = make(map[StmtCacheKey]any)
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

// ResetSQLDigest sets the normalized and digest for sql anyway, **DO NOT USE THIS UNLESS YOU KNOW WHAT YOU ARE DOING NOW**.
func (sc *StatementContext) ResetSQLDigest(s string) {
	sc.digestMemo.normalized, sc.digestMemo.digest = parser.NormalizeDigest(s)
}

// GetPlanDigest gets the normalized plan and plan digest.
func (sc *StatementContext) GetPlanDigest() (normalized string, planDigest *parser.Digest) {
	return sc.planNormalized, sc.planDigest
}

// GetPlan gets the plan field of stmtctx
func (sc *StatementContext) GetPlan() any {
	return sc.plan
}

// SetPlan sets the plan field of stmtctx
func (sc *StatementContext) SetPlan(plan any) {
	sc.plan = plan
}

// GetFlatPlan gets the flatPlan field of stmtctx
func (sc *StatementContext) GetFlatPlan() any {
	return sc.flatPlan
}

// SetFlatPlan sets the flatPlan field of stmtctx
func (sc *StatementContext) SetFlatPlan(flat any) {
	sc.flatPlan = flat
}

// GetBinaryPlan gets the binaryPlan field of stmtctx
func (sc *StatementContext) GetBinaryPlan() string {
	return sc.binaryPlan
}

// SetBinaryPlan sets the binaryPlan field of stmtctx
func (sc *StatementContext) SetBinaryPlan(binaryPlan string) {
	sc.binaryPlan = binaryPlan
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

// SetUseChunkAlloc set use chunk alloc status
func (sc *StatementContext) SetUseChunkAlloc() {
	sc.useChunkAlloc = true
}

// ClearUseChunkAlloc clear useChunkAlloc status
func (sc *StatementContext) ClearUseChunkAlloc() {
	sc.useChunkAlloc = false
}

// GetUseChunkAllocStatus returns useChunkAlloc status
func (sc *StatementContext) GetUseChunkAllocStatus() bool {
	return sc.useChunkAlloc
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

// PlanCacheType is the flag of plan cache
type PlanCacheType int

const (
	// DefaultNoCache no cache
	DefaultNoCache PlanCacheType = iota
	// SessionPrepared session prepared plan cache
	SessionPrepared
	// SessionNonPrepared session non-prepared plan cache
	SessionNonPrepared
)

// SetHintWarning sets the hint warning and records the reason.
func (sc *StatementContext) SetHintWarning(reason string) {
	sc.AppendWarning(plannererrors.ErrInternal.FastGen(reason))
}

// SetHintWarningFromError sets the hint warning and records the reason.
func (sc *StatementContext) SetHintWarningFromError(reason error) {
	sc.AppendWarning(reason)
}

// TableEntry presents table in db.
type TableEntry struct {
	DB    string
	Table string
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	if sc.InHandleForeignKeyTrigger {
		// For compatibility with MySQL, not add the affected row cause by the foreign key trigger.
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mu.affectedRows += rows
}

// SetAffectedRows sets affected rows.
func (sc *StatementContext) SetAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.affectedRows = rows
	sc.mu.Unlock()
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
	return sc.WarnHandler.GetWarnings()
}

// CopyWarnings copies the warnings to the dst.
func (sc *StatementContext) CopyWarnings(dst []SQLWarn) []SQLWarn {
	return sc.WarnHandler.CopyWarnings(dst)
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (sc *StatementContext) TruncateWarnings(start int) []SQLWarn {
	return sc.WarnHandler.TruncateWarnings(start)
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	return uint16(sc.WarnHandler.WarningCount())
}

// NumErrorWarnings gets warning and error count.
func (sc *StatementContext) NumErrorWarnings() (ec uint16, wc int) {
	return sc.WarnHandler.NumErrorWarnings()
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []SQLWarn) {
	sc.WarnHandler.SetWarnings(warns)
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.WarnHandler.AppendWarning(warn)
}

// AppendWarnings appends some warnings.
func (sc *StatementContext) AppendWarnings(warns []SQLWarn) {
	sc.WarnHandler.AppendWarnings(warns)
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	sc.WarnHandler.AppendNote(warn)
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.WarnHandler.AppendError(warn)
}

// GetExtraWarnings gets extra warnings.
func (sc *StatementContext) GetExtraWarnings() []SQLWarn {
	return sc.ExtraWarnHandler.GetWarnings()
}

// SetExtraWarnings sets extra warnings.
func (sc *StatementContext) SetExtraWarnings(warns []SQLWarn) {
	sc.ExtraWarnHandler.SetWarnings(warns)
}

// AppendExtraWarning appends an extra warning with level 'Warning'.
func (sc *StatementContext) AppendExtraWarning(warn error) {
	sc.ExtraWarnHandler.AppendWarning(warn)
}

// AppendExtraNote appends an extra warning with level 'Note'.
func (sc *StatementContext) AppendExtraNote(warn error) {
	sc.ExtraWarnHandler.AppendNote(warn)
}

// AppendExtraError appends an extra warning with level 'Error'.
func (sc *StatementContext) AppendExtraError(warn error) {
	sc.ExtraWarnHandler.AppendError(warn)
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
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.resetMuForRetry()
	sc.MaxRowID = 0
	sc.BaseRowID = 0
	sc.TableIDs = sc.TableIDs[:0]
	sc.IndexNames = sc.IndexNames[:0]
	sc.TaskID = AllocateTaskID()
	sc.SyncExecDetails.Reset()
	sc.WarnHandler.TruncateWarnings(0)
	sc.ExtraWarnHandler.TruncateWarnings(0)

	// `TaskID` is reset, we'll need to reset distSQLCtx
	sc.distSQLCtxCache.init = sync.Once{}
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	var details execdetails.ExecDetails
	sc.mu.Lock()
	defer sc.mu.Unlock()
	details = sc.SyncExecDetails.GetExecDetails()
	details.LockKeysDuration = time.Duration(atomic.LoadInt64(&sc.LockKeysDuration))
	return details
}

// PushDownFlags converts StatementContext to tipb.SelectRequest.Flags.
func (sc *StatementContext) PushDownFlags() uint64 {
	var flags uint64
	ec := sc.ErrCtx()
	if sc.InInsertStmt {
		flags |= model.FlagInInsertStmt
	} else if sc.InUpdateStmt || sc.InDeleteStmt {
		flags |= model.FlagInUpdateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= model.FlagInSelectStmt
	}
	if sc.TypeFlags().IgnoreTruncateErr() {
		flags |= model.FlagIgnoreTruncate
	} else if sc.TypeFlags().TruncateAsWarning() {
		flags |= model.FlagTruncateAsWarning
		// TODO: remove this flag from TiKV.
		flags |= model.FlagOverflowAsWarning
	}
	if sc.TypeFlags().IgnoreZeroInDate() {
		flags |= model.FlagIgnoreZeroInDate
	}
	if ec.LevelForGroup(errctx.ErrGroupDividedByZero) != errctx.LevelError {
		flags |= model.FlagDividedByZeroAsWarning
	}
	if sc.InLoadDataStmt {
		flags |= model.FlagInLoadDataStmt
	}
	if sc.InRestrictedSQL {
		flags |= model.FlagInRestrictedSQL
	}
	return flags
}

// InitFromPBFlagAndTz set the flag and timezone of StatementContext from a `tipb.SelectRequest.Flags` and `*time.Location`.
func (sc *StatementContext) InitFromPBFlagAndTz(flags uint64, tz *time.Location) {
	sc.InInsertStmt = (flags & model.FlagInInsertStmt) > 0
	sc.InSelectStmt = (flags & model.FlagInSelectStmt) > 0
	sc.InDeleteStmt = (flags & model.FlagInUpdateOrDeleteStmt) > 0
	levels := sc.ErrLevels()
	levels[errctx.ErrGroupDividedByZero] = errctx.ResolveErrLevel(false,
		(flags&model.FlagDividedByZeroAsWarning) > 0,
	)
	sc.SetErrLevels(levels)
	sc.SetTimeZone(tz)
	sc.SetTypeFlags(types.DefaultStmtFlags.
		WithIgnoreTruncateErr((flags & model.FlagIgnoreTruncate) > 0).
		WithTruncateAsWarning((flags & model.FlagTruncateAsWarning) > 0).
		WithIgnoreZeroInDate((flags & model.FlagIgnoreZeroInDate) > 0).
		WithAllowNegativeToUnsigned(!sc.InInsertStmt))
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

// UseDynamicPartitionPrune indicates whether dynamic partition is used during the query
func (sc *StatementContext) UseDynamicPartitionPrune() bool {
	return sc.UseDynamicPruneMode
}

// DetachMemDiskTracker detaches the memory and disk tracker from the sessionTracker.
func (sc *StatementContext) DetachMemDiskTracker() {
	if sc == nil {
		return
	}
	if sc.MemTracker != nil {
		sc.MemTracker.Detach()
	}
	if sc.DiskTracker != nil {
		sc.DiskTracker.Detach()
	}
}

// SetStaleTSOProvider sets the stale TSO provider.
func (sc *StatementContext) SetStaleTSOProvider(eval func() (uint64, error)) {
	sc.StaleTSOProvider.Lock()
	defer sc.StaleTSOProvider.Unlock()
	sc.StaleTSOProvider.value = nil
	sc.StaleTSOProvider.eval = eval
}

// GetStaleTSO returns the TSO for stale-read usage which calculate from PD's last response.
func (sc *StatementContext) GetStaleTSO() (uint64, error) {
	sc.StaleTSOProvider.Lock()
	defer sc.StaleTSOProvider.Unlock()
	if sc.StaleTSOProvider.value != nil {
		return *sc.StaleTSOProvider.value, nil
	}
	if sc.StaleTSOProvider.eval == nil {
		return 0, nil
	}
	tso, err := sc.StaleTSOProvider.eval()
	if err != nil {
		return 0, err
	}
	sc.StaleTSOProvider.value = &tso
	return tso, nil
}

// AddSetVarHintRestore records the variables which are affected by SET_VAR hint. And restore them to the old value later.
func (sc *StatementContext) AddSetVarHintRestore(name, val string) {
	if sc.SetVarHintRestore == nil {
		sc.SetVarHintRestore = make(map[string]string)
	}
	sc.SetVarHintRestore[name] = val
}

// GetUsedStatsInfo returns the map for recording the used stats during query.
// If initIfNil is true, it will initialize it when this map is nil.
func (sc *StatementContext) GetUsedStatsInfo(initIfNil bool) *UsedStatsInfo {
	if sc.usedStatsInfo.Load() == nil && initIfNil {
		sc.usedStatsInfo.CompareAndSwap(nil, &UsedStatsInfo{})
	}
	return sc.usedStatsInfo.Load()
}

// RecordedStatsLoadStatusCnt returns the total number of recorded column/index stats status, which is not full loaded.
func (sc *StatementContext) RecordedStatsLoadStatusCnt() (cnt int) {
	allStatus := sc.GetUsedStatsInfo(false)
	for _, status := range allStatus.Values() {
		if status == nil {
			continue
		}
		cnt += status.recordedColIdxCount()
	}
	return
}

// TypeCtxOrDefault returns the reference to the `TypeCtx` inside the statement context.
// If the statement context is nil, it'll return a newly created default type context.
// **don't** use this function if you can make sure the `sc` is not nil. We should limit the usage of this function as
// little as possible.
func (sc *StatementContext) TypeCtxOrDefault() types.Context {
	if sc != nil {
		return sc.typeCtx
	}

	return types.DefaultStmtNoWarningContext
}

// GetOrInitDistSQLFromCache returns the `DistSQLContext` inside cache. If it didn't exist, return a new one created by
// the `create` function.
func (sc *StatementContext) GetOrInitDistSQLFromCache(create func() *distsqlctx.DistSQLContext) *distsqlctx.DistSQLContext {
	sc.distSQLCtxCache.init.Do(func() {
		sc.distSQLCtxCache.dctx = create()
	})

	return sc.distSQLCtxCache.dctx
}

// GetOrInitRangerCtxFromCache returns the `RangerContext` inside cache. If it didn't exist, return a new one created by
// the `create` function.
func (sc *StatementContext) GetOrInitRangerCtxFromCache(create func() any) any {
	sc.rangerCtxCache.init.Do(func() {
		sc.rangerCtxCache.rctx = create()
	})

	return sc.rangerCtxCache.rctx
}

// GetOrInitBuildPBCtxFromCache returns the `BuildPBContext` inside cache. If it didn't exist, return a new one created by
// the `create` function. It uses the `any` to avoid cycle dependency.
func (sc *StatementContext) GetOrInitBuildPBCtxFromCache(create func() any) any {
	sc.buildPBCtxCache.init.Do(func() {
		sc.buildPBCtxCache.bctx = create()
	})

	return sc.buildPBCtxCache.bctx
}

func newErrCtx(tc types.Context, otherLevels errctx.LevelMap, handler contextutil.WarnAppender) errctx.Context {
	l := errctx.LevelError
	if flags := tc.Flags(); flags.IgnoreTruncateErr() {
		l = errctx.LevelIgnore
	} else if flags.TruncateAsWarning() {
		l = errctx.LevelWarn
	}

	otherLevels[errctx.ErrGroupTruncate] = l
	return errctx.NewContextWithLevels(otherLevels, handler)
}

// UsedStatsInfoForTable records stats that are used during query and their information.
type UsedStatsInfoForTable struct {
	Name                  string
	TblInfo               *model.TableInfo
	Version               uint64
	RealtimeCount         int64
	ModifyCount           int64
	ColumnStatsLoadStatus map[int64]string
	IndexStatsLoadStatus  map[int64]string
	ColAndIdxStatus       any
}

// FormatForExplain format the content in the format expected to be printed in the execution plan.
// case 1: if stats version is 0, print stats:pseudo.
// case 2: if stats version is not 0, and there are column/index stats that are not full loaded,
// print stats:partial, then print status of 3 column/index status at most. For the rest, only
// the count will be printed, in the format like (more: 1 onlyCmsEvicted, 2 onlyHistRemained).
func (s *UsedStatsInfoForTable) FormatForExplain() string {
	// statistics.PseudoVersion == 0
	if s.Version == 0 {
		return "stats:pseudo"
	}
	var b strings.Builder
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) == 0 {
		return ""
	}
	b.WriteString("stats:partial")
	outputNumsLeft := 3
	statusCnt := make(map[string]uint64, 1)
	var strs []string
	strs = append(strs, s.collectFromColOrIdxStatus(false, &outputNumsLeft, statusCnt)...)
	strs = append(strs, s.collectFromColOrIdxStatus(true, &outputNumsLeft, statusCnt)...)
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	if len(statusCnt) > 0 {
		b.WriteString("...(more: ")
		keys := maps.Keys(statusCnt)
		slices.Sort(keys)
		var cntStrs []string
		for _, key := range keys {
			cntStrs = append(cntStrs, strconv.FormatUint(statusCnt[key], 10)+" "+key)
		}
		b.WriteString(strings.Join(cntStrs, ", "))
		b.WriteString(")")
	}
	b.WriteString("]")
	return b.String()
}

// WriteToSlowLog format the content in the format expected to be printed to the slow log, then write to w.
// The format is table name partition name:version[realtime row count;modify count][index load status][column load status].
func (s *UsedStatsInfoForTable) WriteToSlowLog(w io.Writer) {
	ver := "pseudo"
	// statistics.PseudoVersion == 0
	if s.Version != 0 {
		ver = strconv.FormatUint(s.Version, 10)
	}
	fmt.Fprintf(w, "%s:%s[%d;%d]", s.Name, ver, s.RealtimeCount, s.ModifyCount)
	if ver == "pseudo" {
		return
	}
	if len(s.ColumnStatsLoadStatus)+len(s.IndexStatsLoadStatus) > 0 {
		fmt.Fprintf(w,
			"[%s][%s]",
			strings.Join(s.collectFromColOrIdxStatus(false, nil, nil), ","),
			strings.Join(s.collectFromColOrIdxStatus(true, nil, nil), ","),
		)
	}
}

// collectFromColOrIdxStatus prints the status of column or index stats to a slice
// of the string in the format of "col/idx name:status".
// If outputNumsLeft is not nil, this function will output outputNumsLeft column/index
// status at most, the rest will be counted in statusCnt, which is a map of status->count.
func (s *UsedStatsInfoForTable) collectFromColOrIdxStatus(
	forColumn bool,
	outputNumsLeft *int,
	statusCnt map[string]uint64,
) []string {
	var status map[int64]string
	if forColumn {
		status = s.ColumnStatsLoadStatus
	} else {
		status = s.IndexStatsLoadStatus
	}
	keys := maps.Keys(status)
	slices.Sort(keys)
	strs := make([]string, 0, len(status))
	for _, id := range keys {
		if outputNumsLeft == nil || *outputNumsLeft > 0 {
			var name string
			if s.TblInfo != nil {
				if forColumn {
					name = s.TblInfo.FindColumnNameByID(id)
				} else {
					name = s.TblInfo.FindIndexNameByID(id)
				}
			}
			if len(name) == 0 {
				name = "ID " + strconv.FormatInt(id, 10)
			}
			strs = append(strs, name+":"+status[id])
			if outputNumsLeft != nil {
				*outputNumsLeft--
			}
		} else if statusCnt != nil {
			statusCnt[status[id]] = statusCnt[status[id]] + 1
		}
	}
	return strs
}

func (s *UsedStatsInfoForTable) recordedColIdxCount() int {
	return len(s.IndexStatsLoadStatus) + len(s.ColumnStatsLoadStatus)
}

// UsedStatsInfo is a map for recording the used stats during query.
// The key is the table ID, and the value is the used stats info for the table.
type UsedStatsInfo struct {
	store sync.Map
}

// GetUsedInfo gets the used stats info for the table.
func (u *UsedStatsInfo) GetUsedInfo(tableID int64) *UsedStatsInfoForTable {
	v, ok := u.store.Load(tableID)
	if !ok {
		return nil
	}
	return v.(*UsedStatsInfoForTable)
}

// RecordUsedInfo records the used stats info for the table.
func (u *UsedStatsInfo) RecordUsedInfo(tableID int64, info *UsedStatsInfoForTable) {
	u.store.Store(tableID, info)
}

// Keys returns all the table IDs for the used stats info.
func (u *UsedStatsInfo) Keys() []int64 {
	var ret []int64
	if u == nil {
		return ret
	}
	u.store.Range(func(k, v any) bool {
		ret = append(ret, k.(int64))
		return true
	})
	return ret
}

// Values returns all the used stats info for the table.
func (u *UsedStatsInfo) Values() []*UsedStatsInfoForTable {
	var ret []*UsedStatsInfoForTable
	if u == nil {
		return ret
	}
	u.store.Range(func(k, v any) bool {
		ret = append(ret, v.(*UsedStatsInfoForTable))
		return true
	})
	return ret
}

// StatsLoadResult indicates result for StatsLoad
type StatsLoadResult struct {
	Item  model.TableItemID
	Error error
}

// HasError returns whether result has error
func (r StatsLoadResult) HasError() bool {
	return r.Error != nil
}

// ErrorMsg returns StatsLoadResult err msg
func (r StatsLoadResult) ErrorMsg() string {
	if r.Error == nil {
		return ""
	}
	b := bytes.NewBufferString("tableID:")
	b.WriteString(strconv.FormatInt(r.Item.TableID, 10))
	b.WriteString(", id:")
	b.WriteString(strconv.FormatInt(r.Item.ID, 10))
	b.WriteString(", isIndex:")
	b.WriteString(strconv.FormatBool(r.Item.IsIndex))
	b.WriteString(", err:")
	b.WriteString(r.Error.Error())
	return b.String()
}
