# Session, DDL, Server, and Domain Package Issues

## Session Package

### session.go (5,035 lines)
**Struct** (lines 190-261): 18 fields aggregating multiple concerns.

Key fields:
- `sessionPlanCache` - plan cache (lazy-initialized at line 437)
- `sessionVars` - 350+ field mega-struct
- `pctx`, `exprctx`, `tblctx` - multiple context objects
- `statsCollector`, `idxUsageCollector`, `stmtStats` - statistics
- `lockedTables`, `advisoryLocks` - lock tracking maps

**Method count**: 327 public/private methods on session struct.

### SessionVars Mega-Struct (3,853 lines)
**File**: `pkg/sessionctx/variable/session.go:780-1129` (350+ fields)

Field categories:
- **Lines 792-806**: System variable storage (maps + extracted hot-path values)
- **Lines 815-818**: Transaction context (TxnCtx + TxnCtxMu lock)
- **Lines 810-812**: Prepared statements (PreparedStmts map, PreparedStmtNameToID)
- **Lines 945-1107**: 100+ optimizer tuning parameters
- **Lines 1070-1087**: 9 cost factor fields

**Issues**:
- `PreparedStmts` map can grow unbounded on malicious clients (no eviction)
- `nonPreparedPlanCacheStmts` LRU may not be properly cleaned
- 350+ fields cause cache line misses

### TransactionContext (lines 179-261)
- 19 fields in `TxnCtxNoNeedToRestore`
- 4 fields in `TxnCtxNeedToRestore` (with 3 maps)
- `tdmLock` (line 238) acquired for EVERY delta update - OLTP hot path

### LazyTxn (txn.go:49-77)
- `mu struct { RWMutex; TxnInfo }` for lock view
- `updateState()` called frequently, allocates metrics observations

### Plan Cache (session.go:437-441)
```go
if s.sessionPlanCache == nil {
    s.sessionPlanCache = plannercore.NewLRUPlanCache(
        uint(s.GetSessionVars().SessionPlanCacheSize), ...)
}
```
Lazy initialization means first query pays construction cost.

Multiple cleanup paths (lines 1679-1680, 2504-2505, 2523-2524) all do `DeleteAll()`.

## DDL Package

### executor.go (7,201 lines)
Largest file in codebase. 100+ methods for all DDL operations.

**Recommended split**:
- `executor_schema.go` - CREATE/DROP/ALTER SCHEMA
- `executor_table.go` - CREATE/DROP/ALTER TABLE
- `executor_column.go` - ADD/DROP/MODIFY COLUMN
- `executor_index.go` - ADD/DROP INDEX
- `executor_partition.go` - Partition operations

### Schema Version Lock (CRITICAL)
**File**: `pkg/ddl/ddl.go:387-445`
```go
type schemaVersionManager struct {
    schemaVersionMu sync.Mutex  // GLOBAL lock for ALL DDL
    lockOwner       int64
    // ...
}
```
- `lockSchemaVersion()` blocks ALL other DDL jobs (line 427)
- Lock held during entire schema version transaction (lines 408-417)
- Every schema version increment blocks all concurrent DDL

### DDL Context Locking
**File**: `pkg/ddl/ddl.go:347-373`

Multiple lock acquisition patterns:
- `jobCtx.Lock/Unlock` in `attachTopProfilingInfo()` (lines 457-458)
- `jobCtx.Lock/Unlock` in `setDDLSourceForDiagnosis()` (lines 468-469)
- `jobCtx.Lock/Unlock` in `getResourceGroupTaggerForTopSQL()` (lines 478-480)

### Reorg Context Reference Counting
**File**: `pkg/ddl/ddl.go:623-652`

Manual reference counting without panic safety:
```go
func (dc *ddlCtx) newReorgCtx(jobID int64, ...) *reorgCtx {
    rc.references.Add(1)
    dc.reorgCtx.reorgCtxMap[jobID] = rc
}
func (dc *ddlCtx) removeReorgCtx(jobID int64) {
    ctx.references.Sub(1)
    if ctx.references.Load() == 0 {
        delete(dc.reorgCtx.reorgCtxMap, jobID)
    }
}
```
If a job panics, reference count may never reach 0 -> memory leak.

### Job Callback Channel Leak
**File**: `pkg/ddl/ddl.go:214-227`
```go
type JobWrapper struct {
    ResultCh []chan jobSubmitResult  // slice of channels
}
```
When jobs are merged, multiple channels appended. If cancelled, channels may
not be drained -> goroutine leaks.

### Backfilling Warning Map Contention
**File**: `pkg/ddl/backfilling.go:159-173`
Per-job warnings map shared by 10+ backfill workers without fine-grained sync.

## Server Package

### clientConn Struct (conn.go:165-209)
26 fields including nested locks:
```go
ctx struct { sync.RWMutex; *TiDBContext }  // line 181
mu struct { sync.RWMutex; cancelFunc }     // line 199-202
```

### Connection Close Race
**File**: `pkg/server/conn.go:378-429`
```go
func (cc *clientConn) Close() error {
    cc.server.rwlock.Lock()
    delete(cc.server.clients, cc.connectionID)
    cc.server.rwlock.Unlock()
    return closeConn(cc)  // Called AFTER releasing server lock
}
```
`closeConn` called without server lock protection. Multiple goroutines can
call Close() concurrently. Session close is not protected.

### Protocol Parsing Allocations
- `writeInitialHandshake()` line 454: `make([]byte, 4, 128)` heap allocation
  instead of using arena allocator
- `authSwitchRequest()` line 276: Each auth attempt reallocates

### Buffer Management
- `alloc arena.Allocator` (line 175) - 32KB minimum
- `chunkAlloc chunk.Allocator` (line 176) - cleanup depends on GC
- No explicit buffer cleanup in clientConn lifecycle

## Domain Package

### Domain Struct (domain.go:147-236)
89 fields managing 8+ subsystems:

1. **Storage**: store, infoCache, etcdClient, autoidClient
2. **DDL**: ddl, ddlExecutor, ddlNotifier
3. **Session Pools**: 4 types (advancedSysSessionPool, sysSessionPool, etc.)
4. **Schema**: privHandle, bindHandle, isSyncer, globalCfgSyncer
5. **Background Workers**: 8+ (TTL, runaway, BR, stats, etc.)
6. **Metrics/Telemetry**: various tracking fields

### Info Schema Refresh
**File**: `domain.go:256-264`
```go
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (...) {
    if is := do.infoCache.GetBySnapshotTS(snapshotTS); is != nil {
        return is, nil  // cache hit
    }
    is, _, _, _, err := do.isSyncer.LoadWithTS(snapshotTS, true)  // SYNC etcd load
    return is, err
}
```
Cache miss = synchronous etcd load. No async pre-fetching.

### Dual Session Pools
- `advancedSysSessionPool` (line 164) - new pool type
- `sysSessionPool` (line 168) - deprecated but still allocated
- Both exist, increasing complexity

### Expired Timestamp Lock
Lines 200-205: Separate RWMutex protecting ONE field:
```go
expiredTimeStamp4PC struct {
    sync.RWMutex
    expiredTimeStamp types.Time
}
```
Overhead of a full RWMutex for a single timestamp field.
