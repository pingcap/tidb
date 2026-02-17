# Executor Package Issues

## God Files

| File | Lines | Issue |
|------|-------|-------|
| `builder.go` | 6,222 | 103-case switch for all executor types |
| `infoschema_reader.go` | 4,213 | All INFORMATION_SCHEMA table reads |
| `simple.go` | 3,198 | All "simple" statement execution |
| `show.go` | 2,825 | All SHOW statements |
| `adapter.go` | 2,417 | Query execution adapter |

## Builder Pattern Problem

**File**: `pkg/executor/builder.go` (lines 171-344)

103+ case branches dispatch to individual `buildXxx()` methods.
Import count: 100+ imports from 34 packages.

**Current**:
```go
func (b *executorBuilder) build(p base.Plan) exec.Executor {
    switch v := p.(type) {
    case *plannercore.Delete: return b.buildDelete(v)
    case *plannercore.Execute: return b.buildExecute(v)
    // ... 100+ more ...
    }
}
```

**Target**: Registry pattern where each executor file registers its own builder function.

## Hash Join V1/V2 Duplication

| File | Lines | Version |
|------|-------|---------|
| `join/hash_join_v1.go` | 1,458 | V1 |
| `join/hash_join_v2.go` | 1,538 | V2 |
| `join/hash_table_v1.go` | 720 | V1 hash table |
| `join/hash_table_v2.go` | 258 | V2 hash table |
| `join/joiner.go` | 1,084 | Shared |

~80% code overlap. Both active, switched by system variable.

**V2 missing support**: null-aware joins, cross joins, NullEQ joins.
Complete these, then remove V1.

## Executor Interface Overload

**File**: `internal/exec/executor.go` (lines 51-77)

11 methods mixing concerns:
- Lifecycle: `Open()`, `Close()`
- Execution: `Next()`
- Debugging: `HandleSQLKillerSignal()`
- Profiling: `RegisterSQLAndPlanInExecForTopProfiling()`
- Detachment: `DetachExecutor()`

**Also**: `BaseExecutor` vs `BaseExecutorV2` coexist. BaseExecutor adds only
`sessionctx.Context` to BaseExecutorV2. Causes confusion.

## Aggregate Executor Issues

**File**: `aggregate/agg_hash_executor.go`

### Memory: O(P*F) Map Proliferation
Lines 301-307: Per partial worker, creates `[finalConcurrency]AggPartialResultMapper`.
With defaults (P=4, F=4): 64 separate maps.

### Goroutine Management
Lines 550-627: 7 goroutines spawned per execution:
1. fetchChildData
2. partialWorker x partialConcurrency
3. finalWorker x finalConcurrency

No worker pool. Fresh goroutines every execution.

### Close() Safety
Line 164: `Close()` only guards with `prepared.CompareAndSwap`.
If `Open()` never completed, goroutines may leak.

## Sort Executor Issues

**File**: `sortexec/sort.go`

### Dual Code Paths
- Lines 64-65: `IsUnparallel` flag creates two separate paths
- Two `multiWayMerge` instances (lines 70-71)
- Two `spillHelper` instances
- Two `spillAction` types (lines 95-96)

### Channel Draining
Lines 114-144: Complex `Close()` with manual channel draining.
Line 126-129: Iteration through chunkChannel to drain.

## Concurrent Map Issues

**File**: `join/concurrent_map.go`
- Line 23: `ShardCount = 320` hardcoded
- Lines 82-91: `IterCb()` iterates all shards sequentially
- Line 32: RWMutex per shard

## Memory Management Gaps

### Missing Chunk Pooling
- `aggregate/agg_hash_executor.go:341-345`: New chunk per input without pooling
- `projection.go:84-85`: Unbounded projectionOutput channels

### Hash Join Memory
- `join/hash_join_v2.go:79-101`: hashTableContext.reset() manual tracker detach
- Lines 321-323: Parallel rowTable arrays with incomplete cleanup

## Hot-Path Issues Summary

| Location | Issue | Fix |
|----------|-------|-----|
| `select.go:259` | Iterator recreation per Next() | Cache on struct |
| `adapter.go:122` | ResultField slice per call | Cache on struct |
| `select.go:728` | selected slice re-init | Reuse existing |
| `select.go:263` | BuildHandle per column/row | Early exit |
| `distsql.go:161` | rebuildIndexRanges creates new slice | Pre-allocate |
| `internal/exec/executor.go` | reflect.TypeOf in Next() | Cache type string |
