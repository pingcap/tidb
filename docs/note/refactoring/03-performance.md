# Performance Issues

## 1. Hot-Path Allocations in Executor (CRITICAL)

### 1.1 Chunk Iterator Recreation per Next()
**File**: `pkg/executor/select.go:259`
```go
// Called EVERY Next() - should be cached on the executor struct
iter := chunk.NewIterator4Chunk(req)
```
**Fix**: Store iterator as field on SelectLockExec, reset instead of recreating.

### 1.2 ResultField Slice per Fields() Call
**File**: `pkg/executor/adapter.go:122-157`
```go
// Called for every query result
rfs := make([]*resolve.ResultField, 0, schema.Len())
```
**Fix**: Cache on recordSet struct, rebuild only when schema changes.

### 1.3 Selected Slice Re-initialization
**File**: `pkg/executor/select.go:728-729`
```go
e.selected = e.selected[:0:cap(e.selected)]
// But then: e.selected, err = expression.VectorizedFilter(...)
// VectorizedFilter may allocate new slice
```
**Fix**: Pass pre-allocated slice to VectorizedFilter for reuse.

### 1.4 BuildHandle() per Column per Row
**File**: `pkg/executor/select.go:263-265`
```go
for row := iter.Begin(); row != iter.End(); row = iter.Next() {
    // BuildHandle called for every row without early exit
}
```

### 1.5 Reflect on Every Executor Next()
**File**: `pkg/executor/internal/exec/executor.go`
```go
r, ctx := tracing.StartRegionEx(ctx, reflect.TypeOf(e).String()+".Next")
```
**Fix**: Cache the type string on the executor at construction time.

## 2. String Function Rune Allocations (CRITICAL)

**File**: `pkg/expression/builtin_string_vec.go`

20+ locations perform expensive `[]rune(str)` in vectorized loops:

| Line | Function | Issue |
|------|----------|-------|
| 220 | builtinLeftUTF8Sig | `[]rune(str)` + `string(runes[:n])` = 2 allocs/row |
| 265 | builtinRightUTF8Sig | `[]rune(str)` per row |
| 335 | builtinReverseUTF8Sig | `reverseRunes([]rune(str))` per row |
| 439-440 | substring ops | Double rune alloc: `len([]rune(str))` + `len([]rune(subStr))` |
| 448 | substring | `string([]rune(str)[pos:])` back-conversion |
| 825-846 | builtinSubstringIndexSig | `strings.Split()` + `strings.Join()` per row |
| 1096-1097 | builtinLpadUTF8Sig | Two `len([]rune(...))` calls |
| 1109 | builtinLpadUTF8Sig | `string([]rune(strings.Repeat(...))[:tailLen])` |
| 1651, 1915, 2053 | various | Additional rune conversions |

**Fix**: Use `utf8.RuneCountInString()` for length. Use index-based UTF-8 traversal
(`utf8.DecodeRuneInString()`) instead of full `[]rune()` conversion.

## 3. Hash Join V1/V2 Duplication

**Files**:
- `pkg/executor/join/hash_join_v1.go` (1,458 lines)
- `pkg/executor/join/hash_join_v2.go` (1,538 lines)
- `pkg/executor/join/hash_table_v1.go` (720 lines)
- `pkg/executor/join/hash_table_v2.go` (258 lines)

**Problem**: ~80% code overlap. Both maintained indefinitely, switched by system variable.
No migration or deprecation path.

**Fix**: Complete V2 for all join types (including null-aware, cross join, NullEQ),
then remove V1.

## 4. Concurrent Hash Map Fixed Sharding

**File**: `pkg/executor/join/concurrent_map.go:23`
```go
ShardCount = 320 // hardcoded, no adaptive tuning
```

**Additional issues**:
- `IterCb()` (lines 82-91) iterates all 320 shards sequentially under lock
- RWMutex per shard (line 32) - coarse-grained

**Fix**: Adaptive shard count based on expected key count. Consider lock-free
alternatives for read-heavy patterns.

## 5. Aggregate Executor Memory Overhead

**File**: `pkg/executor/aggregate/agg_hash_executor.go`

**Issues**:
- Lines 301-307: Creates `partialResultsMap[finalConcurrency]AggPartialResultMapper`
  per partial worker. Default 4 partial x 4 final = 64 separate maps.
- Lines 550-627: 7 goroutines spawned per execution without worker pool.
- Lines 108-112: Multiple channel arrays created in Open().
- Lines 386-388: `partialInputChs` each with capacity 1 (conservative).

**Fix**: Shared result map with partitioned access. Worker pool for goroutine reuse.

## 6. Sort Executor Dual Code Paths

**File**: `pkg/executor/sortexec/sort.go`

**Problem**: Completely separate code for parallel vs unparallel:
- Two `multiWayMerge` instances (lines 70-71)
- Two `spillHelper` instances
- Two `spillAction` types (lines 95-96)
- No shared infrastructure

**Fix**: Unify into single path with configurable parallelism.

## 7. Planner Algorithm Complexity

### 7.1 O(n^2) Candidate Comparison
**File**: `pkg/planner/core/find_best_task.go:829`
```go
func compareCandidates(...) // Called in nested loops with histogram lookups
```

### 7.2 Selectivity Bitmask Limitation
**File**: `pkg/planner/cardinality/selectivity.go:67-70`
```go
// TODO: If len(exprs) is bigger than 63, we could use bitset structure
// Falls back to pseudo-selectivity for >63 expressions
```

### 7.3 Missing Memoization
**File**: `pkg/planner/core/find_best_task.go`
`cardinality.Selectivity()` called per candidate path without caching for identical
expression sets. Each call recalculates from scratch.

**Fix**: Memoize selectivity results keyed by expression set hash.

## 8. DDL Schema Version Global Lock

**File**: `pkg/ddl/ddl.go:387-445`
```go
type schemaVersionManager struct {
    schemaVersionMu sync.Mutex  // GLOBAL LOCK for ALL DDL jobs
    // ...
}
```
- `lockSchemaVersion()` blocks ALL other DDL jobs (line 427)
- Lock held during entire transaction (lines 408-417)

**Fix**: Per-table or per-schema locking instead of global.

## 9. TableDeltaMap Lock on OLTP Hot Path

**File**: `pkg/sessionctx/variable/session.go:369-383`
```go
func (tc *TransactionContext) UpdateDeltaForTable(...) {
    tc.tdmLock.Lock()  // Acquired for EVERY row delta update
    defer tc.tdmLock.Unlock()
    // ...
}
```

**Fix**: Batch delta updates. Use lock-free accumulator or per-table counters.

## 10. Datum Type Boxing Overhead

**File**: `pkg/types/datum.go`

**Datum struct** (72 bytes per instance):
```go
type Datum struct {
    k         byte     // 1 byte
    decimal   uint16   // 2 bytes
    length    uint32   // 4 bytes
    i         int64    // 8 bytes
    collation string   // 16 bytes (could be collation ID = 2 bytes)
    b         []byte   // 24 bytes
    x         any      // 16 bytes (boxing overhead)
}
```

**Issues**:
- `any` field requires type assertion for MyDecimal, Time, Duration access
- `collation` as string wastes 16 bytes (could be uint16 collation ID)
- `SetValueWithDefaultCollation()` has 27-case type switch (lines 595-642)
- `SetValue()` duplicates the same switch (lines 644-689)

**Fix**: Replace `any` with tagged union. Use collation ID instead of string.

## 11. Goroutine Leak Risks

| Executor | File | Issue |
|----------|------|-------|
| HashAgg | `aggregate/agg_hash_executor.go` | 7 goroutines, Close() only guards with CompareAndSwap |
| Projection | `projection.go` | fetcher + N workers, channel closure not fully verified |
| IndexMerge | `index_merge_reader.go` | 3 WaitGroups, complex lifecycle |
| Sort | `sortexec/sort.go` | Manual chunkChannel draining |

**Fix**: Bounded worker pools with guaranteed cleanup via context cancellation.
