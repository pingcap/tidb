# Prioritized Refactoring Plan

## P0 - Critical (Highest Impact, Reasonable Effort)

### P0-1: Fix Hot-Path Allocations
**Impact**: Direct query latency improvement for ALL queries.
**Effort**: Small (1-2 days each).

| # | Location | Fix | Est. Impact |
|---|----------|-----|-------------|
| 1 | `executor/select.go:259` | Cache chunk iterator on struct | Every SELECT with locks |
| 2 | `executor/adapter.go:122` | Cache ResultField slice | Every query result |
| 3 | `executor/select.go:728` | Reuse selected slice | Every filtered scan |
| 4 | `executor/internal/exec/executor.go` | Cache reflect.TypeOf string | Every Next() with tracing |

### P0-2: String Function Rune Optimization
**Impact**: 2-10x improvement for UTF-8 string operations.
**Effort**: Medium (3-5 days).

Replace `[]rune(str)` in 20+ locations in `expression/builtin_string_vec.go` with:
- `utf8.RuneCountInString()` for length
- `utf8.DecodeRuneInString()` for index-based traversal
- Direct byte-slice substring for zero-copy results

### P0-3: Eliminate Production Panics
**Impact**: Server stability.
**Effort**: Medium (3-5 days for 81 panics).

Priority targets:
1. `pkg/kv/key.go` - 6 panics on data path -> return error
2. `pkg/session/txnmanager.go:115` - assertion panic -> log + return error
3. `pkg/server/http_status.go` - 5 panics -> graceful error response
4. `pkg/planner/core/exhaust_physical_plans.go:99` - `panic("unreachable")` -> error

### P0-4: Hash Join V1 Deprecation Path
**Impact**: Remove 3,000 lines of duplicate code; simplify maintenance.
**Effort**: Large (2-4 weeks).

Steps:
1. Audit V2 missing join types: null-aware, cross join, NullEQ
2. Implement missing types in V2
3. Run full test suite with V2-only
4. Add deprecation warning for V1 system variable
5. Remove V1 code

## P1 - High Priority (Significant Impact, Medium-Large Effort)

### P1-1: executor/builder.go Registry Pattern
**Effort**: Large (1-2 weeks).

Steps:
1. Define `BuilderFunc` type: `func(*executorBuilder, base.Plan) exec.Executor`
2. Create `builderRegistry map[reflect.Type]BuilderFunc`
3. Move each `buildXxx()` to its own file, register in `init()`
4. Replace switch with registry lookup
5. Update all tests

### P1-2: Cost Model Unification
**Effort**: Large (2-3 weeks).

Steps:
1. Audit ver1 vs ver2 accuracy with benchmark suite
2. Document cases where they diverge
3. Fix ver2 to handle ver1 compatibility cases
4. Deprecate ver1 behind feature flag
5. Remove ver1

### P1-3: SessionVars Decomposition
**Effort**: Large (2-3 weeks).

Split 350+ fields into focused sub-structs:
```go
type SessionVars struct {
    SysVars    *SystemVariables    // system variable storage
    TxnState   *TransactionState   // transaction context
    PrepState  *PreparedState      // prepared statements
    OptConfig  *OptimizerConfig    // 100+ tuning params
    CostConfig *CostFactors        // 9 cost factors
    StmtState  *StatementState     // per-statement state
}
```

### P1-4: DDL Schema Version Lock Scope
**Effort**: Medium (1-2 weeks).

Replace global `schemaVersionMu` with per-table or per-schema locking.
Requires careful analysis of invariants.

### P1-5: Planner Statistics Memoization
**Effort**: Medium (1-2 weeks).

Add memoization for `cardinality.Selectivity()`:
```go
type selectivityCache struct {
    mu    sync.RWMutex
    cache map[uint64]float64  // expression set hash -> selectivity
}
```

### P1-6: Planner-Executor Dependency Break
**Effort**: Small-Medium (3-5 days).

Move `joinversion` config to shared package:
`pkg/planner/core/operator/physicalop/physical_hash_join.go` imports `executor/join/joinversion`
-> Move joinversion to `pkg/config/` or `pkg/planner/property/`

## P2 - Medium Priority

### P2-1: DDL executor.go Split
Split 7,201-line file by operation type (schema, table, column, index, partition).

### P2-2: session.go Decomposition
Split into lifecycle, transaction, execution, variable management.

### P2-3: domain.go Decomposition
Extract subsystem managers (stats, DDL, schema sync, TTL, etc.).

### P2-4: Goroutine Pool for Executors
Replace per-execution goroutine spawning with bounded worker pools in:
- Hash aggregate (7 goroutines per execution)
- Projection (1 fetcher + N workers)
- Sort (fetcher + sort workers)

### P2-5: Aggregate Executor Map Reduction
Replace O(P*F) maps with shared result map with partitioned access.

### P2-6: Concurrent Map Adaptive Sharding
Replace fixed `ShardCount=320` with adaptive sizing.

### P2-7: Datum Type Optimization
- Replace `collation string` with `collationID uint16`
- Replace `any` field with typed union (saves 14 bytes + removes boxing)
- Merge duplicate `SetValue`/`SetValueWithDefaultCollation`

### P2-8: Expression Clone Generation
Extend code generation to cover all 71+ Clone() methods.

## P3 - Low Priority (Cleanup)

### P3-1: Remove old cascades code (`planner/cascades/old/`)
### P3-2: Executor interface segregation (split 11-method interface)
### P3-3: Context propagation (5,266 Background/TODO calls)
### P3-4: sessionctx.Context interface split (76 methods)
### P3-5: nolint audit (738 suppressed warnings)
### P3-6: pkg/util/ reorganization (111+ subdirectories)
### P3-7: Sort executor path unification (merge parallel/unparallel)

## Execution Order

Recommended sequence for maximum impact with minimum risk:

```
Week 1-2:  P0-1 (hot-path fixes) + P0-3 (panic elimination)
Week 2-3:  P0-2 (rune optimization) + P1-6 (dependency break)
Week 3-5:  P1-1 (builder registry) + P1-5 (stats memoization)
Week 5-7:  P0-4 (hash join V1 deprecation)
Week 7-9:  P1-2 (cost model) + P1-3 (SessionVars split)
Week 9-10: P1-4 (DDL lock) + P2-1 (DDL split)
Week 10+:  P2 and P3 items as bandwidth allows
```
