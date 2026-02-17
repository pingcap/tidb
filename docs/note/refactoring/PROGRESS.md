# Refactoring Progress Tracker

Last updated: 2026-02-17 (benchmark validation complete)

## Benchmark Validation (2026-02-17)

### Micro-benchmarks (benchstat, p=0.008, n=5)
- **Vectorized string functions**: -24% to -59% faster (SUBSTR, LOCATE, CHAR_LENGTH)
  - Substring2ArgsUTF8 Vec: -24.4%, Substring3ArgsUTF8 Vec: -38.5%
  - Locate3ArgsUTF8 Vec: -48.3% to -59.5%, Locate2ArgsUTF8 Vec: -7.6% to -15.0%
  - CharLengthUTF8 NonVec: -20.8%
  - Geomean: **-23.9% sec/op, -5.2% B/op, -4.3% allocs/op**
- **reflect.TypeOf caching**: 10.0ns → 5.65ns (**1.8x faster**)
- **Chunk iterator reuse**: negligible (compiler inlines allocation)

### go-ycsb macro (8 threads, 20k ops, 10k records, unistore)
- Workload A (50/50 read/update): ~50k OPS, no regression
- Workload C (100% read): ~78k OPS, no regression

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed
- `[-]` Skipped / Won't do

---

## P0 - Critical (Do First)

- [~] **executor/builder.go split** - Replace 103-case switch with registry/factory pattern
  - File: `pkg/executor/builder.go` (6,222 lines → 4,266 lines)
  - Target: Split into per-operator-type builder files with auto-registration
  - [x] Phase 1: Extract `buildMemTable` (292 lines) → `builder_memtable.go`
  - [x] Phase 1: Extract reader builders (1,652 lines) → `builder_reader.go` (buildNoRangeTableReader, buildTableReader, buildIndexReader, buildIndexLookUpReader, buildIndexMergeReader, buildMPPGather, dataReaderBuilder, all index join reader builders)
  - [ ] Phase 2: Extract join builders, analyze builders, remaining groups

- [ ] **Hash Join V1 deprecation** - Complete V2 for all join types, remove V1
  - Files: `pkg/executor/join/hash_join_v1.go` (1,458 lines), `hash_join_v2.go` (1,538 lines)
  - Target: Single implementation with full join type coverage

- [x] **Hot-path allocation fixes** - Pool iterators, pre-allocate slices
  - [x] `pkg/executor/select.go:259` - chunk iterator recreation per Next() → cached on struct
  - [-] `pkg/executor/adapter.go:122-157` - ResultField slice per Fields() call (already cached, no action needed)
  - [x] `pkg/executor/select.go:728-729` - selected slice re-init → reuse backing array
  - [x] `pkg/executor/internal/exec/executor.go:457` - reflect.TypeOf().String() per Next() → cached in sync.Map
  - [x] `pkg/expression/builtin_string_vec.go` - 17 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_string.go` - 16 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_encryption.go` - 1 []rune allocation replaced

- [x] **Panic elimination in production code** - Replace with error returns
  - [x] 8 runtime panics replaced: builder.go, analyze.go, index_merge_reader.go,
        aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
  - [-] `pkg/kv/key.go` - 6 panics are intentional interface contract assertions (Handle)
  - [-] `pkg/session/txnmanager.go` - panic is inside failpoint (test-only)
  - [-] `pkg/server/http_status.go` - startup-time assertions (acceptable)

## P1 - High Priority

- [ ] **session.go decomposition** - Extract transaction, variable, privilege management
  - File: `pkg/session/session.go` (5,035 lines)
  - Target: <1000 lines per file with clear sub-package boundaries

- [ ] **domain.go decomposition** - Extract subsystem managers
  - File: `pkg/domain/domain.go` (2,739 lines, 89 fields)
  - Target: Composable service managers

- [x] **Planner-executor dependency break** - Remove executor imports from planner
  - Moved `pkg/executor/join/joinversion` → `pkg/util/joinversion`
  - Updated all 11 Go importers; BUILD.bazel files need `make bazel_prepare`

- [ ] **Cost model unification** - Deprecate one of ver1/ver2
  - Files: `pkg/planner/core/plan_cost_ver1.go`, `plan_cost_ver2.go`
  - Target: Single cost model

- [~] **SessionVars decomposition** - Split 350+ field mega-struct
  - File: `pkg/sessionctx/variable/session.go` (3,853 lines)
  - Target: Grouped sub-structs by concern
  - [x] Phase 1: Extracted `TiFlashVars` (27 fields) and `CostModelFactors` (28 fields) as embedded sub-structs
  - [ ] Phase 2: Extract plan cache, optimizer, transaction settings

- [ ] **DDL schema version lock** - Reduce global mutex scope
  - File: `pkg/ddl/ddl.go:387-445`
  - Target: Per-job or fine-grained locking

## P2 - Medium Priority

- [ ] **pkg/util/ reorganization** - Re-organize 111+ subdirectories
- [x] **String function rune optimization** - Use utf8 index-based operations
- [ ] **Goroutine pool for executors** - Aggregate, sort, projection worker pools
- [ ] **Datum type optimization** - Eliminate interface{} fallback, use typed union
- [ ] **Expression clone generation** - Auto-generate all Clone() methods
- [ ] **Aggregate executor map reduction** - Reduce O(P*F) map proliferation
- [ ] **Concurrent hash map adaptive sharding** - Replace fixed ShardCount=320
- [ ] **Statistics memoization in planner** - Cache selectivity calculations

## P3 - Low Priority

- [ ] **Remove old cascades code** - `pkg/planner/cascades/old/`
- [ ] **Executor interface split** - Lifecycle, Execution, Debug interfaces
- [ ] **Context propagation** - Replace 5,266 Background()/TODO() calls
- [ ] **DDL executor.go split** - `pkg/ddl/executor.go` (7,201 lines)
- [ ] **sessionctx.Context interface** - Split 76-method interface
- [ ] **nolint audit** - Investigate 738 suppressed warnings

---

## Completed Items

(Move items here when done, with date and PR link)

- [x] **SelectLockExec iterator caching** - 2026-02-17 - Cache `chunk.Iterator4Chunk` on struct to avoid heap allocation per `Next()` call
- [x] **SelectionExec selected slice reuse** - 2026-02-17 - Reuse `[]bool` backing array across `Open()`/`Close()` cycles instead of re-allocating
- [x] **exec.Next reflect.TypeOf caching** - 2026-02-17 - Cache `reflect.TypeOf(e).String()+".Next"` in `sync.Map` to avoid per-call reflection + string concatenation
- [x] **String function rune optimization** - 2026-02-17 - Replace 27 `[]rune(str)` allocations with `utf8.RuneCountInString` and `utf8.DecodeRuneInString` for zero-copy operations in LEFT, RIGHT, LOCATE, SUBSTR, INSERT, MID, LPAD, RPAD, CHAR_LENGTH
- [x] **Planner panic elimination** - 2026-02-17 - Replace `panic("unreachable")` with error return in `exhaustPhysicalPlans`
- [x] **Planner-executor dependency break** - 2026-02-17 - Move `joinversion` package from `executor/join/joinversion` to `util/joinversion`, eliminating planner→executor import
- [x] **Runtime panic elimination (8 panics)** - 2026-02-17 - Replace panics in builder.go, analyze.go, index_merge_reader.go, aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
- [x] **Additional rune optimizations (6 patterns)** - 2026-02-17 - SUBSTRING non-vec, INSERT non-vec, Quote, WeightString, ValidatePasswordStrength
- [x] **SessionVars decomposition phase 1** - 2026-02-17 - Extract `TiFlashVars` (27 MPP/TiFlash fields) and `CostModelFactors` (28 cost factor fields) into embedded sub-structs, reducing SessionVars from 315 to ~260 direct fields
- [x] **executor/builder.go split phase 1** - 2026-02-17 - Extract `buildMemTable` (292 lines) → `builder_memtable.go` and reader builders (1,652 lines) → `builder_reader.go`, reducing builder.go from 6,223 to 4,266 lines
