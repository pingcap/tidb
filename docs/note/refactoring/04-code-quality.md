# Code Quality and Technical Debt

## 1. Technical Debt Markers

### Counts
| Pattern | Count | Notes |
|---------|-------|-------|
| TODO | ~2,745 | Across all `pkg/` |
| FIXME | ~181 | Including critical ones in planner cardinality |
| nolint | 738 | Suppressed lint warnings across 291 files |
| HACK/XXX | ~59 | Various workarounds |

### Most Critical TODOs

| File | Issue |
|------|-------|
| `pkg/ddl/index.go` | 8+ TODOs for unsupported features (non-strict mode, global index, expressions) |
| `pkg/session/session.go` | 15+ TODOs (regexp pre-compilation, table lock cleanup) |
| `pkg/lightning/checkpoints/checkpoints.go` | 24 TODOs/FIXMEs in data integrity logic |
| `pkg/planner/cardinality/selectivity.go` | FIXMEs for statistics estimation edge cases |
| `pkg/planner/core/plan_cache_param.go:178` | "add sync.Pool for FieldType and Constant" - high-frequency allocations |

## 2. Panics in Production Code (81 instances)

### Critical Panics (Data Path)

| File | Lines | Panic Message |
|------|-------|---------------|
| `pkg/kv/key.go` | 223, 248, 253, 331, 351, 747 | 6 panics in key comparison |
| `pkg/session/txnmanager.go` | 115 | `"forUpdateTS not equal..."` |
| `pkg/session/session.go` | 4768 | `"tso shouldn't be requested"` |
| `pkg/server/http_status.go` | 703-734 | 5 panics for driver/domain errors |
| `pkg/resourcegroup/runaway/checker.go` | 392 | `"unknown type"` |
| `pkg/session/txninfo/txn_info.go` | 93, 96 | Array length assertions |

### Planner Panics

| File | Pattern |
|------|---------|
| `pkg/planner/core/exhaust_physical_plans.go:99` | `panic("unreachable")` in default case |
| Various rule files | `panic("unreachable")` pattern |

**Fix**: Replace with `errors.AssertionFailedf()` or proper error returns.

## 3. Context Propagation Issues

### Counts
| Pattern | Count | Impact |
|---------|-------|--------|
| `context.TODO()` | 649 | Known missing context |
| `context.Background()` | 4,617 | Potentially missing cancellation |
| **Total** | **5,266** | Weak context propagation |

### Critical Locations
- `pkg/session/session.go:2028` - `context.TODO()` for advisory locks (can't be cancelled)
- `pkg/session/session.go:3190` - `context.WithValue(context.TODO(), ...)` as base
- `pkg/ddl/placement_policy.go` - `context.TODO()` for range resets

## 4. Global Variables and init() Functions

| Metric | Count |
|--------|-------|
| Files with global `var` | 945 |
| `init()` functions | 121 across 138 files |

### Most Problematic
- `pkg/statistics/handle/` - Multiple pools and caches with global state
- `pkg/metrics/` - 7+ init() functions for metrics registration
- `pkg/util/memory/global_arbitrator.go` - Global memory management
- `pkg/parser/charset/` - Global charset registrations

**Impact**: Tests can interfere with each other. Requires careful initialization order.

## 5. Mutex vs RWMutex Usage

| Type | Count | Files |
|------|-------|-------|
| `sync.Mutex` | 255 | 172 files |
| `sync.RWMutex` | 131 | 96 files |

**Ratio**: 2:1 favoring exclusive Mutex. Many read-heavy patterns should use RWMutex:
- `pkg/domain/domain.go` - Multiple fields often read-only
- `pkg/statistics/handle/` - Stats are frequently read, rarely written
- `pkg/util/memory/tracker.go` - Memory tracking (3+ Mutex fields)

## 6. Deprecated Code Still in Use

57 deprecated markers across 32 files:
- `pkg/config/config.go` - 10 deprecated configuration options still referenced
- `pkg/meta/model/table.go` - 3 deprecated table model fields
- `pkg/meta/model/job.go` - 3 deprecated job fields
- `pkg/sessionctx/vardef/tidb_vars.go` - 8 deprecated session variables

## 7. Error Handling

### Error Swallowing (Low - 21 instances)
- `pkg/executor/insert.go:516` - `_ = errorHandler(...)`
- `pkg/store/mockstore/unistore/rpc.go:476` - `_ = err` in RPC handler
- `pkg/executor/compact_table.go:114` - `_ = g.Wait()`

### Good Practice
Error swallowing is relatively controlled (21 vs thousands of error checks).
The codebase generally handles errors responsibly.

## 8. Reflect Usage

487+ reflect operations across 87 files. Most are in tests, but one is on hot path:
- `pkg/executor/internal/exec/executor.go` - `reflect.TypeOf(e).String()` in every Next() call
