# Architectural Design Issues

## 1. Bidirectional Planner-Executor Dependency

**Problem**: Planner imports executor implementation details, violating clean layering.

**Evidence**:
- `pkg/executor/builder.go` imports 8+ planner sub-packages (lines 65-72):
  - `planner/core`, `planner/core/base`, `planner/core/operator/logicalop`,
    `planner/core/operator/physicalop`, `planner/core/rule`, `planner/util`,
    `planner/util/coreusage`, `planner/util/partitionpruning`
- `pkg/planner/core/operator/physicalop/physical_hash_join.go` imports:
  - `executor/join/joinversion` (line 22)

**Impact**: Cannot evolve executor independently from planner. Changes in join
implementation force planner changes.

**Fix**: Introduce interface boundary. Move `joinversion` config to a shared
config/capability package that both can import.

## 2. Session/SessionCtx Fragmentation (6 Packages)

**Problem**: Session state split across 6 packages with unclear boundaries.

| Package | Purpose | Files |
|---------|---------|-------|
| `pkg/session/` | Session lifecycle, execution | session.go (5,035 lines) |
| `pkg/sessionctx/` | Context interface | context.go (76-method interface) |
| `pkg/sessionctx/variable/` | Variable management | session.go (3,853 lines, 350+ fields) |
| `pkg/sessionctx/stmtctx/` | Statement context | stmtctx.go |
| `pkg/sessionctx/vardef/` | Variable definitions | tidb_vars.go |
| `pkg/sessiontxn/` | Transaction management | multiple files |

**Impact**: Developers don't know where to add new session state. Related code is
scattered making it hard to understand the full session lifecycle.

**Fix**: Consolidate into fewer packages with clear ownership. Transaction management
should be a first-class sub-package of session.

## 3. pkg/util/ Dumping Ground (111+ Subdirectories)

**Problem**: No organizational principle. Unrelated functionality coexists:

**Examples of diverse contents**:
- `util/chunk/` - Core data structure for in-memory rows
- `util/codec/` - Encoding/decoding (1,827 lines)
- `util/ranger/` - Range calculation for index scan
- `util/hint/` - SQL hint processing
- `util/topsql/` - Top SQL monitoring
- `util/disttask/` - Distributed task framework
- `util/collate/` - Collation implementations

**Fix**: Promote domain-specific utilities to proper packages:
- `util/chunk/` -> `pkg/chunk/` (it's a core data structure, not a utility)
- `util/ranger/` -> `pkg/planner/ranger/` (planner-specific)
- `util/hint/` -> `pkg/planner/hint/` (planner-specific)

## 4. KV Abstraction Leakage

**Problem**: `pkg/kv/kv.go` directly re-exports TiKV client types:
```go
type ValueEntry = tikvstore.ValueEntry
type GetOption = tikvstore.GetOption
type BatchGetOption = tikvstore.BatchGetOption
```

**Impact**: TiKV-specific types leak into TiDB's public interface. Cannot swap
storage backends without changing all consumers.

**Fix**: Define TiDB-owned types and convert at the boundary.

## 5. Dead Code: Old Cascades Optimizer

**Location**: `pkg/planner/cascades/old/`

**Files**: `enforcer_rules.go`, `implementation_rules.go`, `optimize.go`, `stringer.go`

**Problem**: Old optimizer implementation left alongside newer implementation.
Confuses developers about which optimizer is actually used.

**Fix**: Remove `cascades/old/` entirely.

## 6. sessionctx.Context Interface Bloat

**File**: `pkg/sessionctx/context.go` (lines 79-167)

**Problem**: 76 methods mixing:
- Transaction management (RollbackTxn, CommitTxn, StmtCommit, StmtRollback)
- Execution (GetPlanCtx, GetDistSQLCtx, GetTableCtx)
- State (EncodeStates, DecodeStates, ShowProcess)
- Locks (GetAdvisoryLock, ReleaseAdvisoryLock)
- Statistics (GetStmtStats, ReportUsageStats)

**Fix**: Split into focused interfaces:
- `TxnContext` - transaction operations
- `ExecContext` - execution context getters
- `StatsContext` - statistics operations
- `LockContext` - lock operations

## 7. Expression Package Vectorization Sprawl

**Problem**: 128 files in `pkg/expression/` with 40+ `builtin_*_vec.go` files
duplicating scalar implementations. Generated files checked into version control.

**Pattern per function type**:
- `builtin_arithmetic.go` (scalar)
- `builtin_arithmetic_vec.go` (vectorized, hand-written)
- `builtin_arithmetic_vec_generated.go` (vectorized, generated)
- `builtin_arithmetic_vec_generated_test.go` (tests, generated)
- `builtin_threadsafe_generated.go` (thread safety, generated)

**Fix**: Unify scalar and vectorized paths. Consider trait-based approach where
each function implements a single interface that handles both paths.
