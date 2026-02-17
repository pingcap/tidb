# God Files Analysis

Files exceeding 2,000 lines that need splitting due to mixed responsibilities.

## Ranked by Size

| Rank | File | Lines | Package |
|------|------|-------|---------|
| 1 | `pkg/expression/builtin_time.go` | 7,260 | expression |
| 2 | `pkg/planner/core/logical_plan_builder.go` | 7,362 | planner |
| 3 | `pkg/ddl/executor.go` | 7,201 | ddl |
| 4 | `pkg/planner/core/planbuilder.go` | 6,518 | planner |
| 5 | `pkg/executor/builder.go` | 6,222 | executor |
| 6 | `pkg/expression/builtin_string.go` | 5,622 | expression |
| 7 | `pkg/session/session.go` | 5,035 | session |
| 8 | `pkg/executor/infoschema_reader.go` | 4,213 | executor |
| 9 | `pkg/expression/builtin_compare.go` | 3,542 | expression |
| 10 | `pkg/sessionctx/variable/session.go` | 3,853 | sessionctx |
| 11 | `pkg/types/time.go` | 3,500+ | types |
| 12 | `pkg/executor/simple.go` | 3,198 | executor |
| 13 | `pkg/planner/core/find_best_task.go` | 3,030 | planner |
| 14 | `pkg/executor/show.go` | 2,825 | executor |
| 15 | `pkg/domain/domain.go` | 2,739 | domain |
| 16 | `pkg/executor/adapter.go` | 2,417 | executor |
| 17 | `pkg/planner/core/rule/rule_partition_processor.go` | 2,143 | planner |
| 18 | `pkg/planner/core/operator/logicalop/logical_join.go` | 2,172 | planner |

## Detailed Issues

### pkg/executor/builder.go (6,222 lines)

**Problem**: 103+ `case` branches in a single `switch` statement (lines 171-344).
Every executor type has a `buildXxx()` method, all dispatched from one function.

**Current structure**:
```
func (b *executorBuilder) build(p base.Plan) exec.Executor {
    switch v := p.(type) {
    case *plannercore.Delete:        return b.buildDelete(v)
    case *plannercore.Execute:       return b.buildExecute(v)
    case *plannercore.Trace:         return b.buildTrace(v)
    // ... 100+ more cases ...
    }
}
```

**Recommended approach**: Registry pattern where each executor type registers its own builder:
```
// Each file registers itself
func init() { RegisterBuilder(reflect.TypeOf(&plannercore.Delete{}), buildDelete) }
```

**Imports**: 100+ imports across 34 packages (lines 17-100), showing extreme coupling.

### pkg/ddl/executor.go (7,201 lines)

**Problem**: All DDL execution methods (CreateSchema, AlterTable, AddColumn, DropIndex, etc.)
in a single file on a single `Executor` receiver.

**Recommended split**:
- `executor_schema.go` - CREATE/DROP/ALTER SCHEMA
- `executor_table.go` - CREATE/DROP/ALTER TABLE
- `executor_column.go` - ADD/DROP/MODIFY COLUMN
- `executor_index.go` - ADD/DROP INDEX
- `executor_partition.go` - Partition operations
- `executor_constraint.go` - Constraints and foreign keys

### pkg/session/session.go (5,035 lines)

**Problem**: Central session object mixing lifecycle, transaction, variables, privilege, and plan cache.

**Session struct fields** (lines 190-261):
- `sessionPlanCache` - plan cache
- `sessionVars` - all session variables (itself 350+ fields)
- `pctx`, `exprctx`, `tblctx` - multiple context objects
- `statsCollector`, `idxUsageCollector`, `stmtStats` - statistics
- `lockedTables`, `advisoryLocks` - lock tracking

**Recommended split**:
- `session_lifecycle.go` - Open/Close/Reset
- `session_txn.go` - Transaction management (partially exists in txn.go)
- `session_execute.go` - Statement execution
- `session_vars.go` - Variable management

### pkg/planner/core/logical_plan_builder.go (7,362 lines)

**Problem**: Monolithic file handling entire logical plan building with 80+ imports.

**Recommended split by SQL clause**:
- `builder_select.go` - SELECT/UNION plan building
- `builder_join.go` - JOIN plan building
- `builder_agg.go` - GROUP BY/HAVING/aggregation
- `builder_subquery.go` - Subquery handling
- `builder_dml.go` - INSERT/UPDATE/DELETE

### pkg/sessionctx/variable/session.go (3,853 lines)

**Problem**: `SessionVars` struct has 350+ fields (lines 780-1129).

**Field categories that should be separate structs**:
- Lines 792-806: System variable storage (maps + hot-path extracted values)
- Lines 815-818: Transaction context (`TxnCtx`, `TxnCtxMu`)
- Lines 810-812: Prepared statements (`PreparedStmts`, `PreparedStmtNameToID`)
- Lines 945-1107: 100+ optimizer tuning parameters
- Lines 1070-1087: Cost factors (9 fields)

### pkg/domain/domain.go (2,739 lines)

**Problem**: 89 fields managing 8+ subsystems.

**Domain struct** (lines 147-236) manages:
- Storage layer (store, infoCache, etcdClient)
- DDL management (ddl, ddlExecutor, ddlNotifier)
- Session pools (4 different pool types)
- Schema management (5 fields)
- Background workers (8+ fields: TTL, runaway, BR, stats, etc.)
- Telemetry, metrics, resource groups

**Recommended**: Extract each subsystem into its own manager with Domain as coordinator.
