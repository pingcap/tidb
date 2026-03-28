# Proposal: Max Keys Read Limit for SELECT Queries

- Author(s): @Tema
- Tracking Issue: https://github.com/pingcap/tidb/issues/66925

## Table of Contents

<!-- TOC -->
* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Architecture Overview](#architecture-overview)
    * [User Interface](#user-interface)
    * [Session Variable](#session-variable)
    * [Query Hint via SET_VAR](#query-hint-via-set_var)
    * [Plan Cache Compatibility](#plan-cache-compatibility)
    * [Status Variable](#status-variable)
    * [Protobuf Changes](#protobuf-changes)
    * [TiDB Coprocessor Dispatch](#tidb-coprocessor-dispatch)
    * [TiKV Coprocessor Enforcement](#tikv-coprocessor-enforcement)
    * [Error Handling](#error-handling)
    * [Compatibility](#compatibility)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
    * [Relationship with PROCESSED_KEYS in Runaway Queries](#relationship-with-processed_keys-in-runaway-queries)
    * [MariaDB LIMIT ROWS EXAMINED](#mariadb-limit-rows-examined)
    * [Alternative: TiDB-Only Enforcement](#alternative-tidb-only-enforcement-no-tikv-pushdown)
    * [Alternative: Limit in tipb.DAGRequest](#alternative-limit-in-tipbdagrequest-instead-of-coprocessorrequest)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This proposal introduces a `tidb_max_keys_read` session variable and corresponding `SET_VAR` hint that limits the number of storage engine keys read during `SELECT` query execution. The counter tracks keys processed by the TiKV coprocessor — this includes both index keys and table row keys, so a row accessed via an index lookup counts as two keys. The limit is pushed down to TiKV's coprocessor layer for efficient early termination. When the limit is exceeded, the query is terminated with an error. The feature is inspired by MariaDB's [`LIMIT ROWS EXAMINED`](https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/limit-rows-examined) and similar row-examination limiting features present in other MySQL-compatible databases.

## Motivation or Background

Long-running analytical or poorly-optimized `SELECT` queries can examine millions of rows and consume excessive CPU, memory, and I/O resources on both TiDB and TiKV nodes. In a distributed database, a single runaway query can saturate coprocessor threads across multiple TiKV instances, degrading the performance of the entire cluster and impacting other workloads.

Today, TiDB provides `max_execution_time` to limit query duration, but time-based limits are imprecise: a query scanning many rows on fast NVMe storage may finish quickly, while a query scanning few rows on slow or contended storage may time out. A key-based limit provides a **deterministic bound on resource consumption** that is independent of hardware performance.

TiDB also provides `PROCESSED_KEYS` thresholds via the [resource control runaway queries](https://docs.pingcap.com/tidb/stable/tidb-resource-control-runaway-queries/) framework. However, as detailed in the [Investigation & Alternatives](#relationship-with-processed_keys-in-runaway-queries) section, that mechanism operates at the resource-group level and requires DBA-managed cluster configuration. `tidb_max_keys_read` fills a distinct gap as a lightweight, self-service, application-level control.

### Use cases

1. **Resource protection**: Prevent ad-hoc queries from scanning entire tables in production OLTP clusters. An application sets a session-level default (e.g., 100,000 keys) so that no single query can accidentally perform a full table scan on a billion-row table.

2. **Multi-tenant isolation**: In a shared TiDB cluster, different application sessions can have different limits appropriate for their workload class. A reporting service may have a higher limit than a latency-sensitive API service.

3. **Per-query safety nets**: Using `SET_VAR` hints, developers can annotate individual queries with specific key-scan budgets directly in application code, without DBA involvement.

4. **Query tuning and observability**: The companion `tidb_keys_examined` status variable lets developers measure how many keys each query (and each session) actually examines, enabling data-driven query optimization.

5. **MySQL ecosystem migration**: Databases in the MySQL ecosystem (MariaDB's `LIMIT ROWS EXAMINED`, and similar features in other MySQL-compatible databases) provide row-examination limits. TiDB users migrating from these systems expect feature parity.

## Detailed Design

### Architecture Overview

The feature spans three layers: the TiDB SQL layer (variable management, hint parsing, query orchestration), the TiDB coprocessor client (per-task limit propagation and global accumulation), and the TiKV coprocessor (storage-level enforcement).

```
SQL: SELECT /*+ SET_VAR(tidb_max_keys_read=100) */ * FROM t

TiDB Layer:
  SessionVars.MaxKeysRead = 100 (via SET_VAR or SET SESSION)
      ↓
  DistSQLContext.MaxKeysRead = 100
      ↓
  kv.Request.MaxKeysRead = 100
      ↓
  copIterator: tracks cumulative keysRead (atomic counter)
      ↓
  copIteratorTaskSender: per task, sets
    task.maxKeysRead = max_keys_read - keysRead  (remaining keys, full budget per task)
      ↓
  handleTaskOnce(): sets copReq.MaxKeysRead = task.maxKeysRead

TiKV Layer:
  Coprocessor reads Request.tidb_max_keys_read
      ↓
  BatchExecutorsRunner enforces per-task limit during scanning
      ↓
  Returns partial result when per-task limit reached

TiDB Layer (response):
  copIterator.Next(): accumulates keys read from ScanDetail.ProcessedKeys
      ↓
  When cumulative >= tidb_max_keys_read → return error to client
```

The dual enforcement (TiKV stops scanning early, TiDB makes the final determination) ensures both efficiency (TiKV does not waste work) and correctness (TiDB has the global view across all tasks).

### User Interface

#### Setting the Limit

```sql
-- Session variable (applies to all SELECT queries in this session)
SET SESSION tidb_max_keys_read = 1000;

-- Global default (applies to all new sessions)
SET GLOBAL tidb_max_keys_read = 1000;

-- Per-query hint (overrides session variable for this query only)
SELECT /*+ SET_VAR(tidb_max_keys_read=500) */ * FROM orders WHERE status = 'pending';

-- Disable for one query when session has a limit
SELECT /*+ SET_VAR(tidb_max_keys_read=0) */ * FROM orders;
```

**Precedence**: `SET_VAR` hint > Session variable > Global default. Value `0` means unlimited (default).

#### When the Limit Is Reached

```
ERROR 8270 (HY000): Query execution was interrupted due to reaching tidb_max_keys_read limit
```

The transaction remains active. The client can continue with other SQL statements. This matches the behavior of `max_execution_time` — the error aborts the statement, not the transaction.

#### Monitoring Keys Examined

```sql
-- Check cumulative keys examined in this session
SHOW STATUS LIKE 'tidb_keys_examined';

-- Reset the counter
FLUSH STATUS;
```

> **Note**: `tidb_keys_examined` is a new status variable introduced by this proposal. TiDB's `FLUSH STATUS` support is limited compared to MySQL — the reset mechanism for this counter needs to be verified or an alternative (e.g., session variable reset on new connection) should be considered.

### Session Variable

| Property        | Value            |
|-----------------|------------------|
| Name            | `tidb_max_keys_read` |
| Scope           | Global + Session |
| Type            | Unsigned integer |
| Default         | `0` (unlimited)  |
| Min             | `0`              |
| Max             | `math.MaxUint64` |
| SET_VAR hint    | Supported        |

### Query Hint via SET_VAR

No parser changes are needed. The existing `SET_VAR` hint mechanism in TiDB already supports session variables that are marked as hint-updatable (`IsHintUpdatableVerified`). The new `tidb_max_keys_read` variable must be registered in this set (in `pkg/sessionctx/variable/setvar_affect.go`).

The flow:

1. Parser extracts `SET_VAR(tidb_max_keys_read=N)` from the SQL hint comment.
2. `ParseStmtHints()` validates via `setVarHintChecker` (checks the variable exists and is hint-updatable).
3. Planner applies `SetSystemVar("tidb_max_keys_read", "N")` which calls the `SetSession` hook.
4. After the statement completes, the old value is restored from `SetVarHintRestore`.

### Plan Cache Compatibility

Queries using `SET_VAR(tidb_max_keys_read=N)` are **fully compatible** with both the prepared and non-prepared plan caches. No special handling is needed because
`tidb_max_keys_read` does not influence the query plan — it is an execution-time limit applied in the coprocessor dispatch layer (`copIterator`).

#### Trade-off

Different `SET_VAR(tidb_max_keys_read=N)` values produce separate cache entries with identical plans. For example, a query executed with `N=100` and then `N=200`
will create two cache entries storing the same plan but different `StmtHints`. This is a minor inefficiency in cache utilization, acceptable because:

- It is consistent with how all other SET_VAR variables work in TiDB.
- The plan cache has eviction policies that handle this naturally.
- In practice, applications typically use a small number of distinct limit values.

### Status Variable

| Property   | Value                                                              |
|------------|--------------------------------------------------------------------|
| Name       | `tidb_keys_examined`                                               |
| Scope      | Session                                                            |
| Shows      | Cumulative storage engine keys examined (index keys + table row keys) across all SELECT queries in this session |
| Reset      | `FLUSH STATUS`                                                     |
| Initial    | `0`                                                                |

This counter provides developers with a tool for identifying expensive queries within a session, validating that query optimization reduced keys scanned, and monitoring application-level scan patterns — functionality with no equivalent in the runaway queries framework.

### Protobuf Changes

Add a `tidb_max_keys_read` field to `coprocessor.Request` in the **kvproto** repository:

```protobuf
message Request {
    // ... existing fields ...
    uint64 max_keys_read = N; // Max storage engine keys to read for this coprocessor task (0 = unlimited)
}
```

**Why `coprocessor.Request` and not `tipb.DAGRequest`:**

- `DAGRequest` is serialized once into `worker.req.Data` and shared across all tasks (coprocessor.go:1657). Per-task limits would require re-serialization.
- `coprocessor.Request` is built per-task in `handleTaskOnce()` (coprocessor.go:1654), allowing different per-task limits.
- This matches the existing `PagingSize` field which is already per-task on `coprocessor.Request`.

**No new response fields are needed.** TiDB tracks keys examined using the existing `ScanDetail.ProcessedKeys` returned in every coprocessor response. This field is already populated by TiKV and counts all keys processed by the storage engine, including both index keys and table row keys.

### TiDB Coprocessor Dispatch

Each coprocessor task is sent the **remaining key budget** (`max_keys_read - keysRead`) as its per-task limit. This is a full, un-split budget — concurrent tasks each receive the same remaining amount rather than having it divided by concurrency.

#### Rationale

This feature targets OLTP workloads. OLTP queries are typically point lookups or narrow range scans that hit one or very few TiKV nodes. Splitting the limit evenly across concurrent tasks would be problematic because:

1. **Data distribution is non-uniform.** If most data lives on one node, splitting by concurrency would under-limit that node and waste budget on idle ones.
2. **The goal is per-node protection.** The limit acts as a safety net for individual TiKV nodes — preventing any single node from being overwhelmed. Treating it as a per-node budget aligns with that intent.
3. **Simpler and more predictable.** Applications can reason about the limit as "keys examined on any single TiKV node before the query is aborted."

TiDB still maintains a global cumulative check in `copIterator.Next()` using `ScanDetail.ProcessedKeys` across all responses. This secondary check ensures the limit is also enforced globally when a query does span multiple nodes, and provides correctness for older TiKV versions that ignore the proto field.

#### Propagation Path

```
SessionVars.MaxKeysRead
    → DistSQLContext.MaxKeysRead      (pkg/distsql/context/context.go)
    → kv.Request.MaxKeysRead          (pkg/distsql/request_builder.go:SetFromSessionVars)
    → copIterator.maxKeysRead         (pkg/store/copr/coprocessor.go)
    → copTask.maxKeysRead             (remaining = max_keys_read - keysRead, per task)
    → coprocessor.Request.MaxKeysRead (per-task proto field)
```

### TiKV Coprocessor Enforcement

TiKV reads `max_keys_read` from the `coprocessor::Request` and enforces it within the `BatchExecutorsRunner` — the component that drives the scan loop and collects results. This follows the same pattern as paging enforcement where the runner checks accumulated keys against a threshold after each `next_batch()` call.

#### Implementation approach

The enforcement point is in `BatchExecutorsRunner::handle_request()` (runner.rs:733-843), which already has a loop that accumulates `record_all` and checks `paging_size`. The `max_keys_read` check is added to the same loop:

```rust
// In handle_request(), existing paging stop condition at line 790:
if drained.stop()
    || self.paging_size.is_some_and(|p| record_all >= p as usize)
    || self.max_keys_read.is_some_and(|m| record_all >= m as usize)  // NEW
{
    // return partial result...
}
```

When the limit is reached, TiKV returns a partial result (not an error). TiDB makes the final determination and raises the error. This separation ensures backward compatibility — old TiDB versions that don't understand the limit simply receive fewer rows.

The TiKV-side enforcement leverages the existing `scanned_rows_per_range: Vec<usize>` counter in `RangesScanner` (scanner.rs:29-50) for precise tracking. The `BatchExecutorsRunner` already tracks `record_all` which counts rows returned by the outermost executor per batch iteration. For `max_keys_read`, we use the cumulative scanned keys from the scan executor, which accounts for keys examined before filtering.

### Error Handling

The error uses SQLSTATE `HY000` (general error), consistent with `max_execution_time` behavior and MariaDB's `LIMIT ROWS EXAMINED` which also returns a warning/error on limit breach.

The error is raised **at the TiDB layer** (in `copIterator.Next()`) when the cumulative scanned keys across all coprocessor responses exceed the limit. TiKV does not return an error — it returns a partial result, and TiDB makes the final determination. This is important for correctness in the distributed case where TiDB has the global view across all tasks.

### Compatibility

| Aspect | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Partitioned tables** | Works correctly — each partition maps to one or more regions and the limit applies across all accessed partitions.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **Prepared statements** | The `SET_VAR` hint is evaluated at execution time, not prepare time. This follows existing `max_execution_time` behavior.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Plan cache** | Fully compatible. Queries with `SET_VAR(tidb_max_keys_read=N)` are cached normally. Different `N` values produce separate cache entries (because the hint text is part of the SQL text used in the cache key). On cache hit, the `StmtHints.SetVars` stored in the cached plan are applied to session variables before execution, so the correct `MaxKeysRead` value is always in effect. Since `tidb_max_keys_read` does not affect the query plan (it is purely an execution-time limit), the cached plan is always valid regardless of the limit value. |
| **TiFlash** | TiFlash coprocessor requests go through the same `copIterator` path. The limit is sent per-task. TiFlash will ignore it for now; TiDB enforces regardless via `ProcessedKeys` tracking.                                                                                                                                                                                                                                                                                                                                                          |
| **Index lookup** | `IndexLookUpExecutor` issues both index scan and table scan coprocessor requests. Both contribute to the cumulative key count. A single logical row accessed via an index lookup is counted **twice**: once for the index key and once for the table row key. Applications should account for this when choosing a limit — a query examining N logical rows via an index will consume approximately 2N keys against the limit.                                                                                                                   |
| **DML** | Not affected. The limit only applies to SELECT queries. `GetMaxKeysRead()` returns 0 when not in a SELECT statement. The `SET_VAR` hint on DML is silently ignored.                                                                                                                                                                                                                                                                                                                                                                              |
| **Subqueries** | The limit applies to the entire statement. Keys scanned by subqueries contribute to the cumulative count. This matches MariaDB's behavior where "there can be only one `LIMIT ROWS EXAMINED` clause for the whole SELECT statement."                                                                                                                                                                                                                                                                                                             |
| **JOINs** | Rows from all tables participating in the JOIN are counted together.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| **Aggregation** | The limit counts keys examined, not result rows. `SELECT COUNT(*) FROM t` with `tidb_max_keys_read=10` will error if the table has more than 10 rows, even though the result is a single row.                                                                                                                                                                                                                                                                                                                                                         |
| **Upgrade** | New proto field `max_keys_read` defaults to `0` (unlimited). Old TiKV versions ignore unknown protobuf fields. TiDB-side enforcement still works via `ProcessedKeys` tracking.                                                                                                                                                                                                                                                                                                                                                                   |
| **Downgrade** | The session variable and hint are not recognized by older TiDB versions. Setting them returns an error (standard behavior for unknown variables).                                                                                                                                                                                                                                                                                                                                                                                                |
| **PD** | No changes required. PD is not involved in coprocessor request handling.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **BR / TiCDC / Dumpling** | Not affected. These tools use internal APIs and do not go through the session variable path.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

## Test Design

### Functional Tests

**Unit tests in `pkg/sessionctx/variable/sysvar_test.go`:**

- Validate variable registration: name, type, scope (Global + Session).
- Validate value range: accepts 0 to `MaxInt64`, rejects negative values.
- Validate `SET_VAR` hint works: `IsHintUpdatableVerified` returns true.
- Validate `GetMaxKeysRead()` accessor: returns 0 when not in a SELECT statement.

**Unit tests in `pkg/store/copr/`:**

- Test that each dispatched task receives `remaining = max_keys_read - keysRead` as its per-task limit.
- Test cumulative tracking and cancellation in `copIterator.Next()`.
- Test that tasks dispatched after the global limit is reached receive limit `0` (i.e., are not sent).

**TiKV unit tests:**

- `BatchExecutorsRunner` with `max_keys_read` set — verify partial result returned when limit reached.
- `BatchExecutorsRunner` with `max_keys_read = 0` — verify no limit applied.

### Scenario Tests

**Integration tests in `tests/integrationtest/t/executor/max_keys_read.test`:**

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Basic enforcement: table with 100 rows, `SET tidb_max_keys_read = 10`, `SELECT *` | Error 8270 |
| 2 | `SET_VAR` hint: `SELECT /*+ SET_VAR(tidb_max_keys_read=5) */ * FROM t` | Error 8270 |
| 3 | Precedence: Session=100, hint=5 | Hint wins, error at ~5 rows |
| 4 | Disable per-query: Session=10, `SET_VAR(tidb_max_keys_read=0)` | No limit, full result |
| 5 | JOINs: `SELECT * FROM t1 JOIN t2` | Keys from all tables counted together |
| 6 | Aggregation: `SELECT COUNT(*) FROM t` | Counts keys examined, not result rows |
| 7 | Status variable: `SHOW STATUS LIKE 'tidb_keys_examined'` | Returns cumulative count |
| 8 | `FLUSH STATUS` | Resets `tidb_keys_examined` to 0 |
| 9 | DML not affected: `INSERT ... SELECT`, `UPDATE`, `DELETE` | No limit applied |
| 10 | Global scope: `SET GLOBAL tidb_max_keys_read = 100` | Applies to new sessions |
| 11 | Under limit: table with 10 rows, `SET tidb_max_keys_read = 100`, `SELECT *` | Full result, no error |
| 12 | Exact limit: table with 10 rows, `SET tidb_max_keys_read = 10` | Error 8270 — cumulative reaches exactly the limit |
| 13 | Plan cache hit: same SET_VAR hint, second execution | `@@last_plan_from_cache = 1`, limit still enforced |
| 14 | Plan cache with session variable (no hint) | Cached plan reused; session-level `MaxKeysRead` applies at execution time |
| 15 | Index lookup: table with 10 rows, index on col, `SET tidb_max_keys_read = 15`, `SELECT * FROM t WHERE col = x` (full index scan) | Error 8270 — each row contributes 2 keys (index + table), so 10 rows = ~20 keys |
| 16 | Covering index scan: same table, `SELECT col FROM t WHERE col = x` (index-only) | No error at limit=15 — covering scan reads only index keys, so 10 rows = ~10 keys |

**RealTiKV tests in `tests/realtikvtest/`:**

- Table with 10,000+ rows split across multiple regions.
- Verify limit enforced correctly when query hits a single TiKV node (typical OLTP case).
- Verify limit enforced correctly when query spans multiple regions across multiple nodes (global cumulative check fires).
- Verify that each concurrent task receives the remaining key budget, not a fractional split.
- Verify that TiKV returns partial results (not full scan) when limit is set.

### Compatibility Tests

- **Partitioned tables**: limit applies across all accessed partitions.
- **Prepared statements**: limit evaluated at `EXECUTE`, not `PREPARE`.
- **Plan cache**: queries with `SET_VAR(tidb_max_keys_read=N)` are cached normally; different `N` values produce separate cache entries because the hint text is part of the SQL text used in the cache key.
- **Upgrade**: old TiKV ignores new proto field; TiDB-side enforcement still works.
- **Downgrade**: old TiDB does not recognize the variable; returns an error.
- **Coexistence with `max_execution_time`**: both can be active; whichever triggers first wins.
- **Coexistence with `PROCESSED_KEYS`**: both can be active; independent checks, whichever fires first wins.
- **Plan cache with SET_VAR**: Execute `SELECT /*+ SET_VAR(tidb_max_keys_read=5) */ * FROM t` twice. Second execution should get a plan cache hit. The limit should
  still be enforced on the cached path (error 8270 if table has >5 rows).
- **Plan cache without hint**: Execute `SET SESSION tidb_max_keys_read = 5; SELECT * FROM t` twice. Both executions should use the same cached plan, and the limit
  should apply from the session variable (not stored in cache).


### Benchmark Tests

- **Disabled overhead** (`value=0`): Measure throughput with the feature disabled. Expected near-zero overhead (one branch check per coprocessor response).
- **High-limit overhead**: Measure throughput with a limit set high enough to never trigger. Verify no regression compared to disabled.
- **Early termination benefit**: Measure latency reduction when the feature aborts a full table scan early (e.g., 1M row table with `tidb_max_keys_read=1000`). This validates that TiKV pushdown provides tangible savings vs. TiDB-only enforcement.

## Impacts & Risks

### Impacts

- **Positive**: Prevents runaway full table scans from consuming cluster resources. Provides a deterministic resource bound complementary to `max_execution_time`. Enables application-level self-service query governance without DBA involvement.
- **Negative**: Adds a new field to `coprocessor.Request` proto (one-time, backward compatible). Adds a new session variable and status variable to TiDB.

### Risks

- **Key counting semantics**: `ScanDetail.ProcessedKeys` counts storage engine keys processed — this is the intended metric, not a proxy for logical rows. Index lookups contribute two keys per logical row (one index key, one table row key), so applications using index-based access patterns should size limits accordingly. Queries that perform covering index scans (no table row lookup) consume only one key per row. MVCC version traversal may also contribute additional key reads when multiple row versions exist.

- **Multi-node overshoot**: Because each concurrent task receives the full remaining budget, a query spanning multiple TiKV nodes may examine up to `remaining * concurrentNodes` keys before the TiDB-side global check fires. This is the intended behavior for an OLTP feature — queries hitting a single node are bounded precisely, and the global cumulative check at TiDB ensures the limit is still enforced across nodes. Applications with OLAP-style queries spanning many nodes should be aware that the effective global key scan may exceed the configured limit.

## Investigation & Alternatives

### Relationship with PROCESSED_KEYS in Runaway Queries

TiDB already provides a `PROCESSED_KEYS` threshold as part of the [resource control runaway queries](https://docs.pingcap.com/tidb/stable/tidb-resource-control-runaway-queries/) feature (available since v7.2.0). Both features count coprocessor-level keys/rows scanned. This section explains why `tidb_max_keys_read` is still needed as a complementary mechanism.

#### Feature Comparison

| Aspect | PROCESSED_KEYS (Runaway Queries) | tidb_max_keys_read |
|--------|----------------------------------|-------------------|
| Configuration scope | Resource group (cluster-wide DDL) | Session variable or per-query hint |
| Who configures it | DBA via `ALTER RESOURCE GROUP` | Application developer via `SET SESSION` or `SET_VAR` |
| Granularity | All queries from all sessions in the resource group | Individual session or individual query |
| Per-query override | Not possible | `SET_VAR(tidb_max_keys_read=N)` per query |
| Actions on breach | `KILL`, `COOLDOWN`, `DRYRUN`, `SWITCH_GROUP` | Error (query interrupted) |
| Watch/quarantine | Automatic quarantine of repeated offenders | No quarantine |
| Monitoring | `information_schema.tidb_runaway_queries` | `SHOW STATUS LIKE 'tidb_keys_examined'` |
| Dependencies | Requires resource control framework enabled | Standalone, zero-config |
| Storage pushdown | No — TiDB-only enforcement after coprocessor response | Yes — limit pushed to TiKV coprocessor for early termination |
| MySQL ecosystem equivalent | No direct equivalent | MariaDB `LIMIT ROWS EXAMINED` |

#### Why Both Features Are Needed

**1. Different personas, different workflows.**

`PROCESSED_KEYS` is a DBA/platform-level tool. It requires creating resource groups, assigning users/sessions to groups, and configuring cluster-wide policies via DDL. It is designed for platform teams managing multi-tenant clusters.

`tidb_max_keys_read` is a developer/application-level tool. A developer writes `SET SESSION tidb_max_keys_read = 10000` or adds a `SET_VAR` hint to a specific query. No DBA involvement is required. This self-service model is critical for teams that need immediate protection without waiting for DBA configuration.

**2. Per-query flexibility.**

`PROCESSED_KEYS` applies a uniform threshold to all queries in a resource group. There is no way to say "this specific query should be allowed to scan more rows" without changing the resource group configuration or moving the session to a different group.

`tidb_max_keys_read` supports `SET_VAR` hints, allowing per-query override:

```sql
SET SESSION tidb_max_keys_read = 1000;  -- default safety net
-- This specific report query needs more headroom:
SELECT /*+ SET_VAR(tidb_max_keys_read=100000) */ * FROM analytics WHERE date > '2024-01-01';
-- Back to 1000 for the next query automatically
```

**3. No dependency on resource control.**

The resource control framework must be enabled cluster-wide, and resource groups must be created and assigned. In many deployments, resource control is not used or is not yet adopted.

`tidb_max_keys_read` works independently with zero prerequisites. A user sets a session variable and gets immediate protection.

**4. Storage-level pushdown.**

`PROCESSED_KEYS` in the runaway queries framework is enforced entirely at the TiDB layer: the `RunawayChecker.CheckThresholds()` method accumulates `processedKeys` after each coprocessor response and checks against the threshold. By the time this check runs, TiKV has already scanned all the rows for that task and transmitted the results over the network.

`tidb_max_keys_read` pushes the limit down to TiKV via the `coprocessor.Request.max_keys_read` proto field. TiKV can stop scanning early and return a partial result, **saving TiKV CPU, disk I/O, and network bandwidth**. For a query that would scan 10 million rows on a table split across 100 regions, the pushdown avoids transmitting millions of unnecessary rows. This is the primary technical advantage.

**5. Cumulative session counter.**

`tidb_keys_examined` provides visibility into how many storage engine keys (index + table row) a session has examined across all queries — a debugging and tuning tool with no equivalent in the runaway queries framework. Developers use this for:

- Identifying which queries in a transaction are expensive.
- Validating that query optimization reduced keys examined (e.g., an index-only scan eliminates the table row key lookup).
- Monitoring application-level scan patterns.

#### Shared Implementation Details

Both mechanisms read from `ScanDetail.ProcessedKeys` in coprocessor responses. They can share this infrastructure:

- The `RunawayChecker` checks `processedKeys` via `CheckThresholds()` which runs after each coprocessor response in `handleTaskOnce()`.
- The `max_keys_read` check happens in `copIterator.Next()`, also after each coprocessor response.

Both checks are independent and coexist naturally. If both `PROCESSED_KEYS` and `tidb_max_keys_read` are set, whichever threshold is reached first triggers its respective action. They serve different governance models operating at different organizational levels.

### MariaDB LIMIT ROWS EXAMINED

MariaDB provides [`LIMIT ROWS EXAMINED`](https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/limit-rows-examined) as a SQL clause extension:

```sql
SELECT * FROM t1, t2 LIMIT 10 ROWS EXAMINED 10000;
```

https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/limit-rows-examined

Key characteristics:

- **Scope**: Per-statement clause, not a session variable.
- **Counting**: "The server counts the number of read, inserted, modified, and deleted rows during query execution. This takes into account the use of temporary tables, and sorting for intermediate query operations."
- **Behavior on breach**: Returns a partial (possibly empty) result set with a warning, rather than an error. For `GROUP BY`, the last group where the limit triggered is discarded.
- **Restrictions**: Cannot be applied to `DELETE` or `UPDATE`. Cannot be specified per-subquery.
- **Enforcement**: Approximate — "there can be only one `LIMIT ROWS EXAMINED` clause for the whole SELECT statement."

**Differences in our design:**

| Aspect | MariaDB `LIMIT ROWS EXAMINED` | TiDB `tidb_max_keys_read` |
|--------|-------------------------------|---------------------------|
| Interface | SQL clause | Session variable + `SET_VAR` hint |
| Scope levels | Per-query only | Global, Session, and Per-query |
| On limit breach | Warning + partial result | Error (query terminated) |
| Parser changes | Yes (new SQL syntax) | No (reuses existing `SET_VAR` mechanism) |
| Storage pushdown | Single-node (not applicable) | Distributed pushdown to TiKV coprocessor |

We chose the error-on-breach behavior (instead of MariaDB's partial-result behavior) because:

1. Partial results are dangerous in practice — applications may silently operate on incomplete data.
2. An explicit error forces the application to handle the case, either by raising the limit or optimizing the query.
3. This is consistent with TiDB's `max_execution_time` which also returns an error rather than partial results.

We chose session variable + `SET_VAR` hint (instead of a new SQL clause) because:

1. No parser changes needed — lower implementation risk and maintenance burden.
2. Consistent with TiDB's existing `SET_VAR` ecosystem (`max_execution_time`, `tikv_client_read_timeout`, etc.).
3. Session-level defaults are more practical than per-query clauses for applications that want a global safety net.

### Alternative: TiDB-Only Enforcement (No TiKV Pushdown)

Count rows using `ScanDetail.ProcessedKeys` in `copIterator.Next()` and error when the limit is reached, without sending any limit to TiKV.

- **Pro**: No TiKV or proto changes needed. Simpler implementation.
- **Con**: TiKV scans and transfers all rows before TiDB can stop them. Wastes network bandwidth and TiKV CPU. Defeats the purpose of the feature for large tables.

**Rejected** because pushdown is essential for efficiency. This is the key differentiator from the existing `PROCESSED_KEYS` mechanism in runaway queries, which has this exact limitation.

Note: TiDB-side enforcement is still maintained as a **secondary check** for correctness, ensuring the limit is enforced even when TiKV does not support the new proto field (upgrade compatibility).

### Alternative: Limit in `tipb.DAGRequest` Instead of `coprocessor.Request`

- **Pro**: Semantically cleaner — it is a query execution parameter.
- **Con**: `DAGRequest` is serialized once into `worker.req.Data` and shared across all tasks. Cannot have per-task limits without re-serialization.

**Rejected** because the limit must be set per-task (each task carries the remaining budget at dispatch time). The `coprocessor.Request` is built per-task in `handleTaskOnce()`, making it the natural place for per-task configuration.

## Unresolved Questions

1. **Exact error code**: Should we use `8270` (new TiDB-specific code) or align with a MySQL error code? MariaDB uses a warning (not an error). MySQL does not have this feature natively. Proposal: use `8270` in the TiDB error code space with SQLSTATE `HY000`.

2. **ProcessedKeys vs true row count**: Resolved — `tidb_max_keys_read` intentionally counts storage engine keys (not logical rows), consistent with its name and with TiDB's existing `PROCESSED_KEYS` semantics. Index lookups count as two keys per logical row; covering index scans count as one key per row. This is documented in the Compatibility section and the variable name communicates it directly.

3. **Interaction with RunawayChecker**: When both `PROCESSED_KEYS` and `tidb_max_keys_read` are active, which error takes precedence? Proposal: whichever check fires first wins, as they run in different code paths (`CheckThresholds()` in `handleTaskOnce()` vs. the accumulation check in `copIterator.Next()`).

4. **Scope for DML subqueries**: If `INSERT INTO t2 SELECT * FROM t1` is executed, should the SELECT portion be limited? Current proposal: no — `GetMaxKeysRead()` returns 0 when not in a pure SELECT statement, consistent with MariaDB which excludes `DELETE` and `UPDATE` from `LIMIT ROWS EXAMINED`.

