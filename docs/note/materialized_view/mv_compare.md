# Materialized View Compare (Implementation and Design Notes)

This document describes the design and implementation notes for:

```sql
COMPARE MATERIALIZED VIEW <mv> AS OF TIMESTAMP <ts_expr> [OUTPUT INTO TABLE <table>]
```

Current implementation status:

- Parser / AST / planner / executor support is implemented.
- Compare execution uses explicit snapshot semantics from `AS OF TIMESTAMP`.
- The target MV metadata is resolved from the snapshot InfoSchema at the compare snapshot `S`, not from the current InfoSchema. This keeps the MV table ID, column schema, and refresh-info lookup consistent with snapshot `S`, including out-of-place refresh cutover cases.
- `OUTPUT INTO TABLE` is implemented. The output table is created from the snapshot MV public-column schema plus the differ-type column; it does not inherit MV indexes, constraints, partitioning, or MV-specific metadata. If compare fails after creating the output table, the statement drops the auto-created output table during cleanup.

## Goals

1. Compare MV table data at snapshot `S` and base-query snapshot data at refresh watermark `R`.
2. Preserve correctness under different read timestamps:
   - MV side reads at statement snapshot `S`.
   - Base-query side reads at refresh watermark `R` (`LAST_SUCCESS_READ_TSO`).
3. Reuse existing MV diff semantics (group-key identity + null-safe payload comparison).
4. Support two output modes:
   - no `OUTPUT INTO TABLE`: return whether differences exist and the number of differing rows.
   - with `OUTPUT INTO TABLE`: auto-create an output table and persist compare results into it.

## Non-goals

1. No automatic repair / no write-back to MV.
2. No async compare scheduler.
3. No compare for non-grouped MV definitions.
4. No cost-based engine/path selection work (correctness first).

## SQL behavior (user view)

Syntax (spec form):

```sql
COMPARE MATERIALIZED VIEW mv1 AS OF TIMESTAMP 'xxxx' [OUTPUT INTO TABLE [db1.]t1];
```

Semantics:

1. Let `S` be the compare statement snapshot timestamp (from `AS OF TIMESTAMP`).
   - In execution terms, `S` is the read snapshot of this compare statement (the statement-level read TS / start TS).
2. Load snapshot InfoSchema at `S` and resolve the target MV from that snapshot.
   - The MV physical table ID used for refresh-info lookup must come from snapshot `S`.
   - This is required when an out-of-place refresh cutover has changed the current MV physical table ID after `S`.
3. Read MV table and refresh-info row at snapshot `S`.
4. Read `R = mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` from that same snapshot `S`.
5. Execute MV definition SQL on base tables at snapshot `R`.
6. Compare `MV@S` vs `BaseQuery@R` by full-outer semantics on group keys.

Output behavior:

1. Without `OUTPUT INTO TABLE`: return one row in `compare result` describing:
   - whether any difference exists;
   - the number of differing rows.
2. Recommended output text follows the current spec direction:
   - `There are ... differences result in ... compared to source base tables at timestamp 'xxxx' (TSO: yyyy)`
   - the rendered timestamp and TSO should be `R` (the base-query comparison snapshot), because the comparison source side is `BaseQuery@R`.
3. With `OUTPUT INTO TABLE`:
   - TiDB automatically creates the target table;
   - if the target table already exists, the statement returns table-exists error;
   - execution requires `CREATE` and `INSERT` privileges on the target table schema.
   - the target table schema is built from snapshot `S` MV public columns plus the differ-type column;
   - the target table does not copy MV indexes, constraints, partitioning, or MV-specific metadata;
   - if target-table creation succeeds but compare later fails, TiDB rolls back the current output-write transaction and attempts to drop the auto-created target table, including any already committed output batches.

Privilege semantics:

1. `COMPARE MATERIALIZED VIEW` requires `OPERATE VIEW` privilege on the target MV.
2. Compare execution also requires `SELECT` privilege on the MV base table(s), consistent with existing refresh-side validation.
3. If `OUTPUT INTO TABLE` is present, `CREATE` and `INSERT` privileges on the target schema are additionally required.

## Why this is not one SQL statement

A single SQL statement cannot safely express `MV@S` joined with `Base@R` when `S != R`.

Reason:

- stale-read processor enforces one statement-level `AS OF` timestamp across tables;
- mixed timestamps in one statement are rejected (`can not set different time in the as of`).

So compare must be orchestrated as a utility workflow with two read snapshots.

## Why not read both sides at `R`

Reading both sides at `R` would only verify "refresh-at-`R` equivalence" and cannot detect later MV drift/corruption after `R`.

The chosen semantics (`MV@S` vs `Base@R`) answers the operational question more directly:

- "Is the currently served MV state still consistent with what it should be based on the last successful refresh watermark?"

## Chosen direction

Use a utility executor (`CompareMaterializedViewExec`) to orchestrate two snapshot readers, then wire a full outer `HashJoinV1Exec` as the diff engine inside compare execution.

### Phase 1: Capture snapshot, resolve target, and validate

1. Evaluate compare statement `AS OF` to get `S` and validate stale-read TS.
2. Load snapshot InfoSchema for `S`.
3. Resolve the target MV table from snapshot InfoSchema `S` and ensure `MaterializedView` metadata exists.
4. Ensure MV is `Ready` at snapshot `S`.
5. Check base-table `SELECT` privilege against snapshot MV metadata (`checkRefreshMaterializedViewBaseTableSelect`).
6. If `OUTPUT INTO TABLE` is set:
   - check target table does not exist;
   - check `CREATE` and `INSERT` privileges on the target schema.

### Phase 2: Read refresh watermark

1. Read `LAST_SUCCESS_READ_TSO` from `mysql.tidb_mview_refresh_info` at snapshot `S`, using the snapshot MV table ID from Phase 1.
2. Let `R = LAST_SUCCESS_READ_TSO`.

Rules:

1. If refresh-info row is missing: fail as metadata inconsistency.
2. If `R` is `NULL`: fail (`compare` requires a successful refresh watermark).
3. If `R > S`: fail as metadata inconsistency.

### Phase 3: Build two read executors

1. MV reader at snapshot `S`:
   - row shape: MV table public columns in MV order.
2. Base reader at snapshot `R`:
   - SQL source: `mv.MaterializedView.SQLContent`;
   - session SQL mode, time zone, and related definition-time settings restored from MV metadata;
   - field aliases rewritten to MV column names (same as complete-diff builder contract);
   - row shape aligned to MV columns.

Both readers are read-only and independent.

### Phase 4: Reuse full outer hash join in compare executor

Join identity and comparison rules reuse COMPLETE DELTA APPLY semantics:

1. Identity key is `GROUP BY` key columns.
2. Key comparison:
   - `=` for `NOT NULL` key columns.
   - `<=>` for nullable key columns.
3. Side-missing detection should reuse the same deterministic marker-column rule as COMPLETE DELTA APPLY:
   - use one visible `NOT NULL` MV column as side-missing marker;
   - do not infer side-missing from nullable key columns.
4. Payload comparison after join output:
   - null-safe by column (`<=>` semantic).

Diff categories:

1. `mview_differ`: MV row and base-query row both exist, but payload differs.
2. `mview_vacuum`: compared with base-query result at `R`, MV is missing corresponding rows.
3. `mview_excessive`: compared with base-query result at `R`, MV has extra corresponding rows.

Implementation shape:

1. Build `MV@S` and `Base@R` child executors.
2. In `CompareMaterializedViewExec`, wire those two child executors into a full outer `HashJoinV1Exec`, with `MV@S` as the probe/left side and `BaseQuery@R` as the build/right side.
3. Consume hash-join output rows and classify them into:
   - matched-but-different -> `mview_differ`
   - base-only -> `mview_vacuum`
   - mv-only -> `mview_excessive`

This keeps full outer join concurrency and row-matching behavior reused from the existing hash join executor, while leaving compare-specific result classification in `CompareMaterializedViewExec`.

### Phase 5: Emit summary or persist diffs

1. Summary mode (`OUTPUT INTO TABLE` absent):
   - return one summary row in `compare result`;
   - result includes only whether differences exist and the differing-row count.
2. Output-table mode (`OUTPUT INTO TABLE` present):
   - auto-create the target table before writing compare results;
   - output table schema is the same as the MV public-column schema, plus one extra differ-type column.
   - if compare fails after creating the output table, drop the auto-created output table before returning the error.

Output table schema contract:

1. Start from the snapshot `S` MV public-column schema in MV column order.
2. Add one extra column named `_Differ_type_`.
3. If the MV already contains `_Differ_type_`, append a numeric suffix and use `_Differ_type_1`, `_Differ_type_2`, and so on until the name is unique.
4. `_Differ_type_` values are:
   - `mview_differ`
   - `mview_vacuum`
   - `mview_excessive`
5. Row-value source in the output table is:
   - `mview_differ`: write the `MV@S` row values;
   - `mview_excessive`: write the `MV@S` row values;
   - `mview_vacuum`: write the base-query row values from snapshot `R`.
6. The output table keeps MV public column types/defaults/comments where applicable, but it does not inherit indexes, primary/unique constraints, foreign keys, check constraints, partitioning, TTL, placement, TiFlash replica, auto-id options, or MV-specific metadata.

## Contract with existing MV logic

To avoid semantic drift, compare should reuse metadata extraction logic used by MV refresh diff paths:

1. Reuse grouped-key extraction contract from MV definition SQL.
2. Reuse nullable-key vs not-null-key comparison rule (`NullEQ` vs `EQ`).
3. Reuse side-missing marker selection rule.
4. Reuse payload null-safe equality semantics.

For grouped MV, this keeps compare semantics aligned with COMPLETE DELTA APPLY diff semantics.

## Error handling

1. Missing system table (`mysql.tidb_mview_refresh_info`) -> explicit error.
2. Missing refresh-info row for target MV -> explicit metadata error.
3. `LAST_SUCCESS_READ_TSO` is `NULL` -> explicit compare precondition error.
4. Snapshot `R` older than GC safe point -> stale-read validation error.
5. Any internal reader execution failure -> return original error.
6. If `OUTPUT INTO TABLE` was used and the target table was auto-created, any later compare failure triggers best-effort output-table cleanup before returning the original error. If cleanup fails, TiDB writes a warning log.

## Performance notes

1. Correctness is the primary requirement, but the implementation should still avoid obviously poor performance.
2. Summary mode should still count differing rows accurately; it cannot stop after the first mismatch.
3. Output-table mode should stream diff rows and avoid retaining full diff in memory.
4. Output-table writes are batched to avoid a single very large transaction.
5. The current hash-join wiring uses `BaseQuery@R` as the build side. For grouped MV definitions, the base query may already require aggregation state; building the join hash table from the base-query result lets that side finish before probing the MV scan, reducing active-stage overlap between base aggregation work and an additional MV-side build table.
6. The current design uses full-outer diff semantics for clarity and correctness. If a better execution strategy is introduced later, it should preserve the same compare semantics.

## Code map (implementation landing)

- Parser / AST:
  - `pkg/parser/parser.y`
  - `pkg/parser/ast/misc.go`
- Planner:
  - `pkg/planner/core/planbuilder.go` (`buildCompareMaterializedView`)
  - `pkg/planner/core/common_plans.go` (`CompareMaterializedView`)
- Executor implementation:
  - `pkg/executor/materialized_view.go` (`CompareMaterializedViewExec`)
- Reusable MV diff metadata helpers:
  - `pkg/planner/mview/mvmerge.go`

## Test coverage

1. Privilege path remains valid.
2. Correctness cases:
   - no diff,
   - `mview_vacuum`,
   - `mview_excessive`,
   - `mview_differ`,
   - mixed.
3. Nullable key and nullable payload cases.
4. `LAST_SUCCESS_READ_TSO` null/missing row/system-table missing.
5. GC-safe-point rejection for snapshot `R`.
6. `OUTPUT INTO TABLE` success, table-exists conflict, and `_Differ_type_` name-conflict handling.
7. Snapshot metadata after out-of-place refresh cutover, including output table creation from snapshot MV schema rather than current MV indexes.

## Open follow-ups

1. Whether to expose richer compare stats (for example scanned rows, elapsed time per side).
2. Whether to add a dry-run/profile style observability output for compare workflow.
