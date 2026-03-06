# REFRESH MATERIALIZED VIEW Observability: DRY RUN / WITH PROFILE

## Background

`REFRESH MATERIALIZED VIEW` in TiDB is a utility statement with multiple internal phases:

- transaction setup / lock
- history bookkeeping
- data change implementation
- metadata persistence
- commit / finalize

From a user perspective, this is one SQL statement. Internally, it behaves more like a workflow.
`EXPLAIN` is centered around one statement to one plan tree and produces a multi-column table,
which is not ideal for this multi-step workflow. A single-column, step-oriented output is more
readable and more flexible for tooling.

This document describes two refresh-level observability modes:

- `DRY RUN`: plan-only (static) steps and plan trees.
- `WITH PROFILE`: execute refresh and return step-level runtime information.

## Goals

1. Provide step-level observability for MV refresh.
2. Return a single-column output (`refresh steps`) that is easy to read in SQL clients.
3. Reuse existing planner/explain infrastructure where possible.
4. Keep plain refresh behavior unchanged when not using `DRY RUN` or `WITH PROFILE`.

## Non-goals

1. No new persistent history/system tables for observability output.
2. No dashboard integration.
3. No async refresh redesign.

## User-facing behavior

### Syntax

```sql
REFRESH MATERIALIZED VIEW mv FAST DRY RUN;
REFRESH MATERIALIZED VIEW mv COMPLETE DRY RUN;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE FAST DRY RUN;

REFRESH MATERIALIZED VIEW mv FAST WITH PROFILE;
REFRESH MATERIALIZED VIEW mv COMPLETE WITH PROFILE;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE WITH PROFILE;
```

### Semantics

1. `REFRESH MATERIALIZED VIEW ... DRY RUN`:
  - does not execute refresh
  - returns the planned workflow steps and per-step plan trees where applicable
2. `REFRESH MATERIALIZED VIEW ... WITH PROFILE`:
  - executes the refresh
  - returns step-level runtime information and per-step execution details
3. Privilege checks are the same as plain refresh.
4. `DRY RUN` and `WITH PROFILE` are mutually exclusive.

### Output shape

A single column named `refresh steps`.

- Step header row: `[S04 DATA_CHANGE_FAST_MERGE]`
- Plan row: indented and flattened from explain output, preserving `estRows/actRows/task` columns.

For `WITH PROFILE`, the step header line also includes status/time.

Example (FAST, abbreviated):

```
refresh steps
[S01 TXN_BEGIN] status:success, time:281.311µs
[S02 LOCK_REFRESH_INFO] status:success, time:788.718µs
[S03 INSERT_HIST_RUNNING] status:success, time:1.330434ms
[S04 DATA_CHANGE_FAST_MERGE] status:success, time:4.665379ms
  MVDeltaMerge | estRows:N/A | actRows:0 | task:root | time:1.82ms, loops:2 | agg_deps:[...]
  └─HashJoin | estRows:8000.00 | actRows:1 | task:root | time:1.33ms, loops:2 | left outer join, equal:[...]
[S05 PERSIST_REFRESH_INFO] status:success, time:1.901706ms
[S06 TXN_COMMIT] status:success, time:868.568µs
[S07 FINALIZE_HIST] status:success, time:2.525621ms
```

## Step model

Define stable step names and sequential IDs so output is deterministic across runs.

Shared steps:

1. `S01 TXN_BEGIN`
2. `S02 LOCK_REFRESH_INFO`
3. `S03 INSERT_HIST_RUNNING`

Data-change steps by refresh type:

- FAST:
  - `S04 DATA_CHANGE_FAST_MERGE`
- COMPLETE:
  - `S04 DATA_CHANGE_COMPLETE_DELETE`
  - `S05 DATA_CHANGE_COMPLETE_INSERT`

Finalize steps after data change:

- FAST:
  - `S05 PERSIST_REFRESH_INFO`
  - `S06 TXN_COMMIT`
  - `S07 FINALIZE_HIST`
- COMPLETE:
  - `S06 PERSIST_REFRESH_INFO`
  - `S07 TXN_COMMIT`
  - `S08 FINALIZE_HIST`

Notes:

- Step IDs are sequential within a refresh. COMPLETE uses one extra data-change step, so
the finalize steps are shifted by one compared to FAST.
- Some steps have no physical plan tree (for example `TXN_BEGIN`, `COMMIT`) and only expose
a single header row.

## Plan-row rendering strategy

### DRY RUN

1. For a step that has a concrete plan:
  - construct `core.Explain` with `Format='brief'`
  - call `RenderResult()` to get rows
2. Remove explain ID suffixes for stability.
3. Flatten each explain row into a single string and indent it under the step header, keeping
  `estRows` and `task` fields.

### WITH PROFILE

1. Execute refresh with step observer hooks.
2. For SQL-bearing steps, snapshot `Explain` rows in `Analyze` mode after each step.
3. Flatten each explain row into a single string with execution info included, keeping
  `estRows`, `actRows`, and `task` fields.

## How per-refresh-type steps are planned

### FAST refresh

Data-change step uses existing internal statement:

- `RefreshMaterializedViewImplementStmt` -> planner builds `MVDeltaMerge` plan

Dry run builds the internal statement and renders its plan rows without execution.
Profile mode executes the refresh and captures the actual runtime rows.

### COMPLETE refresh

Data-change is two SQL statements:

1. `DELETE FROM <mv>`
2. `INSERT INTO <mv> <mv_select_sql>`

Dry run builds plans for both statements and shows them as `S04` + `S05`.
Profile mode executes both and captures runtime rows separately.

## Error handling and result semantics

1. If a step plan cannot be built:
  - return the build error directly
2. `WITH PROFILE` returns the refresh error on failure (same semantics as plain refresh).
3. Plain refresh behavior is unchanged.

## Compatibility

1. Plain `REFRESH MATERIALIZED VIEW` behavior unchanged.
2. Existing `EXPLAIN` for SELECT/DML unchanged.
3. Output is additive and scoped to refresh dry run / profile only.
4. Privilege model remains the same as current refresh implementation.

## Performance considerations

1. No overhead for plain refresh path.
2. Dry run only performs planning work (no data change).
3. Profile adds expected overhead from per-step runtime statistics, acceptable for diagnostics.

## Implementation plan

### Patch 1: parser + AST

Goal:

1. Add `DRY RUN` and `WITH PROFILE` syntax for refresh.
2. Restore output includes the new keywords.

Main files:

- `pkg/parser/parser.y`
- `pkg/parser/ast/misc.go`
- `pkg/parser/parser_test.go`

Acceptance criteria:

1. Parser accepts `REFRESH MATERIALIZED VIEW ... DRY RUN` and `... WITH PROFILE` and restores SQL text correctly.

### Patch 2: planner entry

Goal:

1. Build dedicated plan nodes with single-column schema.

Main files:

- `pkg/planner/core/common_plans.go`
- `pkg/planner/core/planbuilder.go`
- `pkg/planner/core/casetest/mvrefresh/mv_refresh_test.go`

Acceptance criteria:

1. `REFRESH MATERIALIZED VIEW ... DRY RUN` produces a plan with one column named `refresh steps`.
2. `REFRESH MATERIALIZED VIEW ... WITH PROFILE` produces a plan with one column named `refresh steps`.
3. `DRY RUN` and `WITH PROFILE` are rejected if both specified.

### Patch 3: executor dry run

Goal:

1. Implement dry-run executor to output steps and plans in one column.

Main files:

- `pkg/executor/mv_refresh_observability.go`
- `pkg/executor/builder.go`

Acceptance criteria:

1. FAST dry run shows step headers and `MVDeltaMerge` subtree lines.
2. COMPLETE dry run shows delete and insert as separate steps.

### Patch 4: executor profile

Goal:

1. Execute refresh and return step-level runtime observability in one column.

Main files:

- `pkg/executor/mv_refresh_observability.go`
- `pkg/executor/builder.go`

Acceptance criteria:

1. FAST profile shows step headers with status/time and runtime rows for the merge step.
2. COMPLETE profile shows runtime rows for delete and insert steps.

### Patch 5: quality gate

Goal:

1. Ensure tests are minimal and deterministic.
2. Run required repository checks once at the end.

Suggested validation commands:

```bash
make parser
GOCACHE=/tmp/go-build go test ./pkg/executor/test/executor -run <TestName> -tags=intest,deadlock
```

