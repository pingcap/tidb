# EXPLAIN ROUTINE

- Author(s): tangenta
- Discussion PR: https://git.pingcap.net/pingkai/tidb/pulls/764
- Tracking Issue: N/A

## Introduction

`EXPLAIN ROUTINE` gives TiDB a routine-aware introspection surface for stored procedures and stored functions. This document intentionally describes the feature as one surface: v1 provides a static statement catalog plus per-site plan rendering, and v2 extends the same catalog with runtime summary and single-site drill-down for `EXPLAIN ANALYZE ROUTINE`.

The key contract is the shared statement catalog. Every observable site inside the routine body gets a stable `STMT_ORDINAL`, and both the static and runtime views are built around that ordinal.

## Motivation or Background

Stored routines hide SQL behind procedural control flow, cursor operations, dynamic SQL, and expression subqueries. Ordinary `EXPLAIN` can only see one statement at a time, so routine debugging is awkward:

- users cannot quickly list which SQL sites a routine may execute;
- users cannot map runtime behavior back to a concrete statement site in the routine body; and
- parser/planner/executor splits become hard to review if v1 and v2 are documented as unrelated features.

The goal is to expose one coherent surface:

1. `EXPLAIN ROUTINE` shows the routine-level statement catalog and the static plan for each explainable site;
2. `EXPLAIN ANALYZE ROUTINE` reuses the same catalog and adds runtime observations; and
3. `FOR STMT <n>` drills into one catalogued site by `STMT_ORDINAL`.

## Detailed Design

### SQL Surface

```sql
EXPLAIN [FORMAT = 'row'|'brief'|'verbose'|'tidb_json']
ROUTINE {FUNCTION|PROCEDURE} routine_name(arg1, ...)

EXPLAIN ANALYZE [FORMAT = 'row'|'traditional']
ROUTINE {FUNCTION|PROCEDURE} routine_name(arg1, ...)

EXPLAIN ANALYZE [FORMAT = 'row'|'traditional'|'brief'|'verbose'|'tidb_json']
ROUTINE {FUNCTION|PROCEDURE} routine_name(arg1, ...)
FOR STMT <stmt_ordinal>
```

Notes:

- Plain `EXPLAIN ROUTINE` is a static view and does **not** execute the routine body.
- `EXPLAIN ANALYZE ROUTINE` executes the routine body once under routine-aware instrumentation.
- `FOR STMT <n>` is only supported on `EXPLAIN ANALYZE ROUTINE`; it selects one catalog entry for drill-down.
- Summary-mode `EXPLAIN ANALYZE ROUTINE` keeps the compact routine-level table surface and only accepts the row/traditional summary shape; drill-down mode returns the normal `EXPLAIN ANALYZE` rows for the chosen statement site.

### Shared Statement Catalog

Planner builds one routine statement catalog and uses it for both v1 and v2. Each catalog entry contains:

- routine identity: `ROUTINE_SCHEMA`, `ROUTINE_NAME`, `ROUTINE_TYPE`;
- statement identity: `STMT_ORDINAL`, `BLOCK_PATH`, `STMT_KIND`;
- static text: `SQL_TEXT`;
- explainability metadata: `EXPLAINABLE`, `NOTE`.

`STMT_ORDINAL` is stable within one routine catalog and is the identifier accepted by `FOR STMT <n>`.

`BLOCK_PATH` identifies where the site lives in control flow, for example `root`, `root/if#1/then`, or `root/while#1/body`.

The catalog covers the observable SQL-bearing sites that matter to users, including:

- ordinary static SQL statements in the routine body;
- cursor `OPEN` statements through the cursor's underlying `SELECT`;
- subquery-bearing expression sites such as `RETURN`, `DECLARE ... DEFAULT`, `IF`, `CASE`, and `WHILE` conditions;
- dynamic SQL placeholders (`PREPARE` / `EXECUTE`) so runtime output can still point back to the correct site.

### v1: `EXPLAIN ROUTINE`

`EXPLAIN ROUTINE` walks the shared catalog and renders a static plan for each explainable site.

- Supported formats: `row`, `brief`, `verbose`, `tidb_json`.
- Explainable sites reuse the ordinary `EXPLAIN` renderer.
- Non-explainable sites are still listed in the catalog, with `EXPLAINABLE = NO` and a human-readable `NOTE`.
- One site failing to compile does not abort the whole result; that row is returned with a failure note and the remaining sites still render.

This keeps v1 useful as a discovery tool even when some routine sites are dynamic, unsupported, or currently invalid.

### v2 Summary: `EXPLAIN ANALYZE ROUTINE`

Summary mode executes the routine once and appends runtime columns to the same catalog:

- `RUNTIME_SQL_TEXT`
- `EXEC_COUNT`
- `TOTAL_TIME`
- `AVG_TIME`
- `ROWS_PRODUCED`
- `PLAN_VARIANTS`

Behavior:

- If a site is not executed in the current invocation, its runtime counters remain zero/empty.
- If dynamic SQL resolves to a concrete runtime statement, `RUNTIME_SQL_TEXT` records the resolved SQL.
- `PLAN_VARIANTS` counts distinct observed plan digests for that site within the invocation.

Summary mode is intentionally routine-level: it answers “which sites ran, how often, and what did they execute?” before users ask for a detailed plan tree.

### v2 Drill-Down: `FOR STMT <n>`

Drill-down mode re-executes the routine and returns the normal `EXPLAIN ANALYZE` rows for one target site.

Rules:

- the target `STMT_ORDINAL` must exist in the catalog;
- the target must actually execute in this invocation;
- the target must observe exactly one plan variant; and
- the target must be explainable as a plan-bearing statement.

If these checks pass, the result shape is the ordinary `EXPLAIN ANALYZE` output for the selected format. The routine-level prefix columns are omitted in drill-down mode because the user already chose the target site explicitly.

### Safety and Error Handling

`EXPLAIN ANALYZE ROUTINE` executes routine code, so safety is stricter than v1.

- Analyze mode is limited to read-only observable statements.
- Invalid target ordinals, missing routine arguments, and statically visible write statements are rejected before the routine body starts executing.
- Plain `EXPLAIN ROUTINE` remains non-executing and therefore has no read-only restriction.
- Dynamic SQL may appear in the catalog even when it is not statically explainable in v1; runtime summary can still report what actually ran.

This split keeps the static view broad while making the runtime view safe and predictable.

## Test Design

### Functional Tests

- parser tests for grammar, AST restore, and keyword coverage of `FOR STMT <n>`;
- planner/executor unit tests for catalog construction, format validation, and result columns;
- unit tests for runtime summary, drill-down, invalid target handling, missing arguments, dynamic SQL runtime text, and read-only preflight.

### Scenario Tests

- procedures/functions with nested control flow and `BLOCK_PATH` changes;
- cursor `OPEN` statements;
- expression subqueries in `RETURN`, `DECLARE DEFAULT`, `IF`, `CASE`, and `WHILE`;
- dynamic SQL with both static placeholder rows and runtime-observed SQL text.

### Compatibility Tests

- static v1 output remains keyed by `STMT_ORDINAL`, so v2 can target the same site numbering;
- parser-only PRs and executor/planner PRs stay aligned because they share one documented surface;
- no external component changes are required.

### Benchmark Tests

No separate benchmark is required for the documentation change. Runtime cost for analyze mode is dominated by executing the routine body itself and, for drill-down, the extra invocation needed to render the chosen statement site.

## Impacts & Risks

### Impacts

- Makes stored routine debugging substantially easier.
- Gives reviewers one merged spec instead of separate v1/v2 narratives.
- Keeps the user-facing contract centered on `STMT_ORDINAL`, which simplifies follow-up work.

### Risks

- `EXPLAIN ANALYZE ROUTINE` can still be confusing if users forget that runtime summary and drill-down depend on the actual invocation path.
- Drill-down requires another routine execution, so conditional branches or dynamic SQL may differ between invocations.
- Dynamic SQL remains only partially explainable in the static view.

## Investigation & Alternatives

### Keep Separate v1 and v2 Specs

Rejected. The static catalog and the runtime/drill-down surfaces are not independent features; they share the same syntax family and the same `STMT_ORDINAL` contract. Splitting the documentation hides that relationship and makes review harder.

### Add `FOR STMT <n>` to Plain `EXPLAIN ROUTINE`

Not included here. In the current design, `FOR STMT <n>` is a runtime drill-down selector for `EXPLAIN ANALYZE ROUTINE`, not a second static rendering surface.

## Unresolved Questions

- Whether a future iteration should cache one analyze execution so summary and drill-down do not require separate invocations.
- Whether the static view should eventually render more dynamic SQL cases directly instead of using placeholder rows.
