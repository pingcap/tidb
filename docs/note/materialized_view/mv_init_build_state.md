# Materialized View Initial-Build State

This document records the design for introducing an explicit initial-build state
for materialized views.

The goal is to separate:

- "the MV object already exists in metadata"
- "the MV data is ready for user query and normal refresh"

The state is part of MV object metadata, instead of being inferred indirectly
from refresh metadata tables.

## Motivation

Today `CREATE MATERIALIZED VIEW` creates the MV object first and then runs the
initial build.

That causes two classes of problems:

1. The MV can become externally visible before its initial build is ready.
   - User `SELECT` can observe an empty or incomplete MV.
2. Normal refresh flows can run before the initial build is ready.
   - This includes user-triggered refresh and internally scheduled refresh.
   - In the mild case this causes confusing errors.
   - In the worst case this can lead to correctness risks because "initial build"
     and "normal refresh" are semantically different operations.

The current refresh metadata is not a reliable readiness signal:

- `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` is already used by the
  create path for purge safety.
- During `CREATE MATERIALIZED VIEW`, the DDL worker prewrites a row into
  `mysql.tidb_mview_refresh_info` before the initial build starts.
- Therefore `LAST_SUCCESS_READ_TSO` cannot be reused as "init build is ready".

Readiness must be represented explicitly.

## Design Summary

Introduce an explicit MV initial-build state in `TableInfo.MaterializedView`.

Suggested shape:

```go
type MVInitBuildState byte

const (
    MVInitBuildDeferred MVInitBuildState = iota
    MVInitBuildBuilding
    MVInitBuildReady
)

type MaterializedViewInfo struct {
    ...
    InitBuildState MVInitBuildState `json:"init_build_state,omitempty"`
}
```

The state is the source of truth for whether the MV may:

- be queried by users
- be refreshed by normal refresh flows

The state is stored in object metadata because:

- query planning/execution already consumes `TableInfo`
- readiness is part of MV object semantics, not only scheduling metadata
- system tables such as `mysql.tidb_mview_refresh_info` are still needed for
  refresh/purge bookkeeping, but should not carry object-readiness semantics

## Why 3 States Instead of a Boolean

A boolean such as `InitBuildDone` cannot distinguish:

- initial build has not started yet
- initial build is currently running

These two cases need different behavior:

- `Deferred`
  - user should be told the MV is not built yet
  - future build entrypoint should be allowed to start the first build
- `Building`
  - user should be told the initial build is already in progress
  - another build must not start
- `Ready`
  - normal query and normal refresh are allowed

The minimum state set that fully represents the intended behavior is:

- `Deferred`
- `Building`
- `Ready`

## State Ownership

`InitBuildState` is the only readiness source of truth.

Responsibilities of the two metadata layers become:

- `TableInfo.MaterializedView.InitBuildState`
  - controls queryability and normal refreshability
- `mysql.tidb_mview_refresh_info`
  - stores refresh scheduling and refresh watermarks
  - continues to support purge safety and refresh execution
  - does not define whether the MV is externally ready

## Immediate-Build Flow

This section describes the current non-deferred `CREATE MATERIALIZED VIEW`
behavior after introducing `InitBuildState`.

### Phase 1: Create MV object

When `CREATE MATERIALIZED VIEW` creates the MV table object:

- set `MaterializedView.InitBuildState = Building`
- create the MV table
- update schema version
- prewrite `mysql.tidb_mview_refresh_info` for purge safety

At this point:

- the MV object exists
- the MV is not queryable
- the MV is not refreshable

This requires one schema version because object metadata has changed and all
TiDB nodes must observe the same state.

### Phase 2: Run initial build

The initial build runs as today, using the existing create-MV build path.

During this phase:

- `InitBuildState` remains `Building`
- user `SELECT` is blocked
- normal refresh is blocked

### Phase 3: Mark MV ready

After the initial build succeeds:

- update `mysql.tidb_mview_refresh_info` with the final build read tso and
  create-time `NEXT_TIME`
- update `MaterializedView.InitBuildState` from `Building` to `Ready`
- update schema version again
- finish the DDL job

This second schema version is required because the object-readiness metadata has
changed.

### Rollback

If `CREATE MATERIALIZED VIEW` rolls back:

- delete the MV object
- delete the prewritten `mysql.tidb_mview_refresh_info` row

No extra readiness transition is needed because the object itself disappears.

## Query Gating

Normal user reads must be blocked unless `InitBuildState == Ready`.

Suggested rule:

- `Deferred`: reject query
- `Building`: reject query
- `Ready`: allow query

The gate should use MV object metadata, not refresh-info table state.

This is important because:

- the refresh-info row can already exist before initial build finishes
- the refresh watermark may carry purge-safety meaning during create
- query semantics should not depend on a side metadata table

Recommended behavior:

- gate in the planner/executor path where MV table metadata is resolved
- return an explicit user-facing error

Example error direction:

- `materialized view is not ready: initial build has not completed`
- `materialized view initial build is in progress`

Metadata-inspection statements such as `SHOW CREATE MATERIALIZED VIEW` do not
need to be blocked by this state.

## Refresh Gating

All normal refresh entrypoints must require `InitBuildState == Ready`.

This includes:

- user `REFRESH MATERIALIZED VIEW`
- internal scheduled refresh
- complete refresh variants
- fast refresh

Suggested rule:

- `Deferred`: reject normal refresh
- `Building`: reject normal refresh
- `Ready`: allow normal refresh

Recommended error direction:

- `Deferred`: `materialized view is not ready: initial build has not completed`
- `Building`: `materialized view initial build is in progress`

### Scheduler behavior

The scheduler should not pick MVs whose state is not `Ready`.

However, scheduler-side filtering is only an optimization.

The hard correctness gate must still exist in refresh execution, because:

- users can trigger refresh manually
- scheduler filtering alone cannot prevent all invalid entrypoints

## Interaction With DDL Serialization

If future initial build is exposed as a real DDL entrypoint such as:

```sql
ALTER MATERIALIZED VIEW <mv> BUILD
```

then build-vs-build concurrency can largely reuse existing DDL job
serialization.

That solves:

- two concurrent build DDLs for the same MV
- build DDL vs other DDLs on the same MV

Even with DDL serialization, `InitBuildState` is still needed because DDL
serialization does not block:

- normal user `SELECT`
- non-DDL refresh entrypoints

## Future Deferred Build

This design is intentionally forward-compatible with deferred build.

When deferred build is introduced, the same state machine can be reused without
changing the meaning of existing states.

Suggested future flow:

1. `CREATE MATERIALIZED VIEW ... DEFER BUILD`
   - create MV object
   - set `InitBuildState = Deferred`
   - finish create DDL
2. `ALTER MATERIALIZED VIEW ... BUILD`
   - check state must be `Deferred`
   - transition `Deferred -> Building`
   - update schema version
   - run initial build
   - on success transition `Building -> Ready`
   - update schema version again

This is the main reason the current design uses a 3-state enum instead of a
boolean.

The existing non-deferred create path can be viewed as a special case:

- it enters directly at `Building`
- then transitions to `Ready`

So the proposed state machine already has enough expressive power for both:

- current immediate initial build
- future deferred initial build

## Non-goals

This document does not define:

- the final user-facing SQL syntax for deferred build
- whether future "first build" should reuse `REFRESH MATERIALIZED VIEW` syntax
  or use a dedicated `ALTER MATERIALIZED VIEW ... BUILD` action
- async build execution semantics
- observability additions for the new state

Those can be designed later on top of the same metadata state model.

## Summary

The proposed change is:

1. Add `InitBuildState` to `TableInfo.MaterializedView`.
2. Use it as the only readiness source of truth.
3. Maintain it through create/build transitions with schema-version updates.
4. Block normal `SELECT` and normal `REFRESH` unless the state is `Ready`.
5. Keep `mysql.tidb_mview_refresh_info` focused on refresh/purge metadata,
   instead of overloading it with object-readiness semantics.
6. Reuse the same state machine for future deferred build support.
