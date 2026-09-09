# `pkg/ddl` MV planner access checks receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope:
`CheckMViewReadable` + `CheckMViewUpdatable` + `allowMViewMaintenanceBypass`
from `pkg/planner/core/util.go` — the planner-tier access checks that
prevent DML and premature reads on materialized view tables.

## Drift content and Rust alignment

- `check_m_view_readable`: refuses reads on a materialized view whose
  `InitBuildState` is `Building` (or `Deferred`) with Go's exact error
  message (`materialized view {name} initial build is in progress` /
  `materialized view {name} is not ready: initial build has not
  completed`). `Ready` state passes. Internal maintenance sessions bypass.
- `check_m_view_updatable`: refuses DML on any table with
  `MaterializedView` or `MaterializedViewLog` metadata unless the session
  is in internal maintenance mode. Non-MV tables pass through. Go's
  `ErrNonUpdatableTable` message is reproduced with the alias/table name
  and the operation string.
- `allow_m_view_maintenance_bypass`: implements Go's dual-gate check —
  the maintenance flag must be set AND the session must be running
  restricted SQL. A maintenance flag without restricted SQL produces
  Go's `ErrInternal` message.

## Known gaps recorded (not fixed in this batch)

- These checks are exposed as pure functions for the caller to wire into
  the query execution path (SELECT/INSERT/UPDATE/DELETE dispatch). The
  actual wiring into the session's table access path is the
  session-variable mirror seam (batch 19's documented reduction).

## Regression tests

Six tests in `mview_helpers.rs::mview_access_tests`: the Building-state
read refusal (exact message), the Ready-state passthrough, the
maintenance-session bypass, the restricted-SQL requirement for the
bypass, the MV-table DML refusal (with alias and operation), and the
non-MV-table passthrough.

Fail-before evidence: before this batch no access-check functions
existed; each test binds to symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1155 passed, 7 failed — the exact base failure set.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor -E 'test(mview_access) + test(readable_) + test(updatable_) \
  + test(maintenance_bypass_)'
# 6/6 passed.
```

No Go source changed in this batch.
