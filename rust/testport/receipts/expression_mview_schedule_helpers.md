# `pkg/expression/helper.go` — materialized-view schedule helpers receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`helper.go` drift introduced by the materialized-view DDL commit
`94a9cbedab` — three new exported functions appended after
`boolToInt64`. The remainder of `pkg/expression` keeps its own area
receipts; no other `helper.go` behavior drifted in this window.

## Drift content and Rust alignment

- `MaterializedScheduleTimeToUnixSeconds(t *types.Time, scheduleTimeZone
  *time.Location) (*int64, error)` →
  `materialized_schedule_time_to_unix_seconds(Option<CoreTime>,
  Option<&ResolvedTimeZone>) -> Result<Option<i64>, String>` in
  `tidb-expr/src/expr_util/mview_schedule.rs`. Nil-time and nil-zone
  boundaries keep Go's `(nil, nil)` and `"materialized schedule timezone is
  unavailable"` results; `types.Time.GoTime(zone).Unix()` maps through
  `CoreTime::to_datetime` under `ResolvedTimeZone::{Local, Named, Fixed}`
  (the Fixed offset builds a `chrono::FixedOffset`, Go's fixed zone).
- `MaterializedScheduleTypeFlagsWithSQLMode(mode mysql.SQLMode) types.Flags`
  → `materialized_schedule_type_flags_with_sql_mode(SqlMode) ->
  ConversionFlags`: `types.StrictFlags` is the crate's `STRICT_FLAGS`, and
  Go's `WithTruncateAsWarning` / `WithIgnoreInvalidDateErr` /
  `WithIgnoreZeroInDate` / `WithCastTimeToYearThroughConcat` chain maps to
  `with_truncate_as_warning` / `with_ignore_invalid_date_err` /
  `with_ignore_zero_in_date_err` / `with_cast_time_to_year_through_concat`.
- `MaterializedScheduleErrLevelsWithSQLMode(mode mysql.SQLMode)
  errctx.LevelMap` → `materialized_schedule_err_levels_with_sql_mode(SqlMode)
  -> LevelMap`: Go's `LevelMap` zero value is `LevelMap::strict()`, and the
  four groups resolve through `resolve_err_level` exactly where Go does —
  Truncate/BadNull/NoDefault with `(ignore=false, warn=!strict)` and
  DividedByZero with `(ignore=!HasErrorForDivisionByZeroMode,
  warn=!strict)`. Go's `ResolveErrLevel` lets `ignore` win over `warn`, so
  non-strict bundles without the division flag resolve DividedByZero to
  `Level::Ignore` (pinned by a test).

The Rust owner previously carried no materialized-view helper surface, so
this batch implements the missing Go behavior; no Rust-only behavior needed
removal. `tidb-expr` gains a `tidb-model` dependency (acyclic) for the
`ResolvedTimeZone` parameter type; `tidb-error` and `tidb-datatype` were
already dependencies.

## Known gaps recorded (not fixed in this batch)

None for the drifted surface. The helper's callers (the `pkg/ddl` mview
schedule-expression builder and refresh worker) are queued separately.

## Regression tests

`expr_util::mview_schedule::tests` (4 running tests):

- `nil_time_and_missing_zone_boundaries` — Go's `(nil, nil)` and the exact
  unavailable-timezone error text;
- `schedule_time_converts_under_the_given_zone` — a `+08:00` fixed zone and
  a named `UTC` zone resolving the same instant to the same Unix seconds;
- `type_flags_follow_the_sql_mode` — strict versus non-strict
  `ALLOW_INVALID_DATES` bundles across all four flags;
- `err_levels_follow_the_sql_mode` — all four groups under strict and
  relaxed bundles, including the ignore-over-warn DividedByZero resolution.

Fail-before evidence: the module and its tests do not exist in the pre-batch
tree (no Rust owner carried any of this behavior), so the tests bind to
symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1127/1127 passed (full owner suite, including the 4 new regressions)
cargo +nightly-2026-08-22 check --offline -p tidb-expr
# 0 errors
```

No Go source changed in this batch.
