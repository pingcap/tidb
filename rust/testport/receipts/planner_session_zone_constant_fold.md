# Rust planner session-zone constant-fold receipt

Status: bounded Rust-only alignment batch; this receipt covers constant
folding of timezone-sensitive literal expressions in the executable planner.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go planner package was
enumerated and read from the fetched tree: 568 artifacts under
`pkg/planner/core` (196 production Go files, 166 tests, and generated sources,
fixtures, platform variants, `BUILD.bazel`, and ownership/build metadata).
The related Go expression package was rechecked in full: 208 artifacts (117
production Go files, 78 tests, generated/build inputs, and package metadata).
The session package was also rechecked in full: 92 artifacts (25 production Go
files, 45 tests, and fixture, generated, platform, build, and metadata files).
No Go, generated, fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-planner` has 345
tracked files and `tidb-session` has 222, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust file is
`tidb-planner/src/plan_builder.rs`; the focused end-to-end regression is in
`tidb-session/src/tests_timezone_storage.rs`.

## Alignment

Go's expression rewriter receives the statement `BuildContext`, whose
`GetTimeZone()` is the session's `SessionVars.Location()`. When a literal-only
subtree is folded while planning, `UNIX_TIMESTAMP('2021-11-07 01:30:00')` in
`America/Los_Angeles` therefore uses the zone's DST rules and Go's
`time.Date` repeated-hour resolution.

Rust's `PlanScopeResolver` carried the session zone for expression building,
but its `fold_constant` hook evaluated closed subtrees against
`tidb_expr::NoColumns`. That resolver intentionally uses the deterministic
goeval default (`UTC+11`), so a literal `UNIX_TIMESTAMP` was frozen before
execution with the wrong epoch seconds even though the surrounding statement
context was `America/Los_Angeles`.

The planner now evaluates those closed subtrees with
`tidb_expr::ZonedNoColumns(self.time_zone.clone())`. The no-column semantics
remain unchanged; only the session zone is carried into the fold. The existing
`unix_timestamp_agrees_with_the_stored_resolution` session regression now
passes and pins the unambiguous neighbors plus the repeated-hour value.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_timezone_storage::unix_timestamp_agrees_with_the_stored_resolution -- --exact --nocapture --test-threads=1`

The focused timezone-storage regression passed.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only planner-time evaluation of closed literal subtrees changes: it now uses
the already-captured statement zone. Row-dependent expressions, runtime
evaluation, fixed-offset sessions, and the deterministic no-column behavior
outside a plan scope are unchanged. No Go source, generated output, fixture,
platform variant, or build artifact was modified.

## Follow-up: fold each builtin as it is constructed, with the live context

`PlanScopeResolver::fold_constant` used to defer every value fold when a live
statement context was present, relying on the single top-level
`fold_constant_in_mode(Normal)` at the end of
`PlanBuilder::rewrite_scalar_with_scope`. That single fold cannot reach a
closed leaf under a non-constant parent: `fold_current_value_in` only
descends through lazy short-circuit functions, so a constant `DATE_ADD` under
`LT` under `AND` never folded. Go's `expression.NewFunction` instead folds
each builtin as it is constructed (`foldConstant` in the live `BuildContext`),
which is why the recorded q14 predicate carries
`lt(l_shipdate, 1997-01-01 00:00:00.000000)`.

The resolver now folds the freshly built node immediately with
`self.warning_context` when that context exists, which is the same context
the deferred fold used, so conversion diagnostics keep their owner. The
`last_insert_id` replay special case is subsumed by the general fold.

Regression: `a_constant_date_add_in_a_predicate_folds_before_push_down`
(executor driver tests) plans
`SELECT * FROM t WHERE d >= '1996-01-01' AND d < DATE_ADD('1996-12-01', INTERVAL 1 MONTH)`
and asserts the predicate is the literal `1997-01-01 00:00:00.000000` with no
`date_add` left. It failed before with
`lt(test.t.d, cast_datetime(date_add_month("1996-12-01", 1)))` and passes
after.

```text
cargo test -p tidb-expr
# 1206 passed; 2 failed; 99 ignored -- the same two pre-existing failures

cargo test -p tidb-planner
# 1279 passed; 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1242 passed; 14 failed; the same pre-existing set
```
