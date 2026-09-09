# `pkg/ddl/session` + `pkg/ddl/mview_schedule_expr.go` — schedule-eval seam receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). This batch lands the
DDL-session seam that the materialized-view schedule chain requires, plus the
session-driven half of `mview_schedule_expr.go`. The AST/type-inference half
(`BuildAndValidateMViewScheduleExpr` + `restoreNodeToCanonicalSQL`) lands
with the create-path sub-batch that owns the AST entry point.

## Drift content and Rust alignment

- `pkg/ddl/session` was already fully transcreated in
  `tidb-ddl-session` (`Session` begin/commit/rollback/reset/execute/
  run_in_txn, `Pool` get/put/destroy, `NotifyBeginTxnChannel`) — verified
  against Go master; no drift in the package itself.
- `setCreateMaterializedViewScheduleEvalSession` → three new capability
  methods on `tidb-ddl-session`'s `SessionContext` trait:
  `install_schedule_eval_session(sql_mode, zone) -> ScheduleEvalOriginals`,
  `restore_schedule_eval_session(&originals)` (Go's returned closure), and
  `eval_schedule_expression(expr_sql) -> Result<Option<Time>>` (Go's
  `generatedexpr.ParseExpression` + `BuildSimpleExpr` + `Eval` +
  `ConvertTo(TypeDatetime, MaxFsp)` chain executed against the
  implementing session; SQL NULL yields `None`).
  `ScheduleEvalOriginals` carries Go's five captured originals.
- `deriveCreateMaterializedScheduleNextUnixSeconds` +
  `loadCreateMaterializedViewScheduleNow` +
  `evalCreateMaterializedViewScheduleExprToDatetime` +
  `deriveCreateMaterializedView{,Log}NextUnixSeconds` + the two NULL-schedule
  loggers → `tidb-executor/src/ddl/mview_schedule_expr.rs`, generic over the
  session context, preserving Go's flow exactly: trim, both-empty short
  circuit, `SELECT NOW(6)` load (label `mview-refresh-info-next-time-now`),
  START-with precedence, the 10-second near-now threshold
  (`now + 10s` compared with `Time::compare`), NEXT fallback, NULL-branch
  logging through Go's injected `logNullUpdate` parameter, and the
  `(next_unix_seconds, should_update)` result shape. The loggers use Go's
  exact messages (error level when NEXT is written, warn otherwise) through
  `tracing`.

## Known gaps recorded (not fixed in this batch)

- `BuildAndValidateMViewScheduleExpr` + `restoreNodeToCanonicalSQL` land with
  the create-path sub-batch (b) that owns the AST-to-DDL entry point;
  `tidb_expr::build_simple_expr` and the AST restore pieces are ready.
- The trait extension is capability-shaped: concrete `SessionContext`
  implementations outside this workspace must add the three methods (the
  crate's own mock demonstrates the full implementation).

## Regression tests

`tidb-ddl-session/src/tests.rs`: the mock context implements the new
capabilities with observable state (installed mode/zone, captured
originals).

`tidb-executor/src/ddl/mview_schedule_expr.rs::tests` (9 running tests), each
mirroring a Go flow with a purpose-built session mock:

- `empty_expressions_skip_the_update` (both-empty short circuit, no log);
- `start_with_alone_sets_next_to_the_start_instant`;
- `start_far_future_ignores_next` (START beyond the 10s window wins; NEXT is
  never evaluated — one eval call);
- `start_near_now_uses_next` (START already due, NEXT decides);
- `start_evaluating_null_logs_and_skips_the_update` (NULL START, injected
  logger records the `START WITH` clause);
- `next_only_sets_next_instant` (the log-variant wrapper);
- `failed_now_evaluation_errors_with_the_job_message` (empty NOW row set →
  Go's `create materialized view: failed to evaluate refresh schedule
  expression`);
- `derive_does_not_install_the_eval_session_itself` (Go installs in the
  CREATE path, not the derive);
- `nil_metadata_reports_no_update` (both nil-info wrappers).

Fail-before evidence: the module, the trait capabilities, and the mock state
do not exist in the pre-batch tree; the derive tests bind to symbols absent
before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-ddl-session --no-fail-fast
# 5/5 passed (full owner suite)
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor --no-fail-fast
# failure set identical to the pre-batch base commit `fa74e961db`
# (29 documented pre-existing baseline failures; zero new, zero fixed)
cargo +nightly-2026-08-22 check --offline -p tidb-executor --tests
# 0 errors
```

No Go source changed in this batch.
