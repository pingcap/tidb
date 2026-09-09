# `pkg/infoschema` — materialized-view diff slice receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`pkg/infoschema/builder.go` drift introduced by the materialized-view DDL
commit `94a9cbedab` (two switch-arm extensions, +10/−2). The package's
broader `ApplyDiff`/bundle behavior keeps its own area receipts.

## Drift content and Rust alignment

- Go `getTableIDs`: `ActionCreateMaterializedView` and
  `ActionCreateMaterializedViewLog` join `ActionCreateTable` (the diff's own
  table ID names the new physical table) → the Rust
  `apply_schema_diff`'s create arm now accepts all three action types and
  routes them through the same incremental `create_table` reload.
- Go `updateBundleForTableUpdate`: the placement-bundle cache arms
  (`markTableBundleShouldUpdate` for the log create, the conditional
  mark-or-delete for the view create) — **owner missing**: the Rust
  incremental reload tier (`tidb-exec/src/catalog_reload.rs`) does not model
  Go's placement-rule bundle cache at all (unsupported actions refuse to a
  full reload, which is always correct). Recorded as an inventoried gap that
  belongs to a future placement-bundle owner; the incremental reload itself
  is unaffected because bundle bookkeeping is not part of its contract.

## Regression tests

`crates/tidb-exec/tests/catalog_reload_source.rs` (2 new running tests),
using parsed real Go-shaped `TableInfo` JSON fixtures that carry the
batch-1 `meta/model` metadata:

- `a_create_materialized_view_log_diff_adds_exactly_that_table` — an
  `ACTION_CREATE_MATERIALIZED_VIEW_LOG` diff reloads incrementally (no full
  reload), publishes exactly one new table, and the
  `materialized_view_log` metadata (base table 77) survives the reload;
- `a_create_materialized_view_diff_adds_exactly_that_table` — the
  `ACTION_CREATE_MATERIALIZED_VIEW` equivalent, asserting the view's SQL
  content and base-table IDs survive.

Fail-before evidence: with the new action arms reverted, both diffs hit the
`UnsupportedAction` full-reload refusal and both tests fail.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# failure set identical to the pre-batch base commit `460d161a13`
# (8 documented pre-existing baseline failures; the base run additionally
# shows one flaky `executor_utils::tests::worker_pool_two_workers` that did
# not reproduce on this branch). Zero new failures.
```

No Go source changed in this batch.
