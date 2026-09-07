# `pkg/planner/core` — physical index-join explain parity receipt

## Scope and complete owner inventory

This batch follows Go `master` for the physical index-join candidate, column
allocation, hint-side selection, and `EXPLAIN FORMAT='brief'` contract. Before
editing, the complete tracked Go package tree was inventoried:

```text
git ls-tree -r --name-only master -- pkg/planner/core
git grep -n -E '^func ' master -- 'pkg/planner/core/**/*.go'
```

| Go package tree | Tracked files | Go files | Go production | Go tests | build inputs | fixture/testdata/golden | Go functions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `pkg/planner/core` (including operator subtrees) | 544 | 344 | 192 | 152 | 59 | 141 | 2,625 |

The review covered every listed production and test file, generated/platform
variant, build input, fixture, testdata and golden file. No Go file was edited.
The direct Go owners are:

* `pkg/planner/core/operator/logical/logical_join.go:1595-1657`, where
  `INL_JOIN` records the requested inner side;
* `pkg/planner/core/exhaust_physical_plans.go:1275-1291,1640-1703`, which
  enumerates and filters index-join families and handles invalid force hints;
* `pkg/planner/core/operator/physicalop/physical_index_join.go:77`, whose
  plancodec type is `IndexJoin`;
* `pkg/planner/core/explain.go:578-635`, which renders `inner:`, outer/inner
  keys, equality conditions, and child conditions; and
* `pkg/planner/core/explain.go:159-177`, where `TableRangeScan` is the dynamic
  clustered-handle scan shape.

## Failure and implementation

Rust physical joins all inherited the base `Join` type in explain output, so
the hinted plan was printed as `Join` rather than Go's `IndexJoin`. The index
join's runtime table path also retained the static full-range descriptor and
lost Go's `range: decided by [Column#N]` text. Finally, Rust's projection key
metadata used a non-allocating sentinel for Go's temporary
`buildSchemaByExprs` columns; the later cast column was therefore `Column#10`
instead of the recorded `Column#12`. With the correct join names visible, an
opposite-side `INL_JOIN(t_idx_str)` could also silently fall back to a normal
index join, whereas Go falls back to a hash join when the requested side has no
usable index-join candidate.

The Rust-only changes are:

* `rust/crates/tidb-executor/src/explain.rs:313-373,469-577,698-875` maps
  HashJoin/MergeJoin/IndexJoin families to Go plancodec names, renders the Go
  index-join info fields (including `nulleq` for null-safe keys), and threads a
  narrowly scoped runtime context through the inner table-reader subtree so a
  dynamic clustered scan is named and described as `TableRangeScan`.
* `rust/crates/tidb-planner/src/logical/rewrite.rs:1771-1839` and
  `logical/rule.rs:1000-1008` preserve the statement-visible allocation side
  effects of Go's `LogicalProjection.buildSchemaByExprs` during production
  `BuildKeyInfoPortal` calls. The existing context-free helper remains useful
  to operator tests and retains sentinel matching semantics.
* `rust/crates/tidb-planner/src/find_best_task/dispatch.rs:414-457,2773-2804`
  filters index candidates that contradict an `INL_JOIN`, `INL_HASH_JOIN`, or
  `INL_MERGE_JOIN` side/family hint before normal-cost fallback, matching Go's
  force-hint behavior when no candidate satisfies the requested side.

## Regression coverage

`rust/crates/tidb-session/src/tests_join_key_cast.rs:79-106` now checks the
complete hinted `IndexJoin` info (`inner:Projection`, outer/inner keys, and
`equal cond`) and the dynamic `TableRangeScan` range column. The same focused
module covers the opposite-side hint fallback, returned rows, and the
multi-way join shape. The executor unit test
`explain::tests::physical_join_names_match_go_plancodec_types` covers all three
index-join families plus hash and merge joins.

Focused commands and results:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_join_key_cast -- --nocapture
  4 passed, 0 failed
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib explain::tests -- --nocapture
  2 passed, 0 failed
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib find_best_task::dispatch::tests -- --nocapture
  26 passed, 0 failed
```

## Ready validation profile

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
rustfmt +nightly-2026-08-22 --edition 2021 --check \
  rust/crates/tidb-executor/src/explain.rs \
  rust/crates/tidb-planner/src/find_best_task/dispatch.rs \
  rust/crates/tidb-planner/src/logical/rewrite.rs \
  rust/crates/tidb-planner/src/logical/rule.rs \
  rust/crates/tidb-session/src/tests_join_key_cast.rs
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

The focused rustfmt command, `git diff --check`, executor all-target check,
and Ready lint all pass; the existing workspace warnings remain non-fatal.
The repository-wide `cargo fmt --all -- --check` also ran after rebase but is
currently red on unrelated pre-existing upstream files (`access_cost.rs`,
planner test helpers, and other files outside this batch). None of this
batch's Rust files are listed in that format diff, and no unrelated formatting
was changed.
