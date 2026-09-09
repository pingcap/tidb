# `pkg/executor/sortexec` RankTopN prefix-key parity

Status: Ready for this focused package batch. This receipt covers the two Go
artifacts that own the RankTopN boundary scan, not the complete `sortexec`
package; the package inventory in `rust/docs/parity/sortexec-package-inventory.md`
still records the remaining benchmark and failpoint blockers.

## Go authority

Go source: `origin/master` `f5cf8f6337` (`executor: track window memory usage
(#70630)`, 2026-09-08). The two artifacts are byte-identical to the inventory's
claim-unit commit `4f09ce1bc5ce`:

- `pkg/executor/sortexec/topn.go` (899 lines): `rankInfo`,
  `initBeforeLoadingChunks`, `getPrefixKeys`, `findEndIdx`,
  `loadChunksUntilTotalLimitForRankTopN`, `SetTruncateKeyMetasForTest`.
- `pkg/executor/sortexec/rank_topn_test.go` (216 lines): `TestRankTopN` and its
  single-column and two-column cases.

Both files were read in full. The package has no other RankTopN artifact: no
generated input, platform variant, fixture, or Bazel target of its own.

## Gap and Rust ownership

Go's `rankInfo.TruncateKeyExprs` is a slice. `getPrefixKeys` builds one
`truncateKey` per entry and `findEndIdx` compares the whole slice with
`slices.Equal`, so the boundary group ends when ANY declared column's prefix
changes. `rank_topn_test.go` exercises two entries with prefix counts `-1`
(whole value) and `12` (12-character truncation).

Rust `tidb-executor/src/topn.rs` held one `RankPrefix { column_idx, prefix_len,
field_type }`, so only the planner's single `PrefixCol`/`PrefixLen` pair could
be represented and the two-column Go case was unportable. `RankPrefix` now
holds a `Vec<RankPrefixColumn>`; `with_rank_prefix` keeps the planner surface,
`with_rank_prefixes` installs every `TruncateKeyExprs` entry, `rank_prefix_key`
returns one key per entry, and `rank_prefixes_equal` requires equal lengths and
applies each column's own rule (`-1` = exact whole-value equality, otherwise
the column collation on the truncated value).

## Fail-before / pass-after

`topn::tests::rank_topn_compares_every_declared_prefix_column` ports both
`rank_topn_test.go` cases with `batch = 1` so each row is its own child chunk.
Scenario A's first post-boundary row differs only in column 1's 12-character
prefix; scenario B's differs only in column 0. Both assert that exactly three
rows were pulled from the child.

With `rank_prefixes_equal` temporarily reduced to the first column
(`prefix.columns.iter().take(1)`), scenario A failed with
`left: 5, right: 3` — the operator kept reading chunks whose first column still
matched the boundary. The restored implementation passes all four RankTopN
tests.

## Prerequisite build restore

The `tidb-executor` lib-test target did not compile on the fetched branch: three
call sites in `driver/planner_bridge.rs`'s
`list_columns_prunes_each_referenced_column_and_intersects_tuple_groups` still
called `list_columns_pruned_ids` with three arguments after the function gained
its `&StmtContext` parameter (`error[E0061]`). The test now builds a
`StmtContext::for_query()` and passes it, matching the neighbouring
`partition_pruning_fallback_keeps_explicit_partition_names` test. No production
code changed for this restore.

## Validation

Profile: **Ready** for this focused parity batch within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git diff --stat 4f09ce1bc5ce origin/master -- pkg/executor/sortexec/topn.go
  pkg/executor/sortexec/rank_topn_test.go` — empty; no Go drift.
- Pre-fix `cargo test -p tidb-executor --lib
  topn::tests::rank_topn_compares_every_declared_prefix_column` — failed with
  `left: 5, right: 3` under the first-column-only comparison.
- `cargo test --offline --locked -j12 -p tidb-executor --lib
  topn::tests::rank_topn` — passed (4 tests).
- `cargo test --offline --locked -j12 -p tidb-executor --lib` — the four
  RankTopN tests pass and the batch adds no failure. The branch's full lib
  suite is `1107 passed; 115 failed` both before and after this batch (the
  115 failures are pre-existing and outside `topn`; `hash_agg_spill_tests::
  each_round_gives_the_statements_budget_back` is order-dependent and flips
  between runs, and `driver::tests::point_get::
  residual_selection_uses_logical_rows_over_access_rows` fails identically
  with `topn.rs` reverted).
- `cargo check --offline --locked -j12 -p tidb-executor --all-targets` —
  passed; existing warnings only.
- `cargo clippy --offline --locked -j12 -p tidb-executor --all-targets` —
  passed with the branch's existing warnings; no new lint.
- `cargo fmt --all -- --check` — the two changed files are rustfmt-clean. The
  branch has pre-existing drift in `tidb-executor/src/ddl.rs`,
  `tidb-executor/src/ddl/alter_table.rs`, and
  `tidb-server/src/cluster_session_node/mod.rs`, none of which this batch
  touches.
- `git diff --check -- rust` — passed.

No Go, Bazel, Cargo manifest, generated, or fixture file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: the single-column planner path is unchanged; multi-column
  comparison only adds the entries Go already compares.
- Compatibility: `with_rank_prefix` keeps its signature; `with_rank_prefixes`
  is new.
- Performance: one prefix key per declared column, exactly the planner's
  declared count, so the single-column path evaluates the same work as before.
