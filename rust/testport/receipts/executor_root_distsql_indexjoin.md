# `pkg/executor` Go-master bounded parity receipt

Status: Ready for the fifteen focused executor behavior clusters in this batch. The receipt records the complete root-package inventory at Go `origin/master` `a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (pulled 2026-09-02) and the complete nested `pkg/executor/sortexec` inventory at Go `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (pulled 2026-09-04); it is not a complete transcreation claim for the 101,740-line executor package.

## Complete root inventory

Before editing, every direct production file, test, `BUILD.bazel`, and `OWNERS` artifact was read and inventoried. The root package has 173 direct artifacts and 101,740 lines: 171 Go files plus the build and ownership files, 1,395 top-level test/benchmark/fuzz declarations, and 2,633 function declarations. It has no direct `doc.go`, generated source, fixture, or platform variant.

| Artifact | Lines |
| --- | ---: |
| `BUILD.bazel` | 592 |
| `OWNERS` | 10 |
| `adapter.go` | 2669 |
| `adapter_internal_test.go` | 493 |
| `adapter_slow_log.go` | 283 |
| `adapter_test.go` | 1059 |
| `admin.go` | 919 |
| `admin_plugins.go` | 54 |
| `analyze.go` | 947 |
| `analyze_col.go` | 261 |
| `analyze_col_sampling.go` | 965 |
| `analyze_global_stats.go` | 117 |
| `analyze_idx.go` | 366 |
| `analyze_test.go` | 368 |
| `analyze_utils.go` | 184 |
| `analyze_utils_test.go` | 82 |
| `analyze_worker.go` | 101 |
| `batch_checker.go` | 337 |
| `batch_point_get.go` | 590 |
| `batch_point_get_test.go` | 233 |
| `bench_gencol_test.go` | 110 |
| `benchmark_test.go` | 2094 |
| `bind.go` | 170 |
| `brie.go` | 909 |
| `brie_test.go` | 358 |
| `brie_utils.go` | 186 |
| `brie_utils_test.go` | 423 |
| `builder.go` | 6358 |
| `builder_index_join_cleanup_test.go` | 138 |
| `check_table_index.go` | 938 |
| `checksum.go` | 336 |
| `checksum_test.go` | 42 |
| `chunk_size_control_test.go` | 232 |
| `cluster_table_test.go` | 429 |
| `compact_table.go` | 367 |
| `compact_table_test.go` | 901 |
| `compiler.go` | 598 |
| `copr_cache_test.go` | 95 |
| `coprocessor.go` | 305 |
| `cte.go` | 775 |
| `cte_table_reader.go` | 77 |
| `ddl.go` | 816 |
| `delete.go` | 383 |
| `delete_test.go` | 153 |
| `detach.go` | 177 |
| `detach_integration_test.go` | 419 |
| `detach_test.go` | 73 |
| `distribute.go` | 231 |
| `distribute_table_test.go` | 312 |
| `distsql.go` | 2296 |
| `distsql_test.go` | 757 |
| `executor_failpoint_test.go` | 1238 |
| `executor_pkg_test.go` | 741 |
| `executor_required_rows_test.go` | 792 |
| `expand.go` | 133 |
| `explain.go` | 382 |
| `explain_test.go` | 572 |
| `explain_unit_test.go` | 263 |
| `explainfor_test.go` | 932 |
| `foreign_key.go` | 1017 |
| `grant.go` | 865 |
| `grant_test.go` | 311 |
| `historical_stats_test.go` | 436 |
| `hot_regions_history_table_test.go` | 512 |
| `import_into.go` | 458 |
| `import_into_test.go` | 440 |
| `index_merge_reader.go` | 2109 |
| `infoschema_cluster_table_test.go` | 528 |
| `infoschema_reader.go` | 4315 |
| `infoschema_reader_bench_test.go` | 62 |
| `infoschema_reader_internal_test.go` | 164 |
| `infoschema_reader_keyspace_test.go` | 123 |
| `infoschema_reader_test.go` | 226 |
| `insert.go` | 591 |
| `insert_common.go` | 1848 |
| `insert_test.go` | 788 |
| `inspection_common.go` | 76 |
| `inspection_profile.go` | 813 |
| `inspection_result.go` | 1255 |
| `inspection_result_internal_test.go` | 49 |
| `inspection_result_test.go` | 744 |
| `inspection_summary.go` | 504 |
| `inspection_summary_test.go` | 105 |
| `join_pkg_test.go` | 164 |
| `load_data.go` | 837 |
| `load_stats.go` | 96 |
| `main_test.go` | 71 |
| `mem_reader.go` | 1180 |
| `memtable_reader.go` | 1039 |
| `memtable_reader_test.go` | 1038 |
| `metrics_reader.go` | 365 |
| `metrics_reader_test.go` | 76 |
| `mpp_gather.go` | 150 |
| `operate_ddl_jobs.go` | 228 |
| `opt_rule_blacklist.go` | 53 |
| `parallel_apply.go` | 773 |
| `parallel_apply_test.go` | 1089 |
| `partition_table_test.go` | 2486 |
| `pkg_test.go` | 133 |
| `plan_replayer.go` | 765 |
| `point_get.go` | 874 |
| `point_get_test.go` | 379 |
| `prepared.go` | 240 |
| `prepared_test.go` | 267 |
| `projection.go` | 501 |
| `recommend_index.go` | 108 |
| `reload_expr_pushdown_blacklist.go` | 363 |
| `replace.go` | 235 |
| `resource_tag_test.go` | 224 |
| `revoke.go` | 420 |
| `revoke_test.go` | 245 |
| `sample.go` | 435 |
| `sample_test.go` | 166 |
| `select.go` | 1343 |
| `select_internal_test.go` | 50 |
| `select_into.go` | 256 |
| `select_into_test.go` | 304 |
| `select_test.go` | 70 |
| `set.go` | 437 |
| `set_config.go` | 236 |
| `set_internal_test.go` | 43 |
| `set_test.go` | 1977 |
| `show.go` | 2927 |
| `show_affinity.go` | 158 |
| `show_affinity_test.go` | 284 |
| `show_bdr_role.go` | 52 |
| `show_ddl.go` | 75 |
| `show_ddl_job_queries.go` | 210 |
| `show_ddl_jobs.go` | 408 |
| `show_ddl_jobs_test.go` | 138 |
| `show_next_row_id.go` | 96 |
| `show_placement.go` | 541 |
| `show_placement_labels_test.go` | 81 |
| `show_placement_test.go` | 562 |
| `show_slow_queries.go` | 93 |
| `show_stats.go` | 613 |
| `show_stats_test.go` | 453 |
| `show_test.go` | 274 |
| `shuffle.go` | 506 |
| `shuffle_test.go` | 65 |
| `simple.go` | 3740 |
| `simple_internal_test.go` | 66 |
| `simple_test.go` | 546 |
| `slow_query.go` | 1602 |
| `slow_query_sql_test.go` | 803 |
| `slow_query_test.go` | 1142 |
| `split.go` | 669 |
| `split_test.go` | 452 |
| `statement_ru_plan_walk.go` | 606 |
| `statement_ru_plan_walk_bench_test.go` | 166 |
| `statement_ru_plan_walk_integration_test.go` | 1018 |
| `statement_ru_plan_walk_test.go` | 856 |
| `statement_ru_result.go` | 293 |
| `statement_ru_result_test.go` | 495 |
| `stmtsummary.go` | 425 |
| `stmtsummary_test.go` | 251 |
| `table_reader.go` | 711 |
| `table_readers_required_rows_test.go` | 265 |
| `temporary_table_test.go` | 184 |
| `tikv_regions_peers_table_test.go` | 206 |
| `trace.go` | 295 |
| `trace_test.go` | 75 |
| `traffic.go` | 428 |
| `traffic_test.go` | 629 |
| `union_scan.go` | 332 |
| `union_scan_test.go` | 484 |
| `update.go` | 745 |
| `update_test.go` | 314 |
| `utils.go` | 274 |
| `utils_test.go` | 336 |
| `workloadrepo.go` | 38 |
| `write.go` | 492 |
| `write_concurrent_test.go` | 70 |

Recursive inspection found 519 artifacts, 69 BUILD/OWNERS files, and eight fixture files under `pkg/executor/testdata`. The nested roots `aggfuncs`, `aggregate`, `importer`, `internal`, `join`, `lockstats`, `metrics`, `mppcoordmanager`, `sortexec`, `staticrecordset`, `test`, `testdata`, `unionexec`, and `windows` are separate package or fixture boundaries; their files and generated/platform inputs were inventoried but are not folded into this root claim.

## Go behavior restored

This batch restores two dependency-closed Go-master consumer behaviors:

- IndexReader and IndexLookUp executors now carry a dedicated `rangeMemTracker` for IndexJoin inner tasks. Range construction charges that tracker when supplied, falls back to the executor tracker for ordinary scans, and leaves the executor tracker unset in the dedicated-tracker path. The two focused tests assert positive accounting and fallback semantics.
- Merge-sort IndexLookUp now passes the typed `kv.CoprRequestLimiter` into each request builder. The limiter capacity remains twice the effective DistSQL concurrency, disabled merge-sort returns nil, and the focused regression checks both. This completes the consumer side of the already-restored `pkg/kv`, `pkg/distsql/context`, `pkg/distsql`, and `pkg/store/copr` limiter contract.

The package source and existing tests were compared with the fetched Go master; no unrelated executor features were copied into this bounded batch. Rust `tidb-executor` has no dependency-closed owner for the Go executor tree's IndexJoin partition-pruning memory accounting or merge-sort worker construction, so no speculative Rust facade or Rust-only behavior was added or removed.

## Ready validation

- Pre-fix regression evidence: applying the Go-master range-memory cases to the pre-fix package failed to compile on the missing `rangeMemTracker` fields (and the pre-fix typed-limiter consumer still called the removed `SetCoprRequestRateLimit` API).
- Post-fix focused suite passed with failpoints enabled:

  `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/executor -run 'Test(IndexReaderPartitionRangesUseMemoryTracker|IndexLookUpPartitionRangesUseMemoryTracker|GetMergeSortSharedCoprRequestLimiter)$' -count=1 -vet=off`

- `make lint` passed with the pinned Go environment. `git diff --check` passed.
- `make bazel_prepare` is required because imports and a top-level test changed; it was attempted with the pinned Go environment but is blocked because the local `bazel` executable is unavailable. Existing BUILD metadata already contains `executor_pkg_test.go`; no new test file or BUILD source entry was needed.
- The full root `pkg/executor` suite and live IndexJoin/merge-sort TiKV behavior were not run; nested executor packages remain separate boundaries.

## Risks and remaining boundaries

Correctness risk is limited to tracker selection and limiter capacity wiring; ordinary paths preserve their previous tracker and no-merge behavior. Compatibility risk is limited to using the typed limiter API already defined by `pkg/kv` and `pkg/distsql`. Performance is unchanged when merge-sort is disabled and range tracking adds only the intended accounting. The remainder of the large root executor diff against Go master, nested packages, Bazel analysis, and a native Rust executor worker remain explicit follow-up boundaries.

## Nested `pkg/executor/sortexec` inventory and required-row alignment

The nested Go package was read and inventoried before editing at Go
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. It contains 21
artifacts and 5,919 lines: `BUILD.bazel` (87), `OWNERS` (10),
`benchmark_test.go` (117), `multi_way_merge.go` (200),
`parallel_sort_spill_helper.go` (309), `parallel_sort_spill_test.go` (215),
`parallel_sort_test.go` (189), `parallel_sort_worker.go` (264),
`rank_topn_test.go` (216), `sort.go` (875), `sort_partition.go` (370),
`sort_spill.go` (128), `sort_spill_test.go` (414), `sort_test.go` (141),
`sortexec_pkg_test.go` (143), `topn.go` (899), `topn_chunk_heap.go` (189),
`topn_spill.go` (289), `topn_spill_test.go` (590), and `topn_worker.go` (130).
This inventory includes all production, test, benchmark, build, ownership,
generated/platform, fixture, and build-artifact candidates present under the
nested package; no additional generated or fixture artifact was present.

The Go `SortExec.Next` contract fills the parent chunk until `req.IsFull()`.
The source regression `executor_required_rows_test.go::TestSortRequiredRows`
asserts that pulls requesting 1, 5, 3, and 10 rows from a ten-row sorted run
return 1, 5, 3, and 1 rows respectively. Rust `SortExec::next` previously
used only `max_chunk_size`, so the first pull returned all ten rows. It now
uses `req.required_rows().min(max_chunk_size)` for both the single-partition
and merge paths, preserving the caller's required-row bound while retaining
the executor cap.

Rust owners were inventoried in `tidb-executor`: `sort.rs` (1,431 lines),
`sort_partition.rs` (1,050), `sort_util.rs` (1,385), and
`parallel_sort_spill_helper.rs` (407), 4,273 lines total. The old
`tests_required_rows_source.rs` support file is not declared by `lib.rs` and
is therefore not executable coverage; the Go-derived regression is placed in
the compiled `sort.rs` test module. Rust has no dependency-closed owners for
the nested Go spill worker and TopN families in this bounded fix, so they
remain explicit follow-up boundaries rather than speculative ports.

Pre-fix evidence is the focused test failure (`left 10`, `right 1` for the
one-row request). Post-fix evidence is the same exact test passing, together
with the Ready checks listed below.

## Sort required-row Ready validation

- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --all-targets`
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::sort_honors_each_output_chunk_required_rows -- --exact --nocapture`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The complete `tidb-executor` library suite, spill/TopN integration behavior,
and live TiKV execution were not run; this batch changes only the common sort
pull boundary and its focused regression.

## Parallel sort spill threshold alignment

The same complete `pkg/executor/sortexec` inventory above covers this second
Rust-only fix. Go `sort_spill.go::hasEnoughDataToSpill` returns true when the
sort tracker owns **at least** one tenth of the triggered quota. Rust's
`ParallelSortSpillAction` used a strict `>` comparison, so an exact boundary
would invoke the previous fallback action instead of requesting the
coordinated spill. The production comparison now uses `>=`.

The focused regression is
`sort::tests::parallel_sort_requests_spill_at_exact_tenth_of_quota`. Before
the production change it failed with `the inclusive tenth-of-quota boundary
must request a spill`; after the change it passed. The test constructs the
same quota and operator tracker boundary as Go and calls the actual spill
action, so it observes the branch directly rather than inferring it from a
larger spill run.

Rust ownership is unchanged from the inventory above: `sort.rs` owns the
parallel action and its compiled test module; `sort_partition.rs`,
`sort_util.rs`, and `parallel_sort_spill_helper.rs` remain the other inventoried
owners. No Go, Bazel, generated, fixture, or platform artifact changed.

## Parallel spill threshold Ready validation

- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::parallel_sort_requests_spill_at_exact_tenth_of_quota -- --exact --nocapture` (with a temporary host-only `openssl` vendored feature toggle; reverted immediately after the run)
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

All commands passed after the fix; the Cargo manifest was restored to its
original `openssl = "0.10"` line and has no diff. As with the prior slice, the
full executor suite, spill integration under live TiKV, and native TiKV
behavior were not run.

## Parallel sort spill trigger guard alignment

The same complete `pkg/executor/sortexec` inventory covers this third
Rust-only fix. Go's `parallelSortSpillAction.actionImpl` first requires
`tracker.CheckExceed()` and then checks the sort-owned tenth-of-quota boundary.
Rust had restored the inclusive threshold but omitted the trigger check, so a
sort that had reached its threshold could request a spill during a callback
that had not actually exceeded the statement quota. The production guard now
requires both conditions, matching Go's branch order and fallback behavior.

The focused regression is
`sort::tests::parallel_sort_does_not_spill_before_trigger_tracker_exceeds_quota`.
Before the production change it failed with `a non-exceeded trigger tracker
must not request a spill`; after the change it passed. The test holds the sort
tracker exactly at one tenth of the quota while the callback tracker remains
one byte below its limit, so it directly proves that the quota gate controls
the spill request.

Rust ownership is unchanged from the inventory above: `sort.rs` owns the
parallel action and its compiled test module. No Go, Bazel, generated, fixture,
or platform artifact changed.

## Parallel spill trigger guard Ready validation

- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::parallel_sort_does_not_spill_before_trigger_tracker_exceeds_quota -- --exact --nocapture` (with a temporary host-only `openssl` vendored feature toggle; reverted immediately after the run)
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The focused test passed after the fix; the Cargo manifest was restored to its
original `openssl = "0.10"` line and has no diff. The full executor suite,
parallel spill integration under live TiKV, and native TiKV behavior were not
run.

## TopN spill threshold alignment

The same complete `pkg/executor/sortexec` inventory covers this fourth
Rust-only fix. Go's `TopNSpillAction` resolves the package-level
`hasEnoughDataToSpill` in `sort_spill.go`, whose inclusive threshold is one
tenth of the triggering quota. Rust reused `aggregate`'s helper, which uses a
fifth, so a TopN holding exactly 10% of quota fell through to cancellation
instead of requesting a spill. The TopN action now applies the sortexec
tenth-of-quota check directly; aggregation's separate fifth threshold is
unchanged.

The focused regression is
`topn_spill::tests::topn_requests_spill_at_exact_tenth_of_quota`. Before the
production change it failed with `TopN must request a spill at the inclusive
tenth-of-quota boundary`; after the change it passed. The test calls the actual
TopN action with the operator at 10% and the trigger at its quota, proving the
package-level helper selection and inclusive boundary.

Rust ownership is unchanged from the inventory above: `topn_spill.rs` owns the
TopN action and its compiled regression module, while `agg_spill.rs` retains
the serial aggregation-only helper. No Go, Bazel, generated, fixture, or
platform artifact changed.

## TopN spill threshold Ready validation

- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn_spill::tests::topn_requests_spill_at_exact_tenth_of_quota -- --exact --nocapture` (with a temporary host-only `openssl` vendored feature toggle; reverted immediately after the run)
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The focused test passed after the fix; the Cargo manifest was restored to its
original `openssl = "0.10"` line and has no diff. The full executor suite,
TopN spill integration under live TiKV, and native TiKV behavior were not run.

## Sort/TopN by-item validation alignment

The same complete `pkg/executor/sortexec` inventory covers this fifth
Rust-only fix. Go's `SortExec.buildKeyColumns` and
`TopNExec.initBeforeLoadingChunks` accept only direct child columns and
constants; any scalar, correlated, or other expression returns
`Get unexpected expression` before the child is drained. Rust previously
evaluated arbitrary scalar by-items in `eval_sort_key` and `compare_rows`.
The shared `validate_by_items` gate now rejects those expressions in both
executors, and the low-level key helpers fail closed as well. Constant keys
remain no-op ordering keys, while column keys retain the allocation-free
comparators and spill-merge evaluation.

The focused regressions are
`sort::tests::sort_rejects_non_column_by_item_like_go` and
`topn::tests::topn_rejects_non_column_by_item_like_go`. The Sort regression
failed before the fix because the Rust executor accepted and evaluated the
scalar key; it passes after the gate. The TopN regression exercises the same
shared contract on its own initialization path and passes after the fix.

Rust ownership is unchanged from the nested inventory: `sort.rs` owns the
validation and Sort regression, `topn.rs` owns the TopN call-site and
regression, and `topn_chunk_heap.rs` records the narrowed materialized-key
contract. No Go, Bazel, generated, fixture, or platform artifact changed.

## Sort/TopN by-item validation Ready validation

- Pre-fix `sort::tests::sort_rejects_non_column_by_item_like_go` failed with
  `scalar sort keys must be rejected`, proving the old Rust fallback accepted
  the scalar key.
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::sort_rejects_non_column_by_item_like_go -- --exact --nocapture`
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::tests::topn_rejects_non_column_by_item_like_go -- --exact --nocapture`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The focused tests passed after the fix. The Cargo manifest's temporary
host-only vendored-OpenSSL toggle was reverted to `openssl = "0.10"` and has
no diff. The full executor suite, SQL planner coverage for expression
materialization, spill integration under live TiKV, and native TiKV behavior
were not run.

## TopN output-time spill polling alignment

The same complete `pkg/executor/sortexec` inventory covers this eighth
Rust-only fix. Go's `generateTopNResultsWhenNoSpillTriggered` checks the shared
spill flag every ten output rows. If another executor crosses the session
quota during emission, `spillHeap` writes only the still-unemitted suffix;
already returned rows are not replayed, and the single-run output path does not
apply the offset a second time. Rust previously emitted the in-memory heap
without polling the flag, so an externally raised request was ignored.

`TopNExec::next` now performs the same ten-row poll. The new
`spill_remaining_heap` writes the sorted pointer suffix, clears the in-memory
store, seeds the merge's consumed offset, and continues filling the current
request through the run. No memory is duplicated and the ordinary multi-run
path remains unchanged.

The focused regression is
`topn::spill_tests::topn_spills_remaining_rows_when_triggered_during_output`.
Before the fix it failed because `num_spilled_runs()` stayed zero after the
simulated external trigger; after the fix it passes and verifies the current
request returns rows `8..28`, the remaining merge returns `28..64`, and no
spill file remains after close. Rust ownership is unchanged: `topn.rs` owns
the output poll, suffix spill, and regression; `topn_spill.rs` owns the shared
run writer.

## TopN output-time spill polling Ready validation

- Pre-fix `topn::spill_tests::topn_spills_remaining_rows_when_triggered_during_output` failed with `left: 0, right: 1`, proving the old output path ignored the raised spill flag.
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::spill_tests::topn_spills_remaining_rows_when_triggered_during_output -- --exact --nocapture`
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The focused test passed after the fix. The Cargo manifest's temporary
host-only vendored-OpenSSL toggle was reverted to `openssl = "0.10"` and has
no diff. The full executor suite, concurrent external-memory trigger under a
live SQL session, spill integration under live TiKV, and native TiKV behavior
were not run.

## Sort/TopN constant by-item evaluation alignment

The same complete `pkg/executor/sortexec` inventory covers this ninth
Rust-only fix. Go's `SortExec.buildKeyColumns` and
`TopNExec.initBeforeLoadingChunks` accept constant by-items but omit them from
the materialized comparison key; constants therefore never run through the row
evaluation context and cannot affect ordering. Rust's merge-key path previously
called `Constant::eval`, so a deferred constant could fail with the
Rust-specific unsupported evaluation error (and an ordinary constant could
participate in ordering). `eval_sort_key` now emits a positional `NULL`
placeholder for constants, `less_by_items` skips those slots, and TopN delegates
to the shared helper. Column keys retain the existing evaluation and ordering
behavior.

The focused regression is
`sort::tests::sort_does_not_evaluate_constant_by_item_like_go`. Before the
production change it failed with `left: [Int(2)] right: [Null]`, proving that
the deferred constant was evaluated by the old Rust path; after the change it
passes and also proves the placeholder compares equal to another constant
slot. Rust ownership is unchanged: `sort.rs` owns the helper and regression,
`topn.rs` uses the shared helper, and `topn_chunk_heap.rs` records the
materialized-key contract. No Go, Bazel, generated, fixture, or platform
artifact changed.

## Sort/TopN constant by-item Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::sort_does_not_evaluate_constant_by_item_like_go -- --exact --nocapture` failed with `left: [Int(2)] right: [Null]`.
- The same focused command passed after the production change (with a temporary host-only `openssl` vendored feature toggle; reverted immediately after the run).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The Cargo manifest was restored to its original `openssl = "0.10"` line and
has no diff. The full executor suite, planner materialization coverage,
concurrent constant evaluation under live SQL, spill integration under live
TiKV, and native TiKV behavior were not run.

## TopN repeated post-spill worker rounds alignment

The same complete `pkg/executor/sortexec` inventory covers this tenth
Rust-only fix. Go's `fetchChunksFromChild` checks `isSpillNeeded` after every
dispatched chunk during `executeTopNWhenSpillTriggered`; `topNSpillHelper.spill`
waits for all current workers, drains every worker heap into a sorted run, and
then fetching continues. Rust previously waited for EOF and wrote only one
final run per worker, so a shared trigger raised while workers were processing
could leave their bounded heaps resident until the entire post-spill phase
finished. Rust now tracks a monotonic request generation: each worker drains
once per generation, the last worker acknowledgement clears the shared flag,
and final worker heaps are written as another run. The existing multi-way
merge consumes all intermediate and final runs.

The focused regression is
`topn::spill_tests::parallel_topn_re_spills_worker_heaps_after_shared_trigger`.
Before the production change it failed with `got 2`, showing only the two
final worker runs; after the change it passes with an intermediate run per
worker plus the final runs. Rust ownership remains within `topn.rs` and
`topn_spill.rs`; no Go, Bazel, generated, fixture, or platform artifact
changed.

## TopN repeated post-spill worker rounds Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::spill_tests::parallel_topn_re_spills_worker_heaps_after_shared_trigger -- --exact --nocapture` failed with `a repeated worker spill must create an intermediate run per worker; got 2` (the first host-only run was blocked by missing OpenSSL discovery before the vendored retry).
- The same focused command passed after the fix with a temporary host-only `openssl` vendored feature toggle; the manifest was reverted immediately after the run.
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::spill_tests:: -- --nocapture` passed all 10 TopN spill tests (the broader `--lib topn` filter still exposes four unrelated pre-existing analyze/planner fixture failures).
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --all-targets` passed with the same temporary vendored-OpenSSL toggle; the manifest was reverted immediately after the run.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The full executor suite, concurrent quota-trigger stress with live worker
interleavings, spill integration under live TiKV, and native TiKV behavior were
not run.

## TopN spill cancellation polling alignment

The same complete `pkg/executor/sortexec` inventory covers this eleventh
Rust-only fix. Go's `topNSpillHelper.spillHeap` calls
`SQLKiller.HandleSignal` every 100 heap positions while serializing a sorted
run, before appending that row. Rust's `SpilledRun::write` previously copied
every row without consulting the statement killer, so a query cancelled during
a large spill still completed the file write and returned success. The Rust
writer now receives the statement memory handle, polls at Go's 100-position
cadence, and preserves the original heap index for the output-time suffix path.
`DataInDiskByChunks`'s existing `Drop` cleanup removes a partial run when the
poll returns `ExecError::Killed`.

The focused regression is
`topn::spill_tests::topn_spill_honors_query_kill_during_run_write`. Before the
production change it failed with `a killed spill must return an executor
cancellation error: Ok(())`; after the change it passes and verifies that the
cancelled spill leaves no file after `close`. Rust ownership remains within
`topn.rs` and `topn_spill.rs`; no Go, Bazel, generated, fixture, or platform
artifact changed.

## TopN spill cancellation polling Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +1.97 test --manifest-path rust/Cargo.toml -p tidb-executor --lib topn::spill_tests::topn_spill_honors_query_kill_during_run_write -- --exact --nocapture` failed with `a killed spill must return an executor cancellation error: Ok(())` (after the repository-required Rust 1.97 toolchain and temporary vendored-OpenSSL host workaround were applied).
- The same focused command passed after the fix with `LC_ALL=C LANG=C` and a temporary vendored-OpenSSL feature toggle; the manifest was reverted immediately after the run.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The focused regression was run on Rust 1.97 because the workspace declares
that minimum compiler; the default Rust 1.95 invocation was rejected before
compilation. The full executor suite, cancellation stress with concurrent
workers, spill integration under live TiKV, and native TiKV behavior were not
run.

## Parallel sort worker cancellation polling alignment

The same complete `pkg/executor/sortexec` inventory covers this twelfth
Rust-only fix. Go's `parallelSortWorker.keyColumnsLess` checks the SQL killer
after every 20,000 row comparisons, while
`multiWayMergeLocalSortedRows` checks it every 100 emitted rows. Rust's
parallel worker now carries the statement memory handle through local batch
sorting and in-memory K-way merging. Its comparator checkpoint records the
first cancellation error while allowing the standard sort operation to unwind,
and its merge loop checks before emitting each 100-row boundary. Serial sort
paths retain their existing behavior because Go applies these checkpoints only
to parallel workers.

The focused regressions are
`sort::tests::parallel_worker_honors_query_kill_during_batch_sort` and
`sort::tests::parallel_worker_honors_query_kill_during_local_merge`. Before
the production changes, the batch-sort test returned `Ok([...])` despite a
pending query kill, and the local-merge test likewise returned a complete
`Ok([...])` result. After the changes both tests return
`ExecError::Killed`. Rust ownership remains within `sort.rs` and
`sort_partition.rs`; no Go, Bazel, generated, fixture, or platform artifact
changed.

## Parallel sort worker cancellation polling Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::parallel_worker_honors_query_kill_during_local_merge -- --exact --nocapture` failed with `a killed worker merge must return an executor cancellation error: Ok([...])`.
- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::parallel_worker_honors_query_kill_during_batch_sort -- --exact --nocapture` failed with `a killed worker batch sort must return an executor cancellation error: Ok([...])`.
- Post-fix focused cancellation tests passed with the temporary host-only vendored-OpenSSL feature toggle; the manifest was reverted immediately after the run.
- Existing parallel-sort regressions passed: `parallel_worker_merges_multiple_batches_without_copying_chunks`, `parallel_sort_workers_share_input_and_heap_merge_their_runs`, and `parallel_sort_spills_worker_rounds_and_final_batches`.
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --all-targets` passed with the same temporary vendored-OpenSSL toggle; the manifest was reverted immediately after the run.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The full executor suite, concurrent cancellation stress across all worker
lanes, spill integration under live TiKV, and native TiKV behavior were not
run. The default Rust 1.95 toolchain remains below the workspace's Rust 1.97
minimum; the pinned nightly toolchain was used for the Ready checks.

## Serial sort cancellation polling alignment

The same complete `pkg/executor/sortexec` inventory covers this thirteenth
Rust-only fix. Go's serial `sortPartition.keyColumnsLess` polls the statement
killer every 10,240 row comparisons and checks it after each full spill chunk.
Rust's serial `SortExec` path now passes `StatementMemory` into partition
sorting and serial spill writes, using the 10,240-comparison interval and a
post-chunk check. The parallel worker retains its separate 20,000-comparison
interval and 100-row local-merge polling aligned in the preceding cluster;
the public no-memory partition API remains available for non-statement callers.

The focused regression is
`sort::tests::serial_sort_partition_honors_query_kill_during_batch_sort`.
Before the production change it returned `Ok(())` with a pending query kill;
after the change it returns `ExecError::Killed`. Rust ownership remains within
`sort.rs` and `sort_partition.rs`; no Go, Bazel, generated, fixture, or
platform artifact changed.

## Serial sort cancellation polling Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib sort::tests::serial_sort_partition_honors_query_kill_during_batch_sort -- --exact --nocapture` failed with `a killed serial partition sort must return an executor cancellation error: Ok(())`.
- The same focused command passed after the fix with a temporary host-only vendored-OpenSSL feature toggle; the manifest was reverted immediately after the run.
- Existing serial sort/accounting regressions passed: `a_sort_accounts_its_materialized_rows_against_the_statement`, `test_unparallel_sort_spill_disk`, `a_spilled_descending_sort_returns_every_row_in_order`, and `the_same_sort_raises_8175_when_tmp_storage_is_disabled`.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`

The full executor suite, cancellation during a live serial spill write,
spill integration under live TiKV, and native TiKV behavior were not run. The
default Rust 1.95 toolchain remains below the workspace's Rust 1.97 minimum;
the pinned nightly toolchain was used for the focused validation.

## RankTopN prefix-group short-circuit alignment

The same complete `pkg/executor/sortexec` inventory covers this fourteenth
Rust-only fix. Go's `rankInfo` path in `topn.go` reads chunks until
`offset + count`, then retains only the contiguous rows sharing the final
truncated prefix key. Rust previously carried `PhysicalTopN.prefix_col` and
`prefix_len` through planning metadata but never activated the executor
short-circuit. `TopNExec` now resolves that child column in the physical
builder, cuts its value with the existing `index_prefix_cut` implementation,
compares truncated keys under the source collation (while preserving Go's
exact-value `-1` path), and stops after the boundary prefix group. It sorts
the retained rows by the normal TopN keys and caps output at `offset + count`,
so boundary-group rows are fetched for correctness but never leaked to the
parent.

The focused regressions are
`topn::tests::rank_topn_stops_after_the_boundary_prefix_group`, which observes
that a chunk containing the first later prefix is fetched but not retained,
and `topn::tests::rank_topn_unspecified_prefix_uses_exact_value_equality`,
which keeps case-distinct values separate on Go's complete-value (`-1`)
path. Before the production branch was restored, the short-circuit test
failed with `left: 8, right: 6`, proving the old executor drained all rows;
after the fix both tests pass. The existing `index_prefix_cut.rs` source and
its Unicode/binary tests were inventoried as the shared prefix-key owner; no
new fixture or generated/platform artifact was needed.

Rust ownership remains within `topn.rs` and
`driver/physical_builder.rs`; no Go, Bazel, generated, fixture, or platform
artifact changed. Full planner partial-order candidate generation and live
TiKV prefix-index scans remain explicit follow-up boundaries because the
current Rust `CopTask` still does not carry Go's
`PartialOrderMatchResult`; this batch only activates already-materialized
`PhysicalTopN` prefix metadata and does not invent planner state.

## RankTopN prefix-group short-circuit Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::tests::rank_topn_stops_after_the_boundary_prefix_group -- --exact --nocapture` failed with `left: 8, right: 6` after temporarily disabling the missing rank branch.
- Post-fix focused command passed for both rank regressions with the temporary host-only vendored-OpenSSL feature toggle; the Cargo manifest was reverted immediately to `openssl = "0.10"`.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The complete `tidb-executor` library suite, planner partial-order candidate
enumeration, live prefix-index execution, spill interaction for RankTopN, and
native TiKV behavior were not run. The default Rust 1.95 toolchain remains
below the workspace's Rust 1.97 minimum; the pinned nightly toolchain was used
for the Ready checks.

## TopN zero-count short-circuit alignment

The same complete `pkg/executor/sortexec` inventory covers this fifteenth
Rust-only fix. Go's `TopNExec.fetchChunks` closes its result channel before
touching the child whenever `Limit.Count == 0`; this is true even when
`Limit.Offset` is nonzero because the planner replaces the operator with a
dual. Rust previously tested only `offset + count == 0`, so an offset-only
request drained and accounted child rows before returning an empty result.
`TopNExec` now retains the effective count and short-circuits on the count
itself, preserving Go's no-read behavior without changing overflow clamping.

The focused regression is
`topn::tests::a_zero_count_returns_nothing_without_draining_the_child`. Before
the production change it failed with `left: 5, right: 0`, proving that the
child had been consumed for `OFFSET 7 LIMIT 0`; after the change it passes and
observes zero emitted child rows. Rust ownership remains within `topn.rs`; no
Go, Bazel, generated, fixture, or platform artifact changed.

## TopN zero-count short-circuit Ready validation

- Pre-fix `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --lib topn::tests::a_zero_count_returns_nothing_without_draining_the_child -- --nocapture` failed with `LIMIT 0 must not fetch rows even with a nonzero OFFSET` (`left: 5, right: 0`).
- The same focused command passed after the fix with a temporary host-only vendored-OpenSSL feature toggle; the manifest was reverted immediately to `openssl = "0.10"`.
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-executor --all-targets` passed with the same temporary vendored-OpenSSL toggle; the manifest was reverted immediately after the run.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint`

The complete `tidb-executor` library suite, planner-generated dual plans,
live SQL execution with offset-only limits, spill interaction, and native
TiKV behavior were not run. The default Rust 1.95 toolchain remains below the
workspace's Rust 1.97 minimum; the pinned nightly toolchain was used for the
focused validation.
