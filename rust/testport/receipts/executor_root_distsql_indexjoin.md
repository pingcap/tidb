# `pkg/executor` Go-master bounded parity receipt

Status: Ready for the seven focused executor behavior clusters in this batch. The receipt records the complete root-package inventory at Go `origin/master` `a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (pulled 2026-09-02) and the complete nested `pkg/executor/sortexec` inventory at Go `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (pulled 2026-09-04); it is not a complete transcreation claim for the 101,740-line executor package.

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
