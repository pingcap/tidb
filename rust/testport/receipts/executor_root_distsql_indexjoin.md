# `pkg/executor` Go-master bounded parity receipt

Status: Ready for the two focused executor behavior clusters in this batch. The receipt records the complete root-package inventory at Go `origin/master` `a74cc596996d8a4c940b4d64fca46ac1c6d5c0d7` (pulled 2026-09-02); it is not a complete transcreation claim for the 101,740-line executor package.

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
