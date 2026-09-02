# `pkg/ddl` automatic index pre-split parity receipt

Comparison source: Go `master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete package inventory

The direct Go package inventory contains 139 artifacts and 89,091 Go/BUILD
lines: 63 production files, 74 test files, `BUILD.bazel`, and `OWNERS`.
`doc.go` was read first, before implementation files, as required by the
package workflow. There are no fixture directories, generated Go outputs,
platform-specific variants, or standalone build artifacts in this direct
package. The 1,016 top-level `Test*`/`Benchmark*` declarations were counted
from the complete test inventory; nested test helpers are included in the
file-by-file read but not double-counted.

The complete direct artifact list (sorted, with the production/test boundary
shown by suffix) is:

```text
BUILD.bazel OWNERS doc.go
add_column.go affinity.go backfill_metrics.go backfilling.go backfilling_clean_s3.go
backfilling_dist_executor.go backfilling_dist_scheduler.go backfilling_import_cloud.go
backfilling_merge_sort.go backfilling_merge_temp.go backfilling_operators.go
backfilling_read_index.go backfilling_txn_executor.go cluster.go column.go constraint.go
create_table.go ddl.go ddl_algorithm.go ddl_history.go ddl_running_jobs.go
ddl_tiflash_api.go ddl_workerpool.go delete_range.go delete_range_util.go dist_owner.go
engine_attribute.go executor.go foreign_key.go generated_column.go index.go
index_auto_presplit.go index_cop.go index_merge_tmp.go index_presplit.go
job_scheduler.go job_submitter.go job_worker.go masking_policy.go metabuild.go
modify_column.go multi_schema_change.go mock.go options.go owner_mgr.go partition.go
placement_policy.go reorg.go reorg_util.go rollingback.go sanity_check.go schema.go
schema_version.go sequence.go split_region.go stat.go storage_class.go table.go
table_lock.go table_mode.go ttl.go
affinity_test.go attributes_sql_test.go backfill_metrics_test.go
backfilling_dist_scheduler_test.go backfilling_test.go backfilling_txn_executor_test.go
bench_test.go cancel_test.go cluster_test.go column_change_test.go column_modify_test.go
column_test.go column_type_change_test.go constraint_test.go db_cache_test.go
db_change_failpoints_test.go db_change_test.go db_integration_test.go db_rename_test.go
db_table_test.go db_test.go ddl_algorithm_test.go ddl_error_test.go ddl_history_test.go
ddl_running_jobs_test.go ddl_test.go ddl_workerpool_test.go executor_nokit_test.go
executor_test.go external_workload_ttl_test.go fail_test.go foreign_key_test.go
index_auto_presplit_test.go index_change_test.go index_cop_test.go index_modify_test.go
index_nokit_test.go integration_test.go job_scheduler_test.go job_scheduler_testkit_test.go
job_submitter_test.go job_worker_test.go main_test.go masking_policy_internal_test.go
masking_policy_test.go metabuild_test.go modify_column_test.go multi_schema_change_test.go
mv_index_test.go options_test.go owner_mgr_test.go partition_test.go
placement_policy_ddl_test.go placement_policy_test.go placement_sql_test.go
primary_key_handle_test.go reorg_test.go reorg_util_test.go repair_table_test.go
restart_test.go rollingback_test.go schema_test.go schema_version_test.go sequence_test.go
stat_test.go storage_class_partition_test.go storage_class_test.go table_mode_test.go
table_modify_test.go table_split_test.go table_test.go tiflash_replica_test.go ttl_test.go
```

The list above is the direct root-package boundary. Go's nested packages under
`pkg/ddl` (for example `ingest`, `jobsubmit`, `placement`, `schemaver`,
`serverstate`, `session`, and `util`) are separate package claims and were not
silently folded into this receipt.

## Go-master delta and Rust implementation

Go commit `c6054025ed4c32ab3672a2a24ea46892714d21ec` adds automatic index
pre-splitting and carries the option through parser AST, DDL job arguments,
multi-schema planning, and the index-region split worker. The Rust batch adds
the same durable marker and its dependency-closed planning half:

- `tidb-ast` accepts `PRE_SPLIT_REGIONS = AUTO`, restores the gated special
  comment, and keeps manual `SplitOpt` precedence.
- `tidb-model::IndexArg` persists `auto_presplit` as a field independent of
  `split_opt`, preserving rolling-upgrade JSON shape.
- The lexer/feature catalogs recognize the unreserved `AUTO` token and the
  `auto_presplit` feature gate.
- `tidb-exec::cluster_ddl` carries the marker through CREATE INDEX, ALTER
  TABLE ADD INDEX, grouped constraints, and the durable `DdlWrite`.
- `tidb-exec::auto_pre_split` reproduces Go's non-partitioned/partial/prefix
  gates, health and row-count thresholds, Analyze V2/null-count validation,
  TopN and histogram weighted-value merge, internal quantile sampling,
  collation-byte preservation, index boundary-key construction, sorting, and
  deduplication. The real-TiKV PD split callback remains an explicit boundary:
  this checkout has no dependency-closed `SplittableStore` owner in the DDL
  transaction path, so it is not replaced with speculative PD behavior.

No Rust-only automatic-pre-split behavior was found to remove. Existing
manual `PRE_SPLIT_REGIONS` behavior remains separate and takes precedence.

The focused Rust planner helpers now construct the source-shaped integer
`FieldType` explicitly with `FieldType::new(FieldTypeCode::LongLong)`. The
datatype owner intentionally has no `Default` implementation, so the prior
test-only `FieldType::default()` calls prevented the `tidb-exec` library (and
unrelated owner tests) from compiling. This is a compile-correctness repair;
planner behavior and production code are unchanged.

## Focused regression matrix

| Go behavior | Rust evidence | Status |
| --- | --- | --- |
| AUTO parser acceptance/restoration and manual precedence | `tidb-parser::tests::go_parser_auto_pre_split_index_rows_match`, `restore_context::go_ast_test_auto_presplit_index_special_comment` | Direct |
| Separate `auto_presplit` job-argument JSON field | `tidb-model::job_args_tests::go_test_auto_pre_split_index_arg_json_is_separate_from_manual_split` | Direct |
| Marker reaches CREATE INDEX catalog write | `tidb-exec::tests::cluster_ddl_source::create_index_auto_pre_split_marker_reaches_catalog_write` | Direct |
| TopN + histogram merge, threshold sampling, and key boundaries | `tidb-exec::tests::auto_pre_split_source::auto_pre_split_merges_topn_and_histogram_before_sampling_keys` | Direct |
| Analyze V1 skip and negative null-count rejection | `tidb-exec::tests::auto_pre_split_source::auto_pre_split_skips_unreliable_or_unsafe_statistics` | Direct |
| Full Go worker/PD split and failpoint lifecycle | No dependency-closed Rust owner in this branch | Explicit boundary |

## Validation

Profile: **Ready** for this package batch. The changes are Rust/parser/model
source changes, so Rust formatting, focused source tests, package compilation,
`make lint`, and `git diff --check` are required. `make bazel_prepare` is not
required because no Go/Bazel file was changed in this batch.

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS

env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
      -p tidb-exec --test all auto_pre_split -- --nocapture
# PASS: 3 tests

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-parser --lib --no-default-features go_parser_auto_pre_split_index_rows_match -- --nocapture
# PASS

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-model --lib go_test_auto_pre_split_index_arg_json_is_separate_from_manual_split -- --nocapture
# PASS

env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec -q
# PASS (existing warnings only)

env OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
      -p tidb-exec --lib cluster_auto_id -- --test-threads=1
# PASS: 8 tests; this owner compile gate also covers auto_pre_split unit helpers

make lint
# PASS

git diff --check
# PASS
```

Not verified locally: Bazel analysis (the executable is unavailable), live PD
region splitting, full Go `pkg/ddl` integration tests, Windows builds, and
full-workspace tests. Correctness risk is concentrated at the explicit PD
split integration boundary; the planner itself is pure and covered by focused
TopN/histogram, gate, encoding, parser, model, and catalog regressions.
