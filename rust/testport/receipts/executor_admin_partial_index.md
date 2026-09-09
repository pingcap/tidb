# `pkg/executor` ADMIN CHECK partial-index accounting

## Go authority

`pkg/executor/check_table_index.go:141-169` rejects a partial index from the
ordinary checker unless `tidb_enable_fast_table_check` is enabled, while the
fast checker and `pkg/executor/admin.go:395-402` evaluate the partial predicate
and skip false/NULL rows during recovery. The stored index therefore represents
the predicate-filtered relation, not the full table row count.

## Rust change

`admin_check::check_table` now derives the expected row set per index by using
the compiled `KvTable` predicate. Count mismatches use the filtered count;
ROW-to-INDEX checks only require entries for matching rows; and INDEX-to-ROW
checks reject stale entries for rows that no longer satisfy the predicate.
The existing Rust API has one consistency-check path rather than Go's fast
check session toggle, so this ports the predicate/accounting behavior without
inventing a second session variable.

## Focused regression

`tests_admin_check_admintest_source::admin_check_counts_only_rows_matching_partial_index_predicate`
creates three rows (true, false, and NULL), verifies that only one partial
entry exists logically, and passes both `ADMIN CHECK TABLE` and named-index
checks.

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib admin_check_counts_only_rows_matching_partial_index_predicate -- --nocapture
# 1 passed; 0 failed
```

No Go, generated, platform, Bazel, or build-artifact file changed. The
fast-check session-toggle/error-8273 surface remains an explicit boundary.
