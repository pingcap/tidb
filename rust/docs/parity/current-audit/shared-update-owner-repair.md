# Shared UPDATE owner repair, 2026-09-30

This maintains the existing integrated executor/session paths against TiDB
master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`, starting from integration
`05d116eed80d2d5aa35244ab5def03b4c93a0811`. Both refs were fetched before
editing; the integration pull was already current. This is not acceptance of
the complete Go executor/session packages or a repository-wide parity claim.

## Removed ownership and replacement

`driver/multi_dml.rs::write_row`, the ordinary UPDATE IGNORE/normal write
loops, `InsertUndo`/`apply_insert_undo`, and UPDATE's row-preimage replay loop
are removed. `driver/dml/update_record.rs::UpdateRecords` owns record writes
for ordinary UPDATE, joined UPDATE and ON DUPLICATE KEY UPDATE. It shares
unchanged-row locking, generation/null/partition validation, duplicate and
IGNORE outcomes, and FK checks/cascades. Expression preparation remains with
the appropriate statement (ODKU has distinct assignment evaluation rules).

The source contracts are `pkg/executor/write.go::updateRecord`,
`pkg/executor/update.go::{prepare,mergeNonGenerated,mergeGenerated,exec}`,
`pkg/executor/insert.go::updateDupRow` and
`pkg/executor/adapter.go::handleForeignKeyTrigger` at the master above.
The alias merge is separate from update-once tracking: aliases merge by base
table/row identity while each target position retains its own changed flag.
Generated values are merged around the record operation, preserving index
preimages. ODKU checks its actual update result instead of a discarded insert
candidate, and an ignored duplicate cannot undo earlier accepted rows.

Ordinary UPDATE FK checks see the completed statement writes and run before
cascades; IGNORE checks candidates before writing. Unchanged child FK keys
are not rechecked by ordinary updates. FK participation is resolved once per
written table, rather than rescanning catalog relationships for every row.
Known complete old rows are reused by the table writer; a projection omitting
hidden index columns lets the writer read the complete old image.

Session catalog staging and the cluster mutation-buffer checkpoint retain
statement rollback ownership. EXPLAIN ANALYZE DML and internal user/binding/
bootstrap SQL writes now use that session owner as well. Auto-id allocator
state remains outside the row rollback, as existing tests require.

## Regression evidence

On unchanged production HEAD, the seven `shared_update_record` session tests
had six failures and one passing control. Failures demonstrate alias column
loss (including generated values/indexes), child FK bypass, missing parent
cascade/restriction, ODKU checking the wrong row and ODKU IGNORE rejecting a
later duplicate. The final-parent-rows control guards against introducing
premature checks while fixing the bypass. The new EXPLAIN ANALYZE regression
also failed on unchanged HEAD: row 1 moved to 11 despite a later duplicate.
All eight now pass. A new embedded cluster regression verifies alias results
and exact mutation-buffer restoration after a later duplicate.

Baseline tests temporarily used the three unchanged production files from
`git show HEAD:<path>`; a Python `finally` restored the complete working bytes.
No test expectations were changed or suppressed to obtain passing results.

## Validation commands

Commands run from `rust/` unless noted. The targeted filters cover the shared
SQL entrypoints, FK operations, generated/default columns, rollback and the
session callers migrated off the executor undo logs.

    cargo test --locked -p tidb-session --lib shared_update_record
    cargo test --locked -p tidb-session --lib tests_statement_rollback
    cargo test --locked -p tidb-session --lib tests_foreign_key
    cargo test --locked -p tidb-session --lib tests_generated_columns
    cargo test --locked -p tidb-session --lib bootstrap::tests
    cargo test --locked -p tidb-session --lib tests_extra_handle_access
    cargo test --locked -p tidb-session --lib tests_binding
    cargo test --locked -p tidb-session --lib tests_grants
    cargo test --locked -p tidb-session --lib tests_multi_table_dml
    cargo test --locked -p tidb-session --lib tests_core::dml
    cargo test --locked -p tidb-executor --lib driver::tests::dml
    cargo test --locked -p tidb-executor --lib driver::tests::column_defaults
    cargo test --locked -p tidb-executor --lib tests_insert_on_duplicate_key_source
    cargo test --locked -p tidb-server --lib shared_update_record_uses_cluster_statement_buffer
    cargo test --locked -p tidb-server --lib a_failed_statement_leaves_no_bytes_of_its_own_in_the_mutation_buffer
    cargo test --locked -p tidb-server --lib unchanged_updates_lock_only_matched_rows
    cargo check --locked -p tidb-executor -p tidb-session -p tidb-server --all-targets

Root validation/publication gates:

    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "executor: share update record policy and statement rollback"
    cd rust && cargo build --locked -p tidb-server
    git push origin HEAD:hparser-integration

The first sandboxed lint attempt could not resolve proxy.golang.org; the
network-enabled retry passed. Formatting uses rustfmt edition 2021 with
`skip_children=true` on changed Rust files, retaining only formatting changes
intersecting edited ranges to avoid unrelated churn.

Four DML/default failures were independently reproduced on unchanged production HEAD:
`a_syntax_error_carries_gos_sentence_and_position`,
`an_assignment_cast_reports_cast_values_own_error`,
`named_and_nested_defaults_use_the_referenced_column`, and
`on_duplicate_defaults_cover_conflict_and_no_conflict`. The last two are parser
errors in named DEFAULT expressions; the cast failure differs in error text.
The wider account suite has the same 31 failures on unchanged HEAD and this
change (73 passing); failure-name set comparison found no introduced or
removed failures. These include existing SHOW GRANTS rendering and privilege
expectations. Together with the four DML/default failures, 35 baseline
failures remain; this receipt does not report those suites as fully passing.

Passing targeted groups: shared update 7, statement rollback 8, foreign keys
57, generated columns 25, binding 20, bootstrap 1, executor DML 12 and ODKU
source 1. The three scoped embedded tests and all-target check passed.

Zero-test exploratory filters `tests_bindings` and `tests_account` are not
counted as validation; their actual suites are `tests_binding`/`tests_grants`.

Local logs use `/private/tmp/tidb-shared-update-*.log` (red, baseline, defaults-
baseline, validation, cluster, native-rollback, locks, all-targets, binding,
grants, grants-baseline, lint, commit, prepush-build).

## Remaining scope and risks

E01's reproduced alias lost-write path is repaired. E02's runtime bypass is
removed, but joined DML still builds empty FK plan metadata: the plan-driven
FK operator integration is open. Existing catalog FK scans/cascade functions
are reused and are not a complete port of Go's planned FK executors. E03's
materialized source/identity handoff, including partition identity edge cases,
and all other open findings remain. None is hidden by this repair receipt.

Record policy and FK timing change correctness-sensitive paths; the local
regressions cover both in-process catalog staging and embedded cluster buffer
staging. No real TiKV/TiFlash deployment, concurrent FK race suite, complete
upstream package test inventory, or sysbench/TPCC/TPCH/YCSB run was performed.
No throughput improvement or complete package parity is claimed.
