// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/executor/executor_pkg_test.go`: the statement-context reset
//! policy (`ResetContextOfStmt`, `pkg/executor/select.go:923`) and the
//! executor-internal helpers the file unit-tests.
//!
//! The Go test `TestErrLevelsForResetStmtContext` (pkg/executor/executor_pkg_test.go:364)
//! asserts the SEVEN-GROUP `errctx.LevelMap` each statement class receives;
//! this tier models the Truncate, BadNull and DividedByZero groups (see
//! `crate::stmt_context`), so the running tests below pin exactly those
//! groups for every Go case and the unmodeled groups are tracked in the
//! `#[ignore]` tests beside them.

use tidb_expr::Columns;

use crate::stmt_context::{StatementClass, StmtContext};

/// Go `mysql.ModeStrictAllTables | mysql.ModeErrorForDivisionByZero`
/// (pkg/executor/executor_pkg_test.go:395): STRICT_ALL_TABLES plus ERROR_FOR_DIVISION_BY_ZERO.
const STRICT_MODE: bool = true;
const ERROR_FOR_DIVISION_BY_ZERO: bool = true;

/// How the modeled `ErrGroupBadNull` level observes. Go's group drives the
/// NOT NULL check AND `SLEEP`'s argument validation
/// (`pkg/executor/stmtctx`-owned `HandleSleepIncorrectArgument`), and the
/// Rust tier consumes the same stored level in
/// `tidb_expr::Columns::handle_sleep_incorrect_argument`, so the probe maps
/// `Err` -> LevelError, `Ok` + warning -> LevelWarn, `Ok` alone ->
/// LevelIgnore.
fn assert_bad_null_level(context: &StmtContext, expected: tidb_expr::ErrorLevel) {
    let observed = match context.handle_sleep_incorrect_argument() {
        Err(_) => tidb_expr::ErrorLevel::Error,
        Ok(()) => {
            let warned = !context.take_warnings().is_empty();
            if warned {
                tidb_expr::ErrorLevel::Warn
            } else {
                tidb_expr::ErrorLevel::Ignore
            }
        }
    };
    assert_eq!(observed, expected, "ErrGroupBadNull level");
}

/// Builds the context Go's `ResetContextOfStmt` builds for an
/// INSERT/UPDATE/DELETE under the given mode bits and IGNORE modifier
/// (`pkg/executor/select.go:1009-1030`, `ResetUpdateStmtCtx`
/// select.go, `ResetDeleteStmtCtx`).
fn dml_context(
    class: StatementClass,
    strict: bool,
    ignore: bool,
    error_for_division_by_zero: bool,
    single_insert: bool,
    enable_strict_not_null_check: bool,
) -> StmtContext {
    let context =
        crate::stmt_context::StmtContext::for_dml(error_for_division_by_zero, strict, ignore)
            .with_statement_class(class);
    // The single-row bad-NULL promotion exists only for INSERT
    // (`pkg/executor/select.go`'s `*ast.InsertStmt` arm); UPDATE/DELETE take
    // the plain `!strictSQLMode || ignoreErr` rule (`ResetUpdateStmtCtx`).
    if class == StatementClass::Insert {
        context.with_single_insert_bad_null_policy(single_insert, enable_strict_not_null_check)
    } else {
        context
    }
}

/// Go `pkg/executor/executor_pkg_test.go:364::TestErrLevelsForResetStmtContext` -- the
/// groups this tier models: `ErrGroupTruncate`, `ErrGroupBadNull` and
/// `ErrGroupDividedByZero`, across the Go table's statement classes, modes
/// and IGNORE modifiers.
///
/// Derived from `pkg/executor/select.go:923` `ResetContextOfStmt`: the switch
/// arms for INSERT/UPDATE/DELETE resolve truncate `WithTruncateAsWarning(
/// !strictSQLMode || ignoreErr)` and divided-by-zero
/// `ResolveErrLevel(!hasErrorForDivisionByZero, !strictSQLMode || ignoreErr)`
/// (`pkg/errctx/context.go` `ResolveErrLevel`); the SELECT/SetOpr arm pins
/// truncate-as-warning with no mode input, and the base map's divided-by-zero
/// default is `LevelWarn` (`pkg/sessionctx/stmtctx/stmtctx.go:561`
/// `DefaultStmtErrLevels`).
#[test]
fn err_levels_for_reset_stmt_context_modeled_groups() {
    // (name, build, expected truncate, expected bad null, expected divided by zero)
    enum Stmt {
        Insert,
        Update,
        Delete,
        Select,
    }
    struct Case {
        name: &'static str,
        stmt: Stmt,
        strict: bool,
        ignore: bool,
        error_for_division_by_zero: bool,
        single_insert: bool,
        expected_truncate: tidb_expr::ErrorLevel,
        expected_bad_null: tidb_expr::ErrorLevel,
        expected_divided_by_zero: tidb_expr::ErrorLevel,
    }
    let cases = [
        // "strict,write" (pkg/executor/executor_pkg_test.go:394-412)
        Case {
            name: "strict,write",
            stmt: Stmt::Insert,
            strict: STRICT_MODE,
            ignore: false,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Error,
            expected_bad_null: tidb_expr::ErrorLevel::Error,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Error,
        },
        // "non-strict,write" (pkg/executor/executor_pkg_test.go:413-430)
        Case {
            name: "non-strict,write",
            stmt: Stmt::Insert,
            strict: false,
            ignore: false,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Warn,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
        // "strict,insert ignore" (pkg/executor/executor_pkg_test.go:431-448)
        Case {
            name: "strict,insert ignore",
            stmt: Stmt::Insert,
            strict: STRICT_MODE,
            ignore: true,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Warn,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
        // "strict,update ignore" (pkg/executor/executor_pkg_test.go:449-466)
        Case {
            name: "strict,update ignore",
            stmt: Stmt::Update,
            strict: STRICT_MODE,
            ignore: true,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Warn,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
        // "strict,delete ignore" (pkg/executor/executor_pkg_test.go:467-484)
        Case {
            name: "strict,delete ignore",
            stmt: Stmt::Delete,
            strict: STRICT_MODE,
            ignore: true,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Warn,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
        // "strict without error_for_division_by_zero,write"
        // (pkg/executor/executor_pkg_test.go:485-502): the sql_mode here is
        // `ModeStrictAllTables` WITHOUT `ModeErrorForDivisionByZero`.
        Case {
            name: "strict without error_for_division_by_zero,write",
            stmt: Stmt::Insert,
            strict: STRICT_MODE,
            ignore: false,
            error_for_division_by_zero: false,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Error,
            expected_bad_null: tidb_expr::ErrorLevel::Error,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Ignore,
        },
        // "strict,select/union" (pkg/executor/executor_pkg_test.go:503-520): the SELECT
        // arm ignores the mode entirely.
        Case {
            name: "strict,select/union",
            stmt: Stmt::Select,
            strict: STRICT_MODE,
            ignore: false,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Error,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
        // "non-strict,select/union" (pkg/executor/executor_pkg_test.go:521-538): Go's own
        // table repeats the strict expectation verbatim, because a read's
        // levels have no mode input.
        Case {
            name: "non-strict,select/union",
            stmt: Stmt::Select,
            strict: false,
            ignore: false,
            error_for_division_by_zero: ERROR_FOR_DIVISION_BY_ZERO,
            single_insert: false,
            expected_truncate: tidb_expr::ErrorLevel::Warn,
            expected_bad_null: tidb_expr::ErrorLevel::Error,
            expected_divided_by_zero: tidb_expr::ErrorLevel::Warn,
        },
    ];
    for case in &cases {
        let context = match &case.stmt {
            Stmt::Select => StmtContext::for_query().with_statement_class(StatementClass::Select),
            Stmt::Insert => dml_context(
                StatementClass::Insert,
                case.strict,
                case.ignore,
                case.error_for_division_by_zero,
                case.single_insert,
                true,
            ),
            Stmt::Update => dml_context(
                StatementClass::UpdateOrDelete,
                case.strict,
                case.ignore,
                case.error_for_division_by_zero,
                false,
                true,
            ),
            Stmt::Delete => dml_context(
                StatementClass::UpdateOrDelete,
                case.strict,
                case.ignore,
                case.error_for_division_by_zero,
                false,
                true,
            ),
        };
        // Drain the warning buffer before probing; the bad-null probe
        // appends a warning exactly when the level is Warn. The drained
        // entries are deliberately dropped (`let _`), only the probe's own
        // appended warning is inspected afterwards.
        let _ = context.take_warnings();
        assert_eq!(
            context.truncate_level(),
            case.expected_truncate,
            "{}: ErrGroupTruncate level",
            case.name,
        );
        assert_bad_null_level(&context, case.expected_bad_null);
        assert_eq!(
            context.division_by_zero_level(),
            case.expected_divided_by_zero,
            "{}: ErrGroupDividedByZero level",
            case.name,
        );
    }
}

/// Go `pkg/executor/executor_pkg_test.go:364::TestErrLevelsForResetStmtContext` -- the
/// groups this tier does NOT model.
// go-parity-gap: the Rust `StmtContext` carries no ErrGroupDupKey,
// ErrGroupNoDefault, ErrGroupAutoIncReadFailed or ErrGroupNoMatchedPartition
// level (`ResetUpdateStmtCtx`/`ResetDeleteStmtCtx` set the first four;
// `*ast.LoadDataStmt` sets NoMatchedPartition = Warn), and there is no LOAD
// DATA constructor, so Go's full seven-group `LevelMap()` assertions for
// those groups and the two load_data cases cannot be pinned.
#[test]
#[ignore]
fn err_levels_full_level_map_parity() {}

/// Go `pkg/executor/executor_pkg_test.go:648::TestStrictNotNullCheckForInsert` -- the
/// `ErrGroupBadNull` column: the single-row INSERT promotion
/// `(strictSQLMode || isSingleInsert) && EnableStrictNotNullCheck && !ignoreErr`
/// (`pkg/executor/select.go`'s `*ast.InsertStmt` arm, ported as
/// `StmtContext::with_single_insert_bad_null_policy`).
#[test]
fn strict_not_null_check_for_insert_bad_null_level() {
    struct Case {
        name: &'static str,
        strict: bool,
        enable_strict_not_null_check: bool,
        is_single_insert: bool,
        expect_bad_null_level: tidb_expr::ErrorLevel,
    }
    let cases = [
        // pkg/executor/executor_pkg_test.go:683-690
        Case {
            name: "non-strict,single-row,disable",
            strict: false,
            enable_strict_not_null_check: false,
            is_single_insert: true,
            expect_bad_null_level: tidb_expr::ErrorLevel::Warn,
        },
        // pkg/executor/executor_pkg_test.go:691-698
        Case {
            name: "strict,single-row,disable",
            strict: STRICT_MODE,
            enable_strict_not_null_check: false,
            is_single_insert: true,
            expect_bad_null_level: tidb_expr::ErrorLevel::Warn,
        },
        // pkg/executor/executor_pkg_test.go:699-706
        Case {
            name: "non-strict,single-row,enable",
            strict: false,
            enable_strict_not_null_check: true,
            is_single_insert: true,
            expect_bad_null_level: tidb_expr::ErrorLevel::Error,
        },
        // pkg/executor/executor_pkg_test.go:707-714
        Case {
            name: "strict,single-row,enable",
            strict: STRICT_MODE,
            enable_strict_not_null_check: true,
            is_single_insert: true,
            expect_bad_null_level: tidb_expr::ErrorLevel::Error,
        },
        // pkg/executor/executor_pkg_test.go:715-722: a MULTI-row insert never promotes.
        Case {
            name: "non-strict,multi-row,disable",
            strict: false,
            enable_strict_not_null_check: false,
            is_single_insert: false,
            expect_bad_null_level: tidb_expr::ErrorLevel::Warn,
        },
        // pkg/executor/executor_pkg_test.go:723-730
        Case {
            name: "strict,multi-row,disable",
            strict: STRICT_MODE,
            enable_strict_not_null_check: false,
            is_single_insert: false,
            expect_bad_null_level: tidb_expr::ErrorLevel::Warn,
        },
        // pkg/executor/executor_pkg_test.go:731-738
        Case {
            name: "non-strict,multi-row,enable",
            strict: false,
            enable_strict_not_null_check: true,
            is_single_insert: false,
            expect_bad_null_level: tidb_expr::ErrorLevel::Warn,
        },
        // pkg/executor/executor_pkg_test.go:739-746
        Case {
            name: "strict,multi-row,enable",
            strict: STRICT_MODE,
            enable_strict_not_null_check: true,
            is_single_insert: false,
            expect_bad_null_level: tidb_expr::ErrorLevel::Error,
        },
    ];
    for case in &cases {
        let context = dml_context(
            StatementClass::Insert,
            case.strict,
            false,
            ERROR_FOR_DIVISION_BY_ZERO,
            case.is_single_insert,
            case.enable_strict_not_null_check,
        );
        let _ = context.take_warnings();
        assert_bad_null_level(&context, case.expect_bad_null_level);
    }
}

/// Go `pkg/executor/executor_pkg_test.go:648::TestStrictNotNullCheckForInsert` -- the
/// `ErrGroupNoDefault` column.
// go-parity-gap: `ErrGroupNoDefault`'s level (`ResolveErrLevel(false,
// !strictSQLMode || stmt.IgnoreErr)` in the INSERT arm) is not modeled on the
// Rust `StmtContext`, so the test's second assertion column has no surface to
// pin.
#[test]
#[ignore]
fn strict_not_null_check_for_insert_no_default_level() {}

/// Go `pkg/executor/executor_pkg_test.go:56::TestFillEmbedTextValues`.
// go-parity-gap: `InsertValues.fillEmbedTextValues`/`getEmbedTextGeneratedCols`
// (`pkg/executor/insert.go`) need the vector type (`TypeTiDBVectorFloat32`),
// the `embed_text` builtin plus the starter/premium deployment-mode and
// inference `EmbedFn` plumbing; none are ported in this tier.
#[test]
#[ignore]
fn fill_embed_text_values_caches_vector_generated_columns() {}

/// Go `pkg/executor/executor_pkg_test.go:190::TestBuildKvRangesForIndexJoinWithoutCwc`.
// go-parity-gap: Go's `buildKvRangesForIndexJoin` (`pkg/executor/join/
// index_join_builder.go`-owned helper) substitutes join-key datums into the
// index ranges via `keyOff2IdxOff` and encodes ordered `KeyRange`s; this tier
// re-seeds `IndexJoinProbes` on a live source (`crate::access_path`) instead
// of exposing the range-substitution function, so the ordering assertion has
// no callable seam.
#[test]
#[ignore]
fn build_kv_ranges_for_index_join_without_cwc_orders_substituted_ranges() {}

/// Go `pkg/executor/executor_pkg_test.go:219::
/// TestBuildKvRangesForIndexJoinWithoutCwcAndWithMemoryTracker`.
// go-parity-gap: the same missing `buildKvRangesForIndexJoin` seam, plus the
// exact byte accounting the Go test pins (10 probe keys consume 23640 bytes
// and 20 keys exactly double it through the index-worker memory tracker);
// this tier's probe path performs no equivalent tracker accounting.
#[test]
#[ignore]
fn build_kv_ranges_for_index_join_tracks_probe_bytes_linearly() {}

/// Go `pkg/executor/executor_pkg_test.go:276::
/// TestIndexReaderPartitionRangesUseMemoryTracker`.
// go-parity-gap: Go opens an `IndexReaderExecutor` over pruned partitions and
// asserts the range mem tracker consumed the encoded partition key ranges
// (`pkg/executor/index_reader.go`); this tier's index reads have no
// partition-range construction with tracker accounting to build.
#[test]
#[ignore]
fn index_reader_partition_ranges_use_memory_tracker() {}

/// Go `pkg/executor/executor_pkg_test.go:296::
/// TestIndexLookUpPartitionRangesUseMemoryTracker`.
// go-parity-gap: same missing seam for `IndexLookUpExecutor.buildTableKeyRanges`
// (`pkg/executor/index_lookup.go`): partition key-range building plus the
// range/executor tracker split has no ported counterpart.
#[test]
#[ignore]
fn index_lookup_partition_ranges_use_memory_tracker() {}

/// Go `pkg/executor/executor_pkg_test.go:335::TestSlowQueryRuntimeStats`.
// go-parity-gap: `slowQueryRuntimeStats.String/Clone/Merge`
// (`pkg/executor/slow_query.go`) -- the slow-log reader's runtime statistics
// with the exact "initialize: 1ms, read_file: 1s, parse_log: {time:100ms,
// concurrency:15}, ..." rendering -- is unported.
#[test]
#[ignore]
fn slow_query_runtime_stats_render_merge_and_clone() {}

/// Go `pkg/executor/executor_pkg_test.go:351::TestFilterTemporaryTableKeys`.
// go-parity-gap: `filterTemporaryTableKeys` (`pkg/executor/point_get.go`)
// drops txn keys belonging to session temporary tables recorded in
// `TxnCtx.TemporaryTables`; the Rust tier has no temporary-table txn-key
// registry to filter.
#[test]
#[ignore]
fn filter_temporary_table_keys_drops_registered_table_prefixes() {}

/// Go `pkg/executor/executor_pkg_test.go:539::
/// TestAddUnchangedKeysForLockByRow_GlobalIndexNewTableID`.
// go-parity-gap: `addUnchangedKeysForLockByRow` on a partitioned table with a
// GLOBAL index during `ALTER TABLE ... ` DDL (`pi.NewTableID` +
// `pi.DDLChangedIndex` switching the locked index key to the new table ID,
// `pkg/executor/executor.go`) needs the pessimistic unchanged-key lock path,
// partition-by-row resolution and global-index key generation -- unported.
#[test]
#[ignore]
fn add_unchanged_keys_for_lock_by_row_uses_global_index_new_table_id() {}
