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

//! Source-backed tests for statement push-down flag synthesis.

use tidb_datatype::{ConversionFlags, STRICT_FLAGS};
use tidb_exec::error_context::{ErrGroup, Level, LevelMap};
use tidb_exec::statement_pushdown::{
    init_from_pb_flags, push_down_flags, push_down_flags_with_type_flags_and_err_levels,
    PushDownFlagsInput, StatementKind, FLAG_DIVIDED_BY_ZERO_AS_WARNING, FLAG_IGNORE_TRUNCATE,
    FLAG_IGNORE_ZERO_IN_DATE, FLAG_IN_INSERT_STMT, FLAG_IN_LOAD_DATA_STMT, FLAG_IN_RESTRICTED_SQL,
    FLAG_IN_SELECT_STMT, FLAG_IN_UPDATE_OR_DELETE_STMT, FLAG_OVERFLOW_AS_WARNING,
    FLAG_TRUNCATE_AS_WARNING,
};

#[test]
fn statement_context_pushdown_cases_match_source_table() {
    // Source: pkg/sessionctx/stmtctx/stmtctx_test.go:89-132 and
    // pkg/sessionctx/stmtctx/stmtctx.go:1251-1268.
    let source_cases = [
        (
            PushDownFlagsInput {
                statement_kind: StatementKind::Insert,
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_INSERT_STMT,
        ),
        (
            PushDownFlagsInput {
                statement_kind: StatementKind::UpdateOrDelete,
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_UPDATE_OR_DELETE_STMT,
        ),
        (
            PushDownFlagsInput {
                statement_kind: StatementKind::Select,
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_SELECT_STMT,
        ),
        (
            PushDownFlagsInput {
                type_flags: STRICT_FLAGS.with_ignore_truncate_err(true),
                ..PushDownFlagsInput::default()
            },
            FLAG_IGNORE_TRUNCATE,
        ),
        (
            PushDownFlagsInput {
                type_flags: STRICT_FLAGS.with_truncate_as_warning(true),
                ..PushDownFlagsInput::default()
            },
            FLAG_TRUNCATE_AS_WARNING | FLAG_OVERFLOW_AS_WARNING,
        ),
        (
            PushDownFlagsInput {
                type_flags: STRICT_FLAGS.with_ignore_zero_in_date_err(true),
                ..PushDownFlagsInput::default()
            },
            FLAG_IGNORE_ZERO_IN_DATE,
        ),
        (
            PushDownFlagsInput {
                err_levels: LevelMap::strict().with_level(ErrGroup::DividedByZero, Level::Warn),
                ..PushDownFlagsInput::default()
            },
            FLAG_DIVIDED_BY_ZERO_AS_WARNING,
        ),
        (
            PushDownFlagsInput {
                in_load_data_stmt: true,
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_LOAD_DATA_STMT,
        ),
        (
            PushDownFlagsInput {
                statement_kind: StatementKind::Select,
                type_flags: STRICT_FLAGS.with_truncate_as_warning(true),
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_SELECT_STMT | FLAG_TRUNCATE_AS_WARNING | FLAG_OVERFLOW_AS_WARNING,
        ),
        (
            PushDownFlagsInput {
                type_flags: STRICT_FLAGS.with_ignore_truncate_err(true),
                err_levels: LevelMap::strict().with_level(ErrGroup::DividedByZero, Level::Warn),
                ..PushDownFlagsInput::default()
            },
            FLAG_IGNORE_TRUNCATE | FLAG_DIVIDED_BY_ZERO_AS_WARNING,
        ),
        (
            PushDownFlagsInput {
                statement_kind: StatementKind::UpdateOrDelete,
                type_flags: STRICT_FLAGS.with_ignore_zero_in_date_err(true),
                in_load_data_stmt: true,
                ..PushDownFlagsInput::default()
            },
            FLAG_IN_UPDATE_OR_DELETE_STMT | FLAG_IGNORE_ZERO_IN_DATE | FLAG_IN_LOAD_DATA_STMT,
        ),
    ];

    for (input, expected) in source_cases {
        assert_eq!(push_down_flags(input), expected);
    }

    // UPDATE and DELETE share the same source bit.
    assert_eq!(
        push_down_flags(PushDownFlagsInput {
            statement_kind: StatementKind::UpdateOrDelete,
            ..PushDownFlagsInput::default()
        }),
        FLAG_IN_UPDATE_OR_DELETE_STMT
    );
}

#[test]
fn pushdown_preserves_source_precedence_and_execution_mode_bits() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1251-1288 and
    // pkg/meta/model/flags.go:19-50.
    let both_truncation_modes = STRICT_FLAGS
        .with_ignore_truncate_err(true)
        .with_truncate_as_warning(true);
    assert_eq!(
        push_down_flags_with_type_flags_and_err_levels(both_truncation_modes, LevelMap::strict()),
        FLAG_IGNORE_TRUNCATE
    );

    // The Go helper checks `!= LevelError`, so both warning and ignore levels
    // publish the divided-by-zero warning bit.
    for level in [Level::Warn, Level::Ignore] {
        let levels = LevelMap::strict().with_level(ErrGroup::DividedByZero, level);
        assert_eq!(
            push_down_flags_with_type_flags_and_err_levels(ConversionFlags::default(), levels),
            FLAG_DIVIDED_BY_ZERO_AS_WARNING
        );
    }

    let all_modes = PushDownFlagsInput {
        statement_kind: StatementKind::Insert,
        in_load_data_stmt: true,
        in_restricted_sql: true,
        ..PushDownFlagsInput::default()
    };
    assert_eq!(
        push_down_flags(all_modes),
        FLAG_IN_INSERT_STMT | FLAG_IN_LOAD_DATA_STMT | FLAG_IN_RESTRICTED_SQL
    );

    // INSERT wins over UPDATE/DELETE and SELECT, matching the source else-if
    // chain when several StatementContext booleans are true.
    assert_eq!(
        push_down_flags(PushDownFlagsInput {
            statement_kind: StatementKind::Insert,
            ..PushDownFlagsInput::default()
        }),
        FLAG_IN_INSERT_STMT
    );
}

/// The bits `InitFromPBFlagAndTz` reads back
/// (`pkg/sessionctx/stmtctx/stmtctx.go:1291-1307`). `FlagInLoadDataStmt` and
/// `FlagInRestrictedSQL` are not decoded in the source and therefore cannot
/// survive a round trip; `FlagOverflowAsWarning` is regenerated by the
/// encoder whenever truncate-as-warning is.
const ROUND_TRIP_MASK: u64 = FLAG_IGNORE_TRUNCATE
    | FLAG_TRUNCATE_AS_WARNING
    | FLAG_OVERFLOW_AS_WARNING
    | FLAG_IGNORE_ZERO_IN_DATE
    | FLAG_DIVIDED_BY_ZERO_AS_WARNING
    | FLAG_IN_INSERT_STMT
    | FLAG_IN_UPDATE_OR_DELETE_STMT
    | FLAG_IN_SELECT_STMT;

#[test]
fn init_from_pb_round_trips_the_source_pushdown_table() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1291-1307 against the same
    // TestStatementContextPushDownFLags outputs the forward test above pins
    // (pkg/sessionctx/stmtctx/stmtctx_test.go:89-134): the 12 expected flag
    // values there, decoded and re-encoded.
    let source_flag_values: [u64; 12] = [8, 16, 16, 32, 1, 66, 128, 256, 1024, 98, 257, 1168];

    for flags in source_flag_values {
        let statement = init_from_pb_flags(flags, LevelMap::strict());
        let recomputed = push_down_flags(PushDownFlagsInput {
            type_flags: statement.type_flags,
            err_levels: statement.err_levels,
            statement_kind: statement.statement_kind(),
            in_load_data_stmt: false,
            in_restricted_sql: false,
        });
        assert_eq!(recomputed, flags & ROUND_TRIP_MASK, "flags {flags}");
    }
}

#[test]
fn init_from_pb_decodes_statement_state_like_the_source() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1291-1307.
    let insert = init_from_pb_flags(FLAG_IN_INSERT_STMT, LevelMap::strict());
    assert!(insert.in_insert_stmt);
    assert!(!insert.in_select_stmt);
    assert!(!insert.in_delete_stmt);
    // WithAllowNegativeToUnsigned(!sc.InInsertStmt).
    assert!(!insert.type_flags.allow_negative_to_unsigned());
    assert_eq!(insert.statement_kind(), StatementKind::Insert);

    // The shared UPDATE-or-DELETE bit lands on InDeleteStmt only.
    let delete = init_from_pb_flags(FLAG_IN_UPDATE_OR_DELETE_STMT, LevelMap::strict());
    assert!(delete.in_delete_stmt);
    assert!(delete.type_flags.allow_negative_to_unsigned());
    assert_eq!(delete.statement_kind(), StatementKind::UpdateOrDelete);

    let select = init_from_pb_flags(FLAG_IN_SELECT_STMT, LevelMap::strict());
    assert!(select.in_select_stmt);
    assert_eq!(select.statement_kind(), StatementKind::Select);

    // errctx.ResolveErrLevel(false, bit): warning bit -> LevelWarn, no bit
    // -> LevelError, and other groups pass through the caller's levels.
    let with_warning = init_from_pb_flags(FLAG_DIVIDED_BY_ZERO_AS_WARNING, LevelMap::strict());
    assert_eq!(with_warning.err_levels[ErrGroup::DividedByZero], Level::Warn);
    let carried = LevelMap::strict().with_level(ErrGroup::Truncate, Level::Ignore);
    let without_warning = init_from_pb_flags(0, carried);
    assert_eq!(
        without_warning.err_levels[ErrGroup::DividedByZero],
        Level::Error
    );
    assert_eq!(without_warning.err_levels[ErrGroup::Truncate], Level::Ignore);

    // SetTypeFlags starts from types.DefaultStmtFlags, then applies the
    // three truncation/zero-date bits.
    let truncation = init_from_pb_flags(
        FLAG_IGNORE_TRUNCATE | FLAG_TRUNCATE_AS_WARNING | FLAG_IGNORE_ZERO_IN_DATE,
        LevelMap::strict(),
    );
    assert!(truncation.type_flags.ignore_truncate_err());
    assert!(truncation.type_flags.truncate_as_warning());
    assert!(truncation.type_flags.ignore_zero_in_date_err());
    // DefaultStmtFlags carries FlagIgnoreZeroDateErr.
    assert!(truncation.type_flags.ignore_zero_date_err());
    assert_eq!(
        init_from_pb_flags(0, LevelMap::strict()).type_flags,
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
    );
}
