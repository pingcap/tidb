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
    push_down_flags, push_down_flags_with_type_flags_and_err_levels, PushDownFlagsInput,
    StatementKind, FLAG_DIVIDED_BY_ZERO_AS_WARNING, FLAG_IGNORE_TRUNCATE, FLAG_IGNORE_ZERO_IN_DATE,
    FLAG_IN_INSERT_STMT, FLAG_IN_LOAD_DATA_STMT, FLAG_IN_RESTRICTED_SQL, FLAG_IN_SELECT_STMT,
    FLAG_IN_UPDATE_OR_DELETE_STMT, FLAG_OVERFLOW_AS_WARNING, FLAG_TRUNCATE_AS_WARNING,
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
