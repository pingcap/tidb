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

//! Statement-to-coprocessor flag synthesis from `stmtctx.go`.
//!
//! This leaf owns the dependency-closed bit mapping used by
//! `StatementContext.PushDownFlags` and
//! `PushDownFlagsWithTypeFlagsAndErrLevels`: conversion flags and the
//! divided-by-zero level become the source TiKV request bits, then statement
//! kind and execution-mode bits are composed with the source precedence. It
//! does not own a live `StatementContext`, parse SQL, construct a request, or
//! execute anything in TiKV.

use tidb_datatype::ConversionFlags;

use crate::error_context::{ErrGroup, Level, LevelMap};

/// TiKV request bit for ignored truncation errors.
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// TiKV request bit for truncation-as-warning.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;
/// TiKV request bit for an INSERT statement.
pub const FLAG_IN_INSERT_STMT: u64 = 1 << 3;
/// TiKV request bit for an UPDATE or DELETE statement.
pub const FLAG_IN_UPDATE_OR_DELETE_STMT: u64 = 1 << 4;
/// TiKV request bit for a SELECT statement.
pub const FLAG_IN_SELECT_STMT: u64 = 1 << 5;
/// TiKV request bit for overflow-as-warning.
pub const FLAG_OVERFLOW_AS_WARNING: u64 = 1 << 6;
/// TiKV request bit for ignored zero-in-date errors.
pub const FLAG_IGNORE_ZERO_IN_DATE: u64 = 1 << 7;
/// TiKV request bit for divided-by-zero-as-warning.
pub const FLAG_DIVIDED_BY_ZERO_AS_WARNING: u64 = 1 << 8;
/// TiKV request bit for a `LOAD DATA` statement.
pub const FLAG_IN_LOAD_DATA_STMT: u64 = 1 << 10;
/// TiKV request bit for restricted SQL (for example, auto-analyze work).
pub const FLAG_IN_RESTRICTED_SQL: u64 = 1 << 11;

/// Statement kind bits composed by `StatementContext.PushDownFlags`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementKind {
    /// No DML/SELECT kind bit is set.
    #[default]
    None,
    /// INSERT takes precedence over the other statement-kind booleans.
    Insert,
    /// UPDATE and DELETE share one bit.
    UpdateOrDelete,
    /// SELECT is considered after INSERT and UPDATE/DELETE.
    Select,
}

/// Dependency-closed input to [`push_down_flags`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushDownFlagsInput {
    /// Conversion behavior selected by the statement type context.
    pub type_flags: ConversionFlags,
    /// Source error levels; only the divided-by-zero group is inspected.
    pub err_levels: LevelMap,
    /// Statement kind flags before source precedence is applied.
    pub statement_kind: StatementKind,
    /// Whether this is a `LOAD DATA` execution.
    pub in_load_data_stmt: bool,
    /// Whether this is restricted SQL.
    pub in_restricted_sql: bool,
}

/// Converts source type flags and error levels to TiKV request bits.
///
/// Ignore-truncation wins over truncation-as-warning. The latter also emits
/// the source overflow-as-warning compatibility bit. Any divided-by-zero
/// level other than [`Level::Error`] emits the warning bit.
#[must_use]
pub fn push_down_flags_with_type_flags_and_err_levels(
    type_flags: ConversionFlags,
    err_levels: LevelMap,
) -> u64 {
    let mut flags = 0;
    if type_flags.ignore_truncate_err() {
        flags |= FLAG_IGNORE_TRUNCATE;
    } else if type_flags.truncate_as_warning() {
        flags |= FLAG_TRUNCATE_AS_WARNING | FLAG_OVERFLOW_AS_WARNING;
    }
    if type_flags.ignore_zero_in_date_err() {
        flags |= FLAG_IGNORE_ZERO_IN_DATE;
    }
    if err_levels[ErrGroup::DividedByZero] != Level::Error {
        flags |= FLAG_DIVIDED_BY_ZERO_AS_WARNING;
    }
    flags
}

/// Synthesizes the source `StatementContext.PushDownFlags` bitfield.
#[must_use]
pub fn push_down_flags(input: PushDownFlagsInput) -> u64 {
    let mut flags =
        push_down_flags_with_type_flags_and_err_levels(input.type_flags, input.err_levels);
    flags |= match input.statement_kind {
        StatementKind::None => 0,
        StatementKind::Insert => FLAG_IN_INSERT_STMT,
        StatementKind::UpdateOrDelete => FLAG_IN_UPDATE_OR_DELETE_STMT,
        StatementKind::Select => FLAG_IN_SELECT_STMT,
    };
    if input.in_load_data_stmt {
        flags |= FLAG_IN_LOAD_DATA_STMT;
    }
    if input.in_restricted_sql {
        flags |= FLAG_IN_RESTRICTED_SQL;
    }
    flags
}
