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
//! also owns the inverse direction, `InitFromPBFlagAndTz`'s decoding of the
//! same bits back into statement state ([`init_from_pb_flags`]). It does not
//! own a live `StatementContext`, parse SQL, construct a request, or execute
//! anything in TiKV.

use tidb_datatype::{ConversionFlags, DEFAULT_STATEMENT_FLAGS};

use crate::error_context::{resolve_err_level, ErrGroup, Level, LevelMap};

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

/// The flags a plain `SELECT` produces, from Go's `*ast.SelectStmt` arm of
/// `ResetContextOfStmt` (`pkg/executor/select.go`): `WithTruncateAsWarning`
/// and `WithIgnoreZeroInDate` are written as LITERALS there, with no SQL-mode
/// input, and `ErrGroupDividedByZero` is `LevelWarn` before the switch. No
/// session variable can change any of the three, which is what makes one
/// value correct for every plain read.
///
/// It evaluates to 482 (`2 | 32 | 64 | 128 | 256`), but is computed rather
/// than written down: a change to the bit mapping must move this too.
///
/// DEFERRED LIVE CHECK: only a real TiKV can confirm the region acts on these
/// bits. The named case is `SELECT ROUND(s) FROM t` with `s = '12abc'`, which
/// TiDB answers with the truncated value plus a 1292 warning. Under flags `0`
/// the region fails the request instead; under 482 with no warning sink the
/// answer is right and the warning is silently lost. Both halves are needed
/// for the observable TiDB behavior, and neither unit test below reaches a
/// region.
#[must_use]
pub fn select_push_down_flags() -> u64 {
    let mut err_levels = LevelMap::strict();
    err_levels[ErrGroup::DividedByZero] = Level::Warn;
    push_down_flags(PushDownFlagsInput {
        type_flags: ConversionFlags::default()
            .with_truncate_as_warning(true)
            .with_ignore_zero_in_date_err(true),
        err_levels,
        statement_kind: StatementKind::Select,
        in_load_data_stmt: false,
        in_restricted_sql: false,
    })
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

/// Statement state reconstructed from TiKV request bits by the source
/// `StatementContext.InitFromPBFlagAndTz`
/// (`pkg/sessionctx/stmtctx/stmtctx.go:1291-1307`), the inverse direction of
/// [`push_down_flags`]. The `*time.Location` half of the Go method is a
/// plain store on the statement context and stays with the context owner;
/// this leaf owns only the flag mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PbInitializedStatement {
    /// Go `sc.InInsertStmt`, from `FlagInInsertStmt`.
    pub in_insert_stmt: bool,
    /// Go `sc.InSelectStmt`, from `FlagInSelectStmt`.
    pub in_select_stmt: bool,
    /// Go `sc.InDeleteStmt`: the shared UPDATE-or-DELETE bit lands on the
    /// DELETE boolean, exactly as in the source.
    pub in_delete_stmt: bool,
    /// Go `sc.SetErrLevels(levels)`: the caller's levels with only the
    /// divided-by-zero group resolved from `FlagDividedByZeroAsWarning`.
    pub err_levels: LevelMap,
    /// Go `sc.SetTypeFlags(...)`: `DefaultStmtFlags` plus the three
    /// truncation/zero-date bits, with negative-to-unsigned allowed for
    /// everything but INSERT.
    pub type_flags: ConversionFlags,
}

impl PbInitializedStatement {
    /// The [`StatementKind`] the reconstructed booleans produce back in
    /// [`push_down_flags`], with the source else-if precedence
    /// (INSERT, then UPDATE/DELETE, then SELECT).
    #[must_use]
    pub const fn statement_kind(&self) -> StatementKind {
        if self.in_insert_stmt {
            StatementKind::Insert
        } else if self.in_delete_stmt {
            StatementKind::UpdateOrDelete
        } else if self.in_select_stmt {
            StatementKind::Select
        } else {
            StatementKind::None
        }
    }
}

/// The flag half of the source `InitFromPBFlagAndTz`
/// (`pkg/sessionctx/stmtctx/stmtctx.go:1291-1307`): decodes a
/// `tipb.SelectRequest.Flags` bitfield into statement booleans, the
/// divided-by-zero error level, and statement type flags.
///
/// `err_levels` plays Go's `sc.ErrLevels()`: every group except
/// divided-by-zero passes through unchanged. `FlagInLoadDataStmt` and
/// `FlagInRestrictedSQL` are not read, matching the source.
#[must_use]
pub fn init_from_pb_flags(flags: u64, err_levels: LevelMap) -> PbInitializedStatement {
    let in_insert_stmt = (flags & FLAG_IN_INSERT_STMT) > 0;
    let in_select_stmt = (flags & FLAG_IN_SELECT_STMT) > 0;
    let in_delete_stmt = (flags & FLAG_IN_UPDATE_OR_DELETE_STMT) > 0;
    let err_levels = err_levels.with_level(
        ErrGroup::DividedByZero,
        resolve_err_level(false, (flags & FLAG_DIVIDED_BY_ZERO_AS_WARNING) > 0),
    );
    PbInitializedStatement {
        in_insert_stmt,
        in_select_stmt,
        in_delete_stmt,
        err_levels,
        type_flags: DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err((flags & FLAG_IGNORE_TRUNCATE) > 0)
            .with_truncate_as_warning((flags & FLAG_TRUNCATE_AS_WARNING) > 0)
            .with_ignore_zero_in_date_err((flags & FLAG_IGNORE_ZERO_IN_DATE) > 0)
            .with_allow_negative_to_unsigned(!in_insert_stmt),
    }
}
