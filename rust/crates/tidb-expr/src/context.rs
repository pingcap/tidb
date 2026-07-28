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

//! Evaluation errors and the current column/session resolver seam.
//!
//! This remains the deliberately narrow predecessor of TiDB's expression
//! evaluation context. It owns no scalar representation; every value crossing
//! the seam is the dependency-leaf [`tidb_datatype::Datum`].

use tidb_datatype::Datum;

/// Why an expression could not be evaluated in the supported domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalError {
    /// The expression uses a construct outside the currently ported domain.
    Unsupported(&'static str),
    /// An integer literal did not fit the supported integer domain.
    IntOverflow,
    /// A floating-point arithmetic result overflowed to infinity.
    FloatOverflow,
    /// A fixed-point decimal operation exceeded MyDecimal's source buffer.
    DecimalOverflow,
    /// A sequence operation failed at runtime.
    Sequence(&'static str),
    /// Go `ErrDivisionByZero` (1365) raised at error level.
    DivisionByZero,
    /// A `json`-class error that carries its own MySQL error code.
    Json(JsonError),
}

/// The `json`-class errors the JSON builtins raise, each paired with the
/// `pkg/errno` code and `pkg/errno/errname.go` message TiDB reports for it.
///
/// These are separated from [`EvalError::Unsupported`] because they are
/// SQL-visible behavior, not porting boundaries: an application distinguishes
/// "your document is malformed" (3140) from "your path is malformed" (3143),
/// and both arrive on the wire with the code TiDB sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonError {
    /// `ErrInvalidJSONText` (3140). Go's argument is `encoding/json`'s own
    /// message; every malformed document TiDB's `ParseBinaryJSONFromString`
    /// rejects reports the root-value variant, which is what this carries.
    InvalidText,
    /// `ErrInvalidJSONText` (3140) for the OTHER `encoding/json` message
    /// TiDB forwards: an argument that carries no value at all. Only the
    /// column write path can produce it -- a SQL string literal reaching a
    /// JSON builtin is never empty by the time it is parsed.
    EmptyText,
    /// `ErrInvalidJSONPath` (3143), with the 1-based rune position Go's
    /// `parseJSONPathExpr` reports (`jsonPathStream.pos`, or a literal 1 when
    /// the expression does not begin with `$`).
    InvalidPath(usize),
    /// `ErrInvalidJSONPathMultipleSelection` (3149): a wildcard or range leg
    /// in a position where TiDB requires a path selecting exactly one value.
    InvalidPathMultipleSelection,
    /// `ErrInvalidTypeForJSON` (3146): a non-JSON, non-string argument where
    /// the signature demands a JSON document.
    InvalidTypeForJson {
        /// Go's 1-based argument index.
        argument: usize,
        /// The lowercase function name Go names in the message.
        function: &'static str,
    },
    /// `ErrIncorrectType` (3064): an argument of the wrong SQL type.
    IncorrectType {
        /// Go's 1-based argument index.
        argument: usize,
        /// The lowercase function name Go names in the message.
        function: &'static str,
    },
    /// `ErrJSONDocumentNULLKey` (3158): a NULL where an object key belongs.
    NullMemberName,
    /// `ErrJSONVacuousPath` (3153): the root path `$` where the mutation
    /// needs a path INTO the document (`JSON_REMOVE`'s only vacuous case).
    VacuousPath,
    /// `ErrInvalidJSONPathArrayCell` (3165): `JSON_ARRAY_INSERT`'s last leg
    /// is not an array index, so there is no cell to insert before.
    InvalidPathArrayCell,
}

impl JsonError {
    /// The `pkg/errno` code TiDB reports for this error.
    #[must_use]
    pub const fn code(&self) -> u16 {
        match self {
            JsonError::InvalidText | JsonError::EmptyText => 3140,
            JsonError::InvalidPath(_) => 3143,
            JsonError::InvalidPathMultipleSelection => 3149,
            JsonError::InvalidTypeForJson { .. } => 3146,
            JsonError::IncorrectType { .. } => 3064,
            JsonError::NullMemberName => 3158,
            JsonError::VacuousPath => 3153,
            JsonError::InvalidPathArrayCell => 3165,
        }
    }

    /// The message `pkg/errno/errname.go` formats for this error.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            JsonError::InvalidText => "Invalid JSON text: The document root must not be followed \
                 by other values."
                .to_owned(),
            JsonError::EmptyText => "Invalid JSON text: The document is empty".to_owned(),
            JsonError::InvalidPath(position) => format!(
                "Invalid JSON path expression. The error is around character position {position}."
            ),
            JsonError::InvalidPathMultipleSelection => {
                "In this situation, path expressions may not contain the * and ** tokens or an \
                 array range."
                    .to_owned()
            }
            JsonError::InvalidTypeForJson { argument, function } => format!(
                "Invalid data type for JSON data in argument {argument} to function {function}; \
                 a JSON string or JSON type is required."
            ),
            JsonError::IncorrectType { argument, function } => {
                format!("Incorrect type for argument {argument} in function {function}.")
            }
            JsonError::NullMemberName => {
                "JSON documents may not contain NULL member names.".to_owned()
            }
            JsonError::VacuousPath => {
                "The path expression '$' is not allowed in this context.".to_owned()
            }
            JsonError::InvalidPathArrayCell => {
                "A path expression is not a path to a cell in an array.".to_owned()
            }
        }
    }
}

/// Go `errctx.Level`: what a statement does with an error group it can also
/// tolerate. The default is Go's own zero value for a fresh statement
/// context, which warns.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ErrorLevel {
    /// The condition is neither reported nor recorded.
    Ignore,
    /// The condition becomes a warning and evaluation continues.
    #[default]
    Warn,
    /// The condition fails the statement.
    Error,
}

/// The session `time_zone` surfaced through [`Columns::time_zone`]: a fixed
/// offset (`time.FixedZone`) or a named IANA zone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionTimeZone {
    /// A fixed offset east of UTC with its display name.
    Fixed {
        /// The zone's display name.
        name: String,
        /// Seconds east of UTC.
        offset_secs: i32,
    },
    /// A named IANA zone.
    Named(chrono_tz::Tz),
}

/// Resolves column and session state during evaluation.
pub trait Columns {
    /// Returns the referenced column, matched by its final name segment.
    fn get(&self, path: &[String]) -> Option<Datum>;

    /// The statement's fixed `(utc_secs, nanos, tz_offset_seconds)` clock.
    fn now(&self) -> Option<(i64, u32, i32)> {
        None
    }

    /// Go `SessionVars.CurrentDB`, which `DATABASE()`/`SCHEMA()` return.
    /// `None` is the no-database-selected state, where Go returns NULL.
    fn current_database(&self) -> Option<String> {
        None
    }

    /// Go `SessionVars.User`, in the two spellings its builtins report:
    /// `CURRENT_USER()` is the matched grant identity (`String()`) and
    /// `USER()` is the login identity (`LoginString()`). `None` is a session
    /// with no authenticated user at all.
    fn current_user(&self) -> Option<String> {
        None
    }

    /// The login identity `USER()`/`SESSION_USER()` report.
    fn login_user(&self) -> Option<String> {
        None
    }

    /// Go `SessionVars.ActiveRoles` rendered the way `CURRENT_ROLE()` prints
    /// it: the backtick-quoted role identities joined by bare commas, or the
    /// literal `NONE` when no role is active. `None` is a resolver with no
    /// session at all, which reports NULL like `CURRENT_USER()` does.
    fn current_role(&self) -> Option<String> {
        None
    }

    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports as an
    /// unsigned `LongLong`. Go treats a missing `SessionVars` as an error
    /// rather than NULL (`builtinConnectionIDSig.evalInt`), but that case is
    /// unreachable from a real session; `None` here is only the no-session
    /// resolver (`NoColumns`), which reports NULL like `CURRENT_USER` does.
    fn connection_id(&self) -> Option<u64> {
        None
    }

    /// Reads a supported system variable.
    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        let _ = (scope, name);
        None
    }

    /// Go `errctx.ErrGroupDividedByZero`'s level for the running statement.
    ///
    /// Go sets `LevelWarn` for a query, and for `INSERT`/`UPDATE`/`DELETE`
    /// resolves it from the SQL mode: no `ERROR_FOR_DIVISION_BY_ZERO` ignores
    /// it entirely, a non-strict mode (or `IGNORE`) warns, and the default
    /// strict mode fails the statement.
    fn division_by_zero_level(&self) -> ErrorLevel {
        ErrorLevel::Warn
    }

    /// Records a statement warning, which Go appends to `StmtCtx.warnings`.
    fn append_warning(&self, code: u16, message: &str) {
        let _ = (code, message);
    }

    /// Go `handleDivisionByZeroError`: applies this statement's level to a
    /// zero divisor. The value the caller returns is `NULL` either way; only
    /// whether the statement survives differs.
    fn handle_division_by_zero(&self) -> Result<(), EvalError> {
        match self.division_by_zero_level() {
            ErrorLevel::Ignore => Ok(()),
            ErrorLevel::Warn => {
                self.append_warning(1365, "Division by 0");
                Ok(())
            }
            ErrorLevel::Error => Err(EvalError::DivisionByZero),
        }
    }

    /// Reads a case-insensitive user variable.
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        let _ = name;
        None
    }

    /// Assigns a user variable through the resolver's interior mutability.
    fn set_uservar(&self, name: &str, value: Datum) {
        let _ = (name, value);
    }

    /// The affected-row count published by the preceding statement.
    fn row_count(&self) -> Option<i64> {
        None
    }

    /// The session's last automatically generated unsigned identifier.
    fn last_insert_id(&self) -> Option<u64> {
        None
    }

    /// Records `LAST_INSERT_ID(expr)` for the next statement boundary.
    fn set_last_insert_id(&self, value: u64) {
        let _ = value;
    }

    /// TiDB's session `time_zone`. The default is the exact fixed zone the
    /// goeval oracle's mock session pins (`UTC+11`), so constant folding
    /// stays byte-comparable with the golden corpus; a real session
    /// overrides this.
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: "UTC+11".to_string(),
            offset_secs: 11 * 3600,
        }
    }
    /// TiDB's session `default_week_format`.
    fn default_week_format(&self) -> i64 {
        0
    }

    /// TiDB's session `div_precision_increment`.
    fn div_precision_increment(&self) -> u32 {
        4
    }

    /// Advances this session's `RAND()` generator.
    fn rand_next(&self) -> Option<f64> {
        None
    }

    /// Advances one statement-scoped constant `RAND(N)` occurrence.
    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        let _ = (key, seed);
        None
    }

    /// Steps the named sequence and returns the new value.
    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let _ = path;
        Err(EvalError::Unsupported("unsupported function"))
    }

    /// Returns the last value produced by this session for the sequence.
    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let _ = path;
        Err(EvalError::Unsupported("unsupported function"))
    }

    /// Rebases the sequence and returns the requested value when applied.
    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        let _ = (path, value);
        Err(EvalError::Unsupported("unsupported function"))
    }
}

/// A resolver with no columns or session state.
///
/// Its warning sink discards, which is what an evaluation outside a statement
/// -- a test, a DEFAULT expression folded at DDL time -- wants.
pub struct NoColumns;

impl Columns for NoColumns {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
}
