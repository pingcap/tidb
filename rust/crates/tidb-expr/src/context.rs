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
    /// A binary operation reached the evaluator with an operand pair that no
    /// domain dispatch claims.
    ///
    /// This is deliberately an error rather than a panic. The catch-all it
    /// replaces asserted that the dispatches above it were exhaustive, and
    /// that assertion was FALSE TWICE in production paths -- once for
    /// `Float32` (a `FLOAT` column compared with an integer) and once for
    /// `Json` (`MIN` over a json column, TiDB issue 31640). Each time a
    /// single user query aborted the whole process, killing every other
    /// connection. A statement-level error is a far cheaper way to learn the
    /// same fact.
    ///
    /// Both kinds are carried because the identity of the unhandled kind is
    /// the entire diagnostic value; a fixed string names neither.
    UnsupportedOperandPair(tidb_datatype::DatumKind, tidb_datatype::DatumKind),
    /// An integer literal did not fit the supported integer domain.
    IntOverflow,
    /// A floating-point arithmetic result overflowed to infinity.
    FloatOverflow,
    /// A fixed-point decimal operation exceeded MyDecimal's source buffer.
    DecimalOverflow,
    /// A `NEXTVAL`/`LASTVAL`/`SETVAL` failed at runtime. Both cases carry the
    /// sequence's qualified name because both of TiDB's messages print it.
    Sequence(SequenceEvalError),
    /// Go `ErrDivisionByZero` (1365) raised at error level.
    DivisionByZero,
    /// Go `types.ErrTruncatedWrongVal` (1292) raised at error level, carrying
    /// the already-formatted message body. A SELECT never reaches this arm --
    /// `ResetContextOfStmt` gives it `WithTruncateAsWarning(true)` with no
    /// mode input -- but a strict `INSERT` does, so the condition needs an
    /// error spelling as well as a warning one.
    TruncatedWrongValue(String),
    /// A `json`-class error that carries its own MySQL error code.
    Json(JsonError),
    /// Go `collate.ErrIllegalMix2Collation`/`ErrIllegalMix3Collation` (1267):
    /// the operands of one operation carry collations that cannot be
    /// aggregated, and no explicit `COLLATE` clause resolves the tie. Carries
    /// the fully formatted message, whose operand list is derived per call.
    IllegalMixCollation(String),
    /// Go `collate.ErrIllegalMixCollation` (1271): the same condition for an
    /// operation with an arity other than two or three, which MySQL reports
    /// WITHOUT naming the operands.
    IllegalMixCollationGeneric(String),
    /// Go `charset.ErrUnknownCollation` (1273): a `COLLATE` clause naming a
    /// collation the registry does not know.
    UnknownCollation(String),
    /// Go `charset.ErrCollationCharsetMismatch` (1253): a `COLLATE` clause
    /// naming a collation that does not belong to the value's character set.
    CollationCharsetMismatch {
        /// The collation written after `COLLATE`.
        collation: String,
        /// The value's own character set.
        charset: String,
    },
}

impl EvalError {
    /// The MySQL error code this error reaches the client with, when it has
    /// one of its own. `None` means the caller's generic mapping applies.
    #[must_use]
    pub fn mysql_code(&self) -> Option<u16> {
        match self {
            EvalError::Json(json) => Some(json.code()),
            EvalError::IllegalMixCollation(_) => Some(1267),
            EvalError::IllegalMixCollationGeneric(_) => Some(1271),
            EvalError::CollationCharsetMismatch { .. } => Some(1253),
            EvalError::UnknownCollation(_) => Some(1273),
            EvalError::DivisionByZero => Some(1365),
            EvalError::TruncatedWrongValue(_) => Some(1292),
            EvalError::Sequence(error) => Some(error.code()),
            _ => None,
        }
    }

    /// The message body for an error that carries its own code.
    #[must_use]
    pub fn mysql_message(&self) -> Option<String> {
        match self {
            EvalError::Json(json) => Some(json.message()),
            EvalError::IllegalMixCollation(message)
            | EvalError::IllegalMixCollationGeneric(message) => Some(message.clone()),
            EvalError::CollationCharsetMismatch { collation, charset } => Some(format!(
                "COLLATION '{collation}' is not valid for CHARACTER SET '{charset}'"
            )),
            EvalError::UnknownCollation(name) => Some(format!("Unknown collation: '{name}'")),
            EvalError::DivisionByZero => Some("Division by 0".to_owned()),
            EvalError::TruncatedWrongValue(message) => Some(message.clone()),
            EvalError::Sequence(error) => Some(error.message()),
            _ => None,
        }
    }
}

/// Why a sequence builtin failed, with the code and message TiDB reports.
///
/// The two are DIFFERENT error classes in Go, and neither is the auto-id
/// allocator's 1467: reading past the end of a `NOCYCLE` sequence is
/// `table.ErrSequenceHasRunOut`, and naming something that is not a sequence
/// is the ordinary `infoschema.ErrTableNotExists`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceEvalError {
    /// Go `table.ErrSequenceHasRunOut` (4135). Captured:
    /// `Sequence 'test.s4' has run out`.
    RunOut(String),
    /// Go `infoschema.ErrTableNotExists` (1146). Captured for
    /// `select nextval(nosuch)`: `Table 'test.nosuch' doesn't exist`.
    NotASequence(String),
}

impl SequenceEvalError {
    /// The MySQL error number.
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            SequenceEvalError::RunOut(_) => 4135,
            SequenceEvalError::NotASequence(_) => 1146,
        }
    }

    /// The message TiDB prints.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            SequenceEvalError::RunOut(name) => format!("Sequence '{name}' has run out"),
            SequenceEvalError::NotASequence(name) => format!("Table '{name}' doesn't exist"),
        }
    }
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
    /// `ErrJSONBadOneOrAllArg` (3154): `JSON_CONTAINS_PATH`'s `one_or_all`
    /// argument is neither `'one'` nor `'all'`.
    BadOneOrAllArg {
        /// The lowercase function name Go names in the message.
        function: &'static str,
    },
    /// `ErrInvalidJSONContainsPathType` (3150): `JSON_SEARCH`'s `one_or_all`
    /// argument is neither `'one'` nor `'all'`.
    ///
    /// Go raises a DIFFERENT error here than `JSON_CONTAINS_PATH` raises for
    /// the identical mistake -- `builtinJSONSearchSig.evalJSON` checks the
    /// argument itself and returns this, so `BinaryJSON.Search`'s own
    /// `ErrJSONBadOneOrAllArg` is unreachable from SQL.
    InvalidContainsPathType,
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
            JsonError::BadOneOrAllArg { .. } => 3154,
            JsonError::InvalidContainsPathType => 3150,
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
            JsonError::BadOneOrAllArg { function } => format!(
                "The oneOrAll argument to {function} may take these values: 'one' or 'all'."
            ),
            JsonError::InvalidContainsPathType => {
                "The second argument can only be either 'one' or 'all'.".to_owned()
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

/// The session `time_zone` surfaced through [`Columns::time_zone`].
///
/// The type itself lives in `tidb-datatype` because the storage codecs need
/// it and sit below this crate, exactly as Go's `tablecodec` sits below
/// `sessionctx`; see [`tidb_datatype::SessionTimeZone`].
pub use tidb_datatype::SessionTimeZone;

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

    /// Go `types.Flags`'s truncation bits, collapsed to the three outcomes
    /// `types.Context.HandleTruncate` picks between: `IgnoreTruncateErr`
    /// discards, `TruncateAsWarning` warns, and neither fails the statement.
    ///
    /// `ResetContextOfStmt`'s `*ast.SelectStmt` arm sets
    /// `WithTruncateAsWarning(true)` with NO mode input, so a read warns in
    /// every SQL mode; `util.GetTypeFlagsForInsert` uses
    /// `!strictSQLMode || ignoreErr`, so the same condition fails a strict
    /// write. A resolver with no statement answers the read behaviour, which
    /// is Go's own zero value for a fresh `StmtCtx`.
    fn truncate_level(&self) -> ErrorLevel {
        ErrorLevel::Warn
    }

    /// Go `types.Context.HandleTruncate`: applies this statement's level to a
    /// value that lost information during conversion. The best-effort value
    /// the caller already computed stands either way -- Go returns the
    /// scanned prefix from `getValidIntPrefix` before this is consulted -- so
    /// only whether the statement survives, and whether the client is told,
    /// differ.
    fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
        match self.truncate_level() {
            ErrorLevel::Ignore => Ok(()),
            ErrorLevel::Warn => {
                self.append_warning(1292, message);
                Ok(())
            }
            ErrorLevel::Error => Err(EvalError::TruncatedWrongValue(message.to_owned())),
        }
    }

    /// Records a statement warning, which Go appends to `StmtCtx.warnings`.
    fn append_warning(&self, code: u16, message: &str) {
        let _ = (code, message);
    }

    /// Go `SessionVars.SQLMode`'s three temporal bits, shared with the write
    /// path (`tidb_executor::zero_date`) so one table decides what a zero,
    /// zero-in, or invalid date means on BOTH sides of a value.
    ///
    /// A resolver with no session answers TiDB's shipped `DefaultSQLMode`,
    /// which is the mode a folded constant would meet on a default server.
    fn date_modes(&self) -> tidb_datatype::DateModes {
        tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE
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

    /// Go `sequenceOperator.GetSequenceNextVal`: consumes and returns the next
    /// value of the sequence `path` names -- `[name]` or `[db, name]`, exactly
    /// as the SQL spelled it, resolved against the session's current database.
    ///
    /// Consuming is NOT transactional: Go allocates in its own meta
    /// transaction, so a rolled-back statement still spends the value
    /// (captured). Nothing behind this method undoes it either.
    ///
    /// This is THE sequence seam, shared by both evaluators: the row path
    /// (`eval_func`) hands over the parser's own path, and the chunk path
    /// (`eval_func_values_in`) hands over the string constants the rewriter
    /// substituted for the column reference. Keeping one method means a
    /// `NEXTVAL` cannot behave differently depending on which evaluator ran.
    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let _ = path;
        Err(EvalError::Unsupported("NEXTVAL requires a session"))
    }

    /// Go `sequenceOperator.GetSequenceLastVal`: the last value THIS SESSION
    /// took from the sequence, or `Datum::Null` when it has taken none --
    /// `LASTVAL` is session state, not the stored counter (captured: `lastval`
    /// on a sequence this session has not read is `<nil>`).
    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        let _ = path;
        Err(EvalError::Unsupported("LASTVAL requires a session"))
    }

    /// Go `sequenceOperator.SetSequenceVal`: moves the sequence forward.
    /// `Datum::Null` is Go's `alreadySatisfied` -- a sequence never moves
    /// backwards, and reports NULL rather than an error when asked to.
    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        let _ = (path, value);
        Err(EvalError::Unsupported("SETVAL requires a session"))
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
