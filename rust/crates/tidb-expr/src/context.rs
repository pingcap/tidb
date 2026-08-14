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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tidb_datatype::Datum;

/// The transaction timestamp visible to one SQL session.
///
/// The handle is shared with statement contexts because an autocommit
/// transaction opens lazily on its first storage read, after expression
/// evaluation has already been wired. Publishing through this handle lets a
/// later expression in that same statement observe the timestamp without
/// allocating one for constant-only statements.
#[derive(Clone, Debug, Default)]
pub struct CurrentTso(Arc<AtomicU64>);

impl CurrentTso {
    /// Publishes the timestamp of the transaction that just opened.
    pub fn publish(&self, tso: u64) {
        self.0.store(tso, Ordering::Release);
    }

    /// Clears the completed transaction from the session.
    pub fn clear(&self) {
        self.publish(0);
    }

    /// Returns the signed value `TIDB_CURRENT_TSO()` exposes.
    #[must_use]
    pub fn value(&self) -> i64 {
        i64::try_from(self.0.load(Ordering::Acquire)).unwrap_or(i64::MAX)
    }
}

/// Why an expression could not be evaluated in the supported domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalError {
    /// The expression uses a construct outside the currently ported domain.
    Unsupported(&'static str),
    /// Go `expression.ErrFunctionNotExists` (1305).
    FunctionNotExists(String),
    /// Go `plannererrors.ErrNoDB` (1046), raised before 1305 when resolving an
    /// unknown function with no current database.
    NoDatabaseSelected,
    /// Go `expression.ErrOperandColumns` (1241): the right row operand does
    /// not contain the number of columns required by the left operand.
    OperandColumns(usize),
    /// Go `ErrWrongParamcountToNativeFct` (1582).
    WrongParameterCount(&'static str),
    /// Go `ErrWrongArguments` (1210), with the source-formatted argument
    /// description.
    IncorrectArguments(String),
    /// Go `types.ErrOverflow` / MySQL 1690 for a builtin-owned range check.
    DataOutOfRange {
        /// The value class printed before "value is out of range".
        value: &'static str,
        /// The expression/function printed inside quotes.
        expression: &'static str,
    },
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
    /// Go `types.VectorFloat32` returns a value-domain error for an invalid
    /// vector operation (for example unequal dimensions or an elementwise
    /// overflow). The text is the source operation's diagnostic, which SQL
    /// exposes as a generic execution error rather than changing it into an
    /// unrelated arithmetic-overflow class.
    Vector(String),
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
    /// Go `errWarnAllowedPacketOverflowed` (1301) raised at ERROR level,
    /// which `handleAllowedPacketOverflowed` does for a statement with
    /// neither `TruncateAsWarning` nor `IgnoreTruncateErr` -- a strict
    /// `INSERT`. Carries the fully formatted message, which is the same text
    /// the warning spelling appends.
    AllowedPacketOverflowed(String),
    /// Go `types.ErrTruncatedWrongVal` (1292) raised at error level, carrying
    /// the already-formatted message body. A SELECT never reaches this arm --
    /// `ResetContextOfStmt` gives it `WithTruncateAsWarning(true)` with no
    /// mode input -- but a strict `INSERT` does, so the condition needs an
    /// error spelling as well as a warning one.
    TruncatedWrongValue(String),
    /// A `json`-class error that carries its own MySQL error code.
    Json(JsonError),
    /// Go `types.ErrWrongValue` (1292) / `ErrWrongValue2` (1525) raised while
    /// BUILDING a typed temporal literal (`DATE 'lit'`, `TIMESTAMP 'lit'`).
    /// Unlike the cast of the same text, these reject the whole statement --
    /// see `crate::time_literal` for the three ways the literal differs from
    /// the cast. The code is carried because this one syntax raises both.
    WrongTemporalLiteral {
        /// The MySQL error number, 1292 or 1525.
        code: u16,
        /// The fully formatted message body.
        message: String,
    },
    /// A source-owned MySQL error raised while validating an expression's
    /// declared type before evaluation (for example `DECIMAL(M,D)` or a
    /// temporal fractional precision). The code and text are both fixed by
    /// TiDB's preprocessing contract.
    InvalidTypeDeclaration {
        /// The MySQL error number.
        code: u16,
        /// The fully formatted message body.
        message: String,
    },
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

/// The supported values of MySQL's `block_encryption_mode` session variable.
///
/// Go selects an immutable AES signature from this value while building the
/// expression. Rust carries the same validated statement snapshot through
/// [`Columns`], so every row of one statement uses one mode without reaching
/// back into mutable session state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BlockEncryptionMode {
    /// AES-128 ECB, TiDB's shipped default.
    #[default]
    Aes128Ecb,
    /// AES-192 ECB.
    Aes192Ecb,
    /// AES-256 ECB.
    Aes256Ecb,
    /// AES-128 CBC.
    Aes128Cbc,
    /// AES-192 CBC.
    Aes192Cbc,
    /// AES-256 CBC.
    Aes256Cbc,
    /// AES-128 OFB.
    Aes128Ofb,
    /// AES-192 OFB.
    Aes192Ofb,
    /// AES-256 OFB.
    Aes256Ofb,
    /// AES-128 CFB.
    Aes128Cfb,
    /// AES-192 CFB.
    Aes192Cfb,
    /// AES-256 CFB.
    Aes256Cfb,
}

impl BlockEncryptionMode {
    /// Parses one value already admitted by the system-variable catalog.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value.to_ascii_lowercase().as_str() {
            "aes-128-ecb" => Self::Aes128Ecb,
            "aes-192-ecb" => Self::Aes192Ecb,
            "aes-256-ecb" => Self::Aes256Ecb,
            "aes-128-cbc" => Self::Aes128Cbc,
            "aes-192-cbc" => Self::Aes192Cbc,
            "aes-256-cbc" => Self::Aes256Cbc,
            "aes-128-ofb" => Self::Aes128Ofb,
            "aes-192-ofb" => Self::Aes192Ofb,
            "aes-256-ofb" => Self::Aes256Ofb,
            "aes-128-cfb" => Self::Aes128Cfb,
            "aes-192-cfb" => Self::Aes192Cfb,
            "aes-256-cfb" => Self::Aes256Cfb,
            _ => return None,
        })
    }

    /// MySQL's XOR-folded key width for this mode, in bytes.
    #[must_use]
    pub const fn key_size(self) -> usize {
        match self {
            Self::Aes128Ecb | Self::Aes128Cbc | Self::Aes128Ofb | Self::Aes128Cfb => 16,
            Self::Aes192Ecb | Self::Aes192Cbc | Self::Aes192Ofb | Self::Aes192Cfb => 24,
            Self::Aes256Ecb | Self::Aes256Cbc | Self::Aes256Ofb | Self::Aes256Cfb => 32,
        }
    }

    /// Whether the selected mode requires a third initialization-vector
    /// argument.
    #[must_use]
    pub const fn iv_required(self) -> bool {
        !matches!(self, Self::Aes128Ecb | Self::Aes192Ecb | Self::Aes256Ecb)
    }
}

/// Resolves column and session state during evaluation.
pub trait Columns {
    /// Returns the referenced column, matched by its final name segment.
    fn get(&self, path: &[String]) -> Option<Datum>;

    /// Whether the statement's SQL mode forces integer subtraction to use a
    /// signed result even when an operand is unsigned.
    fn no_unsigned_subtraction(&self) -> bool {
        false
    }

    /// The statement's fixed `(utc_secs, nanos, tz_offset_seconds)` clock.
    fn now(&self) -> Option<(i64, u32, i32)> {
        None
    }

    /// Whether `SYSDATE` is an alias of the statement-scoped `NOW`.
    fn sysdate_is_now(&self) -> bool {
        false
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

    /// Go `SessionVars.LastFoundRows`, published when the preceding result
    /// set was completely drained. Statements without a result set do not
    /// replace it.
    fn found_rows(&self) -> Option<u64> {
        None
    }

    /// Go `TIDB_CURRENT_TSO()`: the active transaction's start timestamp, or
    /// zero when this session has no active transaction.
    fn current_tso(&self) -> i64 {
        0
    }

    /// Reads a supported system variable.
    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        let _ = (scope, name);
        None
    }

    /// The process identity returned by `TIDB_VERSION()`.
    ///
    /// Unlike ordinary information builtins, Go reads this from immutable
    /// build/config state and does not require a session. A real statement
    /// overrides this default with the identity captured by its server.
    fn tidb_info(&self) -> String {
        tidb_util::printer::get_tidb_info(&tidb_util::versioninfo::VersionInfo::build_default())
    }

    /// The statement snapshot of `@@block_encryption_mode`.
    fn block_encryption_mode(&self) -> BlockEncryptionMode {
        BlockEncryptionMode::default()
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

    /// Whether the session SQL mode contains either strict-mode flag.
    fn strict_sql_mode(&self) -> bool {
        true
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

    /// Number of warnings currently held by the statement evaluator.
    ///
    /// Go's `NewFunctionTryFold` bookmarks this count before evaluating a
    /// constant candidate and restores the bookmark when evaluation warns.
    fn warning_count(&self) -> usize {
        0
    }

    /// Removes warnings at and after `bookmark`.
    fn truncate_warnings(&self, _bookmark: usize) {}

    /// Go `EvalContext.GetMaxAllowedPacket`, which every result-sizing string
    /// builtin captures into its signature at BUILD time
    /// (`builtinSpaceSig.maxAllowedPacket` and friends).
    ///
    /// The default is Go's `DefMaxAllowedPacket`, 64 MiB -- the value a
    /// default server runs with, and the one this crate's string builtins
    /// were already hardcoding. A session-backed resolver overrides it; until
    /// one does, a server whose `max_allowed_packet` was lowered still sizes
    /// results by the default here.
    fn max_allowed_packet(&self) -> u64 {
        64 << 20
    }

    /// Go `handleAllowedPacketOverflowed` (`pkg/expression/errors.go:88-96`):
    /// a string builtin whose result would exceed `max_allowed_packet` warns
    /// 1301 and yields NULL -- unless the statement has neither
    /// `TruncateAsWarning` nor `IgnoreTruncateErr`, in which case the same
    /// condition is returned as a statement ERROR.
    ///
    /// ```text
    /// if f := tc.Flags(); f.TruncateAsWarning() || f.IgnoreTruncateErr() {
    ///     tc.AppendWarning(err)
    ///     return nil
    /// }
    /// return errors.Trace(err)
    /// ```
    ///
    /// That is the SAME pair of flags [`Columns::truncate_level`] already
    /// reduces, so a strict `INSERT` errors and every read and non-strict
    /// write warns. The caller's `NULL` stands on the warning arm.
    fn handle_allowed_packet_overflowed(&self, expr_name: &str) -> Result<(), EvalError> {
        let message = format!(
            "Result of {expr_name}() was larger than max_allowed_packet ({}) - truncated",
            self.max_allowed_packet()
        );
        match self.truncate_level() {
            // `IgnoreTruncateErr` is Go's first disjunct, which warns as well
            // -- the source appends unconditionally on that arm.
            ErrorLevel::Ignore | ErrorLevel::Warn => {
                self.append_warning(1301, &message);
                Ok(())
            }
            ErrorLevel::Error => Err(EvalError::AllowedPacketOverflowed(message)),
        }
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

    /// The statement's implicit `LIKE` escape when `ESCAPE` is omitted.
    ///
    /// Ordinary contexts keep MySQL's historical backslash. A live session
    /// overrides this when both `NO_BACKSLASH_ESCAPES` and
    /// `tidb_enable_no_backslash_escapes_in_like` apply.
    fn like_default_escape(&self) -> u8 {
        b'\\'
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

/// [`NoColumns`] carrying the session's `time_zone`.
///
/// The eval-time twin of [`crate::rewriter::ZonedNoResolver`], and it exists
/// for the same reason: [`NoColumns`] answers the trait's DEFAULT zone
/// (`UTC+11`, the goeval oracle's mock session), which is nobody's session.
/// Any evaluation that HAS a session behind it -- restoring a virtual
/// generated column on a read, most of all, since Go rebuilds that
/// expression in the reading session -- must carry that session's zone here
/// rather than inherit the oracle's.
pub struct ZonedNoColumns(pub SessionTimeZone);

impl Columns for ZonedNoColumns {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.0.clone()
    }
}
