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

//! What an evaluation failure reaches a MySQL client as.
//!
//! One arm per failure and no wildcard, so a newly added [`EvalError`] is a
//! compile error until someone gives it a code and a message. TiDB's
//! executor and the coprocessor (`toPBError`) both answer through it.

use tidb_error::mysql::SqlError;
use tidb_hack::GoToLower;

use crate::EvalError;

/// MySQL `ER_DATA_OUT_OF_RANGE`.
const ER_DATA_OUT_OF_RANGE: u16 = 1690;
/// Go `collate.ErrIllegalMix2Collation` / `ErrIllegalMix3Collation`.
const ER_CANT_AGGREGATE_COLLATIONS: u16 = 1267;
/// Go `collate.ErrIllegalMixCollation`, for any other arity.
const ER_CANT_AGGREGATE_NCOLLATIONS: u16 = 1271;
/// Go `charset.ErrCollationCharsetMismatch`.
const ER_COLLATION_CHARSET_MISMATCH: u16 = 1253;
/// Go `charset.ErrUnknownCollation`.
const ER_UNKNOWN_COLLATION: u16 = 1273;
/// Go `ErrDivisionByZero`.
const ER_DIVISION_BY_ZERO: u16 = 1365;
/// Go `types.ErrTruncatedWrongVal`.
const ER_TRUNCATED_WRONG_VALUE: u16 = 1292;
/// Go `expression.ErrCutValueGroupConcat`.
const ER_CUT_VALUE_GROUP_CONCAT: u16 = 1260;
/// Go `ErrWarnAllowedPacketOverflowed`.
const ER_WARN_ALLOWED_PACKET_OVERFLOWED: u16 = 1301;
/// Go `expression.ErrOperandColumns`.
const ER_OPERAND_COLUMNS: u16 = tidb_error::mysql::errcode::ErrOperandColumns;

impl EvalError {
    /// Go `terror.ToSQLError` for an evaluation failure: the MySQL code and
    /// message it reaches a client as, wherever it is raised -- TiDB's
    /// executor, or a coprocessor answering it in `SelectResponse.error`
    /// (`toPBError`).
    ///
    /// Every arm names its code and its message TOGETHER, so a code never
    /// arrives with an empty message.
    #[must_use]
    pub fn to_sql_error(&self) -> SqlError {
        match self.clone() {
            // The `json` and sequence classes carry TiDB's own code (3140
            // malformed document, 3143 malformed path, 4135 exhausted sequence,
            // ...), which applications branch on, and render their own message.
            // Both arms carry a code chosen at run time, so no single literal
            // SQLSTATE can be right for all of them: the JSON codes split across
            // 22032 (3140, 3146, 3158), 42000 (3143, 3149, 3153, 3154, 3165) and
            // HY000 (3150, 3064), and a sequence failure is either 4135 (HY000)
            // or 1146 (42S02). Deriving it from the code is the only answer that
            // holds for every one.
            EvalError::Unsupported(reason) => sql_error(1105, reason),
            EvalError::NotImplemented(name) => sql_error(
                1235,
                format!("This version of TiDB doesn't yet support '{name}'"),
            ),
            EvalError::WrongValueForType {
                value_class,
                value,
                function,
            } => sql_error(
                1411,
                // The template is "Incorrect %-.32s value: '%-.128s' for function
                // %-.32s": each operand is cut to that many characters.
                format!(
                    "Incorrect {} value: '{}' for function {}",
                    value_class.chars().take(32).collect::<String>(),
                    value.chars().take(128).collect::<String>(),
                    function.chars().take(32).collect::<String>()
                ),
            ),
            EvalError::Json(json) => sql_error(json.code(), json.message()),
            EvalError::Sequence(sequence) => sql_error(sequence.code(), sequence.message()),
            // The collation class is how a user learns a query needs an explicit
            // `COLLATE`. The operand list is formatted where the tie is detected,
            // so for the two mix errors the payload IS the message.
            EvalError::IllegalMixCollation(message) => {
                sql_error(ER_CANT_AGGREGATE_COLLATIONS, message)
            }
            EvalError::IllegalMixCollationGeneric(message) => {
                sql_error(ER_CANT_AGGREGATE_NCOLLATIONS, message)
            }
            EvalError::IllegalMix3Collation(message) => sql_error(1270, message),
            EvalError::UnknownCollation(name) => {
                sql_error(ER_UNKNOWN_COLLATION, format!("Unknown collation: '{name}'"))
            }
            EvalError::CollationCharsetMismatch { collation, charset } => sql_error(
                ER_COLLATION_CHARSET_MISMATCH,
                format!("COLLATION '{collation}' is not valid for CHARACTER SET '{charset}'"),
            ),
            EvalError::DivisionByZero => sql_error(ER_DIVISION_BY_ZERO, "Division by 0".to_owned()),
            EvalError::TruncatedWrongValue(message) => sql_error(ER_TRUNCATED_WRONG_VALUE, message),
            EvalError::Conversion(error) => error.to_sql_error(),
            EvalError::GroupConcatCut(message) => sql_error(ER_CUT_VALUE_GROUP_CONCAT, message),
            // Go `errWarnAllowedPacketOverflowed` (1301), the ERROR spelling of
            // the warning a read takes: same code, same text.
            EvalError::AllowedPacketOverflowed(message) => {
                sql_error(ER_WARN_ALLOWED_PACKET_OVERFLOWED, message)
            }
            EvalError::FunctionNotExists(name) => {
                sql_error(1305, format!("FUNCTION {name} does not exist"))
            }
            EvalError::NoDatabaseSelected => sql_error(1046, "No database selected".to_owned()),
            // A typed temporal literal raises 1292 (22007) for a parse failure and
            // 1525 (HY000) for its regex gate, so the code travels with the
            // message and the SQLSTATE is derived from it like the JSON class
            // above.
            EvalError::WrongTemporalLiteral { code, message } => sql_error(code, message),
            EvalError::InvalidTypeDeclaration { code, message } => sql_error(code, message),
            EvalError::OperandColumns(columns) => sql_error(
                ER_OPERAND_COLUMNS,
                format!("Operand should contain {columns} column(s)"),
            ),
            EvalError::WrongParameterCount(function) => sql_error(
                1582,
                format!("Incorrect parameter count in the call to native function '{function}'"),
            ),
            EvalError::IncorrectArguments(message) => sql_error(1210, message),
            EvalError::InvalidGroupFuncUse => {
                sql_error(1111, "Invalid use of group function".to_owned())
            }
            EvalError::TooManyGroupingArguments => sql_error(
                3601,
                "Too many arguments for function GROUPING; maximum allowed is 64".to_owned(),
            ),
            EvalError::FieldInGroupingNotGroupBy(index) => sql_error(
                3602,
                format!("Argument #{index} of GROUPING function is not in GROUP BY"),
            ),
            // Go `types.ErrTooBigPrecision` (1426): the clock signatures raise it
            // at evaluation time, not at parse time
            // (`pkg/expression/builtin_time.go:2730` and siblings).
            EvalError::TooBigFsp { fsp, function } => sql_error(
                1426,
                format!("Too-big precision {fsp} specified for '{function}'. Maximum is 6."),
            ),
            EvalError::AdvisoryLock { code, message } => sql_error(code, message),
            EvalError::DataOutOfRange { value, expression } => sql_error(
                ER_DATA_OUT_OF_RANGE,
                format!("{value} value is out of range in '{expression}'"),
            ),
            // CAPTURED from TiDB: `select 9223372036854775807 + 1` is
            // `1690 / 22003 / BIGINT value is out of range in '(9223372036854775807 + 1)'`,
            // `select 1e308 * 10` the DOUBLE spelling, and a 65-digit product the
            // DECIMAL one. See [`out_of_range`] for the one part still missing.
            EvalError::IntOverflow => out_of_range("BIGINT"),
            EvalError::FloatOverflow => out_of_range("DOUBLE"),
            EvalError::DecimalOverflow => out_of_range("DECIMAL"),
            // Porting boundaries with no TiDB answer to match: TiDB evaluates
            // these, so there is no Go message for "not ported yet". The carried
            // text is the whole diagnostic.
            // Go `ErrBadField` with `clauseMsg[expressionClause]` — the clause a
            // resolution site without its own clause name reports.
            EvalError::UnknownColumn(column) => {
                // go folds an UNQUOTED identifier to its stored lowercase form
                // before naming it in ErrBadField.
                sql_error(
                    1054,
                    format!("Unknown column '{}' in 'expression'", column.go_to_lower()),
                )
            }
            // Go `ErrBadField` with the clause the name was written in
            // (`clauseMsg`), for example `Unknown column 'j' in 'order clause'`.
            EvalError::UnknownColumnInClause(column, clause) => sql_error(
                1054,
                format!("Unknown column '{}' in '{clause}'", column.go_to_lower()),
            ),
            EvalError::UnsupportedOperandPair(lhs, rhs) => sql_error(
                1105,
                format!(
                    "a binary operation between a {lhs:?} and a {rhs:?} value is not supported yet"
                ),
            ),
            EvalError::Vector(message) => sql_error(1105, message),
            EvalError::ParamIndexExceedParamCounts => {
                sql_error(1105, "Param index exceed param counts")
            }
        }
    }
}

/// A `SqlError` whose SQLSTATE is the one the code carries (Go `NewErr`'s
/// `MySQLState` lookup).
fn sql_error(code: u16, message: impl Into<String>) -> SqlError {
    SqlError {
        code,
        message: message.into(),
        state: tidb_error::mysql::mysql_state(code),
    }
}

/// Go `types.ErrOverflow`, whose SQLSTATE is 22003 rather than HY000.
///
/// Go's message ends `in '<expr>'`, naming the statement's rendered
/// expression. No [`EvalError`] carries one -- the overflow is raised deep in
/// arithmetic that never sees the expression tree -- so the class prefix and
/// the code are as much of the answer as this tier can give. The code is the
/// part a client branches on, and it is now the one TiDB sends.
fn out_of_range(class: &str) -> SqlError {
    sql_error(
        ER_DATA_OUT_OF_RANGE,
        format!("{class} value is out of range"),
    )
}
