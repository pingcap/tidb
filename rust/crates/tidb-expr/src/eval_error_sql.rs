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

use tidb_charset::charset_errors as charset;
use tidb_datatype::collate_errors as collate;
use crate::expression_errors as expression;
use tidb_datatype::types_errors as types;
use tidb_terror::MysqlError;
use tidb_mysql::errors::SqlError;
use tidb_terror::TerrorError;
use tidb_hack::GoToLower;

use crate::{EvalError, JsonError, SequenceEvalError};

/// An evaluation failure as the client sees it. The codes are Go
/// `expression`'s (and `types`') error prototypes, which is why this lives in
/// the expression crate rather than beside `EvalError`.
pub trait EvalErrorSql {
    /// Go `terror.ToSQLError` for an evaluation failure: the MySQL code and
    /// message it reaches a client as, wherever it is raised -- TiDB's
    /// executor, or a coprocessor answering it in `SelectResponse.error`
    /// (`toPBError`).
    fn to_sql_error(&self) -> SqlError;

    /// The same error as the session layer's MySQL error.
    fn to_mysql_error(&self) -> MysqlError;
}

/// The Go error a JSON or sequence evaluation failure raises: `types`' and
/// `expression`'s error prototypes.
pub trait ToTerror {
    /// The Go error, as Go raises it.
    fn to_terror(&self) -> tidb_terror::TerrorError;
}

impl ToTerror for SequenceEvalError {
    /// The Go error this raises, as Go raises it.
    fn to_terror(&self) -> tidb_terror::TerrorError {
        use crate::expression_errors as expression;
        match self {
            SequenceEvalError::Raised(error) => error.clone(),
            SequenceEvalError::AccessDenied {
                privilege,
                user,
                host,
                sequence,
            } => expression::ERR_SEQUENCE_ACCESS_DENIED.generate_with_stack_by_args(&[
                (*privilege).into(),
                user.as_str().into(),
                host.as_str().into(),
                sequence.as_str().into(),
            ]),
        }
    }
}

impl ToTerror for JsonError {
    /// The Go error this raises, as Go raises it: the `json` document / path
    /// errors in `types`, the argument errors in `expression`.
    fn to_terror(&self) -> tidb_terror::TerrorError {
        use crate::expression_errors as expression;
        use tidb_datatype::types_errors as types;
        match self {
            JsonError::InvalidText => types::ERR_INVALID_JSON_TEXT.generate_with_stack_by_args(&[
                "The document root must not be followed by other values.".into(),
            ]),
            JsonError::EmptyText => types::ERR_INVALID_JSON_TEXT
                .generate_with_stack_by_args(&["The document is empty".into()]),
            JsonError::InvalidCharset => {
                types::ERR_INVALID_JSON_CHARSET.generate_with_stack_by_args(&["binary".into()])
            }
            JsonError::InvalidTextInParam {
                argument,
                function,
                reason,
                position,
            } => types::ERR_INVALID_JSON_TEXT_IN_PARAM.generate_with_stack_by_args(&[
                (*argument).into(),
                (*function).into(),
                (*reason).into(),
                (*position).into(),
            ]),
            JsonError::InvalidPath(position) => {
                types::ERR_INVALID_JSON_PATH.generate_with_stack_by_args(&[(*position).into()])
            }
            JsonError::InvalidPathMultipleSelection => {
                types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION.generate_with_stack_by_args(&[])
            }
            JsonError::InvalidTypeForJson { argument, function } => {
                expression::ERR_INVALID_TYPE_FOR_JSON
                    .generate_with_stack_by_args(&[(*argument).into(), (*function).into()])
            }
            JsonError::InvalidJsonType {
                argument,
                function,
                required,
            } => types::ERR_INVALID_JSON_TYPE.generate_with_stack_by_args(&[
                (*argument).into(),
                (*function).into(),
                required.as_str().into(),
            ]),
            JsonError::IncorrectType { argument, function } => expression::ERR_INCORRECT_TYPE
                .generate_with_stack_by_args(&[argument.to_string().into(), (*function).into()]),
            JsonError::NullMemberName => {
                types::ERR_JSON_DOCUMENT_NULL_KEY.generate_with_stack_by_args(&[])
            }
            JsonError::VacuousPath => types::ERR_JSON_VACUOUS_PATH.generate_with_stack_by_args(&[]),
            JsonError::InvalidPathArrayCell => {
                types::ERR_INVALID_JSON_PATH_ARRAY_CELL.generate_with_stack_by_args(&[])
            }
            JsonError::BadOneOrAllArg { function } => types::ERR_JSON_BAD_ONE_OR_ALL_ARG
                .generate_with_stack_by_args(&[(*function).into()]),
            JsonError::DocumentTooDeep => {
                types::ERR_JSON_DOCUMENT_TOO_DEEP.generate_with_stack_by_args(&[])
            }
        }
    }
}

impl EvalErrorSql for EvalError {
    /// Every arm names its code and its message TOGETHER, so a code never
    /// arrives with an empty message.
    fn to_sql_error(&self) -> SqlError {
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
            EvalError::NotImplemented(name) => {
                go(&expression::ERR_NOT_SUPPORTED_YET, &[name.as_ref().into()])
            }
            EvalError::WrongValueForType {
                value_class,
                value,
                function,
            } => go(
                &expression::ERR_WRONG_VALUE_FOR_TYPE,
                &[value_class.into(), value.into(), function.into()],
            ),
            EvalError::Json(json) => json.to_terror().to_sql_error(),
            EvalError::Sequence(sequence) => sequence.to_terror().to_sql_error(),
            // The collation class is how a user learns a query needs an explicit
            // `COLLATE`. The operand list is formatted where the tie is detected,
            // so for the two mix errors the payload IS the message.
            EvalError::IllegalMixCollation(message) => {
                classed(&collate::ERR_ILLEGAL_MIX2_COLLATION, message)
            }
            EvalError::IllegalMixCollationGeneric(message) => {
                classed(&collate::ERR_ILLEGAL_MIX_COLLATION, message)
            }
            EvalError::IllegalMix3Collation(message) => {
                classed(&collate::ERR_ILLEGAL_MIX3_COLLATION, message)
            }
            EvalError::UnknownCollation(name) => {
                go(&charset::ERR_UNKNOWN_COLLATION, &[name.into()])
            }
            EvalError::CollationCharsetMismatch { collation, charset } => go(
                &charset::ERR_COLLATION_CHARSET_MISMATCH,
                &[collation.into(), charset.into()],
            ),
            EvalError::DivisionByZero => go(&expression::ERR_DIVISION_BY_ZERO, &[]),
            EvalError::TruncatedWrongValue(message) => {
                classed(&types::ERR_TRUNCATED_WRONG_VAL, message)
            }
            EvalError::Conversion(error) => error.to_sql_error(),
            EvalError::GroupConcatCut(message) => {
                classed(&expression::ERR_CUT_VALUE_GROUP_CONCAT, message)
            }
            // Go `errWarnAllowedPacketOverflowed` (1301), the ERROR spelling of
            // the warning a read takes: same code, same text.
            EvalError::AllowedPacketOverflowed(message) => {
                classed(&expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED, message)
            }
            EvalError::FunctionNotExists(name) => go(
                &expression::ERR_FUNCTION_NOT_EXISTS,
                &["FUNCTION".into(), name.into()],
            ),
            EvalError::NoDatabaseSelected => go(&tidb_plannererrors::ERR_NO_DB, &[]),
            // A typed temporal literal raises 1292 (22007) for a parse failure and
            // 1525 (HY000) for its regex gate, so the code travels with the
            // message and the SQLSTATE is derived from it like the JSON class
            // above.
            EvalError::WrongTemporalLiteral { code, message } => {
                classed(types_declaration_proto(code), message)
            }
            EvalError::InvalidTypeDeclaration { code, message } => {
                classed(types_declaration_proto(code), message)
            }
            EvalError::OperandColumns(columns) => {
                go(&expression::ERR_OPERAND_COLUMNS, &[columns.into()])
            }
            EvalError::WrongParameterCount(function) => go(
                &expression::ERR_INCORRECT_PARAMETER_COUNT,
                &[function.into()],
            ),
            EvalError::IncorrectArguments(message) => {
                classed(&expression::ERR_INCORRECT_ARGS, message)
            }
            EvalError::InvalidGroupFuncUse => {
                go(&tidb_plannererrors::ERR_INVALID_GROUP_FUNC_USE, &[])
            }
            EvalError::TooManyGroupingArguments => go(
                &tidb_plannererrors::ERR_INVALID_NUMBER_OF_ARGS,
                &["GROUPING".into(), 64.into()],
            ),
            EvalError::FieldInGroupingNotGroupBy(index) => go(
                &tidb_plannererrors::ERR_FIELD_IN_GROUPING_NOT_GROUP_BY,
                &[format!("#{index}").into()],
            ),
            // Go `types.ErrTooBigPrecision` (1426): the clock signatures raise it
            // at evaluation time, not at parse time
            // (`pkg/expression/builtin_time.go:2730` and siblings).
            EvalError::TooBigFsp { fsp, function } => go(
                &types::ERR_TOO_BIG_PRECISION,
                &[fsp.into(), function.into(), 6.into()],
            ),
            EvalError::AdvisoryLock { code, message } => match code {
                3057 => classed(&expression::ERR_USER_LOCK_WRONG_NAME, message),
                3058 => classed(&expression::ERR_USER_LOCK_DEADLOCK, message),
                // A backend (store) failure carries its own code and no Go class.
                _ => sql_error(code, message),
            },
            EvalError::DataOutOfRange { value, expression } => {
                go(&types::ERR_OVERFLOW, &[value.into(), expression.into()])
            }
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
                go(
                    &tidb_plannererrors::ERR_UNKNOWN_COLUMN,
                    &[column.go_to_lower().into(), "expression".into()],
                )
            }
            // Go `ErrBadField` with the clause the name was written in
            // (`clauseMsg`), for example `Unknown column 'j' in 'order clause'`.
            EvalError::UnknownColumnInClause(column, clause) => go(
                &tidb_plannererrors::ERR_UNKNOWN_COLUMN,
                &[column.go_to_lower().into(), clause.into()],
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

    /// The statement error an evaluation failure reaches the client as: the
    /// [`Self::to_sql_error`] triple with its Go identity, marked as raised by
    /// expression evaluation -- the class go's statement warning buffer never
    /// records as its own row (`driver_tidb.go` `StmtCtx.AppendError` sees
    /// parse/plan/executor failures only).
    ///
    /// One exception keeps the statement-level shape: go's
    /// `charFunctionClass.getFunction` BUILD failure (`Unknown charset ...`)
    /// is a build error, not an evaluation event, so the warning buffer
    /// records its 1105 row beside the argument casts' truncation warnings
    /// (oracle: `CHAR('中文测试', 'ünïcödé')`). Every json-class error is
    /// evaluation-origin whatever its text.
    fn to_mysql_error(&self) -> MysqlError {
        let error = MysqlError::from_sql_error(self.to_sql_error());
        if !matches!(self, EvalError::Json(_)) && error.message.starts_with("Unknown charset ") {
            error
        } else {
            error.from_evaluation()
        }
    }
}

/// A `SqlError` whose SQLSTATE is the one the code carries (Go `NewErr`'s
/// `MySQLState` lookup) and that has no Go class: a failure Go raises through
/// a plain `errors.New` (code 1105) or through a backend's own error code.
fn sql_error(code: u16, message: impl Into<String>) -> SqlError {
    SqlError {
        code,
        message: message.into(),
        state: tidb_mysql::errors::mysql_state(code),
        identity: None,
    }
}

/// Go `proto.GenWithStackByArgs(args...)`: the prototype's template formatted
/// with Go's arguments.
fn go(proto: &TerrorError, args: &[tidb_mysql::errors::FormatArg]) -> SqlError {
    proto.generate_with_stack_by_args(args).to_sql_error()
}

/// Go `proto.GenWithStack(message)`: the prototype's class and code with the
/// message this site rendered. The wire code, the SQLSTATE and the identity
/// `Error()` prints all come from the Go definition.
fn classed(proto: &TerrorError, message: impl Into<String>) -> SqlError {
    proto.generate(message).to_sql_error()
}

/// The `types` prototypes a typed-literal / declared-type refusal raises:
/// `ErrWrongValue` (1292), `ErrWrongValue2` (1525), `ErrMBiggerThanD` (1427),
/// `ErrTooBigPrecision` (1426) and `ErrTooBigScale` (1425). The producing
/// variants name only these.
fn types_declaration_proto(code: u16) -> &'static TerrorError {
    match code {
        1292 => &types::ERR_WRONG_VALUE,
        1525 => &types::ERR_WRONG_VALUE2,
        1427 => &types::ERR_M_BIGGER_THAN_D,
        1426 => &types::ERR_TOO_BIG_PRECISION,
        1425 => &types::ERR_TOO_BIG_SCALE,
        other => unreachable!("no types prototype for declaration error {other}"),
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
    classed(
        &types::ERR_OVERFLOW,
        format!("{class} value is out of range"),
    )
}

#[cfg(test)]
mod mysql_error_tests {
    use super::*;
    use crate::JsonError;

    /// `to_mysql_error` is the `to_sql_error` triple and identity, flagged as
    /// evaluation-origin; only the `Unknown charset` build failure is not.
    #[test]
    fn to_mysql_error_keeps_the_sql_error_and_flags_evaluation() {
        for error in [
            EvalError::IntOverflow,
            EvalError::DivisionByZero,
            EvalError::Json(JsonError::InvalidText),
            EvalError::Unsupported("a window frame in RANGE units"),
            EvalError::Vector("vectors have different dimensions: 1 and 2".to_owned()),
        ] {
            let sql = error.to_sql_error();
            let mysql = error.to_mysql_error();
            assert_eq!(mysql.code, sql.code, "{error:?}");
            assert_eq!(&mysql.state, sql.state.as_bytes(), "{error:?}");
            assert_eq!(mysql.message, sql.message, "{error:?}");
            assert_eq!(mysql.identity, sql.identity, "{error:?}");
            assert!(mysql.is_from_evaluation(), "{error:?}");
        }
        let charset = EvalError::Unsupported("Unknown charset ünïcödé");
        let mysql = charset.to_mysql_error();
        assert_eq!(mysql, MysqlError::unknown("Unknown charset ünïcödé"));
        assert!(!mysql.is_from_evaluation());
    }
}
