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

//! What an execution failure reaches the client as.
//!
//! Split out of the driver's own rendering table because this half is
//! ONE-ARM-PER-FAILURE over two other crates' error enums, and the property
//! that matters here is the same one that matters there: no wildcard arm, so
//! a newly added execution or evaluation failure is a compile error until
//! someone gives it a message. Both entry points below returned Rust `Debug`
//! text before, which is a message no MySQL client can read and a leak of the
//! enum's internal shape.

use tidb_expr::EvalError;

use super::{DriverError, MysqlError};
use crate::executor::ExecError;

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

/// The MySQL error an execution failure reaches the client as.
pub(super) fn to_mysql_error(error: ExecError) -> MysqlError {
    match error {
        ExecError::Eval(eval) => eval_to_mysql_error(eval),
        // A porting boundary rather than a TiDB error: the carried text is
        // already the whole message, and Go has no answer to compare it with.
        ExecError::Unsupported(reason) => MysqlError::unknown(reason),
        // `From<ExecError> for DriverError` rewrites each of these into its
        // own driver variant, so they arrive here only from an `ExecError` a
        // caller wrapped by hand. Rendering through the twin keeps ONE
        // spelling of each message instead of a second copy. Named one by one
        // rather than with `_` so a new executor failure has to be placed.
        error @ (ExecError::SubqueryReturnsMoreThanOneRow
        | ExecError::MemoryExceedForQuery { .. }
        | ExecError::JsonDocumentNullKey
        | ExecError::InvalidJsonCharset { .. }) => DriverError::from(error).to_mysql_error(),
    }
}

/// The MySQL error an evaluation failure reaches the client as.
///
/// Every arm names its code and its message TOGETHER. They used to be two
/// independent lookups (`EvalError::mysql_code` and `EvalError::mysql_message`,
/// two separate matches over the same enum), and only their happening to
/// enumerate the same variants kept a code from arriving with an empty
/// message -- a bare number with the cause deleted. Producing the pair in one
/// arm is what removes that state rather than defaulting it away.
fn eval_to_mysql_error(error: EvalError) -> MysqlError {
    match error {
        // The `json` and sequence classes carry TiDB's own code (3140
        // malformed document, 3143 malformed path, 4135 exhausted sequence,
        // ...), which applications branch on, and render their own message.
        // Both arms carry a code chosen at run time, so no single literal
        // SQLSTATE can be right for all of them: the JSON codes split across
        // 22032 (3140, 3146, 3158), 42000 (3143, 3149, 3153, 3154, 3165) and
        // HY000 (3150, 3064), and a sequence failure is either 4135 (HY000)
        // or 1146 (42S02). Deriving it from the code is the only answer that
        // holds for every one.
        EvalError::Json(json) => MysqlError::coded(json.code(), json.message()),
        EvalError::Sequence(sequence) => MysqlError::coded(sequence.code(), sequence.message()),
        // The collation class is how a user learns a query needs an explicit
        // `COLLATE`. The operand list is formatted where the tie is detected,
        // so for the two mix errors the payload IS the message.
        EvalError::IllegalMixCollation(message) => {
            MysqlError::new(ER_CANT_AGGREGATE_COLLATIONS, *b"HY000", message)
        }
        EvalError::IllegalMixCollationGeneric(message) => {
            MysqlError::new(ER_CANT_AGGREGATE_NCOLLATIONS, *b"HY000", message)
        }
        EvalError::UnknownCollation(name) => MysqlError::new(
            ER_UNKNOWN_COLLATION,
            *b"HY000",
            format!("Unknown collation: '{name}'"),
        ),
        EvalError::CollationCharsetMismatch { collation, charset } => MysqlError::coded(
            ER_COLLATION_CHARSET_MISMATCH,
            format!("COLLATION '{collation}' is not valid for CHARACTER SET '{charset}'"),
        ),
        EvalError::DivisionByZero => {
            MysqlError::coded(ER_DIVISION_BY_ZERO, "Division by 0".to_owned())
        }
        EvalError::TruncatedWrongValue(message) => {
            MysqlError::coded(ER_TRUNCATED_WRONG_VALUE, message)
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
        EvalError::Unsupported(reason) => MysqlError::unknown(reason),
        EvalError::UnsupportedOperandPair(lhs, rhs) => MysqlError::unknown(format!(
            "a binary operation between a {lhs:?} and a {rhs:?} value is not supported yet"
        )),
    }
}

/// Go `types.ErrOverflow`, whose SQLSTATE is 22003 rather than HY000.
///
/// Go's message ends `in '<expr>'`, naming the statement's rendered
/// expression. No [`EvalError`] carries one -- the overflow is raised deep in
/// arithmetic that never sees the expression tree -- so the class prefix and
/// the code are as much of the answer as this tier can give. The code is the
/// part a client branches on, and it is now the one TiDB sends.
fn out_of_range(class: &str) -> MysqlError {
    MysqlError::new(
        ER_DATA_OUT_OF_RANGE,
        *b"22003",
        format!("{class} value is out of range"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::DatumKind;
    use tidb_expr::{JsonError, SequenceEvalError};

    fn rendered(error: ExecError) -> MysqlError {
        DriverError::Exec(error).to_mysql_error()
    }

    /// CAPTURED from TiDB: `select 9223372036854775807 + 1` is 1690 / 22003,
    /// `select 1e308 * 10` the DOUBLE spelling and a 65-digit product the
    /// DECIMAL one. All three used to reach the client as the generic 1105
    /// carrying the Rust text `Eval(IntOverflow)`.
    #[test]
    fn an_overflow_reports_the_code_and_class_tidb_reports() {
        for (error, class) in [
            (EvalError::IntOverflow, "BIGINT"),
            (EvalError::FloatOverflow, "DOUBLE"),
            (EvalError::DecimalOverflow, "DECIMAL"),
        ] {
            let mysql = rendered(ExecError::Eval(error));
            assert_eq!(mysql.code, 1690);
            assert_eq!(&mysql.state, b"22003");
            assert_eq!(mysql.message, format!("{class} value is out of range"));
        }
    }

    /// An error that carries a code carries its message with it. The two used
    /// to be read as two independent `Option`s, so a code with no message
    /// rendered as that number and an EMPTY string.
    #[test]
    fn an_error_with_a_code_of_its_own_never_renders_an_empty_message() {
        let coded = [
            EvalError::Json(JsonError::InvalidText),
            EvalError::Sequence(SequenceEvalError::RunOut("test.s1".to_owned())),
            EvalError::DivisionByZero,
            EvalError::UnknownCollation("nosuch".to_owned()),
            EvalError::CollationCharsetMismatch {
                collation: "latin1_bin".to_owned(),
                charset: "utf8mb4".to_owned(),
            },
            EvalError::IllegalMixCollation("mixed".to_owned()),
            EvalError::IllegalMixCollationGeneric("mixed".to_owned()),
            EvalError::TruncatedWrongValue("truncated".to_owned()),
        ];
        for error in coded {
            let mysql = rendered(ExecError::Eval(error.clone()));
            assert_ne!(mysql.code, 1105, "{error:?} lost its own code");
            assert!(!mysql.message.is_empty(), "{error:?} rendered no message");
        }
        // The captured wording for the two this tier can produce end to end.
        assert_eq!(
            rendered(ExecError::Eval(EvalError::Sequence(
                SequenceEvalError::RunOut("test.s1".to_owned())
            ))),
            MysqlError::new(4135, *b"HY000", "Sequence 'test.s1' has run out")
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::DivisionByZero)),
            MysqlError::new(1365, *b"22012", "Division by 0")
        );
    }

    /// A porting boundary reports the reason it carries and nothing else. It
    /// used to arrive as `Unsupported("...")` -- the Rust variant name, the
    /// parentheses and the escaped quotes included.
    #[test]
    fn a_porting_boundary_reports_its_reason_and_no_rust_syntax() {
        for error in [
            ExecError::unsupported("a hash join over three inputs"),
            ExecError::Eval(EvalError::Unsupported("a window frame in RANGE units")),
            ExecError::Eval(EvalError::UnsupportedOperandPair(
                DatumKind::Float32,
                DatumKind::Json,
            )),
        ] {
            let mysql = rendered(error.clone());
            assert_eq!(mysql.code, 1105, "{error:?}");
            assert!(
                !mysql.message.contains("Unsupported")
                    && !mysql.message.contains('"')
                    && !mysql.message.starts_with("Eval("),
                "Rust debug text reached the wire for {error:?}: {}",
                mysql.message
            );
        }
        assert_eq!(
            rendered(ExecError::unsupported("a hash join over three inputs")).message,
            "a hash join over three inputs"
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::UnsupportedOperandPair(
                DatumKind::Float32,
                DatumKind::Json
            )))
            .message,
            "a binary operation between a Float32 and a Json value is not supported yet"
        );
    }
}
