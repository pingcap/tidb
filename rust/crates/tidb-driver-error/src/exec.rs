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

use tidb_exprctx::EvalError;

use super::{DriverError, MysqlError};
use crate::ExecError;
use tidb_expr::EvalErrorSql;

/// The MySQL error an execution failure reaches the client as.
pub(super) fn to_mysql_error(error: ExecError) -> MysqlError {
    match error {
        ExecError::Mysql(error) => error,
        // `EvalError::to_mysql_error` owns the evaluation-origin flag and its
        // `Unknown charset` build-failure exception.
        ExecError::Eval(eval) => eval.to_mysql_error(),
        // An internal invariant error carries the exact message Go returns
        // through its generic error path.
        ExecError::Internal(message) => MysqlError::unknown(message),
        // A porting boundary rather than a TiDB error: the carried text is
        // already the whole message, and Go has no answer to compare it with.
        ExecError::Unsupported(reason) => MysqlError::unknown(reason),
        // Go surfaces a spill failure as the raw error from `Next`, i.e. 1105.
        ExecError::SpillFailed(reason) => MysqlError::unknown(reason),
        ExecError::Killed(error) => MysqlError::from_sql_error(error),
        // `From<ExecError> for DriverError` rewrites each of these into its
        // own driver variant, so they arrive here only from an `ExecError` a
        // caller wrapped by hand. Rendering through the twin keeps ONE
        // spelling of each message instead of a second copy. Named one by one
        // rather than with `_` so a new executor failure has to be placed.
        error @ (ExecError::MemoryExceedForQuery { .. } | ExecError::Retrieval(_)) => {
            DriverError::from(error).to_mysql_error()
        }
    }
}

/// An evaluation failure inside a pushed-down predicate. Go's coprocessor
/// answers it in the `SelectResponse` under the error's own MySQL code
/// (`toPBError`), and TiDB re-raises that as
/// `dbterror.ClassTiKV.Synthesize(code, msg)`: a code `ClassTiKV` registers
/// keeps its number, anything else reaches the client as 1105 with the bare
/// message (`select * from t where a > 0x80` over a utf8mb4 `a`).
pub fn coprocessor_evaluation_error(error: EvalError) -> ExecError {
    let mysql = error.to_mysql_error();
    // `ClassTiKV.Synthesize(code, msg)` keeps the TiKV class whatever the code:
    // a registered one answers under its own number, any other under 1105 --
    // `Error()` prints `[tikv:<code>]` either way.
    ExecError::Mysql(tidb_dbterror::ErrClass::synthesize_tikv(mysql.code, mysql.message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::DatumKind;
    use tidb_exprctx::JsonError;
use tidb_exprctx::SequenceEvalError;

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

    /// What tidb-table's sequence raises for a `NOCYCLE` sequence read past
    /// its end (Go `table.ErrSequenceHasRunOut`).
    fn sequence_run_out(database: &str, sequence: &str) -> tidb_terror::TerrorError {
        tidb_table::table_errors::ERR_SEQUENCE_HAS_RUN_OUT
            .generate_with_stack_by_args(&[database.into(), sequence.into()])
    }

    /// An error that carries a code carries its message with it. The two used
    /// to be read as two independent `Option`s, so a code with no message
    /// rendered as that number and an EMPTY string.
    #[test]
    fn an_error_with_a_code_of_its_own_never_renders_an_empty_message() {
        let coded = [
            EvalError::Json(JsonError::InvalidText),
            EvalError::Sequence(SequenceEvalError::Raised(sequence_run_out("test", "s1"))),
            EvalError::DivisionByZero,
            EvalError::UnknownCollation("nosuch".to_owned()),
            EvalError::CollationCharsetMismatch {
                collation: "latin1_bin".to_owned(),
                charset: "utf8mb4".to_owned(),
            },
            EvalError::IllegalMixCollation("mixed".to_owned()),
            EvalError::IllegalMixCollationGeneric("mixed".to_owned()),
            EvalError::TruncatedWrongValue("truncated".to_owned()),
            EvalError::GroupConcatCut("group concat cut".to_owned()),
            EvalError::AllowedPacketOverflowed("overflowed".to_owned()),
            EvalError::WrongParameterCount("aes_encrypt"),
            EvalError::IncorrectArguments("bad AES initialization vector".to_owned()),
            EvalError::DataOutOfRange {
                value: "length",
                expression: "random_bytes".to_string(),
            },
        ];
        for error in coded {
            let mysql = rendered(ExecError::Eval(error.clone()));
            assert_ne!(mysql.code, 1105, "{error:?} lost its own code");
            assert!(!mysql.message.is_empty(), "{error:?} rendered no message");
        }
        // The captured wording for the two this tier can produce end to end.
        assert_eq!(
            rendered(ExecError::Eval(EvalError::Sequence(
                SequenceEvalError::Raised(sequence_run_out("test", "s1"))
            ))),
            MysqlError::from_terror(
                &tidb_table::table_errors::ERR_SEQUENCE_HAS_RUN_OUT
                    .generate_with_stack("Sequence 'test.s1' has run out")
            )
            .from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::DivisionByZero)),
            MysqlError::from_terror(
                &tidb_expr::expression_errors::ERR_DIVISION_BY_ZERO
                    .generate_with_stack_by_args(&[])
            )
            .from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::WrongParameterCount(
                "aes_encrypt"
            ))),
            MysqlError::from_terror(
                &tidb_expr::expression_errors::ERR_INCORRECT_PARAMETER_COUNT
                    .generate_with_stack_by_args(&["aes_encrypt".into()])
            )
            .from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::IncorrectArguments(
                "The initialization vector supplied to aes_encrypt is too short".to_owned()
            ))),
            MysqlError::from_terror(
                &tidb_expr::expression_errors::ERR_INCORRECT_ARGS.generate_with_stack(
                    "The initialization vector supplied to aes_encrypt is too short"
                )
            )
            .from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::DataOutOfRange {
                value: "length",
                expression: "random_bytes".to_string(),
            })),
            MysqlError::from_terror(
                &tidb_datatype::types_errors::ERR_OVERFLOW
                    .generate_with_stack("length value is out of range in 'random_bytes'")
            )
            .from_evaluation()
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
            ExecError::Eval(EvalError::Vector(
                "vectors have different dimensions: 1 and 2".to_owned(),
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
        assert_eq!(
            rendered(ExecError::Eval(EvalError::Vector(
                "vectors have different dimensions: 1 and 2".to_owned(),
            )))
            .message,
            "vectors have different dimensions: 1 and 2"
        );
    }

    /// The driver renders an evaluation failure exactly as
    /// `EvalError::to_mysql_error` does, flag included.
    #[test]
    fn an_evaluation_failure_renders_through_eval_error_to_mysql_error() {
        for error in [
            EvalError::IntOverflow,
            EvalError::DivisionByZero,
            EvalError::Json(JsonError::InvalidText),
            EvalError::Unsupported("Unknown charset ünïcödé"),
            EvalError::Unsupported("a window frame in RANGE units"),
        ] {
            assert_eq!(rendered(ExecError::Eval(error.clone())), error.to_mysql_error());
        }
        assert!(!rendered(ExecError::Eval(EvalError::Unsupported("Unknown charset x")))
            .is_from_evaluation());
        let pushed = coprocessor_evaluation_error(EvalError::DivisionByZero);
        let ExecError::Mysql(pushed) = pushed else { panic!("{pushed:?}") };
        assert_eq!(
            pushed,
            tidb_dbterror::ErrClass::synthesize_tikv(1365, "Division by 0")
        );
    }

    #[test]
    fn an_internal_executor_error_is_not_reported_as_unsupported() {
        let mysql = rendered(ExecError::internal("chunk invariant failed"));
        assert_eq!(mysql, MysqlError::unknown("chunk invariant failed"));
    }

    #[test]
    fn local_temporary_space_quota_reaches_the_wire_unchanged() {
        let message = tidb_spill_storage::LOCAL_TEMPORARY_SPACE_QUOTA_ERROR;
        let mysql = rendered(ExecError::SpillFailed(message.to_owned()));
        assert_eq!(mysql, MysqlError::unknown(message));
    }
}
