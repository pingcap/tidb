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

/// The MySQL error an execution failure reaches the client as.
#[test]
fn coprocessor_cot_error_matches_go() {
    // pkg/expression/builtin_math_test.go: TestCot.
    let storage = crate::storage::StorageError::Sql(MysqlError::new(
        1690,
        "DOUBLE value is out of range in 'cot(0)'",
    ));
    let error = to_mysql_error(ExecError::from(crate::kv_table::KvTableError::from(
        storage,
    )));
    assert_eq!(error.code, 1690);
    assert_eq!(error.state, *b"22003");
    assert_eq!(error.message, "DOUBLE value is out of range in 'cot(0)'");
}

pub(super) fn to_mysql_error(error: ExecError) -> MysqlError {
    match error {
        ExecError::Mysql(error) => error,
        ExecError::Eval(eval) => {
            let error = eval_to_mysql_error(eval);
            // go's charFunctionClass.getFunction BUILD failure (`Unknown
            // charset ...`) is a statement-level build error, not an
            // evaluation event: the warning buffer records the 1105 row
            // beside the argument casts' truncation warnings (oracle:
            // CHAR('中文测试', 'ünïcödé')).
            if error.message.starts_with("Unknown charset ") {
                error
            } else {
                error.from_evaluation()
            }
        }
        // An internal invariant error carries the exact message Go returns
        // through its generic error path.
        ExecError::Internal(message) => MysqlError::unknown(message),
        // A porting boundary rather than a TiDB error: the carried text is
        // already the whole message, and Go has no answer to compare it with.
        ExecError::Unsupported(reason) => MysqlError::unknown(reason),
        // Go surfaces a spill failure as the raw error from `Next`, i.e. 1105.
        ExecError::SpillFailed(reason) => MysqlError::unknown(reason),
        ExecError::Killed(error) => {
            let mut state = [0; 5];
            state.copy_from_slice(error.state.as_bytes());
            MysqlError::with_state(error.code, state, error.message)
        }
        // `From<ExecError> for DriverError` rewrites each of these into its
        // own driver variant, so they arrive here only from an `ExecError` a
        // caller wrapped by hand. Rendering through the twin keeps ONE
        // spelling of each message instead of a second copy. Named one by one
        // rather than with `_` so a new executor failure has to be placed.
        ExecError::JsonDocumentNullKey => {
            // Evaluation-origin: go's warning buffer stays EMPTY beside the
            // fatal 3158 (oracle g-group: `JSON_OBJECTAGG(NULL, 1)`).
            DriverError::from(error).to_mysql_error().from_evaluation()
        }
        error @ (ExecError::SubqueryReturnsMoreThanOneRow
        | ExecError::CteMaxRecursionDepth(_)
        | ExecError::MemoryExceedForQuery { .. }
        | ExecError::InvalidJsonCharset { .. }) => DriverError::from(error).to_mysql_error(),
    }
}

/// The codes `ClassTiKV` registers (`pkg/store/driver/error/error.go`,
/// `pkg/kv/error.go`): the only ones a coprocessor error keeps.
const CLASS_TIKV_CODES: &[u16] = {
    use tidb_error::tidb::errcode::*;
    &[
        ErrSharedLockLost,
        ErrLockExpire,
        ErrAssertionFailed,
        ErrTiKVStoreLimit,
        ErrTiKVServerTimeout,
        ErrTiFlashServerTimeout,
        ErrTxnAbortedByGC,
        ErrTiKVStaleCommand,
        ErrQueryInterrupted,
        ErrTiKVMaxTimestampNotSynced,
        ErrLockAcquireFailAndNoWaitSet,
        ErrResolveLockTimeout,
        ErrLockWaitTimeout,
        ErrTiKVServerBusy,
        ErrTiFlashServerBusy,
        ErrPDServerTimeout,
        ErrRegionUnavailable,
        ErrResourceGroupNotExists,
        ErrResourceGroupConfigUnavailable,
        ErrResourceGroupThrottled,
        ErrUnknown,
        ErrDataOutOfRange,
        ErrTruncatedWrongValue,
        ErrDivisionByZero,
    ]
};

/// An evaluation failure inside a pushed-down predicate. Go's coprocessor
/// answers it in the `SelectResponse` under the error's own MySQL code
/// (`toPBError`), and TiDB re-raises that as
/// `dbterror.ClassTiKV.Synthesize(code, msg)`: a code `ClassTiKV` registers
/// keeps its number, anything else reaches the client as 1105 with the bare
/// message (`select * from t where a > 0x80` over a utf8mb4 `a`).
pub(crate) fn coprocessor_evaluation_error(error: EvalError) -> ExecError {
    let mysql = eval_to_mysql_error(error).from_evaluation();
    if CLASS_TIKV_CODES.contains(&mysql.code) {
        ExecError::Mysql(mysql)
    } else {
        ExecError::Mysql(MysqlError::unknown(mysql.message))
    }
}

/// The MySQL error an evaluation failure reaches the client as
/// ([`EvalError::to_sql_error`]).
fn eval_to_mysql_error(error: EvalError) -> MysqlError {
    let sql = error.to_sql_error();
    let mysql = MysqlError::new(sql.code, sql.message);
    match error {
        // Every json-class error is evaluation-origin: go's warning buffer
        // never records a FATAL evaluation error as its own row beside the
        // statement error (the column-sourced `JSON_EXTRACT(a, 1)` answers
        // 1105 with an EMPTY SHOW WARNINGS; oracle g-group:
        // `JSON_OBJECTAGG(NULL, 1)` answers 3158 the same way).
        EvalError::Json(_) => mysql.from_evaluation(),
        _ => mysql,
    }
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
                SequenceEvalError::RunOut("test.s1".to_owned())
            ))),
            MysqlError::new(4135, "Sequence 'test.s1' has run out").from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::DivisionByZero)),
            MysqlError::new(1365, "Division by 0").from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::WrongParameterCount(
                "aes_encrypt"
            ))),
            MysqlError::new(
                1582,
                "Incorrect parameter count in the call to native function 'aes_encrypt'"
            ).from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::IncorrectArguments(
                "The initialization vector supplied to aes_encrypt is too short".to_owned()
            ))),
            MysqlError::new(
                1210,
                "The initialization vector supplied to aes_encrypt is too short"
            ).from_evaluation()
        );
        assert_eq!(
            rendered(ExecError::Eval(EvalError::DataOutOfRange {
                value: "length",
                expression: "random_bytes".to_string(),
            })),
            MysqlError::new(1690, "length value is out of range in 'random_bytes'").from_evaluation()
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

    #[test]
    fn an_internal_executor_error_is_not_reported_as_unsupported() {
        let mysql = rendered(ExecError::internal("chunk invariant failed"));
        assert_eq!(mysql, MysqlError::new(1105, "chunk invariant failed"));
    }

    #[test]
    fn local_temporary_space_quota_reaches_the_wire_unchanged() {
        let message = tidb_util::spill_storage::LOCAL_TEMPORARY_SPACE_QUOTA_ERROR;
        let mysql = rendered(ExecError::SpillFailed(message.to_owned()));
        assert_eq!(mysql, MysqlError::new(1105, message));
    }
}
