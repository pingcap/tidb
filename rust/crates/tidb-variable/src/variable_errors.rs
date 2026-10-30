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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_dbterror::catalog_support::{plain_err, std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `sessionctx/variable.errWarnDeprecatedSyntax` (`pkg/sessionctx/variable/error.go:25`).
pub static ERR_WARN_DEPRECATED_SYNTAX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrWarnDeprecatedSyntax));
/// Source `sessionctx/variable.ErrSnapshotTooOld` (`pkg/sessionctx/variable/error.go:26`).
pub static ERR_SNAPSHOT_TOO_OLD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrSnapshotTooOld));
/// Source `sessionctx/variable.ErrUnsupportedValueForVar` (`pkg/sessionctx/variable/error.go:27`).
pub static ERR_UNSUPPORTED_VALUE_FOR_VAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrUnsupportedValueForVar));
/// Source `sessionctx/variable.ErrUnknownSystemVar` (`pkg/sessionctx/variable/error.go:28`).
pub static ERR_UNKNOWN_SYSTEM_VAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrUnknownSystemVariable));
/// Source `sessionctx/variable.ErrIncorrectScope` (`pkg/sessionctx/variable/error.go:29`).
pub static ERR_INCORRECT_SCOPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrIncorrectGlobalLocalVar));
/// Source `sessionctx/variable.ErrUnknownTimeZone` (`pkg/sessionctx/variable/error.go:30`).
pub static ERR_UNKNOWN_TIME_ZONE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrUnknownTimeZone));
/// Source `sessionctx/variable.ErrReadOnly` (`pkg/sessionctx/variable/error.go:31`).
pub static ERR_READ_ONLY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrVariableIsReadonly));
/// Source `sessionctx/variable.ErrWrongValueForVar` (`pkg/sessionctx/variable/error.go:32`).
pub static ERR_WRONG_VALUE_FOR_VAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrWrongValueForVar));
/// Source `sessionctx/variable.ErrWrongTypeForVar` (`pkg/sessionctx/variable/error.go:33`).
pub static ERR_WRONG_TYPE_FOR_VAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrWrongTypeForVar));
/// Source `sessionctx/variable.ErrTruncatedWrongValue` (`pkg/sessionctx/variable/error.go:34`).
pub static ERR_TRUNCATED_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrTruncatedWrongValue));
/// Source `sessionctx/variable.ErrMaxPreparedStmtCountReached` (`pkg/sessionctx/variable/error.go:35`).
pub static ERR_MAX_PREPARED_STMT_COUNT_REACHED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrMaxPreparedStmtCountReached));
/// Source `sessionctx/variable.ErrUnsupportedIsolationLevel` (`pkg/sessionctx/variable/error.go:36`).
pub static ERR_UNSUPPORTED_ISOLATION_LEVEL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrUnsupportedIsolationLevel));
/// Source `sessionctx/variable.errUnknownSystemVariable` (`pkg/sessionctx/variable/error.go:37`).
pub static ERR_UNKNOWN_SYSTEM_VARIABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrUnknownSystemVariable));
/// Source `sessionctx/variable.errGlobalVariable` (`pkg/sessionctx/variable/error.go:38`).
pub static ERR_GLOBAL_VARIABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrGlobalVariable));
/// Source `sessionctx/variable.errLocalVariable` (`pkg/sessionctx/variable/error.go:39`).
pub static ERR_LOCAL_VARIABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrLocalVariable));
/// Source `sessionctx/variable.errValueNotSupportedWhen` (`pkg/sessionctx/variable/error.go:40`).
pub static ERR_VALUE_NOT_SUPPORTED_WHEN: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Variable,
        errcode::ErrNotSupportedYet,
        "%s = OFF is not supported when %s = ON",
    )
});
/// Source `sessionctx/variable.errSetGlobalMaxAllowedPacket` (`pkg/sessionctx/variable/error.go:41`).
pub static ERR_SET_GLOBAL_MAX_ALLOWED_PACKET: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Variable,
        errcode::ErrNotSupportedYet,
        "SET GLOBAL max_allowed_packet is not supported in starter deployment mode",
    )
});
/// Source `sessionctx/variable.ErrNotSupportedInNextGen` (`pkg/sessionctx/variable/error.go:42`).
pub static ERR_NOT_SUPPORTED_IN_NEXT_GEN: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Variable,
        errcode::ErrNotSupportedYet,
        "%s is not supported in the next generation of TiDB",
    )
});
/// Source `sessionctx/variable.ErrNotValidPassword` (`pkg/sessionctx/variable/error.go:43`).
pub static ERR_NOT_VALID_PASSWORD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrNotValidPassword));
/// Source `sessionctx/variable.ErrFunctionsNoopImpl` (`pkg/sessionctx/variable/error.go:47`).
pub static ERR_FUNCTIONS_NOOP_IMPL: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(Class::Variable, errcode::ErrNotSupportedYet, "function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
});
/// Source `sessionctx/variable.ErrVariableNoLongerSupported` (`pkg/sessionctx/variable/error.go:48`).
pub static ERR_VARIABLE_NO_LONGER_SUPPORTED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrVariableNoLongerSupported));
/// Source `sessionctx/variable.ErrInvalidDefaultUTF8MB4Collation` (`pkg/sessionctx/variable/error.go:49`).
pub static ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Variable, errcode::ErrInvalidDefaultUTF8MB4Collation));
/// Source `sessionctx/variable.ErrWarnDeprecatedSyntaxNoReplacement` (`pkg/sessionctx/variable/error.go:50`).
pub static ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Variable,
        errcode::ErrWarnDeprecatedSyntaxNoReplacement,
        "Updating '%s' is deprecated. It will be made read-only in a future release.",
    )
});
/// Source `sessionctx/variable.ErrWarnDeprecatedSyntaxSimpleMsg` (`pkg/sessionctx/variable/error.go:51`).
pub static ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Variable,
        errcode::ErrWarnDeprecatedSyntaxNoReplacement,
        "%s is deprecated and will be removed in a future release.",
    )
});

#[cfg(test)]
mod go_class_code_tests {
    use tidb_errno::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX.class(),
            TerrorClass::Variable,
            "variable::ERR_WARN_DEPRECATED_SYNTAX"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX.code().value(),
            errcode::ErrWarnDeprecatedSyntax as isize,
            "variable::ERR_WARN_DEPRECATED_SYNTAX"
        );
        assert_eq!(
            super::ERR_SNAPSHOT_TOO_OLD.class(),
            TerrorClass::Variable,
            "variable::ERR_SNAPSHOT_TOO_OLD"
        );
        assert_eq!(
            super::ERR_SNAPSHOT_TOO_OLD.code().value(),
            errcode::ErrSnapshotTooOld as isize,
            "variable::ERR_SNAPSHOT_TOO_OLD"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_VALUE_FOR_VAR.class(),
            TerrorClass::Variable,
            "variable::ERR_UNSUPPORTED_VALUE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_VALUE_FOR_VAR
                .code()
                .value(),
            errcode::ErrUnsupportedValueForVar as isize,
            "variable::ERR_UNSUPPORTED_VALUE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_UNKNOWN_SYSTEM_VAR.class(),
            TerrorClass::Variable,
            "variable::ERR_UNKNOWN_SYSTEM_VAR"
        );
        assert_eq!(
            super::ERR_UNKNOWN_SYSTEM_VAR.code().value(),
            errcode::ErrUnknownSystemVariable as isize,
            "variable::ERR_UNKNOWN_SYSTEM_VAR"
        );
        assert_eq!(
            super::ERR_INCORRECT_SCOPE.class(),
            TerrorClass::Variable,
            "variable::ERR_INCORRECT_SCOPE"
        );
        assert_eq!(
            super::ERR_INCORRECT_SCOPE.code().value(),
            errcode::ErrIncorrectGlobalLocalVar as isize,
            "variable::ERR_INCORRECT_SCOPE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_TIME_ZONE.class(),
            TerrorClass::Variable,
            "variable::ERR_UNKNOWN_TIME_ZONE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_TIME_ZONE.code().value(),
            errcode::ErrUnknownTimeZone as isize,
            "variable::ERR_UNKNOWN_TIME_ZONE"
        );
        assert_eq!(
            super::ERR_READ_ONLY.class(),
            TerrorClass::Variable,
            "variable::ERR_READ_ONLY"
        );
        assert_eq!(
            super::ERR_READ_ONLY.code().value(),
            errcode::ErrVariableIsReadonly as isize,
            "variable::ERR_READ_ONLY"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_VAR.class(),
            TerrorClass::Variable,
            "variable::ERR_WRONG_VALUE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_VAR.code().value(),
            errcode::ErrWrongValueForVar as isize,
            "variable::ERR_WRONG_VALUE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_WRONG_TYPE_FOR_VAR.class(),
            TerrorClass::Variable,
            "variable::ERR_WRONG_TYPE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_WRONG_TYPE_FOR_VAR.code().value(),
            errcode::ErrWrongTypeForVar as isize,
            "variable::ERR_WRONG_TYPE_FOR_VAR"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VALUE.class(),
            TerrorClass::Variable,
            "variable::ERR_TRUNCATED_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VALUE.code().value(),
            errcode::ErrTruncatedWrongValue as isize,
            "variable::ERR_TRUNCATED_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_MAX_PREPARED_STMT_COUNT_REACHED.class(),
            TerrorClass::Variable,
            "variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED"
        );
        assert_eq!(
            super::ERR_MAX_PREPARED_STMT_COUNT_REACHED
                .code()
                .value(),
            errcode::ErrMaxPreparedStmtCountReached as isize,
            "variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_ISOLATION_LEVEL.class(),
            TerrorClass::Variable,
            "variable::ERR_UNSUPPORTED_ISOLATION_LEVEL"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_ISOLATION_LEVEL
                .code()
                .value(),
            errcode::ErrUnsupportedIsolationLevel as isize,
            "variable::ERR_UNSUPPORTED_ISOLATION_LEVEL"
        );
        assert_eq!(
            super::ERR_UNKNOWN_SYSTEM_VARIABLE.class(),
            TerrorClass::Variable,
            "variable::ERR_UNKNOWN_SYSTEM_VARIABLE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_SYSTEM_VARIABLE
                .code()
                .value(),
            errcode::ErrUnknownSystemVariable as isize,
            "variable::ERR_UNKNOWN_SYSTEM_VARIABLE"
        );
        assert_eq!(
            super::ERR_GLOBAL_VARIABLE.class(),
            TerrorClass::Variable,
            "variable::ERR_GLOBAL_VARIABLE"
        );
        assert_eq!(
            super::ERR_GLOBAL_VARIABLE.code().value(),
            errcode::ErrGlobalVariable as isize,
            "variable::ERR_GLOBAL_VARIABLE"
        );
        assert_eq!(
            super::ERR_LOCAL_VARIABLE.class(),
            TerrorClass::Variable,
            "variable::ERR_LOCAL_VARIABLE"
        );
        assert_eq!(
            super::ERR_LOCAL_VARIABLE.code().value(),
            errcode::ErrLocalVariable as isize,
            "variable::ERR_LOCAL_VARIABLE"
        );
        assert_eq!(
            super::ERR_VALUE_NOT_SUPPORTED_WHEN.class(),
            TerrorClass::Variable,
            "variable::ERR_VALUE_NOT_SUPPORTED_WHEN"
        );
        assert_eq!(
            super::ERR_VALUE_NOT_SUPPORTED_WHEN
                .code()
                .value(),
            errcode::ErrNotSupportedYet as isize,
            "variable::ERR_VALUE_NOT_SUPPORTED_WHEN"
        );
        assert_eq!(
            super::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET.class(),
            TerrorClass::Variable,
            "variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET"
        );
        assert_eq!(
            super::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET
                .code()
                .value(),
            errcode::ErrNotSupportedYet as isize,
            "variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET"
        );
        assert_eq!(
            super::ERR_NOT_SUPPORTED_IN_NEXT_GEN.class(),
            TerrorClass::Variable,
            "variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN"
        );
        assert_eq!(
            super::ERR_NOT_SUPPORTED_IN_NEXT_GEN
                .code()
                .value(),
            errcode::ErrNotSupportedYet as isize,
            "variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN"
        );
        assert_eq!(
            super::ERR_NOT_VALID_PASSWORD.class(),
            TerrorClass::Executor,
            "variable::ERR_NOT_VALID_PASSWORD"
        );
        assert_eq!(
            super::ERR_NOT_VALID_PASSWORD.code().value(),
            errcode::ErrNotValidPassword as isize,
            "variable::ERR_NOT_VALID_PASSWORD"
        );
        assert_eq!(
            super::ERR_FUNCTIONS_NOOP_IMPL.class(),
            TerrorClass::Variable,
            "variable::ERR_FUNCTIONS_NOOP_IMPL"
        );
        assert_eq!(
            super::ERR_FUNCTIONS_NOOP_IMPL.code().value(),
            errcode::ErrNotSupportedYet as isize,
            "variable::ERR_FUNCTIONS_NOOP_IMPL"
        );
        assert_eq!(
            super::ERR_VARIABLE_NO_LONGER_SUPPORTED.class(),
            TerrorClass::Variable,
            "variable::ERR_VARIABLE_NO_LONGER_SUPPORTED"
        );
        assert_eq!(
            super::ERR_VARIABLE_NO_LONGER_SUPPORTED
                .code()
                .value(),
            errcode::ErrVariableNoLongerSupported as isize,
            "variable::ERR_VARIABLE_NO_LONGER_SUPPORTED"
        );
        assert_eq!(
            super::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION.class(),
            TerrorClass::Variable,
            "variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION"
        );
        assert_eq!(
            super::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION
                .code()
                .value(),
            errcode::ErrInvalidDefaultUTF8MB4Collation as isize,
            "variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
            TerrorClass::Variable,
            "variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT
                .code()
                .value(),
            errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
            "variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG.class(),
            TerrorClass::Variable,
            "variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG
                .code()
                .value(),
            errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
            "variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG"
        );
    }
}
