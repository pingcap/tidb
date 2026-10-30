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

use crate::{Columns, ErrorLevel, EvalError};
use crate::EvalErrorSql;

/// Source `expression.errCannotConvertString` (`pkg/expression/builtin_convert_charset.go:43`).
pub static ERR_CANNOT_CONVERT_STRING: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrCannotConvertString));
/// Source `expression.ErrIncorrectParameterCount` (`pkg/expression/errors.go:28`).
pub static ERR_INCORRECT_PARAMETER_COUNT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrWrongParamcountToNativeFct));
/// Source `expression.ErrDivisionByZero` (`pkg/expression/errors.go:29`).
pub static ERR_DIVISION_BY_ZERO: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrDivisionByZero));
/// Source `expression.ErrRegexp` (`pkg/expression/errors.go:30`).
pub static ERR_REGEXP: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrRegexp));
/// Source `expression.ErrOperandColumns` (`pkg/expression/errors.go:31`).
pub static ERR_OPERAND_COLUMNS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrOperandColumns));
/// Source `expression.ErrCutValueGroupConcat` (`pkg/expression/errors.go:32`).
pub static ERR_CUT_VALUE_GROUP_CONCAT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrCutValueGroupConcat));
/// Source `expression.ErrFunctionsNoopImpl` (`pkg/expression/errors.go:33`).
pub static ERR_FUNCTIONS_NOOP_IMPL: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(Class::Expression, errcode::ErrNotSupportedYet, "function %s has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
});
/// Source `expression.ErrInvalidArgumentForLogarithm` (`pkg/expression/errors.go:34`).
pub static ERR_INVALID_ARGUMENT_FOR_LOGARITHM: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrInvalidArgumentForLogarithm));
/// Source `expression.ErrIncorrectType` (`pkg/expression/errors.go:35`).
pub static ERR_INCORRECT_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrIncorrectType));
/// Source `expression.ErrInvalidTypeForJSON` (`pkg/expression/errors.go:36`).
pub static ERR_INVALID_TYPE_FOR_JSON: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrInvalidTypeForJSON));
/// Source `expression.ErrInvalidTableSample` (`pkg/expression/errors.go:37`).
pub static ERR_INVALID_TABLE_SAMPLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrInvalidTableSample));
/// Source `expression.ErrNotSupportedYet` (`pkg/expression/errors.go:38`).
pub static ERR_NOT_SUPPORTED_YET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrNotSupportedYet));
/// Source `expression.ErrInvalidJSONForFuncIndex` (`pkg/expression/errors.go:39`).
pub static ERR_INVALID_JSON_FOR_FUNC_INDEX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrInvalidJSONValueForFuncIndex));
/// Source `expression.ErrDataOutOfRangeFuncIndex` (`pkg/expression/errors.go:40`).
pub static ERR_DATA_OUT_OF_RANGE_FUNC_INDEX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrDataOutOfRangeFunctionalIndex));
/// Source `expression.ErrFuncIndexDataIsTooLong` (`pkg/expression/errors.go:41`).
pub static ERR_FUNC_INDEX_DATA_IS_TOO_LONG: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrFunctionalIndexDataIsTooLong));
/// Source `expression.ErrFunctionNotExists` (`pkg/expression/errors.go:42`).
pub static ERR_FUNCTION_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrSpDoesNotExist));
/// Source `expression.errZlibZData` (`pkg/expression/errors.go:45`).
pub static ERR_ZLIB_Z_DATA: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrZlibZData));
/// Source `expression.errZlibZBuf` (`pkg/expression/errors.go:46`).
pub static ERR_ZLIB_Z_BUF: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrZlibZBuf));
/// Source `expression.errIncorrectArgs` (`pkg/expression/errors.go:47`).
pub static ERR_INCORRECT_ARGS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrWrongArguments));
/// Source `expression.errUnknownCharacterSet` (`pkg/expression/errors.go:48`).
pub static ERR_UNKNOWN_CHARACTER_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrUnknownCharacterSet));
/// Source `expression.errDefaultValue` (`pkg/expression/errors.go:49`).
pub static ERR_DEFAULT_VALUE: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Expression,
        errcode::ErrInvalidDefault,
        "invalid default value",
    )
});
/// Source `expression.errDeprecatedSyntaxNoReplacement` (`pkg/expression/errors.go:50`).
pub static ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err(
        Class::Expression,
        errcode::ErrWarnDeprecatedSyntaxNoReplacement,
    )
});
/// Source `expression.errWarnAllowedPacketOverflowed` (`pkg/expression/errors.go:51`).
pub static ERR_WARN_ALLOWED_PACKET_OVERFLOWED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrWarnAllowedPacketOverflowed));
/// Source `expression.errWarnOptionIgnored` (`pkg/expression/errors.go:52`).
pub static ERR_WARN_OPTION_IGNORED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::WarnOptionIgnored));
/// Source `expression.errTruncatedWrongValue` (`pkg/expression/errors.go:53`).
pub static ERR_TRUNCATED_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrTruncatedWrongValue));
/// Source `expression.errUnknownLocale` (`pkg/expression/errors.go:54`).
pub static ERR_UNKNOWN_LOCALE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrUnknownLocale));
/// Source `expression.errNonUniq` (`pkg/expression/errors.go:55`).
pub static ERR_NON_UNIQ: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrNonUniq));
/// Source `expression.errWrongValueForType` (`pkg/expression/errors.go:56`).
pub static ERR_WRONG_VALUE_FOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrWrongValueForType));
/// Source `expression.errUnknown` (`pkg/expression/errors.go:57`).
pub static ERR_UNKNOWN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrUnknown));
/// Source `expression.errSpecificAccessDenied` (`pkg/expression/errors.go:58`).
pub static ERR_SPECIFIC_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrSpecificAccessDenied));
/// Source `expression.errUserLockDeadlock` (`pkg/expression/errors.go:59`).
pub static ERR_USER_LOCK_DEADLOCK: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrUserLockDeadlock));
/// Source `expression.errUserLockWrongName` (`pkg/expression/errors.go:60`).
pub static ERR_USER_LOCK_WRONG_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrUserLockWrongName));
/// Source `expression.errJSONInBooleanContext` (`pkg/expression/errors.go:61`).
pub static ERR_JSON_IN_BOOLEAN_CONTEXT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrJSONInBooleanContext));
/// Source `expression.errBadNull` (`pkg/expression/errors.go:62`).
pub static ERR_BAD_NULL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrBadNull));
/// Source `expression.errSequenceAccessDenied` (`pkg/expression/errors.go:65`).
pub static ERR_SEQUENCE_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Expression, errcode::ErrTableaccessDenied));
/// Source `expression.errUnsupportedJSONComparison` (`pkg/expression/errors.go:66`).
pub static ERR_UNSUPPORTED_JSON_COMPARISON: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Expression,
        errcode::ErrNotSupportedYet,
        "comparison of JSON in the LEAST and GREATEST operators",
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
            super::ERR_CANNOT_CONVERT_STRING.class(),
            TerrorClass::Expression,
            "expression::ERR_CANNOT_CONVERT_STRING"
        );
        assert_eq!(
            super::ERR_CANNOT_CONVERT_STRING
                .code()
                .value(),
            errcode::ErrCannotConvertString as isize,
            "expression::ERR_CANNOT_CONVERT_STRING"
        );
        assert_eq!(
            super::ERR_INCORRECT_PARAMETER_COUNT.class(),
            TerrorClass::Expression,
            "expression::ERR_INCORRECT_PARAMETER_COUNT"
        );
        assert_eq!(
            super::ERR_INCORRECT_PARAMETER_COUNT
                .code()
                .value(),
            errcode::ErrWrongParamcountToNativeFct as isize,
            "expression::ERR_INCORRECT_PARAMETER_COUNT"
        );
        assert_eq!(
            super::ERR_DIVISION_BY_ZERO.class(),
            TerrorClass::Expression,
            "expression::ERR_DIVISION_BY_ZERO"
        );
        assert_eq!(
            super::ERR_DIVISION_BY_ZERO.code().value(),
            errcode::ErrDivisionByZero as isize,
            "expression::ERR_DIVISION_BY_ZERO"
        );
        assert_eq!(
            super::ERR_REGEXP.class(),
            TerrorClass::Expression,
            "expression::ERR_REGEXP"
        );
        assert_eq!(
            super::ERR_REGEXP.code().value(),
            errcode::ErrRegexp as isize,
            "expression::ERR_REGEXP"
        );
        assert_eq!(
            super::ERR_OPERAND_COLUMNS.class(),
            TerrorClass::Expression,
            "expression::ERR_OPERAND_COLUMNS"
        );
        assert_eq!(
            super::ERR_OPERAND_COLUMNS.code().value(),
            errcode::ErrOperandColumns as isize,
            "expression::ERR_OPERAND_COLUMNS"
        );
        assert_eq!(
            super::ERR_CUT_VALUE_GROUP_CONCAT.class(),
            TerrorClass::Expression,
            "expression::ERR_CUT_VALUE_GROUP_CONCAT"
        );
        assert_eq!(
            super::ERR_CUT_VALUE_GROUP_CONCAT
                .code()
                .value(),
            errcode::ErrCutValueGroupConcat as isize,
            "expression::ERR_CUT_VALUE_GROUP_CONCAT"
        );
        assert_eq!(
            super::ERR_FUNCTIONS_NOOP_IMPL.class(),
            TerrorClass::Expression,
            "expression::ERR_FUNCTIONS_NOOP_IMPL"
        );
        assert_eq!(
            super::ERR_FUNCTIONS_NOOP_IMPL.code().value(),
            errcode::ErrNotSupportedYet as isize,
            "expression::ERR_FUNCTIONS_NOOP_IMPL"
        );
        assert_eq!(
            super::ERR_INVALID_ARGUMENT_FOR_LOGARITHM.class(),
            TerrorClass::Expression,
            "expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM"
        );
        assert_eq!(
            super::ERR_INVALID_ARGUMENT_FOR_LOGARITHM
                .code()
                .value(),
            errcode::ErrInvalidArgumentForLogarithm as isize,
            "expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM"
        );
        assert_eq!(
            super::ERR_INCORRECT_TYPE.class(),
            TerrorClass::Expression,
            "expression::ERR_INCORRECT_TYPE"
        );
        assert_eq!(
            super::ERR_INCORRECT_TYPE.code().value(),
            errcode::ErrIncorrectType as isize,
            "expression::ERR_INCORRECT_TYPE"
        );
        assert_eq!(
            super::ERR_INVALID_TYPE_FOR_JSON.class(),
            TerrorClass::Expression,
            "expression::ERR_INVALID_TYPE_FOR_JSON"
        );
        assert_eq!(
            super::ERR_INVALID_TYPE_FOR_JSON
                .code()
                .value(),
            errcode::ErrInvalidTypeForJSON as isize,
            "expression::ERR_INVALID_TYPE_FOR_JSON"
        );
        assert_eq!(
            super::ERR_INVALID_TABLE_SAMPLE.class(),
            TerrorClass::Expression,
            "expression::ERR_INVALID_TABLE_SAMPLE"
        );
        assert_eq!(
            super::ERR_INVALID_TABLE_SAMPLE.code().value(),
            errcode::ErrInvalidTableSample as isize,
            "expression::ERR_INVALID_TABLE_SAMPLE"
        );
        assert_eq!(
            super::ERR_NOT_SUPPORTED_YET.class(),
            TerrorClass::Expression,
            "expression::ERR_NOT_SUPPORTED_YET"
        );
        assert_eq!(
            super::ERR_NOT_SUPPORTED_YET.code().value(),
            errcode::ErrNotSupportedYet as isize,
            "expression::ERR_NOT_SUPPORTED_YET"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_FOR_FUNC_INDEX.class(),
            TerrorClass::Expression,
            "expression::ERR_INVALID_JSON_FOR_FUNC_INDEX"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_FOR_FUNC_INDEX
                .code()
                .value(),
            errcode::ErrInvalidJSONValueForFuncIndex as isize,
            "expression::ERR_INVALID_JSON_FOR_FUNC_INDEX"
        );
        assert_eq!(
            super::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX.class(),
            TerrorClass::Expression,
            "expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX"
        );
        assert_eq!(
            super::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX
                .code()
                .value(),
            errcode::ErrDataOutOfRangeFunctionalIndex as isize,
            "expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX"
        );
        assert_eq!(
            super::ERR_FUNC_INDEX_DATA_IS_TOO_LONG.class(),
            TerrorClass::Expression,
            "expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG"
        );
        assert_eq!(
            super::ERR_FUNC_INDEX_DATA_IS_TOO_LONG
                .code()
                .value(),
            errcode::ErrFunctionalIndexDataIsTooLong as isize,
            "expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG"
        );
        assert_eq!(
            super::ERR_FUNCTION_NOT_EXISTS.class(),
            TerrorClass::Expression,
            "expression::ERR_FUNCTION_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_FUNCTION_NOT_EXISTS.code().value(),
            errcode::ErrSpDoesNotExist as isize,
            "expression::ERR_FUNCTION_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_ZLIB_Z_DATA.class(),
            TerrorClass::Expression,
            "expression::ERR_ZLIB_Z_DATA"
        );
        assert_eq!(
            super::ERR_ZLIB_Z_DATA.code().value(),
            errcode::ErrZlibZData as isize,
            "expression::ERR_ZLIB_Z_DATA"
        );
        assert_eq!(
            super::ERR_ZLIB_Z_BUF.class(),
            TerrorClass::Expression,
            "expression::ERR_ZLIB_Z_BUF"
        );
        assert_eq!(
            super::ERR_ZLIB_Z_BUF.code().value(),
            errcode::ErrZlibZBuf as isize,
            "expression::ERR_ZLIB_Z_BUF"
        );
        assert_eq!(
            super::ERR_INCORRECT_ARGS.class(),
            TerrorClass::Expression,
            "expression::ERR_INCORRECT_ARGS"
        );
        assert_eq!(
            super::ERR_INCORRECT_ARGS.code().value(),
            errcode::ErrWrongArguments as isize,
            "expression::ERR_INCORRECT_ARGS"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET.class(),
            TerrorClass::Expression,
            "expression::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET
                .code()
                .value(),
            errcode::ErrUnknownCharacterSet as isize,
            "expression::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_DEFAULT_VALUE.class(),
            TerrorClass::Expression,
            "expression::ERR_DEFAULT_VALUE"
        );
        assert_eq!(
            super::ERR_DEFAULT_VALUE.code().value(),
            errcode::ErrInvalidDefault as isize,
            "expression::ERR_DEFAULT_VALUE"
        );
        assert_eq!(
            super::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
            TerrorClass::Expression,
            "expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT
                .code()
                .value(),
            errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
            "expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_WARN_ALLOWED_PACKET_OVERFLOWED.class(),
            TerrorClass::Expression,
            "expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED"
        );
        assert_eq!(
            super::ERR_WARN_ALLOWED_PACKET_OVERFLOWED
                .code()
                .value(),
            errcode::ErrWarnAllowedPacketOverflowed as isize,
            "expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED"
        );
        assert_eq!(
            super::ERR_WARN_OPTION_IGNORED.class(),
            TerrorClass::Expression,
            "expression::ERR_WARN_OPTION_IGNORED"
        );
        assert_eq!(
            super::ERR_WARN_OPTION_IGNORED.code().value(),
            errcode::WarnOptionIgnored as isize,
            "expression::ERR_WARN_OPTION_IGNORED"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VALUE.class(),
            TerrorClass::Expression,
            "expression::ERR_TRUNCATED_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VALUE
                .code()
                .value(),
            errcode::ErrTruncatedWrongValue as isize,
            "expression::ERR_TRUNCATED_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_LOCALE.class(),
            TerrorClass::Expression,
            "expression::ERR_UNKNOWN_LOCALE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_LOCALE.code().value(),
            errcode::ErrUnknownLocale as isize,
            "expression::ERR_UNKNOWN_LOCALE"
        );
        assert_eq!(
            super::ERR_NON_UNIQ.class(),
            TerrorClass::Expression,
            "expression::ERR_NON_UNIQ"
        );
        assert_eq!(
            super::ERR_NON_UNIQ.code().value(),
            errcode::ErrNonUniq as isize,
            "expression::ERR_NON_UNIQ"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_TYPE.class(),
            TerrorClass::Expression,
            "expression::ERR_WRONG_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_TYPE.code().value(),
            errcode::ErrWrongValueForType as isize,
            "expression::ERR_WRONG_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_UNKNOWN.class(),
            TerrorClass::Expression,
            "expression::ERR_UNKNOWN"
        );
        assert_eq!(
            super::ERR_UNKNOWN.code().value(),
            errcode::ErrUnknown as isize,
            "expression::ERR_UNKNOWN"
        );
        assert_eq!(
            super::ERR_SPECIFIC_ACCESS_DENIED.class(),
            TerrorClass::Expression,
            "expression::ERR_SPECIFIC_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_SPECIFIC_ACCESS_DENIED
                .code()
                .value(),
            errcode::ErrSpecificAccessDenied as isize,
            "expression::ERR_SPECIFIC_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_USER_LOCK_DEADLOCK.class(),
            TerrorClass::Expression,
            "expression::ERR_USER_LOCK_DEADLOCK"
        );
        assert_eq!(
            super::ERR_USER_LOCK_DEADLOCK.code().value(),
            errcode::ErrUserLockDeadlock as isize,
            "expression::ERR_USER_LOCK_DEADLOCK"
        );
        assert_eq!(
            super::ERR_USER_LOCK_WRONG_NAME.class(),
            TerrorClass::Expression,
            "expression::ERR_USER_LOCK_WRONG_NAME"
        );
        assert_eq!(
            super::ERR_USER_LOCK_WRONG_NAME.code().value(),
            errcode::ErrUserLockWrongName as isize,
            "expression::ERR_USER_LOCK_WRONG_NAME"
        );
        assert_eq!(
            super::ERR_JSON_IN_BOOLEAN_CONTEXT.class(),
            TerrorClass::Expression,
            "expression::ERR_JSON_IN_BOOLEAN_CONTEXT"
        );
        assert_eq!(
            super::ERR_JSON_IN_BOOLEAN_CONTEXT
                .code()
                .value(),
            errcode::ErrJSONInBooleanContext as isize,
            "expression::ERR_JSON_IN_BOOLEAN_CONTEXT"
        );
        assert_eq!(
            super::ERR_BAD_NULL.class(),
            TerrorClass::Expression,
            "expression::ERR_BAD_NULL"
        );
        assert_eq!(
            super::ERR_BAD_NULL.code().value(),
            errcode::ErrBadNull as isize,
            "expression::ERR_BAD_NULL"
        );
        assert_eq!(
            super::ERR_SEQUENCE_ACCESS_DENIED.class(),
            TerrorClass::Expression,
            "expression::ERR_SEQUENCE_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_SEQUENCE_ACCESS_DENIED
                .code()
                .value(),
            errcode::ErrTableaccessDenied as isize,
            "expression::ERR_SEQUENCE_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_JSON_COMPARISON.class(),
            TerrorClass::Expression,
            "expression::ERR_UNSUPPORTED_JSON_COMPARISON"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_JSON_COMPARISON
                .code()
                .value(),
            errcode::ErrNotSupportedYet as isize,
            "expression::ERR_UNSUPPORTED_JSON_COMPARISON"
        );
    }
}

// Go `pkg/expression`'s error-level helpers over an `EvalContext`
// (`errors.go`'s `handleDivisionByZeroError` / `handleAllowedPacketOverflowed`,
// and the `TypeCtx().HandleTruncate` sites), free functions as in Go.

/// Go `types.Context.HandleTruncate`: applies this statement's level to a
/// value that lost information during conversion. The best-effort value
/// the caller already computed stands either way -- Go returns the
/// scanned prefix from `getValidIntPrefix` before this is consulted -- so
/// only whether the statement survives, and whether the client is told,
/// differ.
pub fn handle_truncate<C: Columns + ?Sized>(ctx: &C, message: &str) -> Result<(), EvalError> {
    let error = EvalError::TruncatedWrongValue(message.to_owned());
    match ctx.truncate_level() {
        ErrorLevel::Ignore => Ok(()),
        ErrorLevel::Warn => {
            ctx.append_error_warning(&error.to_sql_error());
            Ok(())
        }
        ErrorLevel::Error => Err(error),
    }
}

/// Applies this statement's truncate policy to a `GROUP_CONCAT` value
/// that exceeded `group_concat_max_len`.
pub fn handle_group_concat_cut<C: Columns + ?Sized>(ctx: &C, message: &str) -> Result<(), EvalError> {
    match ctx.truncate_level() {
        ErrorLevel::Ignore => Ok(()),
        ErrorLevel::Warn => {
            ctx.append_error_warning(&EvalError::GroupConcatCut(message.to_owned()).to_sql_error());
            Ok(())
        }
        ErrorLevel::Error => Err(EvalError::GroupConcatCut(message.to_owned())),
    }
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
pub fn handle_allowed_packet_overflowed<C: Columns + ?Sized>(ctx: &C, expr_name: &str) -> Result<(), EvalError> {
    let message = format!(
        "Result of {expr_name}() was larger than max_allowed_packet ({}) - truncated",
        ctx.max_allowed_packet()
    );
    match ctx.truncate_level() {
        // `IgnoreTruncateErr` is Go's first disjunct, which warns as well
        // -- the source appends unconditionally on that arm.
        ErrorLevel::Ignore | ErrorLevel::Warn => {
            ctx.append_error_warning(
                &EvalError::AllowedPacketOverflowed(message.clone()).to_sql_error(),
            );
            Ok(())
        }
        ErrorLevel::Error => Err(EvalError::AllowedPacketOverflowed(message)),
    }
}

/// Go `handleDivisionByZeroError`: applies this statement's level to a
/// zero divisor. The value the caller returns is `NULL` either way; only
/// whether the statement survives differs.
pub fn handle_division_by_zero<C: Columns + ?Sized>(ctx: &C) -> Result<(), EvalError> {
    match ctx.division_by_zero_level() {
        ErrorLevel::Ignore => Ok(()),
        ErrorLevel::Warn => {
            ctx.append_error_warning(&EvalError::DivisionByZero.to_sql_error());
            Ok(())
        }
        ErrorLevel::Error => Err(EvalError::DivisionByZero),
    }
}

/// Go `builtinSleepSig`'s `ec.HandleErrorWithAlias(errBadNull, errIncorrectArgs,
/// errIncorrectArgs.FastGenByArgs("sleep"))` for `SLEEP(NULL)` and a negative
/// duration: the statement's `ErrGroupBadNull` level decides.
pub fn handle_sleep_incorrect_argument<C: Columns + ?Sized>(ctx: &C) -> Result<(), EvalError> {
    let error = EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned());
    match ctx.bad_null_level() {
        ErrorLevel::Ignore => Ok(()),
        ErrorLevel::Warn => {
            ctx.append_error_warning(&error.to_sql_error());
            Ok(())
        }
        ErrorLevel::Error => Err(error),
    }
}
