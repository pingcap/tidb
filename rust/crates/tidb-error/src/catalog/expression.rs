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

use super::{plain_err, std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

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
