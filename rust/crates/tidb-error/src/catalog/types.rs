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

use super::{std_err, std_err_msg, Class};
use crate::terror::TerrorError;
use crate::tidb::errcode;

/// Source `types.ErrInvalidJSONText` (`pkg/types/json_constants.go:222`).
pub static ERR_INVALID_JSON_TEXT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONText));
/// Source `types.ErrInvalidJSONType` (`pkg/types/json_constants.go:224`).
pub static ERR_INVALID_JSON_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONType));
/// Source `types.ErrInvalidJSONTextInParam` (`pkg/types/json_constants.go:226`).
pub static ERR_INVALID_JSON_TEXT_IN_PARAM: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONTextInParam));
/// Source `types.ErrInvalidJSONPath` (`pkg/types/json_constants.go:228`).
pub static ERR_INVALID_JSON_PATH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONPath));
/// Source `types.ErrInvalidJSONCharset` (`pkg/types/json_constants.go:230`).
pub static ERR_INVALID_JSON_CHARSET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONCharset));
/// Source `types.ErrInvalidJSONData` (`pkg/types/json_constants.go:232`).
pub static ERR_INVALID_JSON_DATA: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONData));
/// Source `types.ErrInvalidJSONPathMultipleSelection` (`pkg/types/json_constants.go:234`).
pub static ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONPathMultipleSelection));
/// Source `types.ErrInvalidJSONContainsPathType` (`pkg/types/json_constants.go:236`).
pub static ERR_INVALID_JSON_CONTAINS_PATH_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONContainsPathType));
/// Source `types.ErrJSONDocumentNULLKey` (`pkg/types/json_constants.go:238`).
pub static ERR_JSON_DOCUMENT_NULL_KEY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrJSONDocumentNULLKey));
/// Source `types.ErrJSONDocumentTooDeep` (`pkg/types/json_constants.go:240`).
pub static ERR_JSON_DOCUMENT_TOO_DEEP: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrJSONDocumentTooDeep));
/// Source `types.ErrJSONObjectKeyTooLong` (`pkg/types/json_constants.go:242`).
pub static ERR_JSON_OBJECT_KEY_TOO_LONG: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Types,
        errcode::ErrJSONObjectKeyTooLong,
        errcode::ErrJSONObjectKeyTooLong,
        "",
    )
});
/// Source `types.ErrInvalidJSONPathArrayCell` (`pkg/types/json_constants.go:244`).
pub static ERR_INVALID_JSON_PATH_ARRAY_CELL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrInvalidJSONPathArrayCell));
/// Source `types.ErrUnsupportedSecondArgumentType` (`pkg/types/json_constants.go:246`).
pub static ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Json, errcode::ErrUnsupportedSecondArgumentType));
/// Source `types.ErrDataTooLong` (`pkg/types/errors.go:35`).
pub static ERR_DATA_TOO_LONG: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDataTooLong));
/// Source `types.ErrIllegalValueForType` (`pkg/types/errors.go:37`).
pub static ERR_ILLEGAL_VALUE_FOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrIllegalValueForType));
/// Source `types.ErrTruncated` (`pkg/types/errors.go:39`).
pub static ERR_TRUNCATED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::WarnDataTruncated));
/// Source `types.ErrOverflow` (`pkg/types/errors.go:41`).
pub static ERR_OVERFLOW: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDataOutOfRange));
/// Source `types.ErrDivByZero` (`pkg/types/errors.go:43`).
pub static ERR_DIV_BY_ZERO: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDivisionByZero));
/// Source `types.ErrTooBigDisplayWidth` (`pkg/types/errors.go:45`).
pub static ERR_TOO_BIG_DISPLAY_WIDTH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTooBigDisplaywidth));
/// Source `types.ErrTooBigFieldLength` (`pkg/types/errors.go:47`).
pub static ERR_TOO_BIG_FIELD_LENGTH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTooBigFieldlength));
/// Source `types.ErrTooBigSet` (`pkg/types/errors.go:49`).
pub static ERR_TOO_BIG_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTooBigSet));
/// Source `types.ErrTooBigScale` (`pkg/types/errors.go:51`).
pub static ERR_TOO_BIG_SCALE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTooBigScale));
/// Source `types.ErrTooBigPrecision` (`pkg/types/errors.go:53`).
pub static ERR_TOO_BIG_PRECISION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTooBigPrecision));
/// Source `types.ErrBadNumber` (`pkg/types/errors.go:55`).
pub static ERR_BAD_NUMBER: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrBadNumber));
/// Source `types.ErrInvalidFieldSize` (`pkg/types/errors.go:57`).
pub static ERR_INVALID_FIELD_SIZE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrInvalidFieldSize));
/// Source `types.ErrMBiggerThanD` (`pkg/types/errors.go:59`).
pub static ERR_M_BIGGER_THAN_D: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrMBiggerThanD));
/// Source `types.ErrWarnDataOutOfRange` (`pkg/types/errors.go:62`).
pub static ERR_WARN_DATA_OUT_OF_RANGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrWarnDataOutOfRange));
/// Source `types.ErrDuplicatedValueInType` (`pkg/types/errors.go:64`).
pub static ERR_DUPLICATED_VALUE_IN_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDuplicatedValueInType));
/// Source `types.ErrDatetimeFunctionOverflow` (`pkg/types/errors.go:66`).
pub static ERR_DATETIME_FUNCTION_OVERFLOW: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDatetimeFunctionOverflow));
/// Source `types.ErrCastAsSignedOverflow` (`pkg/types/errors.go:68`).
pub static ERR_CAST_AS_SIGNED_OVERFLOW: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrCastAsSignedOverflow));
/// Source `types.ErrCastNegIntAsUnsigned` (`pkg/types/errors.go:70`).
pub static ERR_CAST_NEG_INT_AS_UNSIGNED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrCastNegIntAsUnsigned));
/// Source `types.ErrInvalidYearFormat` (`pkg/types/errors.go:72`).
pub static ERR_INVALID_YEAR_FORMAT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrInvalidYearFormat));
/// Source `types.ErrInvalidYear` (`pkg/types/errors.go:74`).
pub static ERR_INVALID_YEAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrInvalidYear));
/// Source `types.ErrTruncatedWrongVal` (`pkg/types/errors.go:76`).
pub static ERR_TRUNCATED_WRONG_VAL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTruncatedWrongValue));
/// Source `types.ErrInvalidWeekModeFormat` (`pkg/types/errors.go:78`).
pub static ERR_INVALID_WEEK_MODE_FORMAT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrInvalidWeekModeFormat));
/// Source `types.ErrWrongFieldSpec` (`pkg/types/errors.go:80`).
pub static ERR_WRONG_FIELD_SPEC: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrWrongFieldSpec));
/// Source `types.ErrSyntax` (`pkg/types/errors.go:82`).
pub static ERR_SYNTAX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err_msg(Class::Types, errcode::ErrParse, errcode::ErrSyntax, ""));
/// Source `types.ErrWrongValue` (`pkg/types/errors.go:84`).
pub static ERR_WRONG_VALUE: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Types,
        errcode::ErrTruncatedWrongValue,
        errcode::ErrWrongValue,
        "",
    )
});
/// Source `types.ErrWrongValue2` (`pkg/types/errors.go:86`).
pub static ERR_WRONG_VALUE2: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Types,
        errcode::ErrWrongValue,
        errcode::ErrWrongValue,
        "",
    )
});
/// Source `types.ErrWrongValueForType` (`pkg/types/errors.go:88`).
pub static ERR_WRONG_VALUE_FOR_TYPE: LazyLock<TerrorError> = LazyLock::new(|| {
    std_err_msg(
        Class::Types,
        errcode::ErrWrongValueForType,
        errcode::ErrWrongValueForType,
        "",
    )
});
/// Source `types.ErrPartitionStatsMissing` (`pkg/types/errors.go:91`).
pub static ERR_PARTITION_STATS_MISSING: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrPartitionStatsMissing));
/// Source `types.ErrPartitionColumnStatsMissing` (`pkg/types/errors.go:94`).
pub static ERR_PARTITION_COLUMN_STATS_MISSING: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrPartitionColumnStatsMissing));
/// Source `types.ErrIncorrectDatetimeValue` (`pkg/types/errors.go:96`).
pub static ERR_INCORRECT_DATETIME_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrIncorrectDatetimeValue));
/// Source `types.ErrJSONBadOneOrAllArg` (`pkg/types/errors.go:98`).
pub static ERR_JSON_BAD_ONE_OR_ALL_ARG: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrJSONBadOneOrAllArg));
/// Source `types.ErrJSONVacuousPath` (`pkg/types/errors.go:100`).
pub static ERR_JSON_VACUOUS_PATH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrJSONVacuousPath));
/// Source `types.ErrTimestampInDSTTransition` (`pkg/types/errors.go:103`).
pub static ERR_TIMESTAMP_IN_DST_TRANSITION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrTimeStampInDSTTransition));
