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

use tidb_dbterror::catalog_support::{std_err, std_err_msg, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

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
            super::ERR_INVALID_JSON_TEXT.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_TEXT"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_TEXT.code().value(),
            errcode::ErrInvalidJSONText as isize,
            "types::ERR_INVALID_JSON_TEXT"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_TYPE.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_TYPE"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_TYPE.code().value(),
            errcode::ErrInvalidJSONType as isize,
            "types::ERR_INVALID_JSON_TYPE"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_TEXT_IN_PARAM.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_TEXT_IN_PARAM"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_TEXT_IN_PARAM
                .code()
                .value(),
            errcode::ErrInvalidJSONTextInParam as isize,
            "types::ERR_INVALID_JSON_TEXT_IN_PARAM"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_PATH"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH.code().value(),
            errcode::ErrInvalidJSONPath as isize,
            "types::ERR_INVALID_JSON_PATH"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_CHARSET.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_CHARSET"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_CHARSET.code().value(),
            errcode::ErrInvalidJSONCharset as isize,
            "types::ERR_INVALID_JSON_CHARSET"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_DATA.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_DATA"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_DATA.code().value(),
            errcode::ErrInvalidJSONData as isize,
            "types::ERR_INVALID_JSON_DATA"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION
                .code()
                .value(),
            errcode::ErrInvalidJSONPathMultipleSelection as isize,
            "types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_CONTAINS_PATH_TYPE.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_CONTAINS_PATH_TYPE
                .code()
                .value(),
            errcode::ErrInvalidJSONContainsPathType as isize,
            "types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE"
        );
        assert_eq!(
            super::ERR_JSON_DOCUMENT_NULL_KEY.class(),
            TerrorClass::Json,
            "types::ERR_JSON_DOCUMENT_NULL_KEY"
        );
        assert_eq!(
            super::ERR_JSON_DOCUMENT_NULL_KEY.code().value(),
            errcode::ErrJSONDocumentNULLKey as isize,
            "types::ERR_JSON_DOCUMENT_NULL_KEY"
        );
        assert_eq!(
            super::ERR_JSON_DOCUMENT_TOO_DEEP.class(),
            TerrorClass::Json,
            "types::ERR_JSON_DOCUMENT_TOO_DEEP"
        );
        assert_eq!(
            super::ERR_JSON_DOCUMENT_TOO_DEEP.code().value(),
            errcode::ErrJSONDocumentTooDeep as isize,
            "types::ERR_JSON_DOCUMENT_TOO_DEEP"
        );
        assert_eq!(
            super::ERR_JSON_OBJECT_KEY_TOO_LONG.class(),
            TerrorClass::Types,
            "types::ERR_JSON_OBJECT_KEY_TOO_LONG"
        );
        assert_eq!(
            super::ERR_JSON_OBJECT_KEY_TOO_LONG.code().value(),
            errcode::ErrJSONObjectKeyTooLong as isize,
            "types::ERR_JSON_OBJECT_KEY_TOO_LONG"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH_ARRAY_CELL.class(),
            TerrorClass::Json,
            "types::ERR_INVALID_JSON_PATH_ARRAY_CELL"
        );
        assert_eq!(
            super::ERR_INVALID_JSON_PATH_ARRAY_CELL
                .code()
                .value(),
            errcode::ErrInvalidJSONPathArrayCell as isize,
            "types::ERR_INVALID_JSON_PATH_ARRAY_CELL"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE.class(),
            TerrorClass::Json,
            "types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE"
        );
        assert_eq!(
            super::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE
                .code()
                .value(),
            errcode::ErrUnsupportedSecondArgumentType as isize,
            "types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE"
        );
        assert_eq!(
            super::ERR_DATA_TOO_LONG.class(),
            TerrorClass::Types,
            "types::ERR_DATA_TOO_LONG"
        );
        assert_eq!(
            super::ERR_DATA_TOO_LONG.code().value(),
            errcode::ErrDataTooLong as isize,
            "types::ERR_DATA_TOO_LONG"
        );
        assert_eq!(
            super::ERR_ILLEGAL_VALUE_FOR_TYPE.class(),
            TerrorClass::Types,
            "types::ERR_ILLEGAL_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_ILLEGAL_VALUE_FOR_TYPE.code().value(),
            errcode::ErrIllegalValueForType as isize,
            "types::ERR_ILLEGAL_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_TRUNCATED.class(),
            TerrorClass::Types,
            "types::ERR_TRUNCATED"
        );
        assert_eq!(
            super::ERR_TRUNCATED.code().value(),
            errcode::WarnDataTruncated as isize,
            "types::ERR_TRUNCATED"
        );
        assert_eq!(
            super::ERR_OVERFLOW.class(),
            TerrorClass::Types,
            "types::ERR_OVERFLOW"
        );
        assert_eq!(
            super::ERR_OVERFLOW.code().value(),
            errcode::ErrDataOutOfRange as isize,
            "types::ERR_OVERFLOW"
        );
        assert_eq!(
            super::ERR_DIV_BY_ZERO.class(),
            TerrorClass::Types,
            "types::ERR_DIV_BY_ZERO"
        );
        assert_eq!(
            super::ERR_DIV_BY_ZERO.code().value(),
            errcode::ErrDivisionByZero as isize,
            "types::ERR_DIV_BY_ZERO"
        );
        assert_eq!(
            super::ERR_TOO_BIG_DISPLAY_WIDTH.class(),
            TerrorClass::Types,
            "types::ERR_TOO_BIG_DISPLAY_WIDTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_DISPLAY_WIDTH.code().value(),
            errcode::ErrTooBigDisplaywidth as isize,
            "types::ERR_TOO_BIG_DISPLAY_WIDTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_FIELD_LENGTH.class(),
            TerrorClass::Types,
            "types::ERR_TOO_BIG_FIELD_LENGTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_FIELD_LENGTH.code().value(),
            errcode::ErrTooBigFieldlength as isize,
            "types::ERR_TOO_BIG_FIELD_LENGTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_SET.class(),
            TerrorClass::Types,
            "types::ERR_TOO_BIG_SET"
        );
        assert_eq!(
            super::ERR_TOO_BIG_SET.code().value(),
            errcode::ErrTooBigSet as isize,
            "types::ERR_TOO_BIG_SET"
        );
        assert_eq!(
            super::ERR_TOO_BIG_SCALE.class(),
            TerrorClass::Types,
            "types::ERR_TOO_BIG_SCALE"
        );
        assert_eq!(
            super::ERR_TOO_BIG_SCALE.code().value(),
            errcode::ErrTooBigScale as isize,
            "types::ERR_TOO_BIG_SCALE"
        );
        assert_eq!(
            super::ERR_TOO_BIG_PRECISION.class(),
            TerrorClass::Types,
            "types::ERR_TOO_BIG_PRECISION"
        );
        assert_eq!(
            super::ERR_TOO_BIG_PRECISION.code().value(),
            errcode::ErrTooBigPrecision as isize,
            "types::ERR_TOO_BIG_PRECISION"
        );
        assert_eq!(
            super::ERR_BAD_NUMBER.class(),
            TerrorClass::Types,
            "types::ERR_BAD_NUMBER"
        );
        assert_eq!(
            super::ERR_BAD_NUMBER.code().value(),
            errcode::ErrBadNumber as isize,
            "types::ERR_BAD_NUMBER"
        );
        assert_eq!(
            super::ERR_INVALID_FIELD_SIZE.class(),
            TerrorClass::Types,
            "types::ERR_INVALID_FIELD_SIZE"
        );
        assert_eq!(
            super::ERR_INVALID_FIELD_SIZE.code().value(),
            errcode::ErrInvalidFieldSize as isize,
            "types::ERR_INVALID_FIELD_SIZE"
        );
        assert_eq!(
            super::ERR_M_BIGGER_THAN_D.class(),
            TerrorClass::Types,
            "types::ERR_M_BIGGER_THAN_D"
        );
        assert_eq!(
            super::ERR_M_BIGGER_THAN_D.code().value(),
            errcode::ErrMBiggerThanD as isize,
            "types::ERR_M_BIGGER_THAN_D"
        );
        assert_eq!(
            super::ERR_WARN_DATA_OUT_OF_RANGE.class(),
            TerrorClass::Types,
            "types::ERR_WARN_DATA_OUT_OF_RANGE"
        );
        assert_eq!(
            super::ERR_WARN_DATA_OUT_OF_RANGE.code().value(),
            errcode::ErrWarnDataOutOfRange as isize,
            "types::ERR_WARN_DATA_OUT_OF_RANGE"
        );
        assert_eq!(
            super::ERR_DUPLICATED_VALUE_IN_TYPE.class(),
            TerrorClass::Types,
            "types::ERR_DUPLICATED_VALUE_IN_TYPE"
        );
        assert_eq!(
            super::ERR_DUPLICATED_VALUE_IN_TYPE.code().value(),
            errcode::ErrDuplicatedValueInType as isize,
            "types::ERR_DUPLICATED_VALUE_IN_TYPE"
        );
        assert_eq!(
            super::ERR_DATETIME_FUNCTION_OVERFLOW.class(),
            TerrorClass::Types,
            "types::ERR_DATETIME_FUNCTION_OVERFLOW"
        );
        assert_eq!(
            super::ERR_DATETIME_FUNCTION_OVERFLOW
                .code()
                .value(),
            errcode::ErrDatetimeFunctionOverflow as isize,
            "types::ERR_DATETIME_FUNCTION_OVERFLOW"
        );
        assert_eq!(
            super::ERR_CAST_AS_SIGNED_OVERFLOW.class(),
            TerrorClass::Types,
            "types::ERR_CAST_AS_SIGNED_OVERFLOW"
        );
        assert_eq!(
            super::ERR_CAST_AS_SIGNED_OVERFLOW.code().value(),
            errcode::ErrCastAsSignedOverflow as isize,
            "types::ERR_CAST_AS_SIGNED_OVERFLOW"
        );
        assert_eq!(
            super::ERR_CAST_NEG_INT_AS_UNSIGNED.class(),
            TerrorClass::Types,
            "types::ERR_CAST_NEG_INT_AS_UNSIGNED"
        );
        assert_eq!(
            super::ERR_CAST_NEG_INT_AS_UNSIGNED.code().value(),
            errcode::ErrCastNegIntAsUnsigned as isize,
            "types::ERR_CAST_NEG_INT_AS_UNSIGNED"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR_FORMAT.class(),
            TerrorClass::Types,
            "types::ERR_INVALID_YEAR_FORMAT"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR_FORMAT.code().value(),
            errcode::ErrInvalidYearFormat as isize,
            "types::ERR_INVALID_YEAR_FORMAT"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR.class(),
            TerrorClass::Types,
            "types::ERR_INVALID_YEAR"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR.code().value(),
            errcode::ErrInvalidYear as isize,
            "types::ERR_INVALID_YEAR"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VAL.class(),
            TerrorClass::Types,
            "types::ERR_TRUNCATED_WRONG_VAL"
        );
        assert_eq!(
            super::ERR_TRUNCATED_WRONG_VAL.code().value(),
            errcode::ErrTruncatedWrongValue as isize,
            "types::ERR_TRUNCATED_WRONG_VAL"
        );
        assert_eq!(
            super::ERR_INVALID_WEEK_MODE_FORMAT.class(),
            TerrorClass::Types,
            "types::ERR_INVALID_WEEK_MODE_FORMAT"
        );
        assert_eq!(
            super::ERR_INVALID_WEEK_MODE_FORMAT.code().value(),
            errcode::ErrInvalidWeekModeFormat as isize,
            "types::ERR_INVALID_WEEK_MODE_FORMAT"
        );
        assert_eq!(
            super::ERR_WRONG_FIELD_SPEC.class(),
            TerrorClass::Types,
            "types::ERR_WRONG_FIELD_SPEC"
        );
        assert_eq!(
            super::ERR_WRONG_FIELD_SPEC.code().value(),
            errcode::ErrWrongFieldSpec as isize,
            "types::ERR_WRONG_FIELD_SPEC"
        );
        assert_eq!(
            super::ERR_SYNTAX.class(),
            TerrorClass::Types,
            "types::ERR_SYNTAX"
        );
        assert_eq!(
            super::ERR_SYNTAX.code().value(),
            errcode::ErrParse as isize,
            "types::ERR_SYNTAX"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE.class(),
            TerrorClass::Types,
            "types::ERR_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE.code().value(),
            errcode::ErrTruncatedWrongValue as isize,
            "types::ERR_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE2.class(),
            TerrorClass::Types,
            "types::ERR_WRONG_VALUE2"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE2.code().value(),
            errcode::ErrWrongValue as isize,
            "types::ERR_WRONG_VALUE2"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_TYPE.class(),
            TerrorClass::Types,
            "types::ERR_WRONG_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE_FOR_TYPE.code().value(),
            errcode::ErrWrongValueForType as isize,
            "types::ERR_WRONG_VALUE_FOR_TYPE"
        );
        assert_eq!(
            super::ERR_PARTITION_STATS_MISSING.class(),
            TerrorClass::Types,
            "types::ERR_PARTITION_STATS_MISSING"
        );
        assert_eq!(
            super::ERR_PARTITION_STATS_MISSING.code().value(),
            errcode::ErrPartitionStatsMissing as isize,
            "types::ERR_PARTITION_STATS_MISSING"
        );
        assert_eq!(
            super::ERR_PARTITION_COLUMN_STATS_MISSING.class(),
            TerrorClass::Types,
            "types::ERR_PARTITION_COLUMN_STATS_MISSING"
        );
        assert_eq!(
            super::ERR_PARTITION_COLUMN_STATS_MISSING
                .code()
                .value(),
            errcode::ErrPartitionColumnStatsMissing as isize,
            "types::ERR_PARTITION_COLUMN_STATS_MISSING"
        );
        assert_eq!(
            super::ERR_INCORRECT_DATETIME_VALUE.class(),
            TerrorClass::Types,
            "types::ERR_INCORRECT_DATETIME_VALUE"
        );
        assert_eq!(
            super::ERR_INCORRECT_DATETIME_VALUE.code().value(),
            errcode::ErrIncorrectDatetimeValue as isize,
            "types::ERR_INCORRECT_DATETIME_VALUE"
        );
        assert_eq!(
            super::ERR_JSON_BAD_ONE_OR_ALL_ARG.class(),
            TerrorClass::Types,
            "types::ERR_JSON_BAD_ONE_OR_ALL_ARG"
        );
        assert_eq!(
            super::ERR_JSON_BAD_ONE_OR_ALL_ARG.code().value(),
            errcode::ErrJSONBadOneOrAllArg as isize,
            "types::ERR_JSON_BAD_ONE_OR_ALL_ARG"
        );
        assert_eq!(
            super::ERR_JSON_VACUOUS_PATH.class(),
            TerrorClass::Types,
            "types::ERR_JSON_VACUOUS_PATH"
        );
        assert_eq!(
            super::ERR_JSON_VACUOUS_PATH.code().value(),
            errcode::ErrJSONVacuousPath as isize,
            "types::ERR_JSON_VACUOUS_PATH"
        );
        assert_eq!(
            super::ERR_TIMESTAMP_IN_DST_TRANSITION.class(),
            TerrorClass::Executor,
            "types::ERR_TIMESTAMP_IN_DST_TRANSITION"
        );
        assert_eq!(
            super::ERR_TIMESTAMP_IN_DST_TRANSITION
                .code()
                .value(),
            errcode::ErrTimeStampInDSTTransition as isize,
            "types::ERR_TIMESTAMP_IN_DST_TRANSITION"
        );
    }
}
