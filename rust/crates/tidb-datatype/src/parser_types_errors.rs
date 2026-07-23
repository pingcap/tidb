// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exported error prototypes from `pkg/parser/types/etc.go`.

use std::sync::LazyLock;

use tidb_error::mysql::errcode;
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_error::tidb::errcode as tidb_errcode;

fn types_error(code: u16) -> TerrorError {
    TerrorError::registered_from_catalog(
        TerrorClass::Types,
        TerrorCode::new(
            isize::try_from(code).expect("MySQL error code fits the source int domain"),
        ),
    )
}

fn tidb_error(class: TerrorClass, code: u16) -> TerrorError {
    let message = tidb_error::tidb::message_by_code(code)
        .copied()
        .expect("TiDB error code exists in the generated catalog");
    TerrorError::registered_standard(
        class,
        TerrorCode::new(isize::try_from(code).expect("error code fits source int")),
        message,
    )
}

/// Source `types.ErrInvalidDefault`.
pub static ERR_INVALID_DEFAULT: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrInvalidDefault));
/// Source `types.ErrDataOutOfRange`.
pub static ERR_DATA_OUT_OF_RANGE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrDataOutOfRange));
/// Source `types.ErrTruncatedWrongValue`.
pub static ERR_TRUNCATED_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrTruncatedWrongValue));
/// Source `types.ErrIllegalValueForType`.
pub static ERR_ILLEGAL_VALUE_FOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrIllegalValueForType));

macro_rules! types_errors {
    ($($name:ident => $code:path),+ $(,)?) => {
        $(
            #[doc = concat!("Source `pkg/types/errors.go::", stringify!($name), "`.")]
            pub static $name: LazyLock<TerrorError> =
                LazyLock::new(|| types_error($code));
        )+
    };
}

types_errors! {
    ERR_DATA_TOO_LONG => errcode::ErrDataTooLong,
    ERR_TRUNCATED => errcode::WarnDataTruncated,
    ERR_OVERFLOW => errcode::ErrDataOutOfRange,
    ERR_DIV_BY_ZERO => errcode::ErrDivisionByZero,
    ERR_TOO_BIG_DISPLAY_WIDTH => errcode::ErrTooBigDisplaywidth,
    ERR_TOO_BIG_FIELD_LENGTH => errcode::ErrTooBigFieldlength,
    ERR_TOO_BIG_SET => errcode::ErrTooBigSet,
    ERR_TOO_BIG_SCALE => errcode::ErrTooBigScale,
    ERR_TOO_BIG_PRECISION => errcode::ErrTooBigPrecision,
    ERR_INVALID_FIELD_SIZE => errcode::ErrInvalidFieldSize,
    ERR_M_BIGGER_THAN_D => errcode::ErrMBiggerThanD,
    ERR_WARN_DATA_OUT_OF_RANGE => errcode::ErrWarnDataOutOfRange,
    ERR_DUPLICATED_VALUE_IN_TYPE => errcode::ErrDuplicatedValueInType,
    ERR_DATETIME_FUNCTION_OVERFLOW => errcode::ErrDatetimeFunctionOverflow,
    ERR_WRONG_FIELD_SPEC => errcode::ErrWrongFieldSpec,
    ERR_SYNTAX => errcode::ErrParse,
    ERR_WRONG_VALUE => errcode::ErrTruncatedWrongValue,
    ERR_WRONG_VALUE_2 => errcode::ErrWrongValue,
    ERR_WRONG_VALUE_FOR_TYPE => errcode::ErrWrongValueForType,
    ERR_JSON_BAD_ONE_OR_ALL_ARG => errcode::ErrJSONBadOneOrAllArg,
    ERR_JSON_VACUOUS_PATH => errcode::ErrJSONVacuousPath,
}

macro_rules! tidb_types_errors {
    ($($name:ident => $code:path),+ $(,)?) => {
        $(
            #[doc = concat!("Source `pkg/types/errors.go::", stringify!($name), "`.")]
            pub static $name: LazyLock<TerrorError> =
                LazyLock::new(|| tidb_error(TerrorClass::Types, $code));
        )+
    };
}

tidb_types_errors! {
    ERR_BAD_NUMBER => tidb_errcode::ErrBadNumber,
    ERR_CAST_AS_SIGNED_OVERFLOW => tidb_errcode::ErrCastAsSignedOverflow,
    ERR_CAST_NEG_INT_AS_UNSIGNED => tidb_errcode::ErrCastNegIntAsUnsigned,
    ERR_INVALID_YEAR_FORMAT => tidb_errcode::ErrInvalidYearFormat,
    ERR_INVALID_YEAR => tidb_errcode::ErrInvalidYear,
    ERR_INCORRECT_DATETIME_VALUE => tidb_errcode::ErrIncorrectDatetimeValue,
    ERR_INVALID_WEEK_MODE_FORMAT => tidb_errcode::ErrInvalidWeekModeFormat,
    ERR_PARTITION_STATS_MISSING => tidb_errcode::ErrPartitionStatsMissing,
    ERR_PARTITION_COLUMN_STATS_MISSING => tidb_errcode::ErrPartitionColumnStatsMissing,
}

/// Source `ErrTimestampInDSTTransition` belongs to the executor class.
pub static ERR_TIMESTAMP_IN_DST_TRANSITION: LazyLock<TerrorError> = LazyLock::new(|| {
    tidb_error(
        TerrorClass::Executor,
        tidb_errcode::ErrTimeStampInDSTTransition,
    )
});

/// Source error-label strings used by temporal diagnostics.
pub const DATETIME_STR: &str = "datetime";
/// Source error-label strings used by temporal diagnostics.
pub const DATE_STR: &str = "date";
/// Source error-label strings used by temporal diagnostics.
pub const TIME_STR: &str = "time";
/// Source error-label strings used by temporal diagnostics.
pub const TIMESTAMP_STR: &str = "timestamp";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_error_sql_codes_match_terror_codes() {
        let errors = [
            &*ERR_INVALID_DEFAULT,
            &*ERR_DATA_TOO_LONG,
            &*ERR_ILLEGAL_VALUE_FOR_TYPE,
            &*ERR_TRUNCATED,
            &*ERR_OVERFLOW,
            &*ERR_DIV_BY_ZERO,
            &*ERR_TOO_BIG_DISPLAY_WIDTH,
            &*ERR_TOO_BIG_FIELD_LENGTH,
            &*ERR_TOO_BIG_SET,
            &*ERR_TOO_BIG_SCALE,
            &*ERR_TOO_BIG_PRECISION,
            &*ERR_BAD_NUMBER,
            &*ERR_INVALID_FIELD_SIZE,
            &*ERR_M_BIGGER_THAN_D,
            &*ERR_WARN_DATA_OUT_OF_RANGE,
            &*ERR_DUPLICATED_VALUE_IN_TYPE,
            &*ERR_DATETIME_FUNCTION_OVERFLOW,
            &*ERR_CAST_AS_SIGNED_OVERFLOW,
            &*ERR_CAST_NEG_INT_AS_UNSIGNED,
            &*ERR_INVALID_YEAR_FORMAT,
            &*ERR_TRUNCATED_WRONG_VALUE,
            &*ERR_INVALID_WEEK_MODE_FORMAT,
            &*ERR_WRONG_VALUE,
        ];
        for error in errors {
            assert_eq!(
                error.to_sql_error().code,
                u16::try_from(error.code().value()).unwrap()
            );
        }
    }
}
