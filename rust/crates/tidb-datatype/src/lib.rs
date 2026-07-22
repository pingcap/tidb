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
// See the License for the specific language governing permissions and
// limitations under the License.

//! TiDB's dependency-leaf SQL scalar domain.
//!
//! This crate owns byte-preserving [`Datum`] values, their registered
//! [`Collation`] / [`Charset`] relation, the source-backed [`FieldType`]
//! metadata and exact MySQL type-predicate partitions used during expression
//! construction and storage decisions, the source-defined [`EvalType`]
//! classification shared by parser and runtime packages, and exact fixed-point
//! [`Decimal`] arithmetic. It has no dependency on the AST, expression
//! evaluator, or executor.

pub mod ascii_encoding;
mod binary_literal;
pub mod char_length;
mod charset;
mod collation;
mod config_duration;
pub mod conversion_context;
mod datum;
mod decimal;
mod duration;
pub(crate) mod encoding_base;
mod encoding_table;
mod enum_set;
mod eval_type;
mod field_name;
mod field_type;
mod format;
mod fsp;
mod multibyte_encoding;
mod overflow;
mod packed_time;
mod parser_types_errors;
mod time_parse;
mod truncate;
pub mod utf8_encoding;

#[cfg(test)]
mod decimal_tests;

#[cfg(test)]
mod duration_tests;

#[cfg(test)]
mod collation_tests;

#[cfg(test)]
mod eval_type_tests;

#[cfg(test)]
mod enum_set_tests;

#[cfg(test)]
mod fsp_tests;

#[cfg(test)]
mod overflow_tests;

pub use binary_literal::{
    parse_bit_str, parse_hex_str, BinaryLiteral, BinaryLiteralIntOutcome, BinaryLiteralParseError,
    BinaryLiteralWidth, BitLiteral, HexLiteral, InvalidBinaryLiteralWidth,
};
pub use char_length::{produce_char_value, DataTooLongError};
pub use charset::{
    add_charset, add_collation, add_supported_collation, get_charset_info, get_charset_info_by_id,
    get_collation_by_id, get_collation_by_name, get_default_charset_and_collate,
    get_default_collation, get_default_collation_legacy, get_supported_charsets,
    get_supported_collations, remove_charset, valid_charset_and_collation, Charset, CharsetError,
    CharsetInfo, Collation, CollationInfo, PAD_NONE, PAD_SPACE,
};
pub use config_duration::{parse_config_duration, ConfigDurationError};
pub use conversion_context::{
    ConversionContext, ConversionFlags, ConversionLocation, ConversionWarningAppender,
    IgnoreConversionWarnings, DEFAULT_STATEMENT_FLAGS, IGNORE_CONVERSION_WARNINGS, STRICT_FLAGS,
};
pub use datum::{Datum, DatumKind, DatumStringError, StringDatum};
pub use decimal::{decimal_bin_size, Decimal, DecimalCodecError, DecimalCodecWarning};
pub use duration::{
    can_fallback_to_datetime, classify_duration_datetime_fallback, parse_duration,
    round_duration_fsp, truncate_overflow_mysql_time, DurationDateTimeFallbackKind,
    DurationOverflow, DurationParseError, DurationParseEvent, DurationRangeResult,
    DurationRoundError, ParsedDuration, RoundedDuration, MAX_TIME_NANOS, MIN_TIME_NANOS,
    TIME_MAX_HOUR, TIME_MAX_MINUTE, TIME_MAX_SECOND,
};
pub use encoding_base::{TransformOp, TransformResult};
pub use encoding_table::{lookup_encoding, HtmlEncoding, HtmlEncodingError};
pub use enum_set::{
    parse_enum, parse_enum_name, parse_enum_value, parse_set, parse_set_name, parse_set_value,
    EnumParseError, MysqlEnum, MysqlSet, SetParseError,
};
pub use eval_type::{
    EvalType, InvalidEvalType, ET_DATETIME, ET_DECIMAL, ET_DURATION, ET_INT, ET_JSON, ET_REAL,
    ET_STRING, ET_TIMESTAMP, ET_VECTOR_FLOAT32,
};
pub use field_name::{
    contains_column, FieldName, FieldNameMetadata, IdentifierMetadata, QualifiedColumnName,
    EMPTY_NAME,
};
pub use field_type::{
    agg_field_type, aggregate_eval_type, default_field_type_for_value, enum_set_display_length,
    enum_set_display_length_from_lengths, field_type_has_charset, merge_field_type, set_type_flag,
    str_to_type, type_str, type_to_str, FieldType, FieldTypeCode, FieldTypeFlags, FieldTypeValue,
    MAX_DECIMAL_SCALE, MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH, VAR_STORAGE_LEN,
};
pub use format::{
    output_format, FlatFormatter, FormatFragment, FormatWriteError, Formatter, IndentFormatter,
};
pub use fsp::{
    align_frac, check_fsp, parse_frac, FspError, DEFAULT_FSP, MAX_FSP, MIN_FSP, UNSPECIFIED_FSP,
};
pub use multibyte_encoding::{
    count_valid_bytes, count_valid_bytes_decode, find_encoding, find_encoding_take_utf8_as_noop,
    is_supported_encoding, Encoding, EncodingError, EncodingResult, EncodingType,
};
pub use overflow::{
    add_duration, add_int64, add_integer, add_uint64, div_int64, div_int_with_uint,
    div_uint_with_int, mul_int64, mul_integer, mul_uint64, sub_duration, sub_int64,
    sub_int_with_uint, sub_uint64, sub_uint_with_int, OverflowError,
};
pub use packed_time::{PackedTime, PackedTimeError, PackedTimeParts};
pub use parser_types_errors::{
    ERR_DATA_OUT_OF_RANGE, ERR_ILLEGAL_VALUE_FOR_TYPE, ERR_INVALID_DEFAULT,
    ERR_TRUNCATED_WRONG_VALUE,
};
pub use time_parse::parse_date_format;
pub use truncate::{is_truncation_error_code, TruncationPolicy};
pub use utf8_encoding::{
    Utf8Encoding, Utf8Mb3StrictEncoding, Utf8Op, Utf8TransformError, Utf8TransformResult,
    UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING,
};
