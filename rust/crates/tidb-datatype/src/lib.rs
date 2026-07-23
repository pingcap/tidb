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
mod binary_json;
mod binary_json_ops;
mod binary_literal;
pub mod char_length;
mod charset;
mod collation;
mod compare;
mod config_duration;
pub mod conversion_context;
mod convert;
mod core_time;
mod datum;
mod datum_convert;
mod datum_eval;
mod decimal;
mod duration;
pub(crate) mod encoding_base;
mod encoding_table;
mod enum_set;
mod etc;
mod eval_type;
mod explain_format;
mod field_name;
mod field_type;
mod format;
mod fsp;
mod json_path;
mod multibyte_encoding;
mod mysql_time;
mod numeric_helper;
mod overflow;
mod packed_time;
mod parser_types_errors;
mod source_string;
mod str_to_date;
mod time_parse;
mod truncate;
pub mod utf8_encoding;
mod vector;

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

pub use binary_json::{
    compare_binary_json, decode_escaped_unicode, quote_json_string, unquote_json_string,
    unquote_string, BinaryJSON, BinaryJSONError, BinaryJSONValue, Opaque, JSON_LITERAL_FALSE,
    JSON_LITERAL_NULL, JSON_LITERAL_TRUE, JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_DATE,
    JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION, JSON_TYPE_CODE_FLOAT64, JSON_TYPE_CODE_INT64,
    JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OPAQUE, JSON_TYPE_CODE_STRING,
    JSON_TYPE_CODE_TIMESTAMP, JSON_TYPE_CODE_UINT64,
};
pub use binary_json_ops::{
    contains_binary_json, merge_binary_json, merge_patch_binary_json, overlaps_binary_json,
    peek_binary_json_len, JSONModifyType, JSONSearchMode,
};
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
pub use collation::{
    binary_collation_name, binary_collator, collation_id_to_name, collation_name_to_id,
    collation_to_proto, compatible_collate, get_binary_collator, get_binary_collator_slice,
    get_collator, get_collator_by_id, get_collator_with_mode, get_supported_collation_by_name,
    is_bin_collation, is_ci_collation, is_default_collation_for_utf8mb4, is_pad_space_collation,
    new_collation_enabled, proto_to_collation, restore_collation_id_if_needed,
    rewrite_new_collation_id_if_needed, set_new_collation_enabled,
    substitute_missing_collation_to_default, supported_collations, CollationError, Collator,
    WildcardPattern, DEFAULT_LEN,
};
pub use compare::{compare_int, vec_compare_ii, vec_compare_iu, vec_compare_ui, vec_compare_uu};
pub use config_duration::{parse_config_duration, ConfigDurationError, ParseFloatErrorKind};
pub use conversion_context::{
    ConversionContext, ConversionFlags, ConversionLocation, ConversionWarningAppender,
    IgnoreConversionWarnings, DEFAULT_STATEMENT_FLAGS, IGNORE_CONVERSION_WARNINGS, STRICT_FLAGS,
};
pub use convert::{
    convert_decimal_str_to_uint, convert_decimal_to_uint, convert_float_to_int,
    convert_float_to_uint, convert_int_to_int, convert_int_to_uint, convert_scientific_notation,
    convert_uint_to_int, convert_uint_to_uint, float_string_to_integer_string,
    integer_signed_lower_bound, integer_signed_upper_bound, integer_unsigned_upper_bound,
    json_to_decimal, json_to_float, json_to_int, json_to_int64, number_to_duration,
    round_integer_string, scalar_to_string, str_to_datetime, str_to_duration, str_to_float,
    str_to_int, str_to_uint, valid_float_prefix, valid_integer_prefix, Converted, DurationOrTime,
    NumericPrefix, ScalarConversionError, ScalarConversionEvent, ScalarStringValue,
};
pub use core_time::{
    calc_daynr, calc_days_in_year, calc_weekday, get_date_from_daynr, get_last_day, is_leap_year,
    CoreTime, DateAddError, TimeConversionError, TimeDifference, TimestampInterval, Weekday,
};
pub use datum::{
    clone_row, datums_contain_null, datums_to_string, datums_to_string_no_error,
    datums_to_string_no_error_smart, estimated_mem_usage, is_printable, sort_datums, Datum,
    DatumKind, DatumStringError, DatumValueError, StringDatum,
};
pub use datum_convert::{
    change_reverse_result_by_bound, get_max_value, get_min_value, produce_float_with_type,
    produce_string_with_type, RoundingType,
};
pub use datum_eval::{compute_plus, DatumArithmeticError};
pub use decimal::{
    decimal_bin_size, Decimal, DecimalCodecError, DecimalCodecWarning, DecimalIntegerWarning,
    DecimalParseError,
};
pub use duration::{
    can_fallback_to_datetime, classify_duration_datetime_fallback, parse_duration,
    parse_mysql_duration, round_duration_fsp, truncate_overflow_mysql_time,
    DurationDateTimeFallbackKind, DurationOverflow, DurationParseError, DurationParseEvent,
    DurationRangeResult, DurationRoundError, DurationValueError, MySqlDuration, ParsedDuration,
    RoundedDuration, MAX_TIME_NANOS, MIN_TIME_NANOS, TIME_MAX_HOUR, TIME_MAX_MINUTE,
    TIME_MAX_SECOND,
};
pub use encoding_base::{TransformOp, TransformResult};
pub use encoding_table::{lookup_encoding, HtmlEncoding, HtmlEncodingError};
pub use enum_set::{
    parse_enum, parse_enum_name, parse_enum_value, parse_set, parse_set_name, parse_set_value,
    EnumParseError, MysqlEnum, MysqlSet, SetParseError,
};
pub use etc::eof_as_nil;
pub use eval_type::{
    EvalType, InvalidEvalType, ET_DATETIME, ET_DECIMAL, ET_DURATION, ET_INT, ET_JSON, ET_REAL,
    ET_STRING, ET_TIMESTAMP, ET_VECTOR_FLOAT32,
};
pub use explain_format::{
    EXPLAIN_FORMATS, EXPLAIN_FORMAT_BINARY, EXPLAIN_FORMAT_BRIEF, EXPLAIN_FORMAT_COST_TRACE,
    EXPLAIN_FORMAT_DOT, EXPLAIN_FORMAT_HINT, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_PLAN_CACHE,
    EXPLAIN_FORMAT_PLAN_TREE, EXPLAIN_FORMAT_ROW, EXPLAIN_FORMAT_TIDB_JSON,
    EXPLAIN_FORMAT_TRADITIONAL, EXPLAIN_FORMAT_TRUE_CARD_COST, EXPLAIN_FORMAT_VERBOSE,
};
pub use field_name::{
    contains_column, FieldName, FieldNameMetadata, IdentifierMetadata, QualifiedColumnName,
    EMPTY_NAME,
};
pub use field_type::{
    agg_field_type, aggregate_eval_type, default_field_type_for_value, enum_set_display_length,
    enum_set_display_length_from_lengths, field_type_has_charset, merge_field_type,
    parser_default_field_type_for_value, set_type_flag, str_to_type, type_str, type_to_str,
    FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags, FieldTypeValue, MAX_DECIMAL_SCALE,
    MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH, VAR_STORAGE_LEN,
};
pub use format::{
    output_format, FlatFormatter, FormatFragment, FormatWriteError, Formatter, IndentFormatter,
};
pub use fsp::{
    align_frac, check_fsp, parse_frac, FspError, DEFAULT_FSP, MAX_FSP, MIN_FSP, UNSPECIFIED_FSP,
};
pub use json_path::{
    parse_json_path_expr, JSONPathArraySelection, JSONPathError, JSONPathExpression, JSONPathLeg,
};
pub use multibyte_encoding::{
    count_valid_bytes, count_valid_bytes_decode, find_encoding, find_encoding_take_utf8_as_noop,
    is_supported_encoding, Encoding, EncodingError, EncodingResult, EncodingType,
};
pub use mysql_time::{
    core_time_from_datetime, date_fsp, format_int_width, get_frac_index, get_fsp, get_timezone,
    round_datetime_fraction, truncate_datetime_fraction, Time, TimeError, TimeType, TimezoneSuffix,
};
pub use numeric_helper::{
    decimal_length_to_precision, get_max_float, precision_to_length_no_truncation, round,
    round_float, string_to_int, truncate, truncate_float, truncate_float_to_string, FloatOverflow,
    StringToIntError,
};
pub use overflow::{
    add_duration, add_int64, add_integer, add_uint64, div_int64, div_int_with_uint,
    div_uint_with_int, mul_int64, mul_integer, mul_uint64, sub_duration, sub_int64,
    sub_int_with_uint, sub_uint64, sub_uint_with_int, OverflowError,
};
pub use packed_time::{PackedTime, PackedTimeError, PackedTimeParts};
pub use parser_types_errors::{
    DATETIME_STR, DATE_STR, ERR_BAD_NUMBER, ERR_CAST_AS_SIGNED_OVERFLOW,
    ERR_CAST_NEG_INT_AS_UNSIGNED, ERR_DATA_OUT_OF_RANGE, ERR_DATA_TOO_LONG,
    ERR_DATETIME_FUNCTION_OVERFLOW, ERR_DIV_BY_ZERO, ERR_DUPLICATED_VALUE_IN_TYPE,
    ERR_ILLEGAL_VALUE_FOR_TYPE, ERR_INCORRECT_DATETIME_VALUE, ERR_INVALID_DEFAULT,
    ERR_INVALID_FIELD_SIZE, ERR_INVALID_WEEK_MODE_FORMAT, ERR_INVALID_YEAR,
    ERR_INVALID_YEAR_FORMAT, ERR_JSON_BAD_ONE_OR_ALL_ARG, ERR_JSON_VACUOUS_PATH,
    ERR_M_BIGGER_THAN_D, ERR_OVERFLOW, ERR_PARTITION_COLUMN_STATS_MISSING,
    ERR_PARTITION_STATS_MISSING, ERR_SYNTAX, ERR_TIMESTAMP_IN_DST_TRANSITION,
    ERR_TOO_BIG_DISPLAY_WIDTH, ERR_TOO_BIG_FIELD_LENGTH, ERR_TOO_BIG_PRECISION, ERR_TOO_BIG_SCALE,
    ERR_TOO_BIG_SET, ERR_TRUNCATED, ERR_TRUNCATED_WRONG_VALUE, ERR_WARN_DATA_OUT_OF_RANGE,
    ERR_WRONG_FIELD_SPEC, ERR_WRONG_VALUE, ERR_WRONG_VALUE_2, ERR_WRONG_VALUE_FOR_TYPE,
    TIMESTAMP_STR, TIME_STR,
};
pub use source_string::{HackedStr, PlainStr, SourceString};
pub use str_to_date::get_format_type;
pub use time_parse::{
    adjust_year, extract_datetime_num, extract_duration_num, extract_duration_value, is_clock_unit,
    is_date_format, is_date_unit, is_microsecond_unit, parse_date_format, parse_datetime,
    parse_duration_value, parse_time, parse_time_from_decimal, parse_time_from_float64,
    parse_time_from_int64, parse_time_from_num, parse_time_from_year, parse_year, time_from_days,
    timestamp_diff, ParsedInterval, ParsedTime,
};
pub use truncate::{is_truncation_error_code, TruncationPolicy};
pub use utf8_encoding::{
    Utf8Encoding, Utf8Mb3StrictEncoding, Utf8Op, Utf8TransformError, Utf8TransformResult,
    UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING,
};
pub use vector::{
    check_vector_dim_valid, deserialize_vector_float32, peek_vector_float32, VectorError,
    VectorFloat32, MAX_VECTOR_DIMENSION,
};
