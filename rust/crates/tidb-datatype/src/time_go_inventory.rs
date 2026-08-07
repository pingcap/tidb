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

//! Complete lockdown inventory for `pkg/types/time.go`.
//!
//! Go is the source of truth. All 151 production functions, 561 syntactic
//! control-flow loci, and 75 attributed original test/support declarations
//! have exactly one checked verdict. PORTED rows name live Rust symbols or
//! checked-in test/benchmark receipts; UNREACHABLE rows carry closed type or
//! call-path proofs. Source hashes, declaration scans, exact branch-range
//! scans, compile anchors, and receipt markers make omissions and drift fail.

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE: &str = include_str!("../../../../pkg/types/time.go");
const GO_SOURCE_SHA256: &str = "e4f3da22a90a6271f86e11f073c7afb6b6fc25ee32bb95e59904ccb861e0c425";
const GO_SOURCE_LINE_COUNT: usize = 3_554;
const GO_FUNCTION_COUNT: usize = 151;
const GO_IF_COUNT: usize = 352;
const GO_SWITCH_COUNT: usize = 25;
const GO_CASE_OR_DEFAULT_COUNT: usize = 164;
const GO_TIME_TEST: &str = include_str!("../../../../pkg/types/time_test.go");
const GO_CORE_TIME_TEST: &str = include_str!("../../../../pkg/types/core_time_test.go");
const GO_FORMAT_TEST: &str = include_str!("../../../../pkg/types/format_test.go");
const GO_TIME_TEST_SHA256: &str =
    "b0871c17e7aac503dbcbfa27f7bda57b528ac897a8c0d5fb7333d3b49433f3f8";
const GO_CORE_TIME_TEST_SHA256: &str =
    "da68a780cba365a0995052db91cfe61f33330ca24fc49b38cec49a57d236fbd4";
const GO_FORMAT_TEST_SHA256: &str =
    "1cf0aaa612bd51f03c341e0cbc591b2667d28ce399946835dc27a13e46645310";
const RUST_TEST_SUPPORT: &str = concat!(
    include_str!("mysql_time.rs"),
    include_str!("time_parse.rs"),
    include_str!("duration_tests.rs"),
    include_str!("core_time.rs"),
    include_str!("str_to_date.rs"),
    include_str!("../benches/time.rs"),
);

const EXPECTED_FUNCTION_KEYS: &str = "FromGoTime\nFromDate\nFromDateChecked\nTime.Clock\nNewTime\nTime.getFspTt\n*Time.setFspTt\nTime.Type\nTime.Fsp\n*Time.SetType\n*Time.SetFsp\nTime.CoreTime\n*Time.SetCoreTime\nCurrentTime\n*Time.ConvertTimeZone\nTime.String\nTime.IsZero\nTime.InvalidZero\nTime.ToNumber\nTime.FillNumber\nTime.Convert\nTime.ConvertToDuration\nTime.Compare\nTime.CompareString\nroundTime\nTime.RoundFrac\nTime.MarshalJSON\n*Time.UnmarshalJSON\nGetFsp\nGetFracIndex\nRoundFrac\nTruncateFrac\nTime.ToPackedUint\n*Time.FromPackedUint\nTime.Check\n*Time.Sub\n*Time.Add\nTimestampDiff\nParseDateFormat\nisValidSeparator\nGetTimezone\nsplitDateTime\nparseDatetime\nscanTimeArgs\nParseYear\nadjustYear\nAdjustYear\nNewDuration\nMaxMySQLDuration\nDuration.Neg\nDuration.Add\nDuration.Sub\nDuration.DurationFormat\nDuration.convertDateFormat\nDuration.String\nDuration.formatFrac\nDuration.ToNumber\nDuration.ConvertToTime\nDuration.ConvertToTimeWithTimestamp\nDuration.ConvertToYear\nDuration.ConvertToYearFromNow\nDuration.RoundFrac\nDuration.Compare\nDuration.CompareString\nDuration.Hour\nDuration.Minute\nDuration.Second\nDuration.MicroSecond\nisNegativeDuration\nmatchColon\nmatchDayHHMMSS\nmatchHHMMSSDelimited\nmatchHHMMSSCompact\nhhmmssAddOverflow\ncheckHHMMSS\nmatchFrac\nmatchDuration\ncanFallbackToDateTime\nParseDuration\nTruncateOverflowMySQLTime\nsplitDuration\ngetTime\nparseDateTimeFromNum\nParseTime\nParseTimeWithString\nParseTimeFromFloatString\nparseTime\nadjustTimestampErrForDST\nParseDatetime\nParseTimestamp\nParseDate\nParseTimeFromYear\nParseTimeFromNum\nParseDatetimeFromNum\nParseTimestampFromNum\nParseDateFromNum\nTimeFromDays\ncheckDateType\ncheckDateRange\ncheckMonthDay\ncheckTimestampType\ncheckDatetimeType\nExtractDatetimeNum\nExtractDurationNum\nparseSingleTimeValue\nparseTimeValue\nparseAndValidateDurationValue\nParseDurationValue\nExtractDurationValue\nIsClockUnit\nIsDateUnit\nIsMicrosecondUnit\nIsDateFormat\nParseTimeFromInt64\nParseTimeFromFloat64\nParseTimeFromDecimal\nTime.DateFormat\nTime.convertDateFormat\nFormatIntWidthN\nabbrDayOfMonth\n*Time.StrToDate\nmysqlTimeFix\nstrToDate\ngetFormatToken\nskipWhiteSpace\nGetFormatType\nmatchDateWithToken\nparseNDigits\nsecondsNumeric\nminutesNumeric\nparseSep\ntime12Hour\ntime24Hour\nisAMOrPM\ndayOfMonthNumeric\nhour24Numeric\nhour12Numeric\nmicroSeconds\nyearNumericFourDigits\nyearNumericTwoDigits\nyearNumericNDigits\ndayOfYearNumeric\nabbreviatedMonth\nhasCaseInsensitivePrefix\nfullNameMonth\nmonthNumeric\nDateFSP\nDateTimeIsOverflow\nskipAllNums\nskipAllPunct\nskipAllAlpha";

// Audited owner slice: the complete pkg/types/time.go declaration surface.
// The lockdown remains WIP until the owner test/support inventory and mutation
// probes below are complete.
const AUDITED_FUNCTIONS: &[Row] = &[
    ("FromGoTime", Verdict::Ported, "core_time_from_datetime"),
    ("FromDate", Verdict::Ported, "CoreTime::from_date"),
    (
        "FromDateChecked",
        Verdict::Ported,
        "Time::from_date_checked",
    ),
    ("Time.Clock", Verdict::Ported, "Time::clock"),
    ("NewTime", Verdict::Ported, "Time::new"),
    (
        "Time.getFspTt",
        Verdict::Ported,
        "Time::go_raw, Time::kind, Time::fsp",
    ),
    (
        "*Time.setFspTt",
        Verdict::Ported,
        "Time::set_kind, Time::set_fsp",
    ),
    ("Time.Type", Verdict::Ported, "Time::kind"),
    ("Time.Fsp", Verdict::Ported, "Time::fsp"),
    ("*Time.SetType", Verdict::Ported, "Time::set_kind"),
    ("*Time.SetFsp", Verdict::Ported, "Time::set_fsp"),
    ("Time.CoreTime", Verdict::Ported, "Time::core_time"),
    ("*Time.SetCoreTime", Verdict::Ported, "Time::set_core_time"),
    ("CurrentTime", Verdict::Ported, "Time::current"),
    (
        "*Time.ConvertTimeZone",
        Verdict::Ported,
        "Time::convert_time_zone",
    ),
    ("Time.String", Verdict::Ported, "impl Display for Time"),
    ("Time.IsZero", Verdict::Ported, "Time::is_zero"),
    ("Time.InvalidZero", Verdict::Ported, "Time::invalid_zero"),
    ("Time.ToNumber", Verdict::Ported, "Time::to_number"),
    ("Time.FillNumber", Verdict::Ported, "Time::to_number"),
    ("Time.Convert", Verdict::Ported, "Time::convert_kind"),
    (
        "Time.ConvertToDuration",
        Verdict::Ported,
        "Time::to_duration",
    ),
    ("Time.Compare", Verdict::Ported, "Time::compare"),
    (
        "Time.CompareString",
        Verdict::Ported,
        "Time::compare_string",
    ),
    ("roundTime", Verdict::Ported, "round_datetime_fraction"),
    ("Time.RoundFrac", Verdict::Ported, "Time::round_frac"),
    ("Time.MarshalJSON", Verdict::Ported, "Time::to_go_json"),
    ("*Time.UnmarshalJSON", Verdict::Ported, "Time::from_go_json"),
    ("GetFsp", Verdict::Ported, "get_fsp"),
    ("GetFracIndex", Verdict::Ported, "get_frac_index"),
    ("RoundFrac", Verdict::Ported, "round_datetime_fraction"),
    (
        "TruncateFrac",
        Verdict::Ported,
        "truncate_datetime_fraction",
    ),
    ("Time.ToPackedUint", Verdict::Ported, "Time::to_packed_uint"),
    (
        "*Time.FromPackedUint",
        Verdict::Ported,
        "Time::from_packed_uint",
    ),
    ("Time.Check", Verdict::Ported, "Time::validate"),
    ("*Time.Sub", Verdict::Ported, "Time::sub"),
    ("*Time.Add", Verdict::Ported, "Time::add_duration"),
    ("TimestampDiff", Verdict::Ported, "timestamp_diff"),
    ("ParseDateFormat", Verdict::Ported, "parse_date_format"),
    ("isValidSeparator", Verdict::Ported, "parse_date_format"),
    ("GetTimezone", Verdict::Ported, "get_timezone"),
    ("splitDateTime", Verdict::Ported, "parse_time"),
    ("parseDatetime", Verdict::Ported, "parse_time"),
    ("scanTimeArgs", Verdict::Ported, "parse_time"),
    ("ParseYear", Verdict::Ported, "parse_year"),
    ("adjustYear", Verdict::Ported, "adjust_year"),
    ("AdjustYear", Verdict::Ported, "adjust_year"),
    ("NewDuration", Verdict::Ported, "MySqlDuration::new"),
    (
        "MaxMySQLDuration",
        Verdict::Ported,
        "MySqlDuration::maximum",
    ),
    ("Duration.Neg", Verdict::Ported, "MySqlDuration::negated"),
    (
        "Duration.Add",
        Verdict::Ported,
        "MySqlDuration::checked_add",
    ),
    (
        "Duration.Sub",
        Verdict::Ported,
        "MySqlDuration::checked_sub",
    ),
    (
        "Duration.DurationFormat",
        Verdict::Ported,
        "MySqlDuration::duration_format",
    ),
    (
        "Duration.convertDateFormat",
        Verdict::Ported,
        "MySqlDuration::duration_format",
    ),
    (
        "Duration.String",
        Verdict::Ported,
        "impl Display for MySqlDuration",
    ),
    (
        "Duration.formatFrac",
        Verdict::Ported,
        "impl Display for MySqlDuration",
    ),
    (
        "Duration.ToNumber",
        Verdict::Ported,
        "MySqlDuration::to_number",
    ),
    (
        "Duration.ConvertToTime",
        Verdict::Ported,
        "MySqlDuration::convert_to_time",
    ),
    (
        "Duration.ConvertToTimeWithTimestamp",
        Verdict::Ported,
        "MySqlDuration::convert_to_time",
    ),
    (
        "Duration.ConvertToYear",
        Verdict::Ported,
        "MySqlDuration::convert_to_year",
    ),
    (
        "Duration.ConvertToYearFromNow",
        Verdict::Ported,
        "MySqlDuration::convert_to_year",
    ),
    (
        "Duration.RoundFrac",
        Verdict::Ported,
        "MySqlDuration::round_frac",
    ),
    (
        "Duration.Compare",
        Verdict::Ported,
        "MySqlDuration::compare",
    ),
    (
        "Duration.CompareString",
        Verdict::Ported,
        "MySqlDuration::compare_string",
    ),
    ("Duration.Hour", Verdict::Ported, "MySqlDuration::hour"),
    ("Duration.Minute", Verdict::Ported, "MySqlDuration::minute"),
    ("Duration.Second", Verdict::Ported, "MySqlDuration::second"),
    (
        "Duration.MicroSecond",
        Verdict::Ported,
        "MySqlDuration::microsecond",
    ),
    ("isNegativeDuration", Verdict::Ported, "parse_duration"),
    ("matchColon", Verdict::Ported, "parse_duration"),
    ("matchDayHHMMSS", Verdict::Ported, "parse_duration"),
    ("matchHHMMSSDelimited", Verdict::Ported, "parse_duration"),
    ("matchHHMMSSCompact", Verdict::Ported, "parse_duration"),
    ("hhmmssAddOverflow", Verdict::Ported, "parse_duration"),
    ("checkHHMMSS", Verdict::Ported, "parse_duration"),
    ("matchFrac", Verdict::Ported, "parse_duration"),
    ("matchDuration", Verdict::Ported, "parse_duration"),
    (
        "canFallbackToDateTime",
        Verdict::Ported,
        "can_fallback_to_datetime",
    ),
    ("ParseDuration", Verdict::Ported, "parse_mysql_duration"),
    (
        "TruncateOverflowMySQLTime",
        Verdict::Ported,
        "truncate_overflow_mysql_time",
    ),
    (
        "splitDuration",
        Verdict::Ported,
        "MySqlDuration component methods",
    ),
    ("getTime", Verdict::Ported, "parse_time_from_num"),
    (
        "parseDateTimeFromNum",
        Verdict::Ported,
        "parse_time_from_num",
    ),
    ("ParseTime", Verdict::Ported, "parse_time"),
    ("ParseTimeWithString", Verdict::Ported, "parse_time"),
    ("ParseTimeFromFloatString", Verdict::Ported, "parse_time"),
    ("parseTime", Verdict::Ported, "parse_time"),
    ("adjustTimestampErrForDST", Verdict::Ported, "parse_time"),
    ("ParseDatetime", Verdict::Ported, "parse_datetime"),
    ("ParseTimestamp", Verdict::Ported, "parse_time"),
    ("ParseDate", Verdict::Ported, "parse_time"),
    ("ParseTimeFromYear", Verdict::Ported, "parse_time_from_year"),
    (
        "ParseTimeFromNum",
        Verdict::Ported,
        "parse_time_from_num_with_zero_date_error",
    ),
    (
        "ParseDatetimeFromNum",
        Verdict::Ported,
        "parse_time_from_num",
    ),
    (
        "ParseTimestampFromNum",
        Verdict::Ported,
        "parse_time_from_num",
    ),
    ("ParseDateFromNum", Verdict::Ported, "parse_time_from_num"),
    ("TimeFromDays", Verdict::Ported, "time_from_days"),
    ("checkDateType", Verdict::Ported, "Time::validate"),
    ("checkDateRange", Verdict::Ported, "Time::validate"),
    ("checkMonthDay", Verdict::Ported, "Time::validate"),
    (
        "checkTimestampType",
        Verdict::Ported,
        "Time::validate, parse_time",
    ),
    ("checkDatetimeType", Verdict::Ported, "Time::validate"),
    (
        "ExtractDatetimeNum",
        Verdict::Ported,
        "extract_datetime_num",
    ),
    (
        "ExtractDurationNum",
        Verdict::Ported,
        "extract_duration_num",
    ),
    (
        "parseSingleTimeValue",
        Verdict::Ported,
        "parse_duration_value",
    ),
    ("parseTimeValue", Verdict::Ported, "parse_duration_value"),
    (
        "parseAndValidateDurationValue",
        Verdict::Ported,
        "extract_duration_value",
    ),
    (
        "ParseDurationValue",
        Verdict::Ported,
        "parse_duration_value",
    ),
    (
        "ExtractDurationValue",
        Verdict::Ported,
        "extract_duration_value",
    ),
    ("IsClockUnit", Verdict::Ported, "is_clock_unit"),
    ("IsDateUnit", Verdict::Ported, "is_date_unit"),
    ("IsMicrosecondUnit", Verdict::Ported, "is_microsecond_unit"),
    ("IsDateFormat", Verdict::Ported, "is_date_format"),
    (
        "ParseTimeFromInt64",
        Verdict::Ported,
        "parse_time_from_int64",
    ),
    (
        "ParseTimeFromFloat64",
        Verdict::Ported,
        "parse_time_from_float64",
    ),
    (
        "ParseTimeFromDecimal",
        Verdict::Ported,
        "parse_time_from_decimal",
    ),
    ("Time.DateFormat", Verdict::Ported, "Time::date_format"),
    (
        "Time.convertDateFormat",
        Verdict::Ported,
        "Time::date_format",
    ),
    ("FormatIntWidthN", Verdict::Ported, "format_int_width"),
    ("abbrDayOfMonth", Verdict::Ported, "Time::date_format"),
    ("*Time.StrToDate", Verdict::Ported, "Time::str_to_date_into"),
    ("mysqlTimeFix", Verdict::Ported, "fix_meridiem"),
    ("strToDate", Verdict::Ported, "parse_format"),
    ("getFormatToken", Verdict::Ported, "next_token"),
    ("skipWhiteSpace", Verdict::Ported, "skip_whitespace"),
    ("GetFormatType", Verdict::Ported, "get_format_type"),
    ("matchDateWithToken", Verdict::Ported, "parse_token"),
    ("parseNDigits", Verdict::Ported, "parse_optional_digits"),
    ("secondsNumeric", Verdict::Ported, "parse_token"),
    ("minutesNumeric", Verdict::Ported, "parse_token"),
    ("parseSep", Verdict::Ported, "parse_separator"),
    ("time12Hour", Verdict::Ported, "parse_compound_time"),
    ("time24Hour", Verdict::Ported, "parse_compound_time"),
    ("isAMOrPM", Verdict::Ported, "parse_token"),
    ("dayOfMonthNumeric", Verdict::Ported, "parse_token"),
    ("hour24Numeric", Verdict::Ported, "parse_token"),
    ("hour12Numeric", Verdict::Ported, "parse_token"),
    ("microSeconds", Verdict::Ported, "parse_token"),
    ("yearNumericFourDigits", Verdict::Ported, "parse_year"),
    ("yearNumericTwoDigits", Verdict::Ported, "parse_year"),
    ("yearNumericNDigits", Verdict::Ported, "parse_year"),
    ("dayOfYearNumeric", Verdict::Ported, "parse_token"),
    ("abbreviatedMonth", Verdict::Ported, "parse_month_name"),
    ("hasCaseInsensitivePrefix", Verdict::Ported, "has_prefix"),
    ("fullNameMonth", Verdict::Ported, "parse_month_name"),
    ("monthNumeric", Verdict::Ported, "parse_token"),
    ("DateFSP", Verdict::Ported, "date_fsp"),
    ("DateTimeIsOverflow", Verdict::Ported, "Time::is_overflow"),
    ("skipAllNums", Verdict::Ported, "skip_while, is_go_number"),
    (
        "skipAllPunct",
        Verdict::Ported,
        "skip_while, is_go_punctuation",
    ),
    ("skipAllAlpha", Verdict::Ported, "skip_while, is_go_letter"),
];

// Every function-shaped test/support artifact in the two Go temporal test
// owners, plus format_test.go's two direct time.go consumers. Each PORTED row
// names a checked-in Rust test or executable benchmark marker; the gate below
// rejects source drift, duplicate/omitted Go artifacts, or a vanished marker.
const GO_TEST_SUPPORT: &[Row] = &[
    (
        "time_test.go::TestTimeEncoding",
        Verdict::Ported,
        "test_go_time_encoding_source_rows",
    ),
    (
        "time_test.go::TestDateTime",
        Verdict::Ported,
        "test_parse_datetime_source_rows",
    ),
    (
        "time_test.go::TestTimestamp",
        Verdict::Ported,
        "test_parse_timestamp_source_bounds",
    ),
    (
        "time_test.go::TestDate",
        Verdict::Ported,
        "test_parse_date_source_rows",
    ),
    (
        "time_test.go::TestTime",
        Verdict::Ported,
        "complete_duration_parser_matches_all_test_time_rows",
    ),
    (
        "time_test.go::TestDurationAdd",
        Verdict::Ported,
        "duration_methods_match_source_rows",
    ),
    (
        "time_test.go::TestDurationSub",
        Verdict::Ported,
        "duration_methods_match_source_rows",
    ),
    (
        "time_test.go::TestTimeFsp",
        Verdict::Ported,
        "test_date_time_and_type_fsp",
    ),
    (
        "time_test.go::TestYear",
        Verdict::Ported,
        "test_year_source_rows",
    ),
    (
        "time_test.go::TestCodec",
        Verdict::Ported,
        "test_time_encoding",
    ),
    (
        "time_test.go::TestParseTimeFromNum",
        Verdict::Ported,
        "test_parse_time_from_num_source_rows",
    ),
    (
        "time_test.go::TestToNumber",
        Verdict::Ported,
        "test_to_number_and_duration_source_shapes",
    ),
    (
        "time_test.go::TestParseTimeFromFloatString",
        Verdict::Ported,
        "test_parse_time_from_float_string_source_rows",
    ),
    (
        "time_test.go::TestParseFrac",
        Verdict::Ported,
        "test_parse_datetime_fsp_source_rows",
    ),
    (
        "time_test.go::TestRoundFrac",
        Verdict::Ported,
        "test_round_frac_source_rows",
    ),
    (
        "time_test.go::TestConvert",
        Verdict::Ported,
        "test_convert_kind_dst_gap_and_overflow_source_rows",
    ),
    (
        "time_test.go::TestCompare",
        Verdict::Ported,
        "test_compare_string_source_rows",
    ),
    (
        "time_test.go::TestDurationClock",
        Verdict::Ported,
        "duration_methods_match_source_rows",
    ),
    (
        "time_test.go::TestParseDateFormat",
        Verdict::Ported,
        "go_parse_date_format_vectors",
    ),
    (
        "time_test.go::TestTimestampDiff",
        Verdict::Ported,
        "test_timestamp_diff_and_extract_source_rows",
    ),
    (
        "time_test.go::TestDateFSP",
        Verdict::Ported,
        "test_invalid_zero_and_format_int_width_n",
    ),
    (
        "time_test.go::TestConvertTimeZone",
        Verdict::Ported,
        "test_convert_time_zone_source_rows",
    ),
    (
        "time_test.go::TestTimeAdd",
        Verdict::Ported,
        "test_time_add_and_sub_source_rows",
    ),
    (
        "time_test.go::TestTruncateOverflowMySQLTime",
        Verdict::Ported,
        "truncate_overflow_mysql_time_matches_source_endpoints",
    ),
    (
        "time_test.go::TestCheckTimestamp",
        Verdict::Ported,
        "test_validate_timestamp_source_bounds_and_dst_rows",
    ),
    (
        "time_test.go::TestExtractDurationValue",
        Verdict::Ported,
        "test_extract_duration_value_source_rows",
    ),
    (
        "time_test.go::TestCurrentTime",
        Verdict::Ported,
        "test_current_time_preserves_requested_type_and_zero_fsp",
    ),
    (
        "time_test.go::TestInvalidZero",
        Verdict::Ported,
        "test_invalid_zero_and_format_int_width_n",
    ),
    (
        "time_test.go::TestGetFsp",
        Verdict::Ported,
        "test_get_fsp_and_frac_index",
    ),
    (
        "time_test.go::TestExtractDatetimeNum",
        Verdict::Ported,
        "test_timestamp_diff_and_extract_source_rows",
    ),
    (
        "time_test.go::TestExtractDurationNum",
        Verdict::Ported,
        "test_timestamp_diff_and_extract_source_rows",
    ),
    (
        "time_test.go::TestParseDurationValue",
        Verdict::Ported,
        "test_parse_duration_value_source_rows",
    ),
    (
        "time_test.go::TestIsClockUnit",
        Verdict::Ported,
        "test_interval_and_date_format_classifiers_source_rows",
    ),
    (
        "time_test.go::TestIsDateUnit",
        Verdict::Ported,
        "test_interval_and_date_format_classifiers_source_rows",
    ),
    (
        "time_test.go::TestIsMicrosecondUnit",
        Verdict::Ported,
        "test_interval_and_date_format_classifiers_source_rows",
    ),
    (
        "time_test.go::TestIsDateFormat",
        Verdict::Ported,
        "test_interval_and_date_format_classifiers_source_rows",
    ),
    (
        "time_test.go::TestParseTimeFromInt64",
        Verdict::Ported,
        "test_parse_time_from_int_float_decimal_source_rows",
    ),
    (
        "time_test.go::TestParseTimeFromFloat64",
        Verdict::Ported,
        "test_parse_time_from_int_float_decimal_source_rows",
    ),
    (
        "time_test.go::TestParseTimeFromDecimal",
        Verdict::Ported,
        "test_parse_time_from_int_float_decimal_source_rows",
    ),
    (
        "time_test.go::TestGetFormatType",
        Verdict::Ported,
        "test_get_format_type_source_rows",
    ),
    (
        "time_test.go::TestGetFracIndex",
        Verdict::Ported,
        "test_get_fsp_and_frac_index",
    ),
    (
        "time_test.go::TestTimeOverflow",
        Verdict::Ported,
        "datetime_overflow_matches_time_go_source_boundaries",
    ),
    (
        "time_test.go::TestTruncateFrac",
        Verdict::Ported,
        "test_standalone_round_and_truncate_frac_source_rows",
    ),
    (
        "time_test.go::TestTimeSub",
        Verdict::Ported,
        "test_time_add_and_sub_source_rows",
    ),
    (
        "time_test.go::TestCheckMonthDay",
        Verdict::Ported,
        "test_validate_month_day_source_rows",
    ),
    (
        "time_test.go::TestFormatIntWidthN",
        Verdict::Ported,
        "test_invalid_zero_and_format_int_width_n",
    ),
    (
        "time_test.go::TestFromGoTime",
        Verdict::Ported,
        "test_from_datetime_rounds_to_microseconds_like_source",
    ),
    (
        "time_test.go::TestGetTimezone",
        Verdict::Ported,
        "test_get_timezone_source_rows",
    ),
    (
        "time_test.go::TestParseWithTimezone",
        Verdict::Ported,
        "test_parse_with_timezone_source_rows",
    ),
    (
        "time_test.go::TestMarshalTime",
        Verdict::Ported,
        "test_go_json_round_trip_source_row",
    ),
    (
        "time_test.go::TestDurationConvertToYearFromNow",
        Verdict::Ported,
        "duration_time_and_year_conversion_match_source_rows",
    ),
    (
        "time_test.go::BenchmarkFormat",
        Verdict::Ported,
        "BenchmarkFormat",
    ),
    (
        "time_test.go::BenchmarkTimeAdd",
        Verdict::Ported,
        "BenchmarkTimeAdd",
    ),
    (
        "time_test.go::BenchmarkTimeCompare",
        Verdict::Ported,
        "BenchmarkTimeCompare",
    ),
    (
        "time_test.go::benchmarkDateFormat",
        Verdict::Ported,
        "BenchmarkParseDateFormat",
    ),
    (
        "time_test.go::BenchmarkParseDateFormat",
        Verdict::Ported,
        "BenchmarkParseDateFormat",
    ),
    (
        "time_test.go::benchmarkDatetimeFormat",
        Verdict::Ported,
        "BenchmarkParseDatetimeFormat",
    ),
    (
        "time_test.go::BenchmarkParseDatetimeFormat",
        Verdict::Ported,
        "BenchmarkParseDatetimeFormat",
    ),
    (
        "time_test.go::benchmarkStrToDate",
        Verdict::Ported,
        "BenchmarkStrToDate",
    ),
    (
        "time_test.go::BenchmarkStrToDate",
        Verdict::Ported,
        "BenchmarkStrToDate",
    ),
    (
        "core_time_test.go::TestWeekBehaviour",
        Verdict::Ported,
        "test_week_behaviour_and_week",
    ),
    (
        "core_time_test.go::TestWeek",
        Verdict::Ported,
        "test_week_behaviour_and_week",
    ),
    (
        "core_time_test.go::TestCalcDaynr",
        Verdict::Ported,
        "test_calc_daynr",
    ),
    (
        "core_time_test.go::TestCalcTimeTimeDiff",
        Verdict::Ported,
        "test_calc_time_time_diff",
    ),
    (
        "core_time_test.go::TestCompareTime",
        Verdict::Ported,
        "test_compare_time",
    ),
    (
        "core_time_test.go::TestGetDateFromDaynr",
        Verdict::Ported,
        "test_get_date_from_daynr",
    ),
    (
        "core_time_test.go::TestMixDateAndTime",
        Verdict::Ported,
        "test_mix_date_and_time",
    ),
    (
        "core_time_test.go::TestIsLeapYear",
        Verdict::Ported,
        "test_is_leap_year_and_get_last_day",
    ),
    (
        "core_time_test.go::TestGetLastDay",
        Verdict::Ported,
        "test_is_leap_year_and_get_last_day",
    ),
    (
        "core_time_test.go::TestGetFixDays",
        Verdict::Ported,
        "test_fix_days_source_rows",
    ),
    (
        "core_time_test.go::TestAddDate",
        Verdict::Ported,
        "test_add_date_source_boundaries_and_month_end",
    ),
    (
        "core_time_test.go::TestWeekday",
        Verdict::Ported,
        "test_weekday_normalizes_invalid_calendar_dates",
    ),
    (
        "core_time_test.go::TestAdjustedGoTime",
        Verdict::Ported,
        "test_adjusted_datetime_source_dst_rows",
    ),
    (
        "format_test.go::TestTimeFormatMethod",
        Verdict::Ported,
        "test_date_format_source_rows",
    ),
    (
        "format_test.go::TestStrToDate",
        Verdict::Ported,
        "test_str_to_date_source_rows",
    ),
];

// One row per syntactic `if`, `switch`, `case`, or `default` in the audited
// slice. Each row names the boundary test that exercises both sides, or gives
// a closed representation reason when Rust intentionally cannot admit the Go
// state. Line numbers are pinned by the full-source hash above.
const AUDITED_BRANCHES: &[Row] = &[
    ("FromDateChecked@208.field_widths", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("NewTime@272.date_metadata", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("NewTime@276.unspecified_fsp", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("NewTime@280.timestamp_bit", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("Type@297.date", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("Type@300.timestamp_or_datetime", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("Fsp@309.date_or_fraction", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetType@319.leaving_date", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetType@322.dispatch", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetType@323.date", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetType@325.timestamp", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetType@327.datetime", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetFsp@335.date_noop", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("SetFsp@338.unspecified", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("ConvertTimeZone@364.zero_or_convert", Verdict::Ported, "test_convert_time_zone_source_rows"),
    ("ConvertTimeZone@366.invalid_wall_time", Verdict::Ported, "time_owner_slice_conversion_error_boundaries"),
    ("String@376.date_or_datetime", Verdict::Ported, "time_owner_slice_construction_and_metadata_boundaries"),
    ("String@386.fraction", Verdict::Ported, "test_to_number_and_duration_source_shapes"),
    ("FillNumber@425.zero", Verdict::Ported, "test_to_number_and_duration_source_shapes"),
    ("FillNumber@433.date_or_datetime", Verdict::Ported, "test_to_number_and_duration_source_shapes"),
    ("FillNumber@440.controlled_format_error", Verdict::Unreachable, "Rust uses fixed infallible formatting for the same controlled layouts"),
    ("FillNumber@445.fraction", Verdict::Ported, "test_to_number_and_duration_source_shapes"),
    ("Convert@457.same_type_or_zero", Verdict::Ported, "time_owner_slice_conversion_error_boundaries"),
    ("Convert@464.timestamp_dst_gap", Verdict::Ported, "test_convert_kind_dst_gap_and_overflow_source_rows"),
    ("Convert@466.dst_adjustment_success", Verdict::Ported, "test_convert_kind_dst_gap_and_overflow_source_rows"),
    ("ConvertToDuration@479.zero", Verdict::Ported, "convert_to_duration_matches_time_go_source_rows"),
    ("CompareString@502.parse_error", Verdict::Ported, "test_compare_string_source_rows"),
    ("RoundFrac@517.date_or_zero", Verdict::Ported, "time_owner_slice_rounding_error_boundaries"),
    ("RoundFrac@523.invalid_fsp", Verdict::Ported, "time_owner_slice_rounding_error_boundaries"),
    ("RoundFrac@527.same_fsp", Verdict::Ported, "time_owner_slice_rounding_error_boundaries"),
    ("RoundFrac@533.valid_or_incomplete_date", Verdict::Ported, "test_round_frac_source_rows"),
    ("RoundFrac@546.incomplete_date_day_carry", Verdict::Ported, "time_owner_slice_rounding_error_boundaries"),
    ("RoundFrac@551.checked_repack", Verdict::Unreachable, "Rust reconstructs the same original bit-width-valid CoreTime fields and a normalized clock"),
    ("NewTime.invalid_direct_fsp", Verdict::Declined, "measured Go probe: fsp=7 aliases DATE and fsp=-2 contaminates packed calendar bits; Rust check_fsp preserves a valid typed value or returns InvalidFsp"),
    ("SetType.invalid_type_code", Verdict::Unreachable, "Rust TimeType has exactly Date, DateTime, and Timestamp"),
    ("GetFsp@573.absent_fraction", Verdict::Ported, "test_get_fsp_and_frac_index"),
    ("GetFsp@578.cap_six", Verdict::Ported, "test_get_fsp_and_frac_index"),
    ("GetFracIndex@593.timezone_suffix", Verdict::Ported, "test_get_fsp_and_frac_index"),
    ("GetFracIndex@600.last_punctuation", Verdict::Ported, "test_get_fsp_and_frac_index"),
    ("GetFracIndex@601.dot", Verdict::Ported, "test_get_fsp_and_frac_index"),
    ("RoundFrac@617.invalid_fsp", Verdict::Ported, "time_owner_slice_standalone_rounding_boundaries"),
    ("TruncateFrac@627.invalid_fsp", Verdict::Ported, "time_owner_slice_standalone_rounding_boundaries"),
    ("ToPackedUint@648.zero", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("FromPackedUint@661.zero", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("Check@690.type_dispatch", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("Check@691.timestamp", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("Check@693.date_or_datetime", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("Sub@703.timestamp_instant_or_calendar", Verdict::Ported, "time_owner_slice_subtract_boundaries"),
    ("Sub@712.negative", Verdict::Ported, "time_owner_slice_subtract_boundaries"),
    ("Sub@719.maximum_fsp", Verdict::Ported, "time_owner_slice_subtract_boundaries"),
    ("Add@738.date_clears_clock", Verdict::Ported, "time_owner_slice_add_date_boundary"),
    ("ToPackedUint.invalid_sql_fields", Verdict::Declined, "Go packs any CoreTime bit-field value; Rust rejects fields outside documented SQL ranges 0-9999/12/31/23/59/59/999999"),
    ("FromPackedUint.invalid_sql_fields", Verdict::Declined, "Go masks a 24-bit microsecond payload through FromDate; Rust returns OutOfRange instead of silently truncating malformed storage"),
    ("Sub.invalid_timestamp_wall_time", Verdict::Declined, "Go logs GoTime conversion errors and subtracts zero time values; Rust returns the conversion error instead of manufacturing a duration"),
    ("ParseDateFormat@760.empty", Verdict::Ported, "go_parse_date_format_vectors"),
    ("ParseDateFormat@765.leading_digit", Verdict::Ported, "go_parse_date_format_vectors"),
    ("ParseDateFormat@777.separator", Verdict::Ported, "go_parse_date_format_vectors"),
    ("ParseDateFormat@784.consecutive_separators", Verdict::Ported, "go_parse_date_format_vectors"),
    ("ParseDateFormat@795.middle_nondigit", Verdict::Ported, "go_parse_date_format_vectors"),
    ("isValidSeparator@807.punctuation", Verdict::Ported, "go_parse_date_format_vectors"),
    ("isValidSeparator@812.date_time_whitespace", Verdict::Ported, "go_parse_date_format_vectors"),
    ("isValidSeparator@816.trailing_nondigit", Verdict::Ported, "go_parse_date_format_vectors"),
    ("GetTimezone@858.uppercase_z", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@862.last_sign", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@865.last_colon", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@874.z_at_end", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@877.sign_suffix_width", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@880.colon_position", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@883.valid_shape", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@888.sign_fields", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@892.z_fields", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@895.separator_field", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@898.hour_present", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@900.hour_digits", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@904.minute_present", Verdict::Ported, "test_get_timezone_source_rows"),
    ("GetTimezone@906.minute_digits", Verdict::Ported, "test_get_timezone_source_rows"),
    ("splitDateTime@924.timezone_suffix", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("splitDateTime@933.fraction_suffix", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@963.trailing_fraction_junk", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1018.fraction_absorption_candidate", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1019.fraction_absorbed_or_retained", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1024.signed_timezone_candidate", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1027.timezone_absorbed_or_retained", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1029.absorb_timezone_hour", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1032.absorb_timezone_minute", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1038.part_count_dispatch", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1039.no_parts", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1041.compact_part", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1044.float_numeric", Verdict::Ported, "test_parse_time_from_float_string_source_rows"),
    ("parseDatetime@1046.float_integer_parse_error", Verdict::Ported, "test_parse_time_from_float_string_source_rows"),
    ("parseDatetime@1051.float_datetime_parse_error", Verdict::Ported, "test_parse_time_from_float_string_source_rows"),
    ("parseDatetime@1059.float_hhmmss_classification", Verdict::Ported, "test_parse_time_from_float_string_source_rows"),
    ("parseDatetime@1067.compact_width_dispatch", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1068.width_14", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1072.width_12", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1076.width_11", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1080.width_10", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1083.width_9", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1086.width_8", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1088.width_7", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1091.width_5_or_6", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1095.unsupported_width", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1098.date_width_fraction_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1103.string_fraction_not_float", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1105.fraction_clock_width_dispatch", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1106.empty_fraction_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1107.hour_fraction_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1109.hour_minute_fraction_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1111.full_fraction_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1118.width_9_or_10_seconds", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1119.empty_or_present_seconds", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1126.compact_warning", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1130.two_parts_invalid", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1132.three_parts", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1135.four_parts", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1138.five_parts", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1141.six_parts", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1146.excess_parts_truncated", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1154.field_scan_error", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1155.field_scan_eof", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1164.two_digit_year_candidate", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1165.all_zero_year_exception", Verdict::Ported, "test_parse_datetime_source_rows"),
    ("parseDatetime@1173.fraction_only_with_clock", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1177.fraction_parse_error", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1183.core_bit_width", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1186.fraction_carry", Verdict::Ported, "test_parse_datetime_fsp_source_rows"),
    ("parseDatetime@1189.carry_invalid_wall_time", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseDatetime@1194.explicit_timezone", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1196.timezone_requires_clock", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1199.timezone_hour_present", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1202.timezone_minute_present", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1206.timezone_offset_bounds", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1213.negative_timezone", Verdict::Ported, "test_parse_with_timezone_source_rows"),
    ("parseDatetime@1218.explicit_zone_invalid_wall_time", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("scanTimeArgs@1231.argument_count", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("scanTimeArgs@1238.integer_parse", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("ParseYear@1248.parse_int16", Verdict::Ported, "test_year_source_rows"),
    ("ParseYear@1253.one_or_two_digit_window", Verdict::Ported, "test_year_source_rows"),
    ("ParseYear@1255.four_digit_width", Verdict::Ported, "test_year_source_rows"),
    ("ParseYear@1259.year_domain", Verdict::Ported, "test_year_source_rows"),
    ("adjustYear@1269.zero_to_69", Verdict::Ported, "test_year_source_rows"),
    ("adjustYear@1271.seventy_to_99", Verdict::Ported, "test_year_source_rows"),
    ("AdjustYear@1279.zero_without_window", Verdict::Ported, "test_year_source_rows"),
    ("AdjustYear@1283.negative", Verdict::Declined, "Go returns value 0 beside ErrWarnDataOutOfRange; Rust Result returns OutOfRange without a value"),
    ("AdjustYear@1286.below_minimum", Verdict::Declined, "Go returns MinYear 1901 beside ErrWarnDataOutOfRange; Rust Result returns OutOfRange without a value"),
    ("AdjustYear@1289.above_maximum", Verdict::Declined, "Go returns MaxYear 2155 beside ErrWarnDataOutOfRange; Rust Result returns OutOfRange without a value"),
    ("Duration.Add@1327.zero_identity", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Add@1331.int64_overflow", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Add@1334.maximum_fsp", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Sub@1342.zero_identity", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Sub@1346.int64_overflow", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Sub@1349.maximum_fsp", Verdict::Ported, "duration_methods_match_source_rows"),
    ("DurationFormat@1362.pattern_character", Verdict::Ported, "duration_methods_match_source_rows"),
    ("DurationFormat@1363.conversion_error", Verdict::Unreachable, "Go convertDateFormat always returns nil and Rust conversion is infallible"),
    ("DurationFormat@1371.percent_opens_pattern", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1381.token_dispatch", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1382.hour_padded", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1384.hour_unpadded", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1386.hour12_padded", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1388.hour12_zero", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1393.hour12_unpadded", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1395.hour12_unpadded_zero", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1400.minute", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1402.meridiem", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1404.am_or_pm", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1409.clock12", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1412.clock12_dispatch", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1413.midnight", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1415.noon", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1417.morning", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1419.afternoon", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1422.clock24", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1424.seconds", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1426.microseconds", Verdict::Ported, "duration_methods_match_source_rows"),
    ("convertDateFormat@1428.literal_token", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.String@1440.negative_sign", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.String@1445.fraction", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.ToNumber@1470.negative_sign", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.ToNumber@1474.integer_or_fraction", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.ConvertToYearFromNow@1516.concat_mode", Verdict::Ported, "duration_time_and_year_conversion_match_source_rows"),
    ("Duration.RoundFrac@1538.nil_timezone", Verdict::Ported, "duration rounding is timezone-invariant; MySqlDuration::round_frac has no timezone input"),
    ("Duration.RoundFrac@1544.invalid_fsp", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("Duration.RoundFrac@1548.same_fsp", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("Duration.Compare@1560.ordering", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.Compare@1562.equality", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.CompareString@1573.parse_error", Verdict::Ported, "duration_methods_match_source_rows"),
    ("Duration.ConvertToTime.calendar_mix", Verdict::Ported, "convert_to_time_uses_calendar_clock_fields_across_dst_gap"),
    ("Duration.constructor_outside_int64", Verdict::Declined, "Go duration arithmetic wraps int64 on oversized direct fields; Rust debug arithmetic rejects or panics outside the documented MySQL TIME domain"),
    ("isNegativeDuration@1611.leading_minus", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchColon@1621.colon_required", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDayHHMMSS@1630.day_number", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDayHHMMSS@1635.required_space", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDayHHMMSS@1640.clock", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchHHMMSSDelimited@1651.hour", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchHHMMSSDelimited@1658.colon", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchHHMMSSDelimited@1659.first_colon_required", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchHHMMSSDelimited@1665.component", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchHHMMSSCompact@1677.number", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("hhmmssAddOverflow@1688.carry", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("matchFrac@1705.dot_optional", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchFrac@1710.zero_or_more_digits", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchFrac@1715.fraction_rounding", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("matchDuration@1724.fsp", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("matchDuration@1728.empty", Verdict::Ported, "malformed_duration_input_has_only_the_two_source_outcomes"),
    ("matchDuration@1738.day_form", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDuration.delimited_form", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDuration.compact_form", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDuration.no_grammar", Verdict::Ported, "malformed_duration_input_has_only_the_two_source_outcomes"),
    ("matchDuration@1751.long_leftover_is_null", Verdict::Ported, "malformed_duration_input_has_only_the_two_source_outcomes"),
    ("matchDuration@1755.fraction_carry", Verdict::Ported, "round_duration_fsp_matches_source_rows"),
    ("matchDuration@1760.minute_second_bounds", Verdict::Ported, "malformed_duration_input_has_only_the_two_source_outcomes"),
    ("matchDuration@1764.hour_clamp", Verdict::Ported, "truncate_overflow_mysql_time_matches_source_endpoints"),
    ("matchDuration@1766.negative_clamp", Verdict::Ported, "truncate_overflow_mysql_time_matches_source_endpoints"),
    ("matchDuration@1775.negative_value", Verdict::Ported, "parse_duration_matches_source_colon_and_day_forms"),
    ("matchDuration@1779.short_leftover_warning", Verdict::Ported, "duration_parse_events_classify_source_warning_branches"),
    ("canFallbackToDateTime@1791.leading_digits", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime@1794.compact_12_or_14", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime@1799.first_punctuation", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime@1804.second_digits", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime@1809.second_punctuation", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime@1814.third_digits", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("canFallbackToDateTime.trailing_space_or_t", Verdict::Ported, "can_fallback_to_datetime_matches_source_shape_rows"),
    ("ParseDuration@1827.direct_success", Verdict::Ported, "complete_duration_parser_matches_all_test_time_rows"),
    ("ParseDuration@1830.fallback_shape", Verdict::Ported, "complete_duration_parser_handles_source_datetime_fallback_rows"),
    ("ParseDuration@1835.fallback_parse", Verdict::Ported, "complete_duration_parser_handles_source_datetime_fallback_rows"),
    ("ParseDuration@1840.datetime_to_duration", Verdict::Ported, "complete_duration_parser_handles_source_datetime_fallback_rows"),
    ("TruncateOverflowMySQLTime@1850.positive", Verdict::Ported, "truncate_overflow_mysql_time_matches_source_endpoints"),
    ("TruncateOverflowMySQLTime.negative", Verdict::Ported, "truncate_overflow_mysql_time_matches_source_endpoints"),
    ("splitDuration@1861.negative", Verdict::Ported, "duration_methods_match_source_rows"),
    ("duration_parser.source_space_bytes", Verdict::Ported, "duration_parser_accepts_go_latin1_space_bytes"),
    ("getTime@1894.field_bit_width", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1909.zero", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1915.full_datetime", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1921.too_short", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1927.year_2000_window", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1933.invalid_70_boundary", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1939.year_1900_window", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1945.full_date", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1951.invalid_short_datetime", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1960.datetime_2000_window", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1966.invalid_datetime_70_boundary", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("parseDateTimeFromNum@1972.datetime_1900_window", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("ParseTimeFromFloatString@2006.zero_prefix", Verdict::Ported, "test_parse_time_from_float_string_source_rows"),
    ("parseTime@2014.invalid_fsp", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseTime@2019.parse_error", Verdict::Ported, "test_parse_datetime_source_errors_or_warnings"),
    ("parseTime@2024.validation_error", Verdict::Ported, "test_parse_timestamp_source_bounds"),
    ("parseTime@2025.timestamp_nonzero", Verdict::Ported, "parse_timestamp_preserves_go_dst_adjusted_value_and_diagnostic"),
    ("parseTime@2027.dst_diagnostic", Verdict::Ported, "parse_timestamp_preserves_go_dst_adjusted_value_and_diagnostic"),
    ("adjustTimestampErrForDST@2037.type_or_zero", Verdict::Ported, "parse_timestamp_preserves_go_dst_adjusted_value_and_diagnostic"),
    ("adjustTimestampErrForDST@2043.bounds", Verdict::Ported, "parse_timestamp_preserves_go_dst_adjusted_value_and_diagnostic"),
    ("adjustTimestampErrForDST@2046.closest_valid_time", Verdict::Ported, "parse_timestamp_preserves_go_dst_adjusted_value_and_diagnostic"),
    ("ParseTimeFromYear@2073.zero", Verdict::Ported, "time_from_year_and_days_source_boundaries"),
    ("ParseTimeFromNum@2085.zero", Verdict::Ported, "numeric_zero_time_conversion_keeps_go_value_and_diagnostic"),
    ("ParseTimeFromNum@2087.zero_diagnostic_flag", Verdict::Ported, "numeric_zero_time_conversion_keeps_go_value_and_diagnostic"),
    ("ParseTimeFromNum@2088.zero_type_dispatch", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("ParseTimeFromNum@2089.zero_timestamp", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("ParseTimeFromNum@2091.zero_date", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("ParseTimeFromNum@2093.zero_datetime", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("ParseTimeFromNum@2100.invalid_fsp", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("ParseTimeFromNum@2105.numeric_parse", Verdict::Ported, "test_parse_time_from_num_source_rows"),
    ("ParseTimeFromNum@2111.requested_type_validation", Verdict::Ported, "parse_time_from_num_preserves_go_type_matrix"),
    ("TimeFromDays@2135.negative", Verdict::Ported, "time_from_year_and_days_source_boundaries"),
    ("TimeFromDays@2140.bit_width", Verdict::Unreachable, "get_date_from_daynr returns either zero or fields within CoreTime bit widths"),
    ("checkDateType@2148.all_zero", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkDateType@2152.zero_in_date_policy", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkDateType@2156.range", Verdict::Ported, "validation_rejects_microseconds_past_go_max_datetime"),
    ("checkDateType@2160.month_day", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkDateRange@2170.negative_fields", Verdict::Unreachable, "CoreTime calendar fields are unsigned bit fields"),
    ("checkDateRange@2173.maximum_datetime", Verdict::Ported, "validation_rejects_microseconds_past_go_max_datetime"),
    ("checkMonthDay@2180.month_bounds", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkMonthDay@2185.invalid_date_policy", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkMonthDay@2186.nonzero_month", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkMonthDay@2189.nonleap_february", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkMonthDay@2194.day_bounds", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkTimestampType@2201.zero", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("checkTimestampType@2206.bound_timezone", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("checkTimestampType@2209.zone_conversion", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("checkTimestampType@2217.timestamp_bounds", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("checkTimestampType@2221.local_wall_time", Verdict::Ported, "test_validate_timestamp_source_bounds_and_dst_rows"),
    ("checkDatetimeType@2229.date_validation", Verdict::Ported, "test_validate_month_day_source_rows"),
    ("checkDatetimeType@2234.hour", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("checkDatetimeType@2237.minute", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("checkDatetimeType@2240.second", Verdict::Ported, "time_owner_slice_packed_and_validation_boundaries"),
    ("parseTime.general_error_value", Verdict::Declined, "Go returns a typed zero Time beside ordinary parse errors; Rust Result returns the typed error without a value"),
    ("ParseTimeFromNum.general_error_value", Verdict::Declined, "Go returns a typed zero Time beside ordinary numeric parse errors; Rust Result returns the typed error without a value"),
    ("ParseTimeFromYear.invalid_caller_input", Verdict::Unreachable, "Go says the invoker must promise year is in [MinYear, MaxYear]; Rust rejects values outside the representable unsigned year"),
];

type BranchGroup = (&'static [usize], Verdict, &'static str);

// Exact control-flow loci for pkg/types/time.go:2248-2762. Every line is one
// `if`, `else if`, `switch`, `case`, or `default` in the Go owner. Grouping
// rows by the running source-table test keeps 131 explicit loci readable; the
// gate below flattens them, rejects duplicates, and compares them with a fresh
// scan of this exact Go range.
const INTERVAL_BRANCH_GROUPS: &[BranchGroup] = &[
    (
        &[
            2250, 2251, 2253, 2256, 2258, 2265, 2267, 2271, 2275, 2279, 2283, 2286, 2293, 2294,
            2296, 2298, 2300, 2302, 2304, 2306, 2308, 2310, 2312, 2314, 2316, 2318, 2320, 2322,
            2325,
        ],
        Verdict::Ported,
        "test_timestamp_diff_and_extract_source_rows",
    ),
    (
        &[
            2337, 2341, 2348, 2357, 2361, 2363, 2367, 2371, 2374, 2379, 2380, 2381, 2387, 2388,
            2394, 2395, 2401, 2402, 2408, 2409, 2413, 2414, 2418, 2419, 2423, 2424, 2428, 2429,
            2447, 2456, 2460, 2473, 2478, 2482, 2487, 2491, 2495, 2499, 2510, 2513, 2517, 2527,
            2528, 2530, 2532, 2534, 2536, 2538, 2540, 2542, 2544, 2546, 2548, 2550, 2552,
        ],
        Verdict::Ported,
        "test_parse_duration_value_source_rows",
    ),
    (
        &[
            2560, 2561, 2563, 2568, 2570, 2574, 2576, 2580, 2582, 2586, 2588, 2592, 2594, 2598,
            2600, 2604, 2606, 2610, 2612, 2616, 2618, 2622, 2624, 2628, 2630, 2635,
        ],
        Verdict::Ported,
        "test_extract_duration_value_source_rows",
    ),
    (
        &[
            2642, 2643, 2649, 2656, 2657, 2661, 2668, 2669, 2671, 2681, 2682, 2684, 2687,
        ],
        Verdict::Ported,
        "test_interval_and_date_format_classifiers_source_rows",
    ),
    (
        &[2704, 2707, 2722, 2727, 2731, 2739, 2745, 2749],
        Verdict::Ported,
        "test_parse_time_from_int_float_decimal_source_rows",
    ),
];

// Exact control-flow loci for pkg/types/time.go:2763-2937. These rules are
// covered by the source-row DATE_FORMAT test and the direct helper boundary
// test, including invalid months, all 12-hour partitions, every ordinal
// suffix, negative integer padding, unknown conversions, and a trailing `%`.
const FORMAT_BRANCH_GROUPS: &[BranchGroup] = &[
    (
        &[
            2767, 2768, 2776, 2791, 2792, 2794, 2798, 2800, 2804, 2806, 2808, 2811, 2813, 2815,
            2817, 2819, 2821, 2823, 2828, 2830, 2835, 2837, 2839, 2844, 2847, 2848, 2850, 2852,
            2854, 2857, 2859, 2861, 2863, 2866, 2869, 2872, 2875, 2878, 2880, 2882, 2884, 2889,
            2891, 2896, 2898, 2901, 2923, 2924, 2926, 2928, 2930,
        ],
        Verdict::Ported,
        "test_date_format_source_rows",
    ),
    (
        &[2911],
        Verdict::Ported,
        "test_invalid_zero_and_format_int_width_n",
    ),
];

// Exact control-flow loci for the final STR_TO_DATE/overflow owner block.
// The first group is exercised by the copied Go rows plus explicit exhausted
// input, receiver mutation, helper-boundary, Unicode-category, warning, and
// timestamp-boundary tests. The second group contains source error arms that
// Rust's non-null timezone reference, closed TimeType enum, and valid fixed
// endpoint constants make unrepresentable.
const STR_TO_DATE_BRANCH_GROUPS: &[BranchGroup] = &[
    (
        &[
            2942, 2948, 2954, 2957, 2968, 2972, 2973, 2976, 2979, 2981, 2982, 2984, 2989, 2993,
            3008, 3012, 3013, 3021, 3027, 3037, 3042, 3043, 3050, 3059, 3129, 3132, 3136, 3137,
            3138, 3140, 3144, 3152, 3156, 3166, 3180, 3189, 3206, 3209, 3212, 3225, 3230, 3237,
            3242, 3248, 3253, 3259, 3262, 3267, 3268, 3270, 3272, 3280, 3293, 3300, 3305, 3311,
            3316, 3324, 3336, 3341, 3342, 3344, 3346, 3360, 3369, 3379, 3389, 3410, 3412, 3423,
            3431, 3433, 3442, 3450, 3460, 3470, 3487, 3488, 3495, 3497, 3515, 3526, 3537, 3548,
        ],
        Verdict::Ported,
        "STR_TO_DATE source rows and boundary suite; datetime_overflow_matches_time_go_source_boundaries",
    ),
    (
        &[3480],
        Verdict::Unreachable,
        "Rust accepts &TZ, which cannot represent Go's nil Context.Location result",
    ),
    (
        &[3489, 3492, 3498, 3501, 3505, 3508],
        Verdict::Unreachable,
        "the fixed valid Min/MaxDatetime and Min/MaxTimestamp endpoints and chrono timezone conversion are total",
    ),
    (
        &[3511],
        Verdict::Unreachable,
        "closed Rust TimeType has exactly DATE, DATETIME, and TIMESTAMP",
    ),
];

fn source_function_keys() -> Vec<String> {
    GO_SOURCE
        .lines()
        .filter_map(|line| line.strip_prefix("func "))
        .map(|signature| {
            if let Some(rest) = signature.strip_prefix('(') {
                let (receiver, rest) = rest.split_once(')').expect("valid Go receiver");
                let receiver = receiver.split_whitespace().last().expect("receiver type");
                let name = rest.trim_start().split_once('(').expect("function name").0;
                format!("{receiver}.{name}")
            } else {
                signature
                    .split_once('(')
                    .expect("function name")
                    .0
                    .to_owned()
            }
        })
        .collect()
}

fn test_support_function_keys(file: &str, source: &str) -> Vec<String> {
    source
        .lines()
        .filter_map(|line| line.strip_prefix("func "))
        .map(|signature| {
            let name = signature
                .split_once('(')
                .expect("test/support function name")
                .0;
            format!("{file}::{name}")
        })
        .collect()
}

fn source_control_lines(start: usize, end: usize) -> Vec<usize> {
    GO_SOURCE
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line_number = index + 1;
            let line = line.trim_start();
            (line_number >= start
                && line_number <= end
                && (line.starts_with("if ")
                    || line.starts_with("} else if ")
                    || line.starts_with("switch ")
                    || line.starts_with("case ")
                    || line.starts_with("default:")))
            .then_some(line_number)
        })
        .collect()
}

#[test]
fn time_go_owner_does_not_drift_before_inventory_review() {
    let actual_hash = format!("{:x}", Sha256::digest(GO_SOURCE.as_bytes()));
    assert_eq!(actual_hash, GO_SOURCE_SHA256, "pkg/types/time.go changed");
    assert_eq!(GO_SOURCE.lines().count(), GO_SOURCE_LINE_COUNT);

    let expected: Vec<_> = EXPECTED_FUNCTION_KEYS.lines().collect();
    let actual = source_function_keys();
    assert_eq!(actual.len(), GO_FUNCTION_COUNT);
    assert_eq!(actual, expected, "Go function declaration surface changed");

    // The SHA catches arbitrary edits; these three structural counts make a
    // control-flow addition explicit in the test failure, so it cannot be
    // mistaken for a harmless comment-only source refresh while the final
    // branch verdict table is being completed.
    let lines: Vec<_> = GO_SOURCE.lines().collect();
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.trim_start().starts_with("if "))
            .count(),
        GO_IF_COUNT
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.trim_start().starts_with("switch "))
            .count(),
        GO_SWITCH_COUNT
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| {
                let line = line.trim_start();
                line.starts_with("case ") || line.starts_with("default:")
            })
            .count(),
        GO_CASE_OR_DEFAULT_COUNT
    );
}

#[test]
fn every_go_temporal_test_and_support_artifact_has_one_live_rust_receipt() {
    assert_eq!(
        format!("{:x}", Sha256::digest(GO_TIME_TEST.as_bytes())),
        GO_TIME_TEST_SHA256,
        "pkg/types/time_test.go changed"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(GO_CORE_TIME_TEST.as_bytes())),
        GO_CORE_TIME_TEST_SHA256,
        "pkg/types/core_time_test.go changed"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(GO_FORMAT_TEST.as_bytes())),
        GO_FORMAT_TEST_SHA256,
        "pkg/types/format_test.go changed"
    );
    assert_eq!(GO_TIME_TEST.lines().count(), 2_340);
    assert_eq!(GO_CORE_TIME_TEST.lines().count(), 348);
    assert_eq!(GO_FORMAT_TEST.lines().count(), 198);

    let mut source = test_support_function_keys("time_test.go", GO_TIME_TEST);
    source.extend(test_support_function_keys(
        "core_time_test.go",
        GO_CORE_TIME_TEST,
    ));
    source.extend(test_support_function_keys("format_test.go", GO_FORMAT_TEST));
    let inventory = GO_TEST_SUPPORT
        .iter()
        .map(|(name, _, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(source.len(), 75);
    assert_eq!(inventory, source);

    for (name, verdict, rust_marker) in GO_TEST_SUPPORT {
        assert!(!name.is_empty());
        assert!(!rust_marker.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        if *verdict == Verdict::Ported {
            assert!(
                RUST_TEST_SUPPORT.contains(rust_marker),
                "PORTED Go test/support artifact {name} lost Rust receipt {rust_marker}"
            );
        }
    }
}

#[test]
fn time_go_function_inventory_has_exactly_one_nonempty_verdict_per_declaration() {
    let actual = source_function_keys();
    let inventory = AUDITED_FUNCTIONS
        .iter()
        .map(|(name, _, _)| *name)
        .collect::<Vec<_>>();
    assert_eq!(&actual[..inventory.len()], inventory);
    assert_eq!(AUDITED_FUNCTIONS.len(), 151);
    assert_eq!(AUDITED_BRANCHES.len(), 286);
    for (name, verdict, reason) in AUDITED_FUNCTIONS.iter().chain(AUDITED_BRANCHES) {
        assert!(!name.is_empty());
        assert!(!reason.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
    }
}

#[test]
fn interval_owner_branch_loci_have_exactly_one_verdict_and_running_evidence() {
    let mut inventory = Vec::new();
    for (lines, verdict, reason) in INTERVAL_BRANCH_GROUPS {
        assert!(!reason.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        inventory.extend_from_slice(lines);
    }
    inventory.sort_unstable();
    assert_eq!(inventory.len(), 131);
    assert!(inventory.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(inventory, source_control_lines(2248, 2762));
}

#[test]
fn format_owner_branch_loci_have_exactly_one_verdict_and_running_evidence() {
    let mut inventory = Vec::new();
    for (lines, verdict, reason) in FORMAT_BRANCH_GROUPS {
        assert!(!reason.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        inventory.extend_from_slice(lines);
    }
    inventory.sort_unstable();
    assert_eq!(inventory.len(), 52);
    assert!(inventory.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(inventory, source_control_lines(2763, 2937));
}

#[test]
fn str_to_date_owner_branch_loci_have_exactly_one_verdict_and_running_evidence() {
    let mut inventory = Vec::new();
    for (lines, verdict, reason) in STR_TO_DATE_BRANCH_GROUPS {
        assert!(!reason.is_empty());
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        inventory.extend_from_slice(lines);
    }
    inventory.sort_unstable();
    assert_eq!(inventory.len(), 92);
    assert!(inventory.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(inventory, source_control_lines(2938, 3554));
}

#[test]
fn every_ported_time_owner_prefix_symbol_still_compiles() {
    fn assert_display<T: std::fmt::Display>() {}

    let _ = crate::core_time_from_datetime::<chrono_tz::Tz>;
    let _ = crate::CoreTime::from_date;
    let _ = crate::Time::from_date_checked;
    let _ = crate::Time::clock;
    let _ = crate::Time::new;
    let _ = crate::Time::go_raw;
    let _ = crate::Time::kind;
    let _ = crate::Time::fsp;
    let _ = crate::Time::set_kind;
    let _ = crate::Time::set_fsp;
    let _ = crate::Time::core_time;
    let _ = crate::Time::set_core_time;
    let _ = crate::Time::current;
    let _ = crate::Time::convert_time_zone::<chrono_tz::Tz, chrono_tz::Tz>;
    let _ = crate::Time::is_zero;
    let _ = crate::Time::invalid_zero;
    let _ = crate::Time::to_number;
    let _ = crate::Time::convert_kind::<chrono_tz::Tz>;
    let _ = crate::Time::to_duration;
    let _ = crate::Time::compare;
    let _ = crate::Time::compare_string::<chrono_tz::Tz>;
    let _ = crate::round_datetime_fraction::<chrono_tz::Tz>;
    let _ = crate::Time::round_frac::<chrono_tz::Tz>;
    let _ = crate::Time::to_go_json;
    let _ = crate::Time::from_go_json;
    let _ = crate::get_fsp;
    let _ = crate::get_frac_index;
    let _ = crate::truncate_datetime_fraction::<chrono_tz::Tz>;
    let _ = crate::Time::to_packed_uint;
    let _ = crate::Time::from_packed_uint;
    let _ = crate::Time::validate::<chrono_tz::Tz>;
    let _ = crate::Time::sub::<chrono_tz::Tz>;
    let _ = crate::Time::add_duration;
    let _ = crate::timestamp_diff;
    let _ = crate::parse_date_format;
    let _ = crate::get_timezone;
    let _ = crate::parse_time::<chrono_tz::Tz>;
    let _ = crate::parse_year;
    let _ = crate::adjust_year;
    let _ = crate::MySqlDuration::new;
    let _ = crate::MySqlDuration::maximum;
    let _ = crate::MySqlDuration::negated;
    let _ = crate::MySqlDuration::checked_add;
    let _ = crate::MySqlDuration::checked_sub;
    let _ = crate::MySqlDuration::duration_format;
    let _ = crate::MySqlDuration::to_number;
    let _ = crate::MySqlDuration::convert_to_time::<chrono_tz::Tz>;
    let _ = crate::MySqlDuration::convert_to_year::<chrono_tz::Tz>;
    let _ = crate::MySqlDuration::round_frac;
    let _ = crate::MySqlDuration::compare;
    let _ = crate::MySqlDuration::compare_string;
    let _ = crate::MySqlDuration::hour;
    let _ = crate::MySqlDuration::minute;
    let _ = crate::MySqlDuration::second;
    let _ = crate::MySqlDuration::microsecond;
    let _ = crate::parse_duration;
    let _ = crate::can_fallback_to_datetime;
    let _ = crate::parse_mysql_duration::<chrono_tz::Tz>;
    let _ = crate::truncate_overflow_mysql_time;
    let _ = crate::parse_time_from_num::<chrono_tz::Tz>;
    let _ = crate::parse_time_from_num_with_zero_date_error::<chrono_tz::Tz>;
    let _ = crate::parse_datetime::<chrono_tz::Tz>;
    let _ = crate::parse_time_from_year;
    let _ = crate::time_from_days;
    let _ = crate::extract_datetime_num;
    let _ = crate::extract_duration_num;
    let _ = crate::parse_duration_value;
    let _ = crate::extract_duration_value;
    let _ = crate::is_clock_unit;
    let _ = crate::is_date_unit;
    let _ = crate::is_microsecond_unit;
    let _ = crate::is_date_format;
    let _ = crate::parse_time_from_int64::<chrono_tz::Tz>;
    let _ = crate::parse_time_from_float64::<chrono_tz::Tz>;
    let _ = crate::parse_time_from_decimal::<chrono_tz::Tz>;
    let _ = crate::Time::date_format;
    let _ = crate::format_int_width;
    let _ = crate::Time::str_to_date_into::<chrono_tz::Tz>;
    let _ = crate::str_to_date::fix_meridiem;
    let _ = crate::str_to_date::parse_format;
    let _ = crate::str_to_date::next_token;
    let _ = crate::str_to_date::skip_whitespace;
    let _ = crate::get_format_type;
    let _ = crate::str_to_date::parse_token;
    let _ = crate::str_to_date::parse_optional_digits;
    let _ = crate::str_to_date::parse_separator;
    let _ = crate::str_to_date::parse_compound_time;
    let _ = crate::str_to_date::parse_year;
    let _ = crate::str_to_date::parse_month_name;
    let _ = crate::str_to_date::has_prefix;
    let _ = crate::date_fsp;
    let _ = crate::Time::is_overflow::<chrono_tz::Tz>;
    let _ = crate::str_to_date::skip_while("", crate::str_to_date::is_go_number);
    let _ = crate::str_to_date::is_go_number;
    let _ = crate::str_to_date::is_go_punctuation;
    let _ = crate::str_to_date::is_go_letter;
    assert_display::<crate::Time>();
    assert_display::<crate::MySqlDuration>();
}
