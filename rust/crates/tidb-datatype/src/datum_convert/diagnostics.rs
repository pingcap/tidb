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

use crate::parser_types_errors::{ERR_OVERFLOW, ERR_TRUNCATED_WRONG_VALUE};
use crate::{ConversionContext, Datum, ScalarConversionError, ScalarConversionEvent};
use tidb_error::terror::TerrorError;

/// Go Datum.ConvertTo's best-effort value and final error. Warnings are
/// delivered in order to the caller's existing context, not stored here.
#[derive(Debug)]
pub struct DatumConversion {
    /// Value retained even when conversion reports a fatal condition.
    pub value: Datum,
    /// The original typed error after conversion-stage precedence is applied.
    pub error: Option<TerrorError>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConversionLocation, ConversionWarningAppender, DatumKind, FieldType, FieldTypeCode,
        FieldTypeFlags, SessionTimeZone, DEFAULT_STATEMENT_FLAGS,
    };
    use std::cell::RefCell;

    #[derive(Default)]
    struct Warnings(RefCell<Vec<String>>);

    impl ConversionWarningAppender for Warnings {
        fn append_conversion_warning(&self, error: TerrorError) {
            self.0.borrow_mut().push(error.to_string());
        }
    }

    #[derive(serde::Deserialize)]
    struct OracleRow {
        name: String,
        mode: String,
        input: String,
        code: u8,
        flen: i64,
        decimal: i64,
        charset: String,
        collation: String,
        unsigned: bool,
        value: String,
        kind: u8,
        error: String,
        warnings: Option<Vec<String>>,
    }

    #[test]
    fn contextual_conversion_matches_go_values_errors_and_ordered_warnings() {
        // Actual types.Datum.ConvertTo outputs at Go revision
        // 23bff313186b8ceb61fcbe9faac43ae700e7fb14, not Rust-generated answers.
        let fixture = include_str!("../../tests/datum_conversion_go_fixture.jsonl");
        for line in fixture.lines() {
            let row: OracleRow = serde_json::from_str(line).unwrap();
            let case = format!("{} / {}", row.name, row.mode);
            let flags = match row.mode.as_str() {
                "strict" => DEFAULT_STATEMENT_FLAGS,
                "warn" => DEFAULT_STATEMENT_FLAGS.with_truncate_as_warning(true),
                "ignore" => DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true),
                mode => panic!("unexpected mode {mode}"),
            };
            let mut target = FieldType::new(FieldTypeCode::from_mysql_type(row.code))
                .with_flen(row.flen)
                .with_decimal(row.decimal)
                .with_collation_name(row.collation)
                .with_charset_name(row.charset);
            if row.unsigned {
                target = target.with_added_flags(FieldTypeFlags::UNSIGNED);
            }
            let warnings = Warnings::default();
            let context = ConversionContext::new(flags, ConversionLocation::UTC, &warnings);
            let converted = Datum::new_string(row.input)
                .convert_to_in_context(&target, &context, &SessionTimeZone::utc())
                .unwrap_or_else(|error| panic!("{case}: {error}"));
            assert_eq!(
                converted.value.sql_string().unwrap(),
                row.value,
                "{case}: value"
            );
            let kind = match row.kind {
                1 => DatumKind::Int,
                3 => DatumKind::Float32,
                4 => DatumKind::Real,
                5 => DatumKind::String,
                6 => DatumKind::Bytes,
                8 => DatumKind::Decimal,
                kind => panic!("unexpected Go datum kind {kind}"),
            };
            assert_eq!(converted.value.kind(), kind, "{case}: kind");
            assert_eq!(
                converted.error.map(|e| e.to_string()).unwrap_or_default(),
                row.error,
                "{case}: error"
            );
            assert_eq!(
                *warnings.0.borrow(),
                row.warnings.unwrap_or_default(),
                "{case}: warnings"
            );
        }
    }

    #[test]
    fn unsigned_contextual_conversion_matches_go_diagnostics() {
        // Captured TestRefineUnsignedDiagnosticsOracle: strict/warning modes,
        // parser errors, narrow-width overflow precedence and warning retention.
        for (input, code, warn, expected, error) in [
            (
                "12tail",
                1,
                false,
                12_u64,
                Some("[types:1292]Truncated incorrect DOUBLE value: '12tail'"),
            ),
            (
                "255.1tail",
                1,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '255.1'"),
            ),
            (
                "256tail",
                1,
                false,
                255_u64,
                Some("[types:1690]constant 256 overflows tinyint"),
            ),
            (
                "3.5tail",
                1,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '3.5'"),
            ),
            (
                "-1",
                1,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-1'"),
            ),
            (
                "-1e100",
                1,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-9223372036854775808'"),
            ),
            (
                "1e100",
                1,
                false,
                255_u64,
                Some("[types:1690]constant 18446744073709551615 overflows tinyint"),
            ),
            (
                "18446744073709551616",
                1,
                false,
                255_u64,
                Some("[types:1690]constant 18446744073709551615 overflows tinyint"),
            ),
            (
                "12tail",
                8,
                false,
                12_u64,
                Some("[types:1292]Truncated incorrect DOUBLE value: '12tail'"),
            ),
            (
                "255.1tail",
                8,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '255.1'"),
            ),
            (
                "256tail",
                8,
                false,
                256_u64,
                Some("[types:1292]Truncated incorrect DOUBLE value: '256tail'"),
            ),
            (
                "3.5tail",
                8,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '3.5'"),
            ),
            (
                "-1",
                8,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-1'"),
            ),
            (
                "-1e100",
                8,
                false,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-9223372036854775808'"),
            ),
            (
                "1e100",
                8,
                false,
                18446744073709551615_u64,
                Some("[types:1690]BIGINT value is out of range in '1e100'"),
            ),
            (
                "18446744073709551616",
                8,
                false,
                18446744073709551615_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '18446744073709551616'"),
            ),
            ("12tail", 1, true, 12_u64, None),
            ("255.1tail", 1, true, 255_u64, None),
            (
                "256tail",
                1,
                true,
                255_u64,
                Some("[types:1690]constant 256 overflows tinyint"),
            ),
            ("3.5tail", 1, true, 4_u64, None),
            (
                "-1",
                1,
                true,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-1'"),
            ),
            (
                "-1e100",
                1,
                true,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-9223372036854775808'"),
            ),
            (
                "1e100",
                1,
                true,
                255_u64,
                Some("[types:1690]constant 18446744073709551615 overflows tinyint"),
            ),
            (
                "18446744073709551616",
                1,
                true,
                255_u64,
                Some("[types:1690]constant 18446744073709551615 overflows tinyint"),
            ),
            ("12tail", 8, true, 12_u64, None),
            ("255.1tail", 8, true, 255_u64, None),
            ("256tail", 8, true, 256_u64, None),
            ("3.5tail", 8, true, 4_u64, None),
            (
                "-1",
                8,
                true,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-1'"),
            ),
            (
                "-1e100",
                8,
                true,
                0_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '-9223372036854775808'"),
            ),
            (
                "1e100",
                8,
                true,
                18446744073709551615_u64,
                Some("[types:1690]BIGINT value is out of range in '1e100'"),
            ),
            (
                "18446744073709551616",
                8,
                true,
                18446744073709551615_u64,
                Some("[types:1690]BIGINT UNSIGNED value is out of range in '18446744073709551616'"),
            ),
        ] {
            let warnings = Warnings::default();
            let context = ConversionContext::new(
                DEFAULT_STATEMENT_FLAGS.with_truncate_as_warning(warn),
                ConversionLocation::UTC,
                &warnings,
            );
            let target = FieldType::new(FieldTypeCode::from_mysql_type(code)).with_unsigned(true);
            let converted = Datum::new_string(input)
                .convert_to_in_context(&target, &context, &SessionTimeZone::utc())
                .unwrap();
            assert_eq!(
                converted.value,
                Datum::UInt(expected),
                "{input} / {code} / {warn}"
            );
            assert_eq!(
                converted.error.as_ref().map(ToString::to_string).as_deref(),
                error,
                "{input} / {code} / {warn}"
            );
            let expected_warnings = if warn && input.ends_with("tail") {
                vec![format!(
                    "[types:1292]Truncated incorrect DOUBLE value: '{input}'"
                )]
            } else {
                vec![]
            };
            assert_eq!(
                *warnings.0.borrow(),
                expected_warnings,
                "{input} / {code} / {warn}"
            );
        }
    }

    #[test]
    fn contextual_integer_conversion_reports_numeric_overflow() {
        let context = ConversionContext::strict();
        let zone = SessionTimeZone::utc();
        for (input, target, expected) in [
            (
                Datum::Int(256),
                FieldType::new(FieldTypeCode::Tiny).with_added_flags(FieldTypeFlags::UNSIGNED),
                Datum::UInt(255),
            ),
            (
                Datum::Decimal(crate::Decimal::parse_mysql("-0.1").0),
                FieldType::new(FieldTypeCode::Tiny).with_added_flags(FieldTypeFlags::UNSIGNED),
                Datum::UInt(0),
            ),
            (
                Datum::Decimal(crate::Decimal::parse_mysql("9223372036854775808.1").0),
                FieldType::new(FieldTypeCode::LongLong),
                Datum::Int(i64::MAX),
            ),
        ] {
            let result = input
                .convert_to_in_context(&target, &context, &zone)
                .unwrap();
            assert_eq!(result.value, expected);
            assert_eq!(result.error.unwrap().code().value(), 1690);
        }
    }

    #[test]
    fn contextual_conversion_retains_caller_warnings_without_leaking_errors() {
        let warnings = Warnings::default();
        let context = ConversionContext::new(
            DEFAULT_STATEMENT_FLAGS.with_truncate_as_warning(true),
            ConversionLocation::UTC,
            &warnings,
        );
        let target = FieldType::new(FieldTypeCode::Tiny);
        let zone = SessionTimeZone::utc();
        let overflow = Datum::new_string("128tail")
            .convert_to_in_context(&target, &context, &zone)
            .unwrap();
        assert_eq!(overflow.value, Datum::Int(127));
        assert_eq!(overflow.error.unwrap().code().value(), 1690);
        let exact = Datum::Int(7)
            .convert_to_in_context(&target, &context, &zone)
            .unwrap();
        assert_eq!(exact.value, Datum::Int(7));
        assert!(exact.error.is_none());
        assert_eq!(warnings.0.borrow().len(), 1);

        // Ignore wins when both bits are set. Existing caller-owned warnings
        // are neither cleared nor duplicated by another conversion.
        let ignored = context.with_flags(context.flags().with_ignore_truncate_err(true));
        let converted = Datum::new_string("12tail")
            .convert_to_in_context(&target, &ignored, &zone)
            .unwrap();
        assert_eq!(converted.value, Datum::Int(12));
        assert!(converted.error.is_none());
        assert_eq!(
            *warnings.0.borrow(),
            ["[types:1292]Truncated incorrect DOUBLE value: '128tail'"]
        );

        let null = Datum::Null
            .convert_to_in_context(&target, &context, &zone)
            .unwrap();
        assert_eq!(null.value, Datum::Null);
        assert!(null.error.is_none());
        assert_eq!(warnings.0.borrow().len(), 1);
    }
}

/// Shared conversion stages use this sink for diagnostic identity before
/// reducing their low-level result to the legacy single-event interface.
/// Disabled sinks do not build messages or allocate warning containers.
pub(crate) struct Diagnostics<'a, 'w> {
    context: Option<&'a ConversionContext<'w>>,
    pub(super) error: Option<TerrorError>,
    pub(super) unmapped: bool,
}

impl<'a, 'w> Diagnostics<'a, 'w> {
    pub(crate) fn new(context: Option<&'a ConversionContext<'w>>) -> Self {
        Self {
            context,
            error: None,
            unmapped: false,
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.context.is_some()
    }

    pub(crate) fn unhandled(&mut self, event: Option<&ScalarConversionEvent>) {
        self.unmapped |= event.is_some();
    }

    pub(super) fn unreported<T>(
        &mut self,
        converted: Result<crate::Converted<T>, crate::DatumValueError>,
    ) -> Result<crate::Converted<T>, crate::DatumValueError> {
        let converted = converted?;
        self.unhandled(converted.event.as_ref());
        Ok(converted)
    }

    pub(crate) fn error(&mut self, make: impl FnOnce() -> TerrorError) {
        if self.context.is_some() && self.error.is_none() {
            self.error = Some(make());
        }
    }

    pub(crate) fn replace_error(&mut self, make: impl FnOnce() -> TerrorError) {
        if self.context.is_some() {
            self.error = Some(make());
        }
    }

    pub(super) fn warn(&mut self, make: impl FnOnce() -> TerrorError) {
        if let Some(context) = self.context {
            context.append_warning(make());
        }
    }

    pub(super) fn truncate(&mut self, make: impl FnOnce() -> TerrorError) {
        if let Some(context) = self.context {
            if let Some(error) = context.handle_truncate(Some(make())) {
                if self.error.is_none() {
                    self.error = Some(error);
                }
            }
        }
    }

    pub(super) fn numeric_overflow(&mut self, event: Option<&ScalarConversionEvent>) {
        match event {
            None => {}
            Some(ScalarConversionEvent::Overflow(ScalarConversionError::Overflow {
                value,
                target,
            })) => {
                self.error(|| {
                    ERR_OVERFLOW.generate(format!(
                        "constant {value} overflows {}",
                        crate::type_str(*target),
                    ))
                });
            }
            Some(_) => self.unmapped = true,
        }
    }

    pub(crate) fn truncated_numeric_input(&mut self, input: &str) {
        self.truncate(|| {
            ERR_TRUNCATED_WRONG_VALUE
                .generate(format!("Truncated incorrect DOUBLE value: '{input}'",))
        });
    }

    pub(crate) fn parsed_integer(&mut self, event: Option<&ScalarConversionEvent>) {
        match event {
            None | Some(ScalarConversionEvent::Truncated) => {}
            Some(ScalarConversionEvent::Overflow(ScalarConversionError::Overflow {
                value,
                ..
            })) => {
                // StrToInt's ParseInt failure replaces its prefix error.
                self.replace_error(|| {
                    ERR_OVERFLOW.generate(format!("BIGINT value is out of range in '{value}'",))
                });
            }
            Some(_) => self.unmapped = true,
        }
    }
}
