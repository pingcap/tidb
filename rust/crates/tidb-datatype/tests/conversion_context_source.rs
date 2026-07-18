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

//! Direct obligations from `pkg/types/context_test.go` plus the declared
//! BinaryLiteral consumer integration.

use std::sync::Mutex;

use tidb_datatype::{
    parse_hex_str, ConversionContext, ConversionFlags, ConversionLocation,
    ConversionWarningAppender, DEFAULT_STATEMENT_FLAGS, STRICT_FLAGS,
};
use tidb_error::terror::{TerrorClass, TerrorError};

#[derive(Default)]
struct WarningStore(Mutex<Vec<TerrorError>>);

impl ConversionWarningAppender for WarningStore {
    fn append_conversion_warning(&self, warning: TerrorError) {
        self.0.lock().unwrap().push(warning);
    }
}

#[test]
fn test_with_new_flags_preserves_original_location_and_sink() {
    let warnings = WarningStore::default();
    let context = ConversionContext::new(
        STRICT_FLAGS.with_skip_ascii_check(true),
        ConversionLocation::UTC,
        &warnings,
    );
    let derived = context.with_flags(STRICT_FLAGS.with_skip_utf8_check(true));

    assert!(context.flags().skip_ascii_check());
    assert!(!context.flags().skip_utf8_check());
    assert!(!derived.flags().skip_ascii_check());
    assert!(derived.flags().skip_utf8_check());
    assert_eq!(context.location(), &ConversionLocation::UTC);
    assert_eq!(derived.location(), &ConversionLocation::UTC);
}

#[test]
fn test_simple_on_off_flags_matches_source_bit_operations() {
    type FlagCase = (
        u16,
        fn(ConversionFlags) -> bool,
        fn(ConversionFlags, bool) -> ConversionFlags,
    );
    let cases: &[FlagCase] = &[
        (
            ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED,
            ConversionFlags::allow_negative_to_unsigned,
            ConversionFlags::with_allow_negative_to_unsigned,
        ),
        (
            ConversionFlags::SKIP_ASCII_CHECK,
            ConversionFlags::skip_ascii_check,
            ConversionFlags::with_skip_ascii_check,
        ),
        (
            ConversionFlags::SKIP_UTF8_CHECK,
            ConversionFlags::skip_utf8_check,
            ConversionFlags::with_skip_utf8_check,
        ),
        (
            ConversionFlags::SKIP_UTF8MB4_CHECK,
            ConversionFlags::skip_utf8mb4_check,
            ConversionFlags::with_skip_utf8mb4_check,
        ),
    ];

    for &(bit, read, write) in cases {
        assert!(!read(STRICT_FLAGS));
        assert!(!read(ConversionFlags::from_bits(0)));
        assert!(read(ConversionFlags::from_bits(bit)));

        let set_empty = write(ConversionFlags::from_bits(0), true);
        assert_eq!(set_empty.bits(), bit);
        assert!(read(set_empty));
        let set_full = write(ConversionFlags::from_bits(u16::MAX), true);
        assert_eq!(set_full.bits(), u16::MAX);
        assert!(read(set_full));

        let clear_empty = write(ConversionFlags::from_bits(0), false);
        assert_eq!(clear_empty.bits(), 0);
        assert!(!read(clear_empty));
        let clear_full = write(ConversionFlags::from_bits(u16::MAX), false);
        assert_eq!(clear_full.bits(), !bit);
        assert!(!read(clear_full));
    }
}

#[test]
fn remaining_source_flags_and_context_accessors_are_exact() {
    let flags = STRICT_FLAGS
        .with_ignore_truncate_err(true)
        .with_truncate_as_warning(true)
        .with_ignore_zero_date_err(true)
        .with_ignore_zero_in_date_err(true)
        .with_ignore_invalid_date_err(true)
        .with_cast_time_to_year_through_concat(true);
    assert!(flags.ignore_truncate_err());
    assert!(flags.truncate_as_warning());
    assert!(flags.ignore_zero_date_err());
    assert!(flags.ignore_zero_in_date_err());
    assert!(flags.ignore_invalid_date_err());
    assert!(flags.cast_time_to_year_through_concat());
    assert_eq!(
        DEFAULT_STATEMENT_FLAGS.bits(),
        ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED | ConversionFlags::IGNORE_ZERO_DATE_ERR
    );

    let warnings = WarningStore::default();
    let context = ConversionContext::new(flags, ConversionLocation::UTC, &warnings);
    let shanghai = context.with_location(ConversionLocation::named("Asia/Shanghai"));
    assert_eq!(context.location().name(), "UTC");
    assert_eq!(shanghai.location().name(), "Asia/Shanghai");
    assert_eq!(shanghai.flags(), flags);
}

#[test]
fn binary_literal_uses_context_warning_and_shared_terror_identity() {
    let literal = parse_hex_str("0x1010ffff8080ff12ff").unwrap();

    let strict = ConversionContext::strict();
    let (value, error) = literal.to_int_with_context(&strict);
    assert_eq!(value, u64::MAX);
    let error = error.expect("strict context returns truncation");
    assert_eq!(error.class(), TerrorClass::Types);
    assert_eq!(error.rfc_code(), "types:1292");
    assert_eq!(
        error.message(),
        "Truncated incorrect BINARY value: '0x1010ffff8080ff12ff'"
    );

    let warnings = WarningStore::default();
    let warning_context = ConversionContext::new(
        STRICT_FLAGS.with_truncate_as_warning(true),
        ConversionLocation::UTC,
        &warnings,
    );
    let (_, error) = literal.to_int_with_context(&warning_context);
    assert!(error.is_none());
    let retained = warnings.0.lock().unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].rfc_code(), "types:1292");
    drop(retained);

    let ignore_context =
        warning_context.with_flags(warning_context.flags().with_ignore_truncate_err(true));
    let (_, error) = literal.to_int_with_context(&ignore_context);
    assert!(error.is_none());
    assert_eq!(warnings.0.lock().unwrap().len(), 1, "ignore wins");
}
