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

//! Direct truncation policy and `BinaryLiteral.ToInt` source obligations.

use tidb_datatype::{parse_hex_str, TruncationPolicy};
use tidb_error::mysql::{errcode as mysql, FormatArg, SqlError};
use tidb_error::tidb::errcode as tidb;

#[test]
fn binary_literal_to_int_uses_the_source_context_policy() {
    let rows = [
        ("x''", 0, false),
        ("0x00", 0x0, false),
        ("0xff", 0xff, false),
        ("0x10ff", 0x10ff, false),
        ("0x1010ffff", 0x1010ffff, false),
        ("0x1010ffff8080", 0x1010ffff8080, false),
        ("0x1010ffff8080ff12", 0x1010ffff8080ff12, false),
        ("0x1010ffff8080ff12ff", u64::MAX, true),
    ];

    for (input, expected, has_error) in rows {
        let literal = parse_hex_str(input).unwrap();
        let (value, error) = literal.to_int_with_policy(TruncationPolicy::STRICT, |_| {});
        assert_eq!(value, expected, "{input}");
        assert_eq!(error.is_some(), has_error, "{input}");
        if let Some(error) = error {
            assert_eq!(error.code, mysql::ErrTruncatedWrongValue);
            assert_eq!(error.state, "22007");
            assert_eq!(
                error.message,
                "Truncated incorrect BINARY value: '0x1010ffff8080ff12ff'"
            );
        }
    }
}

#[test]
fn ignore_precedes_warning_and_warning_receives_the_original_error() {
    let literal = parse_hex_str("0x1010ffff8080ff12ff").unwrap();

    let mut warnings = Vec::new();
    let (value, error) = literal
        .to_int_with_policy(TruncationPolicy::new(false, true), |warning| {
            warnings.push(warning)
        });
    assert_eq!(value, u64::MAX);
    assert!(error.is_none());
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].code, mysql::ErrTruncatedWrongValue);

    warnings.clear();
    let (value, error) = literal.to_int_with_policy(TruncationPolicy::new(true, true), |warning| {
        warnings.push(warning)
    });
    assert_eq!(value, u64::MAX);
    assert!(error.is_none());
    assert!(warnings.is_empty(), "ignore must win over warning");
}

#[test]
fn handle_truncate_uses_the_exact_source_error_allowlist() {
    let recognized = [
        tidb::ErrTruncatedWrongValue,
        tidb::ErrDataTooLong,
        tidb::ErrTruncatedWrongValueForField,
        tidb::ErrWarnDataOutOfRange,
        tidb::ErrDataOutOfRange,
        tidb::ErrBadNumber,
        tidb::ErrWrongValueForType,
        tidb::ErrDatetimeFunctionOverflow,
        tidb::WarnDataTruncated,
        tidb::ErrIncorrectDatetimeValue,
    ];
    for code in recognized {
        let error = SqlError::new_f(code, "sentinel", &[], &[]);
        assert!(
            TruncationPolicy::new(true, false)
                .handle(Some(error), |_| {})
                .is_none(),
            "recognized code {code}"
        );
    }

    let unknown = SqlError::new_f(999, "unknown", &[], &[FormatArg::from("unused")]);
    let returned = TruncationPolicy::new(true, true).handle(Some(unknown.clone()), |_| {
        panic!("unknown errors are not warnings")
    });
    assert_eq!(returned, Some(unknown));
    assert!(TruncationPolicy::STRICT.handle(None, |_| {}).is_none());
}
