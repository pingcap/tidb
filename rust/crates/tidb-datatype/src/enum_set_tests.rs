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

use crate::{parse_enum, parse_enum_value, parse_set, parse_set_value, Collation, SetParseError};

/// Every row and assertion in `pkg/types/enum_test.go::TestEnum`: ten
/// source-defined rows, thirteen executions across the original collations.
#[test]
fn enum_executes_every_original_test_enum_row_and_assertion() {
    let parse_rows: &[(&[&str], &str, usize)] =
        &[(&["a", "b"], "a", 1), (&["a"], "b", 0), (&["a"], "1", 1)];
    for collation in [Collation::Utf8Mb4Bin, Collation::Utf8UnicodeCi] {
        for &(elements, name, expected) in parse_rows {
            match parse_enum(elements, name, collation) {
                Ok(value) => {
                    assert_ne!(expected, 0);
                    assert_eq!(value.to_string(), elements[expected - 1]);
                    assert_eq!(value.to_number(), expected as f64);
                    assert_eq!(value.copy(), value);
                }
                Err(error) => {
                    assert_eq!(expected, 0);
                    assert_eq!(error.returned_value().to_number(), 0.0);
                    assert_eq!(error.returned_value().to_string(), "");
                    assert_eq!(error.class(), "types");
                    assert_eq!(error.mysql_code(), 1265);
                }
            }
        }
    }

    let ci_rows: &[(&[&str], &str, usize)] = &[
        (&["a", "b"], "A     ", 1),
        (&["a"], "A", 1),
        (&["a"], "b", 0),
        (&["啊"], "啊", 1),
        (&["a"], "1", 1),
    ];
    for &(elements, name, expected) in ci_rows {
        match parse_enum(elements, name, Collation::Utf8GeneralCi) {
            Ok(value) => {
                assert_ne!(expected, 0);
                assert_eq!(value.to_string(), elements[expected - 1]);
                assert_eq!(value.to_number(), expected as f64);
            }
            Err(error) => {
                assert_eq!(expected, 0);
                assert_eq!(error.returned_value().to_number(), 0.0);
                assert_eq!(error.returned_value().to_string(), "");
            }
        }
    }

    for (number, expected) in [(1, 1), (0, 0)] {
        match parse_enum_value(&["a"], number) {
            Ok(value) => {
                assert_ne!(expected, 0);
                assert_eq!(value.to_number(), expected as f64);
            }
            Err(_) => assert_eq!(expected, 0),
        }
    }
}

/// Every row and assertion in `pkg/types/set_test.go::TestSet`: eighteen
/// source-defined rows, twenty-five executions across the original collations.
#[test]
fn set_executes_every_original_test_set_row_and_assertion() {
    let elements = ["a", "b", "c", "d"];
    let parse_rows = [
        ("a", 1, "a"),
        ("a,b,a", 3, "a,b"),
        ("b,a", 3, "a,b"),
        ("a,b,c,d", 15, "a,b,c,d"),
        ("d", 8, "d"),
        ("", 0, ""),
        ("0", 0, ""),
    ];
    for collation in [Collation::Utf8Mb4Bin, Collation::Utf8UnicodeCi] {
        for (name, expected_value, expected_name) in parse_rows {
            let value = parse_set(&elements, name, collation).expect("original ParseSet success");
            assert_eq!(value.to_number(), expected_value as f64);
            assert_eq!(value.to_string(), expected_name);
            assert_eq!(value.copy(), value);
        }
    }

    for (name, expected_value, expected_name) in [("A ", 1, "a"), ("a,B,a", 3, "a,b")] {
        let value = parse_set(&elements, name, Collation::Utf8GeneralCi)
            .expect("original ParseSet_ci success");
        assert_eq!(value.to_number(), expected_value as f64);
        assert_eq!(value.to_string(), expected_name);
    }

    for (number, expected_name) in [(0, ""), (1, "a"), (3, "a,b"), (9, "a,d")] {
        let value = parse_set_value(&elements, number).expect("original ParseSetValue success");
        assert_eq!(value.to_number(), number as f64);
        assert_eq!(value.to_string(), expected_name);
    }

    for name in ["a.e", "e.f"] {
        assert!(parse_set(&elements, name, Collation::Utf8Mb4Bin).is_err());
    }
    for number in [100, 16, 64] {
        assert!(parse_set_value(&elements, number).is_err());
    }
}

#[test]
fn enum_and_set_preserve_exact_error_contexts_and_remaining_bits() {
    let enum_item = parse_enum(&["a"], "bad", Collation::Utf8Mb4Bin).unwrap_err();
    assert_eq!(
        enum_item.context(),
        "convert to MySQL enum failed: item bad is not in enum [a]"
    );
    let enum_boundary = parse_enum_value(&["a"], 2).unwrap_err();
    assert_eq!(
        enum_boundary.context(),
        "convert to MySQL enum failed: number 2 overflow enum boundary [1, 1]"
    );

    let set_error = parse_set(&["a", "b"], "bad", Collation::Utf8Mb4Bin).unwrap_err();
    assert_eq!(set_error.to_string(), "item bad is not in Set [a b]");
    let remaining = parse_set_value(&["a", "b", "c", "d"], 100).unwrap_err();
    assert_eq!(remaining.to_string(), "invalid number 96 for Set [a b c d]");

    let too_many = vec!["a"; 65];
    assert!(matches!(
        parse_set_value(&too_many, 1),
        Err(SetParseError::TooManyElements(65))
    ));
}
