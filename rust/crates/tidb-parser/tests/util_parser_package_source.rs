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

//! Complete source-test translation for `pkg/util/parser`
//! (`ast_test.go` + `parser_test.go`; `main_test.go` is the goleak harness,
//! whose equivalent here is the test runner itself).

use tidb_parser::util_parser::{
    any_char, any_punct, digit, get_default_db, is_punct_byte, is_space_byte, number, one_char,
    restore_with_default_db, simple_cases, space, space0, PatternNotMatch,
};

// Go `TestSimpleCases` (pkg/util/parser/ast_test.go:26).
#[test]
fn test_simple_cases() {
    let tests = [
        (
            "insert into t values(1, 2)",
            "test",
            "insert into test.t values(1, 2)",
        ),
        (
            "insert into mydb.t values(1, 2)",
            "test",
            "insert into mydb.t values(1, 2)",
        ),
        (
            "insert into t(a, b) values(1, 2)",
            "test",
            "insert into test.t(a, b) values(1, 2)",
        ),
        (
            "insert into value value(2, 3)",
            "test",
            "insert into test.value value(2, 3)",
        ),
    ];
    for (sql, db, ans) in tests {
        let stmt = tidb_parser::parse(sql).expect(sql);
        let got = simple_cases(&stmt, db, sql).expect(sql);
        assert_eq!(got, ans, "{sql}");
        // The public entry point takes the same fast path.
        assert_eq!(restore_with_default_db(&stmt, db, sql), ans, "{sql}");
    }
}

// Go `TestSpace` (pkg/util/parser/parser_test.go:24).
#[test]
fn test_space() {
    let ok_table = [(0, " 1", "1"), (0, "1", "1"), (1, "     1", "1"), (2, "  1", "1")];
    for (times, input, expected) in ok_table {
        let rest = space(input, times).expect(input);
        assert_eq!(rest, expected);
    }

    let err_table = [(1, "1"), (2, " 1")];
    for (times, input) in err_table {
        // Go returns (input, err); the Rust error keeps the input untouched.
        assert_eq!(space(input, times), Err(PatternNotMatch), "{input}");
    }
}

// Go `TestDigit` (pkg/util/parser/parser_test.go:57).
#[test]
fn test_digit() {
    let ok_table = [
        (0, "123abc", "123", "abc"),
        (1, "123abc", "123", "abc"),
        (2, "123 @)@)", "123", " @)@)"),
        (3, "456 121", "456", " 121"),
    ];
    for (times, input, expected_digits, expected_rest) in ok_table {
        let (digits, rest) = digit(input, times).expect(input);
        assert_eq!(digits, expected_digits);
        assert_eq!(rest, expected_rest);
    }

    let err_table = [(1, "int"), (2, "1int"), (3, "12 int")];
    for (times, input) in err_table {
        assert_eq!(digit(input, times), Err(PatternNotMatch), "{input}");
    }
}

// Go `TestNumber` (pkg/util/parser/parser_test.go:96).
#[test]
fn test_number() {
    let ok_table = [
        ("123abc", 123, "abc"),
        ("123abc", 123, "abc"),
        ("123 @)@)", 123, " @)@)"),
        ("456 121", 456, " 121"),
    ];
    for (input, expected_num, expected_rest) in ok_table {
        let (num, rest) = number(input).expect(input);
        assert_eq!(num, expected_num);
        assert_eq!(rest, expected_rest);
    }

    let err_table = ["int", "abcint", "@)@)int"];
    for input in err_table {
        assert_eq!(number(input), Err(PatternNotMatch), "{input}");
    }
}

// Go `TestCharAndAnyChar` (pkg/util/parser/parser_test.go:132).
#[test]
fn test_char_and_any_char() {
    let ok_table = [
        (b'i', "int", "nt"),
        (b'1', "1int", "int"),
        (b'1', "12 int", "2 int"),
    ];
    for (c, input, expected) in ok_table {
        let rest = one_char(input, c).expect(input);
        assert_eq!(rest, expected);

        let rest = any_char(input).expect(input);
        assert_eq!(rest, expected);
    }

    let err_table = [(b'i', "xint"), (b'1', "x1int"), (b'1', "x12 int")];
    for (c, input) in err_table {
        assert_eq!(one_char(input, c), Err(PatternNotMatch), "{input}");
    }
}

// Not in the Go test files: pins Go-observed behavior of the surfaces the Go
// tests leave uncovered (GetDefaultDB, AnyPunct/Space byte classes, Space0),
// so their transcreation has its own tripwire.
#[test]
fn uncovered_surfaces_match_go_behavior() {
    // GetDefaultDB: any implicit table -> db name; all qualified -> "".
    let implicit = tidb_parser::parse("select * from t join mydb.u").unwrap();
    assert_eq!(get_default_db(&implicit, "test"), "test");
    let qualified = tidb_parser::parse("select * from mydb.t").unwrap();
    assert_eq!(get_default_db(&qualified, "test"), "");
    let insert_implicit = tidb_parser::parse("insert into t values (1)").unwrap();
    assert_eq!(get_default_db(&insert_implicit, "test"), "test");
    let insert_qualified = tidb_parser::parse("insert into mydb.t values (1)").unwrap();
    assert_eq!(get_default_db(&insert_qualified, "test"), "");

    // Byte classes, generated from Go's unicode.IsPunct/IsSpace over
    // rune(byte): '$' is a symbol (not punct) to Go, NBSP is a space.
    assert!(is_punct_byte(b'!'));
    assert!(is_punct_byte(b'@'));
    assert!(!is_punct_byte(b'$'));
    assert!(!is_punct_byte(b'+'));
    assert!(is_punct_byte(0xBF)); // inverted question mark
    assert!(is_space_byte(b' '));
    assert!(is_space_byte(0xA0)); // NBSP
    assert!(!is_space_byte(b'x'));
    assert_eq!(any_punct(".x"), Ok("x"));
    assert_eq!(any_punct("$x"), Err(PatternNotMatch));

    // Space0 never fails and skips all leading spaces.
    assert_eq!(space0("   x "), "x ");
    assert_eq!(space0("x"), "x");
}
