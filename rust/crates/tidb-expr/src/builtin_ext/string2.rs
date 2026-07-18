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

//! `string2` family builtins — see `super`'s doc for the dispatch contract
//! and `rust/PARALLEL.md` for ownership. Every builtin here is a faithful
//! port of its Go implementation in `pkg/expression/builtin_*.go`, cited
//! per function.

use crate::coerce::coerce_str;
use crate::string_fn::{format_num_locale, substring};
use crate::{Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("SUBSTRING" | "SUBSTR" | "MID", 2) => Some(substring(vals)),
        ("LOCATE", 3) => Some(locate3(vals)),
        ("FORMAT", 3) => Some(format_with_locale(vals)),
        ("FIND_IN_SET", 2) => Some(find_in_set(vals)),
        ("EXPORT_SET", 3..=5) => Some(export_set(vals)),
        ("LTRIM", 1) => Some(ltrim(&vals[0])),
        ("RTRIM", 1) => Some(rtrim(&vals[0])),
        _ => None,
    }
}

/// `LTRIM(str)`, ported from `builtinLTrimSig.evalString` in
/// `pkg/expression/builtin_string.go`.
///
/// TiDB passes the argument through ETString, then calls Go's
/// `strings.TrimLeft(str, " ")`: it removes only U+0020 SPACE, not general
/// Unicode whitespace.  Keeping the character pattern literal prevents Rust
/// from accidentally accepting tabs, CR, or LF as trimmable input.
fn ltrim(value: &Datum) -> Result<Datum, EvalError> {
    Ok(coerce_str(value)?.map_or(Datum::Null, |value| {
        Datum::new_string(value.trim_start_matches(' ').to_string())
    }))
}

/// `RTRIM(str)`, ported from `builtinRTrimSig.evalString` in
/// `pkg/expression/builtin_string.go`; it has the same ETString and
/// U+0020-only contract as [`ltrim`].
fn rtrim(value: &Datum) -> Result<Datum, EvalError> {
    Ok(coerce_str(value)?.map_or(Datum::Null, |value| {
        Datum::new_string(value.trim_end_matches(' ').to_string())
    }))
}

/// `LOCATE(substr, str, pos)`, ported from `builtinLocate3ArgsUTF8Sig` in
/// `pkg/expression/builtin_string.go`.  The value domain has no collation
/// metadata, so this is limited to its fixed default exact-character
/// comparison; session-selected non-binary collations are not represented.
fn locate3(vals: &[Datum]) -> Result<Datum, EvalError> {
    let (Some(needle), Some(haystack), Datum::Int(position)) =
        (coerce_str(&vals[0])?, coerce_str(&vals[1])?, &vals[2])
    else {
        return Ok(Datum::Null);
    };
    let needle: Vec<char> = needle.chars().collect();
    let haystack: Vec<char> = haystack.chars().collect();
    let position = position.wrapping_sub(1);
    if needle.len() > haystack.len()
        || position < 0
        || position as usize > haystack.len() - needle.len()
    {
        return Ok(Datum::Int(0));
    }
    if needle.is_empty() {
        return Ok(Datum::Int(position + 1));
    }
    for start in position as usize..=haystack.len() - needle.len() {
        if haystack[start..start + needle.len()] == needle {
            return Ok(Datum::Int(start as i64 + 1));
        }
    }
    Ok(Datum::Int(0))
}

/// `FORMAT(x, d, locale)`, ported from `builtinFormatWithLocaleSig` in
/// `pkg/expression/builtin_string.go`.  A `NULL` locale uses TiDB's `en_US`
/// fallback; the accompanying unknown-locale warning cannot be represented
/// without the missing statement context.
fn format_with_locale(vals: &[Datum]) -> Result<Datum, EvalError> {
    match coerce_str(&vals[2])? {
        Some(locale) => format_num_locale(vals, &locale),
        None => format_num_locale(vals, "en_US"),
    }
}

/// `FIND_IN_SET(str, strlist)`, ported from `builtinFindInSetSig.evalInt` in
/// `pkg/expression/builtin_string.go`.  The real signature compares collation
/// keys; this UTF-8 value domain has no collation/session metadata, so it
/// preserves the source's exact `KeyWithoutTrimRightSpace` behavior while
/// leaving session-selected collations outside the scalar boundary.
fn find_in_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    let (Some(needle), Some(list)) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    if list.is_empty() {
        return Ok(Datum::Int(0));
    }
    Ok(Datum::Int(
        list.split(',')
            .position(|entry| entry == needle)
            .map_or(0, |index| index as i64 + 1),
    ))
}

/// `EXPORT_SET(bits, on, off[, separator[, number_of_bits]])`, ported from
/// `builtinExportSet{3,4,5}ArgSig` and `exportSet` in
/// `pkg/expression/builtin_string.go`.  This port accepts the native signed
/// integer bits domain; string/decimal coercion would require TiDB's warning
/// context and is intentionally not approximated.
fn export_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Datum::Int(bits) = vals[0] else {
        return Ok(Datum::Null);
    };
    let (Some(on), Some(off)) = (coerce_str(&vals[1])?, coerce_str(&vals[2])?) else {
        return Ok(Datum::Null);
    };
    let separator = if vals.len() >= 4 {
        let Some(separator) = coerce_str(&vals[3])? else {
            return Ok(Datum::Null);
        };
        separator
    } else {
        ",".to_string()
    };
    let mut count = if vals.len() == 5 {
        match vals[4] {
            Datum::Null => return Ok(Datum::Null),
            Datum::Int(count) => count,
            _ => return Err(EvalError::Unsupported("EXPORT_SET number_of_bits coercion")),
        }
    } else {
        64
    };
    if !(0..=64).contains(&count) {
        count = 64;
    }
    let mut parts = Vec::with_capacity(count as usize);
    for bit in 0..count {
        parts.push(if bits & (1_i64 << bit) > 0 {
            on.as_str()
        } else {
            off.as_str()
        });
    }
    Ok(Datum::new_string(parts.join(&separator)))
}

#[cfg(test)]
mod tests {
    use super::{dispatch, locate3};
    use crate::coerce::coerce_str;
    use crate::string_fn::{char_func, format_num, position, substring, to_base64};
    use crate::Datum;

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals)
            .expect("string2 name/arity should dispatch")
            .expect("Go-derived vector should evaluate")
    }

    #[test]
    fn go_string_vectors_cover_new_arities_and_functions() {
        assert_eq!(
            call("SUBSTRING", &[string("Sakila"), Datum::Int(-3)])
                .sql_string()
                .unwrap(),
            "ila"
        );
        assert_eq!(
            call("LOCATE", &[string("A"), string("大A写的A"), Datum::Int(3)])
                .sql_string()
                .unwrap(),
            "5"
        );
        assert_eq!(
            call(
                "FORMAT",
                &[string("1234.567"), Datum::Int(2), string("de_DE")]
            )
            .sql_string()
            .unwrap(),
            "1.234,57"
        );
        assert_eq!(
            call("FIND_IN_SET", &[string("b"), string("a,b,c")])
                .sql_string()
                .unwrap(),
            "2"
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(-6),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5)
                ]
            )
            .sql_string()
            .unwrap(),
            "N,Y,N,Y,Y"
        );
    }

    /// Complete representable rows from `TestChar` and `TestFindInSet` in
    /// `pkg/expression/builtin_string_test.go`.  Charset conversion warnings
    /// are session state, but the default binary CHAR signature and the
    /// source's exact `KeyWithoutTrimRightSpace` comparison are fully scalar
    /// here.
    #[test]
    fn char_and_find_in_set_match_go_source_vectors() {
        assert_eq!(
            char_func(&[
                string("65"),
                Datum::Int(16_740),
                Datum::Real(67.5),
                string("utf8"),
            ]),
            Err(crate::EvalError::Unsupported("CHAR ... USING charset"))
        );
        assert_eq!(
            char_func(&[
                string("65"),
                Datum::Int(16_740),
                Datum::Real(67.5),
                Datum::Null,
            ])
            .unwrap(),
            Datum::new_bytes(b"AAdD".to_vec())
        );
        assert_eq!(
            char_func(&[string("a"), Datum::Int(-1), Datum::Real(67.5), Datum::Null]).unwrap(),
            Datum::new_bytes(vec![0, 0xff, 0xff, 0xff, 0xff, b'D'])
        );

        for (needle, list, want) in [
            ("foo", "foo,bar", 1),
            ("foo", "foobar,bar", 0),
            (" foo ", "foo, foo ", 2),
            ("", "foo,bar,", 3),
            ("", "", 0),
            ("1", "1", 1),
            ("a,b", "a,b,c", 0),
        ] {
            assert_eq!(
                call("FIND_IN_SET", &[string(needle), string(list)]),
                Datum::Int(want),
                "FIND_IN_SET({needle:?}, {list:?})"
            );
        }
        assert_eq!(
            call("FIND_IN_SET", &[Datum::Int(1), Datum::Int(1)]),
            Datum::Int(1)
        );
        assert_eq!(
            call("FIND_IN_SET", &[Datum::Int(1), string("1")]),
            Datum::Int(1)
        );
        assert_eq!(
            call("FIND_IN_SET", &[string("1"), Datum::Int(1)]),
            Datum::Int(1)
        );
        for args in [
            vec![string("foo"), Datum::Null],
            vec![Datum::Null, string("bar")],
        ] {
            assert_eq!(call("FIND_IN_SET", &args), Datum::Null);
        }
    }

    /// Complete value-domain rows from `TestFormat`.  Go warning counts and
    /// unsupported locale errors are session side effects; numeric-prefix
    /// coercion, rounding, precision clamping, grouping, and NULL results
    /// remain directly observable in this evaluator.
    #[allow(clippy::excessive_precision)]
    #[test]
    fn format_matches_go_source_vectors() {
        for (number, precision, want) in [
            (
                Datum::Real(12332.12341111111111111111111111111111111111111),
                Datum::Int(4),
                Some("12,332.1234"),
            ),
            (Datum::Null, Datum::Int(22), None),
            (Datum::Real(1.12345), Datum::Int(4), Some("1.1235")),
            (Datum::Real(9.99999), Datum::Int(4), Some("10.0000")),
            (Datum::Real(1.99999), Datum::Int(4), Some("2.0000")),
            (Datum::Real(1.09999), Datum::Int(4), Some("1.1000")),
            (Datum::Real(-2.5), Datum::Int(0), Some("-3")),
            (
                Datum::Real(12332.123444),
                Datum::Int(4),
                Some("12,332.1234"),
            ),
            (Datum::Real(12332.123444), Datum::Int(0), Some("12,332")),
            (Datum::Real(12332.123444), Datum::Int(-4), Some("12,332")),
            (
                Datum::Real(-12332.123444),
                Datum::Int(4),
                Some("-12,332.1234"),
            ),
            (Datum::Real(-12332.123444), Datum::Int(0), Some("-12,332")),
            (Datum::Real(-12332.123444), Datum::Int(-4), Some("-12,332")),
            (string("12332.123444"), string("4"), Some("12,332.1234")),
            (string("12332.123444A"), string("4"), Some("12,332.1234")),
            (string("-12332.123444"), string("4"), Some("-12,332.1234")),
            (string("-12332.123444A"), string("4"), Some("-12,332.1234")),
            (string("A123345"), string("4"), Some("0.0000")),
            (string("-A123345"), string("4"), Some("0.0000")),
            (string("-12332.123444"), string("A"), Some("-12,332")),
            (string("12332.123444"), string("A"), Some("12,332")),
            (string("-12332.123444"), string("4A"), Some("-12,332.1234")),
            (string("12332.123444"), string("4A"), Some("12,332.1234")),
            (string("-A12332.123444"), string("A"), Some("0")),
            (string("A12332.123444"), string("A"), Some("0")),
            (string("-A12332.123444"), string("4A"), Some("0.0000")),
            (string("A12332.123444"), string("4A"), Some("0.0000")),
            (string("-.12332.123444"), string("4A"), Some("-0.1233")),
            (string(".12332.123444"), string("4A"), Some("0.1233")),
            (
                string("12332.1234567890123456789012345678901"),
                Datum::Int(22),
                Some("12,332.1234567890110000000000"),
            ),
            (
                Datum::Int(1),
                Datum::Int(1024),
                Some("1.000000000000000000000000000000"),
            ),
            (string(""), Datum::Int(1), Some("0.0")),
            (Datum::Int(1), string(""), Some("1")),
        ] {
            let result = format_num(&[number, precision]).unwrap();
            let expected = want.map(string).unwrap_or(Datum::Null);
            assert_eq!(result, expected);
        }
        assert_eq!(
            call(
                "FORMAT",
                &[string("-12332.123456"), Datum::Int(-4), string("zh_CN")]
            ),
            string("-12,332")
        );
        assert_eq!(
            call(
                "FORMAT",
                &[string("-12332.123456"), string("4"), string("de_GE")]
            ),
            string("-12,332.1235")
        );
        assert_eq!(
            call("FORMAT", &[Datum::Int(1), Datum::Int(4), Datum::Null]),
            string("1.0000")
        );
    }

    /// Complete scalar rows from `TestExportSet`.  The Go implementation's
    /// warning/session plumbing is absent, while bit ordering, default and
    /// explicit separators, zero-width output, and out-of-range bit counts
    /// are all value-domain behavior.
    #[test]
    fn export_set_matches_go_source_vectors() {
        let cases = [
            (
                vec![
                    Datum::Int(-9_223_372_036_854_775_807),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5),
                ],
                "Y,N,N,N,N",
            ),
            (
                vec![
                    Datum::Int(-6),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(5),
                ],
                "N,Y,N,Y,Y",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(4),
                ],
                "Y,N,Y,N",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(0),
                ],
                "",
            ),
            (
                vec![
                    Datum::Int(5),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(1),
                ],
                "Y",
            ),
            (
                vec![
                    Datum::Int(6),
                    string("1"),
                    string("0"),
                    string(","),
                    Datum::Int(10),
                ],
                "0,1,1,0,0,0,0,0,0,0",
            ),
            (
                vec![
                    Datum::Int(333_333),
                    string("Ysss"),
                    string("sN"),
                    string("---"),
                    Datum::Int(9),
                ],
                "Ysss---sN---Ysss---sN---Ysss---sN---sN---sN---sN",
            ),
        ];
        for (args, want) in cases {
            assert_eq!(call("EXPORT_SET", &args), string(want));
        }
        assert_eq!(
            call("EXPORT_SET", &[Datum::Int(7), string("Y"), string("N")])
                .sql_string()
                .unwrap()
                .split(',')
                .count(),
            64
        );
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(7),
                    string("Y"),
                    string("N"),
                    Datum::Int(6),
                    Datum::Int(4),
                ]
            ),
            string("Y6Y6Y6N")
        );
        // Go clamps a five-argument bit count above 64 back to the default.
        assert_eq!(
            call(
                "EXPORT_SET",
                &[
                    Datum::Int(7),
                    string("Y"),
                    string("N"),
                    string(","),
                    Datum::Int(133),
                ]
            )
            .sql_string()
            .unwrap()
            .split(',')
            .count(),
            64
        );
    }

    /// Complete value-domain rows from `TestToBase64`, including Go's 76
    /// column wrapping and numeric/NULL coercion.  Charset conversion tests
    /// are represented by passing their resulting GBK bytes directly, which
    /// is exactly the byte boundary consumed by `EvalString`.
    #[test]
    fn to_base64_matches_go_source_vectors() {
        for (input, want) in [
            (string(""), ""),
            (string("abc"), "YWJj"),
            (string("ab c"), "YWIgYw=="),
            (Datum::Int(1), "MQ=="),
            (Datum::Real(1.1), "MS4x"),
            (string("ab\nc"), "YWIKYw=="),
            (string("ab\tc"), "YWIJYw=="),
            (string("qwerty123456"), "cXdlcnR5MTIzNDU2"),
            (string("一二三"), "5LiA5LqM5LiJ"),
        ] {
            assert_eq!(to_base64(&[input]).unwrap(), string(want));
        }
        assert_eq!(to_base64(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(
            to_base64(&[Datum::new_bytes(vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd])]).unwrap(),
            string("0ru2/sj9")
        );
        assert_eq!(
            to_base64(&[Datum::new_bytes(vec![0xff, 0x00])]).unwrap(),
            string("/wA=")
        );
        let long = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        assert_eq!(
            to_base64(&[string(long)]).unwrap(),
            string(
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw=="
            )
        );
        let triple = format!("{long}{long}{long}");
        assert_eq!(
            to_base64(&[string(&triple)]).unwrap(),
            string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv")
        );
    }

    /// Complete representable scalar tables from `TestSubstring` and
    /// `TestLocate` in `pkg/expression/builtin_string_test.go`.  These tests
    /// deliberately call the same value-domain helpers used by the public
    /// dispatcher: the Go source's injected error datum, session-selected
    /// collations, and vector warning plumbing are not values this seed
    /// evaluator can carry, but every ordinary UTF-8 and valid binary-literal
    /// row is still asserted here.
    #[test]
    fn substring_and_locate_match_go_source_vectors() {
        for (input, pos, want) in [
            ("Quadratically", 5, "ratically"),
            ("Sakila", 1, "Sakila"),
            ("Sakila", 2, "akila"),
            ("Sakila", -3, "ila"),
            ("Sakila", 0, ""),
            ("Sakila", 100, ""),
            ("Sakila", -100, ""),
        ] {
            assert_eq!(
                substring(&[string(input), Datum::Int(pos)]).unwrap(),
                string(want),
                "SUBSTRING({input:?}, {pos})"
            );
        }
        for (input, pos, length, want) in [
            ("Quadratically", 5, 6, "ratica"),
            ("Sakila", -5, 3, "aki"),
            ("Sakila", 2, 0, ""),
            ("Sakila", 2, -1, ""),
            ("Sakila", 2, 100, "akila"),
        ] {
            assert_eq!(
                substring(&[string(input), Datum::Int(pos), Datum::Int(length),]).unwrap(),
                string(want),
                "SUBSTRING({input:?}, {pos}, {length})"
            );
        }
        for args in [
            vec![Datum::Null, Datum::Int(2), Datum::Int(3)],
            vec![string("Sakila"), Datum::Null, Datum::Int(3)],
            vec![string("Sakila"), Datum::Int(2), Datum::Null],
        ] {
            assert_eq!(substring(&args).unwrap(), Datum::Null);
        }

        // LOCATE(substr, str) is the two-argument form.  The source's
        // binary rows contain valid UTF-8 bytes, so they exercise the same
        // exact byte/rune result in this value domain without inventing a
        // session collation object.
        let locate2 = [
            ("bar", "foobarbar", 4),
            ("xbar", "foobar", 0),
            ("", "foobar", 1),
            ("foobar", "", 0),
            ("", "", 1),
            ("好世", "你好世界", 2),
            ("界面", "你好世界", 0),
            ("b", "中a英b文", 4),
            ("bAr", "foobArbar", 4),
        ];
        for (needle, haystack, want) in locate2 {
            assert_eq!(
                position(Some(needle.to_string()), Some(haystack.to_string())),
                Datum::Int(want),
                "LOCATE({needle:?}, {haystack:?})"
            );
        }
        for (needle, haystack) in [
            (None, Some("foobar".to_string())),
            (Some("bar".to_string()), None),
            (None, None),
            (Some("".to_string()), None),
            (None, Some("".to_string())),
        ] {
            assert_eq!(position(needle, haystack), Datum::Null);
        }

        let locate3_cases = [
            ("bar", "foobarbar", 5, 7),
            ("xbar", "foobar", 1, 0),
            ("", "foobar", 2, 2),
            ("foobar", "", 1, 0),
            ("", "", 2, 0),
            ("A", "大A写的A", 0, 0),
            ("A", "大A写的A", 1, 2),
            ("A", "大A写的A", 2, 2),
            ("A", "大A写的A", 3, 5),
            ("BaR", "foobarBaR", 5, 7),
            ("foo", "foobar", -1, 0),
        ];
        for (needle, haystack, start, want) in locate3_cases {
            assert_eq!(
                locate3(&[string(needle), string(haystack), Datum::Int(start)]).unwrap(),
                Datum::Int(want),
                "LOCATE({needle:?}, {haystack:?}, {start})"
            );
        }
        for args in [
            vec![Datum::Null, Datum::Null, Datum::Int(1)],
            vec![string(""), Datum::Null, Datum::Int(1)],
            vec![Datum::Null, string(""), Datum::Int(1)],
            vec![Datum::Null, string("bar"), Datum::Int(0)],
            vec![string("bar"), Datum::Null, Datum::Int(-1)],
        ] {
            assert_eq!(locate3(&args).unwrap(), Datum::Null);
        }

        let binary = [
            (Datum::new_bytes(b"BaR".to_vec()), string("foobArbar"), 0),
            (string("BaR"), Datum::new_bytes(b"foobArbar".to_vec()), 0),
            (Datum::new_bytes(b"bAr".to_vec()), string("foobarBaR"), 0),
            (string("bAr"), Datum::new_bytes(b"foobarBaR".to_vec()), 0),
            (string("bAr"), Datum::new_bytes(b"foobarbAr".to_vec()), 7),
        ];
        for (needle, haystack, want) in binary {
            assert_eq!(
                position(coerce_str(&needle).unwrap(), coerce_str(&haystack).unwrap(),),
                Datum::Int(want)
            );
        }
    }

    #[test]
    fn substring_positive_length_overflow_matches_go_int64_boundary() {
        // `builtinSubstring3ArgsUTF8Sig` computes `end := pos + length` as
        // int64 and returns an empty string when that addition wraps.  This
        // catches the tempting-but-wrong Rust `saturating_add` translation.
        assert_eq!(
            substring(&[string("Sakila"), Datum::Int(2), Datum::Int(i64::MAX),]).unwrap(),
            string("")
        );
        // At position one the same maximum length does not overflow and
        // therefore returns the complete string.
        assert_eq!(
            substring(&[string("Sakila"), Datum::Int(1), Datum::Int(i64::MAX),]).unwrap(),
            string("Sakila")
        );
    }

    #[test]
    fn ltrim_and_rtrim_preserve_non_space_whitespace() {
        // Exact vectors from TestLTrim/TestRTrim in
        // pkg/expression/builtin_string_test.go. These are intentionally not
        // `str::trim*`: Go's `spaceChars` is the one-byte ASCII space.
        let cases = [
            ("LTRIM", "   bar   ", "bar   "),
            ("LTRIM", "\t   bar   ", "\t   bar   "),
            ("LTRIM", "   \rbar   ", "\rbar   "),
            ("LTRIM", "   \nbar   ", "\nbar   "),
            ("RTRIM", "   bar   ", "   bar"),
            ("RTRIM", "bar     \n", "bar     \n"),
            ("RTRIM", "bar\n     ", "bar\n"),
            ("RTRIM", "bar     \t", "bar     \t"),
            ("RTRIM", "bar\t     ", "bar\t"),
        ];
        for (name, input, want) in cases {
            assert_eq!(
                call(name, &[string(input)]),
                string(want),
                "{name}({input:?})"
            );
        }
        for name in ["LTRIM", "RTRIM"] {
            assert_eq!(call(name, &[Datum::Null]), Datum::Null);
            assert_eq!(call(name, &[Datum::Int(123)]), string("123"));
            assert!(dispatch(name, &[]).is_none());
        }
    }

    #[test]
    fn to_base64_wraps_at_the_go_76_column_boundary() {
        let input = string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");
        assert_eq!(
            to_base64(&[input]).unwrap().sql_string().unwrap(),
            "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw=="
        );
    }

    #[test]
    fn char_preserves_go_four_byte_signed_integer_encoding() {
        assert_eq!(
            char_func(&[
                Datum::Int(65),
                Datum::Int(16_740),
                Datum::Int(67),
                Datum::Null
            ])
            .unwrap()
            .sql_string()
            .unwrap(),
            "AAdC"
        );
        assert_eq!(
            char_func(&[Datum::Int(-1), Datum::Null]).unwrap(),
            Datum::new_bytes(vec![0xff; 4])
        );
    }
}
