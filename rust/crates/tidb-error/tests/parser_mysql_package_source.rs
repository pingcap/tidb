// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Exact translation of `pkg/parser/mysql/error_test.go` plus catalog/state
//! invariants required by the complete package claim.

use tidb_error::mysql::errcode::{ErrNoDB, ErrWrongValueForVar};
use tidb_error::mysql::{mysql_state, FormatArg, SqlError, CATALOG, DEFAULT_MYSQL_STATE};

#[test]
fn test_sql_error() {
    let error = SqlError::new_f(ErrNoDB, "no db error", &[], &[]);
    assert_eq!(error.to_string(), "ERROR 1046 (3D000): no db error");
    let error = SqlError::new_f(0, "customized error", &[], &[]);
    assert_eq!(error.to_string(), "ERROR 0 (HY000): customized error");
    let error = SqlError::new(ErrNoDB, &[]);
    assert_eq!(
        error.to_string(),
        "ERROR 1046 (3D000): No database selected"
    );
    let error = SqlError::new(0, &[FormatArg::from("customized error"), FormatArg::nil()]);
    assert_eq!(error.to_string(), "ERROR 0 (HY000): customized error<nil>");
}

#[test]
fn generated_catalog_and_state_are_total_for_source_contracts() {
    assert_eq!(CATALOG.len(), 952);
    assert_eq!(mysql_state(ErrNoDB), "3D000");
    assert_eq!(mysql_state(0), DEFAULT_MYSQL_STATE);
    assert_eq!(
        SqlError::new(ErrWrongValueForVar, &["sql_mode".into(), "bad".into()]).message,
        "Variable 'sql_mode' can't be set to the value of 'bad'"
    );
    assert_eq!(
        SqlError::new_f(0, "command %d not supported now", &[], &[3_u8.into()]).message,
        "command 3 not supported now"
    );
    assert_eq!(
        SqlError::new(0, &[1_i64.into(), 2_i64.into()]).message,
        "1 2"
    );
}

fn render(format: &str, argument: FormatArg) -> String {
    SqlError::new_f(0, format, &[], &[argument]).message
}

#[test]
fn generic_formatter_preserves_typed_go_diagnostics() {
    macro_rules! signed_matrix {
        ($value:expr, $name:literal) => {{
            let argument = FormatArg::from($value);
            assert_eq!(
                render("%s", argument.clone()),
                concat!("%!s(", $name, "=7)")
            );
            assert_eq!(render("%d", argument.clone()), "7");
            assert_eq!(render("%v", argument.clone()), "7");
            assert_eq!(render("%T", argument), $name);
        }};
    }
    signed_matrix!(7_i8, "int8");
    signed_matrix!(7_i16, "int16");
    signed_matrix!(7_i32, "int32");
    signed_matrix!(7_i64, "int64");
    signed_matrix!(7_isize, "int");

    macro_rules! unsigned_matrix {
        ($value:expr, $name:literal) => {{
            let argument = FormatArg::from($value);
            assert_eq!(
                render("%s", argument.clone()),
                concat!("%!s(", $name, "=7)")
            );
            assert_eq!(render("%d", argument.clone()), "7");
            assert_eq!(render("%#v", argument.clone()), "0x7");
            assert_eq!(render("%T", argument), $name);
        }};
    }
    unsigned_matrix!(7_u8, "uint8");
    unsigned_matrix!(7_u16, "uint16");
    unsigned_matrix!(7_u32, "uint32");
    unsigned_matrix!(7_u64, "uint64");
    unsigned_matrix!(7_usize, "uint");

    let boolean = FormatArg::from(true);
    assert_eq!(render("%d", boolean.clone()), "%!d(bool=true)");
    assert_eq!(render("%q", boolean.clone()), "%!q(bool=true)");
    assert_eq!(render("%t", boolean), "true");

    for (argument, name) in [
        (FormatArg::from(1.5_f32), "float32"),
        (FormatArg::from(1.5_f64), "float64"),
    ] {
        assert_eq!(render("%d", argument.clone()), format!("%!d({name}=1.5)"));
        assert_eq!(render("%q", argument.clone()), format!("%!q({name}=1.5)"));
        assert_eq!(render("%f", argument.clone()), "1.500000");
        assert_eq!(render("%T", argument), name);
    }
    let large = FormatArg::from(1e20_f64);
    assert_eq!(render("%v", large.clone()), "1e+20");
    assert_eq!(render("%e", large.clone()), "1.000000e+20");
    assert_eq!(render("%E", large.clone()), "1.000000E+20");
    assert_eq!(render("%G", large.clone()), "1E+20");
    assert_eq!(render("%x", large), "0x1.5af1d78b58c4p+66");
    let infinity = FormatArg::from(f64::INFINITY);
    assert_eq!(render("%v", infinity.clone()), "+Inf");
    assert_eq!(render("%f", infinity.clone()), "+Inf");
    assert_eq!(render("%d", infinity), "%!d(float64=+Inf)");

    assert_eq!(render("%x", FormatArg::from(-7_i64)), "-7");
    assert_eq!(
        render("%x", FormatArg::from(f64::MIN_POSITIVE / 2_f64.powi(52))),
        "0x1p-1074"
    );
    assert_eq!(
        render("%X", FormatArg::from(f32::MIN_POSITIVE / 2_f32.powi(23))),
        "0X1P-149"
    );
}

#[test]
fn generic_formatter_quotes_strings_and_renders_rust_char_as_rune() {
    let string = FormatArg::from("hello\n");
    assert_eq!(render("%q", string.clone()), "\"hello\\n\"");
    assert_eq!(render("%x", string.clone()), "68656c6c6f0a");
    assert_eq!(render("%.2x", string.clone()), "6865");
    assert_eq!(render("%d", string), "%!d(string=hello\n)");

    let character = FormatArg::from('A');
    assert_eq!(render("%v", character.clone()), "65");
    assert_eq!(render("%d", character.clone()), "65");
    assert_eq!(render("%s", character.clone()), "%!s(int32=65)");
    assert_eq!(render("%c", character.clone()), "A");
    assert_eq!(render("%q", character.clone()), "'A'");
    assert_eq!(render("%T", character), "int32");

    assert_eq!(
        SqlError::new_f(
            0,
            "%q %s",
            &[],
            &[FormatArg::from(true), FormatArg::from("next")],
        )
        .message,
        "%!q(bool=true) next"
    );

    assert_eq!(render("%.2s", FormatArg::from("hello")), "he");
    assert_eq!(render("%.2d", FormatArg::from(7_i64)), "07");
    assert_eq!(render("%.2f", FormatArg::from(1.5_f64)), "1.50");
    assert_eq!(
        render("%.2d", FormatArg::from(true)),
        "%!d(bool=true)",
        "precision must not truncate a mismatch diagnostic"
    );

    assert_eq!(
        SqlError::new(0, &[FormatArg::from('A')]).message,
        "65",
        "fmt.Sprint renders a rune's numeric codepoint"
    );
}

#[test]
fn generic_formatter_honors_go_flags_width_and_verb_precision() {
    for (format, expected) in [
        ("%+d", "+7"),
        ("%05d", "00007"),
        ("%-5d", "7    "),
        ("% d", " 7"),
    ] {
        assert_eq!(render(format, FormatArg::from(7_i64)), expected);
    }
    assert_eq!(render("%05d", FormatArg::from(-7_i64)), "-0007");
    assert_eq!(render("% d", FormatArg::from(-7_i64)), "-7");

    assert_eq!(render("%.2g", FormatArg::from(123.45_f64)), "1.2e+02");
    assert_eq!(render("%.2G", FormatArg::from(123.45_f64)), "1.2E+02");
    assert_eq!(render("%#.0e", FormatArg::from(1.0_f64)), "1.e+00");
    assert_eq!(render("%#.0e", FormatArg::from(1.5_f64)), "2.e+00");
    assert_eq!(render("%#.0E", FormatArg::from(1.0_f64)), "1.E+00");
    assert_eq!(render("%#.3g", FormatArg::from(1.2_f64)), "1.20");
    assert_eq!(render("%#.3G", FormatArg::from(1.2_f64)), "1.20");

    assert_eq!(render("%.2x", FormatArg::from(1.5_f64)), "0x1.80p+00");
    assert_eq!(render("%.2X", FormatArg::from(1.5_f64)), "0X1.80P+00");
    assert_eq!(render("%.0x", FormatArg::from(1.5_f64)), "0x1p+01");
    assert_eq!(render("%#.0x", FormatArg::from(1.5_f64)), "0x1.p+01");
    assert_eq!(render("%#.0X", FormatArg::from(1.5_f64)), "0X1.P+01");
    assert_eq!(
        render("%.2x", FormatArg::from(f64::MIN_POSITIVE / 2_f64.powi(52))),
        "0x1.00p-1074"
    );
    assert_eq!(
        render("%.2X", FormatArg::from(f32::MIN_POSITIVE / 2_f32.powi(23))),
        "0X1.00P-149"
    );

    // Go applies hexadecimal string precision to input bytes, not runes.
    assert_eq!(render("%.1x", FormatArg::from("éx")), "c3");
    assert_eq!(render("%.2x", FormatArg::from("éx")), "c3a9");
    assert_eq!(render("%.3x", FormatArg::from("éx")), "c3a978");
    assert_eq!(render("%5s", FormatArg::from("é")), "    é");
    assert_eq!(render("%-5s", FormatArg::from("é")), "é    ");
    assert_eq!(render("%+08d", FormatArg::from(7_i64)), "+0000007");
    assert_eq!(render("%#012x", FormatArg::from(31_i64)), "0x00000000001f");
    assert_eq!(
        render("%#16.2x", FormatArg::from(1.5_f64)),
        "      0x1.80p+00"
    );
}

#[test]
fn generic_formatter_shared_width_and_remaining_primitive_verbs_match_go() {
    assert_eq!(render("%+07.2f", FormatArg::from(1.0_f64)), "+001.00");
    assert_eq!(render("%+07.2F", FormatArg::from(1.0_f64)), "+001.00");
    assert_eq!(render("%05s", FormatArg::from("abc")), "00abc");
    assert_eq!(render("%06.0d", FormatArg::from(0_i64)), "      ");

    for (format, expected) in [("%5q", "  \"a\""), ("%-5q", "\"a\"  "), ("%05q", "00\"a\"")] {
        assert_eq!(render(format, FormatArg::from("a")), expected);
    }
    for (format, expected) in [("%5c", "    A"), ("%-5c", "A    "), ("%05c", "0000A")] {
        assert_eq!(render(format, FormatArg::from('A')), expected);
    }
    for (format, expected) in [("%6t", "  true"), ("%-6t", "true  "), ("%06t", "00true")] {
        assert_eq!(render(format, FormatArg::from(true)), expected);
    }
    for (format, expected) in [
        ("%10T", "     int64"),
        ("%-10T", "int64     "),
        ("%010T", "00000int64"),
    ] {
        assert_eq!(render(format, FormatArg::from(1_i64)), expected);
    }

    for (format, expected) in [
        ("%b", "111"),
        ("%#b", "0b111"),
        ("%08b", "00000111"),
        ("%o", "7"),
        ("%#o", "07"),
        ("%O", "0o7"),
        ("%#O", "0o07"),
    ] {
        assert_eq!(render(format, FormatArg::from(7_i64)), expected);
    }
    assert_eq!(render("%U", FormatArg::from('A')), "U+0041");
    assert_eq!(render("%#U", FormatArg::from('A')), "U+0041 'A'");
    assert_eq!(render("%12U", FormatArg::from('A')), "      U+0041");
    assert_eq!(
        render("%b", FormatArg::from(1.5_f64)),
        "6755399441055744p-52"
    );
    assert_eq!(
        render("%.3b", FormatArg::from(1.5_f64)),
        "6755399441055744p-52"
    );
    assert_eq!(render("%#q", FormatArg::from("abc")), "`abc`");
    assert_eq!(render("%#q", FormatArg::from("a\nb")), "\"a\\nb\"");
    assert_eq!(render("%+q", FormatArg::from("é")), "\"\\u00e9\"");
    assert_eq!(render("%#x", FormatArg::from("abc")), "0x616263");
    assert_eq!(render("% x", FormatArg::from("abc")), "61 62 63");
    assert_eq!(render("%# x", FormatArg::from("abc")), "0x61 0x62 0x63");
    assert_eq!(render("%#X", FormatArg::from("ab")), "0X6162");
    assert_eq!(render("%+v", FormatArg::from(7_i64)), "7");
    assert_eq!(render("% v", FormatArg::from(7_i64)), " 7");
    assert_eq!(render("%05v", FormatArg::from(7_i64)), "00007");
}

#[test]
fn generic_formatter_edge_values_preserve_original_go_domains() {
    assert_eq!(render("%q", FormatArg::from('\0')), "'\\x00'");
    assert_eq!(render("%#q", FormatArg::from("a\tb")), "`a\tb`");

    assert_eq!(render("%U", FormatArg::from(-1_i64)), "U+FFFFFFFFFFFFFFFF");
    assert_eq!(
        render("%U", FormatArg::from(u64::MAX)),
        "U+FFFFFFFFFFFFFFFF"
    );
    assert_eq!(render("%U", FormatArg::from(0x11_0000_u64)), "U+110000");
    assert_eq!(render("%.6U", FormatArg::from('⌘')), "U+002318");
    assert_eq!(render("%#U", FormatArg::from(0x11_0000_u64)), "U+110000");
    assert_eq!(render("%#U", FormatArg::from('A')), "U+0041 'A'");
    assert_eq!(render("%010U", FormatArg::from('A')), "    U+0041");

    assert_eq!(render("%#o", FormatArg::from(0_i64)), "0");
    assert_eq!(render("%#O", FormatArg::from(0_i64)), "0o0");
    assert_eq!(render("%.0O", FormatArg::from(0_i64)), "");
    assert_eq!(render("%#.3O", FormatArg::from(7_i64)), "0o007");

    assert_eq!(render("%#x", FormatArg::from("")), "");
    assert_eq!(render("%#5x", FormatArg::from("")), "     ");
    assert_eq!(render("%.3T", FormatArg::from(0_i64)), "int");

    assert_eq!(render("% b", FormatArg::from(f64::INFINITY)), " Inf");
    assert_eq!(render("%+b", FormatArg::from(f64::NAN)), "+NaN");
    assert_eq!(render("%08b", FormatArg::from(f64::INFINITY)), "    +Inf");
    assert_eq!(render("% F", FormatArg::from(f64::INFINITY)), " Inf");
    assert_eq!(render("%+F", FormatArg::from(f64::NAN)), "+NaN");
    assert_eq!(
        render("%08F", FormatArg::from(f64::NEG_INFINITY)),
        "    -Inf"
    );
    assert_eq!(render("%08v", FormatArg::from(-1.5_f64)), "-00001.5");
}

#[test]
fn generic_formatter_go_print_radix_zero_and_rounded_g_boundaries() {
    assert_eq!(render("%q", FormatArg::from('\u{0e00}')), "'\\u0e00'");
    assert_eq!(render("%q", FormatArg::from('\u{10ffff}')), "'\\U0010ffff'");
    assert_eq!(
        render("%#q", FormatArg::from("\u{feff}abc")),
        "\"\\ufeffabc\""
    );
    assert_eq!(render("%#v", FormatArg::from("\0")), "\"\\x00\"");

    assert_eq!(render("%#08b", FormatArg::from(7_i64)), "0b00000111");
    assert_eq!(render("%#08O", FormatArg::from(7_i64)), "0o00000007");
    for format in ["%#.0b", "%#.0o", "%.0x", "%#.0x"] {
        assert_eq!(render(format, FormatArg::from(0_i64)), "", "{format}");
    }

    assert_eq!(render("%.2g", FormatArg::from(99.9_f64)), "1e+02");
    assert_eq!(render("%.2G", FormatArg::from(99.9_f64)), "1E+02");
    assert_eq!(render("%.2g", FormatArg::from(9.99_f64)), "10");
}
