//! The charset TRANSCODING boundary, checked against the TiDB capture
//! (`pkg/executor/zz_dump_transcode_test.go`).
//!
//! Every expectation here is a captured `ROWS`/`ERR` line, not a guess. The
//! architecture they pin down is documented in
//! `tidb_expr::convert_charset`: a character string is ALWAYS stored and
//! evaluated as UTF-8, whatever its declared charset, and the only implicit
//! transcode is the `to_binary` wrap Go's `HandleBinaryLiteral` puts on a
//! non-legacy-charset argument of a binary-aware function.
#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn charset_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE g1 (a VARCHAR(20) CHARSET gbk)",
        "CREATE TABLE g3 (a VARCHAR(20) CHARSET gbk COLLATE gbk_chinese_ci)",
        "CREATE TABLE u1 (a VARCHAR(20) CHARSET utf8mb4)",
        "CREATE TABLE l1 (a VARCHAR(20) CHARSET latin1)",
        "CREATE TABLE a1 (a VARCHAR(20) CHARSET ascii)",
        "CREATE TABLE b1 (a VARCHAR(20) CHARSET gb18030)",
    ] {
        session.run(sql).unwrap();
    }
    session
}

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][0].clone()
}

fn error_code(session: &mut Session, sql: &str) -> u16 {
    session.run(sql).unwrap_err().to_mysql_error().code
}

/// The storage verdict. A `gbk` column holds the UTF-8 bytes of its value:
/// `HEX` reports the GBK form only because it transcodes at the boundary,
/// while `CHAR_LENGTH` -- which does NOT transcode -- counts the same two
/// characters for both columns. Captured: `D2BBC1D0`/4/2 for the gbk column
/// and `E4B880E58897`/6/2 for the utf8mb4 one.
#[test]
fn gbk_column_stores_utf8_and_transcodes_at_the_boundary() {
    let mut session = charset_session();
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    session
        .run("INSERT INTO u1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a), ASCII(a) FROM g1")),
        vec![vec!["D2BBC1D0", "4", "2", "210"]]
    );
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a) FROM u1")),
        vec![vec!["E4B880E58897", "6", "2"]]
    );
    // The value itself still reads back as the original characters.
    assert_eq!(one(&mut session, "SELECT a FROM g1"), "\u{4e00}\u{5217}");
    // `CAST(x AS BINARY)` is the same wrap through Go's `funcPropAuto` arm.
    assert_eq!(
        row_text(session.run("SELECT HEX(CAST(a AS BINARY)), LENGTH(CAST(a AS BINARY)) FROM g1")),
        vec![vec!["D2BBC1D0", "4"]]
    );
}

/// A character the column's charset cannot represent is 1366 at write time,
/// not mangled bytes. Captured: `Incorrect string value '\xF0\x9F\x98\x89'`
/// for `😉` and `'\xE2\x82\xAC'` for `€` (which GBK rejects through the
/// `customGBKEncoder` special case), while `一列` is accepted.
#[test]
fn unrepresentable_character_is_rejected_at_write_time() {
    let mut session = charset_session();
    assert_eq!(
        error_code(&mut session, "INSERT INTO g1 VALUES ('\u{1f609}')"),
        1366
    );
    assert_eq!(
        error_code(&mut session, "INSERT INTO g1 VALUES ('\u{20ac}')"),
        1366
    );
    assert_eq!(
        error_code(&mut session, "INSERT INTO a1 VALUES ('\u{4e00}')"),
        1366
    );
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    session
        .run("INSERT INTO b1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a) FROM b1")),
        vec![vec!["D2BBC1D0", "4"]]
    );
}

/// `CONVERT(x USING cs)` RETAGS rather than transcodes: converting to gbk and
/// straight back to utf8mb4 returns the identical bytes, and an
/// unrepresentable character becomes `?` with no error. Captured:
/// `D2BBC1D0`, `E4B880E58897`, `?`.
#[test]
fn convert_using_retags_and_replaces() {
    let mut session = charset_session();
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(CONVERT('\u{4e00}\u{5217}' USING gbk)), \
             LENGTH(CONVERT('\u{4e00}\u{5217}' USING gbk))"
        )),
        vec![vec!["D2BBC1D0", "4"]]
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT HEX(CONVERT(CONVERT('\u{4e00}\u{5217}' USING gbk) USING utf8mb4))"
        ),
        "E4B880E58897"
    );
    assert_eq!(
        one(&mut session, "SELECT CONVERT('\u{1f609}' USING gbk)"),
        "?"
    );
    assert_eq!(
        one(&mut session, "SELECT HEX(CONVERT('\u{4e00}' USING ascii))"),
        "3F"
    );
}

/// TiDB's `latin1` is a byte-preserving alias for UTF-8, not ISO-8859-1
/// (`pkg/parser/charset/encoding_latin1.go` builds it on `encoding.Nop` with
/// an always-true `IsValid`), so it is a LEGACY charset that never transcodes.
/// Captured: `HEX('é')` in a latin1 column is `C3A9`, a raw `0xE9` stays
/// `E9`, and even a character outside Latin-1 survives untouched.
#[test]
fn latin1_is_a_byte_preserving_utf8_alias() {
    let mut session = charset_session();
    session.run("INSERT INTO l1 VALUES ('\u{e9}')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a) FROM l1")),
        vec![vec!["C3A9", "2", "1"]]
    );
    session.run("INSERT INTO l1 VALUES (0xE9)").unwrap();
    session.run("INSERT INTO l1 VALUES ('\u{4e00}')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a) FROM l1")),
        vec![vec!["C3A9"], vec!["E9"], vec!["E4B880"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(CONVERT('\u{e9}' USING latin1)), HEX(CONVERT('\u{4e00}' USING latin1))"
        )),
        vec![vec!["C3A9", "E4B880"]]
    );
}

/// `CHARSET gbk` with no `COLLATE` resolves to `gbk_chinese_ci`, not
/// `gbk_bin` -- TiDB leaves the Chinese charsets at their registry default.
/// Captured: `ORDER BY` on the gbk column follows GBK weights (`列` before
/// `一`, the reverse of Unicode code-point order), and the `_ci` comparison
/// folds case.
#[test]
fn gbk_defaults_to_chinese_ci_and_orders_by_gbk_weight() {
    let mut session = charset_session();
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}'),('\u{5217}'),('b'),('A')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, HEX(a) FROM g1 ORDER BY a")),
        vec![
            vec!["A", "41"],
            vec!["b", "62"],
            vec!["\u{5217}", "C1D0"],
            vec!["\u{4e00}", "D2BB"],
        ]
    );
    session.run("INSERT INTO g3 VALUES ('a'),('A')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM g3 WHERE a = 'A'")),
        vec![vec!["2"]]
    );
}
