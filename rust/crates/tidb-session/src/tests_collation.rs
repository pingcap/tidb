//! Expression-level collation derivation, checked against the TiDB capture
//! (`pkg/executor/zz_dump_coll_test.go`).
//!
//! Every expectation here is a `ZZ` line from that capture, not a guess: the
//! `_ci` comparisons, the coercibility precedence that decides which side's
//! collation wins, the `binary` NO PAD rule against `utf8mb4_bin`'s PAD SPACE,
//! the collation-aware `INSTR`/`LOCATE`/`STRCMP`, `CONCAT`'s derived result
//! collation, and the exact 1267/1271/1253 error texts.
#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

/// The fixture the capture used: the same four values in a
/// `utf8mb4_general_ci` column, a `utf8mb4_bin` column and a `VARBINARY` one.
fn collation_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE ci (c VARCHAR(32) CHARSET utf8mb4 COLLATE utf8mb4_general_ci)",
        "CREATE TABLE bn (c VARCHAR(32) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
        "CREATE TABLE vb (c VARBINARY(32))",
        "CREATE TABLE uc (c VARCHAR(32) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci)",
        "INSERT INTO ci VALUES ('a'),('A'),('b'),('B')",
        "INSERT INTO bn VALUES ('a'),('A'),('b'),('B')",
        "INSERT INTO vb VALUES ('a'),('A'),('b'),('B')",
        "INSERT INTO uc VALUES ('a'),('A'),('b'),('B')",
    ] {
        session.run(sql).unwrap();
    }
    session
}

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][0].clone()
}

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// A `_ci` column compared with a literal folds case: the literal is
/// COERCIBLE (4) and the column IMPLICIT (2), so the column's collation wins
/// every one of these. Captured: each count is 2, where a byte-wise
/// comparison would return 1.
#[test]
fn ci_column_against_literal_folds_case() {
    let mut session = collation_session();
    for (sql, expected) in [
        ("SELECT COUNT(*) FROM ci WHERE c = 'A'", "2"),
        ("SELECT COUNT(*) FROM ci WHERE c <> 'A'", "2"),
        ("SELECT COUNT(*) FROM ci WHERE c < 'B'", "2"),
        ("SELECT COUNT(*) FROM ci WHERE c IN ('A')", "2"),
        ("SELECT COUNT(*) FROM ci WHERE c BETWEEN 'A' AND 'A'", "2"),
        ("SELECT COUNT(*) FROM ci WHERE c LIKE 'a'", "2"),
        // The `utf8mb4_bin` column is the control: it still matches one row.
        ("SELECT COUNT(*) FROM bn WHERE c = 'A'", "1"),
        // Two bare literals are both COERCIBLE, so the connection collation
        // (utf8mb4_bin) decides and the case difference stands.
        ("SELECT 'a' = 'A'", "0"),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
}

/// Two IMPLICIT operands of the same charset but different collations: the
/// `_bin` side wins (Go `isBinCollation`), so joining the `_ci` column to the
/// `_bin` column pairs 4 rows, not the 8 a case-folding comparison would.
#[test]
fn bin_collation_wins_an_implicit_tie() {
    let mut session = collation_session();
    assert_eq!(
        one(
            &mut session,
            "SELECT COUNT(*) FROM ci, bn WHERE ci.c = bn.c"
        ),
        "4"
    );
    // A binary-charset operand outranks both: `binary` has more precedence at
    // equal coercibility.
    assert_eq!(
        one(
            &mut session,
            "SELECT COUNT(*) FROM vb, ci WHERE vb.c = ci.c"
        ),
        "4"
    );
}

/// An explicit `COLLATE` clause is EXPLICIT (0) and outranks any column, on
/// either side of the comparison.
#[test]
fn explicit_collate_outranks_a_column() {
    let mut session = collation_session();
    for (sql, expected) in [
        (
            "SELECT COUNT(*) FROM ci WHERE c = 'A' COLLATE utf8mb4_bin",
            "1",
        ),
        (
            "SELECT COUNT(*) FROM ci WHERE c COLLATE utf8mb4_bin = 'A'",
            "1",
        ),
        (
            "SELECT COUNT(*) FROM bn WHERE c = 'A' COLLATE utf8mb4_general_ci",
            "2",
        ),
        (
            "SELECT COUNT(*) FROM bn WHERE c COLLATE utf8mb4_general_ci = 'A'",
            "2",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_general_ci = 'A' COLLATE utf8mb4_general_ci",
            "1",
        ),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
}

/// `ORDER BY` and `GROUP BY` over a `_ci` column use the collation's order and
/// its identity: captured, TiDB orders `a, A, b, B` (ties in insertion order)
/// and produces 2 groups, where byte order gives `A, B, a, b` and 4 groups.
#[test]
fn ci_order_by_and_group_by() {
    let mut session = collation_session();
    assert_eq!(
        row_text(session.run("SELECT c FROM ci ORDER BY c")),
        vec![vec!["a"], vec!["A"], vec!["b"], vec!["B"]]
    );
    assert_eq!(
        row_text(session.run("SELECT c FROM bn ORDER BY c")),
        vec![vec!["A"], vec!["B"], vec!["a"], vec!["b"]]
    );
    assert_eq!(one(&mut session, "SELECT COUNT(DISTINCT c) FROM ci"), "2");
    assert_eq!(one(&mut session, "SELECT COUNT(DISTINCT c) FROM bn"), "4");
    assert_eq!(
        row_text(session.run("SELECT c, COUNT(*) FROM ci GROUP BY c ORDER BY c")),
        vec![vec!["a", "2"], vec!["b", "2"]]
    );
}

/// `binary` is NO PAD and `utf8mb4_bin` is PAD SPACE: captured,
/// `_binary'a' = 'a  '` is 0 while `'a' = 'a  '` is 1.
#[test]
fn binary_is_no_pad_and_utf8mb4_bin_is_pad_space() {
    let mut session = collation_session();
    for (sql, expected) in [
        ("SELECT _binary'a' = 'a  '", "0"),
        ("SELECT _binary'a' = _binary'a  '", "0"),
        ("SELECT 'a' = 'a  '", "1"),
        (
            "SELECT 'a' COLLATE utf8mb4_bin = 'a  ' COLLATE utf8mb4_bin",
            "1",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_general_ci = 'a  ' COLLATE utf8mb4_general_ci",
            "1",
        ),
        // A VARBINARY column is binary-collated, so it never folds case.
        ("SELECT COUNT(*) FROM vb WHERE c = 'a'", "1"),
        ("SELECT COUNT(*) FROM vb WHERE c = 'A'", "1"),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
}

/// The collation-aware string builtins: `INSTR`/`LOCATE` search and `STRCMP`
/// compares with the derived collator, so the `_ci` and `_bin` forms differ.
#[test]
fn collation_aware_string_builtins() {
    let mut session = collation_session();
    for (sql, expected) in [
        ("SELECT INSTR('ABC', 'b')", "0"),
        ("SELECT INSTR('ABC' COLLATE utf8mb4_general_ci, 'b')", "2"),
        ("SELECT INSTR('ABC' COLLATE utf8mb4_bin, 'b')", "0"),
        ("SELECT LOCATE('b', 'ABC')", "0"),
        ("SELECT LOCATE('b' COLLATE utf8mb4_general_ci, 'ABC')", "2"),
        ("SELECT STRCMP('a', 'A')", "1"),
        (
            "SELECT STRCMP('a' COLLATE utf8mb4_general_ci, 'A' COLLATE utf8mb4_general_ci)",
            "0",
        ),
        (
            "SELECT STRCMP('a' COLLATE utf8mb4_bin, 'A' COLLATE utf8mb4_bin)",
            "1",
        ),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
    // Against the `_ci` COLUMN, the column's own collation is derived.
    assert_eq!(
        row_text(session.run("SELECT INSTR(c, 'b') FROM ci")),
        vec![vec!["0"], vec!["0"], vec!["1"], vec!["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT STRCMP(c, 'A') FROM ci")),
        vec![vec!["0"], vec!["0"], vec!["1"], vec!["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LOCATE('b', c) FROM ci")),
        vec![vec!["0"], vec!["0"], vec!["1"], vec!["1"]]
    );
}

/// `utf8mb4_general_ci` maps sharp-s to `S` and strips the common accents,
/// and folds every character above U+FFFF to one weight -- so two different
/// emoji compare EQUAL under it. `utf8mb4_unicode_ci` instead expands
/// sharp-s to `ss`; both are captured.
#[test]
fn general_ci_folding_matches_the_capture() {
    let mut session = collation_session();
    for (sql, expected) in [
        ("SELECT 'ß' = 's'", "0"),
        ("SELECT 'ß' COLLATE utf8mb4_general_ci = 's'", "1"),
        ("SELECT 'ß' COLLATE utf8mb4_general_ci = 'ss'", "0"),
        ("SELECT 'ß' COLLATE utf8mb4_unicode_ci = 'ss'", "1"),
        ("SELECT 'é' COLLATE utf8mb4_general_ci = 'e'", "1"),
        ("SELECT 'é' COLLATE utf8mb4_unicode_ci = 'e'", "1"),
        ("SELECT 'É' COLLATE utf8mb4_general_ci = 'é'", "1"),
        ("SELECT 'ä' COLLATE utf8mb4_general_ci = 'a'", "1"),
        ("SELECT '😀' COLLATE utf8mb4_general_ci = '😁'", "1"),
        ("SELECT STRCMP('ß' COLLATE utf8mb4_general_ci, 's')", "0"),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
}

/// Two operands whose collations cannot be aggregated raise 1267 with the
/// operand list, or 1271 without it when the arity is neither 2 nor 3. The
/// message text is byte-identical to the capture.
#[test]
fn illegal_mix_of_collations() {
    let mut session = collation_session();
    for (sql, code, message) in [
        (
            "SELECT 'a' COLLATE utf8mb4_general_ci = 'A' COLLATE utf8mb4_bin",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,EXPLICIT) and (utf8mb4_bin,EXPLICIT) for operation '='",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_general_ci = 'b' COLLATE utf8mb4_unicode_ci",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation '='",
        ),
        (
            "SELECT CONCAT('a' COLLATE utf8mb4_general_ci, 'b' COLLATE utf8mb4_unicode_ci)",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation 'concat'",
        ),
        (
            "SELECT INSTR('a' COLLATE utf8mb4_general_ci, 'b' COLLATE utf8mb4_unicode_ci)",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation 'instr'",
        ),
        (
            "SELECT 'a' COLLATE utf8mb4_general_ci LIKE 'b' COLLATE utf8mb4_unicode_ci",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation 'like'",
        ),
        (
            "SELECT COUNT(*) FROM ci, uc WHERE ci.c = uc.c",
            1267,
            "Illegal mix of collations (utf8mb4_general_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='",
        ),
        (
            "SELECT CONCAT_WS('-', 'a' COLLATE utf8mb4_general_ci, 'b' COLLATE utf8mb4_unicode_ci, 'c')",
            1271,
            "Illegal mix of collations for operation 'concat_ws'",
        ),
    ] {
        assert_eq!(error_of(&mut session, sql), (code, message.to_owned()), "{sql}");
    }
}

/// A `COLLATE` clause naming a collation outside the value's charset is 1253,
/// exactly as captured for `latin1_bin` on a (utf8mb4) string literal.
#[test]
fn collate_clause_must_match_the_charset() {
    let mut session = collation_session();
    assert_eq!(
        error_of(
            &mut session,
            "SELECT 'a' COLLATE latin1_bin = 'A' COLLATE latin1_bin"
        ),
        (
            1253,
            "COLLATION 'latin1_bin' is not valid for CHARACTER SET 'utf8mb4'".to_owned()
        )
    );
}
