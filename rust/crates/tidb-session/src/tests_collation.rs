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

/// Go `FoldConstant` copies an expression's coercibility onto the replacement
/// constant. `CONVERT ... USING` therefore stays IMPLICIT after folding,
/// while an explicit `COLLATE` remains EXPLICIT.
#[test]
fn constant_folding_preserves_expression_coercibility() {
    let mut session = Session::new();
    for (sql, expected) in [
        ("SELECT COERCIBILITY(CONVERT('a' USING utf8mb4))", "2"),
        (
            "SELECT COERCIBILITY(CONVERT('a' USING utf8mb4) COLLATE utf8mb4_general_ci)",
            "0",
        ),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
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

/// `FIELD`, `FIND_IN_SET` and `REGEXP` also search with the derived collator,
/// so each has a `_ci` answer that differs from its `_bin` one.
///
/// Go reaches this three different ways -- `builtinFieldStringSig` calls
/// `b.ctor.Compare`, `findInSetByKey` compares
/// `collator.KeyWithoutTrimRightSpace`, and `getRegexpMatchType` turns a `_ci`
/// collation into RE2's `i` flag -- and all three were previously ignored
/// here, so an explicit `COLLATE` on an argument silently changed nothing.
#[test]
fn field_find_in_set_and_regexp_use_the_derived_collation() {
    let mut session = collation_session();
    for (sql, expected) in [
        // Both operands bare: the connection collation (utf8mb4_bin) decides.
        ("SELECT FIELD('ABC', 'x', 'abc')", "0"),
        (
            "SELECT FIELD('ABC' COLLATE utf8mb4_general_ci, 'x', 'abc')",
            "2",
        ),
        ("SELECT FIELD('ABC' COLLATE utf8mb4_bin, 'x', 'abc')", "0"),
        // The COLLATE may sit on either operand: coercibility ranks them and
        // EXPLICIT beats the other side's COERCIBLE whichever side it is on.
        ("SELECT FIELD('ABC', 'abc' COLLATE utf8mb4_general_ci)", "1"),
        // `utf8mb4_bin` is PAD SPACE, so the collator ignores trailing blanks.
        ("SELECT FIELD('a ' COLLATE utf8mb4_bin, 'a')", "1"),
        ("SELECT FIND_IN_SET('B', 'a,b,c')", "0"),
        (
            "SELECT FIND_IN_SET('B' COLLATE utf8mb4_general_ci, 'a,b,c')",
            "2",
        ),
        ("SELECT FIND_IN_SET('B' COLLATE utf8mb4_bin, 'a,b,c')", "0"),
        (
            "SELECT FIND_IN_SET('b', 'a,B,c' COLLATE utf8mb4_general_ci)",
            "2",
        ),
        // `FIND_IN_SET` keys WITHOUT trimming right spaces, so unlike `FIELD`
        // above a trailing blank still makes the member differ even under a
        // PAD SPACE collation.
        (
            "SELECT FIND_IN_SET('a ' COLLATE utf8mb4_general_ci, 'a,b')",
            "0",
        ),
        ("SELECT 'ABC' REGEXP 'abc'", "0"),
        ("SELECT 'ABC' COLLATE utf8mb4_general_ci REGEXP 'abc'", "1"),
        ("SELECT 'ABC' COLLATE utf8mb4_unicode_ci REGEXP 'abc'", "1"),
        ("SELECT 'ABC' COLLATE utf8mb4_bin REGEXP 'abc'", "0"),
        ("SELECT 'ABC' REGEXP 'abc' COLLATE utf8mb4_general_ci", "1"),
        // A non-string argument list keeps Go's REAL signature, which consults
        // no collation at all: `FIELD(1, '1')` matches numerically.
        ("SELECT FIELD(1, '1')", "1"),
        ("SELECT FIELD('1', 1)", "1"),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
    // Derived from a COLUMN rather than a COLLATE clause: the `_ci` column is
    // IMPLICIT and beats the literal's COERCIBLE, the `_bin` column is the
    // control. Rows are the fixture's 'a','A','b','B' in insertion order.
    for (sql, expected) in [
        ("SELECT FIELD(c, 'A') FROM ci", ["1", "1", "0", "0"]),
        ("SELECT FIELD(c, 'A') FROM bn", ["0", "1", "0", "0"]),
        (
            "SELECT FIND_IN_SET(c, 'x,A,y') FROM ci",
            ["2", "2", "0", "0"],
        ),
        (
            "SELECT FIND_IN_SET(c, 'x,A,y') FROM bn",
            ["0", "2", "0", "0"],
        ),
        ("SELECT c REGEXP 'a' FROM ci", ["1", "1", "0", "0"]),
        ("SELECT c REGEXP 'a' FROM bn", ["1", "0", "0", "0"]),
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            expected.map(|cell| vec![cell.to_owned()]).to_vec(),
            "{sql}"
        );
    }
}

/// A `binary` collation selects a different `INSTR`/`LOCATE` SIGNATURE, not
/// just a different comparison: Go's `builtinInstrSig` /
/// `builtinLocate2ArgsSig` report a BYTE offset where the `...UTF8Sig` pair
/// report a character offset.
///
/// `'aéb'` is the fixture that can tell them apart -- 4 bytes, 3 characters --
/// so the byte answer (4) and the character answer (3) genuinely differ.
#[test]
fn instr_and_locate_report_byte_offsets_under_a_binary_collation() {
    let mut session = collation_session();
    for (sql, expected) in [
        ("SELECT INSTR(CAST('aéb' AS BINARY), 'b')", "4"),
        ("SELECT INSTR('aéb', 'b')", "3"),
        ("SELECT LOCATE('b', CAST('aéb' AS BINARY))", "4"),
        ("SELECT LOCATE('b', 'aéb')", "3"),
        // A miss is still 0, and an empty needle still matches at 1.
        ("SELECT INSTR(CAST('aéb' AS BINARY), 'z')", "0"),
        ("SELECT INSTR(CAST('aéb' AS BINARY), '')", "1"),
    ] {
        assert_eq!(one(&mut session, sql), expected, "{sql}");
    }
    // A VARBINARY column derives the same `binary` collation, and never folds
    // case: only the 'B' row matches.
    assert_eq!(
        row_text(session.run("SELECT INSTR(c, 'B') FROM vb")),
        vec![vec!["0"], vec!["0"], vec!["0"], vec!["1"]]
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
