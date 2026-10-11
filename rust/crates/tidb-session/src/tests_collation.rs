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

/// Go's parser stamps ordinary string literals with
/// `@@character_set_connection`/`@@collation_connection`, and expression
/// derivation uses the same pair for string results with no stronger source.
#[test]
fn set_names_reaches_literal_and_folded_expression_collations() {
    let mut session = Session::new();
    session
        .run("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
        .unwrap();
    for sql in [
        "SELECT COLLATION('a')",
        "SELECT COLLATION(CONCAT('a', 'b'))",
        "SELECT COLLATION(CAST(1 AS CHAR))",
        "SELECT COLLATION(IFNULL(CONCAT(NULL), '~'))",
        "SELECT COLLATION(IFNULL(CONCAT(NULL), IFNULL(CONCAT(NULL), '~')))",
    ] {
        assert_eq!(one(&mut session, sql), "utf8mb4_general_ci", "{sql}");
    }
    // Go's system constants deliberately use TiDB's server default rather
    // than the connection pair (pkg/expression/collation.go).
    assert_eq!(
        one(&mut session, "SELECT COLLATION(VERSION())"),
        "utf8mb4_bin"
    );
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

/// Go `checkJoinKeyCollation`: a merge join cannot key on a binary column
/// paired with a `general_ci` one, so the equality becomes an other
/// condition evaluated under the derived (binary) collation.
#[test]
fn a_merge_join_over_conflicting_collations_compares_as_binary() {
    let mut s = Session::new();
    s.run("create table t1 (id int, v varchar(5) character set binary, key(v))")
        .unwrap();
    s.run(
        "create table t2 (v varchar(5) character set utf8mb4 collate utf8mb4_general_ci, key(v))",
    )
    .unwrap();
    s.run("insert into t1 values (1, 'a'), (2, 'À'), (3, 'b')")
        .unwrap();
    s.run("insert into t2 values ('a'), ('À'), ('b')").unwrap();
    let rows = row_text(s.run(
        "select /*+ TIDB_SMJ(t1, t2) */ t1.id, t2.v from t1, t2 where t1.v = t2.v order by t1.id",
    ));
    assert_eq!(rows, vec![vec!["1", "a"], vec!["2", "À"], vec!["3", "b"]]);
}

/// Go `buildIndexLookUpJoin` ("Use the probe table's collation"): an
/// `ascii_bin` outer key finds and matches the `general_ci` inner index.
#[test]
fn an_index_join_matches_under_the_inner_key_collation() {
    let mut s = Session::new();
    s.run("create table a1 (a int, b char(10), key(b)) collate utf8mb4_general_ci")
        .unwrap();
    s.run("create table a2 (a int, b char(10), key(b)) collate ascii_bin")
        .unwrap();
    s.run("insert into a1 values (1, 'a')").unwrap();
    s.run("insert into a2 values (1, 'A'), (2, 'a')").unwrap();
    let mut joined =
        row_text(s.run("select /*+ inl_join(a1) */ a1.b, a2.b from a1 join a2 where a1.b = a2.b"));
    joined.sort();
    assert_eq!(joined, vec![vec!["a", "A"], vec!["a", "a"]]);
}

/// Go `SetCollationExpr`: `a COLLATE x` over a column is a CAST, so the
/// GROUP BY groups under `utf8mb4_bin`, not the column's `general_ci`.
#[test]
fn group_by_a_collated_column_groups_by_the_collation() {
    let mut s = Session::new();
    s.run("create table g (a char(10) collate utf8mb4_general_ci)")
        .unwrap();
    s.run("insert into g values ('a'), ('A'), ('a')").unwrap();
    let mut counts = row_text(s.run("select count(1) from g group by a collate utf8mb4_bin"));
    counts.sort();
    assert_eq!(counts, vec![vec!["1"], vec!["2"]]);
}

/// Go `LogicalProjection.AppendExpr` and `buildProjectionField` keep the
/// expression's coercibility on the column they create: a `COLLATE`d join
/// key projected below the join, and a `COLLATE`d select field under a
/// UNION, stay EXPLICIT.
#[test]
fn projected_collate_expressions_stay_explicit() {
    let mut s = Session::new();
    s.run("create table t (a varchar(10) collate utf8mb4_bin, b varchar(10) collate utf8mb4_general_ci, d date, i int)")
        .unwrap();
    s.run("insert into t values ('a', 'A', '2020-01-01', 1)")
        .unwrap();
    assert_eq!(
        row_text(
            s.run("select t1.a, t2.b from t t1, t t2 where t1.a = t2.b collate utf8mb4_general_ci")
        ),
        vec![vec!["a", "A"]]
    );
    assert_eq!(
        row_text(s.run(
            "select distinct collation(c) from (select d c from t union select i collate binary c from t) x"
        )),
        vec![vec!["binary"]]
    );
}

/// Go's rewriter sets an explicit CAST's coercibility: NUMERIC for a
/// non-string target, so a DATE branch of a UNION yields to the string
/// branch's PAD SPACE collation and '2010-09-09  ' deduplicates against it.
/// A string function over JSON is `fixStringTypeForMaxLength`'s `_bin`
/// collation, which then wins BETWEEN's derivation over general_ci bounds.
#[test]
fn cast_coercibility_and_json_string_functions_follow_go() {
    let mut s = Session::new();
    assert_eq!(
        one(&mut s, "with cte as (select cast('2010-09-09' as date) a union select '2010-09-09  ') select count(*) from cte"),
        "1"
    );
    assert_eq!(
        row_text(s.run("select collation(cast('[]' as json)), coercibility(cast('[]' as json))")),
        vec![vec!["utf8mb4_bin", "5"]]
    );
    s.run("set names utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    assert_eq!(
        one(
            &mut s,
            "select insert('[]', 0, 100, cast('[]' as json)) between 'W' and 'm'"
        ),
        "0"
    );
    assert_eq!(
        one(
            &mut s,
            "select reverse(cast('[]' as json)) between 'W' and 'm'"
        ),
        "1"
    );
    assert_eq!(
        one(&mut s, "select collation(reverse(cast('[]' as json)))"),
        "utf8mb4_bin"
    );
}

/// Go `betweenToExpression` derives one collation over all three operands
/// and casts each to it, and folds the bounds as it builds them; Go
/// `deriveCollationForIn`/`castCollationForIn` do the same for an IN that is
/// rewritten to equalities.
#[test]
fn between_and_in_cast_to_one_derived_collation() {
    let mut s = Session::new();
    s.run("create table t1(a char(20)) collate utf8mb4_general_ci")
        .unwrap();
    s.run("create table t2(b binary(20), c char(20)) collate utf8mb4_general_ci")
        .unwrap();
    s.run("insert into t1 values ('a')").unwrap();
    s.run("insert into t2 values (0x0, 'A')").unwrap();
    assert!(row_text(s.run("select * from t1, t2 where t1.a between t2.b and t2.c")).is_empty());
    assert!(
        row_text(s.run("explain select * from t1 where 'a' between 'g' and 'f'"))[0][0]
            .starts_with("TableDual")
    );
    s.run("create table tin (a varchar(10) collate utf8mb4_bin)")
        .unwrap();
    s.run("insert into tin values ('a')").unwrap();
    assert_eq!(
        row_text(s.run("select * from tin where a in ('b' collate utf8mb4_general_ci, 'A', 3)")),
        vec![vec!["a"]]
    );
}

/// Go `locateStringWithCollation` searches collation keys, so 'ß' is found
/// at 'ss' under unicode_ci; `builtinInstrUTF8Sig` lowercases under a `_ci`
/// collation and reports the character offset.
#[test]
fn locate_searches_collation_keys_and_instr_lowercases() {
    let mut s = Session::new();
    assert_eq!(
        one(
            &mut s,
            "select locate('ß', 'ss' collate utf8mb4_unicode_ci)"
        ),
        "1"
    );
    assert_eq!(
        one(
            &mut s,
            "select instr('aßB' collate utf8mb4_general_ci, 'b')"
        ),
        "3"
    );
    // general_ci's key folds 'ß' to 's'; Go's INSTR lowercases instead.
    assert_eq!(
        one(&mut s, "select instr('ß' collate utf8mb4_general_ci, 's')"),
        "0"
    );
}

/// The quantified and scalar subquery rewrites keep the subquery column's
/// coercibility (Go `SetCoercibility(rexpr.Coercibility())`), so a COLLATE
/// inside the subquery decides the comparison.
#[test]
fn subquery_rewrites_keep_the_subquery_collation() {
    let mut s = Session::new();
    s.run("create table t (a varchar(10) collate utf8mb4_bin)")
        .unwrap();
    s.run("create table t1 (a varchar(10) collate utf8mb4_bin)")
        .unwrap();
    s.run("insert into t values ('A')").unwrap();
    s.run("insert into t1 values ('a')").unwrap();
    for sql in [
        "select a from t where t.a = all (select a collate utf8mb4_general_ci from t1)",
        "select a from t where t.a = (select a collate utf8mb4_general_ci from t1)",
    ] {
        assert_eq!(row_text(s.run(sql)), vec![vec!["A"]], "{sql}");
    }
    assert!(row_text(
        s.run("select a from t where t.a != any (select a collate utf8mb4_general_ci from t1)")
    )
    .is_empty());
}

/// Go `FoldConstant` keeps the folded expression's collation: a constant IF
/// that picks its first branch still reports the derived unicode_ci.
#[test]
fn a_folded_if_keeps_its_derived_collation() {
    let mut s = Session::new();
    s.run("set names utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    assert_eq!(
        one(
            &mut s,
            "select collation(IF('a' < 'B' collate utf8mb4_general_ci, 'smaller', 'greater' collate utf8mb4_unicode_ci))"
        ),
        "utf8mb4_unicode_ci"
    );
}

/// Go `buildSet` rewrites the assigned expression itself, so a plain literal
/// takes the connection collation and the variable stores that type.
#[test]
fn a_user_variable_stores_the_connection_collation() {
    let mut s = Session::new();
    s.run("set names utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    s.run("set @v1 = 'a', @v2 = (select 'a')").unwrap();
    assert_eq!(
        row_text(s.run("select collation(@v1), collation(@v2), coercibility(@v1)")),
        vec![vec!["utf8mb4_general_ci", "utf8mb4_general_ci", "2"]]
    );
}

/// Go `adjustUTF8MB4Collation`: a `_utf8mb4` introducer takes
/// `default_collation_for_utf8mb4`.
#[test]
fn a_utf8mb4_introducer_takes_the_default_collation_for_utf8mb4() {
    let mut s = Session::new();
    s.run("set @@session.default_collation_for_utf8mb4 = 'utf8mb4_0900_ai_ci'")
        .unwrap();
    assert_eq!(
        one(&mut s, "select collation(_utf8mb4'12345')"),
        "utf8mb4_0900_ai_ci"
    );
}

/// Go `topNRows`: GROUP_CONCAT's ORDER BY pushes rows onto a heap and then
/// runs Go's unstable pdqsort, so rows the ORDER BY ties come out in that
/// schedule's order (captured from TiDB: `Abc,abc,abc,def`).
#[test]
fn group_concat_order_by_ties_follow_go_sort_schedule() {
    let mut s = Session::new();
    s.run("create table t(id int, value varchar(20) collate utf8mb4_general_ci)")
        .unwrap();
    s.run("insert into t values (1, 'abc'), (4, 'Abc'), (3, 'def'), (5, 'abc')")
        .unwrap();
    assert_eq!(
        one(&mut s, "select group_concat(value order by 1) from t"),
        "Abc,abc,abc,def"
    );
}

/// Go `maxMin4Enum`/`maxMin4Set` and the cop's `Datum.Compare` order ENUM and
/// SET values by NAME under the collation, both at the root and in the
/// coprocessor's partial aggregate.
#[test]
fn min_max_over_enum_and_set_compare_names_under_the_collation() {
    let mut s = Session::new();
    s.run(
        "create table tt(b enum('a', 'B', 'c'), c set('a', 'B', 'c')) collate utf8mb4_general_ci",
    )
    .unwrap();
    s.run("insert into tt values ('a', 'a'), ('B', 'B'), ('c', 'c')")
        .unwrap();
    assert_eq!(
        row_text(s.run("select min(b), max(b), min(c), max(c) from tt")),
        vec![vec!["a", "c", "a", "c"]]
    );
    assert_eq!(
        one(&mut s, "select /*+ AGG_TO_COP() */ min(b) from tt"),
        "a"
    );
}

/// Go `executeUse` sets `collation_database` from the schema, and the
/// variable's hook mirrors `character_set_database`.
#[test]
fn use_sets_the_database_charset_variables() {
    let mut s = Session::new();
    s.run("create database cd_utf8 character set utf8 collate utf8_bin")
        .unwrap();
    s.run("use cd_utf8").unwrap();
    assert_eq!(
        row_text(s.run("select @@character_set_database, @@collation_database")),
        vec![vec!["utf8", "utf8_bin"]]
    );
}

/// `CONVERT(binary USING gb18030)` and the implicit `from_binary` are text in
/// the target charset: SUBSTRING counts characters, and a hex literal against
/// a gb18030 index converts once into the index's collation key.
#[test]
fn decoded_binary_is_text_in_the_target_charset() {
    let mut s = Session::new();
    assert_eq!(
        one(
            &mut s,
            "select hex(substring(convert(0x81308131813081328130813381308134 using gb18030), 1, 2))"
        ),
        "8130813181308132"
    );
    s.run("create table g (c varchar(100) character set gb18030, key(c(20)))")
        .unwrap();
    s.run("insert into g values (0x8130883281308833)").unwrap();
    assert_eq!(
        row_text(s.run("select hex(c) from g where c = 0x8130883281308833")),
        vec![vec!["8130883281308833"]]
    );
}

/// Go `safeConvert`: a constant the derived charset cannot represent is an
/// illegal mix ('ㅂ' has no GBK form).
#[test]
fn an_unrepresentable_constant_is_an_illegal_mix() {
    let mut s = Session::new();
    let (code, message) = error_of(
        &mut s,
        "select collation(concat('ㅂ', convert('啊' using gbk) collate gbk_bin))",
    );
    assert_eq!(code, 1267, "{message}");
}

/// Go `newReturnFieldTypeForBaseBuiltinFunc`: a builtin's result type takes
/// the derived collation only when the result is a string; any other result
/// is the binary charset, while the function itself still runs under the
/// derivation (`baseBuiltinFunc.collator`). `INSTR(gbk_col, ...)` is
/// therefore a binary integer, and `HEX` over it transcodes nothing --
/// captured from Go. This tier stamped `gbk` on the integer, so `HEX`
/// wrapped it in a GBK transcode and refused the statement.
#[test]
fn a_non_string_function_result_is_the_binary_charset() {
    let mut session = Session::new();
    session
        .run("create table t (a char(20) charset utf8mb4, b char(20) charset gbk)")
        .unwrap();
    session.run("insert into t values ('一', '一')").unwrap();
    assert_eq!(
        crate::tests_support::query_text(
            &mut session,
            "select charset(instr(b, 0xd2bb)), collation(instr(b, 0xd2bb)), \
             charset(locate('x', b)), charset(b < 'x'), charset(length(b)) from t",
        )
        .1,
        [["binary", "binary", "binary", "binary", "binary"]]
    );
    assert_eq!(
        crate::tests_support::query_text(
            &mut session,
            "select hex(instr(a, 0xe4b880)), hex(instr(b, 0xd2bb)), hex(ascii(b)), \
             hex(length(b)), hex(b < 'x') from t",
        )
        .1,
        [["1", "1", "D2", "2", "0"]]
    );
    // `POSITION(substr IN str)` is Go's `position` call built like any
    // other (`locateFunctionClass`), so the gbk column meeting a binary
    // literal is transcoded before the search, as INSTR's is.
    assert_eq!(
        crate::tests_support::query_text(
            &mut session,
            "select hex(position(a in 0xe4b880)), hex(position(b in 0xd2bb)) from t",
        )
        .1,
        [["1", "1"]]
    );
    // The comparison still runs under the column's gbk_chinese_ci.
    assert_eq!(
        crate::tests_support::query_text(&mut session, "select b = '一', b < 'x' from t").1,
        [["1", "0"]]
    );
}
