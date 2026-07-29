//! SQL behaviors harvested from the retired `tidb_exec::Database` relation
//! engine, re-homed onto the live `Session` path before that engine is deleted.
//!
//! The relation engine's own tests encoded a large body of TiDB semantics that
//! had been probed against real Go. Those assertions are knowledge, not code:
//! deleting the engine would destroy them silently. Every expectation below was
//! re-captured against real TiDB through `gorun` (mock-store `testkit`) for this
//! module rather than copied on the old tests' authority, so each one stands on
//! its own evidence.
//!
//! Tests marked `#[ignore]` assert the captured Go answer for a behavior the
//! live engine currently gets WRONG. They are deliberately left failing: each
//! one names a real divergence, and papering over it by writing the Rust answer
//! into the assertion would destroy exactly the knowledge this module exists to
//! preserve.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

/// One statement's rows as text, panicking on error.
fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The one value a single-row single-column query returns.
fn one(session: &mut Session, sql: &str) -> String {
    rows(session, sql).remove(0).remove(0)
}

// ---------------------------------------------------------------------------
// INSERT conflict resolution
// ---------------------------------------------------------------------------

/// An `ON DUPLICATE KEY UPDATE` assignment resolves a bare column reference
/// against the row ALREADY STORED, while `VALUES(col)` yields the value the
/// rejected row proposed. `gorun`: `b = b + VALUES(b)` over a stored `10` and a
/// proposed `99` gives `109`, so both halves come from different rows.
///
/// `INSERT IGNORE` on the same conflict keeps the stored row untouched, and
/// `REPLACE` deletes the conflicting row before inserting.
#[test]
fn insert_conflict_reads_the_stored_row_and_values_reads_the_proposed_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1,10),(2,20)").unwrap();

    session
        .run("INSERT INTO t VALUES (1,99) ON DUPLICATE KEY UPDATE b = b + VALUES(b)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM t ORDER BY a"),
        [["1", "109"], ["2", "20"]]
    );

    // A non-conflicting row inserts normally; the clause is never applied.
    session
        .run("INSERT INTO t VALUES (3,30) ON DUPLICATE KEY UPDATE b = 999")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT b FROM t WHERE a = 3"), "30");

    // IGNORE discards the proposed row and keeps the stored one.
    session.run("INSERT IGNORE INTO t VALUES (1,777)").unwrap();
    assert_eq!(one(&mut session, "SELECT b FROM t WHERE a = 1"), "109");

    // REPLACE deletes then inserts, so the proposed value wins.
    session.run("REPLACE INTO t VALUES (1,42)").unwrap();
    assert_eq!(one(&mut session, "SELECT b FROM t WHERE a = 1"), "42");
    assert_eq!(one(&mut session, "SELECT count(*) FROM t"), "3");
}

/// `REPLACE` finds its conflicting row through a UNIQUE non-primary key too,
/// not only the primary key: `gorun` answers `2|10` after replacing `(1,10)`
/// with `(2,10)` where `b` is UNIQUE — the old row is gone, not duplicated.
#[test]
fn replace_deletes_the_row_found_through_a_unique_non_primary_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (a INT PRIMARY KEY, b INT UNIQUE)")
        .unwrap();
    session.run("INSERT INTO u VALUES (1,10)").unwrap();
    session.run("REPLACE INTO u VALUES (2,10)").unwrap();
    assert_eq!(rows(&mut session, "SELECT a,b FROM u"), [["2", "10"]]);
}

/// BUG (four distinct divergences): `AUTO_INCREMENT` allocation and
/// publication.
///
/// An `AUTO_INCREMENT` id is CONSUMED before a statement can fail, and the
/// consumption is permanent: a hard duplicate-key insert, an `INSERT IGNORE`
/// that skips its row, and an insert inside a rolled-back transaction each burn
/// an id that no later statement ever reuses. `LAST_INSERT_ID()` reports the
/// FIRST id of a multi-row insert, and only a real publication updates it —
/// a hard failure publishes the id it consumed, an `INSERT IGNORE` does not.
///
/// `gorun` over a table seeded `AUTO_INCREMENT=100`: ids land on 100, 101, then
/// 104 (102 burned by the failure, 103 by the IGNORE), and after a rolled-back
/// insert the next row takes 106, never 105.
///
/// The live engine diverges four ways:
///  1. the `AUTO_INCREMENT=100` table option is ignored and the counter starts
///     at 1;
///  2. a hard duplicate-key failure does NOT publish its consumed id;
///  3. an `INSERT IGNORE` DOES publish its consumed id — the exact inverse of
///     both rules above;
///  4. an id consumed inside a rolled-back transaction is returned to the pool
///     and handed to the next insert.
///
/// Only the burn count for the failure/IGNORE pair (two ids) is already right.
#[test]
#[ignore = "live-engine bug: AUTO_INCREMENT seed, publication, and rollback burn all diverge"]
fn auto_increment_ids_are_burned_by_failures_and_by_rollback() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT UNIQUE) \
             AUTO_INCREMENT=100",
        )
        .unwrap();
    session.run("INSERT INTO ai (v) VALUES (1),(2)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "100");

    // A hard duplicate on `v` still consumes id 102 and publishes it.
    assert!(session.run("INSERT INTO ai (v) VALUES (1)").is_err());
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "102");

    // The IGNORE form consumes id 103 but does not publish it.
    session.run("INSERT IGNORE INTO ai (v) VALUES (1)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "102");

    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();

    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ai (v) VALUES (6)").unwrap();
    session.run("ROLLBACK").unwrap();
    session.run("INSERT INTO ai (v) VALUES (7)").unwrap();

    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["100", "1"], ["101", "2"], ["104", "3"], ["106", "7"],]
    );
}

// ---------------------------------------------------------------------------
// Aggregation over empty and NULL-bearing input
// ---------------------------------------------------------------------------

/// Every aggregate but `COUNT` answers NULL over an empty group — including
/// `GROUP_CONCAT`, which is NULL and not the empty string. `COUNT(DISTINCT ...)`
/// answers 0. A no-`GROUP BY` aggregate query still emits exactly one row even
/// when nothing survives the `WHERE`. All five values captured via `gorun`.
#[test]
fn aggregates_over_an_empty_group_are_null_except_count() {
    let mut session = Session::new();
    session.run("CREATE TABLE e (a INT)").unwrap();
    session.run("INSERT INTO e VALUES (1),(2)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "SELECT sum(a), avg(a), max(a), min(a), group_concat(a) FROM e WHERE a > 100"
        ),
        [["NULL", "NULL", "NULL", "NULL", "NULL"]]
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT count(DISTINCT a) FROM e WHERE a > 100"
        ),
        "0"
    );
}

/// `GROUP_CONCAT` drops a row the instant ANY of its arguments is NULL, so a
/// group whose every row is dropped concatenates to NULL rather than to an
/// empty string. `gorun` over `(1,'x',NULL),(1,'y','q'),(2,NULL,NULL)`:
/// single-argument form gives `x,y` / NULL, and the two-argument form gives
/// `yq` / NULL — the `(1,'x',NULL)` row vanishes from the two-argument form
/// even though its first argument is not NULL.
#[test]
fn group_concat_drops_a_row_when_any_argument_is_null() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE gc (k INT, a VARCHAR(9), b VARCHAR(9))")
        .unwrap();
    session
        .run("INSERT INTO gc VALUES (1,'x',NULL),(1,'y','q'),(2,NULL,NULL)")
        .unwrap();

    assert_eq!(
        rows(
            &mut session,
            "SELECT k, group_concat(a) FROM gc GROUP BY k ORDER BY k"
        ),
        [["1", "x,y"], ["2", "NULL"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT k, group_concat(a,b) FROM gc GROUP BY k ORDER BY k"
        ),
        [["1", "yq"], ["2", "NULL"]]
    );
}

/// `GROUP_CONCAT`'s own `ORDER BY` sorts the group's rows before concatenating,
/// `DISTINCT` dedupes, and `SEPARATOR` replaces the default comma. `gorun` over
/// `(3),(1),(2),(1),(3)`: `DISTINCT ... ORDER BY v DESC` gives `3,2,1` and the
/// plain ordered form keeps every duplicate, `1-1-2-3-3`.
#[test]
fn group_concat_order_by_distinct_and_separator() {
    let mut session = Session::new();
    session.run("CREATE TABLE gcd (v INT)").unwrap();
    session
        .run("INSERT INTO gcd VALUES (3),(1),(2),(1),(3)")
        .unwrap();

    assert_eq!(
        one(
            &mut session,
            "SELECT group_concat(DISTINCT v ORDER BY v DESC) FROM gcd"
        ),
        "3,2,1"
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT group_concat(v ORDER BY v SEPARATOR '-') FROM gcd"
        ),
        "1-1-2-3-3"
    );
}

/// `COUNT(DISTINCT a, b)` skips a row entirely the instant ANY listed column is
/// NULL — a partially-NULL tuple is not a distinct value, it is no value.
/// `gorun` over `(1,1),(1,1),(1,2),(2,NULL),(NULL,1),(NULL,NULL)` answers 2:
/// only `(1,1)` and `(1,2)` survive. The single-column form ignores NULLs the
/// same way, also answering 2.
#[test]
fn count_distinct_multi_arg_skips_a_row_with_any_null_column() {
    let mut session = Session::new();
    session.run("CREATE TABLE cd (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO cd VALUES (1,1),(1,1),(1,2),(2,NULL),(NULL,1),(NULL,NULL)")
        .unwrap();

    assert_eq!(
        one(&mut session, "SELECT count(DISTINCT a, b) FROM cd"),
        "2"
    );
    assert_eq!(one(&mut session, "SELECT count(DISTINCT a) FROM cd"), "2");
    assert_eq!(one(&mut session, "SELECT count(DISTINCT b) FROM cd"), "2");
}

// ---------------------------------------------------------------------------
// Expression semantics
// ---------------------------------------------------------------------------

/// `ESCAPE c` makes the character immediately following `c` literal, wildcards
/// included, and consumes the escape character itself. `gorun` answers
/// `1, 0, 1, 0` for the four patterns below: `'+a%' ESCAPE '+'` means "literal
/// `a` then anything", so `'a+b'` matches and `'+a'` does not; `'+%a'` means
/// "literal `%` then `a`"; and `'aX%' ESCAPE 'X'` is exactly `a%` with no
/// trailing wildcard, so neither `'aXb'` nor `'a%b'` matches it.
#[test]
fn like_escape_clause_makes_the_next_character_literal() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "SELECT 'a+b' LIKE '+a%' ESCAPE '+', '+a' LIKE '+a%' ESCAPE '+', \
             '%a' LIKE '+%a' ESCAPE '+', 'aXb' LIKE 'aX%' ESCAPE 'X'"
        ),
        [["1", "0", "1", "0"]]
    );
}

/// `TRIM`'s `remstr` is removed as a WHOLE repeated occurrence, not
/// per-character, and an EMPTY `remstr` is a no-op rather than an infinite
/// strip. `gorun`: `bar`, `barxxx`, `hi`, `xxhixx`.
#[test]
fn trim_removes_whole_occurrences_and_an_empty_remstr_is_a_no_op() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "SELECT trim('  bar  '), trim(LEADING 'x' FROM 'xxxbarxxx'), \
             trim('xx' FROM 'xxhixx'), trim('' FROM 'xxhixx')"
        ),
        [["bar", "barxxx", "hi", "xxhixx"]]
    );
}

/// Hex and bit literals are RAW BYTES, not numbers, in string context. The
/// empty bit literal is the empty string, but every all-zero form is one NUL
/// byte rather than empty. `gorun`: `0, 1, 1, xA, 1`, and a three-byte UTF-8
/// hex literal has `LENGTH` 3.
#[test]
fn hex_and_bit_literals_carry_their_own_bytes() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "SELECT length(b''), length(b'0'), length(0x0), concat('x', 0x41), length(0x1A)"
        ),
        [["0", "1", "1", "xA", "1"]]
    );
    assert_eq!(one(&mut session, "SELECT length(0xE4BDA0)"), "3");
}

/// `ADDDATE`/`SUBDATE` accept both grammar forms and agree: a bare number means
/// `INTERVAL n DAY`, a negative bare number subtracts, month-end rolls over,
/// and NULL propagates from either argument. `gorun`: `2020-01-02`,
/// `2019-12-31`, `2019-12-31`, `2020-02-01`, NULL, NULL.
#[test]
fn adddate_bare_number_means_days_and_null_propagates() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "SELECT adddate('2020-01-01', 1), subdate('2020-01-01', 1), \
             adddate('2020-01-01', -1), adddate('2020-01-31', 1), \
             adddate(NULL, 1), adddate('2020-01-01', NULL)"
        ),
        [[
            "2020-01-02",
            "2019-12-31",
            "2019-12-31",
            "2020-02-01",
            "NULL",
            "NULL"
        ]]
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT adddate('2020-01-01', INTERVAL 5 HOUR)"
        ),
        "2020-01-01 05:00:00"
    );
}

/// The `/` operator produces a DECIMAL whose scale grows by
/// `div_precision_increment` (4), and `AVG` grows the source column's scale by
/// the same 4 — so `AVG` over `DECIMAL(10,2)` has scale 6 while `SUM`/`MAX`/
/// `MIN` keep the column's own scale. `gorun`: `23.0000`, `33.3333`, and
/// `2.666667 | 8.00 | 4.00 | 1.50`.
#[test]
fn decimal_division_and_avg_grow_the_scale_by_four() {
    let mut session = Session::new();
    assert_eq!(
        rows(&mut session, "SELECT 92/4, 100/3"),
        [["23.0000", "33.3333"]]
    );

    session.run("CREATE TABLE d (v DECIMAL(10,2))").unwrap();
    session
        .run("INSERT INTO d VALUES (1.50),(2.50),(4.00)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT avg(v), sum(v), max(v), min(v) FROM d"),
        [["2.666667", "8.00", "4.00", "1.50"]]
    );
}

// ---------------------------------------------------------------------------
// Divergences: the live engine answers these differently from real TiDB.
// Each test asserts the CAPTURED GO ANSWER and is left failing on purpose.
// ---------------------------------------------------------------------------

/// An integer PRIMARY KEY is the row handle, so assigning to it MOVES the row.
///
/// `gorun` over `(1,10),(2,20)`: `UPDATE pk SET a = a + 10 WHERE a = 1` answers
/// `2|20;11|10` — the row is rewritten under its new handle. The engine used to
/// write the new value back to the OLD handle, where the record format omits
/// the handle column, so the change was silently discarded and the reported
/// `Affected(1)` was a lie.
#[test]
fn update_of_an_integer_primary_key_rewrites_the_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pk (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO pk VALUES (1,10),(2,20)").unwrap();
    session.run("UPDATE pk SET a = a + 10 WHERE a = 1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM pk ORDER BY a"),
        [["2", "20"], ["11", "10"]]
    );
}

/// A clustered COMMON handle (a non-integer primary key) moves the same way:
/// `gorun` over `('x',10),('y',20)` answers `y|20;z|10` for
/// `UPDATE ch SET a = 'z' WHERE a = 'x'`.
#[test]
fn update_of_a_clustered_common_handle_rewrites_the_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ch (a VARCHAR(16) PRIMARY KEY CLUSTERED, b INT)")
        .unwrap();
    session
        .run("INSERT INTO ch VALUES ('x',10),('y',20)")
        .unwrap();
    session.run("UPDATE ch SET a = 'z' WHERE a = 'x'").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM ch ORDER BY a"),
        [["y", "20"], ["z", "10"]]
    );
}

/// A moved row's secondary index entries point AT its handle, so they are
/// rewritten even though the indexed column did not change: `gorun` answers
/// `11|10` for `SELECT a,b FROM si WHERE b = 10` after the move.
///
/// An UPDATE that leaves the primary key alone still rewrites in place:
/// `gorun` over `(1,10),(2,20)` answers `1|11;2|20` for `SET b = b + 1`.
#[test]
fn a_moved_row_keeps_its_secondary_index_entries_and_a_non_key_update_stays_put() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE si (a INT PRIMARY KEY, b INT, KEY kb(b))")
        .unwrap();
    session.run("INSERT INTO si VALUES (1,10),(2,20)").unwrap();
    session.run("UPDATE si SET a = a + 10 WHERE a = 1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM si WHERE b = 10"),
        [["11", "10"]]
    );
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM si ORDER BY a"),
        [["2", "20"], ["11", "10"]]
    );

    session
        .run("CREATE TABLE nopk (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO nopk VALUES (1,10),(2,20)")
        .unwrap();
    session
        .run("UPDATE nopk SET b = b + 1 WHERE a = 1")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM nopk ORDER BY a"),
        [["1", "11"], ["2", "20"]]
    );
}

/// Moving a row onto an occupied handle is a primary-key duplicate, reported
/// exactly as an INSERT's is. `gorun`:
/// `[kv:1062]Duplicate entry '2' for key 'col1.PRIMARY'`, and the table is left
/// as it was.
///
/// Assigning the primary key its existing value changes nothing, so `gorun`
/// reports zero affected rows rather than a self-collision.
#[test]
fn moving_a_row_onto_an_occupied_primary_key_is_a_duplicate_entry() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE col1 (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO col1 VALUES (1,10),(2,20)")
        .unwrap();
    let error = session
        .run("UPDATE col1 SET a = 2 WHERE a = 1")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(error.message, "Duplicate entry '2' for key 'col1.PRIMARY'");
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM col1 ORDER BY a"),
        [["1", "10"], ["2", "20"]]
    );

    session
        .run("CREATE TABLE nn (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO nn VALUES (1,10)").unwrap();
    session.run("UPDATE nn SET a = 1 WHERE a = 1").unwrap();
    assert_eq!(rows(&mut session, "SELECT a,b FROM nn"), [["1", "10"]]);
}

/// A multi-row UPDATE moves one row at a time and checks the primary key as it
/// goes, so whether `a = a + 1` over `(1,10),(2,20)` succeeds depends on the
/// ORDER BY: descending vacates `2` before `1` needs it and `gorun` answers
/// `2|10;3|20`, while ascending walks straight into the row still sitting at
/// `2` and `gorun` errors with
/// `[kv:1062]Duplicate entry '2' for key 'mr2.PRIMARY'`.
///
/// `ORDER BY ... LIMIT` on the key column picks the rows the same way: `gorun`
/// over `(1,10),(2,20),(3,30)` answers `1|10;2|20;103|30` for
/// `SET a = a + 100 ORDER BY a DESC LIMIT 1`.
#[test]
fn a_multi_row_key_update_moves_rows_one_at_a_time_in_order_by_order() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mr (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO mr VALUES (1,10),(2,20)").unwrap();
    session
        .run("UPDATE mr SET a = a + 1 ORDER BY a DESC")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM mr ORDER BY a"),
        [["2", "10"], ["3", "20"]]
    );

    session
        .run("CREATE TABLE mr2 (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO mr2 VALUES (1,10),(2,20)").unwrap();
    let error = session
        .run("UPDATE mr2 SET a = a + 1 ORDER BY a ASC")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(error.message, "Duplicate entry '2' for key 'mr2.PRIMARY'");

    session
        .run("CREATE TABLE ol (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO ol VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session
        .run("UPDATE ol SET a = a + 100 ORDER BY a DESC LIMIT 1")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM ol ORDER BY a"),
        [["1", "10"], ["2", "20"], ["103", "30"]]
    );
}

/// A `SET` list that swaps a clustered handle column with a plain one both
/// reads the original row and moves it: `gorun` over `(1,10)` with `a` the
/// integer PRIMARY KEY answers `10|1` for `SET a = b, b = a`.
#[test]
fn a_set_list_can_swap_a_clustered_handle_column_with_a_plain_one() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE sw3 (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO sw3 VALUES (1,10)").unwrap();
    session.run("UPDATE sw3 SET a = b, b = a").unwrap();
    assert_eq!(rows(&mut session, "SELECT a,b FROM sw3"), [["10", "1"]]);
}

/// Every `SET` assignment reads the row as it was BEFORE the statement, so
/// assignments never chain.
///
/// `gorun` over `(1,10)`: `SET a = 100, b = a` answers `100|1` — `b` takes the
/// ORIGINAL `a`. The engine used to evaluate each assignment over the row the
/// previous ones had already rewritten and answered `100|100`.
#[test]
fn update_set_assignments_all_read_the_original_row() {
    let mut session = Session::new();
    session.run("CREATE TABLE sw (a INT, b INT)").unwrap();
    session.run("INSERT INTO sw VALUES (1,10)").unwrap();
    session.run("UPDATE sw SET a = 100, b = a").unwrap();
    assert_eq!(rows(&mut session, "SELECT a,b FROM sw"), [["100", "1"]]);
}

/// The same rule over three columns: a `SET` list is a simultaneous
/// assignment, so it can rotate values with no temporary. `gorun` over
/// `(1,10,100)`: `SET c = a, a = b, b = c` answers `10|100|1`.
#[test]
fn update_set_assignments_rotate_values_without_a_temporary() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE sw2 (a INT, b INT, c INT)")
        .unwrap();
    session.run("INSERT INTO sw2 VALUES (1,10,100)").unwrap();
    session.run("UPDATE sw2 SET c = a, a = b, b = c").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a,b,c FROM sw2"),
        [["10", "100", "1"]]
    );
}

/// BUG: `GROUP BY TRUE` is treated as a constant (one group) instead of as the
/// positional reference it is.
///
/// TiDB lowers `TRUE` to the integer 1 before resolving `GROUP BY`, so
/// `GROUP BY TRUE` is exactly `GROUP BY 1`. `gorun` over `(1,10),(1,20),(2,30)`
/// answers `1|2;2|1` for `SELECT k, count(*) ... GROUP BY TRUE`, and errors on
/// `SELECT count(*) ... GROUP BY TRUE` because position 1 is an aggregate.
/// `GROUP BY FALSE` is position 0 and is likewise an error. The live engine
/// collapses all three into a single constant group.
#[test]
#[ignore = "live-engine bug: GROUP BY TRUE/FALSE is treated as a constant, not a position"]
fn group_by_true_is_the_position_one_reference() {
    let mut session = Session::new();
    session.run("CREATE TABLE gg (k INT, v INT)").unwrap();
    session
        .run("INSERT INTO gg VALUES (1,10),(1,20),(2,30)")
        .unwrap();

    assert_eq!(
        rows(&mut session, "SELECT k, count(*) FROM gg GROUP BY TRUE"),
        [["1", "2"], ["2", "1"]]
    );
    // Position 1 is an aggregate here, which cannot be grouped on.
    assert!(session
        .run("SELECT count(*) FROM gg GROUP BY TRUE")
        .is_err());
    // FALSE is position 0, which does not exist.
    assert!(session
        .run("SELECT k, count(*) FROM gg GROUP BY FALSE")
        .is_err());
}

/// BUG: `ONLY_FULL_GROUP_BY` is not enforced — a bare ungrouped column is
/// silently answered from an arbitrary row.
///
/// `gorun` rejects both queries below. `SELECT v, count(*) FROM gg` mixes a
/// bare column with an aggregate and no `GROUP BY`; the live engine answers
/// `10|3`, inventing a value. The pinning rule is also purely SYNTACTIC:
/// `GROUP BY k+0` does NOT pin bare `k` even though `k+0` determines it, and
/// `gorun` rejects that too, while the live engine answers `1|2;2|1`.
#[test]
#[ignore = "live-engine bug: ONLY_FULL_GROUP_BY is not enforced for bare ungrouped columns"]
fn a_bare_ungrouped_column_is_rejected() {
    let mut session = Session::new();
    session.run("CREATE TABLE gg (k INT, v INT)").unwrap();
    session
        .run("INSERT INTO gg VALUES (1,10),(1,20),(2,30)")
        .unwrap();

    assert!(session.run("SELECT v, count(*) FROM gg").is_err());
    assert!(session
        .run("SELECT k, count(*) FROM gg GROUP BY k+0")
        .is_err());
}

/// BUG: `CHAR_LENGTH` is not source-typed, so a binary-valued argument is
/// counted in characters instead of bytes.
///
/// TiDB picks `CHAR_LENGTH`'s signature from the argument's TYPE before
/// evaluating it: a hex literal and `UNHEX()` are binary, so their length is a
/// byte count. `gorun` answers 3 for both `char_length(0xE4BDA0)` and
/// `char_length(unhex('E4BDA0'))`, against 1 for the ordinary string literal
/// `'你'`. The live engine answers 1 for all three, decoding the bytes as UTF-8
/// it was never told to decode. Note `LENGTH(0xE4BDA0)` is already correct at
/// 3, so only the character-semantics signature choice is wrong.
#[test]
#[ignore = "live-engine bug: CHAR_LENGTH ignores the argument's binary type"]
fn char_length_counts_bytes_for_a_binary_argument() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "SELECT char_length(0xE4BDA0), char_length(unhex('E4BDA0')), char_length('你')"
        ),
        [["3", "3", "1"]]
    );
}
