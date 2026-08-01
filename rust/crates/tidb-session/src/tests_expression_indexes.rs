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

//! `CREATE INDEX idx ON t((a + 1))` end to end, over a real session.
//!
//! Every expectation here was CAPTURED from real TiDB through
//! `rust/difftests/gorun` before it was written down; the script and its
//! answers are quoted in `tidb_executor::expression_index`'s module doc.
//!
//! The tests split in two, because the unit has two halves that fail
//! differently:
//!
//! * The hidden column must be INVISIBLE everywhere a user enumerates
//!   columns. An omission here is a SILENT WRONG ANSWER -- an extra column in
//!   `SELECT *`, an `INSERT` arity that no longer matches. Each enumeration
//!   site gets its own test so a regression names the site.
//! * The index must be MAINTAINED. An index that exists but is stale is worse
//!   than no index, so the maintenance tests end in `ADMIN CHECK TABLE`,
//!   which compares every index entry against the rows.
//!
//! Mirrors Go `pkg/ddl/create_table.go`'s `BuildHiddenColumnInfo` and the
//! expression-index path of `pkg/ddl/index.go`'s `CreateIndex`.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The error code a statement fails with, or `None` when it succeeded.
fn admin_check(session: &mut Session, table: &str, after: &str) {
    if let Err(error) = session.run(&format!("ADMIN CHECK TABLE {table}")) {
        panic!(
            "ADMIN CHECK TABLE {table} failed after {after}: {}",
            error.to_mysql_error().message
        );
    }
}

/// The error code a statement fails with, or `None` when it succeeded.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// `create table t (a int, b int); create index idx on t((a+1));`
fn indexed() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, b INT)").unwrap();
    session.run("CREATE INDEX idx ON t((a+1))").unwrap();
    session
}

/// The single `SHOW CREATE TABLE` line for a table.
fn show_create(session: &mut Session, table: &str) -> String {
    rows(session, &format!("SHOW CREATE TABLE {table}"))[0][1].clone()
}

// ---------------------------------------------------------------------------
// The hidden column is invisible
// ---------------------------------------------------------------------------

/// Captured: ``KEY `idx` ((`a` + 1))`` -- the index prints its EXPRESSION,
/// and the hidden column `_V$_idx_0` gets no definition line at all.
#[test]
fn show_create_table_prints_the_expression_and_not_the_hidden_column() {
    let mut session = indexed();
    let text = show_create(&mut session, "t");
    assert!(
        text.contains("KEY `idx` ((`a` + 1))"),
        "expected the index to print its expression, got:\n{text}"
    );
    assert!(
        !text.contains("_V$"),
        "the hidden column leaked into SHOW CREATE TABLE:\n{text}"
    );
}

/// Captured: `select * from t` returns TWO columns after `insert into t
/// values (1,2)`, not three.
#[test]
fn select_star_does_not_return_the_hidden_column() {
    let mut session = indexed();
    session.run("INSERT INTO t VALUES (1,2)").unwrap();
    assert_eq!(rows(&mut session, "SELECT * FROM t"), vec![vec!["1", "2"]]);
}

/// Captured: `insert into te values (5)` fails on arity when `te` has two
/// VISIBLE columns -- the hidden one is neither counted nor suppliable.
///
/// The comparison is against a CONTROL table with the same two declared
/// columns and no expression index, so what is asserted is that the two
/// tables have the same arity -- not this tier's own code for a value-count
/// mismatch, which is a separate gap (Go answers 1136, this answers 1105
/// outside the empty-`VALUES ()` case).
#[test]
fn insert_arity_counts_only_the_visible_columns() {
    let mut session = indexed();
    session.run("CREATE TABLE control (a INT, b INT)").unwrap();
    for values in ["(1)", "(1,2,3)"] {
        assert_eq!(
            code(&mut session, &format!("INSERT INTO t VALUES {values}")),
            code(
                &mut session,
                &format!("INSERT INTO control VALUES {values}")
            ),
            "arity of {values} must not depend on the expression index"
        );
        assert!(
            code(&mut session, &format!("INSERT INTO t VALUES {values}")).is_some(),
            "{values} is not two columns and must be refused"
        );
    }
    assert_eq!(code(&mut session, "INSERT INTO t VALUES (1,2)"), None);
}

/// Captured: `desc t` reports `a` and `b` only.
#[test]
fn describe_does_not_list_the_hidden_column() {
    let mut session = indexed();
    let names: Vec<String> = rows(&mut session, "DESC t")
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
    let names: Vec<String> = rows(&mut session, "SHOW COLUMNS FROM t")
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
}

/// Captured: `information_schema.columns` for a table with an expression
/// index reports only the declared columns, and ORDINAL_POSITION counts them
/// from 1 with no gap where the hidden column sits.
#[test]
fn information_schema_columns_skips_the_hidden_column() {
    let mut session = indexed();
    let reported = rows(
        &mut session,
        "SELECT column_name, ordinal_position FROM information_schema.columns \
         WHERE table_name = 't' ORDER BY ordinal_position",
    );
    assert_eq!(reported, vec![vec!["a", "1"], vec!["b", "2"]]);
}

/// Captured: `show index from te` leaves `Column_name` NULL for an expression
/// part and puts the expression in `Expression`; an ordinary part is the
/// other way round.
#[test]
fn show_index_reports_the_expression_and_a_null_column_name() {
    let mut session = Session::new();
    session.run("CREATE TABLE te (a INT)").unwrap();
    session.run("CREATE INDEX idxe ON te((a+1), a)").unwrap();
    let reported = rows(&mut session, "SHOW INDEX FROM te");
    // Column_name is field 4, Expression is field 14.
    assert_eq!(
        (reported[0][4].as_str(), reported[0][14].as_str()),
        ("NULL", "`a` + 1")
    );
    assert_eq!(
        (reported[1][4].as_str(), reported[1][14].as_str()),
        ("a", "NULL")
    );
}

/// Captured: `alter table te add column z int` puts `z` after `a` and before
/// the hidden column, so the declared columns stay contiguous.
#[test]
fn add_column_lands_before_the_hidden_tail() {
    let mut session = indexed();
    session.run("ALTER TABLE t ADD COLUMN z INT").unwrap();
    let text = show_create(&mut session, "t");
    let a = text.find("`a` int").expect("column a");
    let z = text.find("`z` int").expect("column z");
    assert!(a < z, "expected a before z in:\n{text}");
    assert!(!text.contains("_V$"), "hidden column leaked:\n{text}");
    session.run("INSERT INTO t VALUES (1,2,3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t"),
        vec![vec!["1", "2", "3"]]
    );
    admin_check(&mut session, "t", "ADD COLUMN");
}

/// Captured: `alter table te drop index idxe` takes the hidden column with
/// it, so the table's `INSERT` arity and `SHOW CREATE TABLE` go back to what
/// they were.
#[test]
fn dropping_the_index_drops_its_hidden_column() {
    let mut session = indexed();
    session.run("DROP INDEX idx ON t").unwrap();
    let text = show_create(&mut session, "t");
    assert!(!text.contains("idx"), "the index survived:\n{text}");
    assert!(!text.contains("_V$"), "hidden column leaked:\n{text}");
    session.run("INSERT INTO t VALUES (7,8)").unwrap();
    assert_eq!(rows(&mut session, "SELECT * FROM t"), vec![vec!["7", "8"]]);
}

/// Captured: two unnamed expression indexes become `expression_index` and
/// `expression_index_2`, since an expression part has no column to be named
/// after.
#[test]
fn an_unnamed_expression_index_is_called_expression_index() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tf (a INT, INDEX ((a+1)), INDEX ((a+2)))")
        .unwrap();
    let text = show_create(&mut session, "tf");
    assert!(
        text.contains("KEY `expression_index` ((`a` + 1))")
            && text.contains("KEY `expression_index_2` ((`a` + 2))"),
        "unexpected:\n{text}"
    );
}

// ---------------------------------------------------------------------------
// The index is maintained
// ---------------------------------------------------------------------------

/// `ADMIN CHECK TABLE` after every kind of write. This is the assertion the
/// whole unit rests on: it re-derives every index entry from the rows and
/// reports a mismatch, so a stale expression-index entry cannot pass here.
#[test]
fn admin_check_table_passes_across_every_write() {
    let mut session = indexed();
    let check = |session: &mut Session, after: &str| admin_check(session, "t", after);
    session
        .run("INSERT INTO t VALUES (1,2),(3,4),(5,6)")
        .unwrap();
    check(&mut session, "INSERT");
    session.run("UPDATE t SET a = 10 WHERE a = 1").unwrap();
    check(&mut session, "UPDATE of the indexed dependency");
    session.run("UPDATE t SET b = 99 WHERE a = 3").unwrap();
    check(&mut session, "UPDATE of an unindexed column");
    session.run("DELETE FROM t WHERE a = 5").unwrap();
    check(&mut session, "DELETE");
    // `t` has no unique key, so a REPLACE is an ordinary insert here -- what
    // it proves is that the index entry the new row needs is written.
    session.run("REPLACE INTO t VALUES (10, 77)").unwrap();
    check(&mut session, "REPLACE");
    assert_eq!(
        rows(&mut session, "SELECT * FROM t ORDER BY a, b"),
        vec![vec!["3", "99"], vec!["10", "2"], vec!["10", "77"]]
    );
}

/// An expression index built over EXISTING rows: `CREATE INDEX` backfills
/// from the materialized row, so the entries agree with rows written before
/// the index existed.
#[test]
fn creating_the_index_backfills_the_existing_rows() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, b INT)").unwrap();
    session.run("INSERT INTO t VALUES (1,2),(3,4)").unwrap();
    session.run("CREATE INDEX idx ON t((a+1))").unwrap();
    admin_check(&mut session, "t", "ADD COLUMN");
}

/// Captured: a UNIQUE expression index really enforces uniqueness --
/// `insert into u8 values ('AB')` after `('ab')` is 1062 naming `u8.i8`,
/// because `upper(a)` collides.
#[test]
fn a_unique_expression_index_enforces_uniqueness() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u8 (a VARCHAR(20), UNIQUE INDEX i8 ((upper(a))))")
        .unwrap();
    session.run("INSERT INTO u8 VALUES ('ab')").unwrap();
    assert_eq!(
        code(&mut session, "INSERT INTO u8 VALUES ('AB')"),
        Some(1062)
    );
    admin_check(&mut session, "u8", "a rejected duplicate");
}

/// A `CREATE TABLE` that declares the index inline reaches the same place as
/// `CREATE INDEX` does, so it gets the same check.
#[test]
fn an_inline_expression_index_is_maintained_too() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t2 (a INT, INDEX idx ((a+1)))")
        .unwrap();
    session.run("INSERT INTO t2 VALUES (1),(2)").unwrap();
    session.run("UPDATE t2 SET a = 8 WHERE a = 1").unwrap();
    admin_check(&mut session, "t2", "UPDATE");
    assert_eq!(
        show_create(&mut session, "t2"),
        "CREATE TABLE `t2` (\n  `a` int DEFAULT NULL,\n  KEY `idx` ((`a` + 1))\n) \
         ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

// ---------------------------------------------------------------------------
// The refusals, each with the errno gorun reported
// ---------------------------------------------------------------------------

/// Every refusal in one place, so a change to the admissibility scan has to
/// restate what Go answers rather than quietly widening the accepted set.
#[test]
fn refusals_match_gos_errnos() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE r (a INT, s VARCHAR(20))")
        .unwrap();
    // 3762: the expression is nothing but a column.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((a))"),
        Some(3762),
        "a bare column"
    );
    // 3758: a variable, a subquery, or `values(x)`.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((a + @@max_connections))"),
        Some(3758),
        "a system variable"
    );
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((values(a)))"),
        Some(3758),
        "values(x)"
    );
    // 8200: a function call outside GAFunction4ExpressionIndex.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((abs(a)))"),
        Some(8200),
        "a non-GA function"
    );
    // 1111: an aggregate.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((sum(a)))"),
        Some(1111),
        "an aggregate"
    );
    // 3800: a row value.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r(((a,a)))"),
        Some(3800),
        "a row value"
    );
    // 1054: a column the table does not have.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((zz + 1))"),
        Some(1054),
        "an unknown column"
    );
    // A GA function is ACCEPTED -- the whitelist is not a blanket refusal.
    assert_eq!(
        code(&mut session, "CREATE INDEX i ON r((upper(s)))"),
        None,
        "a GA function"
    );
}

/// Captured: 3754. An expression index may not read an `AUTO_INCREMENT`
/// column, because the value it would index is allocated, not written.
#[test]
fn an_expression_index_cannot_read_an_auto_increment_column() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ta (a INT AUTO_INCREMENT PRIMARY KEY, b INT)")
        .unwrap();
    assert_eq!(
        code(&mut session, "CREATE INDEX idxa ON ta((a+1))"),
        Some(3754)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE tb (a INT AUTO_INCREMENT PRIMARY KEY, INDEX idxb ((a+1)))"
        ),
        Some(3754)
    );
}

/// Captured: 1060. A user column already called `_V$_idxh_0` collides with
/// the name the hidden column would take.
#[test]
fn a_user_column_can_collide_with_the_hidden_columns_name() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE th (a INT, `_V$_idxh_0` INT)")
        .unwrap();
    assert_eq!(
        code(&mut session, "CREATE INDEX idxh ON th((a+1))"),
        Some(1060)
    );
}

/// A multi-valued index is a feature of its own -- it indexes each ELEMENT of
/// a JSON array, not the document -- so it is refused rather than built as an
/// ordinary scalar index under a multi-valued index's name. Go accepts it;
/// this is a named gap, not a claimed parity.
#[test]
fn a_multi_valued_index_is_refused_rather_than_built_as_a_scalar_one() {
    let mut session = Session::new();
    session.run("CREATE TABLE mv (j JSON)").unwrap();
    assert!(session
        .run("CREATE INDEX i ON mv((cast(j->'$.a' as unsigned array)))")
        .is_err());
}

/// A failed `CREATE INDEX` must leave the table exactly as it was: the hidden
/// column has to exist before the index is backfilled, so the failure path
/// takes it back off.
#[test]
fn a_failed_create_index_leaves_no_orphan_hidden_column() {
    let mut session = Session::new();
    session.run("CREATE TABLE u (a VARCHAR(20))").unwrap();
    session.run("INSERT INTO u VALUES ('ab'),('AB')").unwrap();
    assert_eq!(
        code(&mut session, "CREATE UNIQUE INDEX i ON u((upper(a)))"),
        Some(1062)
    );
    let text = show_create(&mut session, "u");
    assert!(!text.contains("_V$"), "orphan hidden column:\n{text}");
    assert_eq!(
        rows(&mut session, "SELECT * FROM u ORDER BY a"),
        vec![vec!["AB"], vec!["ab"]]
    );
    session.run("INSERT INTO u VALUES ('cd')").unwrap();
}

/// CAPTURED from TiDB: with `INDEX idx((a+b))`, dropping either column the
/// expression reads is 3837 `Column 'a' has an expression index dependency
/// and cannot be dropped or renamed`.
///
/// Before this check the drop SUCCEEDED, leaving the index's hidden generated
/// column reading a column that no longer existed -- so the regression this
/// guards is a corrupt table, not a missing message.
#[test]
fn a_column_an_expression_index_reads_cannot_be_dropped() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE fi (a INT, b INT, INDEX idx((a+b)))")
        .unwrap();
    for column in ["a", "b"] {
        let error = session
            .run(&format!("ALTER TABLE fi DROP COLUMN {column}"))
            .expect_err("the drop must be refused")
            .to_mysql_error();
        assert_eq!(error.code, 3837);
        assert_eq!(
            error.message,
            format!(
                "Column '{column}' has an expression index dependency and cannot be dropped or \
                 renamed"
            )
        );
    }
    // A column the expression does NOT read still drops.
    session
        .run("CREATE TABLE fi2 (a INT, b INT, c INT, INDEX idx((a+b)))")
        .unwrap();
    session.run("ALTER TABLE fi2 DROP COLUMN c").unwrap();
}

/// CAPTURED from TiDB: a rename orphans the hidden column exactly as a drop
/// does, and reports the same 3837 -- but only AFTER the same-name early
/// return, `_tidb_rowid` (1166) and the duplicate name (1060), which is the
/// order the three probes below assert.
#[test]
fn a_column_an_expression_index_reads_cannot_be_renamed() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE fr (a INT, b INT, INDEX idx((a+b)))")
        .unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE fr RENAME COLUMN a TO z"),
        Some(3837)
    );
    // Renaming a column to its own name is a no-op before any check.
    session.run("ALTER TABLE fr RENAME COLUMN a TO a").unwrap();
    // The duplicate-name and `_tidb_rowid` checks are captured as running
    // FIRST, so they keep their codes even on a depended-on column.
    assert_eq!(
        code(&mut session, "ALTER TABLE fr RENAME COLUMN a TO b"),
        Some(1060)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE fr RENAME COLUMN a TO _tidb_rowid"
        ),
        Some(1166)
    );
}
