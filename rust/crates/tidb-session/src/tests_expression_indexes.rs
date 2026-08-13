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

/// A MODIFY that keeps the column's NAME is refused too, with 3106.
///
/// This tier used to accept it. The rename half of Go's rule was ported and
/// the other half was not, on the reasoning -- written into the code -- that
/// "a MODIFY that keeps the name leaves every generated expression reading a
/// name that still resolves, so only the rename half is a problem". Names are
/// not the only thing an expression index depends on: it also depends on the
/// column's TYPE, because the hidden generated column's stored values were
/// computed from it. Accepting the MODIFY left the index in place over a
/// column whose type had moved, so a later read used an index whose
/// expression no longer matched the column.
///
/// Go raises `ErrUnsupportedOnGeneratedColumn` (3106) with the inner error's
/// full `Error()` text -- class prefix included -- as its argument, so the
/// wire message nests one error inside another. CAPTURED from a mock-backed
/// TiDB session, and identical in
/// `tests/integrationtest/r/ddl/column_change.result:12`:
///
/// ```text
/// create table t(a int, b int as (a+1) virtual, c int)
///   alter table t modify a bigint  -> 3106 '[ddl:3108]Column 'a' has a generated column dependency.' is not supported for generated columns.
///   alter table t modify a int     -> 3106 (same)
///   alter table t change a a2 int  -> 3108 Column 'a' has a generated column dependency.
///   alter table t modify c bigint  -> OK
/// create table e(a varchar(10), c int, index idx((lower(a))))
///   alter table e modify a varchar(20) -> 3106 '[ddl:3837]Column 'a' has an expression index dependency and cannot be dropped or renamed' is not supported for generated columns.
///   alter table e modify a varchar(10) -> 3106 (same)
///   alter table e change a a2 varchar(10) -> 3837
///   alter table e modify c bigint  -> OK
/// ```
///
/// Note the second line of each block: the refusal fires even when the type is
/// UNCHANGED. Go computes the dependency once and never asks what the new type
/// is, so `modify a int` on an `int` column is refused like any other.
///
/// PINNED to refusal. If the expression-index/generated-column rewrite is ever
/// implemented, these become successes and this test has to be rewritten
/// deliberately -- it is not a placeholder for a rewrite Go performs, because
/// Go does not rewrite here any more than it does on RENAME.
#[test]
fn modifying_a_depended_on_column_is_refused_even_without_a_rename() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE gm (a INT, b INT AS (a+1) VIRTUAL, c INT)")
        .unwrap();
    for statement in [
        "ALTER TABLE gm MODIFY a BIGINT",
        // Type unchanged: still refused.
        "ALTER TABLE gm MODIFY a INT",
    ] {
        let error = session
            .run(statement)
            .expect_err("the modify must be refused")
            .to_mysql_error();
        assert_eq!(error.code, 3106, "{statement}");
        assert_eq!(
            error.message,
            "'[ddl:3108]Column 'a' has a generated column dependency.' is not supported for \
             generated columns.",
            "{statement}",
        );
    }
    // The rename half keeps its own code, unwrapped.
    assert_eq!(
        code(&mut session, "ALTER TABLE gm CHANGE a a2 INT"),
        Some(3108)
    );

    let mut session = Session::new();
    session
        .run("CREATE TABLE em (a VARCHAR(10), c INT, INDEX idx((LOWER(a))))")
        .unwrap();
    for statement in [
        "ALTER TABLE em MODIFY a VARCHAR(20)",
        "ALTER TABLE em MODIFY a VARCHAR(10)",
    ] {
        let error = session
            .run(statement)
            .expect_err("the modify must be refused")
            .to_mysql_error();
        assert_eq!(error.code, 3106, "{statement}");
        assert_eq!(
            error.message,
            "'[ddl:3837]Column 'a' has an expression index dependency and cannot be dropped or \
             renamed' is not supported for generated columns.",
            "{statement}",
        );
    }
    assert_eq!(
        code(&mut session, "ALTER TABLE em CHANGE a a2 VARCHAR(10)"),
        Some(3837)
    );

    // The control: a column NOTHING depends on still modifies, in both
    // tables. Without this, refusing every MODIFY would pass the assertions
    // above.
    session.run("ALTER TABLE em MODIFY c BIGINT").unwrap();
}

/// Go `variable.GAFunction4ExpressionIndex`, entry by entry, so REMOVING one
/// from the port flips its row to 8200 and DELETES a statement Go accepts.
///
/// Every code here was captured through `rust/difftests/gorun`, and every one
/// is now asserted EXACTLY: the fifteen JSON rows are refused DOWNSTREAM of
/// the function gate by `pkg/ddl/index.go`'s `checkIndexColumn`, on the
/// hidden column's RESULT TYPE rather than on the function's name.
#[test]
fn every_ga_function_passes_the_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE g (a CHAR(20), b JSON, i INT)")
        .unwrap();
    // (expression, Go's end-to-end code). `0` is accepted; 3753/3757 are the
    // JSON/BLOB result-type gate, which sits AFTER the function gate and is
    // keyed on the result type -- so a row moving between 0 and 3753 is a
    // type-derivation change, not a list change.
    let ga: &[(&str, u16)] = &[
        ("lower(a)", 0),
        ("upper(a)", 0),
        ("md5(a)", 0),
        ("reverse(a)", 0),
        ("vitess_hash(i)", 0),
        ("tidb_shard(i)", 0),
        ("json_type(b)", 0),
        ("json_extract(b,'$.a')", 3753),
        ("json_unquote(json_extract(b,'$.a'))", 3757),
        ("json_array(i)", 3753),
        ("json_object('k',i)", 3753),
        ("json_set(b,'$.a',1)", 3753),
        ("json_insert(b,'$.a',1)", 3753),
        ("json_replace(b,'$.a',1)", 3753),
        ("json_remove(b,'$.a')", 3753),
        ("json_contains(b,'1')", 0),
        ("json_contains_path(b,'one','$.a')", 0),
        ("json_valid(a)", 0),
        ("json_array_append(b,'$','x')", 3753),
        ("json_array_insert(b,'$[0]','x')", 3753),
        ("json_merge_patch(b,b)", 3753),
        ("json_merge_preserve(b,b)", 3753),
        ("json_pretty(b)", 3757),
        ("json_quote(a)", 0),
        ("json_schema_valid('{}',b)", 0),
        ("json_search(b,'one','x')", 3753),
        ("json_storage_size(b)", 0),
        ("json_depth(b)", 0),
        ("json_keys(b)", 3753),
        ("json_length(b)", 0),
    ];
    assert_eq!(ga.len(), 30, "Go's list has 30 entries");
    for (n, (expr, go_code)) in ga.iter().enumerate() {
        let got = code(&mut session, &format!("CREATE INDEX ga{n} ON g(({expr}))"));
        assert_ne!(
            got,
            Some(8200),
            "{expr} is on GAFunction4ExpressionIndex; Go answers {go_code}, never 8200"
        );
        // `json_pretty`, `json_contains_path`, `json_storage_size` and
        // `json_schema_valid` have no evaluator in this tier yet, so they are
        // refused 1105 BEFORE the result-type gate is reached. That is a
        // wrong-REFUSE, the safe direction, and it is the only code allowed
        // to stand in for Go's here.
        if got == Some(1105) {
            continue;
        }
        let expected = (*go_code != 0).then_some(*go_code);
        assert_eq!(got, expected, "{expr}: Go answers {go_code}");
    }
}

/// The mirror of [`every_ga_function_passes_the_gate`]: ADDING any of these
/// to the port would accept a statement Go refuses. `lcase`/`ucase` are the
/// sharp pair -- they are the same builtins as `lower`/`upper` under a second
/// name, and Go lists only the first name, so the gate is the LIST and not a
/// notion of which functions are safe.
#[test]
fn a_function_off_the_ga_list_is_8200() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE n (a CHAR(20), b JSON, i INT)")
        .unwrap();
    for (n, expr) in [
        "lcase(a)",
        "ucase(a)",
        "concat(a,'x')",
        "abs(i)",
        "length(a)",
        "sha1(a)",
        "ifnull(i,0)",
        "coalesce(i,0)",
        "substring(a,1,2)",
        "left(a,2)",
        "hex(a)",
        "bin(i)",
        "crc32(a)",
        "vec_dims(a)",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX ng{n} ON n(({expr}))")),
            Some(8200),
            "{expr} is not on GAFunction4ExpressionIndex"
        );
    }
}

/// The gate walks the WHOLE expression, not its outermost call: a non-GA
/// function anywhere under a GA one is still 8200, and a GA one under a
/// non-GA one does not rescue it.
#[test]
fn the_gate_walks_into_nested_calls() {
    let mut session = Session::new();
    session.run("CREATE TABLE w (a CHAR(20), i INT)").unwrap();
    for (n, expr) in [
        "lower(concat(a,'x'))",
        "concat(lower(a),'x')",
        "upper(left(a,2))",
        "md5(concat(a,a))",
        "abs(i)+lower(a)",
        "cast(abs(i) as signed)",
        "abs(cast(i as signed))",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX ne{n} ON w(({expr}))")),
            Some(8200),
            "{expr} hides a non-GA call"
        );
    }
    // ... and a GA call nested in a GA call is still accepted.
    assert_eq!(
        code(&mut session, "CREATE INDEX ok ON w((lower(upper(a))))"),
        None,
        "lower(upper(a)) is GA all the way down"
    );
}

/// Go COLLECTS every flag `illegalFunctionChecker` trips and then reports
/// them in a FIXED order, so which error a mixed expression gets does not
/// depend on where in the tree the offender sits. 8200 is reported LAST, so
/// anything else in the same expression outranks it.
///
/// Captured from `gorun`; an early-returning walk answers 8200 for the first
/// six of these and is what this pins against.
#[test]
fn gos_report_order_outranks_the_8200_gate() {
    let mut session = Session::new();
    session.run("CREATE TABLE o (a CHAR(20), i INT)").unwrap();
    for (n, (expr, want)) in [
        // 3758 (a blocked function / a variable / `values(x)`) beats 8200.
        ("rand() + sum(i)", 3758),
        ("abs(rand())", 3758),
        ("lower(rand())", 3758),
        ("abs(i) + @@max_connections", 3758),
        ("abs(i) + @x", 3758),
        ("abs(i) + values(i)", 3758),
        ("extract(year from now())", 3758),
        // 1111 (an aggregate) beats 8200, whichever side it is on.
        ("abs(i) + sum(i)", 1111),
        ("sum(i) + abs(i)", 1111),
        ("interval(i,1,2) + sum(i)", 1111),
        // 3800 (a row value) beats 8200.
        ("abs(i) + (i,i)", 3800),
        ("lower(a) + (i,i)", 3800),
        ("trim(a) + (i,i)", 3800),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX or{n} ON o(({expr}))")),
            Some(*want),
            "{expr}: Go reports its flags in a fixed order and 8200 comes last"
        );
    }
}

/// Go `expression.IllegalFunctions4GeneratedColumns` is 3758 in an expression
/// index, and it outranks the GA gate, so a blocked name is 3758 and not
/// 8200 even though it is equally absent from the GA list. Dropping an entry
/// would turn its row into 8200 -- a different error for the same statement.
#[test]
fn a_blocked_function_is_3758_not_8200() {
    let mut session = Session::new();
    session.run("CREATE TABLE il (a CHAR(20), i INT)").unwrap();
    for (n, expr) in [
        "rand()",
        "now()",
        "curdate()",
        "curtime()",
        "sysdate()",
        "unix_timestamp(i)",
        "uuid()",
        "version()",
        "database()",
        "connection_id()",
        "user()",
        "last_insert_id()",
        "found_rows()",
        "benchmark(1, 1)",
        "sleep(0)",
        "json_merge('[]','[]')",
        "utc_timestamp()",
        "values(i)",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX bl{n} ON il(({expr}))")),
            Some(3758),
            "{expr} is on IllegalFunctions4GeneratedColumns"
        );
    }
}

/// Go runs `VerifyArgsWrapper` inside the same walk, so a GA call with the
/// wrong number of arguments is 1582 -- BEFORE the 8200 gate and before the
/// expression is ever built. Without it a bad call is silently ACCEPTED here,
/// which is how `lower(a,a)` used to build an index.
#[test]
fn a_ga_call_with_the_wrong_argument_count_is_1582() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ar (a CHAR(20), b JSON, i INT)")
        .unwrap();
    for (n, expr) in [
        "lower(a,a)",
        "json_extract(b)",
        "json_schema_valid('{}')",
        "json_contains(b,'1','$.a','x')",
        "json_keys(b,'$.a','x')",
        "json_length(b,'$.a','x')",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX ay{n} ON ar(({expr}))")),
            Some(1582),
            "{expr} has an argument count Go's baseFunctionClass refuses"
        );
    }
}

/// The forms Go's grammar builds as `*ast.FuncCallExpr` under a name of its
/// own -- and a temporal LITERAL, which is `dateliteral`/`timeliteral`/
/// `timestampliteral` to that walk. None of those names is on the GA list, so
/// every one of them is 8200 rather than a bare "not supported".
///
/// The literal row is the one this index could not survive either way: the
/// value is folded in the writing session's `@@time_zone` and stored in the
/// key, so a reader in another zone would compute a key its own rows no
/// longer match.
#[test]
fn the_forms_go_reaches_as_function_calls_are_8200() {
    let mut session = Session::new();
    session.run("CREATE TABLE fc (a CHAR(20), i INT)").unwrap();
    for (n, expr) in [
        "interval(i,1,2)",
        "extract(year from '2020-01-01')",
        "position('a' in a)",
        "trim(a)",
        "weight_string(a)",
        "timestampadd(day,1,'2020-01-01')",
        "timestampdiff(day,'2020-01-01','2020-01-02')",
        "get_format(date,'USA')",
        "i member of ('[1,2]')",
        "convert(a using utf8mb4)",
        "substring(a from 1 for 2)",
        "timestamp '2020-01-01 00:00:00'",
        "date '2020-01-01'",
        "time '10:00:00'",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX fk{n} ON fc(({expr}))")),
            Some(8200),
            "{expr} is a function call to Go's walk, under a name off the GA list"
        );
    }
}

/// The other half of the gate: Go's checker only ever looks at FUNCTION
/// CALLS, so operators, `CASE`, `CAST`, `LIKE`, `REGEXP`, `COLLATE`, a
/// charset introducer and a bare literal are ACCEPTED however un-GA they
/// look. Widening the refusal to "anything that is not a GA call" would
/// delete every row here.
#[test]
fn the_forms_go_never_function_checks_are_accepted() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE op (a CHAR(20), b JSON, i INT)")
        .unwrap();
    for (n, expr) in [
        "i+1",
        "-i",
        "i>1",
        "i is null",
        "i div 2",
        "i in (1,2)",
        "i between 1 and 2",
        "case when i>1 then 1 else 2 end",
        "cast(a as char(10))",
        "cast(i as signed)",
        "lower(a) collate utf8mb4_bin",
        "lower(a) like 'x%'",
        "a regexp 'x'",
        "lower(a) is not null",
        "not lower(a)",
        "binary a",
        "_utf8mb4'x'",
        "json_extract(b,'$.a')=1",
        "json_length(b)+1",
        "(lower(a))",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX pn{n} ON op(({expr}))")),
            None,
            "{expr} never reaches Go's function check"
        );
    }
}

/// The gate is per KEY PART and per statement shape: `CREATE TABLE ... KEY`,
/// `UNIQUE KEY`, `ALTER TABLE ADD INDEX` and a part that is not the first all
/// reach it, and a GENERATED COLUMN does not -- Go passes `typeColumn` there,
/// and the 8200 arm is `genType == typeIndex` only.
#[test]
fn the_gate_covers_every_index_shape_but_not_a_generated_column() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE s1 (a CHAR(20), KEY k((lcase(a))))"
        ),
        Some(8200),
        "CREATE TABLE ... KEY"
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE s2 (a CHAR(20), UNIQUE KEY k((lcase(a))))"
        ),
        Some(8200),
        "CREATE TABLE ... UNIQUE KEY"
    );
    session.run("CREATE TABLE s3 (a CHAR(20), i INT)").unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE s3 ADD INDEX k((concat(a,'x')))"),
        Some(8200),
        "ALTER TABLE ADD INDEX"
    );
    assert_eq!(
        code(&mut session, "CREATE INDEX k2 ON s3(i, (lcase(a)))"),
        Some(8200),
        "a non-first key part"
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE INDEX k3 ON s3((lower(a)), (lcase(a)))"
        ),
        Some(8200),
        "a second expression part"
    );
    // A GENERATED COLUMN takes Go's `typeColumn` path, which has no GA gate
    // at all -- captured: `create table t(a char(20), c char(20) as
    // (lcase(a)))` is ACCEPTED, and so is indexing that column afterwards.
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE s4 (a CHAR(20), c CHAR(20) AS (lcase(a)), KEY (c))"
        ),
        None,
        "a generated column is not gated by the GA list"
    );
}

// ---------------------------------------------------------------------------
// `checkIndexColumn` over the HIDDEN column: the result-TYPE refusals
// ---------------------------------------------------------------------------

/// Go `pkg/ddl/index.go`'s `checkIndexColumn` JSON arm, reached over a hidden
/// column: `col.Hidden` turns 3152 into 3753.
///
/// This is a rule about the RESULT TYPE, not about the function name -- every
/// expression here is on `GAFunction4ExpressionIndex` and passes the function
/// gate. `->` is `json_extract` to the parser and `CAST(x AS JSON)` is not a
/// function call at all, and both are refused for the same reason.
///
/// Captured through the DDL probe against real TiDB:
///
/// ```text
/// create index i on t((json_extract(j,'$.a')))  3753
/// create index i on t((j->'$.a'))               3753
/// create index i on t((cast(j as json)))        3753
/// create index i on t((json_extract(j,'$.a')+0)) OK  -- bigint result
/// ```
#[test]
fn an_expression_index_whose_result_is_json_is_3753() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE j (a CHAR(20), b JSON, i INT)")
        .unwrap();
    for (n, expr) in [
        "json_extract(b,'$.a')",
        "b->'$.a'",
        "cast(b as json)",
        "json_array(i)",
        "json_object('k',i)",
        "json_set(b,'$.a',1)",
        "json_insert(b,'$.a',1)",
        "json_replace(b,'$.a',1)",
        "json_remove(b,'$.a')",
        "json_array_append(b,'$','x')",
        "json_array_insert(b,'$[0]','x')",
        "json_merge_patch(b,b)",
        "json_merge_preserve(b,b)",
        "json_search(b,'one','x')",
        "json_keys(b)",
        // The TOP-level function decides, as Go's `expr.GetType()` does: a
        // JSON call under an arithmetic operator has a bigint result and is
        // ACCEPTED. Without this row the gate could be written as "the tree
        // mentions a JSON function", which refuses a statement Go accepts.
        "json_keys(json_extract(b,'$.a'))",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX jx{n} ON j(({expr}))")),
            Some(3753),
            "{expr}"
        );
    }
    assert_eq!(
        code(
            &mut session,
            "CREATE INDEX jok ON j((json_extract(b,'$.a')+0))"
        ),
        None,
        "an arithmetic result is a bigint to Go, not JSON"
    );
    // The wording is Go's, captured: it names neither the index nor the
    // expression.
    let error = session
        .run("CREATE INDEX jmsg ON j((json_extract(b,'$.a')))")
        .unwrap_err();
    assert_eq!(
        error.to_mysql_error().message,
        "Cannot create an expression index on a function that returns a JSON or GEOMETRY value"
    );
}

/// The same arm's BLOB/TEXT half: `col.Hidden` turns 1170 into 3757, and an
/// expression key part has no length syntax to escape it with.
///
/// Go types `JSON_UNQUOTE` and `JSON_PRETTY` LONGTEXT (measured: `tp=251`,
/// flen 4294967295 and 67108864), which `types.IsTypeBlob` calls a BLOB. `->>`
/// is `json_unquote(json_extract(...))` to the parser.
///
/// ```text
/// create index i on t((json_unquote(b)))  3757
/// create index i on t((b->>'$.a'))        3757
/// ```
#[test]
fn an_expression_index_whose_result_is_text_is_3757() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (a CHAR(20), b JSON, i INT)")
        .unwrap();
    for (n, expr) in [
        "json_unquote(b)",
        "b->>'$.a'",
        "json_unquote(json_extract(b,'$.a'))",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX ux{n} ON u(({expr}))")),
            Some(3757),
            "{expr}"
        );
    }
    // `CAST` is what Go's own message recommends, and it works: the result is
    // a `varchar(20)`, which is neither JSON nor a BLOB.
    assert_eq!(
        code(
            &mut session,
            "CREATE INDEX uok ON u((cast(json_unquote(b) as char(20))))"
        ),
        None,
        "Go's own suggested escape from 3757"
    );
    let error = session
        .run("CREATE INDEX umsg ON u((json_unquote(b)))")
        .unwrap_err();
    assert_eq!(
        error.to_mysql_error().message,
        "Cannot create an expression index on an expression that returns a BLOB or TEXT. \
         Please consider using CAST"
    );
}

/// The refusals reach `ALTER TABLE ... ADD INDEX` and `CREATE TABLE` too, not
/// just `CREATE INDEX`: Go runs `buildIndexColumns` -- and so
/// `checkIndexColumn` -- from one place for all three.
#[test]
fn the_result_type_refusals_reach_every_index_statement() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE r1 (b JSON, KEY k((json_extract(b,'$.a'))))"
        ),
        Some(3753),
        "CREATE TABLE"
    );
    session.run("CREATE TABLE r2 (b JSON)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE r2 ADD INDEX k((json_extract(b,'$.a')))"
        ),
        Some(3753),
        "ALTER TABLE ADD INDEX"
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE r2 ADD INDEX k2((json_unquote(b)))"
        ),
        Some(3757),
        "ALTER TABLE ADD INDEX, the BLOB half"
    );
    // A non-first key part reaches it as well -- the check is per part.
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE r3 (a INT, b JSON, KEY k(a, (json_extract(b,'$.a'))))"
        ),
        Some(3753),
        "a second key part"
    );
}

/// The three rows the type gate could not reach until the STRING family's
/// argument-driven flen landed. All three are `checkIndexColumn` arms that
/// read the hidden column's WIDTH rather than its family, and all three were
/// wrong-ACCEPTS here.
///
/// What decides each is Go's `getFunction` copying `args[0]`'s flen onto the
/// result and `baseBuiltinFunc.getRetTp` then re-typing a wide one:
///
/// ```text
/// index i((lower(mt)))  MEDIUMTEXT  flen 16777215 -> mediumblob -> 3757
/// index i((lower(lt)))  LONGTEXT    flen 4294967295 -> longblob -> 3757
/// index i((lower(t)))   TEXT        flen 65535, still var_string -> 1071
/// index i((lower(v)))   VARCHAR(0)  flen 0                       -> 3761
/// ```
///
/// The TEXT row is the one that proves the rule is Go's flen and not a
/// family test: 65535 is one short of `getRetTp`'s MEDIUM boundary, so the
/// result is NOT a blob and the refusal is the index-too-long 1071 (65535
/// characters at four bytes each is 262140, against a 3072-byte limit)
/// rather than 3757.
#[test]
fn a_string_builtins_argument_width_reaches_the_index_type_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE w (mt MEDIUMTEXT, lt LONGTEXT, t TEXT, v VARCHAR(0), c VARCHAR(20))")
        .unwrap();
    for name in ["lower", "upper", "reverse"] {
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX i1 ON w(({name}(mt)))")),
            Some(3757),
            "{name}(mediumtext)"
        );
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX i2 ON w(({name}(lt)))")),
            Some(3757),
            "{name}(longtext)"
        );
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX i3 ON w(({name}(t)))")),
            Some(1071),
            "{name}(text)"
        );
        assert_eq!(
            code(&mut session, &format!("CREATE INDEX i4 ON w(({name}(v)))")),
            Some(3761),
            "{name}(varchar(0))"
        );
        // The control: a width the gate is happy with is still accepted, so
        // the derivation did not simply refuse the whole family.
        assert_eq!(
            code(
                &mut session,
                &format!("CREATE INDEX ok_{name} ON w(({name}(c)))")
            ),
            None,
            "{name}(varchar(20))"
        );
    }
}

/// Go expands a parenthesized `ADD COLUMN` list into its column actions
/// before its constraints. The grouped key may therefore name the newly added
/// column, and it backfills rows already present in the table.
#[test]
fn grouped_add_columns_applies_its_index_after_the_new_column() {
    let mut session = Session::new();
    session.run("CREATE TABLE grouped (a INT)").unwrap();
    session.run("INSERT INTO grouped VALUES (1), (2)").unwrap();
    session
        .run("ALTER TABLE grouped ADD (b INT DEFAULT 7, KEY kb(b))")
        .unwrap();

    assert_eq!(
        rows(&mut session, "SELECT a, b FROM grouped ORDER BY a"),
        vec![vec!["1", "7"], vec!["2", "7"]]
    );
    admin_check(&mut session, "grouped", "the grouped index backfill");
    assert!(
        show_create(&mut session, "grouped").contains("KEY `kb` (`b`)"),
        "the grouped constraint must be stored in table metadata"
    );
}
