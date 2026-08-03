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

//! `GENERATED ALWAYS AS (...)` end to end: one test per rule
//! `tidb_executor::generated_column` transcreates, over a real session.
//!
//! Every expectation here was CAPTURED from real TiDB through
//! `rust/difftests/gorun` before it was written down -- the whole script is
//! preserved in this file's own layout, statement by statement, so a reader
//! can see which Go answer each assertion is quoting. The error codes come
//! from `tests/integrationtest/r/**`, TiDB's own recorded output, which is
//! where the exact 3105/3106/3107 message texts live.
//!
//! Mirrors Go `pkg/ddl/generated_column.go`, `pkg/table/column.go`'s
//! generated-column write rules, and `pkg/planner/core/planbuilder.go`'s
//! `getInsertColExpr` / `buildUpdateLists`.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The error code a statement fails with, or `None` when it succeeded.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// The error message a statement fails with.
fn message(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(_) => panic!("expected `{sql}` to fail"),
        Err(error) => error.to_mysql_error().message,
    }
}

/// `create table t1 (a int, b int as (a+1) virtual, c int as (b+1) stored)`.
fn chain() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a INT, b INT AS (a+1) VIRTUAL, c INT AS (b+1) STORED)")
        .unwrap();
    session
}

/// gorun: `insert into t1 (a) values (1),(2); select * from t1` answers
/// `1|2|3` and `2|3|4` -- both the virtual and the stored column computed.
#[test]
fn a_virtual_and_a_stored_column_are_both_computed_on_insert() {
    let mut session = chain();
    session.run("INSERT INTO t1 (a) VALUES (1),(2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t1 ORDER BY a"),
        vec![
            vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "3".to_owned(), "4".to_owned()],
        ]
    );
}

/// THE STALENESS TRAP. gorun: after `update t1 set a = 10 where a = 1`,
/// `select * from t1` answers `10|11|12` -- the STORED column was rewritten
/// from the new dependency, not left holding the value computed from the old
/// one.
#[test]
fn updating_a_dependency_recomputes_a_stored_column() {
    let mut session = chain();
    session.run("INSERT INTO t1 (a) VALUES (1),(2)").unwrap();
    session.run("UPDATE t1 SET a = 10 WHERE a = 1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t1 ORDER BY a"),
        vec![
            vec!["2".to_owned(), "3".to_owned(), "4".to_owned()],
            vec!["10".to_owned(), "11".to_owned(), "12".to_owned()],
        ]
    );
}

/// gorun: `insert into t1 (a,b) values (5,5)`, `insert into t1 (a,c) values
/// (5,5)` and `update t1 set b = 3 where a = 2` are all errors. The code and
/// text are TiDB's own recorded 3105.
#[test]
fn writing_to_a_generated_column_is_refused() {
    let mut session = chain();
    session.run("INSERT INTO t1 (a) VALUES (2)").unwrap();
    assert_eq!(
        message(&mut session, "INSERT INTO t1 (a,b) VALUES (5,5)"),
        "The value specified for generated column 'b' in table 't1' is not allowed."
    );
    assert_eq!(
        code(&mut session, "INSERT INTO t1 (a,c) VALUES (5,5)"),
        Some(3105)
    );
    assert_eq!(
        code(&mut session, "UPDATE t1 SET b = 3 WHERE a = 2"),
        Some(3105)
    );
    // No column list means every column is a target, so a full VALUES row
    // writes to the generated ones too.
    assert_eq!(
        code(&mut session, "INSERT INTO t1 VALUES (7,8,9)"),
        Some(3105)
    );
}

/// gorun: `insert into t9 values (1, default)` and `insert into t9 (a,b)
/// values (3, default)` both succeed -- `DEFAULT` is the one permitted value
/// for a generated column, and it means "use the expression".
#[test]
fn default_is_the_one_permitted_value_for_a_generated_column() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t9 (a INT PRIMARY KEY, b INT AS (a+1) STORED, UNIQUE KEY(b))")
        .unwrap();
    session.run("INSERT INTO t9 VALUES (1, DEFAULT)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t9"),
        vec![vec!["1".to_owned(), "2".to_owned()]]
    );
    session.run("INSERT INTO t9 (a) VALUES (2)").unwrap();
    session
        .run("INSERT INTO t9 (a,b) VALUES (3, DEFAULT)")
        .unwrap();
    // gorun: `delete from t9 where b = 2` then `select * from t9` answers
    // `2|3` and `3|4` -- a generated column is usable as a predicate.
    session.run("DELETE FROM t9 WHERE b = 2").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t9 ORDER BY a"),
        vec![
            vec!["2".to_owned(), "3".to_owned()],
            vec!["3".to_owned(), "4".to_owned()],
        ]
    );
}

/// gorun: `create table t2 (a int, b int as (c+1), c int)` is ACCEPTED. Only
/// a GENERATED dependency has to be prior; an ordinary column may come later.
#[test]
fn a_later_ordinary_column_may_be_a_dependency() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t2 (a INT, b INT AS (c+1), c INT)")
        .unwrap();
    session.run("INSERT INTO t2 (a,c) VALUES (1,5)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t2"),
        vec![vec!["1".to_owned(), "6".to_owned(), "5".to_owned()]]
    );
}

/// gorun: `create table t3 (a int, b int as (zz+1))` is an error; TiDB's own
/// recorded text is `Unknown column 'z' in 'generated column function'`.
#[test]
fn an_unknown_dependency_is_refused_by_name() {
    let mut session = Session::new();
    assert_eq!(
        message(&mut session, "CREATE TABLE t3 (a INT, b INT AS (zz+1))"),
        "Unknown column 'zz' in 'generated column function'"
    );
}

/// Go `verifyColumnGeneration`: a generated column that reads a generated
/// column defined at or after it is 3107.
#[test]
fn a_non_prior_generated_dependency_is_refused() {
    let mut session = Session::new();
    assert_eq!(
        message(
            &mut session,
            "CREATE TABLE t (a INT, b INT AS (c+1), c INT AS (a+1))"
        ),
        "Generated column can refer only to generated columns defined prior to it."
    );
    // Its own value is a non-prior reference too.
    assert_eq!(
        code(&mut session, "CREATE TABLE t (a INT, b INT AS (b+1))"),
        Some(3107)
    );
}

/// gorun: `create table tb (a int, b int as (a+1), primary key(b))` is an
/// error; TiDB's recorded text is the 3106 form.
#[test]
fn a_virtual_generated_column_may_not_be_the_primary_key() {
    let mut session = Session::new();
    assert_eq!(
        message(
            &mut session,
            "CREATE TABLE tb (a INT, b INT AS (a+1), PRIMARY KEY(b))"
        ),
        "'Defining a virtual generated column as primary key' is not supported for generated columns."
    );
    // A STORED one has a value in the row, so it can be.
    session
        .run("CREATE TABLE tc (a INT, b INT AS (a+1) STORED, PRIMARY KEY(b))")
        .unwrap();
    session.run("INSERT INTO tc (a) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM tc"),
        vec![vec!["1".to_owned(), "2".to_owned()]]
    );
}

/// gorun `show create table t1` -- the expression round-trips in Go's own
/// restored spelling, and `VIRTUAL` is what an omitted keyword prints.
#[test]
fn show_create_table_round_trips_the_expression() {
    let mut session = chain();
    let text = rows(&mut session, "SHOW CREATE TABLE t1")[0][1].clone();
    assert!(
        text.contains("`b` int GENERATED ALWAYS AS (`a` + 1) VIRTUAL,"),
        "{text}"
    );
    assert!(
        text.contains("`c` int GENERATED ALWAYS AS (`b` + 1) STORED\n"),
        "{text}"
    );
    // gorun `show create table t4`: an omitted keyword prints VIRTUAL, and a
    // generated column prints no DEFAULT at all.
    session
        .run("CREATE TABLE t4 (a INT, b INT AS (a+1), c INT AS (b+1))")
        .unwrap();
    let text = rows(&mut session, "SHOW CREATE TABLE t4")[0][1].clone();
    assert!(
        text.contains("`c` int GENERATED ALWAYS AS (`b` + 1) VIRTUAL\n"),
        "{text}"
    );
    assert!(!text.contains("VIRTUAL DEFAULT"), "{text}");
    // gorun `show create table t8`: `NOT NULL` still trails the clause.
    session
        .run("CREATE TABLE t8 (a INT, b INT AS (a+1) NOT NULL)")
        .unwrap();
    let text = rows(&mut session, "SHOW CREATE TABLE t8")[0][1].clone();
    assert!(
        text.contains("`b` int GENERATED ALWAYS AS (`a` + 1) VIRTUAL NOT NULL"),
        "{text}"
    );
}

/// THE INDEX TRAP. gorun over `t5 (a int, b int as (a+1) virtual, key(b))`:
/// after `update t5 set a = 100 where a = 1`, reading through the index and
/// reading around it answer the SAME rows. A virtual column is not in the row
/// bytes, so the index is the only place its old value could survive.
#[test]
fn an_index_over_a_virtual_column_agrees_with_the_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b INT AS (a+1) VIRTUAL, KEY(b))")
        .unwrap();
    session
        .run("INSERT INTO t5 (a) VALUES (1),(2),(3)")
        .unwrap();
    // gorun: `select b from t5 where b = 3` answers `3`.
    assert_eq!(
        rows(&mut session, "SELECT b FROM t5 WHERE b = 3"),
        vec![vec!["3".to_owned()]]
    );
    session.run("UPDATE t5 SET a = 100 WHERE a = 1").unwrap();
    let expected = vec![
        vec!["2".to_owned(), "3".to_owned()],
        vec!["3".to_owned(), "4".to_owned()],
        vec!["100".to_owned(), "101".to_owned()],
    ];
    assert_eq!(
        rows(&mut session, "SELECT * FROM t5 USE INDEX (b) ORDER BY b"),
        expected
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM t5 IGNORE INDEX (b) ORDER BY b"),
        expected
    );
}

/// The same for a STORED column, whose value IS in the bytes: gorun over
/// `t6 (a int, b int as (a+1) stored, key(b))` after `update t6 set a = 50`.
#[test]
fn an_index_over_a_stored_column_agrees_with_the_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t6 (a INT, b INT AS (a+1) STORED, KEY(b))")
        .unwrap();
    session.run("INSERT INTO t6 (a) VALUES (1),(2)").unwrap();
    session.run("UPDATE t6 SET a = 50 WHERE a = 1").unwrap();
    let expected = vec![
        vec!["2".to_owned(), "3".to_owned()],
        vec!["50".to_owned(), "51".to_owned()],
    ];
    assert_eq!(
        rows(&mut session, "SELECT * FROM t6 USE INDEX (b) ORDER BY b"),
        expected
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM t6 IGNORE INDEX (b) ORDER BY b"),
        expected
    );
}

/// gorun: `create table ta (a int as (1+1)); insert into ta values ();
/// select * from ta` answers `2` -- a table of nothing but generated columns
/// still takes a row.
#[test]
fn a_table_of_only_generated_columns_takes_a_row() {
    let mut session = Session::new();
    session.run("CREATE TABLE ta (a INT AS (1+1))").unwrap();
    session.run("INSERT INTO ta VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM ta"),
        vec![vec!["2".to_owned()]]
    );
}

/// gorun `create table t7 (a int, b int generated always as (a+1))`: the long
/// spelling is the same column.
#[test]
fn generated_always_as_is_the_same_column() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t7 (a INT, b INT GENERATED ALWAYS AS (a+1))")
        .unwrap();
    session.run("INSERT INTO t7 (a) VALUES (4)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t7"),
        vec![vec!["4".to_owned(), "5".to_owned()]]
    );
}

/// A virtual column's value must never reach the row bytes: reading it back
/// through a projection that drops its dependency still computes it, which is
/// only possible if the dependency survived the column pruning.
#[test]
fn a_pruned_scan_still_computes_a_virtual_column() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tp (a INT, b INT AS (a+1) VIRTUAL, c INT)")
        .unwrap();
    session.run("INSERT INTO tp (a,c) VALUES (7,0)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT b FROM tp"),
        vec![vec!["8".to_owned()]]
    );
}

/// An `ALTER TABLE` that MODIFIES a generated column is still refused, and
/// the refusal is loud.
///
/// This test used to assert that ADDING one was refused too, and that
/// assertion was ENCODING A GAP rather than a rule: Go accepts
/// `ALTER TABLE ... ADD COLUMN ... AS (expr) VIRTUAL`, and so does this tier
/// now -- see
/// [`adding_a_virtual_generated_column_by_alter_computes_over_existing_rows`].
#[test]
fn modifying_a_generated_column_by_alter_is_refused_loudly() {
    let mut session = chain();
    assert!(session
        .run("ALTER TABLE t1 MODIFY COLUMN b BIGINT AS (a+1) VIRTUAL")
        .is_err());
}

// A generated column's expression is evaluated under the SQL MODE of the
// statement that writes the row, exactly as any other expression of that
// statement is. Evaluating it under no mode at all made
// `ERROR_FOR_DIVISION_BY_ZERO` unreachable, so a write that TiDB refuses
// stored a NULL instead -- a silent wrong VALUE in every stored generated
// column, not an expression-index quirk.
//
// The whole script below was captured from real TiDB (mock store, default
// `sql_mode` = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,
// NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,
// NO_ENGINE_SUBSTITUTION`):
//
//   create table d1 (a int, b int as (100/a) stored);
//   insert into d1 (a) values (5);            -- OK
//   insert into d1 (a) values (0);            -- ERR 1365 Division by 0
//   select * from d1;                         -- 5|20
//   create table d2 (a int, b int as (100/a) virtual);
//   insert into d2 (a) values (0);            -- ERR 1365 Division by 0
//   create table e2 (a int);
//   insert into e2 values (0),(5);
//   alter table e2 add index i((100/a));      -- ERR 1365 Division by 0
//   set sql_mode='';
//   create table d3 (a int, b int as (100/a) stored);
//   insert into d3 (a) values (0);            -- OK
//   select * from d3;                         -- 0|<nil>
//   create table e3 (a int);
//   insert into e3 values (0),(5);
//   alter table e3 add index i((100/a));      -- OK

/// The write is refused, and the table keeps only the row that computed.
/// STORED and VIRTUAL alike: what fails is the EXPRESSION, before the
/// question of where the value is kept ever arises.
#[test]
fn a_zero_divisor_in_a_generated_column_fails_the_write_under_the_default_mode() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE d1 (a INT, b INT AS (100/a) STORED)")
        .unwrap();
    session.run("INSERT INTO d1 (a) VALUES (5)").unwrap();
    assert_eq!(
        code(&mut session, "INSERT INTO d1 (a) VALUES (0)"),
        Some(1365)
    );
    assert_eq!(
        message(&mut session, "INSERT INTO d1 (a) VALUES (0)"),
        "Division by 0"
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM d1"),
        vec![vec!["5".to_owned(), "20".to_owned()]]
    );

    session
        .run("CREATE TABLE d2 (a INT, b INT AS (100/a) VIRTUAL)")
        .unwrap();
    assert_eq!(
        code(&mut session, "INSERT INTO d2 (a) VALUES (0)"),
        Some(1365)
    );
    assert!(rows(&mut session, "SELECT * FROM d2").is_empty());
}

/// An `UPDATE` recomputes the column, so it is the same write and the same
/// refusal -- and the row it could not compute is left as it was.
#[test]
fn an_update_that_makes_a_generated_column_divide_by_zero_is_refused() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u1 (a INT, b INT AS (100/a) STORED)")
        .unwrap();
    session.run("INSERT INTO u1 (a) VALUES (5)").unwrap();
    assert_eq!(code(&mut session, "UPDATE u1 SET a=0"), Some(1365));
    assert_eq!(
        rows(&mut session, "SELECT * FROM u1"),
        vec![vec!["5".to_owned(), "20".to_owned()]]
    );
}

/// An index backfill is a WRITE of the entries it computes, so it evaluates
/// at the write level too. This is the case that made the bug visible: the
/// hidden column an expression index adds is generated, and an index built
/// over a value TiDB refuses to compute is an index over a value that does
/// not exist.
#[test]
fn an_expression_index_backfill_is_refused_when_a_row_divides_by_zero() {
    let mut session = Session::new();
    session.run("CREATE TABLE e2 (a INT)").unwrap();
    session.run("INSERT INTO e2 VALUES (0),(5)").unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE e2 ADD INDEX i((100/a))"),
        Some(1365)
    );
    // A refused backfill leaves the rows exactly as they were.
    assert_eq!(
        rows(&mut session, "SELECT * FROM e2 ORDER BY a"),
        vec![vec!["0".to_owned()], vec!["5".to_owned()]]
    );
}

/// THE CONTROL, and the reason the mode is threaded rather than the division
/// special-cased: without `ERROR_FOR_DIVISION_BY_ZERO` the very same
/// statements are ACCEPTED and the column reads NULL. A fix that turned these
/// into errors would be worse than the bug it removed.
#[test]
fn without_error_for_division_by_zero_the_same_writes_are_accepted() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("CREATE TABLE d3 (a INT, b INT AS (100/a) STORED)")
        .unwrap();
    session.run("INSERT INTO d3 (a) VALUES (0)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM d3"),
        vec![vec!["0".to_owned(), "NULL".to_owned()]]
    );
    session.run("CREATE TABLE e3 (a INT)").unwrap();
    session.run("INSERT INTO e3 VALUES (0),(5)").unwrap();
    session.run("ALTER TABLE e3 ADD INDEX i((100/a))").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM e3 ORDER BY a"),
        vec![vec!["0".to_owned()], vec!["5".to_owned()]]
    );
}

/// `ALTER TABLE ... ADD COLUMN <name> <type> AS (expr) VIRTUAL`, and the
/// STORED half TiDB refuses. Captured from real TiDB through `gorun`:
///
/// ```text
/// create table g (a int, b varchar(20), d int as (a+1) virtual)
/// insert into g (a,b) values (1,'p'),(2,'q')
/// alter table g add column f int as (a*2) stored
///   -> Error|3106|'Adding generated stored column through ALTER TABLE'
///      is not supported for generated columns.
/// alter table g add column e varchar(60) as (concat(b,'yy')) virtual  -> OK
/// select * from g   -> 1|p|2|pyy ; 2|q|3|qyy
/// ```
///
/// The `select` is the load-bearing half: a VIRTUAL column added by ALTER
/// computes over rows that were written BEFORE it existed, because nothing
/// was ever stored for it. A refusal-shaped implementation, or one that added
/// the column as an ordinary NULL one, would both pass a DDL-only assertion.
#[test]
fn adding_a_virtual_generated_column_by_alter_computes_over_existing_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE g (a INT, b VARCHAR(20), d INT AS (a+1) VIRTUAL)")
        .unwrap();
    session
        .run("INSERT INTO g (a,b) VALUES (1,'p'),(2,'q')")
        .unwrap();

    assert_eq!(
        message(
            &mut session,
            "ALTER TABLE g ADD COLUMN f INT AS (a*2) STORED"
        ),
        "'Adding generated stored column through ALTER TABLE' is not supported for \
         generated columns."
    );

    session
        .run("ALTER TABLE g ADD COLUMN e VARCHAR(60) AS (concat(b,'yy')) VIRTUAL")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM g ORDER BY a"),
        vec![
            vec![
                "1".to_owned(),
                "p".to_owned(),
                "2".to_owned(),
                "pyy".to_owned()
            ],
            vec![
                "2".to_owned(),
                "q".to_owned(),
                "3".to_owned(),
                "qyy".to_owned()
            ],
        ]
    );
}

/// `ALTER TABLE ... MODIFY COLUMN` may not change a column's generated-ness
/// in EITHER direction, and Go words all of it for the STORED case even when
/// the column is VIRTUAL. Captured from real TiDB:
///
/// ```text
/// alter table g modify column d int              -- d IS `a+1` VIRTUAL
///   -> Error|3106|'Changing the STORED status' is not supported for generated columns.
/// alter table g modify column a bigint as (1) virtual   -- a is ORDINARY
///   -> Error|3106|'Changing the STORED status' is not supported for generated columns.
/// ```
///
/// NOT PORTED, and pinned here so the gap is visible: Go ACCEPTS a MODIFY
/// that keeps the generated-ness, including one that REPLACES the expression
/// (`alter table g modify column d int as (a+5) virtual` succeeds and the
/// rows read back recomputed). This tier refuses it rather than applying the
/// new type with the OLD expression still attached.
#[test]
fn modifying_a_columns_generated_status_is_3106_either_way() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE g (a INT, d INT AS (a+1) VIRTUAL)")
        .unwrap();
    let stored_status = "'Changing the STORED status' is not supported for generated columns.";
    assert_eq!(
        message(&mut session, "ALTER TABLE g MODIFY COLUMN d INT"),
        stored_status,
        "generated -> ordinary"
    );
    assert_eq!(
        message(
            &mut session,
            "ALTER TABLE g MODIFY COLUMN a BIGINT AS (1) VIRTUAL"
        ),
        stored_status,
        "ordinary -> generated"
    );
    assert_eq!(
        code(&mut session, "ALTER TABLE g MODIFY COLUMN d INT"),
        Some(3106)
    );
    // The gap Go does not have.
    assert_eq!(
        message(
            &mut session,
            "ALTER TABLE g MODIFY COLUMN d BIGINT AS (a+1) VIRTUAL"
        ),
        "ALTER TABLE MODIFY COLUMN of a generated column is not supported yet",
        "Go accepts this and replaces the expression"
    );
}
