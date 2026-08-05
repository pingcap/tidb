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

//! SQL-level `PREPARE` / `EXECUTE` / `DEALLOCATE PREPARE`, against captures
//! from a real (mock-backed) TiDB session.
//!
//! Every expectation in this file is a line of one of two captures taken
//! through a `pkg/session` session that reports the errno of each refusal --
//! the same parse/plan/execute path `pkg/executor/prepared.go` runs in. The
//! captured transcript, verbatim in the parts these tests assert:
//!
//! ```text
//! create table t (a int, b varchar(20), c decimal(10,2));         OK
//! insert into t values (1,'one',1.5),(2,'two',2.5),(3,'three',3.5),(null,null,null);  OK
//! prepare stmt from 'select * from t where a = ?';                OK
//! set @a = 2;                                                     OK
//! execute stmt using @a;                       RS[a|b|c]  2|two|2.50
//! prepare p0 from 'select a from t order by a';                   OK
//! execute p0;                                  RS[a]  <nil> / 1 / 2 / 3
//! execute p0 using @a;                         ERR 8112 [planner:8112]Wrong parameter count
//! execute stmt;                                ERR 8112 [planner:8112]Wrong parameter count
//! execute nosuch using @a;                     ERR 8111 [planner:8111]Prepared statement not found
//! deallocate prepare nosuch;                   ERR 8111 [planner:8111]Prepared statement not found
//! deallocate prepare p0;                                          OK
//! execute p0;                                  ERR 8111 [planner:8111]Prepared statement not found
//! prepare stmt from 'select b from t where a = ?';                OK
//! execute stmt using @a;                       RS[b]  two
//! set @sql = 'select c from t where a = ?';                       OK
//! prepare stmt2 from @sql;                                        OK
//! execute stmt2 using @a;                      RS[c]  2.50
//! prepare bad from 'selec 1';                  ERR 1064 [parser:1064]...near "selec 1"
//! execute bad;                                 ERR 8111 [planner:8111]Prepared statement not found
//! prepare multi from 'select 1; select 2';     ERR 8115 [executor:8115]Can not prepare multiple statements
//! prepare badt from 'select * from ?';         ERR 1064 [parser:1064]...near "?"
//! prepare badc from 'select ? from t';                            OK
//! prepare badc2 from 'select a from t order by ?';                OK
//! prepare d1 from 'select 1';                                     OK
//! drop prepare d1;                                                OK
//! execute d1;                                  ERR 8111 [planner:8111]Prepared statement not found
//! prepare st from 'select ?+1, ?';                                OK
//! set @i = 7;  set @s = '7';                                      OK
//! execute st using @i, @i;                     RS[?+1|?]  8|7
//! execute st using @s, @s;                     RS[?+1|?]  8|7
//! set @n = NULL;                                                  OK
//! execute st using @n, @n;                     RS[?+1|?]  <nil>|<nil>
//! execute st using @never_set, @never_set;     RS[?+1|?]  <nil>|<nil>
//! prepare st2 from 'select ?, ?';                                 OK
//! execute st2 using @a, @a;                    RS[?|?]  2|2
//! prepare ins from 'insert into t (a,b) values (?,?)';            OK
//! set @x = 9;  set @y = 'nine';                                   OK
//! execute ins using @x, @y;                                       OK
//! select a, b from t where a = 9;              RS[a|b]  9|nine
//! prepare upd from 'update t set b = ? where a = ?';              OK
//! execute upd using @y, @x;                                       OK
//! prepare del from 'delete from t where a = ?';                   OK
//! execute del using @x;                                           OK
//! select count(*) from t;                      RS[count(*)]  4
//! prepare lim from 'select a from t order by a limit ?';          OK
//! set @l = 2;                                                     OK
//! execute lim using @l;                        RS[a]  <nil> / 1
//! prepare inl from 'select a from t where a in (?, ?)';           OK
//! execute inl using @a, @x;                    RS[a]  2
//! set @num = 5;                                                   OK
//! prepare pn from @num;                        ERR 1064 [parser:1064]...near "5"
//! prepare pnull from @never_set;               ERR 1064 [parser:1064]...near "NULL"
//! execute st2 using 1, 2;                      ERR 1064 [parser:1064]...near "1, 2;"
//! execute st2 using @@sql_mode, @@sql_mode;    ERR 1064 [parser:1064]...near "@@sql_mode, ..."
//! prepare rp from 'select ?, ?, ?';                               OK
//! execute rp using @one, @one, @one;           RS[?|?|?]  1|1|1
//! execute rp using @one, @one;                 ERR 8112 [planner:8112]Wrong parameter count
//! ```
//!
//! and from the second capture (same tool, a table with a datetime and a
//! varbinary column):
//!
//! ```text
//! prepare ob from 'select a, b from t order by ?';                OK
//! execute ob using @one;                       RS[a|b]  1|one / 2|two / 3|three
//! execute ob using @two;                       RS[a|b]  1|one / 3|three / 2|two
//! prepare hv from 'select a from t having ?';                     OK
//! execute hv using @one;                       RS[a]  1 / 2 / 3
//! prepare lo from 'select a from t order by a limit ?, ?';        OK
//! execute lo using @one, @two;                 RS[a]  2 / 3
//! prepare hs from 'select ?';   execute hs using @s ('hello');  RS[?]  hello
//! prepare ha from 'select ? as x';  execute ha using @s;        RS[x]  hello
//! prepare dt from 'select a from t where d = ?';                  OK
//! set @dv = '2021-02-03 04:05:06';  execute dt using @dv;       RS[a]  2
//! set @hex = 0x42;  prepare hx2 from 'select ?';
//! execute hx2 using @hex;                      RS[?]  B
//! set @numstr = '2';  execute ns using @numstr;                 RS[a]  2
//! prepare lk from 'select b from t where b like ?';               OK
//! set @pat = 't%';  execute lk using @pat;                      RS[b]  two / three
//! prepare bt from 'select a from t where a between ? and ?';       OK
//! execute bt using @one, @two;                 RS[a]  1 / 2
//! begin; prepare tx from '...'; commit; execute tx using @one;  RS[a]  1
//! prepare pd from 'create table tt (a int)';  execute pd;          OK
//! prepare ps from 'set @z = ?';  execute ps using @one;  select @z;  RS[@z]  1
//! prepare MyStmt from 'select 1';                                 OK
//! execute mystmt;                              ERR 8111 [planner:8111]Prepared statement not found
//! deallocate prepare MYSTMT;                   ERR 8111 [planner:8111]Prepared statement not found
//! prepare sq from 'select a from t where a in (select a from t where b = ?)';  OK
//! set @bt = 'two';  execute sq using @bt;                       RS[a]  2
//! ```

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

/// Errno of a refusal, so a test asserts the code TiDB reports rather than
/// only that something failed.
fn errno(result: Result<StmtResult, DriverError>) -> u16 {
    match result {
        Ok(_) => panic!("expected the statement to be refused"),
        Err(error) => error.to_mysql_error().code,
    }
}

fn header_and_rows(session: &mut Session, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    crate::tests_support::query_text(session, sql)
}

/// The table both captures were taken over.
fn prepared_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int, b varchar(20), c decimal(10,2))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'one',1.5),(2,'two',2.5),(3,'three',3.5),(null,null,null)")
        .unwrap();
    session.run("SET @a = 2").unwrap();
    session
}

/// The plain shape the integrationtest suite writes most: a `SELECT` whose
/// `WHERE` carries one marker, bound from a user variable.
#[test]
fn prepared_select_binds_a_user_variable_by_value() {
    let mut session = prepared_session();
    session
        .run("PREPARE stmt FROM 'select * from t where a = ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE stmt USING @a")),
        [["2", "two", "2.50"]]
    );
}

/// `EXECUTE` with no `USING` runs a statement that carries no marker.
#[test]
fn prepared_statement_without_markers_executes_bare() {
    let mut session = prepared_session();
    session
        .run("PREPARE p0 FROM 'select a from t order by a'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE p0")),
        [["NULL"], ["1"], ["2"], ["3"]]
    );
}

/// Both directions of a count mismatch are `ErrWrongParamCount` (8112), NOT
/// the 1210 the wire's "Incorrect arguments to EXECUTE" would suggest: the
/// check is `planCachePreprocess`'s own, and the capture reports
/// `[planner:8112]Wrong parameter count` for each of the three shapes below.
#[test]
fn parameter_count_mismatch_is_wrong_parameter_count() {
    let mut session = prepared_session();
    session
        .run("PREPARE stmt FROM 'select * from t where a = ?'")
        .unwrap();
    session
        .run("PREPARE p0 FROM 'select a from t order by a'")
        .unwrap();
    session.run("PREPARE rp FROM 'select ?, ?, ?'").unwrap();
    // Values with no marker to bind.
    assert_eq!(errno(session.run("EXECUTE p0 USING @a")), 8112);
    // A marker with no value.
    assert_eq!(errno(session.run("EXECUTE stmt")), 8112);
    // Two values for three markers.
    assert_eq!(errno(session.run("EXECUTE rp USING @a, @a")), 8112);
}

/// A name the session does not hold is 8111, and it is the SAME error whether
/// the statement was never prepared, failed to prepare, or was deallocated.
#[test]
fn unknown_prepared_name_is_prepared_stmt_not_found() {
    let mut session = prepared_session();
    assert_eq!(errno(session.run("EXECUTE nosuch USING @a")), 8111);
    assert_eq!(errno(session.run("DEALLOCATE PREPARE nosuch")), 8111);
    // A failed PREPARE leaves the name unbound rather than half-bound.
    assert_eq!(errno(session.run("PREPARE bad FROM 'selec 1'")), 1064);
    assert_eq!(errno(session.run("EXECUTE bad")), 8111);
    // Deallocated.
    session.run("PREPARE p0 FROM 'select 1'").unwrap();
    session.run("DEALLOCATE PREPARE p0").unwrap();
    assert_eq!(errno(session.run("EXECUTE p0")), 8111);
}

/// `DROP PREPARE` is the same statement as `DEALLOCATE PREPARE`.
#[test]
fn drop_prepare_removes_the_statement() {
    let mut session = prepared_session();
    session.run("PREPARE d1 FROM 'select 1'").unwrap();
    session.run("DROP PREPARE d1").unwrap();
    assert_eq!(errno(session.run("EXECUTE d1")), 8111);
}

/// Re-preparing a name replaces what it held; the old statement is gone
/// rather than shadowed.
#[test]
fn re_preparing_a_name_replaces_the_statement() {
    let mut session = prepared_session();
    session
        .run("PREPARE stmt FROM 'select * from t where a = ?'")
        .unwrap();
    session
        .run("PREPARE stmt FROM 'select b from t where a = ?'")
        .unwrap();
    assert_eq!(row_text(session.run("EXECUTE stmt USING @a")), [["two"]]);
}

/// TiDB keys its prepared statements by the spelling `PREPARE` used, so the
/// names are case-SENSITIVE -- unlike almost every other identifier slot,
/// which is why the capture is the authority here.
#[test]
fn prepared_statement_names_are_case_sensitive() {
    let mut session = prepared_session();
    session.run("PREPARE MyStmt FROM 'select 1'").unwrap();
    assert_eq!(errno(session.run("EXECUTE mystmt")), 8111);
    assert_eq!(errno(session.run("DEALLOCATE PREPARE MYSTMT")), 8111);
    // The exact spelling still works, so nothing was lost.
    assert_eq!(row_text(session.run("EXECUTE MyStmt")), [["1"]]);
}

/// `PREPARE ... FROM @var` parses the variable's VALUE as SQL. A non-string
/// value is stringified first and an unset variable reads as the text `NULL`,
/// so both are syntax errors rather than silently accepted.
#[test]
fn prepare_from_a_user_variable_parses_its_value() {
    let mut session = prepared_session();
    session
        .run("SET @sql = 'select c from t where a = ?'")
        .unwrap();
    session.run("PREPARE stmt2 FROM @sql").unwrap();
    assert_eq!(row_text(session.run("EXECUTE stmt2 USING @a")), [["2.50"]]);

    session.run("SET @num = 5").unwrap();
    assert_eq!(errno(session.run("PREPARE pn FROM @num")), 1064);
    assert_eq!(errno(session.run("PREPARE pnull FROM @never_set")), 1064);
}

/// A syntax error surfaces at PREPARE, because the text is parsed there.
/// Two markers in a position where no VALUE may appear are also parse errors:
/// a table name is one, while a select field and an `ORDER BY` item are NOT --
/// both of those are legal marker positions in TiDB.
#[test]
fn prepare_parses_the_text_and_rejects_illegal_marker_positions() {
    let mut session = prepared_session();
    assert_eq!(errno(session.run("PREPARE bad FROM 'selec 1'")), 1064);
    assert_eq!(
        errno(session.run("PREPARE badt FROM 'select * from ?'")),
        1064
    );
    // Legal: a marker where a value may stand.
    session.run("PREPARE badc FROM 'select ? from t'").unwrap();
    session
        .run("PREPARE badc2 FROM 'select a from t order by ?'")
        .unwrap();
}

/// A text that parses into more than one statement is `ErrPrepareMulti`
/// (8115), while a single statement with a trailing semicolon is fine -- the
/// suite writes that form seven times.
#[test]
fn prepare_admits_one_statement_only() {
    let mut session = prepared_session();
    assert_eq!(
        errno(session.run("PREPARE multi FROM 'select 1; select 2'")),
        8115
    );
    session.run("PREPARE one FROM 'select * from t;'").unwrap();
    assert_eq!(row_text(session.run("EXECUTE one")).len(), 4);
}

/// The parameter's TYPE is the variable's type AT EXECUTE TIME, which is the
/// whole point of binding by variable rather than by literal. `?+1` is 8 for
/// both `@i = 7` and `@s = '7'`, while the bare `?` reports the value the
/// variable holds.
///
/// The column names are the ones Go gives an unaliased field: its own source
/// text, which for a marker is `?` however it was bound. A bound literal
/// would have named these `7+1` and `7`.
#[test]
fn parameter_type_comes_from_the_variable_and_the_header_stays_a_marker() {
    let mut session = prepared_session();
    session.run("PREPARE st FROM 'select ?+1, ?'").unwrap();
    session.run("SET @i = 7").unwrap();
    session.run("SET @s = '7'").unwrap();
    session.run("PREPARE st2 FROM 'select ?, ?'").unwrap();
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE st USING @i, @i"),
        (
            vec!["?+1".to_owned(), "?".to_owned()],
            vec![vec!["8".to_owned(), "7".to_owned()]]
        )
    );
    // The STRING case: `@s = '7'` binds as a string, and `?+1` coerces it the
    // way Go's arithmetic classes do. Captured from Go: `8|7`.
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE st USING @s, @s"),
        (
            vec!["?+1".to_owned(), "?".to_owned()],
            vec![vec!["8".to_owned(), "7".to_owned()]]
        )
    );
    // The bare `?` half -- the value's own kind surviving the binding -- is
    // pinned below and in
    // `parameter_values_keep_their_kind_through_the_binding`.
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE st2 USING @s, @s"),
        (
            vec!["?".to_owned(), "?".to_owned()],
            vec![vec!["7".to_owned(), "7".to_owned()]]
        )
    );
    // An explicit alias wins, as it does for any other field.
    session.run("PREPARE ha FROM 'select ? as x'").unwrap();
    session.run("SET @hello = 'hello'").unwrap();
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE ha USING @hello"),
        (vec!["x".to_owned()], vec![vec!["hello".to_owned()]])
    );
}

/// A field that is a bare COLUMN REFERENCE is named by its column identifier,
/// with the qualifier dropped -- and executing a prepared statement does not
/// change that.
///
/// `buildProjectionField` routes an `ast.ColumnNameExpr` field to
/// `buildProjectionFieldNameFromColumns`, which answers
/// `colNameField.Name.Name`; only a NON-column field falls through to the
/// source-text rule the marker cases above pin. This tier binds by restoring
/// the statement, so it pins the source text of the fields that need it --
/// and pinning a column reference installs an alias that OVERRIDES the
/// column identifier, which is a rename, not a preservation.
///
/// The recorded case is `executor/jointest/join`:
///
/// ```text
/// prepare stmt1 from 'select m1.a from t as m1
///                     where m1.a in (select m2.b+? from t as m2)';
/// set @a = 1; execute stmt1 using @a;   -- header `a`, row 2
/// ```
///
/// The same statement prepared with no `?` pins nothing and always printed
/// the correct `a`, which is why the marker is what exposed this.
#[test]
fn a_column_reference_keeps_its_column_name_through_execute() {
    let mut session = prepared_session();
    session.run("CREATE TABLE mt (a INT, b INT)").unwrap();
    session.run("INSERT INTO mt VALUES (1, 1), (2, 1)").unwrap();
    session
        .run(
            "PREPARE stmt1 FROM 'select m1.a from mt as m1 \
             where m1.a in (select m2.b+? from mt as m2)'",
        )
        .unwrap();
    session.run("SET @a = 1").unwrap();
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE stmt1 USING @a"),
        (vec!["a".to_owned()], vec![vec!["2".to_owned()]])
    );
    session.run("SET @a = 0").unwrap();
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE stmt1 USING @a"),
        (vec!["a".to_owned()], vec![vec!["1".to_owned()]])
    );

    // The identifier is taken AS WRITTEN (`m1.A` is the column `A`), the
    // qualifier is dropped, an explicit alias still wins, and a field that is
    // not a column reference still keeps the source text a restore would have
    // rewritten. All four in one statement, so the split is what is pinned.
    session
        .run("PREPARE mixed FROM 'select m1.A, m1.b, m1.a as x, m1.b+? from mt as m1 where m1.a = 1'")
        .unwrap();
    session.run("SET @i = 7").unwrap();
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE mixed USING @i"),
        (
            vec![
                "A".to_owned(),
                "b".to_owned(),
                "x".to_owned(),
                "m1.b+?".to_owned()
            ],
            vec![vec![
                "1".to_owned(),
                "1".to_owned(),
                "1".to_owned(),
                "8".to_owned()
            ]]
        )
    );
    // The invariant behind all of it: EXECUTE names its columns exactly as
    // the statement written out in full does.
    assert_eq!(
        header_and_rows(&mut session, "EXECUTE mixed USING @i").0,
        header_and_rows(
            &mut session,
            "select m1.A, m1.b, m1.a as x, m1.b+7 from mt as m1 where m1.a = 1"
        )
        .0
        .iter()
        .enumerate()
        .map(|(index, name)| if index == 3 {
            "m1.b+?".to_owned()
        } else {
            name.clone()
        })
        .collect::<Vec<_>>()
    );
}

/// A NULL parameter and an UNSET variable both bind NULL -- not the text
/// "NULL", which would have made `?+1` an error instead of NULL.
#[test]
fn null_and_unset_variables_bind_null() {
    let mut session = prepared_session();
    session.run("PREPARE st FROM 'select ?+1, ?'").unwrap();
    session.run("SET @n = NULL").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE st USING @n, @n")),
        [["NULL", "NULL"]]
    );
    assert_eq!(
        row_text(session.run("EXECUTE st USING @never_set, @never_set")),
        [["NULL", "NULL"]]
    );
}

/// The same variable may fill more than one marker; the suite writes
/// `execute stmt using @a, @a` nine times.
#[test]
fn one_variable_may_fill_several_markers() {
    let mut session = prepared_session();
    session.run("PREPARE st2 FROM 'select ?, ?'").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE st2 USING @a, @a")),
        [["2", "2"]]
    );
    session.run("PREPARE rp FROM 'select ?, ?, ?'").unwrap();
    session.run("SET @one = 1").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE rp USING @one, @one, @one")),
        [["1", "1", "1"]]
    );
}

/// INSERT, UPDATE and DELETE each bind and take effect, so the row the next
/// statement reads is the proof rather than the affected count.
#[test]
fn prepared_dml_binds_and_takes_effect() {
    let mut session = prepared_session();
    session
        .run("PREPARE ins FROM 'insert into t (a,b) values (?,?)'")
        .unwrap();
    session.run("SET @x = 9").unwrap();
    session.run("SET @y = 'nine'").unwrap();
    session.run("EXECUTE ins USING @x, @y").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t WHERE a = 9")),
        [["9", "nine"]]
    );
    session
        .run("PREPARE upd FROM 'update t set b = ? where a = ?'")
        .unwrap();
    session.run("SET @z = 'NINE'").unwrap();
    session.run("EXECUTE upd USING @z, @x").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t WHERE a = 9")),
        [["9", "NINE"]]
    );
    session
        .run("PREPARE del FROM 'delete from t where a = ?'")
        .unwrap();
    session.run("EXECUTE del USING @x").unwrap();
    assert_eq!(row_text(session.run("SELECT count(*) FROM t")), [["4"]]);
}

/// A marker may stand in `LIMIT`, in an `IN` list, in `LIKE`, in `BETWEEN`,
/// in `HAVING` and inside a subquery -- every value position the suite uses.
#[test]
fn markers_bind_in_every_value_position_the_suite_uses() {
    let mut session = prepared_session();
    session.run("SET @one = 1").unwrap();
    session.run("SET @two = 2").unwrap();

    session
        .run("PREPARE lim FROM 'select a from t order by a limit ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE lim USING @two")),
        [["NULL"], ["1"]]
    );

    session
        .run("PREPARE lo FROM 'select a from t order by a limit ?, ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE lo USING @one, @two")),
        [["1"], ["2"]]
    );

    session
        .run("PREPARE inl FROM 'select a from t where a in (?, ?)'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE inl USING @one, @two")),
        [["1"], ["2"]]
    );

    session
        .run("PREPARE lk FROM 'select b from t where b like ?'")
        .unwrap();
    session.run("SET @pat = 't%'").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE lk USING @pat")),
        [["two"], ["three"]]
    );

    session
        .run("PREPARE bt FROM 'select a from t where a between ? and ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE bt USING @one, @two")),
        [["1"], ["2"]]
    );

    session
        .run("PREPARE hv FROM 'select a from t where a is not null having ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE hv USING @one")),
        [["1"], ["2"], ["3"]]
    );

    session
        .run("PREPARE sq FROM 'select a from t where a in (select a from t where b = ?)'")
        .unwrap();
    session.run("SET @bt = 'two'").unwrap();
    assert_eq!(row_text(session.run("EXECUTE sq USING @bt")), [["2"]]);
}

/// `ORDER BY ?` is POSITIONAL, not a constant: with `@one = 1` the rows come
/// back in `a` order and with `@two = 2` in `b` order. Go reaches this through
/// `ConstructPositionExpr`, which turns a marker in an `ORDER BY` item into a
/// column position; here the bound integer literal is a position for the same
/// reason a written `ORDER BY 2` is.
#[test]
fn order_by_a_marker_orders_by_position() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int, b varchar(20))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'one'),(2,'two'),(3,'three')")
        .unwrap();
    session.run("SET @one = 1").unwrap();
    session.run("SET @two = 2").unwrap();
    session
        .run("PREPARE ob FROM 'select a, b from t order by ?'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE ob USING @one")),
        [["1", "one"], ["2", "two"], ["3", "three"]]
    );
    assert_eq!(
        row_text(session.run("EXECUTE ob USING @two")),
        [["1", "one"], ["3", "three"], ["2", "two"]]
    );
}

/// A string parameter compared against an int column converts as a written
/// literal would, and a hex parameter keeps its bytes: `SET @hex = 0x42`
/// reports `B`, which a lossy text conversion could not.
#[test]
fn parameter_values_keep_their_kind_through_the_binding() {
    let mut session = prepared_session();
    session.run("SET @numstr = '2'").unwrap();
    session
        .run("PREPARE ns FROM 'select a from t where a = ?'")
        .unwrap();
    assert_eq!(row_text(session.run("EXECUTE ns USING @numstr")), [["2"]]);

    session.run("SET @hex = 0x42").unwrap();
    session.run("PREPARE hx2 FROM 'select ?'").unwrap();
    assert_eq!(row_text(session.run("EXECUTE hx2 USING @hex")), [["B"]]);
}

/// A prepared `SET` and a prepared DDL both run: `EXECUTE` re-enters the same
/// dispatch a written statement takes, so nothing is special-cased by kind.
#[test]
fn a_prepared_statement_may_be_a_set_or_a_ddl() {
    let mut session = prepared_session();
    session.run("SET @one = 1").unwrap();
    session.run("PREPARE ps FROM 'set @z = ?'").unwrap();
    session.run("EXECUTE ps USING @one").unwrap();
    assert_eq!(row_text(session.run("SELECT @z")), [["1"]]);

    session
        .run("PREPARE pd FROM 'create table tt (a int)'")
        .unwrap();
    session.run("EXECUTE pd").unwrap();
    session.run("INSERT INTO tt VALUES (7)").unwrap();
    assert_eq!(row_text(session.run("SELECT a FROM tt")), [["7"]]);
}

/// Prepared statements outlive the transaction they were prepared in.
#[test]
fn a_prepared_statement_outlives_its_transaction() {
    let mut session = prepared_session();
    session.run("SET @one = 1").unwrap();
    session.run("BEGIN").unwrap();
    session
        .run("PREPARE tx FROM 'select a from t where a = ?'")
        .unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(row_text(session.run("EXECUTE tx USING @one")), [["1"]]);
}

/// The store is per-SESSION: a peer over the same catalog does not see it.
#[test]
fn prepared_statements_are_per_session() {
    let mut session = prepared_session();
    session.run("PREPARE mine FROM 'select 1'").unwrap();
    let mut peer = Session::with_catalog(session.shared_catalog());
    assert_eq!(errno(peer.run("EXECUTE mine")), 8111);
}

/// A prepared statement may not itself be a `PREPARE`, an `EXECUTE` or a
/// `DEALLOCATE`: Go's `GeneratePlanCacheStmtWithAST` refuses those kinds with
/// `ErrUnsupportedPs`. Captured:
/// `prepare pe from 'execute ob using @one'` is
/// `[executor:1295]This command is not supported in the prepared statement
/// protocol yet`, and `tests/integrationtest/t/executor/prepared.test` records
/// the same 1295 for a prepared `deallocate prepare stmt0` and for a prepared
/// `prepare stmt3 from '...'`.
#[test]
fn a_prepared_statement_may_not_be_a_prepared_statement_command() {
    let mut session = prepared_session();
    session.run("PREPARE inner0 FROM 'select 1'").unwrap();
    assert_eq!(
        errno(session.run("PREPARE outer1 FROM 'execute inner0'")),
        1295
    );
    assert_eq!(
        errno(session.run("PREPARE outer2 FROM 'deallocate prepare inner0'")),
        1295
    );
    assert_eq!(
        errno(session.run("PREPARE outer3 FROM \"prepare inner3 from 'select 1'\"")),
        1295
    );
    // The refusal happens at PREPARE, so no name was bound.
    assert_eq!(errno(session.run("EXECUTE outer1")), 8111);
}

/// A `LIMIT` parameter is admitted by KIND, not by what its text reads as: Go
/// takes only a non-negative `int64` or a `uint64`. Captured:
/// `execute l1 using @ls` for `@ls = '2'` and `execute l1 using @neg` for
/// `@neg = -1` are both `[planner:1210]Incorrect arguments to LIMIT`, while
/// `@l = 2` returns two rows. Binding the string would have produced
/// `LIMIT '2'` -- a different statement, quietly accepted or quietly refused
/// for the wrong reason.
#[test]
fn a_limit_parameter_must_be_a_non_negative_integer() {
    let mut session = prepared_session();
    session
        .run("PREPARE l1 FROM 'select a from t order by a limit ?'")
        .unwrap();
    session.run("SET @l = 2").unwrap();
    session.run("SET @ls = '2'").unwrap();
    session.run("SET @neg = -1").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE l1 USING @l")),
        [["NULL"], ["1"]]
    );
    assert_eq!(errno(session.run("EXECUTE l1 USING @ls")), 1210);
    assert_eq!(errno(session.run("EXECUTE l1 USING @neg")), 1210);
    // A decimal is refused too, even one whose value is a whole number
    // (captured: `set @dec = 2.0; execute l1 using @dec` is 1210).
    session.run("SET @dec = 2.0").unwrap();
    assert_eq!(errno(session.run("EXECUTE l1 USING @dec")), 1210);
    // The offset slot is checked the same way.
    session
        .run("PREPARE lo FROM 'select a from t order by a limit ?, ?'")
        .unwrap();
    assert_eq!(errno(session.run("EXECUTE lo USING @neg, @l")), 1210);
    assert_eq!(errno(session.run("EXECUTE lo USING @l, @ls")), 1210);
}

/// The `USING` list admits user variables only: a literal and a system
/// variable are both parse errors, which is why nothing downstream has to
/// consider them.
#[test]
fn using_admits_user_variables_only() {
    let mut session = prepared_session();
    session.run("PREPARE st2 FROM 'select ?, ?'").unwrap();
    assert_eq!(errno(session.run("EXECUTE st2 USING 1, 2")), 1064);
    assert_eq!(
        errno(session.run("EXECUTE st2 USING @@sql_mode, @@sql_mode")),
        1064
    );
}
