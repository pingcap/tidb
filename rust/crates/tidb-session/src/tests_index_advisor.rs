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


//! The index advisor's statement tests, over a session.

use crate::tests_support::row_text;
use crate::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

fn error(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

/// Go `checkResult` of `indexadvisor_sql_test.go`: `db.table.columns` of
/// each recommendation, sorted.
fn recommended(session: &mut Session, statements: &str) -> String {
    let sql = format!(
        "recommend index run for '{}'",
        statements.replace('\'', "''")
    );
    let mut found: Vec<String> = rows(session, &sql)
        .iter()
        .map(|row| format!("{}.{}.{}", row[0], row[1], row[3]))
        .collect();
    found.sort();
    found.join("|")
}

/// `TestIndexAdvisorForSQL`, `ForMultipleTables`, `Prefix`, `CoveringIndex`
/// and `ExistingIndex`: the algorithm's picks, and the plan costs behind
/// them, follow Go's.
#[test]
fn advisor_picks_the_indexes_go_picks() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session.run("create table t (a int, b int, c int)").unwrap();
    assert_eq!(
        recommended(&mut session, "select a from t where a=1"),
        "test.t.a"
    );
    assert_eq!(
        recommended(&mut session, "select a from t where a=1 and b=1"),
        "test.t.a,b"
    );
    assert_eq!(
        recommended(
            &mut session,
            "select * from t where a=1;select * from t where a=1 and b=1"
        ),
        "test.t.a,b"
    );
    assert_eq!(
        recommended(
            &mut session,
            "select * from t where a=1;select * from t where a=1 and b=1; select * from t where a=1 and b=1 and c=1"
        ),
        "test.t.a,b,c"
    );
    session
        .run("create table tc (a int, b int, c int, d int)")
        .unwrap();
    assert_eq!(
        recommended(&mut session, "select b from tc where a=1"),
        "test.tc.a,b"
    );
    assert_eq!(
        recommended(&mut session, "select b from tc where a=1 and d=1"),
        "test.tc.a,d,b"
    );
    session
        .run("create table te (a int, b int, c int, index ab (a, b))")
        .unwrap();
    assert_eq!(recommended(&mut session, "select * from te where a=1"), "");
    assert_eq!(
        recommended(
            &mut session,
            "select * from te where a=1; select * from te where a=1 and b=1; select * from te where c=1"
        ),
        "test.te.c"
    );
    // A statement the optimizer plans with the DNF/order-by shape of
    // `heuristicMergeIndexes`: hypothetical indexes outside the stats
    // collection, as Go's session-local `IndexInfo`s are.
    assert_eq!(
        recommended(
            &mut session,
            "select * from t where (a=1 or b=1) and c=2 order by c"
        ),
        "test.t.c"
    );
    // A CTE is not inlined: the advisor never ran Go's preprocessor.
    assert_eq!(
        recommended(
            &mut session,
            "with c as (select * from t where a=1) select * from c where b=2"
        ),
        ""
    );
    // A locking read still sees hypothetical indexes.
    assert_eq!(
        recommended(&mut session, "select * from t where a=1 for update"),
        "test.t.a"
    );
}

/// The columns `recommend index run` answers, and the statement it prints.
#[test]
fn advisor_reports_go_s_columns_and_restored_queries() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session.run("create table t (a int, b int, c int)").unwrap();
    let answer = rows(
        &mut session,
        "recommend index run for 'select a, count(*) from t where a=1 and (b=2 or c=3)'",
    );
    assert_eq!(answer.len(), 1, "{answer:?}");
    assert_eq!(&answer[0][..5], ["test", "t", "idx_a", "a", "0"]);
    assert_eq!(
        answer[0][5],
        "Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` , count ( ? ) from `test` . `t` where `a` = ? and ( `b` = ? or `c` = ? )"
    );
    assert!(
        answer[0][6].contains(
            "\"Query\":\"SELECT `a`,count(1) FROM `test`.`t` WHERE `a` = 1 AND (`b` = 2 OR `c` = 3)\""
        ),
        "{answer:?}"
    );
    assert_eq!(answer[0][7], "CREATE INDEX idx_a ON t(a);");
    // No recommendation leaves Go's explanation as a warning.
    // An unqualified column names every table of the schema that has it,
    // so the empty case gets a schema of its own.
    session.run("create database kdb").unwrap();
    session.run("use kdb").unwrap();
    session
        .run("create table k (a int, b int, c int, key(a, b, c))")
        .unwrap();
    assert!(rows(
        &mut session,
        "recommend index run for 'select * from k where a=1 and b=1 and c=1'"
    )
    .is_empty());
    let warnings = rows(&mut session, "show warnings");
    assert_eq!(
        warnings,
        vec![vec![
            "Warning".to_owned(),
            "1105".to_owned(),
            " Considered 3 indexable columns(kdb.k.a, kdb.k.b, kdb.k.c), 3 or more index candidates(kdb.k(a), kdb.k(b), kdb.k(c)), no sufficiently beneficial indexes were found.".to_owned()
        ]]
    );
}

/// `options.go`: values are validated, stored in
/// `mysql.tidb_kernel_options`, and read back by SHOW.
#[test]
fn advisor_options_follow_go() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    let shown = rows(&mut session, "recommend index show");
    let names: Vec<&str> = shown.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        names,
        [
            "max_num_index",
            "max_index_columns",
            "max_num_query",
            "timeout"
        ]
    );
    assert_eq!(shown[0][1], "5");
    session
        .run("recommend index set max_num_index=3, timeout='10s'")
        .unwrap();
    let shown = rows(&mut session, "recommend index show");
    assert_eq!((shown[0][1].as_str(), shown[3][1].as_str()), ("3", "10s"));
    assert_eq!(
        error(&mut session, "recommend index set max_num_index=0"),
        (1105, "invalid value 0 for max_num_index".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index set timeout='abc'"),
        (1105, "time: invalid duration \"abc\"".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index set max_num_index='x'"),
        (1105, "invalid value type string".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index set timeout=10"),
        (
            1105,
            "invalid value type int64 for timeout, expected a duration string".to_owned()
        )
    );
    // A non-literal value makes Go's parser drop the whole list.
    assert_eq!(
        error(&mut session, "recommend index set max_num_index=-1"),
        (1105, "option is empty".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index frobnicate"),
        (1105, "unsupported action frobnicate".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index apply 1"),
        (1105, "unsupported action apply".to_owned())
    );
}

/// The refusals of `FilterInvalidQueries` under fix control 43817 and the
/// like, for a statement the user named.
#[test]
fn advisor_refuses_what_go_refuses() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session.run("create table t (a int, b int, c int)").unwrap();
    assert_eq!(
        error(
            &mut session,
            "recommend index run for 'select * from t where a=(select max(b) from t)'"
        ),
        (
            1105,
            "evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817"
                .to_owned()
        )
    );
    assert_eq!(
        error(
            &mut session,
            "recommend index run for 'select * from nosuch where a=1'"
        ),
        (1146, "Table 'test.nosuch' doesn't exist".to_owned())
    );
    assert_eq!(
        error(&mut session, "recommend index run for 'show tables'"),
        (
            1105,
            "empty query set after filtering invalid queries".to_owned()
        )
    );
    assert_eq!(
        error(&mut session, "recommend index run for ';;'"),
        (1105, "empty SQLs".to_owned())
    );
    assert_eq!(
        error(
            &mut session,
            "recommend index run for 'select * from t where a=1' with timeout='1ns'"
        ),
        (1105, "index advisor timeout after 1ns".to_owned())
    );
}

/// `RestoreWithDefaultDB` (binding flags): function names stay as written,
/// the lock clause keeps Go's spelling, and redundant parentheses go.
#[test]
fn binding_restore_keeps_written_names_and_drops_redundant_parentheses() {
    let restore = |sql: &str| {
        let statement = tidb_parser::parse(sql).unwrap();
        tidb_parser::util_parser::restore_with_default_db(&statement, "test", "")
    };
    assert_eq!(
        restore("select Max(a), COUNT(*), group_concat(a separator ',') from t for update"),
        "SELECT Max(`a`),COUNT(1),group_concat(`a` SEPARATOR ',') FROM `test`.`t` for update"
    );
    assert_eq!(
        restore("select * from t where a=1 for update of t"),
        "SELECT * FROM `test`.`t` WHERE `a` = 1 for update OF `test`.`t`"
    );
    assert_eq!(
        restore("select * from t where (a=1) and (b=2 and c=3) or a*(b*c)=1"),
        "SELECT * FROM `test`.`t` WHERE `a` = 1 AND `b` = 2 AND `c` = 3 OR `a` * (`b` * `c`) = 1"
    );
    assert_eq!(
        restore("select * from t where (a-1)-2=4 and a-(1-2)=4 and -(a+1)=1"),
        "SELECT * FROM `test`.`t` WHERE `a` - 1 - 2 = 4 AND `a` - (1 - 2) = 4 AND -(`a` + 1) = 1"
    );
    assert_eq!(
        restore("select count(distinct (b)) from t"),
        "SELECT count(DISTINCT `b`) FROM `test`.`t`"
    );
}

/// Go's resolver visits a window call's columns, so an aggregated query
/// must justify them.
#[test]
fn window_columns_in_an_aggregated_query_need_justifying() {
    let mut session = Session::new();
    session.run("use test").unwrap();
    session.run("create table t (a int, b int, c int)").unwrap();
    assert_eq!(
        error(
            &mut session,
            "select max(a), row_number() over (order by c) from t"
        )
        .0,
        8123
    );
    assert_eq!(
        error(
            &mut session,
            "select a, max(b), row_number() over (order by c) from t group by a"
        )
        .0,
        1055
    );
    assert_eq!(
        rows(
            &mut session,
            "select a, max(b), row_number() over (order by a) from t group by a"
        ),
        Vec::<Vec<String>>::new()
    );
}
