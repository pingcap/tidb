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


//! Tests of `tidb_executor::compiler::plan_cacheable_checker` over a session.

use tidb_executor::compiler::PlanCacheableChecker;

use crate::Session;

fn verdict(session: &mut Session, sql: &str) -> Result<(), String> {
    // A node session's catalog catches up with its node first, as
    // every statement and PREPARE through the node does.
    session.refresh_node_catalog().expect("refresh catalog");
    let mut statement = session.parse_statement(sql).expect("parse");
    let catalog = session.lock_catalog().expect("catalog");
    session.prepared_statement_cacheable(&mut statement, &catalog)
}

#[test]
fn parameterized_window_frames_are_never_cacheable() {
    let mut session = Session::new();
    assert_eq!(
        verdict(
            &mut session,
            "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN ? PRECEDING AND CURRENT ROW) FROM t"
        ),
        Err("query has ? in window function frames is un-cacheable".to_owned())
    );
}

#[test]
fn subquery_admission_reads_the_session_switch() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("SET tidb_enable_plan_cache_for_subquery = OFF")
        .unwrap();
    assert_eq!(
        verdict(
            &mut session,
            "SELECT * FROM t WHERE a IN (SELECT a FROM t WHERE a > ?)"
        ),
        Err("query has sub-queries is un-cacheable".to_owned())
    );
    session
        .run("SET tidb_enable_plan_cache_for_subquery = ON")
        .unwrap();
    assert!(verdict(
        &mut session,
        "SELECT * FROM t WHERE a IN (SELECT a FROM t WHERE a > ?)"
    )
    .is_ok());
}

#[test]
fn cte_names_are_not_resolved_as_physical_tables() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    assert!(verdict(
        &mut session,
        "WITH c AS (SELECT a FROM t) SELECT * FROM c WHERE a > ?"
    )
    .is_ok());
}

#[test]
fn temporary_tables_are_not_cacheable() {
    let mut session = Session::new();
    session
        .run("CREATE GLOBAL TEMPORARY TABLE tmp (a int) ON COMMIT DELETE ROWS")
        .unwrap();
    assert_eq!(
        verdict(&mut session, "SELECT * FROM tmp WHERE a > ?"),
        Err("query accesses temporary tables is un-cacheable".to_owned())
    );
}

#[test]
fn fix_44823_controls_the_accumulated_in_list_limit() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session.run("SET tidb_opt_fix_control = '44823:2'").unwrap();
    assert_eq!(
        verdict(&mut session, "SELECT * FROM t WHERE a IN (?, ?, ?)"),
        Err("too many values in in-list".to_owned())
    );
    session.run("SET tidb_opt_fix_control = '44823:0'").unwrap();
    assert!(verdict(&mut session, "SELECT * FROM t WHERE a IN (?, ?, ?)").is_ok());
}

#[test]
fn fix_49736_forces_ast_cacheability() {
    let mut session = Session::new();
    assert_eq!(
        verdict(&mut session, "SELECT @v + ?"),
        Err("query has user-defined variables is un-cacheable".to_owned())
    );
    session
        .run("SET tidb_opt_fix_control = '49736:ON'")
        .unwrap();
    assert!(verdict(&mut session, "SELECT @v + ?").is_ok());
}

#[test]
fn convert_using_is_the_same_uncacheable_go_function() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a varchar(20))").unwrap();
    assert_eq!(
        verdict(
            &mut session,
            "SELECT * FROM t WHERE CONVERT(a USING utf8mb4) = ?"
        ),
        Err("query has 'convert' is un-cacheable".to_owned())
    );
}
