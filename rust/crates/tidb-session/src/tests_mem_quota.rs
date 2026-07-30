//! `tidb_mem_quota_query` end to end: a statement that outgrows its quota
//! reaches the client as the error Go sends, and one that fits does not.
//!
//! Every expectation here is a Go capture from `pkg/executor/test/memtest`
//! against a `testkit` session on a mock store:
//!
//! ```text
//! select @@tidb_mem_quota_query                 -> 1073741824
//! select @@global.tidb_mem_quota_query          -> 1073741824
//! select @@tidb_server_memory_limit             -> 80%
//! set @@global.tidb_mem_oom_action='CANCEL';
//! set @@tidb_mem_quota_query=1;
//! select a from t order by b                    -> ERRNO 8175
//!   [executor:8175]Your query has been cancelled due to exceeding the allowed
//!   memory limit for a single SQL query. Please try narrowing your query scope
//!   or increase the tidb_mem_quota_query limit and try again.[conn=1]
//! set @@global.tidb_mem_oom_action='LOG';
//! select ... (same statement, same quota)       -> OK, and SHOW WARNINGS empty
//! ```
//!
//! Note `tidb_mem_oom_action` reads `LOG` inside Go's own test binaries only
//! because `GlobalSystemVariableInitialValue` rewrites the initial value under
//! `intest.InTest`; `vardef.DefTiDBMemOOMAction` -- what a shipped server
//! bootstraps -- is `CANCEL`, which is the default honoured here.
#![cfg(test)]

use crate::*;

fn ordered_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    for batch in 0..16 {
        let values: Vec<String> = (0..64)
            .map(|i| {
                let v = batch * 64 + i;
                format!("({v},{})", 1024 - v)
            })
            .collect();
        session
            .run(&format!("INSERT INTO t VALUES {}", values.join(",")))
            .unwrap();
    }
    session
}

#[test]
fn the_shipped_quota_and_action_defaults_match_go() {
    let mut session = Session::new();
    assert_eq!(
        crate::tests_support::scalar_text(&mut session, "SELECT @@tidb_mem_quota_query").as_deref(),
        Some("1073741824")
    );
    assert_eq!(
        crate::tests_support::scalar_text(&mut session, "SELECT @@tidb_mem_oom_action").as_deref(),
        Some("CANCEL")
    );
}

#[test]
fn an_ordinary_statement_is_nowhere_near_the_default_quota() {
    let mut session = ordered_session();
    let rows = crate::tests_support::row_text(session.run("SELECT a FROM t ORDER BY b"));
    assert_eq!(rows.len(), 1024);
    // b descends as a ascends, so the smallest b is the largest a.
    assert_eq!(rows[0][0], "1023");
}

/// Regression: `SELECT @@name` used to answer the registry default forever for
/// a variable with GLOBAL scope only, so `SET GLOBAL` was invisible to every
/// reader. Go's `GetSessionOrGlobalSystemVar` falls through to the global table
/// when `HasSessionScope()` is false (captured: after
/// `set @@global.tidb_mem_oom_action='LOG'`, `select @@tidb_mem_oom_action`
/// reports `LOG`).
#[test]
fn set_global_is_visible_through_an_unprefixed_read_of_a_global_only_var() {
    let mut session = Session::new();
    assert_eq!(
        crate::tests_support::scalar_text(&mut session, "SELECT @@tidb_mem_oom_action").as_deref(),
        Some("CANCEL")
    );
    session
        .run("SET @@global.tidb_mem_oom_action = 'LOG'")
        .unwrap();
    assert_eq!(
        crate::tests_support::scalar_text(&mut session, "SELECT @@tidb_mem_oom_action").as_deref(),
        Some("LOG")
    );
}

#[test]
fn a_sort_past_the_quota_reaches_the_client_as_go_s_8175() {
    let mut session = ordered_session();
    session.run("SET @@tidb_mem_quota_query = 1").unwrap();
    let error = session
        .run("SELECT a FROM t ORDER BY b")
        .expect_err("the quota must be enforced");
    let wire = error.to_mysql_error();
    assert_eq!(wire.code, 8175);
    assert_eq!(&wire.state, b"HY000");
    assert_eq!(
        wire.message,
        "Your query has been cancelled due to exceeding the allowed memory limit for a single \
         SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query \
         limit and try again.[conn=0]"
    );
}

#[test]
fn the_same_statement_succeeds_with_oom_action_log() {
    let mut session = ordered_session();
    session.run("SET @@tidb_mem_quota_query = 1").unwrap();
    session
        .run("SET @@global.tidb_mem_oom_action = 'LOG'")
        .unwrap();
    assert_eq!(
        crate::tests_support::scalar_text(&mut session, "SELECT @@tidb_mem_oom_action").as_deref(),
        Some("LOG"),
        "the global SET must be visible to the statement that follows it"
    );
    // Captured: under LOG the statement RUNS, with no error and no warning.
    let rows = crate::tests_support::row_text(session.run("SELECT a FROM t ORDER BY b"));
    assert_eq!(rows.len(), 1024);
    let warnings = crate::tests_support::row_text(session.run("SHOW WARNINGS"));
    assert!(warnings.is_empty(), "captured: SHOW WARNINGS is empty");
}

#[test]
fn a_quota_of_minus_one_is_unlimited_as_the_sysvar_range_allows() {
    let mut session = ordered_session();
    session.run("SET @@tidb_mem_quota_query = -1").unwrap();
    let rows = crate::tests_support::row_text(session.run("SELECT a FROM t ORDER BY b"));
    assert_eq!(rows.len(), 1024);
}
