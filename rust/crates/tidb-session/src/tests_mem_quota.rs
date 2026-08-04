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

/// Go's own answer, measured on this branch with `testkit` over a 168 MB
/// table and `SELECT a FROM t ORDER BY b` (`tidb_mem_oom_action = CANCEL`):
///
/// | `tidb_mem_quota_query` | tmp storage ON | tmp storage OFF |
/// | --- | --- | --- |
/// | 2 MiB   | 400000 rows | 8175 |
/// | 8 MiB   | 400000 rows | 8175 |
/// | 16 MiB  | 400000 rows | 8175 |
/// | 64 MiB  | 400000 rows | 400000 rows |
///
/// So spilling IS what saves an over-quota `ORDER BY`, and with spilling off
/// 8175 is Go's answer at every quota the sort does not fit in. This test
/// pins the OFF column, which is the one this tier reproduces exactly.
///
/// MEASURED DIVERGENCE, at quotas far below one chunk: at
/// `tidb_mem_quota_query = 1` Go answers 8175 even with tmp storage ON,
/// because the READ path's own tracker is cancelled before the sort's spill
/// action can release anything. This tier accounts in the sort only (see
/// `tidb_executor::mem_quota`'s "WHICH OPERATORS ACCOUNT"), so with spilling
/// on it spills and returns rows there. Closing that needs read-path
/// accounting, not a spill change -- so the gate below is set explicitly
/// rather than the divergence being papered over.
#[test]
fn a_sort_past_the_quota_reaches_the_client_as_go_s_8175() {
    let mut session = ordered_session();
    session
        .run("SET @@global.tidb_enable_tmp_storage_on_oom = 0")
        .unwrap();
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

/// `tests/integrationtest/t/executor/executor.test`'s `TestOOMPanicAction`,
/// verbatim: the same `INSERT` is 8175 at `tidb_mem_quota_query = 200` and
/// succeeds at 10000. This is the sysvar reaching the WRITE path -- before
/// the write path accounted, both spellings simply inserted.
#[test]
fn the_suite_s_oom_insert_is_8175_at_200_and_inserts_at_10000() {
    let mut session = Session::new();
    session.run("CREATE TABLE t1 (a BIGINT)").unwrap();
    session.run("SET @@tidb_mem_quota_query = 200").unwrap();
    let error = session
        .run("INSERT INTO t1 VALUES (1),(2),(3),(4),(5)")
        .expect_err("the quota must be enforced on the write path");
    assert_eq!(error.to_mysql_error().code, 8175);
    assert!(
        crate::tests_support::row_text(session.run("SELECT a FROM t1")).is_empty(),
        "a cancelled INSERT stores nothing"
    );

    session.run("SET @@tidb_mem_quota_query = 10000").unwrap();
    session
        .run("INSERT INTO t1 VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT a FROM t1")).len(),
        5
    );
}

/// `executor/foreign_key.test`: `update t1 set id=id+100000 where id=1` under
/// `tidb_mem_quota_query = 81920` is 8175 in TiDB **with the cascade
/// unapplied** -- captured against a mock-store session: afterwards `select
/// id,pid from t1 where id=1` is still `1 NULL`, 255 rows still hold `pid=1`,
/// none holds `pid=100001`, and `sum(id)` is still 32896. Under the default
/// quota the same statement repoints all 255 (`sum(id)` 132896).
///
/// The suite builds its self-referencing constraint with `ALTER TABLE ... ADD
/// FOREIGN KEY`, which this tier still refuses, so the parent and child are
/// two tables here. Everything the quota decides is the same: 256 child rows
/// of the same width, one parent row updated, the same 81920.
#[test]
fn a_cascade_past_the_quota_repoints_nothing_and_finishes_under_the_default() {
    let mut session = Session::new();
    session.run("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    session
        .run(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, name VARCHAR(200), INDEX(pid), \
             FOREIGN KEY (pid) REFERENCES p (id) ON UPDATE CASCADE)",
        )
        .unwrap();
    session.run("INSERT INTO p VALUES (1)").unwrap();
    let values: Vec<String> = (1..=256)
        .map(|i| {
            format!("({i}, 1, 'abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz')")
        })
        .collect();
    session
        .run(&format!("INSERT INTO c VALUES {}", values.join(",")))
        .unwrap();

    session.run("SET @@tidb_mem_quota_query = 81920").unwrap();
    let error = session
        .run("UPDATE p SET id = id + 100000")
        .expect_err("the cascade must be stopped by the quota");
    assert_eq!(error.to_mysql_error().code, 8175);
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT id FROM p")),
        vec![vec!["1".to_owned()]],
        "the parent row is unchanged"
    );
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT count(*) FROM c WHERE pid = 1")),
        vec![vec!["256".to_owned()]],
        "and NOT ONE child row was repointed -- a half-applied cascade would be a \
         silent wrong answer wearing a correct errno"
    );

    session.run("SET @@tidb_mem_quota_query = DEFAULT").unwrap();
    session.run("UPDATE p SET id = id + 100000").unwrap();
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT count(*) FROM c WHERE pid = 100001")),
        vec![vec!["256".to_owned()]],
        "accept-control: with room the cascade repoints every child row"
    );
}
