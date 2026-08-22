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

//! The `mysql` schema as an OBJECT: what selecting it does, and what naming
//! its tables does.
//!
//! Mirrors the schemas Go's `pkg/session/bootstrap.go` creates, as seen
//! through `tidb_executor::Catalog::default`. Every assertion below was
//! CAPTURED from a real TiDB over `rust/difftests/gorun` before it was
//! written -- the case-preserving `DATABASE()`, the failed `USE` leaving the
//! session where it was, and the 61-table `SHOW TABLES` -- because two of
//! them are the opposite of what the obvious guess would be.

use crate::tests_support::*;
use crate::*;

/// The gap this module exists for: `USE mysql` must SUCCEED.
///
/// Captured from Go:
///
/// ```text
/// select database();  -> test
/// use mysql;          -> OK
/// select database();  -> mysql
/// ```
///
/// It is one statement, but a `USE` that fails is not one wrong answer. The
/// session stays on the previous schema, so every later unqualified name
/// resolves there -- the statement is accepted-then-discarded and the
/// statements behind it silently answer against the wrong database. The
/// classified `executor/admin` divergence was exactly that: `admin check
/// table t` after a refused `use mysql` checked the `t` of the PREVIOUS
/// schema and reported success where TiDB reports 1146.
#[test]
fn use_mysql_selects_the_system_schema() {
    let mut session = Session::new();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "test"
    );

    session.run("USE mysql").unwrap();
    assert_eq!(session.current_database(), "mysql");
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "mysql"
    );
}

/// Schema names match case-insensitively, and `DATABASE()` reports the name
/// AS WRITTEN, not the catalog's stored spelling.
///
/// Captured from Go:
///
/// ```text
/// use MySQL;          -> OK
/// select database();  -> MySQL
/// ```
///
/// The written case is the surprising half. Go's `USE` resolves against the
/// lower form and then stores what the user typed in `SessionVars.CurrentDB`,
/// so `DATABASE()` echoes `MySQL` rather than normalising to `mysql`.
#[test]
fn use_matches_the_schema_name_case_insensitively_and_keeps_what_was_written() {
    let mut session = Session::new();
    session.run("USE MySQL").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MySQL"
    );

    session.run("USE MYSQL").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MYSQL"
    );
}

/// A `USE` of a name that really does not exist fails AND leaves the session
/// on the schema it was already using.
///
/// Captured from Go:
///
/// ```text
/// use MySQL;          -> OK
/// select database();  -> MySQL
/// use nosuchdb;       -> ERR   (1049 Unknown database 'nosuchdb')
/// select database();  -> MySQL
/// ```
///
/// This pins the half of the `USE` contract that is NOT a bug. It is tempting
/// to read "a failed `USE` leaves the session pointed at the old schema" as
/// the defect, but Go does exactly that; the defect was only ever that
/// `mysql` was not a name that existed. Anything that "fixed" the retention
/// would diverge from TiDB.
#[test]
fn a_failed_use_leaves_the_session_on_its_previous_schema() {
    let mut session = Session::new();
    session.run("USE MySQL").unwrap();

    let error = session.run("USE nosuchdb").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1049);
    assert_eq!(error.message, "Unknown database 'nosuchdb'");

    assert_eq!(
        scalar_text(&mut session, "SELECT DATABASE()").unwrap(),
        "MySQL"
    );
}

/// The whole point of the object being EMPTY: naming a bootstrap table
/// REFUSES. Every one of them is absent, and no absent table can answer
/// emptily -- which is what serving a fabricated zero-row `mysql.user` would
/// have done to every privilege query in the corpus.
///
/// Go serves all of these for real (captured: `select count(*) from
/// mysql.user` answers 1, `mysql.tidb` answers 6), so this is a refusal, not
/// parity.
///
/// The errno is where it gets interesting, and the two arms below disagree
/// on purpose:
///
/// * `ADMIN CHECK TABLE` refuses with Go's own **1146**
///   `Table 'mysql.user' doesn't exist`, because `admin_check_arm` resolves
///   through `SchemaErrorKind::UnknownTable`. That is the arm the classified
///   `executor/admin` divergence ran through, which is why selecting the
///   schema is enough to close it.
/// * `SELECT` refuses with **1146** too, as Go does. It answered a generic
///   1105 until the planner's table lookup
///   (`tidb_executor::driver::from` and the DML paths beside it) was moved
///   onto `SchemaErrorKind::UnknownTable`; that divergence was pinned here
///   rather than approved, and is now closed.
///
/// FLIPS TO SUPPORT when the `mysql.*` bootstrap tables are ported into this
/// tier: each name below then has to return rows, and this test is the list
/// of what to convert.
#[test]
fn the_bootstrap_tables_are_refused_by_name() {
    let mut session = Session::new();
    session.run("USE mysql").unwrap();

    // A sample across the families Go's `show tables` in `mysql` lists:
    // accounts, privileges, TiDB's own metadata, and statistics.
    for table in [
        "user",
        "db",
        "tables_priv",
        "global_priv",
        "tidb",
        "stats_meta",
    ] {
        let error = session
            .run(&format!("ADMIN CHECK TABLE {table}"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(
            error.code, 1146,
            "unqualified `{table}` in the mysql schema should be 1146"
        );
        assert_eq!(
            error.message,
            format!("Table 'mysql.{table}' doesn't exist")
        );

        // Both spellings of the name reach the same lookup, and both now
        // report Go's own ErrTableNotExists.
        for sql in [
            format!("SELECT * FROM {table}"),
            format!("SELECT * FROM mysql.{table}"),
        ] {
            let error = session.run(&sql).unwrap_err().to_mysql_error();
            assert_eq!(error.code, 1146, "`{sql}` should refuse");
            assert_eq!(
                error.message,
                format!("Table 'mysql.{table}' doesn't exist")
            );
        }
    }
}

/// `mysql` is listed among the schemas, since it now is one.
///
/// Captured from Go, `select schema_name from information_schema.schemata`:
/// `INFORMATION_SCHEMA;METRICS_SCHEMA;PERFORMANCE_SCHEMA;mysql;sys;test`.
/// This tier lists the three of those six it has -- `METRICS_SCHEMA`,
/// `PERFORMANCE_SCHEMA` and `sys` are absent, a documented divergence on
/// `Catalog::default` -- with `INFORMATION_SCHEMA` first, which is the
/// ordering Go's `fetchShowDatabases` imposes.
#[test]
fn the_system_schema_is_listed_among_the_databases() {
    let mut session = Session::new();
    let names: Vec<String> = row_text(session.run("SHOW DATABASES"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(names, vec!["INFORMATION_SCHEMA", "mysql", "test"]);

    let names: Vec<String> = row_text(
        session.run("SELECT SCHEMA_NAME FROM information_schema.schemata ORDER BY SCHEMA_NAME"),
    )
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    assert_eq!(names, vec!["INFORMATION_SCHEMA", "mysql", "test"]);
}

/// DIVERGENCE, pinned: enumerating `mysql` under-reports.
///
/// Captured from Go, `use mysql; show tables;` returns 61 names --
/// `advisory_locks` through `user`. This tier returns the THREE it stores,
/// bootstrapped by `crate::bootstrap`. Under-reporting an enumeration is the
/// price of refusing every absent name in it (see
/// [`the_bootstrap_tables_are_refused_by_name_with_1146`]); the alternative,
/// fabricating empty tables so the count looks right, would turn a loud 1146
/// into a silent zero-row answer.
///
/// FLIPS TO SUPPORT as the bootstrap tables land: this count rises toward 61.
/// It has risen twice so far -- `bind_info` for GLOBAL bindings, then the two
/// blacklist tables `ADMIN RELOAD` reads (`crate::blacklist`) -- and each
/// arrival is a feature that needed the table, not a name added to make the
/// count look better.
#[test]
fn enumerating_the_system_schema_under_reports() {
    let mut session = Session::new();
    session.run("USE mysql").unwrap();
    let stored = [
        ["bind_info"],
        ["expr_pushdown_blacklist"],
        ["opt_rule_blacklist"],
    ];
    assert_eq!(row_text(session.run("SHOW TABLES")), stored);

    assert_eq!(
        row_text(
            session.run(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'mysql'"
            )
        ),
        stored
    );
}

/// DIVERGENCE, pinned: `DROP DATABASE mysql` is accepted here.
///
/// Captured from Go:
///
/// ```text
/// drop database mysql;  -> [ddl:8267]Drop 'mysql' database is forbidden
/// ```
///
/// The refusal belongs in the `DropDatabase` statement arm that calls
/// `Catalog::drop_database`, in `tidb_session::dispatch`, which a parallel
/// unit owns; `drop_database` returns a bare `bool` and cannot carry 8267 on
/// its own. `DROP DATABASE information_schema` has the same hole today, so
/// this widens a pre-existing gap by one name rather than opening a class.
/// Nothing in the integration corpus drops either schema, so the gap is
/// unmeasured as well as unfixed.
///
/// FLIPS TO SUPPORT when the guard lands: assert `8267` and
/// `Drop 'mysql' database is forbidden` instead of `Ok`.
#[test]
fn dropping_the_mysql_schema_is_not_refused_yet() {
    let mut session = Session::new();
    assert!(session.run("DROP DATABASE mysql").is_ok());
    // And the object really is gone, which is what makes it a divergence
    // rather than a cosmetic one: the `USE` this unit fixed fails again.
    let error = session.run("USE mysql").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1049);
}
