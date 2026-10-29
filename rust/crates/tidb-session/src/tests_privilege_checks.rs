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

//! Privilege checks and SHOW GRANTS as `privilege/privileges.test` records
//! them on this branch's Go.

use crate::tests_support::{authenticated_session, row_text, session_as};
use crate::Session;

fn setup() -> (tidb_privilege::PrivilegeRegistry, Session) {
    let registry = tidb_privilege::PrivilegeRegistry::default();
    let root = authenticated_session(&registry, "root", "%");
    (registry, root)
}

fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"));
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

/// Go `showGrants` prints the account and roles raw between single quotes
/// (`'%s'@'%s'`), escaping only schema and table names; a bare SHOW GRANTS
/// is titled "Grants for User".
#[test]
fn show_grants_quotes_accounts_as_go_does() {
    let (registry, mut root) = setup();
    root.run("create user u1, r1").unwrap();
    root.run("grant select on test.* to u1").unwrap();
    root.run("grant r1 to u1").unwrap();
    let (columns, rows) = crate::tests_support::query_text(&mut root, "show grants for u1");
    assert_eq!(columns, vec!["Grants for u1@%"]);
    assert_eq!(
        rows,
        vec![
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'"],
            vec!["GRANT SELECT ON `test`.* TO 'u1'@'%'"],
            vec!["GRANT 'r1'@'%' TO 'u1'@'%'"],
        ]
    );
    let mut u1 = session_as(&registry, root.shared_catalog(), "u1", "%");
    let (columns, _) = crate::tests_support::query_text(&mut u1, "show grants");
    assert_eq!(columns, vec!["Grants for User"]);
    root.run("create user r2").unwrap();
    let mut u1 = session_as(&registry, root.shared_catalog(), "u1", "%");
    assert_eq!(
        error_of(&mut u1, "show grants for current_user() using r2"),
        (3530, "`r2`@`%` is not granted to u1@%".to_owned())
    );
}

/// Go `buildAdmin` needs SUPER for every ADMIN statement past its early
/// returns, refused as `ErrPrivilegeCheckFail`; FLUSH PLAN CACHE is one of
/// those early returns.
#[test]
fn admin_statements_need_super() {
    let (registry, mut root) = setup();
    root.run("create user plain").unwrap();
    root.run("create table test.admin_t(a int, key idx_a(a))")
        .unwrap();
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    for sql in [
        "admin check table test.admin_t",
        "admin checksum table test.admin_t",
        "admin show ddl jobs",
        "admin cancel ddl jobs 10",
        "admin show slow recent 3",
        "admin set bdr role primary",
    ] {
        assert_eq!(
            error_of(&mut plain, sql),
            (8121, "privilege check for 'Super' fail".to_owned()),
            "{sql}"
        );
    }
    plain.run("admin flush session plan_cache").unwrap();
}

/// Go's `ErrSpecificAccessDenied` (1227) visits: PLACEMENT_ADMIN for a
/// placement policy, FILE for SELECT ... INTO OUTFILE, PROCESS for
/// CLUSTER_INFO.
#[test]
fn specific_privileges_are_refused_by_name() {
    let (registry, mut root) = setup();
    root.run("create user plain").unwrap();
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    for (sql, needed) in [
        (
            "create placement policy x primary_region='cn-east-1' regions='cn-east-1'",
            "SUPER or PLACEMENT_ADMIN",
        ),
        (
            "drop placement policy if exists x",
            "SUPER or PLACEMENT_ADMIN",
        ),
        (
            "select 1 into outfile '/tmp/doesntmatter-no-permissions'",
            "FILE",
        ),
        ("select * from information_schema.cluster_info", "PROCESS"),
    ] {
        assert_eq!(
            error_of(&mut plain, sql),
            (
                1227,
                format!(
                    "Access denied; you need (at least one of) the {needed} privilege(s) for this operation"
                )
            ),
            "{sql}"
        );
    }
    root.run("grant placement_admin on *.* to plain").unwrap();
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    plain
        .run("create placement policy x primary_region='cn-east-1' regions='cn-east-1'")
        .unwrap();
}

/// Go `buildSelect`: a locking read also needs DELETE, UPDATE or LOCK
/// TABLES on each table it locks.
#[test]
fn a_locking_read_needs_a_write_or_lock_privilege() {
    let (registry, mut root) = setup();
    root.run("create user foo").unwrap();
    root.run("create table test.t (x int)").unwrap();
    root.run("create table test.t1 (x int)").unwrap();
    root.run("grant select on test.* to foo").unwrap();
    let mut foo = session_as(&registry, root.shared_catalog(), "foo", "%");
    foo.run("use test").unwrap();
    assert_eq!(
        error_of(&mut foo, "select * from t for update"),
        (
            1142,
            "SELECT with locking clause command denied to user 'foo'@'%' for table 't'".to_owned()
        )
    );
    root.run("grant update on test.t to foo").unwrap();
    let mut foo = session_as(&registry, root.shared_catalog(), "foo", "%");
    foo.run("use test").unwrap();
    assert_eq!(
        row_text(foo.run("select * from t for update")),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        error_of(&mut foo, "select * from t, t1 where t.x = t1.x for update"),
        (
            1142,
            "SELECT with locking clause command denied to user 'foo'@'%' for table 't1'".to_owned()
        )
    );
}

/// `executor/explain.test`: under EXPLAIN Go's `BuildDataSourceFromView`
/// wants SHOW VIEW on every view it expands and SELECT on a view nested in
/// another, refusing either with `ErrViewNoExplain` (1345); the statement
/// itself still runs on SELECT alone.
#[test]
fn explain_over_a_view_needs_show_view_and_select_on_nested_views() {
    let (registry, mut root) = setup();
    root.run("create database ev").unwrap();
    root.run("create table ev.t (id int)").unwrap();
    root.run("create view ev.v as select * from ev.t").unwrap();
    root.run("create view ev.v1 as select * from ev.t").unwrap();
    root.run("create view ev.v2 as select * from ev.v1")
        .unwrap();
    root.run("create user explainer").unwrap();
    root.run("grant select on ev.v to explainer").unwrap();
    root.run("grant select, show view on ev.v2 to explainer")
        .unwrap();
    root.run("grant show view on ev.v1 to explainer").unwrap();
    let mut user = session_as(&registry, root.shared_catalog(), "explainer", "%");
    user.run("select * from ev.v").unwrap();
    let denied = (
        1345,
        "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table".to_owned(),
    );
    assert_eq!(
        error_of(&mut user, "explain format='plan_tree' select * from ev.v"),
        denied
    );
    assert_eq!(
        error_of(&mut user, "explain format='plan_tree' select * from ev.v2"),
        denied
    );
    root.run("grant select on ev.v1 to explainer").unwrap();
    let mut user = session_as(&registry, root.shared_catalog(), "explainer", "%");
    user.run("explain format='plan_tree' select * from ev.v2")
        .unwrap();
}

/// Go's `StmtCtx.ViewDepth` counts every view a statement expands, nested or
/// not, so under EXPLAIN the second view of a join needs SELECT of the reader
/// too, and `BuildDataSourceFromView` refuses it with `ErrViewNoExplain` while
/// it plans -- before `CheckPrivilege` could report the statement's own
/// SELECT visit on that view (1142).
#[test]
fn explain_needs_select_on_every_view_after_the_first() {
    let (registry, mut root) = setup();
    root.run("create database ev").unwrap();
    root.run("create table ev.t (id int)").unwrap();
    root.run("create view ev.v as select * from ev.t").unwrap();
    root.run("create view ev.v1 as select * from ev.t").unwrap();
    root.run("create user explainer").unwrap();
    root.run("grant select, show view on ev.v to explainer").unwrap();
    root.run("grant show view on ev.v1 to explainer").unwrap();
    let mut user = session_as(&registry, root.shared_catalog(), "explainer", "%");
    assert_eq!(
        error_of(
            &mut user,
            "explain format='plan_tree' select * from ev.v join ev.v1"
        ),
        (
            1345,
            "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table".to_owned(),
        )
    );
}

/// `privilege/privileges.test`: Go's `fetchShowColumns` wants a column
/// privilege on the table (refusing as a denied SELECT), SHOW CREATE TABLE
/// any privilege but CREATE TEMPORARY TABLES (refusing as a denied SHOW),
/// and `information_schema.COLUMNS.PRIVILEGES` lists only the column
/// privileges the user holds.
#[test]
fn show_columns_show_create_and_column_privileges_follow_the_grants() {
    let (registry, mut root) = setup();
    root.run("create database sp").unwrap();
    root.run("create table sp.t1 (a int)").unwrap();
    root.run("create view sp.v as select 1").unwrap();
    root.run("create user nobody, viewer").unwrap();
    root.run("grant show view on sp.v to viewer").unwrap();
    let mut nobody = session_as(&registry, root.shared_catalog(), "nobody", "%");
    assert_eq!(
        error_of(&mut nobody, "show create table sp.t1"),
        (
            1142,
            "SHOW command denied to user 'nobody'@'%' for table 't1'".to_owned()
        )
    );
    let mut viewer = session_as(&registry, root.shared_catalog(), "viewer", "%");
    assert_eq!(
        error_of(&mut viewer, "desc sp.v"),
        (
            1142,
            "SELECT command denied to user 'viewer'@'%' for table 'v'".to_owned()
        )
    );
    root.run("grant update, select on sp.v to viewer").unwrap();
    let mut viewer = session_as(&registry, root.shared_catalog(), "viewer", "%");
    viewer.run("desc sp.v").unwrap();
    assert_eq!(
        row_text(viewer.run(
            "select privileges from information_schema.columns where table_schema='sp' and table_name='v'"
        )),
        [["select,update"]]
    );
}

/// Go `requireInsertAndSelectPriv` names the table as written
/// (`tbl.Name.O`), and `RequestVerification` refuses INSERT on a memory
/// database before any grant is read -- even `ALL ON *.*` (captured from Go
/// on this branch).
#[test]
fn stats_lock_names_the_table_as_written_and_refuses_memory_databases() {
    let (registry, mut root) = setup();
    root.run("create user foo").unwrap();
    root.run("create table test.T1 (x int)").unwrap();
    root.run("grant select on test.* to foo").unwrap();
    let mut foo = session_as(&registry, root.shared_catalog(), "foo", "%");
    foo.run("use test").unwrap();
    assert_eq!(
        error_of(&mut foo, "lock stats T1"),
        (
            1142,
            "INSERT command denied to user 'foo'@'%' for table 'T1'".to_owned()
        )
    );
    root.run("grant all on *.* to foo").unwrap();
    let mut foo = session_as(&registry, root.shared_catalog(), "foo", "%");
    assert_eq!(
        error_of(&mut foo, "lock stats information_schema.TABLES"),
        (
            1142,
            "INSERT command denied to user 'foo'@'%' for table 'TABLES'".to_owned()
        )
    );
}

/// Go `executeFlush` for PRIVILEGES calls `NotifyUpdateAllUsersPrivilege`,
/// which reloads this node AND announces the change to every peer.
#[test]
fn flush_privileges_announces_the_reload_to_peers() {
    let mut session = Session::new();
    let announced = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&announced);
    session.set_privilege_change_notifier(std::sync::Arc::new(move || {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    session.run("FLUSH PRIVILEGES").unwrap();
    assert_eq!(announced.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// Go `buildSet` checks every GLOBAL assignment's privilege before any
/// assignment runs, removed variables included: a denied SET changes
/// nothing (captured from Go on this branch).
#[test]
fn set_checks_every_global_privilege_before_applying_any() {
    let (registry, mut root) = setup();
    root.run("create user nosuper").unwrap();
    let mut user = session_as(&registry, root.shared_catalog(), "nosuper", "%");
    let denied = (
        1227,
        "Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation"
            .to_owned(),
    );
    assert_eq!(
        error_of(
            &mut user,
            "set sql_mode = 'ANSI', global max_connections = 10"
        ),
        denied
    );
    assert_eq!(
        row_text(user.run("select @@sql_mode")),
        [["ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"]]
    );
    assert_eq!(
        error_of(&mut user, "set global tidb_enable_alter_placement = 1"),
        denied
    );
}

/// Go `buildUpdateLists` / `buildDelete` append each write target's
/// visitInfo as `DBName.L` and `TableName.L`. An UPDATE's visitInfo carries
/// no error, so a denial is `ErrPrivilegeCheckFail` (8121); DELETE's carries
/// `ErrTableaccessDenied` naming `TableName.L`, `t` however the statement
/// spelled it.
#[test]
fn write_privilege_errors_name_the_lowered_table() {
    let (registry, mut root) = setup();
    root.run("create user foo").unwrap();
    root.run("create table test.t (x int)").unwrap();
    root.run("grant select on test.* to foo").unwrap();
    let mut foo = session_as(&registry, root.shared_catalog(), "foo", "%");
    foo.run("use test").unwrap();
    assert_eq!(
        error_of(&mut foo, "update T set x = 1"),
        (8121, "privilege check for 'Update' fail".to_owned())
    );
    assert_eq!(
        error_of(&mut foo, "delete from T"),
        (
            1142,
            "DELETE command denied to user 'foo'@'%' for table 't'".to_owned()
        )
    );
}

/// Go `IsDynamicPrivilege` and the grant writers compare
/// `strings.ToUpper(name)`: `backup_adm\u{131}n` (dotless `\u{131}` uppercases
/// to `I`) is BACKUP_ADMIN.
#[test]
fn dynamic_privilege_names_fold_with_go_to_upper() {
    let (_registry, mut root) = setup();
    root.run("create user foo").unwrap();
    root.run("grant backup_adm\u{131}n on *.* to foo").unwrap();
    assert_eq!(
        row_text(root.run("show grants for foo")),
        vec![
            vec!["GRANT USAGE ON *.* TO 'foo'@'%'".to_owned()],
            vec!["GRANT BACKUP_ADMIN ON *.* TO 'foo'@'%'".to_owned()]
        ]
    );
}

/// Go `grantDynamicPriv` uppercases the written name with `strings.ToUpper`
/// before its level check, so 3619 names `BACKUP_ADMIN` for `backup_adm\u{131}n`
/// (dotless `\u{131}` uppercases to `I`), alone or beside a static privilege.
/// Captured from a Go TiDB.
#[test]
fn a_misplaced_dynamic_privilege_is_named_in_go_upper_case() {
    let (_registry, mut root) = setup();
    root.run("create user foo").unwrap();
    for sql in [
        "grant backup_adm\u{131}n on test.* to foo",
        "grant backup_adm\u{131}n, select on test.* to foo",
        "revoke backup_adm\u{131}n on test.* from foo",
    ] {
        assert_eq!(
            error_of(&mut root, sql),
            (
                3619,
                "Illegal privilege level specified for BACKUP_ADMIN".to_owned()
            ),
            "{sql}"
        );
    }
}
