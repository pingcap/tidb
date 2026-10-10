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

use crate::tests_support::{row_text, session_as};
use crate::{privilege, Catalog, Session, SharedCatalog};

fn setup() -> (privilege::PrivilegeRegistry, Session) {
    let registry = privilege::PrivilegeRegistry::default();
    let catalog: SharedCatalog = std::sync::Arc::new(std::sync::Mutex::new(Catalog::default()));
    let root = session_as(&registry, catalog, "root", "%");
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
