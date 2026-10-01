// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Diagnostic observations, not assertions that these outcomes are correct.
//! Run as a temporary tidb-session example; see expanded-ownership-review.md.

use tidb_session::privilege::{GlobalPriv, PrivilegeRegistry};
use tidb_session::Session;

fn run(session: &mut Session, sql: &str) {
    println!("{sql}\n  {:?}", session.run(sql));
}

fn main() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE audit_left (id INT PRIMARY KEY, x INT)",
        "CREATE TABLE audit_right (id INT PRIMARY KEY, y INT)",
        "INSERT INTO audit_left VALUES (1,10)",
        "INSERT INTO audit_right VALUES (1,20)",
    ] {
        run(&mut session, sql);
    }
    let grants = PrivilegeRegistry::bootstrapped_from(Vec::new());
    grants.create_user("audit_reader", "%", "");
    grants.grant("audit_reader", "%", GlobalPriv::Select.mask());
    session.attach_privileges(grants.clone());
    session.set_user("audit_reader@%".into(), "audit_reader@localhost".into());
    for sql in [
        "UPDATE audit_left SET x=11 WHERE id=1",
        "UPDATE audit_left a JOIN audit_right b ON a.id=b.id SET a.x=12",
        "UPDATE audit_left a JOIN audit_right b ON a.id=b.id SET x=13",
        "SELECT * FROM audit_left",
    ] {
        run(&mut session, sql);
    }
    grants.create_user("audit_column", "%", "");
    grants.grant_column(
        "audit_column",
        "%",
        "test",
        "audit_left",
        "x",
        GlobalPriv::Select.mask(),
    );
    session.set_user("audit_column@%".into(), "audit_column@localhost".into());
    run(&mut session, "SELECT x FROM audit_left");
    run(&mut session, "SELECT id FROM audit_left");

    let mut root = Session::new();
    root.set_user("root@%".into(), "root@localhost".into());
    root.attach_privileges(PrivilegeRegistry::default());
    for sql in [
        "CREATE USER 'audit_history'@'%' IDENTIFIED BY 'First!1234' PASSWORD HISTORY 3",
        "ALTER USER 'audit_history'@'%' IDENTIFIED BY 'Second!1234'",
        "ALTER USER 'audit_history'@'%' IDENTIFIED BY 'First!1234'",
        "SELECT password_reuse_history FROM mysql.user WHERE user='audit_history'",
        "CREATE USER 'audit_grant'@'%'",
        "CREATE TABLE audit_grant_table (a INT, b INT)",
        "GRANT SELECT(a) ON test.audit_grant_table TO 'audit_grant'@'%'",
        "CREATE USER 'audit_cert'@'%' REQUIRE X509",
        "CREATE TABLE audit_import (a INT PRIMARY KEY, b INT)",
    ] {
        run(&mut root, sql);
    }
    let path = std::env::temp_dir().join(format!("tidb-parity-import-{}.csv", std::process::id()));
    std::fs::write(&path, "1,10\n2,20\n").expect("write diagnostic input");
    let sql = format!(
        "IMPORT INTO audit_import FROM '{}' WITH skip_rows=1",
        path.display()
    );
    // Keep the saved output deterministic while retaining the executed SQL shape.
    println!(
        "IMPORT INTO audit_import FROM '<temporary CSV>' WITH skip_rows=1\n  {:?}",
        root.run(&sql)
    );
    run(&mut root, "SELECT * FROM audit_import ORDER BY a");
    std::fs::remove_file(path).expect("remove diagnostic input");
}
