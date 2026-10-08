// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations.

//! Go vardef/sysvar and ddl/foreign_key policy in an isolated process, so OFF
//! cannot race the other suites' ordinary ON fixtures.
use std::sync::atomic::Ordering;
use tidb_executor::TableEntry;
use tidb_session::{vars::GlobalSysvars, Session};

fn sql(session: &mut Session, statement: &str) {
    session
        .run(statement)
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
}

fn errno(session: &mut Session, statement: &str, code: u16) {
    assert_eq!(
        session
            .run(statement)
            .expect_err(statement)
            .to_mysql_error()
            .code,
        code,
        "{statement}"
    );
}

#[test]
fn foreign_key_global_policy_batch() {
    let globals = GlobalSysvars::new();
    let peer = GlobalSysvars::new();
    let name = "tidb_enable_foreign_key";
    let set = |value: &str| {
        globals.set(name, value.to_owned()).unwrap();
    };
    set("OFF");
    assert!(!tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst));
    assert_eq!(peer.get(name).unwrap(), "OFF", "getter reads live owner");
    let scratch = GlobalSysvars::from_cluster_rows(vec![(name.to_owned(), "ON".to_owned())]);
    assert!(
        !tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst),
        "staging is private"
    );
    globals.replace_from(&scratch);
    assert!(tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst));
    globals.load_from_cluster(vec![(name.to_owned(), "OFF".to_owned())]);
    assert!(!tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst));
    globals
        .set("max_allowed_packet", "67108864".to_owned())
        .unwrap();
    assert!(!tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst));
    globals.reset(name).unwrap();
    assert!(tidb_vardef::ENABLE_FOREIGN_KEY.load(Ordering::SeqCst));

    let mut session = Session::new();
    // Go TestAddForeignKey3's PUBLIC behavior belongs at the session boundary:
    // failed child writes roll back before a later parent cascade executes.
    sql(&mut session, "CREATE TABLE cascade_parent(id INT PRIMARY KEY)");
    sql(&mut session, "CREATE TABLE cascade_child(id INT)");
    sql(&mut session, "INSERT INTO cascade_parent VALUES (1),(2),(3)");
    sql(&mut session, "INSERT INTO cascade_child VALUES (1),(2),(3)");
    sql(&mut session, "ALTER TABLE cascade_child ADD FOREIGN KEY(id) REFERENCES cascade_parent(id) ON DELETE CASCADE");
    errno(&mut session, "INSERT INTO cascade_child VALUES (10)", 1452);
    sql(&mut session, "DELETE FROM cascade_parent WHERE id=1");
    let tidb_session::StmtResult::Rows(rows) = session.run("SELECT id FROM cascade_child ORDER BY id").unwrap() else { panic!("child rows") };
    assert_eq!(rows.iter().map(|row| String::from_utf8(row[0].to_bytes().unwrap()).unwrap()).collect::<Vec<_>>(), ["2", "3"]);

    sql(
        &mut session,
        "CREATE TABLE parent(id INT PRIMARY KEY, k INT, KEY ix(k))",
    );
    sql(
        &mut session,
        "CREATE TABLE child(id INT, FOREIGN KEY fk(id) REFERENCES parent(k))",
    );
    set("OFF");
    errno(&mut session, "INSERT INTO child VALUES (7)", 1452);
    sql(
        &mut session,
        "CREATE TABLE legacy(id BIGINT, FOREIGN KEY fk(id) REFERENCES parent(k))",
    );
    sql(
        &mut session,
        "CREATE TABLE orphan(id INT, FOREIGN KEY fk(id) REFERENCES absent(id))",
    );
    errno(
        &mut session,
        "CREATE TABLE bad(id INT, FOREIGN KEY fk(missing) REFERENCES absent(id))",
        1072,
    );
    {
        let shared = session.shared_catalog();
        let catalog = shared.lock().unwrap();
        let Some(TableEntry::Kv(table)) = catalog.table_in("test", "legacy") else {
            panic!("legacy table")
        };
        assert_eq!(table.foreign_keys()[0].version, 0);
        assert!(
            table.indexes().is_empty(),
            "CREATE does not index version-zero keys"
        );
    }
    sql(&mut session, "CREATE TABLE altered(id INT)");
    sql(
        &mut session,
        "ALTER TABLE altered ADD CONSTRAINT fk FOREIGN KEY(id) REFERENCES parent(k)",
    );
    {
        let shared = session.shared_catalog();
        let catalog = shared.lock().unwrap();
        let Some(TableEntry::Kv(table)) = catalog.table_in("test", "altered") else {
            panic!("altered")
        };
        assert_eq!(table.foreign_keys()[0].version, 0);
        assert_eq!(
            table.indexes().len(),
            1,
            "ALTER creates its index even for version zero"
        );
    }
    sql(&mut session, "CREATE TABLE altered_rows(id INT)");
    sql(&mut session, "INSERT INTO altered_rows VALUES (99)");
    errno(
        &mut session,
        "ALTER TABLE altered_rows ADD CONSTRAINT fk FOREIGN KEY(id) REFERENCES parent(k)",
        1452,
    );
    // Persistent entrypoints consume the same validators, including checks
    // that cannot be bypassed by the process-wide feature switch.
    for (statement, code) in [
        ("TRUNCATE TABLE parent", 1701),
        ("ALTER TABLE parent DROP INDEX ix", 1553),
        ("DROP TABLE parent", 3730),
    ] {
        let parsed = tidb_parser::parse(statement).unwrap();
        let lowered = tidb_exec::cluster_ddl::lower_ddl(&parsed, "test")
            .unwrap()
            .unwrap();
        assert_eq!(
            session
                .validate_persistent_ddl_references(&lowered)
                .unwrap_err()
                .to_mysql_error()
                .code,
            code,
            "{statement}"
        );
    }
    sql(&mut session, "ALTER TABLE parent RENAME COLUMN k TO k2");
    {
        let shared = session.shared_catalog();
        let catalog = shared.lock().unwrap();
        let Some(TableEntry::Kv(table)) = catalog.table_in("test", "child") else {
            panic!("child")
        };
        assert_eq!(table.foreign_keys()[0].ref_cols, ["k"]);
    }
    sql(&mut session, "ALTER TABLE parent RENAME COLUMN k2 TO k");
    let tidb_session::StmtResult::Rows(rows) = session.run("SHOW CREATE TABLE legacy").unwrap() else { panic!("show create") };
    let definition = String::from_utf8(rows[0][1].to_bytes().unwrap()).unwrap();
    assert!(definition.contains("/* FOREIGN KEY INVALID */"));
    sql(&mut session, "RENAME TABLE parent TO renamed");
    {
        let shared = session.shared_catalog();
        let catalog = shared.lock().unwrap();
        let Some(TableEntry::Kv(table)) = catalog.table_in("test", "child") else {
            panic!("child table")
        };
        assert_eq!(table.foreign_keys()[0].ref_table, "parent");
    }
    sql(&mut session, "RENAME TABLE renamed TO parent");
    errno(&mut session, "TRUNCATE TABLE parent", 1701);
    errno(&mut session, "ALTER TABLE parent DROP INDEX ix", 1553);
    errno(&mut session, "DROP TABLE parent", 3730);
    sql(&mut session, "SET foreign_key_checks=OFF");
    sql(&mut session, "DROP TABLE parent");
    sql(&mut session, "SET foreign_key_checks=ON");
    set("ON");
    sql(&mut session, "INSERT INTO legacy VALUES (9)");
    sql(&mut session, "INSERT INTO altered VALUES (9)");
    errno(
        &mut session,
        "CREATE TABLE active(id INT, FOREIGN KEY fk(id) REFERENCES absent(id))",
        1824,
    );
    // The active child still prevents an incompatible parent being published.
    errno(&mut session, "CREATE TABLE parent(k VARCHAR(20))", 3780);
    sql(&mut session, "DROP TABLE child");
    sql(&mut session, "CREATE TABLE parent(k VARCHAR(20))");
    set("OFF");
    sql(&mut session, "CREATE DATABASE other");
    sql(
        &mut session,
        "CREATE TABLE other.referrer(id INT, FOREIGN KEY fk(id) REFERENCES parent(k))",
    );
    sql(&mut session, "DROP DATABASE test");

    errno(&mut session, "CREATE TABLE other.bad_null(id INT NOT NULL, FOREIGN KEY fk(id) REFERENCES other.absent(id) ON DELETE SET NULL)", 1830);

    use tidb_exec::cluster_ddl::{lower_ddl, DdlStatement};
    use tidb_exec::foreign_key_build::{
        check_table_foreign_keys_valid, check_table_foreign_keys_valid_in_owner,
    };
    let parsed = tidb_parser::parse(
        "CREATE TABLE test.persisted(id INT, FOREIGN KEY fk(id) REFERENCES absent(id))",
    )
    .unwrap();
    let DdlStatement::CreateTable { build, .. } = lower_ddl(&parsed, "test").unwrap().unwrap()
    else {
        panic!("create")
    };
    let mut table = build.template().clone_like_go();
    assert_eq!(table.foreign_keys.get(0).unwrap().read().version, 0);
    assert!(table.indices.is_empty());
    let catalog = tidb_exec::cluster_catalog::ClusterCatalog {
        schema_version: 1,
        databases: vec![],
    };
    check_table_foreign_keys_valid(&catalog, "test", &table, true).unwrap();
    check_table_foreign_keys_valid_in_owner(&catalog, "test", &mut table, true).unwrap();
    assert_eq!(
        table.max_foreign_key_id, 1,
        "legacy metadata still gets an ID"
    );
    set("ON");
    check_table_foreign_keys_valid(&catalog, "test", &table, true).unwrap();
    assert_eq!(table.foreign_keys.get(0).unwrap().read().version, 0);
}
