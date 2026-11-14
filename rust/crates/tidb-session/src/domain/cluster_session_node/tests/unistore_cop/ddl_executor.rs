// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go executor admission cases at the direct statement-method boundary.

use super::*;

/// Go `pkg/ddl/table_modify_test.go:TestLockTableReadOnly`.
#[test]
fn test_lock_table_read_only() {
    let previous = tidb_config::config_tree::config::get_global_config();
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = true);
    let (stack, _users) = cop_backed_stack();
    let mut first = stack.factory.open_session(session_context(2150)).unwrap();
    let mut second = stack.factory.open_session(session_context(2151)).unwrap();
    rows(&mut first, "use test");
    rows(&mut second, "use test");
    rows(&mut first, "create table t1 (a int key, b int)");
    rows(&mut first, "create table t2 (a int key)");
    rows(&mut first, "alter table t1 read only");
    rows(&mut first, "select * from t1");
    rows(&mut second, "select * from t1");
    let locked = |session: &mut Session, sql: &str| {
        let expected = tidb_infoschema::infoschema_errors::ERR_TABLE_LOCKED
            .to_sql_error()
            .code;
        let refusal = table_mode::refusal(session, sql).expect("table lock refusal");
        assert!(
            refusal.starts_with(&format!("{expected}|")),
            "{sql}: {refusal}"
        );
    };
    for session in [&mut first, &mut second] {
        for sql in [
            "insert into t1 set a=1,b=2",
            "update t1 set a=1",
            "delete from t1",
        ] {
            locked(session, sql);
        }
    }
    rows(&mut second, "alter table t1 read only");
    locked(&mut second, "insert into t1 set a=1,b=2");
    rows(&mut first, "alter table t1 read write");
    for lock in ["read", "write", "write local"] {
        rows(&mut first, &format!("lock tables t1 {lock}"));
        locked(&mut first, "alter table t1 read only");
        locked(&mut second, "alter table t1 read only");
    }
    rows(&mut first, "unlock tables");
    rows(&mut first, "alter table t1 read only");
    for lock in ["read", "write", "write local"] {
        for session in [&mut first, &mut second] {
            locked(session, &format!("lock tables t1 {lock}"));
        }
    }
    rows(&mut first, "admin cleanup table lock t1");
    rows(&mut second, "insert into t1 set a=1,b=2");
    rows(&mut first, "alter table t1 read write");
    rows(&mut first, "alter table t2 read write");
    // Go testkit probe TestAlterTableReadOnlyDisabled: no lock and no warning
    // when this experimental feature is disabled.
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = false);
    for sql in ["alter table t2 read only", "alter table t2 read write"] {
        rows(&mut first, sql);
        assert!(displayed(rows(&mut first, "show warnings")).is_empty());
        let (_, table) = stack.factory.catalog_snapshot().table_info_by_name("test", "t2").unwrap();
        assert!(table.read().lock.is_none());
    }
    rows(&mut first, "insert into t2 values(1)");
    tidb_config::config_tree::config::store_global_config((*previous).clone());
}

/// Go `pkg/ddl/multi_schema_change_test.go:TestMultiSchemaChangeBlockedByRowLevelChecksum`.
#[test]
fn test_multi_schema_change_blocked_by_row_level_checksum() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2152)).unwrap();
    rows(&mut session, "use test");
    rows(&mut session, "create table t(c int)");
    let previous = tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.load(Ordering::SeqCst);
    for (global, local) in [(true, false), (false, true)] {
        tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.store(global, Ordering::SeqCst);
        session.vars.enable_row_level_checksum = local;
        for sql in [
            "alter table t add column c1 int, add column c2 int",
            "alter table t add (c1 int, c2 int)",
        ] {
            assert_eq!(
                table_mode::refusal(&mut session, sql),
                Some(
                    "8200|Unsupported multi schema change when row level checksum is enabled"
                        .to_owned()
                )
            );
        }
    }
    tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.store(previous, Ordering::SeqCst);
}

/// Go `pkg/ddl/placement_sql_test.go:TestPlacementMode`'s three policy methods.
#[test]
fn test_placement_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2153)).unwrap();
    rows(&mut session, "create placement policy p1 followers=4");
    rows(&mut session, "set tidb_placement_mode='ignore'");
    for sql in [
        "create placement policy p1 primary_region='r1' regions='r1'",
        "create placement policy p3 primary_region='r1' regions='r1'",
        "alter placement policy p1 primary_region='r1' regions='r1'",
        "alter placement policy p3 primary_region='r1' regions='r1'",
        "drop placement policy p1",
        "drop placement policy p3",
    ] {
        rows(&mut session, sql);
        assert_eq!(
            displayed(rows(&mut session, "show warnings")),
            [[
                "Note",
                "1105",
                "Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'"
            ]],
            "{sql}"
        );
        let catalog = stack.factory.catalog_snapshot();
        assert!(catalog.policy("p1").is_some());
        assert!(catalog.policy("p3").is_none());
    }
}
