// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;

/// Go `TestV2Basic` and `TestDataStructFieldsCorrectnessInSchemaChange`,
/// exercised through the domain loader and the embedded storage factory.
#[test]
fn test_v2_basic_domain_loader() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19004)).unwrap();
    for sql in [
        "CREATE DATABASE v2_test",
        "USE v2_test",
        "CREATE TABLE t (a BIGINT)",
        "INSERT INTO t VALUES (1)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let old = stack.factory.catalog.get_latest();
    assert!(old.is_v2());
    assert!(old.schema_commit_ts() > 0);
    assert!(old.schema_commit_ts() <= old.schema_ts());
    assert!(
        tidb_txnkv::kv_storage::Storage::oldest_schema_version(&stack.opener)
            .unwrap()
            .is_some()
    );
    let old_reader = old.v2().unwrap();
    let id = old_reader
        .table_item_by_name("v2_test", "t")
        .unwrap()
        .table_id;
    old_reader.evict_table("v2_test", "t");
    assert!(!old_reader.table_is_cached(id));
    assert_eq!(
        old_reader
            .table_by_id(id, None)
            .unwrap()
            .info
            .read()
            .name
            .original(),
        "t"
    );
    assert!(!old_reader.table_is_cached(id));
    assert_eq!(
        old_reader
            .table_by_name("v2_test", "t", None)
            .unwrap()
            .info
            .read()
            .columns
            .len(),
        1
    );
    assert!(old_reader.table_is_cached(id));
    assert_eq!(displayed(rows(&mut session, "SELECT a FROM t")), [["1"]]);
    session
        .node_conn()
        .execute_write("RENAME TABLE t TO renamed")
        .unwrap();
    let renamed = stack.factory.catalog.get_latest();
    assert!(renamed
        .v2()
        .unwrap()
        .table_item_by_name("v2_test", "t")
        .is_none());
    assert_eq!(
        renamed
            .v2()
            .unwrap()
            .table_item_by_name("v2_test", "renamed")
            .unwrap()
            .table_id,
        id
    );
    old_reader.evict_table("v2_test", "t");
    assert_eq!(
        old_reader
            .table_by_name("v2_test", "t", None)
            .unwrap()
            .info
            .read()
            .name
            .original(),
        "t"
    );
    stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    old_reader.table_by_name("v2_test", "t", None).unwrap();
    // Go's integration test permits concurrent internal readers to lower
    // the same minimum; this read must at least pin its own timestamp.
    assert!(
        stack
            .factory
            .catalog
            .get_and_reset_recent_info_schema_ts(u64::MAX)
            <= old.schema_ts()
    );
    session
        .node_conn()
        .execute_write("DROP TABLE renamed")
        .unwrap();
    assert!(stack
        .factory
        .catalog
        .get_latest()
        .v2()
        .unwrap()
        .table_item_by_id(id)
        .is_none());
    assert!(old_reader.table_by_id(id, None).is_some());
}

/// Go Loader.shouldUseV2 and TiDBSchemaCacheSize.SetGlobal: the same-version
/// reload switches implementations after an INSTANCE assignment.
#[test]
fn schema_cache_size_switches_domain_loader_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19005)).unwrap();
    assert!(stack.factory.catalog.get_latest().is_v2());
    for (size, v2) in [("0", false), ("128MB", true)] {
        session
            .node_conn()
            .execute_write(&format!("SET INSTANCE tidb_schema_cache_size = '{size}'"))
            .unwrap();
        stack._reloader.waker().nudge();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while stack.factory.catalog.get_latest().is_v2() != v2 {
            assert!(
                std::time::Instant::now() < deadline,
                "schema cache mode did not reload"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!displayed(rows(
            &mut session,
            "SELECT variable_name FROM mysql.global_variables LIMIT 1"
        ))
        .is_empty());
    }
    assert_eq!(stack.factory.catalog.data.cache_capacity(), 128 << 20);
}

/// Go `TestGetAndResetRecentInfoSchemaTS`,
/// pkg/infoschema/test/infoschemav2test/v2_test.go.
#[test]
fn test_get_and_reset_recent_info_schema_ts() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19006)).unwrap();
    for sql in [
        "SET GLOBAL tidb_schema_cache_size = 1073741824",
        "REPLACE INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20000101-00:00:00 +0000', '')",
        "USE test",
        "CREATE TABLE dummytbl (id INT)",
    ] {
        rows(&mut session, sql);
    }
    let mut ts_before = stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    let txn = stack.opener.begin_read_only().unwrap();
    let snapshot_ts = txn.start_ts();
    txn.finish_without_writes().unwrap();
    for sql in [
        "ALTER TABLE dummytbl ADD COLUMN c INT",
        "ALTER TABLE dummytbl ADD INDEX idx(c)",
    ] {
        rows(&mut session, sql);
        let ts = stack
            .factory
            .catalog
            .get_and_reset_recent_info_schema_ts(u64::MAX);
        assert!(ts >= ts_before);
        ts_before = ts;
    }
    stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    assert!(rows(&mut session, "SELECT * FROM dummytbl").is_empty());
    let current = stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    assert_ne!(current, u64::MAX);
    rows(&mut session, &format!("SET tidb_snapshot='{snapshot_ts}'"));
    stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    assert!(rows(&mut session, "SELECT * FROM dummytbl").is_empty());
    let historic = stack
        .factory
        .catalog
        .get_and_reset_recent_info_schema_ts(u64::MAX);
    assert_ne!(historic, u64::MAX);
    assert!(historic < current, "historic={historic}, current={current}");
    rows(&mut session, "SET tidb_snapshot=''");
}

/// Go `TestUnrelatedDDLTriggerReload`,
/// pkg/infoschema/test/infoschemav2test/v2_test.go.
#[test]
fn test_unrelated_ddl_trigger_reload() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19007)).unwrap();
    for sql in [
        "USE test",
        "SET GLOBAL tidb_schema_cache_size = 536870912",
        "CREATE TABLE t1 (id INT)",
    ] {
        rows(&mut session, sql);
    }
    assert!(rows(&mut session, "SELECT * FROM t1").is_empty());
    stack
        .factory
        .catalog
        .get_latest()
        .v2()
        .unwrap()
        .evict_table("test", "t1");
    rows(&mut session, "CREATE TABLE t2 (id INT)");
    let error = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError",
        "return(true)",
    );
    assert!(session.node_conn().execute("SELECT * FROM t1").is_err());
    drop(error);
    assert!(rows(&mut session, "SELECT * FROM t1").is_empty());
    rows(&mut session, "CREATE TABLE t3 (id INT)");
    let _error = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError",
        "return(true)",
    );
    assert!(rows(&mut session, "SELECT * FROM t1").is_empty());
}

/// Under InfoSchema V2 (Go's default) a schema's `DBInfo` carries no table
/// list, so DDL reads tables only through Go's V2 lookups
/// (`GetTableReferredForeignKeys`, `TableByName`, `TableExists`,
/// `ListTablesWithSpecialAttribute`). Answers captured from Go with `gorun`.
#[test]
fn v2_ddl_reads_tables_through_go_v2_lookups() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19010)).unwrap();
    for sql in [
        "USE test",
        "CREATE TABLE p (id INT PRIMARY KEY)",
        "CREATE TABLE c (id INT, pid INT, CONSTRAINT fk_1 FOREIGN KEY (pid) REFERENCES p(id))",
        "CREATE PLACEMENT POLICY x FOLLOWERS=1",
        "CREATE TABLE tp (a INT) PLACEMENT POLICY=x",
        "SET GLOBAL tidb_enable_check_constraint = 1",
        "CREATE TABLE ck_t (a INT)",
        "ALTER TABLE ck_t ADD CONSTRAINT ck CHECK (a > 0)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    assert!(stack.factory.catalog.get_latest().is_v2());
    for (sql, code, message) in [
        (
            "DROP TABLE p",
            3730,
            "Cannot drop table 'p' referenced by a foreign key constraint 'fk_1' on table 'c'.",
        ),
        (
            "TRUNCATE TABLE p",
            1701,
            "Cannot truncate a table referenced in a foreign key constraint (`test`.`c` CONSTRAINT `fk_1`)",
        ),
        ("RENAME TABLE c TO p", 1050, "Table 'test.p' already exists"),
        ("DROP PLACEMENT POLICY x", 8241, "Placement policy 'x' is still in use"),
        (
            "ALTER TABLE ck_t ADD CONSTRAINT ck CHECK (a > 1)",
            3822,
            "Duplicate check constraint name 'ck'.",
        ),
        (
            "ALTER DATABASE test SET TIFLASH REPLICA 1",
            1105,
            "the tiflash replica count: 1 should be less than the total tiflash server count: 0",
        ),
    ] {
        let error = session.node_conn().execute_write(sql).unwrap_err();
        assert_eq!((error.code, error.message.as_str()), (code, message), "{sql}");
    }
    // Go `adjustForeignKeyChildTableInfoAfterModifyColumn`: the child follows
    // the parent's renamed column.
    session
        .node_conn()
        .execute_write("ALTER TABLE p RENAME COLUMN id TO id2")
        .unwrap();
    let create = displayed(rows(&mut session, "SHOW CREATE TABLE c"));
    assert!(
        create[0][1].contains("CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `p` (`id2`)"),
        "{}",
        create[0][1]
    );
}

/// A table re-partitioned (`ALTER TABLE ... PARTITION BY`, a new table id)
/// and then dropped leaves no placement-policy user behind: the V2 diff
/// copy of the partitioning step resolved the table and each of its
/// partitions to the same table and listed it once per id, so the diff's
/// drop of the old id left a copy that outlived the table. Go replays
/// `ddl/bdr_mode`'s sequence with `DROP PLACEMENT POLICY` succeeding.
#[test]
fn v2_dropped_repartitioned_table_releases_its_placement_policy() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(19012)).unwrap();
    for sql in [
        "USE test",
        "CREATE PLACEMENT POLICY pp1 FOLLOWERS=1",
        "CREATE TABLE t (a INT)",
        "ALTER TABLE t PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (1000000), PARTITION pMax VALUES LESS THAN (MAXVALUE) PLACEMENT POLICY pp1)",
        "DROP TABLE t",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let latest = stack.factory.catalog.get_latest();
    assert!(latest
        .v2()
        .unwrap()
        .list_tables_with_special_attribute(|_| true)
        .is_empty());
    session
        .node_conn()
        .execute_write("DROP PLACEMENT POLICY pp1")
        .unwrap();
}
