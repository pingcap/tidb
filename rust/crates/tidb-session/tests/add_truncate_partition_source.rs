//! `ALTER TABLE` partition lifecycle: ADD PARTITION extends the top range
//! (new inserts route into it) and TRUNCATE PARTITION clears a partition's
//! rows while keeping its definition (re-inserts route back into it).

use tidb_session::Session;

use crate::support::integer_rows_with_sql as rows;

#[test]
fn add_and_truncate_partition_lifecycle() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (11)").unwrap();

    // ADD PARTITION extends the covered range.
    session
        .run("alter table t add partition (partition p2 values less than (30))")
        .unwrap();
    session.run("insert into t values (25)").unwrap();
    assert_eq!(rows(&mut session, "select a from t partition (p2)"), "25");

    // TRUNCATE PARTITION clears p1's rows but keeps the definition.
    session.run("alter table t truncate partition p1").unwrap();
    assert_eq!(rows(&mut session, "select a from t order by a"), "1;25");
    session.run("insert into t values (12)").unwrap();
    assert_eq!(rows(&mut session, "select a from t partition (p1)"), "12");
}

/// Go `AlterTablePartitioning`: the table is repartitioned with its rows,
/// whether or not it was partitioned before; combined with another
/// specification it is a multi-schema change Go refuses
/// (`fillMultiSchemaInfo`), leaving the table as it was.
#[test]
fn repartition_moves_rows_and_refuses_a_multi_schema_change() {
    for create in [
        "CREATE TABLE t (a INT)",
        "CREATE TABLE t (a INT) PARTITION BY RANGE(a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    ] {
        let mut session = Session::new();
        session.run(create).unwrap();
        session.run("INSERT INTO t VALUES (1),(11)").unwrap();
        let schema_before = format!("{:?}", session.run("SHOW CREATE TABLE t").unwrap());
        let error = session
            .run("ALTER TABLE t ADD COLUMN b INT PARTITION BY HASH(a) PARTITIONS 2")
            .expect_err("Go refuses PARTITION BY inside a multi-schema change")
            .to_mysql_error();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                8200,
                "Unsupported multi schema change for alter table partition by"
            )
        );
        assert_eq!(
            format!("{:?}", session.run("SHOW CREATE TABLE t").unwrap()),
            schema_before
        );

        session
            .run("ALTER TABLE t PARTITION BY HASH(a) PARTITIONS 2")
            .unwrap();
        assert_eq!(rows(&mut session, "SELECT a FROM t ORDER BY a"), "1;11");
        assert_eq!(rows(&mut session, "SELECT a FROM t PARTITION (p1)"), "1;11");
        session.run("INSERT INTO t VALUES (2)").unwrap();
        assert_eq!(rows(&mut session, "SELECT a FROM t PARTITION (p0)"), "2");
    }
}

/// Go `updateTable`'s `needUpdateTs`: a TRUNCATE PARTITION (like TRUNCATE
/// TABLE, RENAME and EXCHANGE PARTITION) leaves the table's `UpdateTS`,
/// which `information_schema.tables.CREATE_TIME` shows, where it was; an
/// ADD PARTITION moves it.
#[test]
fn truncate_partition_keeps_the_tables_update_ts() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    let create_time = "select create_time from information_schema.tables \
                       where table_schema = 'test' and table_name = 't'";
    let created = crate::support::text_rows(&mut session, create_time);
    // CREATE_TIME has second precision.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    session.run("alter table t truncate partition p1").unwrap();
    assert_eq!(crate::support::text_rows(&mut session, create_time), created);
    session
        .run("alter table t add partition (partition p2 values less than (30))")
        .unwrap();
    assert_ne!(crate::support::text_rows(&mut session, create_time), created);
}

/// Go `onTruncateTable` re-creates the table under its new id from the old
/// `TableInfo` and `onRenameTable` moves it as it is: neither stamps
/// `UpdateTS`, so `CREATE_TIME` stays; an ADD COLUMN moves it.
#[test]
fn truncate_and_rename_table_keep_the_tables_update_ts() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    let create_time = |table: &str| {
        format!(
            "select create_time from information_schema.tables \
             where table_schema = 'test' and table_name = '{table}'"
        )
    };
    let created = crate::support::text_rows(&mut session, &create_time("t"));
    // CREATE_TIME has second precision.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    session.run("truncate table t").unwrap();
    assert_eq!(crate::support::text_rows(&mut session, &create_time("t")), created);
    session.run("rename table t to t2").unwrap();
    assert_eq!(crate::support::text_rows(&mut session, &create_time("t2")), created);
    session.run("alter table t2 add column b int").unwrap();
    assert_ne!(crate::support::text_rows(&mut session, &create_time("t2")), created);
}
