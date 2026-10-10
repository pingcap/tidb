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
