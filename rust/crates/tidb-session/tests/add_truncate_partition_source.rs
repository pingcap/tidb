//! `ALTER TABLE` partition lifecycle: ADD PARTITION extends the top range
//! (new inserts route into it) and TRUNCATE PARTITION clears a partition's
//! rows while keeping its definition (re-inserts route back into it).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

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

/// Containment until the complete durable partition owner is available.
/// Go supports these changes through online reorganization; silently swapping
/// routing without that owner must not lose access to the existing records.
#[test]
fn repartition_refusal_preserves_rows_and_schema() {
    for create in [
        "CREATE TABLE t (a INT)",
        "CREATE TABLE t (a INT) PARTITION BY RANGE(a) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
    ] {
        for alter in [
            "ALTER TABLE t PARTITION BY HASH(a) PARTITIONS 2",
            "ALTER TABLE t ADD COLUMN b INT PARTITION BY HASH(a) PARTITIONS 2",
        ] {
            tidb_parser::parse(alter).expect("the refused action list must reach DDL admission");
            let mut session = Session::new();
            session.run(create).unwrap();
            session.run("INSERT INTO t VALUES (1),(11)").unwrap();
            let schema_before = format!("{:?}", session.run("SHOW CREATE TABLE t").unwrap());

            let outcome = session.run(alter);
            assert_eq!(rows(&mut session, "SELECT a FROM t ORDER BY a"), "1;11");
            assert_eq!(
                format!("{:?}", session.run("SHOW CREATE TABLE t").unwrap()),
                schema_before,
                "a refused repartition must preserve the whole schema: {alter}"
            );
            assert!(
                outcome.is_err(),
                "unaccepted repartition was dispatched: {alter}"
            );
            session.run("INSERT INTO t VALUES (2)").unwrap();
            assert_eq!(rows(&mut session, "SELECT a FROM t ORDER BY a"), "1;2;11");
        }
    }
}
