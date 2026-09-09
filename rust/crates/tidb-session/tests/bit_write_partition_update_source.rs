//! Two write shapes: BIT(3) stores b'101' as 5 and refuses 8 with Go's
//! "Data too long" (convertToMysqlBit), and `UPDATE t PARTITION (p0)` moves
//! only the named partition's rows.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
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
fn bit_write_bounds() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, bits bit(3))").unwrap();
    session.run("insert into t values (1, b'101')").unwrap();
    assert_eq!(rows(&mut session, "select bits + 0 from t"), "5");

    let error = session
        .run("insert into t values (2, 8)")
        .expect_err("8 does not fit BIT(3)");
    assert!(
        error.to_string().contains("Data too long for column 'bits' at row 1"),
        "{error}"
    );
}

#[test]
fn partition_qualified_update_spares_other_partitions() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (11)").unwrap();
    session.run("update t partition (p0) set a = a * 2").unwrap();
    assert_eq!(
        rows(&mut session, "select a from t order by a"),
        "2;11",
        "only p0's row doubles; p1's row stays"
    );
}
