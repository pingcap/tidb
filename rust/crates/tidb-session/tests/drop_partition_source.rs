//! `ALTER TABLE ... DROP PARTITION`: the partition's rows go with it, other
//! partitions' rows survive, and the dropped range disappears — inserting a
//! value only the dropped partition covered now fails with Go's 1526
//! ("Table has no partition for value ...").

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
fn dropping_a_partition_removes_its_rows_and_range() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (11), (12)").unwrap();

    session.run("alter table t drop partition p1").unwrap();
    assert_eq!(rows(&mut session, "select a from t order by a"), "1", "p1's rows are gone");

    let error = session
        .run("insert into t values (15)")
        .expect_err("the range p1 covered no longer exists");
    assert!(
        error.to_string().contains("Table has no partition for value 15"),
        "{error}"
    );
    assert_eq!(rows(&mut session, "select a from t order by a"), "1");
}
