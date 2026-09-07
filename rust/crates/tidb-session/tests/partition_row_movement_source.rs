//! Cross-partition row movement on UPDATE: changing the partition key so a
//! row must move (`1` -> `15`, p0 -> p1) relocates the row — Go removes it
//! from the old partition and adds it to the new one
//! (`pkg/table/tables/partition.go`, the `from != to` arm).

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
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn update_moves_the_row_between_partitions() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (11)").unwrap();
    session.run("update t set a = 15 where a = 1").unwrap();

    assert_eq!(
        rows(&mut session, "select a from t order by a"),
        "11;15",
        "the moved row lands in p1 beside 11"
    );
    // The moved row is addressable from its NEW partition only.
    assert_eq!(rows(&mut session, "select a from t partition (p1)"), "11;15");
    assert_eq!(rows(&mut session, "select a from t partition (p0)"), "");
}
