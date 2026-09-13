//! An indexed STORED generated column filters and groups: b = a % 10
//! indexed, WHERE b = 5 answers the 3 rows whose a ends in 5, WHERE
//! b < 3 answers 8, and GROUP BY b orders by the computed value.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (id int primary key, a int, \
             b int as (a % 10) stored, index ib (b))",
        )
        .unwrap();
    let vals: Vec<String> = (1..=25).map(|i| format!("({i}, {i})")).collect();
    session
        .run(&format!("insert into t (id, a) values {}", vals.join(", ")))
        .unwrap();
}

#[test]
fn indexed_stored_generated_filters_and_groups() {
    let mut session = Session::new();
    setup(&mut session);

    // a in {5, 15, 25} -> b = 5.
    assert_eq!(rows(&mut session, "select count(*) from t where b = 5"), "i:3");
    // b in {0, 1, 2}: a = 10,20 | 1,11,21 | 2,12,22.
    assert_eq!(rows(&mut session, "select count(*) from t where b < 3"), "i:8");
    assert_eq!(
        rows(&mut session, "select b, count(*) from t group by b order by b limit 2"),
        "i:0|i:2;i:1|i:3"
    );
}
