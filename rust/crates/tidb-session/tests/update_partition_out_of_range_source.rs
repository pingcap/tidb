//! UPDATE moving a row to a value no partition covers refuses with Go's
//! 1526 text ("Table has no partition for value 50") and the row stays in
//! place. Note the schema: the PK includes the partition column, which the
//! CREATE-time unique-key rule (1503) requires.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn out_of_range_move_refused() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id int, p int, primary key (id, p)) partition by range (p) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1, 5)").unwrap();

    assert_eq!(
        error(&mut session, "update t set p = 50 where id = 1"),
        "Table has no partition for value 50"
    );

    // The row is untouched.
    assert_eq!(rows(&mut session, "select id, p from t"), "Int(1)|Int(5)");
}
