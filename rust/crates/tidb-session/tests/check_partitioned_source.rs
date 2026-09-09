//! CHECK enforcement on a partitioned table: violations are refused in
//! EVERY partition (p0 and p1 alike) while conforming rows land normally.

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

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run(
            "create table t (a int primary key, b int, check (b > 0)) \
             partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1, 1), (11, 11)").unwrap();
}

#[test]
fn checks_span_every_partition() {
    let mut session = Session::new();
    setup(&mut session);

    for a in [2i64, 12i64] {
        let error = session
            .run(&format!("insert into t values ({a}, -{a})"))
            .expect_err("violations are refused in every partition");
        assert!(
            error.to_string().contains("Check constraint 't_chk_1' is violated."),
            "{error}"
        );
    }

    // The conforming rows land in both partitions.
    session.run("insert into t values (3, 3), (13, 13)").unwrap();
    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|1;3|3;11|11;13|13"
    );
}
