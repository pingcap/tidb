//! TIME boundary writes: the maximum `'838:59:59'` stores, an over-long
//! `'839:00:00'` fails strict with Go's "Out of range value" (1264), and
//! negative durations store.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Duration(d) => format!("{d}"),
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
    session.run("create table t (a int primary key, tm time)").unwrap();
}

#[test]
fn time_boundary_writes() {
    let mut session = Session::new();
    setup(&mut session);

    // The maximum TIME literal stores.
    session.run("insert into t values (1, '838:59:59')").unwrap();
    assert_eq!(rows(&mut session, "select tm from t"), "838:59:59");

    // An over-long value is refused under strict mode (1264).
    let error = session
        .run("insert into t values (2, '839:00:00')")
        .expect_err("839 hours is out of range");
    assert!(
        error.to_string().contains("Out of range value for column 'tm' at row 1"),
        "{error}"
    );

    // Negative durations store.
    session.run("insert into t values (3, '-100:00:00')").unwrap();
    assert_eq!(rows(&mut session, "select tm from t where a = 3"), "-100:00:00");
}
