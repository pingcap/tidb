//! Tuple IN and ROW comparison: `(a, b) IN ((1,'x'), (3,'z'))` matches on
//! the full row value, `ROW(a, b) = (2, 'y')` is the explicit spelling, and
//! the negated form keeps the complement.

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
    session.run("create table t (a int, b varchar(4))").unwrap();
    session
        .run("insert into t values (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
}

#[test]
fn tuple_membership_and_row_equality() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        rows(&mut session, "select a from t where (a, b) in ((1, 'x'), (3, 'z')) order by a"),
        "i:1;i:3"
    );
    assert_eq!(rows(&mut session, "select a from t where row(a, b) = (2, 'y')"), "i:2");
    assert_eq!(
        rows(&mut session, "select a from t where (a, b) not in ((1, 'x')) order by a"),
        "i:2;i:3"
    );
}
