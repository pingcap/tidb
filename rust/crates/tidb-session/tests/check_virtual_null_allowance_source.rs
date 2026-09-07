//! CHECK over a VIRTUAL generated column with a NULL allowance:
//! `b = IF(a % 2 = 0, NULL, a)` with `check (b is null or b > 0)` —
//! even rows materialize NULL which passes (UNKNOWN), odd rows store
//! themselves.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
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
        .run("create table t (a int primary key, b int as (if(a % 2 = 0, null, a)) virtual, check (b is null or b > 0))")
        .unwrap();
}

#[test]
fn virtual_null_rows_pass_the_check() {
    let mut session = Session::new();
    setup(&mut session);

    // Even rows materialize b = NULL: UNKNOWN, which the allowance accepts.
    session.run("insert into t (a) values (2)").unwrap();
    // Odd rows store themselves.
    session.run("insert into t (a) values (1)").unwrap();

    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|1;2|NULL"
    );
}
