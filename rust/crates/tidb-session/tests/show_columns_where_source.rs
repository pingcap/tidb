//! SHOW COLUMNS accepts a WHERE predicate over its own columns: `where
//! field = 'name'` picks one column, `field <> 'id'` keeps the rest.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
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
        .run("create table t (id int primary key, name varchar(8), flag boolean)")
        .unwrap();
}

#[test]
fn where_predicate_filters_columns() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        rows(&mut session, "show columns from t where field = 'name'"),
        "name|varchar(8)|YES||Null|"
    );
    let rest = rows(&mut session, "show columns from t where field <> 'id'");
    assert_eq!(rest.split(';').count(), 2, "{rest}");
    assert!(rest.contains("name|"), "{rest}");
    assert!(rest.contains("flag|"), "{rest}");
    assert!(!rest.contains("id|"), "{rest}");
}
