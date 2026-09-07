//! Index DDL error surfaces: a duplicate index name fails with Go's
//! `ErrDupKeyName` (1061, "Duplicate key name 'kb'"), dropping a missing
//! index fails with `ErrCantDropFieldOrKey`-shaped 1091 ("index nope
//! doesn't exist"), and SHOW INDEX lists the PRIMARY entry with the
//! non-unique flag and the secondary index with NonUnique=1.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
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

fn setup(session: &mut Session) {
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("alter table t add index kb (b)").unwrap();
}

#[test]
fn index_ddl_error_texts_and_show_index() {
    let mut session = Session::new();
    setup(&mut session);

    // Duplicate index name: 1061.
    let error = session
        .run("alter table t add index kb (a)")
        .expect_err("the name is taken");
    assert_eq!(error.to_string(), "Duplicate key name 'kb'", "{error}");

    // Missing index: 1091.
    let error = session
        .run("alter table t drop index nope")
        .expect_err("the index does not exist");
    assert_eq!(error.to_string(), "index nope doesn't exist", "{error}");

    // SHOW INDEX lists the clustered PRIMARY and the secondary index.
    let shown = rows(&mut session, "show index from t");
    assert_eq!(shown.len(), 2);
    assert!(shown[0].contains("PRIMARY"), "{shown:?}");
    assert!(shown[0].contains('|'), "row shape: {shown:?}");
    assert!(shown[1].contains("kb"), "{shown:?}");
    assert!(shown[1].contains("b"), "{shown:?}");
}
