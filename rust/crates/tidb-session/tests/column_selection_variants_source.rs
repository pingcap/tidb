//! Column selection variants: bare names, table-qualified names, `t.*`,
//! `*` mixed with extra items, and constant expressions beside columns —
//! each resolves and orders per the projection.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
    session.run("create table t (id int primary key, a int)").unwrap();
    session.run("insert into t values (1, 5), (2, 7)").unwrap();
}

#[test]
fn column_selection_variants() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(rows(&mut session, "select id from t order by id"), "i:1;i:2");
    assert_eq!(rows(&mut session, "select t.id from t order by t.id"), "i:1;i:2");
    assert_eq!(rows(&mut session, "select t.* from t order by id"), "i:1|i:5;i:2|i:7");
    assert_eq!(
        rows(&mut session, "select *, a from t order by id"),
        "i:1|i:5|i:5;i:2|i:7|i:7"
    );
    assert_eq!(
        rows(&mut session, "select 42, id from t order by id"),
        "i:42|i:1;i:42|i:2"
    );
}
