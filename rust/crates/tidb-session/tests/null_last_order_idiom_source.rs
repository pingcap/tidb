//! The NULL-last ordering idiom: `order by (s is null), s` sends NULL rows
//! to the end even though the natural ascending order would put them first.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
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

#[test]
fn boolean_expression_orders_nulls_last() {
    let mut session = Session::new();
    session.run("create table t (s varchar(4))").unwrap();
    session.run("insert into t values ('b'), (NULL), ('a'), (NULL)").unwrap();

    assert_eq!(
        rows(&mut session, "select s from t order by (s is null), s"),
        "'a';'b';NULL;NULL",
        "the (is null) key sorts the NULL rows last"
    );
}
