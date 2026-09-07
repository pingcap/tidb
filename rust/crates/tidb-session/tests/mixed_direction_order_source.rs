//! Mixed-direction multi-key ORDER BY: `order by g asc, s desc` groups by
//! the ascending key and sorts the second key descending within each group.

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
fn ascending_and_descending_keys_compose() {
    let mut session = Session::new();
    session.run("create table t (g int, s varchar(4))").unwrap();
    session
        .run("insert into t values (1, 'd'), (1, 'a'), (2, 'c'), (2, 'b'), (3, 'e')")
        .unwrap();

    assert_eq!(
        rows(&mut session, "select g, s from t order by g asc, s desc"),
        "1|'d';1|'a';2|'c';2|'b';3|'e'"
    );
}
