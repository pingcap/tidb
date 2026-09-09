//! UNION type unification: arms of different types unify to the common
//! result type — int and string arms both render as strings, and a NULL
//! arm keeps its NULL while unifying with the int arm.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn mixed_type_arms_unify() {
    let mut session = Session::new();

    // int 1 renders as the string '1' beside 'abc' (string unification).
    let mut got = rows(&mut session, "select 1 as v union select 'abc' as v");
    got.sort();
    assert_eq!(got, vec!["'1'", "'abc'"]);

    // NULL beside 7 keeps the NULL and the 7.
    let mut got = rows(&mut session, "select null as v union select 7 as v");
    got.sort();
    assert_eq!(got, vec!["7", "NULL"]);

    // 'a' beside 2: unified, order-free.
    let mut got = rows(&mut session, "select 'a' as v union select 2 as v");
    got.sort();
    assert_eq!(got, vec!["'2'", "'a'"]);
}
