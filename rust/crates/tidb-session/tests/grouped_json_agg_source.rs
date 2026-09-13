//! Grouped JSON aggregates: JSON_ARRAYAGG collects each group's values in
//! order and JSON_OBJECTAGG builds per-group documents — verified through
//! JSON_LENGTH/JSON_CONTAINS projections over the binary results.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (g int, v int)").unwrap();
    session.run("insert into t values (1, 10), (1, 20), (2, 30)").unwrap();
}

#[test]
fn grouped_json_aggregates() {
    let mut session = Session::new();
    setup(&mut session);

    // ARRAYAGG: two members for g=1, one for g=2, in input order.
    assert_eq!(
        try_sql(
            &mut session,
            "select g, json_length(json_arrayagg(v)) from t group by g order by g"
        ),
        "i:1|i:2;i:2|i:1"
    );
    assert_eq!(
        try_sql(
            &mut session,
            "select json_contains(json_arrayagg(v), cast(10 as json)) from t where g = 1"
        ),
        "i:1"
    );

    // OBJECTAGG: one member per ROW (keys 10, 20 for g=1; key 30 for g=2).
    assert_eq!(
        try_sql(
            &mut session,
            "select g, json_length(json_objectagg(v, g)) from t group by g order by g"
        ),
        "i:1|i:2;i:2|i:1"
    );
}
