//! JSON_OVERLAPS answers whether two documents share any member, MEMBER OF
//! tests scalar membership in an array, and JSON_TYPE reports OBJECT for an
//! object document.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
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

#[test]
fn overlaps_membership_and_object_type() {
    let mut session = Session::new();

    // Shared member 3.
    assert_eq!(
        try_sql(&mut session, "select json_overlaps('[1, 2, 3]', '[3, 4]')"),
        "i:1"
    );
    // Disjoint arrays.
    assert_eq!(
        try_sql(&mut session, "select json_overlaps('[1, 2]', '[5]')"),
        "i:0"
    );

    // MEMBER OF syntax.
    assert_eq!(try_sql(&mut session, "select 2 member of ('[1, 2, 3]')"), "i:1");

    // Object documents report OBJECT.
    assert_eq!(try_sql(&mut session, "select json_type('{\"a\": 1}')"), "s:OBJECT");
}
