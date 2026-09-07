//! STRCMP's three-way result (-1/0/1) and the NULL-safe <=> operator:
//! NULL <=> NULL is TRUE and anything <=> NULL is FALSE — where the plain =
//! operator yields NULL for both.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn three_way_and_null_safe() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select strcmp('a', 'b'), strcmp('b', 'a'), strcmp('a', 'a')"),
        "i:-1|i:1|i:0"
    );

    // <=> treats NULL as an ordinary comparable value.
    assert_eq!(try_sql(&mut session, "select 1 <=> 1, 1 <=> null, null <=> null"), "i:1|i:0|i:1");

    // Plain = yields NULL with a NULL operand.
    assert_eq!(try_sql(&mut session, "select 1 = null, null = null"), "Null|Null");
}
