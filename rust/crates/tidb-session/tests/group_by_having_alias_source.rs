//! GROUP BY and HAVING resolve SELECT-list aliases (the MySQL extension):
//! `select a as g, sum(v) s from t group by g having s > 2` groups by the
//! aliased column and filters on the aliased aggregate.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::Decimal(d) => {
                            format!("{:?}", d.to_string())
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

#[test]
fn aliases_resolve_in_group_by_and_having() {
    let mut session = Session::new();
    session.run("create table t (a int, v int)").unwrap();
    session.run("insert into t values (1, 1), (1, 2), (2, 3)").unwrap();

    assert_eq!(
        rows(&mut session, "select a as g, sum(v) from t group by g order by g"),
        r#"1|"3";2|"3""#
    );

    // s = sum(v): both groups have 3, so `s > 2` keeps both, `s > 3` drops both.
    assert_eq!(
        rows(&mut session, "select a as g, sum(v) s from t group by g having s > 2 order by g"),
        r#"1|"3";2|"3""#
    );
    assert_eq!(
        rows(&mut session, "select a as g, sum(v) s from t group by g having s > 3 order by g"),
        ""
    );
}
