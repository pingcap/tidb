//! GROUP_CONCAT forms and BIT aggregates: ORDER BY inside GROUP_CONCAT
//! sorts the joined values and SEPARATOR changes the glue; BIT_AND/BIT_OR/
//! BIT_XOR fold {3,1,2} to 0/3/0.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table t (g int, v int)").unwrap();
    session.run("insert into t values (1, 3), (1, 1), (1, 2), (2, 5)").unwrap();
}

#[test]
fn group_concat_forms_and_bit_folds() {
    let mut session = Session::new();
    seed(&mut session);

    // ORDER BY inside GROUP_CONCAT sorts each group's joined values.
    assert_eq!(
        rows(
            &mut session,
            "select group_concat(v order by v) from t group by g order by g"
        ),
        "'1,2,3';'5'"
    );

    // SEPARATOR replaces the default comma.
    assert_eq!(
        rows(
            &mut session,
            "select group_concat(v order by v separator ';') from t group by g order by g"
        ),
        "'1;2;3';'5'"
    );

    // BIT folds over {3,1,2}: AND=0, OR=3, XOR=0.
    assert_eq!(
        rows(&mut session, "select bit_and(v), bit_or(v), bit_xor(v) from t where g = 1"),
        "0|3|0"
    );
}
