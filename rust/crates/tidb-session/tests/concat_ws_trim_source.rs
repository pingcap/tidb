//! CONCAT_WS skips NULL arguments but a NULL separator makes the whole
//! result NULL; TRIM/LTRIM/RTRIM and the remstr forms (BOTH/LEADING with a
//! custom character) strip as written.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn concat_ws_and_trim_forms() {
    let mut session = Session::new();

    // NULL arguments are skipped; the separator is emitted between kept values.
    assert_eq!(
        rows(&mut session, "select concat_ws('-', 'a', null, 'b')"),
        "s:a-b"
    );
    // A NULL separator poisons the whole result.
    assert_eq!(rows(&mut session, "select concat_ws(null, 'a', 'b')"), "Null");

    assert_eq!(rows(&mut session, "select trim('  ab  ')"), "s:ab");
    assert_eq!(rows(&mut session, "select ltrim('  ab  ')"), "s:ab  ");
    assert_eq!(rows(&mut session, "select rtrim('  ab  ')"), "s:  ab");

    // remstr forms.
    assert_eq!(rows(&mut session, "select trim(both 'x' from 'xxabxx')"), "s:ab");
    assert_eq!(rows(&mut session, "select trim(leading 'x' from 'xxab')"), "s:ab");
}
