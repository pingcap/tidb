//! Multibyte string semantics: CHAR_LENGTH counts characters while
//! OCTET_LENGTH counts bytes ('中a' = 2 chars, 4 bytes); SUBSTRING/MID are
//! character-positioned; CONVERT ... USING transcodes.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn char_vs_byte_lengths() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select char_length('中a'), octet_length('中a')"),
        "i:2|i:4"
    );

    // Positions are in characters, not bytes: both answers include '中'.
    assert_eq!(
        rows(&mut session, "select substring('中abc', 1, 2), mid('中abc', 2, 2)"),
        "s:中a|s:ab"
    );

    assert_eq!(rows(&mut session, "select convert('ab' using ascii)"), "s:ab");
}
