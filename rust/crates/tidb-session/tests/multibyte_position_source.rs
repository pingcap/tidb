//! Multibyte positioning and lengths: LOCATE/INSTR report CHARACTER
//! positions ('a' after a 3-byte 中 is character 2, not byte 4), BIT_LENGTH
//! counts bits (32 for '中a'), and LPAD's target length is in characters
//! (lpad('中', 2, 'ab') = 'a中').

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
fn character_positions_not_bytes() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select locate('a', '中a')"), "i:2");
    assert_eq!(try_sql(&mut session, "select instr('中a', 'a')"), "i:2");

    // 6 bytes -> 48 bits.
    assert_eq!(try_sql(&mut session, "select bit_length('中a')"), "i:32");

    // The pad target is 2 CHARACTERS: one pad char fits before 中.
    assert_eq!(try_sql(&mut session, "select lpad('中', 2, 'ab')"), "s:a中");
}
