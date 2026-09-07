//! The hash function family against published reference vectors: MD5 and
//! SHA-1 of "abc", SHA-256, CRC32, and NULL propagation.

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
fn reference_digest_vectors() {
    let mut session = Session::new();

    // FIPS 180-1 / RFC 1321 test vectors for "abc".
    assert_eq!(
        try_sql(&mut session, "select md5('abc')"),
        "s:900150983cd24fb0d6963f7d28e17f72"
    );
    assert_eq!(
        try_sql(&mut session, "select sha1('abc')"),
        "s:a9993e364706816aba3e25717850c26c9cd0d89d"
    );
    assert_eq!(
        try_sql(&mut session, "select sha2('abc', 256)"),
        "s:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );

    // The standard CRC-32 check value for "abc".
    assert_eq!(try_sql(&mut session, "select crc32('abc')"), "i:891568578");

    // NULL in, NULL out.
    assert_eq!(try_sql(&mut session, "select md5(null)"), "Null");
}
