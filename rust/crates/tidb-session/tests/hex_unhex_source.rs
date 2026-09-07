//! HEX/UNHEX edge semantics per Go `builtinUnHexSig`
//! (builtin_string.go:1844-1861): HEX(-1) renders the 64-bit two's
//! complement, UNHEX pads an odd digit count with a leading '0' (NOT NULL),
//! and invalid hex digits yield NULL.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            format!("bytes:{}", String::from_utf8_lossy(bytes))
                        }
                        tidb_datatype::Datum::String(value) => {
                            format!("bytes:{}", String::from_utf8_lossy(value.bytes()))
                        }
                        tidb_datatype::Datum::Null => "Null".to_owned(),
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
fn hex_unhex_edge_rules() {
    let mut session = Session::new();

    // hex('ab') = "6162", hex(255) = "FF", hex(-1) = 16 F's.
    let hexes = rows(&mut session, "select hex('ab'), hex(255), hex(-1)");
    assert!(hexes.contains("6162"), "{hexes}");
    assert!(hexes.contains("bytes:FF"), "{hexes}");
    assert!(hexes.contains("FFFFFFFFFFFFFFFF"), "{hexes}");

    // unhex('6162') = "ab"; odd length pads a leading 0 ('abc' -> 0x0ABC);
    // invalid digits -> NULL.
    let unhexes = rows(&mut session, "select unhex('6162'), unhex('abc'), unhex('zz')");
    assert!(unhexes.contains("bytes:ab"), "{unhexes}");
    // 0x0A 0xBC renders lossily through the UTF-8 projection.
    assert!(unhexes.contains("\u{a}\u{fffd}"), "{unhexes}");
    assert!(unhexes.contains("Null"), "{unhexes}");

    // Round trip.
    assert!(rows(&mut session, "select unhex(hex('ti'))").contains("bytes:ti"));
}
