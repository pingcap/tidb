//! LPAD/RPAD cycle the pad string, truncate the input when the target is
//! shorter, and treat length 0 as empty; REVERSE reverses by rune; REPEAT
//! with a negative count is empty.

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
fn pad_reverse_repeat_edges() {
    let mut session = Session::new();

    assert_eq!(rows(&mut session, "select lpad('ab', 4, 'xy')"), "s:xyab");
    assert_eq!(rows(&mut session, "select lpad('abcdef', 3, 'x')"), "s:abc");
    assert_eq!(rows(&mut session, "select lpad('ab', 0, 'x')"), "s:");
    assert_eq!(rows(&mut session, "select rpad('ab', 4, 'xy')"), "s:abxy");

    // Multibyte-safe reversal.
    assert_eq!(rows(&mut session, "select reverse('héllo')"), "s:olléh");

    assert_eq!(
        rows(&mut session, "select repeat('a', -1), repeat('ab', 3)"),
        "s:|s:ababab"
    );
}
