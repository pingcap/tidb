//! The metadata SHOW family: SHOW COLLATION (charset, id, default flag,
//! PAD SPACE attribute), SHOW CHARACTER SET (description, default
//! collation, maxlen 4), and SHOW ENGINES (InnoDB DEFAULT with the
//! transaction/locking comments).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn metadata_show_shapes() {
    let mut session = Session::new();

    let collation = rows(&mut session, "show collation like 'utf8mb4_general_ci'");
    assert_eq!(collation.len(), 1);
    assert!(collation[0].contains("utf8mb4_general_ci|utf8mb4|"), "{collation:?}");
    assert!(collation[0].contains("|Yes|"), "{collation:?}");
    assert!(collation[0].contains("PAD SPACE"), "{collation:?}");

    let charset = rows(&mut session, "show character set like 'utf8mb4'");
    assert_eq!(charset.len(), 1);
    assert!(charset[0].contains("UTF-8 Unicode"), "{charset:?}");
    assert!(charset[0].contains("utf8mb4_bin"), "{charset:?}");

    let engines = rows(&mut session, "show engines");
    assert_eq!(engines.len(), 1);
    assert!(engines[0].starts_with("InnoDB|DEFAULT|"), "{engines:?}");
    assert!(engines[0].contains("Supports transactions"), "{engines:?}");
}
