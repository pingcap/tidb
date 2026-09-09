//! `SHOW ENGINES`: a single InnoDB row with the DEFAULT support flag and
//! TiDB's transaction/row-lock/FK description.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned()
                        }
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
fn show_engines_lists_innodb_as_default() {
    let mut session = Session::new();
    let engines = strings(&mut session, "show engines");
    assert_eq!(engines.len(), 1, "TiDB lists exactly one engine");

    let innodb = &engines[0];
    assert!(innodb.starts_with("InnoDB|DEFAULT"), "{innodb}");
    assert!(
        innodb.contains("Supports transactions, row-level locking, and foreign keys"),
        "{innodb}"
    );
}
