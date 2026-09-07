//! `SHOW TABLE STATUS`: one row per table carrying the engine, the NEXT
//! auto-increment value (4 after 3 rows), the table comment, and the
//! utf8mb4_bin collation.

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
fn show_table_status_reports_metadata() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int) comment 'probe'")
        .unwrap();
    session.run("insert into t (v) values (1), (2), (3)").unwrap();

    let status = strings(&mut session, "show table status");
    assert_eq!(status.len(), 1);
    let row = &status[0];
    assert!(row.starts_with("t|InnoDB|"), "{row}");
    assert!(row.contains("Int(4)"), "the next auto-increment value: {row}");
    assert!(row.contains("probe"), "the table comment: {row}");
    assert!(row.contains("utf8mb4_bin"), "{row}");
}
