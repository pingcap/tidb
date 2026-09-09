//! SHOW TABLE STATUS reports the Go row shape for an auto-increment table
//! (`fetchTableStatus` column order): Engine InnoDB, Auto_increment (col 11)
//! = the allocator's NEXT value (3 after two inserts, the same
//! `next_auto_increment` source SHOW CREATE uses), Collation, and COMMENT.
//! Rows is a stats ESTIMATE and is intentionally not asserted.

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
fn table_status_reports_next_auto_increment_and_rows() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int) comment 'hello'")
        .unwrap();
    session.run("insert into t (v) values (1)").unwrap();
    session.run("insert into t (v) values (2)").unwrap();

    let row = &strings(&mut session, "show table status like 't'")[0];
    let fields: Vec<&str> = row.split('|').map(|f| f.trim()).collect();

    assert_eq!(fields[0], "t");
    assert_eq!(fields[1], "InnoDB");
    assert_eq!(fields[10], "Int(3)"); // Auto_increment: the NEXT value
    assert_eq!(fields[14], "utf8mb4_bin");
    assert_eq!(fields[17], "hello"); // Comment
}
