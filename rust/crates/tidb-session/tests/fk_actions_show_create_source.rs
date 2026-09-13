//! FK action clauses round-trip in SHOW CREATE:
//! `ON DELETE CASCADE ON UPDATE SET NULL` renders verbatim in the
//! CONSTRAINT clause (utf8mb4_bin decoded).

use tidb_session::Session;

fn show_create(session: &mut Session, table: &str) -> String {
    match session.run(&format!("show create table {table}")).unwrap() {
        tidb_session::StmtResult::Rows(rows) => match &rows[0][1] {
            tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn fk_actions_round_trip() {
    let mut session = Session::new();
    session.run("create table p (id int primary key)").unwrap();
    session
        .run(
            "create table c (id int primary key, pid int, \
             foreign key (pid) references p(id) on delete cascade on update set null)",
        )
        .unwrap();

    let shown = show_create(&mut session, "c");
    assert!(
        shown.contains("CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `p` (`id`) \
ON DELETE CASCADE ON UPDATE SET NULL"),
        "{shown}"
    );
}
