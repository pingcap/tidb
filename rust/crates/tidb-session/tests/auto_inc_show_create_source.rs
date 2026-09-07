//! SHOW CREATE prints the MySQL-compatible `AUTO_INCREMENT=<next>` table
//! option once ids have been allocated — the allocator's NEXT value, so a
//! fresh table prints nothing and `ALTER ... AUTO_INCREMENT = 100` shows
//! 100 before the first insert. Go `executor/show.go:1383-1387`.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
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
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn show_create_prints_auto_increment_option() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();

    // Fresh table: the allocator's next value is 1, so nothing prints.
    assert!(
        !strings(&mut session, "show create table t").contains("AUTO_INCREMENT="),
        "fresh table must not print AUTO_INCREMENT"
    );

    session.run("insert into t (v) values (1), (2)").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=3"), "{shown}");

    // The option rides outside the version gate (plain MySQL-compatible text).
    assert!(!shown.contains("/*T![auto_inc"), "{shown}");
}

#[test]
fn alter_auto_increment_rebases_and_shows() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();
    session.run("insert into t (v) values (1)").unwrap();
    session.run("alter table t auto_increment = 100").unwrap();

    // Before the next insert, the next value is exactly 100.
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=100"), "{shown}");

    session.run("insert into t (v) values (2)").unwrap();

    // After the id=100 insert, the allocator's next value is 101.
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=101"), "{shown}");
}
