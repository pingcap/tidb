//! SHOW CREATE TABLE carries the foreign key definition: the named
//! CONSTRAINT, its REFERENCES clause, and the ON DELETE action — the same
//! text a dump round-trips to re-create the child.

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
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned()
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

fn setup(session: &mut Session) {
    session.run("create table p (a int primary key)").unwrap();
    session
        .run(
            "create table c (x int primary key, pa int, \
             constraint fk_pa foreign key (pa) references p(a) on delete cascade)",
        )
        .unwrap();
}

#[test]
fn show_create_carries_the_fk_definition() {
    let mut session = Session::new();
    setup(&mut session);

    let shown = strings(&mut session, "show create table c");
    assert!(shown.contains("CONSTRAINT `fk_pa` FOREIGN KEY (`pa`) REFERENCES `p` (`a`)"), "{shown}");
    assert!(shown.contains("ON DELETE CASCADE"), "{shown}");
}
