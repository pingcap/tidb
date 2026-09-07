//! SHOW CREATE VIEW prints Go's full header —
//! `CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW` —
//! with the column list and a schema-qualified, alias-restored SELECT body
//! (`executor/show.go` `ShowCreateView`).

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
fn view_header_round_trips() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    session
        .run("create view v as select a, b from t where a > 0")
        .unwrap();

    let shown = strings(&mut session, "show create view v");
    assert!(
        shown.contains(
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v` (`a`, `b`) AS"
        ),
        "{shown}"
    );
    assert!(
        shown.contains("SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t` WHERE `a`>0"),
        "{shown}"
    );
}
