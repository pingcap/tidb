//! Multi-CHECK SHOW CREATE ordering: multiple CHECK constraints print in
//! declaration order (`chk_b` before `chk_c`).

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

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run(
            "create table t (a int primary key, b int, c int, \
             constraint chk_b check (b > 0), \
             constraint chk_c check (c < 0))",
        )
        .unwrap();
}

#[test]
fn multi_check_constraints_print_in_declaration_order() {
    let mut session = Session::new();
    setup(&mut session);

    let shown = strings(&mut session, "show create table t");
    let b_pos = shown.find("chk_b").expect("chk_b missing");
    let c_pos = shown.find("chk_c").expect("chk_c missing");
    assert!(b_pos < c_pos, "declaration order preserved: {shown}");
}
