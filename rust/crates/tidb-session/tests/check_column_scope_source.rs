//! Column-level CHECK scope rules: a column CHECK referencing ANOTHER
//! column is refused with Go's 3813 ("Column check constraint '...' 
//! references other column."), while the equivalent table-level CHECK is
//! accepted and enforces.

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

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
}

#[test]
fn column_scope_check_refuses_and_table_level_enforces() {
    let mut session = Session::new();
    setup(&mut session);

    // Column-level CHECK naming another column: Go 3813.
    let error = session
        .run("create table t (a int check (a > b), b int)")
        .expect_err("a column CHECK cannot reference another column");
    assert!(
        error.to_string().contains("references other column"),
        "{error}"
    );

    // The table-level spelling of the same rule is accepted and enforces.
    session
        .run("create table t2 (a int, b int, constraint pos_b check (a > b))")
        .unwrap();
    let shown = strings(&mut session, "show create table t2").join("\n");
    assert!(shown.contains("pos_b"), "{shown}");
}
