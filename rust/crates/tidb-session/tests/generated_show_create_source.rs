//! SHOW CREATE for generated columns: STORED prints
//! `GENERATED ALWAYS AS (...) STORED` and VIRTUAL prints
//! `GENERATED ALWAYS AS (...) VIRTUAL`.

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
fn stored_and_virtual_generated_show_create() {
    let mut session = Session::new();
    session
        .run("create table st (a int primary key, b int as (a * 3) stored)")
        .unwrap();
    let stored = strings(&mut session, "show create table st");
    assert!(stored.contains("GENERATED ALWAYS AS (`a` * 3) STORED"), "{stored}");

    session
        .run("create table vt (a int primary key, b int as (a * 3) virtual)")
        .unwrap();
    let virt = strings(&mut session, "show create table vt");
    assert!(virt.contains("GENERATED ALWAYS AS (`a` * 3) VIRTUAL"), "{virt}");
}
