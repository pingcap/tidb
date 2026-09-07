//! DESCRIBE and SHOW COLUMNS metadata: a VIRTUAL generated column reports
//! "VIRTUAL GENERATED" in its Extra field, and `SHOW COLUMNS FROM db.table`
//! resolves the schema qualifier.

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
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
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
        .run("create table gen_t (a int primary key, b int as (a * 3) virtual)")
        .unwrap();
    session.run("create database other").unwrap();
    session.run("create table other.u (x int primary key)").unwrap();
}

#[test]
fn generated_extra_and_qualified_show() {
    let mut session = Session::new();
    setup(&mut session);

    // DESCRIBE marks the generated column with VIRTUAL GENERATED.
    let desc = strings(&mut session, "desc gen_t");
    assert_eq!(desc.len(), 2);
    assert!(desc[1].contains("VIRTUAL GENERATED"), "{desc:?}");

    // SHOW COLUMNS resolves the schema-qualified target.
    assert_eq!(strings(&mut session, "show columns from other.u"), vec!["x|int|NO|PRI|Null|"]);
}
