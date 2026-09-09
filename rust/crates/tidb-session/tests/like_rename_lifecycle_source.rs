//! `CREATE TABLE ... LIKE` copies structure but NOT data, the copy's storage
//! is independent of its source, and `RENAME TABLE` moves the rows to the
//! new name leaving the old name missing (1146).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn like_copies_structure_rename_moves_data() {
    let mut session = Session::new();
    session
        .run("create table base_t (a int primary key, b varchar(4) not null, key kb (b))")
        .unwrap();
    session.run("insert into base_t values (1, 'x'), (2, 'y')").unwrap();

    // LIKE copies the structure but no rows.
    session.run("create table copy_t like base_t").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from copy_t"), "0");

    // The copy's storage is independent.
    session.run("insert into copy_t values (5, 'z')").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from base_t"), "2");

    // RENAME TABLE carries the rows; the old name is gone.
    session.run("rename table copy_t to renamed_t").unwrap();
    assert_eq!(rows(&mut session, "select a from renamed_t order by a"), "5");
    let error = session
        .run("select * from copy_t")
        .expect_err("the old name no longer exists");
    assert!(error.to_string().contains("doesn't exist"), "{error}");
}
