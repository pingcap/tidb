//! Idempotent DDL: `DROP DATABASE IF EXISTS` on a missing database is a
//! no-op, and `ALTER TABLE ADD COLUMN IF NOT EXISTS` adds the column once
//! and no-ops on the second run.

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
fn idempotent_ddl_forms() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();

    // DROP DATABASE IF EXISTS on a missing database: no-op.
    session.run("drop database if exists nope_db").unwrap();

    // ADD COLUMN IF NOT EXISTS adds once and no-ops after.
    session.run("alter table t add column if not exists c int").unwrap();
    session.run("alter table t add column if not exists c int").unwrap();
    assert_eq!(strings(&mut session, "desc t").len(), 2, "exactly one c");
}
