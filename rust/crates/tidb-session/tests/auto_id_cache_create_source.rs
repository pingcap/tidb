//! `CREATE TABLE ... AUTO_ID_CACHE=1`: Go's 1-vs-non-1 guard is ALTER-only,
//! so a CREATE carrying the option builds a single-point allocator — ids
//! hand out 1, 2, 3 with no batch gaps, and SHOW CREATE prints
//! `/*T![auto_id_cache] AUTO_ID_CACHE=1 */`.

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
                    .join("\n===CELL===\n")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

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
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn create_with_auto_id_cache_one_allocates_tightly() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key) auto_id_cache=1")
        .unwrap();

    // The single-point allocator hands out consecutive ids.
    session.run("insert into t () values (), (), ()").unwrap();
    assert_eq!(rows(&mut session, "select id from t order by id"), "1;2;3");

    // SHOW CREATE prints the written option back.
    let shown = strings(&mut session, "show create table t").join("\n");
    assert!(
        shown.contains("AUTO_ID_CACHE=1"),
        "the option round-trips: {shown}"
    );
}
