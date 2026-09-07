//! Two CREATE options: `AUTO_INCREMENT = 100` seeds the allocator (the
//! first insert lands on 100), and SHARD_ROW_ID_BITS on a table whose PK
//! IS the handle is refused with Go's 8200 (`Unsupported shard_row_id_bits
//! for table with primary key as row id`) — while a non-handle table
//! round-trips the option through SHOW CREATE.

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
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
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
fn auto_inc_option_seeds_the_allocator() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key) auto_increment = 100")
        .unwrap();
    session.run("insert into t () values ()").unwrap();
    assert_eq!(rows(&mut session, "select id from t"), "100");
}

#[test]
fn shard_bits_on_pk_handle_refused_and_on_heap_round_trips() {
    let mut session = Session::new();
    // An int PK IS the handle: SHARD_ROW_ID_BITS is refused (8200).
    let error = session
        .run("create table t (id int primary key) shard_row_id_bits = 2")
        .expect_err("a PK-handle table cannot be sharded");
    assert_eq!(
        error.to_string(),
        "Unsupported shard_row_id_bits for table with primary key as row id",
        "{error}"
    );

    // A heap table round-trips the option through SHOW CREATE.
    session
        .run("create table u (v int) shard_row_id_bits = 2")
        .unwrap();
    let shown = strings(&mut session, "show create table u");
    assert!(shown.contains("SHARD_ROW_ID_BITS=2"), "{shown}");
}
