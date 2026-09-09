//! Prefix-index SHOW CREATE round-trip and the no-op ODKU idiom: a UNIQUE
//! prefix index prints its `(4)` in SHOW CREATE, and `INSERT ... ON
//! DUPLICATE KEY UPDATE a = a` over an existing row counts 0.

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
        .run("create table t (a int primary key, s varchar(20), unique index uq (s(4)))")
        .unwrap();
    session.run("insert into t values (1, 'abcdef')").unwrap();
}

#[test]
fn prefix_sub_part_and_no_op_odku() {
    let mut session = Session::new();
    setup(&mut session);

    // SHOW CREATE round-trips the prefix length.
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("(4)"), "{shown}");

    // The no-op ODKU idiom: the duplicate row changes nothing → 0.
    let affected = match session
        .run("insert into t values (1, 'abcdef') on duplicate key update a = a")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 0);
}
