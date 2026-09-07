//! `AUTO_RANDOM_BASE = 100` round-trips through SHOW CREATE as the
//! version-gated `/*T![auto_rand_base] AUTO_RANDOM_BASE=100 */` comment.

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
fn auto_random_base_round_trips() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id bigint primary key auto_random(4), v int) \
             auto_random_base = 100",
        )
        .unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_RANDOM_BASE=100"), "{shown}");
}
