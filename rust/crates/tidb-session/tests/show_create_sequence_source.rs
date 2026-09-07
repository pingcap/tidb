//! SHOW CREATE SEQUENCE prints Go's `ConstructResultOfShowCreateSequence`
//! shape (show.go:1575-1599): lowercase keywords, explicit start/min/max/
//! increment/cache/cycle, `ENGINE=InnoDB`, with ascending-sequence defaults
//! filled in (minvalue 1, maxvalue MaxInt64-1).

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
fn sequence_ddl_round_trips() {
    let mut session = Session::new();
    session
        .run("create sequence s start with 100 increment 5 cache 50 cycle")
        .unwrap();

    let shown = strings(&mut session, "show create sequence s");
    assert_eq!(
        shown.lines().nth(1).unwrap(),
        "CREATE SEQUENCE `s` start with 100 minvalue 1 \
         maxvalue 9223372036854775806 increment by 5 cache 50 cycle ENGINE=InnoDB"
    );
}
