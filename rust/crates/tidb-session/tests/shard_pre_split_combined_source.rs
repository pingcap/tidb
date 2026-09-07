//! SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS print as ONE version-gated
//! comment in SHOW CREATE: `/*T! SHARD_ROW_ID_BITS=2 PRE_SPLIT_REGIONS=2 */`
//! (Go `ShowCreateTable` shares the single comment; a PK-handle table
//! refuses the option with 8200 instead).

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
fn shard_and_pre_split_share_one_comment() {
    let mut session = Session::new();
    session
        .run("create table t (v bigint) shard_row_id_bits = 2 pre_split_regions = 2")
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("/*T! SHARD_ROW_ID_BITS=2 PRE_SPLIT_REGIONS=2 */"), "{shown}");
}
