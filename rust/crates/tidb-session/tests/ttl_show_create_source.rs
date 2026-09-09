//! The TTL option family round-trips through SHOW CREATE as three separate
//! version-gated markers — `TTL=`, `TTL_ENABLE='ON'|'OFF'`, and
//! `TTL_JOB_INTERVAL='<duration>'` — in declaration order, after the table
//! options. `TTL_ENABLE` takes a quoted string literal (Go's
//! `ddl_table_option_parser.go` expects stringLit ON/OFF).

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
fn ttl_markers_round_trip() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id int primary key, created datetime) \
             ttl = created + interval 3 month ttl_enable = 'ON' ttl_job_interval = '8h'",
        )
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(
        shown.contains("/*T![ttl] TTL=`created` + INTERVAL 3 MONTH */"),
        "{shown}"
    );
    assert!(shown.contains("/*T![ttl] TTL_ENABLE='ON' */"), "{shown}");
    assert!(
        shown.contains("/*T![ttl] TTL_JOB_INTERVAL='8h' */"),
        "{shown}"
    );
}
