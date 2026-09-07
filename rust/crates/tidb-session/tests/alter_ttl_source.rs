//! ALTER TABLE's TTL forms (`executor.go:1934-1952`, `executor.go:3905`):
//! `TTL_ENABLE`/`TTL_JOB_INTERVAL` flip the config's fields, a full `TTL=`
//! re-definition inherits unset fields, and `REMOVE TTL` clears the config.
//! Enable-only on a table without a config refuses with 8150.

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
        .run(
            "create table t (id int primary key, created datetime) \
             ttl = created + interval 3 month ttl_enable = 'OFF' ttl_job_interval = '8h'",
        )
        .unwrap();
}

#[test]
fn ttl_enable_and_interval_flip_the_config() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("alter table t ttl_enable = 'ON'").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("TTL_ENABLE='ON'"), "{shown}");
    // The untouched interval survives.
    assert!(shown.contains("TTL_JOB_INTERVAL='8h'"), "{shown}");

    session.run("alter table t ttl_job_interval = '12h'").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("TTL_JOB_INTERVAL='12h'"), "{shown}");
    assert!(shown.contains("TTL_ENABLE='ON'"), "{shown}");
}

#[test]
fn ttl_redefinition_inherits_and_remove_clears() {
    let mut session = Session::new();
    setup(&mut session); // enable OFF, interval 8h

    // A full re-definition without TTL_ENABLE keeps OFF; interval stays 8h.
    session
        .run("alter table t ttl = created + interval 6 month")
        .unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("TTL=`created` + INTERVAL 6 MONTH"), "{shown}");
    assert!(shown.contains("TTL_ENABLE='OFF'"), "{shown}");
    assert!(shown.contains("TTL_JOB_INTERVAL='8h'"), "{shown}");

    // REMOVE TTL clears everything.
    session.run("alter table t remove ttl").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(!shown.contains("TTL"), "{shown}");

    // REMOVE TTL on a table without a config is a no-op (Go submits only
    // when one exists).
    session.run("alter table t remove ttl").unwrap();
}

#[test]
fn enable_only_on_non_ttl_table_refuses() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, created datetime)")
        .unwrap();

    let error = session
        .run("alter table t ttl_enable = 'ON'")
        .expect_err("8150")
        .to_string();
    assert_eq!(error, "Cannot set TTL_ENABLE on a table without TTL config");

    let error = session
        .run("alter table t ttl_job_interval = '12h'")
        .expect_err("8150")
        .to_string();
    assert_eq!(
        error,
        "Cannot set TTL_JOB_INTERVAL on a table without TTL config"
    );
}
