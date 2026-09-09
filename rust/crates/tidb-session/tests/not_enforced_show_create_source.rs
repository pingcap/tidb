//! SHOW CREATE for a NOT ENFORCED CHECK: the clause carries the
//! `NOT ENFORCED` marker, matching Go's restore so a dump re-creates the
//! same non-enforcing state.

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
fn not_enforced_marker_round_trips() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run(
            "create table t (a int primary key, b int, \
             constraint chk_b check (b > 0) not enforced)",
        )
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    // TiDB restores the marker as a version-gated comment for MySQL 8.0.16
    // compatibility.
    assert!(
        shown.contains("CONSTRAINT `chk_b` CHECK ((`b` > 0)) /*!80016 NOT ENFORCED */"),
        "{shown}"
    );
}
