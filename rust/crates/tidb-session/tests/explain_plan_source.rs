//! EXPLAIN renders the physical plan: a full-scan query shows the
//! TableReader over TableFullScan with keep-order/stats fields, and a
//! primary-key equality lowers to Point_Get with the handle bound.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
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
fn explain_renders_the_plan_tree() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int)")
        .unwrap();

    // Full scan: reader over the scan operator.
    let lines = rows(&mut session, "explain select * from t");
    let joined = lines.join("\n");
    assert!(joined.contains("TableReader"), "{joined}");
    assert!(joined.contains("TableFullScan"), "{joined}");
    assert!(joined.contains("keep order:false"), "{joined}");

    // PK equality lowers to a point lookup.
    let lines = rows(
        &mut session,
        "explain format = 'brief' select * from t where id = 1",
    );
    let joined = lines.join("\n");
    assert!(joined.contains("Point_Get"), "{joined}");
    assert!(joined.contains("handle:1"), "{joined}");
}
