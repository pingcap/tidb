//! EXPLAIN ANALYZE executes the plan and reports the ANALYZE column set —
//! actRows plus execution-info/memory/disk columns — unlike plain EXPLAIN's
//! estimate-only listing.

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
fn analyze_executes_and_reports_actuals() {
    let mut session = Session::new();
    session.run("create table t (id int primary key)").unwrap();

    let lines = rows(&mut session, "explain analyze select * from t");
    let joined = lines.join("\n");
    assert!(joined.contains("TableReader"), "{joined}");
    assert!(joined.contains("TableFullScan"), "{joined}");

    // The ANALYZE column set: 9 fields (id, estRows, actRows, task, access
    // object, execution info, operator info, memory, disk) versus plain
    // EXPLAIN's 5.
    assert!(lines.iter().all(|line| line.matches('|').count() == 8), "{joined}");

    // Plain EXPLAIN keeps the estimate-only shape.
    let plain = rows(&mut session, "explain select * from t");
    assert!(plain.iter().all(|line| line.matches('|').count() == 4), "{joined}");
}
