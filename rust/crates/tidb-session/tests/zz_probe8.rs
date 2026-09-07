use tidb_session::Session;
#[test]
fn probe8() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    let result = session.run("EXPLAIN SELECT a, b FROM t WHERE a > 5 AND b + 1 < 10").unwrap();
    match result {
        tidb_session::StmtResult::Rows(rows) => {
            for row in rows {
                let cells: Vec<String> = row.iter().map(|d| format!("{d:?}")).collect();
                println!("ROW | {}", cells.join(" | "));
            }
        }
        other => println!("OTHER {other:?}"),
    }
}
