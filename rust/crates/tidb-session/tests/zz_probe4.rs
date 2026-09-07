use tidb_session::Session;
#[test]
fn probe4() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT PRIMARY KEY, b INT, KEY (b)) PARTITION BY HASH(a) (PARTITION P0, PARTITION p1, PARTITION P2)").unwrap();
    session.run("INSERT INTO t VALUES (1,1),(2,2),(3,3)").unwrap();
    let result = session.run("EXPLAIN SELECT * FROM t WHERE a IN (1, 2)").unwrap();
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
