//! RENAME COLUMN on a partitioned table: the data carries, the old name
//! goes missing, and SHOW CREATE TABLE keeps the renamed column AND the
//! full PARTITION BY clause (both partition definitions).

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n===CELL===\n")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn rows(session: &mut Session, sql: &str) -> String {
    strings(session, sql).join("\n")
}

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (a int primary key, b int) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1, 5), (11, 15)").unwrap();
}

#[test]
fn rename_column_preserves_partitioned_show_create() {
    let mut session = Session::new();
    setup(&mut session);

    // RENAME COLUMN carries the data; the old name is unknown.
    session.run("alter table t rename column b to bb").unwrap();
    assert!(rows(&mut session, "select a, bb from t order by a").contains("5"));
    let error = session
        .run("select b from t")
        .expect_err("the old name is gone");
    assert!(error.to_string().contains("Unknown column 'b'"), "{error}");

    // SHOW CREATE keeps the renamed column AND the partition clause.
    let shown = strings(&mut session, "show create table t");
    let ddl = &shown[0];
    assert!(ddl.contains("`bb` int DEFAULT NULL"), "{ddl}");
    assert!(ddl.contains("PARTITION BY RANGE (`a`)"), "{ddl}");
    assert!(ddl.contains("PARTITION `p0` VALUES LESS THAN (10)"), "{ddl}");
    assert!(ddl.contains("PARTITION `p1` VALUES LESS THAN (20)"), "{ddl}");
}
