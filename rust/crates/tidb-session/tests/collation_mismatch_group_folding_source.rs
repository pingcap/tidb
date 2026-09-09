//! Collation effects on grouping and mixing: GROUP BY on a CI column folds
//! case into one group, and mixing EXPLICIT collations of different
//! charsets/collations in one comparison fails with Go's 1267 ("Illegal
//! mix of collations").

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("create table t (s varchar(8)) charset utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    session.run("insert into t values ('Apple'), ('apple'), ('b')").unwrap();
}

#[test]
fn ci_grouping_and_collation_mix_refusal() {
    let mut session = Session::new();
    setup(&mut session);

    // GROUP BY on a CI column folds 'Apple' and 'apple' into one group.
    assert_eq!(
        rows(&mut session, "select s, count(*) from t group by s order by s"),
        "'Apple'|2;'b'|1"
    );

    // Mixing explicit collations of one charset but different rules: 1267.
    let error = session
        .run("select 'x' collate utf8mb4_bin = 'x' collate utf8mb4_general_ci")
        .expect_err("the collations conflict");
    assert!(
        error
            .to_string()
            .contains("Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation '='"),
        "{error}"
    );
}
