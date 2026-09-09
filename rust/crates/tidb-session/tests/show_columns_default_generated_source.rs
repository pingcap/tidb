//! SHOW COLUMNS marks an expression default with `DEFAULT_GENERATED` in the
//! Extra cell (Go `pkg/table/column.go:456`) and reports the expression TEXT
//! without parentheses in the Default cell — unlike SHOW CREATE, which
//! parenthesizes it (`NewColDesc`, column.go:415-440).

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
fn expression_default_gets_default_generated_extra() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v double default (rand()), w int)")
        .unwrap();

    let rows = strings(&mut session, "show columns from t");

    // The expression-default column: DEFAULT_GENERATED extra, bare text.
    let v = rows.iter().find(|row| row.starts_with("v|")).expect("v row");
    assert!(v.ends_with("|DEFAULT_GENERATED"), "{v}");
    assert!(v.contains("|rand()|"), "{v}");

    // A plain column has neither.
    let w = rows.iter().find(|row| row.starts_with("w|")).expect("w row");
    assert!(!w.contains("DEFAULT_GENERATED"), "{w}");
}
