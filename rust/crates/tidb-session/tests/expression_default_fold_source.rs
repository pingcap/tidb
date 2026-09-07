//! Expression defaults follow Go's `getDefaultValue`
//! (`pkg/ddl/add_column.go:814-850`): a NON-function-call expression such
//! as `DEFAULT (1 + 2)` is folded at DDL time via `EvalSimpleAst` with
//! `DefaultIsExpr=false`, so SHOW CREATE prints the settled literal
//! `DEFAULT '3'` — while the whitelisted function forms stay parenthesized
//! (`DEFAULT (rand())`) and CURRENT_DATE prints its marker parenthesized.

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
fn non_function_expression_default_folds() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int default (1 + 2))")
        .unwrap();

    // SHOW CREATE prints the settled literal, not the expression.
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("`v` int DEFAULT '3'"), "{shown}");
    assert!(!shown.contains("DEFAULT ("), "{shown}");

    // Inserts use the folded value.
    session.run("insert into t (id) values (1)").unwrap();
    match session.run("select v from t").unwrap() {
        tidb_session::StmtResult::Rows(rows) => {
            assert_eq!(format!("{:?}", rows[0][0]), "Int(3)");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn whitelisted_function_defaults_stay_parenthesized() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v double default (rand()))")
        .unwrap();
    session
        .run("create table t2 (id int primary key, d date default (curdate()))")
        .unwrap();

    assert!(
        strings(&mut session, "show create table t").contains("DEFAULT (rand())"),
        "{:?}",
        strings(&mut session, "show create table t")
    );
    assert!(
        strings(&mut session, "show create table t2").contains("DEFAULT (CURRENT_DATE)"),
        "{:?}",
        strings(&mut session, "show create table t2")
    );
}
