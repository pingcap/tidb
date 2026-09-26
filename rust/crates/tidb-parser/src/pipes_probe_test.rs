//! Temporary probe (goal q84): the PARSED shape of `||` chains.
use crate::parse;
use tidb_ast::{Expr, Stmt};

fn shape(expr: &Expr) -> String {
    match expr {
        Expr::Binary(op, l, r) => {
            let op_name = match op {
                tidb_ast::BinaryOp::LogicOr => "||".to_owned(),
                other => format!("{other:?}"),
            };
            format!("({} {} {})", shape(l), op_name, shape(r))
        }
        Expr::String(s) => format!("'{}'", s),
        Expr::Func { name, .. } => format!("{}()", name),
        Expr::Column(c) => format!("col({})", c.join(".")),
        _ => "?".to_string(),
    }
}

#[test]
fn probe_parse_pipes_shape() {
    let stmt = parse(
        "SELECT coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') FROM t",
    )
    .expect("parse");
    let Stmt::Query(query) = &stmt else {
        panic!("not query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not select");
    };
    let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.fields().first() else {
        panic!("not expr field");
    };
    // TiDB's grammar binds `||` LEFT-associatively: a || b || c parses as
    // (a || b) || c. Pin the shape — an earlier right-nested render pointed
    // the q84 investigation at the wrong layer.
    assert_eq!(
        shape(expr),
        "((coalesce() || ', ') || coalesce())"
    );
}
