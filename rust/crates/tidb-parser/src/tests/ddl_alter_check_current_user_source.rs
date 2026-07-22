// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct source coverage for the bare `CURRENT_USER` expression accepted by
//! Go's `parseCurrentFunc` in `pkg/parser/expr_cast_parser.go`, exercised by
//! the `ALTER TABLE ... ADD CHECK` rows in `ddl/constraint.test`.

use super::*;

#[test]
fn alter_add_check_bare_current_user_restores_as_nullary_function() {
    let sql = "ALTER TABLE t1 ADD CHECK (CURRENT_USER != f4)";
    let statement = parse(sql).expect("Go accepts bare CURRENT_USER in CHECK");
    assert_eq!(
        statement.restore(),
        "ALTER TABLE `t1` ADD CHECK(CURRENT_USER()!=`f4`) ENFORCED"
    );

    let Stmt::Ddl(ddl) = statement else {
        panic!("expected ALTER TABLE DDL");
    };
    let tidb_ast::DdlStmt::AlterTable(alter) = ddl.into_inner() else {
        panic!("expected ALTER TABLE statement");
    };
    let [tidb_ast::AlterTableAction::AddCheck(check)] = alter.actions.as_slice() else {
        panic!("expected one ALTER TABLE ADD CHECK action");
    };
    assert!(check.enforced);
    assert!(matches!(
        &check.expression,
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::Ne,
            left,
            right,
        ) if matches!(
            left.as_ref(),
            tidb_ast::Expr::Func { name, args }
                if name == "CURRENT_USER" && args.is_empty()
        ) && matches!(right.as_ref(), tidb_ast::Expr::Column(path) if path == &["f4".to_owned()])
    ));
}

#[test]
fn current_user_bare_and_parenthesized_forms_share_go_restore() {
    assert_eq!(r("SELECT CURRENT_USER"), "SELECT CURRENT_USER()");
    assert_eq!(r("SELECT CURRENT_USER()"), "SELECT CURRENT_USER()");
}
