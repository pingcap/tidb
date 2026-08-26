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

//! Ports of `pkg/parser/ast/dml_test.go` (origin/master), part one of the
//! `pkg/parser/ast` batching: every function from `TestDMLVisitorCover`
//! through `TestLoadDataRestore`. The visitor row lives in the in-crate
//! `tests_dml_package_source` module.

use tidb_ast::{DdlStmt, Expr, JoinNode, QueryStmt, RestoreContext, Stmt};

use crate::parser_ast_node_restore_source::{
    case, run_node_restore_test, run_node_restore_test_with_flags_stmt_change,
};

fn select_of(stmt: &Stmt) -> &tidb_ast::SelectStmt {
    match stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            other => panic!("expected a SELECT statement, got {other:?}"),
        },
        other => panic!("expected a query statement, got {other:?}"),
    }
}

fn create_table_of(stmt: &Stmt) -> &tidb_ast::CreateTableStmt {
    match stmt {
        Stmt::Ddl(ddl) => match &**ddl {
            DdlStmt::CreateTable(create) => create,
            other => panic!("expected CREATE TABLE, got {other:?}"),
        },
        other => panic!("expected a DDL statement, got {other:?}"),
    }
}

fn select_from(stmt: &Stmt) -> &tidb_ast::Join {
    select_of(stmt)
        .from
        .as_ref()
        .unwrap_or_else(|| panic!("expected a FROM clause"))
}

fn window_over(stmt: &Stmt) -> &tidb_ast::WindowOver {
    match select_of(stmt).fields.fields().first() {
        Some(tidb_ast::SelectField::Expr {
            expr: Expr::Window { over, .. },
            ..
        }) => over,
        other => panic!("expected a window function call, got {other:?}"),
    }
}

/// `pkg/parser/ast/dml_test.go::TestTableNameRestore`.
#[test]
fn table_name_restore() {
    run_node_restore_test(
        "CREATE TABLE %s (id VARCHAR(128) NOT NULL);",
        &[
            case("dbb.`tbb1`", "`dbb`.`tbb1`"),
            case("`tbb2`", "`tbb2`"),
            case("tbb3", "`tbb3`"),
            case("dbb.`hello-world`", "`dbb`.`hello-world`"),
            case("`dbb`.`hello-world`", "`dbb`.`hello-world`"),
            case("`dbb.HelloWorld`", "`dbb.HelloWorld`"),
        ],
        |stmt, _context| {
            let name = &create_table_of(stmt).name;
            let mut out = String::new();
            tidb_ast::push_name_path(&mut out, name);
            out
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestTableNameIndexHintsRestore`.
#[test]
fn table_name_index_hints_restore() {
    run_node_restore_test(
        "SELECT * FROM %s",
        &[
            case("t use index (hello)", "`t` USE INDEX (`hello`)"),
            case(
                "t use index (hello, world)",
                "`t` USE INDEX (`hello`, `world`)",
            ),
            case("t use index ()", "`t` USE INDEX ()"),
            case("t use key ()", "`t` USE INDEX ()"),
            case("t ignore key ()", "`t` IGNORE INDEX ()"),
            case("t force key ()", "`t` FORCE INDEX ()"),
            case(
                "t use index for order by (idx1)",
                "`t` USE INDEX FOR ORDER BY (`idx1`)",
            ),
            case(
                "t use index (hello, world, yes) force key (good)",
                "`t` USE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)",
            ),
            case(
                "t use index (hello, world, yes) use index for order by (good)",
                "`t` USE INDEX (`hello`, `world`, `yes`) USE INDEX FOR ORDER BY (`good`)",
            ),
            case(
                "t ignore key (hello, world, yes) force key (good)",
                "`t` IGNORE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)",
            ),
            case(
                "t use index for group by (idx1) use index for order by (idx2)",
                "`t` USE INDEX FOR GROUP BY (`idx1`) USE INDEX FOR ORDER BY (`idx2`)",
            ),
            case(
                "t use index for group by (idx1) ignore key for order by (idx2)",
                "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR ORDER BY (`idx2`)",
            ),
            case(
                "t use index for group by (idx1) ignore key for group by (idx2)",
                "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)",
            ),
            case(
                "t use index for order by (idx1) ignore key for group by (idx2)",
                "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)",
            ),
            case(
                "t use index for order by (idx1) ignore key for group by (idx2) use index (idx3)",
                "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)",
            ),
            case(
                "t use index (`foo``bar`) force index (`baz``1`, `xyz`)",
                "`t` USE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)",
            ),
            case(
                "t force index (`foo``bar`) ignore index (`baz``1`, xyz)",
                "`t` FORCE INDEX (`foo``bar`) IGNORE INDEX (`baz``1`, `xyz`)",
            ),
            case(
                "t ignore index (`foo``bar`) force key (`baz``1`, xyz)",
                "`t` IGNORE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)",
            ),
            case(
                "t ignore index (`foo``bar`) ignore key for group by (`baz``1`, xyz)",
                "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)",
            ),
            case(
                "t ignore index (`foo``bar`) ignore key for order by (`baz``1`, xyz)",
                "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)",
            ),
            case(
                "t use index for group by (`foo``bar`) use index for order by (`baz``1`, `xyz`)",
                "`t` USE INDEX FOR GROUP BY (`foo``bar`) USE INDEX FOR ORDER BY (`baz``1`, `xyz`)",
            ),
            case(
                "t use index for group by (`foo``bar`) ignore key for order by (`baz``1`, `xyz`)",
                "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)",
            ),
            case(
                "t use index for group by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)",
                "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)",
            ),
            case(
                "t use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)",
                "`t` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)",
            ),
            case(
                "t tt use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)",
                "`t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)",
            ),
            case(
                "t as tt use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)",
                "`t` AS `tt` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)",
            ),
        ],
        |stmt, context| {
            let JoinNode::Table(table_ref) = &select_from(stmt).left else {
                panic!("expected a plain table reference on the left");
            };
            table_ref.restore_with_context(context)
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestLimitRestore`.
#[test]
fn limit_restore() {
    run_node_restore_test(
        "SELECT 1 %s",
        &[
            case("limit 10", "LIMIT 10"),
            case("limit 10,20", "LIMIT 10,20"),
            // Go's AST stores offset+count even for OFFSET syntax.
            case("limit 20 offset 10", "LIMIT 10,20"),
        ],
        |stmt, _context| {
            let limit = select_of(stmt)
                .limit
                .as_ref()
                .unwrap_or_else(|| panic!("expected a LIMIT clause"));
            limit.restore()
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestWildCardFieldRestore`.
#[test]
fn wild_card_field_restore() {
    run_node_restore_test(
        "SELECT %s",
        &[
            case("*", "*"),
            case("t.*", "`t`.*"),
            case("testdb.t.*", "`testdb`.`t`.*"),
        ],
        field_list_fragment,
    );
}

/// Go restores a single `SelectField` node through the same rendering its
/// whole list uses with `, ` separators; every pinned case projects exactly
/// one field through the template, so the list boundary is equivalent.
fn field_list_fragment(stmt: &Stmt, _context: &tidb_ast::RestoreContext) -> String {
    select_of(stmt).restore_field_list()
}

/// `pkg/parser/ast/dml_test.go::TestSelectFieldRestore`.
#[test]
fn select_field_restore() {
    run_node_restore_test(
        "SELECT %s",
        &[
            case("*", "*"),
            case("t.*", "`t`.*"),
            case("testdb.t.*", "`testdb`.`t`.*"),
            case("col as a", "`col` AS `a`"),
            case("col + 1 a", "`col`+1 AS `a`"),
        ],
        field_list_fragment,
    );
}

/// `pkg/parser/ast/dml_test.go::TestFieldListRestore`.
///
/// Two comma separators exist in Go: `FieldList.Restore` joins with ", "
/// while `SelectStmt.Restore` joins with a bare ",". This node-boundary
/// test pins the former exactly like Go's harness does.
#[test]
fn field_list_restore() {
    run_node_restore_test(
        "SELECT %s",
        &[
            case("*", "*"),
            case("t.*", "`t`.*"),
            case("testdb.t.*", "`testdb`.`t`.*"),
            case("col as a", "`col` AS `a`"),
            case("`t`.*, s.col as a", "`t`.*, `s`.`col` AS `a`"),
        ],
        field_list_fragment,
    );
}

/// `pkg/parser/ast/dml_test.go::TestTableSourceRestore`.
#[test]
fn table_source_restore() {
    run_node_restore_test(
        "select * from %s",
        &[
            case("tbl", "`tbl`"),
            case("tbl as t", "`tbl` AS `t`"),
            case("(select * from tbl) as t", "(SELECT * FROM `tbl`) AS `t`"),
            case(
                "(select * from a union select * from b) as t",
                "(SELECT * FROM `a` UNION SELECT * FROM `b`) AS `t`",
            ),
        ],
        |stmt, context| left_operand(stmt).restore_with_context(context),
    );
}

fn left_operand(stmt: &Stmt) -> &JoinNode {
    &select_from(stmt).left
}

/// `pkg/parser/ast/dml_test.go::TestOnConditionRestore`.
#[test]
fn on_condition_restore() {
    run_node_restore_test(
        "select * from t1 join t2 %s",
        &[
            case("on t1.a=t2.a", "ON `t1`.`a`=`t2`.`a`"),
            case(
                "on t1.a=t2.a and t1.b=t2.b",
                "ON `t1`.`a`=`t2`.`a` AND `t1`.`b`=`t2`.`b`",
            ),
        ],
        |stmt, _context| {
            let on = select_from(stmt)
                .on
                .as_ref()
                .unwrap_or_else(|| panic!("expected an ON condition"));
            format!("ON {}", on.restore())
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestJoinRestore`.
#[test]
fn join_restore() {
    run_node_restore_test(
        "select * from %s",
        &[
            case("t1 natural join t2", "`t1` NATURAL JOIN `t2`"),
            case("t1 natural left join t2", "`t1` NATURAL LEFT JOIN `t2`"),
            case(
                "t1 natural right outer join t2",
                "`t1` NATURAL RIGHT JOIN `t2`",
            ),
            case("t1 straight_join t2", "`t1` STRAIGHT_JOIN `t2`"),
            case(
                "t1 straight_join t2 on t1.a>t2.a",
                "`t1` STRAIGHT_JOIN `t2` ON `t1`.`a`>`t2`.`a`",
            ),
            case("t1 cross join t2", "`t1` JOIN `t2`"),
            case(
                "t1 cross join t2 on t1.a>t2.a",
                "`t1` JOIN `t2` ON `t1`.`a`>`t2`.`a`",
            ),
            case("t1 inner join t2 using (b)", "`t1` JOIN `t2` USING (`b`)"),
            case(
                "t1 join t2 using (b,c) left join t3 on t1.a>t3.a",
                "(`t1` JOIN `t2` USING (`b`,`c`)) LEFT JOIN `t3` ON `t1`.`a`>`t3`.`a`",
            ),
            case(
                "t1 natural join t2 right outer join t3 using (b,c)",
                "(`t1` NATURAL JOIN `t2`) RIGHT JOIN `t3` USING (`b`,`c`)",
            ),
            case("t1, t2", "(`t1`) JOIN `t2`"),
            case("t1, t2, t3", "((`t1`) JOIN `t2`) JOIN `t3`"),
            case(
                "(select * from t) t1, (t2, t3)",
                "(SELECT * FROM `t`) AS `t1`, ((`t2`) JOIN `t3`)",
            ),
            case(
                "(select * from t) t1, t2",
                "(SELECT * FROM `t`) AS `t1`, `t2`",
            ),
            case(
                "(select * from (select a from t1) tb1) tb;",
                "(SELECT * FROM (SELECT `a` FROM `t1`) AS `tb1`) AS `tb`",
            ),
            case(
                "(select * from t) t1 cross join t2",
                "(SELECT * FROM `t`) AS `t1` JOIN `t2`",
            ),
            case(
                "(select * from t) t1 natural join t2",
                "(SELECT * FROM `t`) AS `t1` NATURAL JOIN `t2`",
            ),
            case(
                "(select * from t) t1 cross join t2 on t1.a>t2.a",
                "(SELECT * FROM `t`) AS `t1` JOIN `t2` ON `t1`.`a`>`t2`.`a`",
            ),
            case(
                "(select * from t union select * from t1) tb1, t2;",
                "(SELECT * FROM `t` UNION SELECT * FROM `t1`) AS `tb1`, `t2`",
            ),
            case(
                "(select a from t) t1 join t t2, t3;",
                "((SELECT `a` FROM `t`) AS `t1` JOIN `t` AS `t2`) JOIN `t3`",
            ),
        ],
        |stmt, context| select_from(stmt).restore_with_context(context),
    );

    // The parenthesization of mixed comma joins legitimately changes the
    // tree shape on restore; Go pins those through the StmtChange harness.
    run_node_restore_test_with_flags_stmt_change(
        "select * from %s",
        &[
            case(
                "(a al left join b bl on al.a1 > bl.b1) join (a ar right join b br on ar.a1 > br.b1)",
                "(`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)",
            ),
            case(
                "a al left join b bl on al.a1 > bl.b1, a ar right join b br on ar.a1 > br.b1",
                "(`a` AS `al` LEFT JOIN `b` AS `bl` ON `al`.`a1`>`bl`.`b1`) JOIN (`a` AS `ar` RIGHT JOIN `b` AS `br` ON `ar`.`a1`>`br`.`b1`)",
            ),
            case(
                "t1 join (t2 right join t3 on t2.a > t3.a join (t4 right join t5 on t4.a > t5.a))",
                "`t1` JOIN ((`t2` RIGHT JOIN `t3` ON `t2`.`a`>`t3`.`a`) JOIN (`t4` RIGHT JOIN `t5` ON `t4`.`a`>`t5`.`a`))",
            ),
            case(
                "t1 join t2 right join t3 on t2.a=t3.a",
                "(`t1` JOIN `t2`) RIGHT JOIN `t3` ON `t2`.`a`=`t3`.`a`",
            ),
            case(
                "t1 join (t2 right join t3 on t2.a=t3.a)",
                "`t1` JOIN (`t2` RIGHT JOIN `t3` ON `t2`.`a`=`t3`.`a`)",
            ),
        ],
        tidb_ast::RestoreFlags::DEFAULT,
        |stmt, context| select_from(stmt).restore_with_context(context),
    );
}

/// `pkg/parser/ast/dml_test.go::TestTableRefsClauseRestore`.
#[test]
fn table_refs_clause_restore() {
    run_node_restore_test(
        "select * from %s",
        &[
            case("t", "`t`"),
            case("t1 join t2", "`t1` JOIN `t2`"),
            case("t1, t2", "(`t1`) JOIN `t2`"),
        ],
        |stmt, context| select_from(stmt).restore_with_context(context),
    );
}

/// `pkg/parser/ast/dml_test.go::TestDeleteTableListRestore`.
#[test]
fn delete_table_list_restore() {
    for template in ["DELETE %s FROM t1, t2;", "DELETE FROM %s USING t1, t2;"] {
        run_node_restore_test(
            template,
            &[case("t1,t2", "`t1`,`t2`")],
            |stmt, _context| match stmt {
                Stmt::Dml(ddl) => match &**ddl {
                    tidb_ast::DmlStmt::Delete(delete) => match &delete.kind {
                        tidb_ast::DeleteKind::Multi { targets, .. } => {
                            let mut out = String::new();
                            for (index, target) in targets.iter().enumerate() {
                                if index > 0 {
                                    out.push(',');
                                }
                                tidb_ast::push_name_path(&mut out, target);
                            }
                            out
                        }
                        other => panic!("expected a multi-table delete, got {other:?}"),
                    },
                    other => panic!("expected DELETE, got {other:?}"),
                },
                other => panic!("expected a DML statement, got {other:?}"),
            },
        );
    }
}

/// `pkg/parser/ast/dml_test.go::TestDeleteTableIndexHintRestore`.
#[test]
fn delete_table_index_hint_restore() {
    run_node_restore_test(
        "%s",
        &[
            case(
                "DELETE FROM t1 USE key (`fld1`) WHERE fld=1",
                "DELETE FROM `t1` USE INDEX (`fld1`) WHERE `fld`=1",
            ),
            case(
                "DELETE FROM t1 as tbl USE key (`fld1`) WHERE tbl.fld=2",
                "DELETE FROM `t1` AS `tbl` USE INDEX (`fld1`) WHERE `tbl`.`fld`=2",
            ),
        ],
        |stmt, context| stmt.restore_with_context(context),
    );
}

/// `pkg/parser/ast/dml_test.go::TestByItemRestore`.
#[test]
fn by_item_restore() {
    run_node_restore_test(
        "select * from t order by %s",
        &[
            case("a", "`a`"),
            case("a desc", "`a` DESC"),
            case("NULL", "NULL"),
        ],
        |stmt, _context| {
            let item = &select_of(stmt).order_by[0];
            item.restore()
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestGroupByClauseRestore`.
///
/// Rust owns no standalone `GroupByClause` node — the items and the
/// `WITH ROLLUP` flag live on the SELECT — so the clause assembly is
/// reconstructed here over the production item rendering. None of the
/// pinned rows involve the bare boolean-literal positional quirk.
#[test]
fn group_by_clause_restore() {
    run_node_restore_test(
        "select * from t %s",
        &[
            case("GROUP BY a,b desc", "GROUP BY `a`,`b` DESC"),
            case("GROUP BY 1 desc,b", "GROUP BY 1 DESC,`b`"),
        ],
        |stmt, _context| {
            let select = select_of(stmt);
            assert!(!select.group_by.is_empty());
            let mut out = String::from("GROUP BY ");
            for (index, item) in select.group_by.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&item.expr.restore());
                if item.desc == Some(true) {
                    out.push_str(" DESC");
                }
            }
            if select.rollup {
                out.push_str(" WITH ROLLUP");
            }
            out
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestOrderByClauseRestore`.
#[test]
fn order_by_clause_restore() {
    let cases = [
        case("ORDER BY a", "ORDER BY `a`"),
        case("ORDER BY a,b", "ORDER BY `a`,`b`"),
    ];
    run_node_restore_test("SELECT 1 FROM t1 %s", &cases, order_by_fragment);
    // Go extracts `SetOprStmt.OrderBy` through the same rendering for the
    // set-operation form.
    run_node_restore_test(
        "SELECT 1 FROM t1 UNION SELECT 2 FROM t2 %s",
        &cases,
        |stmt, _context| {
            let Stmt::Query(query) = stmt else {
                panic!("expected a query statement");
            };
            let QueryStmt::SetOpr(set_opr) = &**query else {
                panic!("expected a set operation");
            };
            order_by_items(&set_opr.order_by)
        },
    );
}

fn order_by_fragment(stmt: &Stmt, _context: &tidb_ast::RestoreContext) -> String {
    order_by_items(&select_of(stmt).order_by)
}

fn order_by_items(items: &[tidb_ast::OrderItem]) -> String {
    let mut out = String::from("ORDER BY ");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&item.restore());
    }
    out
}

/// `pkg/parser/ast/dml_test.go::TestAssignmentRestore`.
#[test]
fn assignment_restore() {
    run_node_restore_test(
        "UPDATE t1 SET %s",
        &[case("a=1", "`a`=1"), case("b=1+2", "`b`=1+2")],
        |stmt, _context| match stmt {
            Stmt::Dml(dml) => match &**dml {
                tidb_ast::DmlStmt::Update(update) => update.assignments[0].restore(),
                other => panic!("expected UPDATE, got {other:?}"),
            },
            other => panic!("expected a DML statement, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestHavingClauseRestore`.
///
/// Rust stores HAVING as a plain expression, so the clause keyword is
/// assembled here while the expression itself goes through the production
/// restore path.
#[test]
fn having_clause_restore() {
    run_node_restore_test(
        "select 1 from t1 group by 1 %s",
        &[
            case("HAVING a", "HAVING `a`"),
            case("HAVING NULL", "HAVING NULL"),
            case("HAVING a>b", "HAVING `a`>`b`"),
        ],
        |stmt, _context| {
            let having = select_of(stmt)
                .having
                .as_ref()
                .unwrap_or_else(|| panic!("expected a HAVING clause"));
            format!("HAVING {}", having.restore())
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestFrameBoundRestore`.
#[test]
fn frame_bound_restore() {
    run_node_restore_test(
        "select avg(val) over (rows between %s and current row) from t",
        &[
            case("CURRENT ROW", "CURRENT ROW"),
            case("UNBOUNDED PRECEDING", "UNBOUNDED PRECEDING"),
            case("1 PRECEDING", "1 PRECEDING"),
            case("? PRECEDING", "? PRECEDING"),
            case("INTERVAL 5 DAY PRECEDING", "INTERVAL 5 DAY PRECEDING"),
            case("UNBOUNDED FOLLOWING", "UNBOUNDED FOLLOWING"),
            case("1 FOLLOWING", "1 FOLLOWING"),
            case("? FOLLOWING", "? FOLLOWING"),
            case(
                "INTERVAL '2:30' MINUTE_SECOND FOLLOWING",
                "INTERVAL _UTF8MB4'2:30' MINUTE_SECOND FOLLOWING",
            ),
        ],
        |stmt, _context| match window_over(stmt) {
            tidb_ast::WindowOver::Def(def) => match &def.spec.frame {
                Some(frame) => frame.start.restore(),
                None => panic!("expected a window frame"),
            },
            other => panic!("expected an inline OVER spec, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestFrameClauseRestore`.
#[test]
fn frame_clause_restore() {
    run_node_restore_test(
        "select avg(val) over (%s) from t",
        &[
            case(
                "ROWS CURRENT ROW",
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW",
            ),
            case(
                "ROWS UNBOUNDED PRECEDING",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            ),
            case(
                "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
                "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            ),
            case(
                "RANGE BETWEEN ? PRECEDING AND ? FOLLOWING",
                "RANGE BETWEEN ? PRECEDING AND ? FOLLOWING",
            ),
            case(
                "RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING",
                "RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL _UTF8MB4'2:30' MINUTE_SECOND FOLLOWING",
            ),
        ],
        |stmt, _context| match window_over(stmt) {
            tidb_ast::WindowOver::Def(def) => def
                .spec
                .frame
                .as_ref()
                .unwrap_or_else(|| panic!("expected a window frame"))
                .restore(),
            other => panic!("expected an inline OVER spec, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestPartitionByClauseRestore`.
///
/// The `PARTITION BY` list lives inside the window specification; the clause
/// keyword plus its `", "` joins are assembled here over production
/// expression rendering, matching Go's own node boundary.
#[test]
fn partition_by_clause_restore() {
    run_node_restore_test(
        "select avg(val) over (%s rows current row) from t",
        &[
            case("PARTITION BY a", "PARTITION BY `a`"),
            case("PARTITION BY NULL", "PARTITION BY NULL"),
            case("PARTITION BY a, b", "PARTITION BY `a`, `b`"),
        ],
        |stmt, _context| match window_over(stmt) {
            tidb_ast::WindowOver::Def(def) => {
                assert!(!def.spec.partition_by.is_empty());
                let mut out = String::from("PARTITION BY ");
                for (index, expr) in def.spec.partition_by.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&expr.restore());
                }
                out
            }
            other => panic!("expected an inline OVER spec, got {other:?}"),
        },
    );
}

/// `pkg/parser/ast/dml_test.go::TestWindowSpecRestore`.
#[test]
fn window_spec_restore() {
    // Named-window definitions restore name + AS + the definition body.
    run_node_restore_test(
        "select rank() over w from t window %s",
        &[
            case("w as ()", "`w` AS ()"),
            case("w as (w1)", "`w` AS (`w1`)"),
            case(
                "w as (w1 order by country)",
                "`w` AS (`w1` ORDER BY `country`)",
            ),
            case(
                "w as (partition by a order by b rows current row)",
                "`w` AS (PARTITION BY `a` ORDER BY `b` ROWS BETWEEN CURRENT ROW AND CURRENT ROW)",
            ),
        ],
        |stmt, _context| {
            let select = select_of(stmt);
            let (name, def) = &select.windows[0];
            let body = def.restore_body();
            format!("{} AS ({})", tidb_ast::back_quote(name), body)
        },
    );

    // An OVER payload: bare name or parenthesized (possibly base-extending)
    // definition.
    run_node_restore_test(
        "select rank() over %s from t window w as (order by a)",
        &[
            case("w", "`w`"),
            case("()", "()"),
            case("(w)", "(`w`)"),
            case("(w PARTITION BY country)", "(`w` PARTITION BY `country`)"),
            case(
                "(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
                "(PARTITION BY `a` ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            ),
        ],
        |stmt, _context| window_over(stmt).restore(),
    );
}
