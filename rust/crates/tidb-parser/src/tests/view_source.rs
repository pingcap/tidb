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

//! Exact transcreation of `pkg/parser/parser_test.go::TestView`.

use super::*;

fn create_view(statement: &Stmt) -> &tidb_ast::CreateViewStmt {
    let Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL statement");
    };
    let tidb_ast::DdlStmt::CreateView(view) = ddl.as_ref() else {
        panic!("expected CREATE VIEW statement");
    };
    view
}

/// `pkg/parser/parser_test.go::TestView`.
#[test]
fn test_view() {
    // The Go table is five query shapes crossed with the same twelve CREATE
    // VIEW header/check-option variants. Keeping those dimensions explicit
    // makes the full 60-row obligation reviewable without hiding rows behind
    // one representative case.
    let variants = [
        (
            "create view v as ",
            "",
            "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = undefined view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = merge view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = temptable view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = 'root' view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = 'root' sql security definer view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = 'root' sql security invoker view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as ",
            " with local check option",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS ",
            " WITH LOCAL CHECK OPTION",
        ),
        (
            "create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as ",
            " with cascaded check option",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS ",
            "",
        ),
        (
            "create or replace algorithm = merge definer = current_user view v as ",
            "",
            "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS ",
            "",
        ),
    ];
    let query_shapes = [
        ("select * from t", "SELECT * FROM `t`"),
        ("(select * from t)", "(SELECT * FROM `t`)"),
        (
            "select * from t union select * from t",
            "SELECT * FROM `t` UNION SELECT * FROM `t`",
        ),
        (
            "select * from t union all select * from t",
            "SELECT * FROM `t` UNION ALL SELECT * FROM `t`",
        ),
        (
            "(select * from t union all select * from t)",
            "(SELECT * FROM `t` UNION ALL SELECT * FROM `t`)",
        ),
    ];

    let mut rows = 0;
    for (shape_index, (query_sql, query_expected)) in query_shapes.into_iter().enumerate() {
        for (variant_index, (prefix, suffix, expected_prefix, expected_suffix)) in
            variants.into_iter().enumerate()
        {
            // The final row in Go's parenthesized-UNION-ALL group is
            // intentionally a duplicate non-parenthesized source row. Keep
            // that exact source quirk instead of silently regularizing it.
            let (query_sql, query_expected) = if shape_index == 4 && variant_index == 11 {
                query_shapes[3]
            } else {
                (query_sql, query_expected)
            };
            let sql = format!("{prefix}{query_sql}{suffix}");
            let expected = format!("{expected_prefix}{query_expected}{expected_suffix}");
            assert_eq!(r(&sql), expected, "Go TestView row {rows}: {sql}");
            rows += 1;
        }
    }
    assert_eq!(rows, 60, "the complete Go restore table must execute");

    let statement = parse("create view v as select * from t").expect("simple view parses");
    let view = create_view(&statement);
    assert_eq!(view.algorithm, tidb_ast::ViewAlgorithm::UNDEFINED);
    assert_eq!(view.query.text(), b"select * from t");
    assert_eq!(view.security, tidb_ast::ViewSecurity::DEFINER);
    assert_eq!(view.check_option, tidb_ast::ViewCheckOption::CASCADED);

    let sql = "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = root@localhost
                  SQL SECURITY DEFINER
                  VIEW V(a,b,c) AS select c,d,e from t
                  WITH CASCADED CHECK OPTION;";
    let statement = parse(sql).expect("fully specified view parses");
    let view = create_view(&statement);
    assert!(view.or_replace);
    assert_eq!(view.algorithm, tidb_ast::ViewAlgorithm::UNDEFINED);
    assert_eq!(view.definer.user, "root");
    assert_eq!(view.definer.host, "localhost");
    assert_eq!(view.columns, ["a", "b", "c"]);
    assert_eq!(view.query.text(), b"select c,d,e from t");
    assert_eq!(view.security, tidb_ast::ViewSecurity::DEFINER);
    assert_eq!(view.check_option, tidb_ast::ViewCheckOption::CASCADED);

    let statements = parse_multi(
        "\nCREATE VIEW v1 AS SELECT * FROM t;\nCREATE VIEW v2 AS SELECT 123123123123123;\n",
    )
    .expect("multi-statement views parse");
    assert_eq!(statements.len(), 2);
    assert_eq!(create_view(&statements[0]).query.text(), b"SELECT * FROM t");
    assert_eq!(
        create_view(&statements[1]).query.text(),
        b"SELECT 123123123123123"
    );
}
