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

//! SQL-binding parser/restore tests translated from Go's `TestBinding` and
//! the binding-specific differential vectors.

use super::*;

#[test]
fn binding_derived_query_collapses_redundant_whole_query_parentheses() {
    for (sql, expected) in [
        (
            "create global binding for select * from ((select * from t where a = 1)) tt using select * from (select * from t where a = 1) tt",
            "CREATE GLOBAL BINDING FOR SELECT * FROM (SELECT * FROM `t` WHERE `a`=1) AS `tt` USING SELECT * FROM (SELECT * FROM `t` WHERE `a`=1) AS `tt`",
        ),
        (
            "drop global binding for select * from (((select * from t where a = 1))) tt",
            "DROP GLOBAL BINDING FOR SELECT * FROM (SELECT * FROM `t` WHERE `a`=1) AS `tt`",
        ),
        (
            "select * from ((select 1 union select 2)) tt",
            "SELECT * FROM (SELECT 1 UNION SELECT 2) AS `tt`",
        ),
        (
            "select * from ((select 1) union (select 2)) tt",
            "SELECT * FROM ((SELECT 1) UNION (SELECT 2)) AS `tt`",
        ),
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(statement.restore(), expected, "{sql}");
    }
}

/// SQL binding commands share Go's parser/restore contract but deliberately
/// contain typed nested statements rather than captured SQL text. These vectors
/// cover every command family in `pkg/parser/binding_parser.go` and the
/// scoped-SHOW grammar in `pkg/parser/ddl_show_parser.go`.
#[test]
fn binding_command_restore_and_scope() {
    assert_eq!(
        r("create global binding for select * from t using select * from t use index(a)"),
        "CREATE GLOBAL BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"
    );
    assert_eq!(
        r("create binding using select * from t"),
        "CREATE SESSION BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t`"
    );
    assert_eq!(
        r("create session binding from history using plan digest @a, 'digest'"),
        "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST @`a`, 'digest'"
    );
    // Go's hand parser accepts PLAN/DIGEST as optional compatibility tokens
    // but its AST restore always makes both keywords explicit.
    assert_eq!(
        r("create global binding from history using 'digest'"),
        "CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST 'digest'"
    );
    assert_eq!(
        r("drop binding for select * from t"),
        "DROP SESSION BINDING FOR SELECT * FROM `t`"
    );
    assert_eq!(
        r("drop global binding for select * from t using select * from t use index(a)"),
        "DROP GLOBAL BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"
    );
    assert_eq!(
        r("drop session binding for sql digest @a, 'digest'"),
        "DROP SESSION BINDING FOR SQL DIGEST @`a`, 'digest'"
    );
    assert_eq!(
        r("set binding enabled for select * from t using select * from t use index(a)"),
        "SET BINDING ENABLED FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"
    );
    assert_eq!(
        r("set binding disabled for sql digest 'digest'"),
        "SET BINDING DISABLED FOR SQL DIGEST 'digest'"
    );
    assert_eq!(r("show bindings"), "SHOW SESSION BINDINGS");
    assert_eq!(r("show global bindings"), "SHOW GLOBAL BINDINGS");
    assert_eq!(
        r("show session bindings where original_sql = 'select 1'"),
        "SHOW SESSION BINDINGS WHERE `original_sql`=_UTF8MB4'select 1'"
    );

    let stmt = parse("create global binding for select * from t using select * from t")
        .expect("CREATE BINDING parses");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::CreateBinding(binding) = admin.as_ref() else {
        panic!("expected CreateBinding");
    };
    assert!(matches!(
        &binding.source,
        tidb_ast::CreateBindingSource::Statement { target }
            if matches!(target.origin.as_ref(), tidb_ast::Stmt::Query(query)
                if matches!(query.as_ref(), tidb_ast::QueryStmt::Select(_)))
                && matches!(target.hinted.as_deref(), Some(tidb_ast::Stmt::Query(query))
                    if matches!(query.as_ref(), tidb_ast::QueryStmt::Select(_)))
    ));

    // Do not turn unsupported nested SQL or SET's deliberately string-only
    // digest target into raw text accepted by the outer binding grammar.
    assert!(parse("create binding for grant select on t to u using select * from t").is_err());
    assert!(parse("set binding enabled for sql digest @digest").is_err());
    assert!(parse("drop binding for sql digest 1").is_err());
}

/// `pkg/parser/join_parser.go:798-806` gives a table source one deliberately
/// narrow wildcard-schema form (`*.table`), exercised by
/// `pkg/parser/parser_test.go:6066-6069` through CREATE BINDING. It must not
/// widen generic identifier paths.
#[test]
fn binding_wildcard_schema_table_restore() {
    assert_eq!(
        r("create global binding using select * from *.t1"),
        "CREATE GLOBAL BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"
    );
    assert_eq!(
        r("create global binding using select * from *.t1 where t1.a > (select max(a) from t2)"),
        "CREATE GLOBAL BINDING FOR SELECT * FROM `*`.`t1` WHERE `t1`.`a`>(SELECT MAX(`a`) FROM `t2`) USING SELECT * FROM `*`.`t1` WHERE `t1`.`a`>(SELECT MAX(`a`) FROM `t2`)"
    );
    assert_eq!(
        r("create session binding using select * from *.t1"),
        "CREATE SESSION BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"
    );
    assert_eq!(
        r("create binding using select * from *.t1"),
        "CREATE SESSION BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"
    );
    assert!(parse("select * from *").is_err());
}
