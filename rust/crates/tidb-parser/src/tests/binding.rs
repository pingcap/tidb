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

    // Binding parsing uses the ordinary statement parser. Semantic binding
    // eligibility is checked later by preprocessing, exactly as in Go.
    assert_eq!(
        r("drop binding for grant select on t to u"),
        "DROP SESSION BINDING FOR GRANT SELECT ON `t` TO `u`@`%`"
    );
    assert_eq!(
        r("set binding enabled for grant select on t to u"),
        "SET BINDING ENABLED FOR GRANT SELECT ON `t` TO `u`@`%`"
    );
    assert!(parse("set binding enabled for sql digest @digest").is_err());
    assert!(parse("drop binding for sql digest 1").is_err());
}

#[test]
fn binding_zero_value_and_list_boundaries_match_go_source() {
    assert_eq!(
        r("create binding"),
        "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST "
    );
    assert_eq!(
        r("create binding from history using"),
        "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST "
    );
    assert_eq!(
        r("create binding from history using plan digest 'x',"),
        "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST 'x'"
    );
    assert_eq!(r("drop binding"), "DROP SESSION BINDING FOR SQL DIGEST ");
    assert_eq!(
        r("drop binding for sql ignored"),
        "DROP SESSION BINDING FOR SQL DIGEST "
    );
    assert_eq!(
        r("drop binding for sql digest 'x',"),
        "DROP SESSION BINDING FOR SQL DIGEST 'x'"
    );
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

/// Exact source vectors from `pkg/parser/parser_test.go::TestBinding`.
#[test]
fn test_binding_source_of_truth() {
    for (sql, restored) in [
        ("create global binding for select * from t using select * from t use index(a)", "CREATE GLOBAL BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("create session binding for select * from t using select * from t use index(a)", "CREATE SESSION BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("drop global binding for select * from t", "DROP GLOBAL BINDING FOR SELECT * FROM `t`"),
        ("drop session binding for select * from t", "DROP SESSION BINDING FOR SELECT * FROM `t`"),
        ("drop global binding for select * from t using select * from t use index(a)", "DROP GLOBAL BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("drop session binding for select * from t using select * from t use index(a)", "DROP SESSION BINDING FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("show global bindings", "SHOW GLOBAL BINDINGS"),
        ("show session bindings", "SHOW SESSION BINDINGS"),
        ("set binding enabled for select * from t", "SET BINDING ENABLED FOR SELECT * FROM `t`"),
        ("set binding enabled for select * from t using select * from t use index(a)", "SET BINDING ENABLED FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("set binding disabled for select * from t", "SET BINDING DISABLED FOR SELECT * FROM `t`"),
        ("set binding disabled for select * from t using select * from t use index(a)", "SET BINDING DISABLED FOR SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`)"),
        ("create global binding for select * from t union all select * from t using select * from t use index(a) union all select * from t use index(a)", "CREATE GLOBAL BINDING FOR SELECT * FROM `t` UNION ALL SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`) UNION ALL SELECT * FROM `t` USE INDEX (`a`)"),
        ("create session binding for select * from t union all select * from t using select * from t use index(a) union all select * from t use index(a)", "CREATE SESSION BINDING FOR SELECT * FROM `t` UNION ALL SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`) UNION ALL SELECT * FROM `t` USE INDEX (`a`)"),
        ("drop global binding for select * from t union all select * from t using select * from t use index(a) union all select * from t use index(a)", "DROP GLOBAL BINDING FOR SELECT * FROM `t` UNION ALL SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`) UNION ALL SELECT * FROM `t` USE INDEX (`a`)"),
        ("drop session binding for select * from t union all select * from t using select * from t use index(a) union all select * from t use index(a)", "DROP SESSION BINDING FOR SELECT * FROM `t` UNION ALL SELECT * FROM `t` USING SELECT * FROM `t` USE INDEX (`a`) UNION ALL SELECT * FROM `t` USE INDEX (`a`)"),
        ("drop global binding for select * from t union all select * from t", "DROP GLOBAL BINDING FOR SELECT * FROM `t` UNION ALL SELECT * FROM `t`"),
        ("create session binding for select 1 union select 2 intersect select 3 using select 1 union select 2 intersect select 3", "CREATE SESSION BINDING FOR SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 USING SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"),
        ("drop session binding for select 1 union select 2 intersect select 3 using select 1 union select 2 intersect select 3", "DROP SESSION BINDING FOR SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 USING SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"),
        ("drop session binding for select 1 union select 2 intersect select 3", "DROP SESSION BINDING FOR SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"),
        ("create global binding using select * from *.t1", "CREATE GLOBAL BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"),
        ("create global binding using select * from *.t1 where t1.a > (select max(a) from t2)", "CREATE GLOBAL BINDING FOR SELECT * FROM `*`.`t1` WHERE `t1`.`a`>(SELECT MAX(`a`) FROM `t2`) USING SELECT * FROM `*`.`t1` WHERE `t1`.`a`>(SELECT MAX(`a`) FROM `t2`)"),
        ("create session binding using select * from *.t1", "CREATE SESSION BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"),
        ("create binding using select * from *.t1", "CREATE SESSION BINDING FOR SELECT * FROM `*`.`t1` USING SELECT * FROM `*`.`t1`"),
        ("CREATE GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1", "CREATE GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1"),
        ("CREATE SESSION BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1", "CREATE SESSION BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1"),
        ("drop global binding for update t set a = 1 where b = 1", "DROP GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1"),
        ("drop session binding for update t set a = 1 where b = 1", "DROP SESSION BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1"),
        ("DROP GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1", "DROP GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1"),
        ("DROP SESSION BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1", "DROP SESSION BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1"),
        ("CREATE GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "CREATE GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("CREATE SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "CREATE SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("DROP GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "DROP GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("DROP SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "DROP SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("DROP GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "DROP GLOBAL BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("DROP SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`", "DROP SESSION BINDING FOR UPDATE `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b` USING UPDATE /*+ INL_JOIN(`t1`)*/ `t1` JOIN `t2` SET `t1`.`a`=1 WHERE `t1`.`b`=`t2`.`b`"),
        ("CREATE GLOBAL BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1", "CREATE GLOBAL BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1"),
        ("CREATE SESSION BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1", "CREATE SESSION BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1"),
        ("drop global binding for delete from t where a = 1", "DROP GLOBAL BINDING FOR DELETE FROM `t` WHERE `a`=1"),
        ("drop session binding for delete from t where a = 1", "DROP SESSION BINDING FOR DELETE FROM `t` WHERE `a`=1"),
        ("DROP GLOBAL BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1", "DROP GLOBAL BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1"),
        ("DROP SESSION BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1", "DROP SESSION BINDING FOR DELETE FROM `t` WHERE `a`=1 USING DELETE /*+ USE_INDEX(`t` `a`)*/ FROM `t` WHERE `a`=1"),
        ("CREATE GLOBAL BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1", "CREATE GLOBAL BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("CREATE SESSION BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1", "CREATE SESSION BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("drop global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.a = 1", "DROP GLOBAL BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("drop session binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.a = 1", "DROP SESSION BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("DROP GLOBAL BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1", "DROP GLOBAL BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("DROP SESSION BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1", "DROP SESSION BINDING FOR DELETE `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1 USING DELETE /*+ HASH_JOIN(`t1`, `t2`)*/ `t1`,`t2` FROM `t1` JOIN `t2` ON `t1`.`b`=`t2`.`b` WHERE `t1`.`a`=1"),
        ("CREATE GLOBAL BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "CREATE GLOBAL BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("CREATE SESSION BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "CREATE SESSION BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("drop global binding for insert into t1 select * from t2 where t1.a=1", "DROP GLOBAL BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t1`.`a`=1"),
        ("drop session binding for insert into t1 select * from t2 where t1.a=1", "DROP SESSION BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t1`.`a`=1"),
        ("DROP GLOBAL BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "DROP GLOBAL BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("DROP SESSION BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "DROP SESSION BINDING FOR INSERT INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING INSERT INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("CREATE GLOBAL BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "CREATE GLOBAL BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("CREATE SESSION BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "CREATE SESSION BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("drop global binding for replace into t1 select * from t2 where t1.a=1", "DROP GLOBAL BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t1`.`a`=1"),
        ("drop session binding for replace into t1 select * from t2 where t1.a=1", "DROP SESSION BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t1`.`a`=1"),
        ("DROP GLOBAL BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "DROP GLOBAL BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("DROP SESSION BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1", "DROP SESSION BINDING FOR REPLACE INTO `t1` SELECT * FROM `t2` WHERE `t2`.`a`=1 USING REPLACE INTO `t1` SELECT /*+ USE_INDEX(`t2` `a`)*/ * FROM `t2` WHERE `t2`.`a`=1"),
        ("DROP SESSION BINDING FOR SQL DIGEST 'a'", "DROP SESSION BINDING FOR SQL DIGEST 'a'"),
        ("drop global binding for sql digest 's'", "DROP GLOBAL BINDING FOR SQL DIGEST 's'"),
        ("drop global binding for sql digest @a, @b, 'test1,test2', @c, 'test333'", "DROP GLOBAL BINDING FOR SQL DIGEST @`a`, @`b`, 'test1,test2', @`c`, 'test333'"),
        ("create session binding from history using plan digest 'sss'", "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST 'sss'"),
        ("create session binding from history using plan digest @a, @b, 'test1,test2', @c, 'test333'", "CREATE SESSION BINDING FROM HISTORY USING PLAN DIGEST @`a`, @`b`, 'test1,test2', @`c`, 'test333'"),
        ("CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST 'sss'", "CREATE GLOBAL BINDING FROM HISTORY USING PLAN DIGEST 'sss'"),
        ("set binding enabled for sql digest '1'", "SET BINDING ENABLED FOR SQL DIGEST '1'"),
        ("set binding disabled for sql digest '1'", "SET BINDING DISABLED FOR SQL DIGEST '1'"),
        ("explain explore 'select a from t'", "EXPLAIN EXPLORE 'select a from t'"),
        ("explain explore '23adc8e6f62'", "EXPLAIN EXPLORE '23adc8e6f62'"),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }

    let statement =
        parse("create global binding for select * from t using select * from t use index(a)")
            .expect("parse source CREATE BINDING row");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::CreateBinding(binding) = admin.as_ref() else {
        panic!("expected CREATE BINDING statement");
    };
    let tidb_ast::CreateBindingSource::Statement { target } = &binding.source else {
        panic!("expected statement binding source");
    };
    assert_eq!(target.origin.text(), b"select * from t");
    assert_eq!(
        target.hinted.as_ref().expect("hinted statement").text(),
        b"select * from t use index(a)"
    );
    assert_eq!(binding.scope, tidb_ast::BindingScope::Global);
}
