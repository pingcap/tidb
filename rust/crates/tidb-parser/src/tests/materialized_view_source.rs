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

//! Source-backed regressions for Go `pkg/parser` materialized-view grammar.

use super::*;

#[test]
fn materialized_view_ddl_round_trips_all_statement_forms() {
    for (sql, expected) in [
        (
            "CREATE MATERIALIZED VIEW mv (a) AS SELECT 1",
            "CREATE MATERIALIZED VIEW `mv` (`a`) AS SELECT 1",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (a) COMMENT = 'c1' SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 3 REFRESH FAST NEXT 300 ATTRIBUTES = 'x' AS SELECT 1",
            "CREATE MATERIALIZED VIEW `mv` (`a`) COMMENT = 'c1' SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 3 REFRESH FAST NEXT 300 ATTRIBUTES = 'x' AS SELECT 1",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (a) COMMENT 'c1' AS SELECT 1",
            "CREATE MATERIALIZED VIEW `mv` (`a`) COMMENT = 'c1' AS SELECT 1",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a,b) PURGE IMMEDIATE ALERT ROWS 10",
            "CREATE MATERIALIZED VIEW LOG ON `t` (`a`, `b`) PURGE IMMEDIATE ALERT ROWS 10",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a) PURGE NEXT 300",
            "CREATE MATERIALIZED VIEW LOG ON `t` (`a`) PURGE NEXT 300",
        ),
        (
            "ALTER MATERIALIZED VIEW mv COMMENT = 'c2', REFRESH START WITH now() NEXT 300, ATTRIBUTES = 'y'",
            "ALTER MATERIALIZED VIEW `mv` COMMENT = 'c2', REFRESH START WITH NOW() NEXT 300, ATTRIBUTES = 'y'",
        ),
        (
            "ALTER MATERIALIZED VIEW mv COMMENT 'c2'",
            "ALTER MATERIALIZED VIEW `mv` COMMENT = 'c2'",
        ),
        (
            "ALTER MATERIALIZED VIEW mv REFRESH",
            "ALTER MATERIALIZED VIEW `mv` REFRESH",
        ),
        (
            "ALTER MATERIALIZED VIEW LOG ON t PURGE, ADD COLUMN (b,c)",
            "ALTER MATERIALIZED VIEW LOG ON `t` PURGE, ADD COLUMN (`b`, `c`)",
        ),
        (
            "ALTER MATERIALIZED VIEW LOG ON t PURGE",
            "ALTER MATERIALIZED VIEW LOG ON `t` PURGE",
        ),
        (
            "DROP MATERIALIZED VIEW IF EXISTS mv",
            "DROP MATERIALIZED VIEW IF EXISTS `mv`",
        ),
        (
            "DROP MATERIALIZED VIEW LOG IF EXISTS ON t",
            "DROP MATERIALIZED VIEW LOG IF EXISTS ON `t`",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
        assert_full_visitor_traversal(sql);
    }
}

#[test]
fn materialized_view_ddl_uses_dedicated_statement_variants() {
    for (sql, expected) in [
        (
            "CREATE MATERIALIZED VIEW mv (a) AS SELECT 1",
            "CreateMaterializedView",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a)",
            "CreateMaterializedViewLog",
        ),
        (
            "ALTER MATERIALIZED VIEW mv REFRESH",
            "AlterMaterializedView",
        ),
        (
            "ALTER MATERIALIZED VIEW LOG ON t PURGE",
            "AlterMaterializedViewLog",
        ),
        ("DROP MATERIALIZED VIEW mv", "DropMaterializedView"),
        ("DROP MATERIALIZED VIEW LOG ON t", "DropMaterializedViewLog"),
    ] {
        let Stmt::Ddl(ddl) = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}")) else {
            panic!("{sql} must produce a DDL statement")
        };
        let actual = match ddl.as_ref() {
            tidb_ast::DdlStmt::CreateMaterializedView(_) => "CreateMaterializedView",
            tidb_ast::DdlStmt::CreateMaterializedViewLog(_) => "CreateMaterializedViewLog",
            tidb_ast::DdlStmt::AlterMaterializedView(_) => "AlterMaterializedView",
            tidb_ast::DdlStmt::AlterMaterializedViewLog(_) => "AlterMaterializedViewLog",
            tidb_ast::DdlStmt::DropMaterializedView(_) => "DropMaterializedView",
            tidb_ast::DdlStmt::DropMaterializedViewLog(_) => "DropMaterializedViewLog",
            other => panic!("{sql} produced unexpected DDL variant: {other:?}"),
        };
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn materialized_view_rejects_duplicate_options_and_incomplete_purge() {
    for sql in [
        "CREATE MATERIALIZED VIEW mv (a) COMMENT='c1' COMMENT='c2' AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) SHARD_ROW_ID_BITS=1 SHARD_ROW_ID_BITS=2 AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) PRE_SPLIT_REGIONS=1 PRE_SPLIT_REGIONS=2 AS SELECT 1",
        "CREATE MATERIALIZED VIEW LOG ON t (a) SHARD_ROW_ID_BITS=1 SHARD_ROW_ID_BITS=2",
        "CREATE MATERIALIZED VIEW LOG ON t (a) PRE_SPLIT_REGIONS=1 PRE_SPLIT_REGIONS=2",
        "CREATE MATERIALIZED VIEW LOG ON t (a) PURGE",
        "CREATE MATERIALIZED VIEW LOG ON t (a) PURGE START WITH now()",
    ] {
        assert!(parse(sql).is_err(), "{sql} must be rejected");
    }
}

#[test]
fn materialized_view_rejects_out_of_order_options() {
    for sql in [
        "CREATE MATERIALIZED VIEW mv (a) REFRESH FAST SHARD_ROW_ID_BITS = 4 AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) ATTRIBUTES = 'x' REFRESH FAST AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) REFRESH FAST REFRESH FAST AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) ATTRIBUTES = 'x' ATTRIBUTES = 'y' AS SELECT 1",
    ] {
        assert!(parse(sql).is_err(), "{sql} must be rejected");
    }
}

#[test]
fn materialized_view_refresh_schedule_preserves_parentheses() {
    assert_eq!(
        r("CREATE MATERIALIZED VIEW mv (a) REFRESH FAST START WITH now() NEXT 300 AS (SELECT 1)"),
        "CREATE MATERIALIZED VIEW `mv` (`a`) REFRESH FAST START WITH NOW() NEXT 300 AS (SELECT 1)"
    );
}
