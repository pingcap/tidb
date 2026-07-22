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

//! Typed ADMIN parser/restore tests mirrored from Go's `TestAdminStmt`.

use super::*;

#[test]
fn admin_reload_restores_all_value_less_targets() {
    for (sql, restored) in [
        ("admin reload statistics", "ADMIN RELOAD STATS_EXTENDED"),
        ("admin reload stats_extended", "ADMIN RELOAD STATS_EXTENDED"),
        (
            "admin reload opt_rule_blacklist",
            "ADMIN RELOAD OPT_RULE_BLACKLIST",
        ),
        (
            "admin reload expr_pushdown_blacklist",
            "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST",
        ),
        ("admin reload bindings", "ADMIN RELOAD BINDINGS"),
        ("admin reload cluster", "ADMIN RELOAD CLUSTER BINDINGS"),
        (
            "admin reload cluster bindings",
            "ADMIN RELOAD CLUSTER BINDINGS",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
    assert!(parse("admin reload unknown_target").is_err());
}

#[test]
fn admin_show_bdr_role_restores_as_its_own_command() {
    assert_eq!(r("admin show bdr role"), "ADMIN SHOW BDR ROLE");
    assert!(parse("admin show bdr").is_err());
    assert!(parse("admin show bdr role primary").is_err());
}

/// Go owns `ADMIN SHOW SLOW` in a distinct payload from the sibling DDL-job
/// and NEXT_ROW_ID forms, so only the mode/count grammar enters this slice.
#[test]
fn admin_show_slow_restore_and_scope() {
    assert_eq!(r("admin show slow recent 3"), "ADMIN SHOW SLOW RECENT 3");
    assert_eq!(r("admin show slow top 3"), "ADMIN SHOW SLOW TOP 3");
    assert_eq!(
        r("admin show slow top internal 3"),
        "ADMIN SHOW SLOW TOP INTERNAL 3"
    );
    assert_eq!(r("admin show slow top all 3"), "ADMIN SHOW SLOW TOP ALL 3");

    assert!(parse("admin show slow top").is_err());
    assert!(parse("admin show ddl unexpected").is_err());
    assert!(parse("admin show next_row_id").is_err());
}

/// Exact Go `TestAdminStmt` bare DDL row at `pkg/parser/parser_test.go:491`.
#[test]
fn admin_show_ddl_remains_distinct_from_its_typed_extensions() {
    assert_eq!(r("admin show ddl"), "ADMIN SHOW DDL");
    assert!(matches!(
        parse("admin show ddl"),
        Ok(tidb_ast::Stmt::Admin(admin)) if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdl)
    ));
    assert!(matches!(
        parse("admin show ddl jobs"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdlJobs(_))
    ));
    assert!(matches!(
        parse("admin show ddl job queries 1"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdlJobQueries(_))
    ));
    assert!(parse("admin show ddl unexpected").is_err());
}

/// Exact Go `TestAdminStmt` `ADMIN SHOW DDL JOBS` rows at
/// `pkg/parser/parser_test.go:492-495`.
#[test]
fn admin_show_ddl_jobs_preserves_number_and_where_payload() {
    assert_eq!(r("admin show ddl jobs"), "ADMIN SHOW DDL JOBS");
    assert_eq!(
        r("admin show ddl jobs where id > 0"),
        "ADMIN SHOW DDL JOBS WHERE `id`>0"
    );
    assert_eq!(
        r("admin show ddl jobs 20 where id=0"),
        "ADMIN SHOW DDL JOBS 20 WHERE `id`=0"
    );

    let statement = parse("admin show ddl jobs 20 where id = 0")
        .expect("ADMIN SHOW DDL JOBS with payload parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowDdlJobs(show) = admin.as_ref() else {
        panic!("expected typed AdminShowDdlJobs");
    };
    assert_eq!(show.job_number, 20);
    assert!(show.where_clause.is_some());

    assert!(parse("admin show ddl jobs -1").is_err());
    assert!(matches!(
        parse("admin show ddl"),
        Ok(tidb_ast::Stmt::Admin(admin)) if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdl)
    ));
    assert!(matches!(
        parse("admin show ddl job queries 1"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdlJobQueries(_))
    ));
}

/// Exact Go `TestAdminStmt` `ADMIN SHOW DDL JOB QUERIES` rows at
/// `pkg/parser/parser_test.go:496-501`.
#[test]
fn admin_show_ddl_job_queries_preserves_list_and_limit_alternatives() {
    assert_eq!(
        r("admin show ddl job queries 1"),
        "ADMIN SHOW DDL JOB QUERIES 1"
    );
    assert_eq!(
        r("admin show ddl job queries 1, 2, 3, 4"),
        "ADMIN SHOW DDL JOB QUERIES 1, 2, 3, 4"
    );
    assert_eq!(
        r("admin show ddl job queries limit 5"),
        "ADMIN SHOW DDL JOB QUERIES LIMIT 0, 5"
    );
    assert_eq!(
        r("admin show ddl job queries limit 5, 10"),
        "ADMIN SHOW DDL JOB QUERIES LIMIT 5, 10"
    );
    assert_eq!(
        r("admin show ddl job queries limit 3 offset 2"),
        "ADMIN SHOW DDL JOB QUERIES LIMIT 2, 3"
    );
    assert_eq!(
        r("admin show ddl job queries limit 22 offset 0"),
        "ADMIN SHOW DDL JOB QUERIES LIMIT 0, 22"
    );

    let statement = parse("admin show ddl job queries limit 3 offset 2")
        .expect("ADMIN SHOW DDL JOB QUERIES LIMIT parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowDdlJobQueries(queries) = admin.as_ref() else {
        panic!("expected typed AdminShowDdlJobQueries");
    };
    assert!(matches!(
        queries.as_ref(),
        tidb_ast::AdminShowDdlJobQueriesStmt::Limit {
            offset: 2,
            count: 3,
        }
    ));

    for sql in [
        "admin show ddl job queries",
        "admin show ddl job queries -1",
        "admin show ddl job queries 1,",
        "admin show ddl job queries limit",
        "admin show ddl job queries limit 5,",
        "admin show ddl job queries limit 5 offset",
        "admin show ddl job queries 1 limit 2",
    ] {
        assert!(parse(sql).is_err(), "outside this Go alternative: {sql}");
    }
    assert!(matches!(
        parse("admin show ddl jobs"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowDdlJobs(_))
    ));
}

#[test]
fn admin_show_next_row_id_restore_and_scope() {
    assert_eq!(r("admin show t next_row_id"), "ADMIN SHOW `t` NEXT_ROW_ID");
    assert_eq!(
        r("admin show database_name.table_name next_row_id"),
        "ADMIN SHOW `database_name`.`table_name` NEXT_ROW_ID"
    );
    assert!(parse("admin show t next_row_id extra").is_err());
}

/// The supported rows come from Go's `TestAdminStmt`
/// (`pkg/parser/parser_test.go:502-524`) and signed-range regression at
/// `pkg/parser/parser_test.go:7264`.
#[test]
fn admin_check_checksum_and_recover_restore_and_shape() {
    assert_eq!(
        r("admin check table t1, schema.t2"),
        "ADMIN CHECK TABLE `t1`, `schema`.`t2`"
    );
    assert_eq!(
        r("admin check index tableName idxName"),
        "ADMIN CHECK INDEX `tableName` idxName"
    );
    assert_eq!(
        r("admin check index tableName `idxName` (1, 2), (4, 5)"),
        "ADMIN CHECK INDEX `tableName` idxName (1,2), (4,5)"
    );
    assert_eq!(
        r("admin check index t idx (0, 9223372036854775807)"),
        "ADMIN CHECK INDEX `t` idx (0,9223372036854775807)"
    );

    let stmt = parse("admin check table t1, t2").expect("ADMIN CHECK TABLE parses");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::AdminCheck(check) = admin.as_ref() else {
        panic!("expected AdminCheck");
    };
    assert!(matches!(
        check.as_ref(),
        tidb_ast::AdminCheckStmt::Table { tables } if tables.len() == 2
    ));

    assert!(parse("admin check index t idx (-1, 2)").is_err());
    assert!(parse("admin check index t idx (9223372036854775808, 2)").is_err());
    assert_eq!(
        r("admin checksum table t1, schema.t2"),
        "ADMIN CHECKSUM TABLE `t1`, `schema`.`t2`"
    );
    assert_eq!(
        r("admin recover index schema.t `idx_name`"),
        "ADMIN RECOVER INDEX `schema`.`t` idx_name"
    );
    assert_eq!(
        r("admin cleanup index schema.t `idx_name`"),
        "ADMIN CLEANUP INDEX `schema`.`t` idx_name"
    );
}

#[test]
fn admin_remaining_value_less_and_plugin_source_rows_restore_like_go() {
    for (sql, expected) in [
        (
            "admin create workload snapshot",
            "ADMIN CREATE WORKLOAD SNAPSHOT",
        ),
        (
            "admin plugins disable audit, whitelist",
            "ADMIN PLUGINS DISABLE audit, whitelist",
        ),
        (
            "admin plugins enable audit, whitelist",
            "ADMIN PLUGINS ENABLE audit, whitelist",
        ),
        ("admin flush bindings", "ADMIN FLUSH BINDINGS"),
        ("admin capture bindings", "ADMIN CAPTURE BINDINGS"),
        ("admin evolve bindings", "ADMIN EVOLVE BINDINGS"),
    ] {
        assert_eq!(r(sql), expected, "Go TestAdminStmt row: {sql}");
    }
}

/// Go accepts exactly the two named cluster roles; no-role is the separate
/// `ADMIN UNSET BDR ROLE` command.
#[test]
fn admin_bdr_role_commands_preserve_the_typed_contract() {
    for (sql, role, restore) in [
        (
            "admin set bdr role primary",
            tidb_ast::BdrRole::Primary,
            "ADMIN SET BDR ROLE PRIMARY",
        ),
        (
            "admin set bdr role secondary",
            tidb_ast::BdrRole::Secondary,
            "ADMIN SET BDR ROLE SECONDARY",
        ),
    ] {
        let stmt = parse(sql).expect("ADMIN SET BDR ROLE parses");
        assert!(matches!(
            stmt,
            tidb_ast::Stmt::Admin(admin)
                if matches!(admin.as_ref(), tidb_ast::AdminStmt::SetBdrRole(actual) if *actual == role)
        ));
        assert_eq!(r(sql), restore);
    }
    assert!(matches!(
        parse("admin unset bdr role"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::UnsetBdrRole)
    ));
    assert_eq!(r("admin unset bdr role"), "ADMIN UNSET BDR ROLE");
    assert!(parse("admin set bdr role test_err").is_err());
}
