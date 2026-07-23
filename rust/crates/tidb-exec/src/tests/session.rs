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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Nontransactional session-state execution tests: system-variable readback,
//! user variables, and unsupported session/admin boundaries.

use super::*;

#[test]
fn use_statement_exec() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1)");
    // `USE` is an accepted no-op (flat namespace) — it never errors,
    // even for a database this executor has no notion of, and leaves
    // existing tables untouched.
    assert_eq!(step(&mut db, "use whatever_db"), "OK");
    assert_eq!(step(&mut db, "select a from t"), "RS:1");
    // Accepted mid-transaction without committing or erroring.
    step(&mut db, "begin");
    assert_eq!(step(&mut db, "use another_db"), "OK");
    step(&mut db, "insert into t values (2)");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select a from t"), "RS:1");
}
use crate::session_settings::SqlSelectLimit;

#[test]
fn create_database_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table create_database_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into create_database_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("create database if not exists app default charset utf8")
                .expect("parse CREATE DATABASE")
        ),
        Err(ExecError::Unsupported("CREATE DATABASE"))
    ));

    // The rejected DDL must not clear the transaction snapshot. If it had
    // taken the normal DDL path, this rollback would leave the row behind.
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from create_database_boundary"),
        "RS:"
    );
}

#[test]
fn show_stats_histograms_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table show_stats_histograms_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into show_stats_histograms_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse(
                "show stats_histograms where db_name = 'test' and column_name = 'id'",
            )
            .expect("parse SHOW STATS_HISTOGRAMS")
        ),
        Err(ExecError::Unsupported("SHOW STATS_HISTOGRAMS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_stats_histograms_boundary"),
        "RS:"
    );
}

#[test]
fn show_errors_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_errors_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_errors_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show count(*) errors where Code = 1").expect("parse SHOW ERRORS")
        ),
        Err(ExecError::Unsupported("SHOW ERRORS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_errors_boundary"), "RS:");
}

#[test]
fn show_create_user_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_create_user_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_create_user_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show create user 'root'@'localhost'")
                .expect("parse SHOW CREATE USER")
        ),
        Err(ExecError::Unsupported("SHOW CREATE"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_create_user_boundary"),
        "RS:"
    );
}

#[test]
fn drop_resource_group_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table drop_resource_group_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into drop_resource_group_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("drop resource group if exists rg1")
                .expect("parse DROP RESOURCE GROUP")
        ),
        Err(ExecError::Unsupported("DROP RESOURCE GROUP"))
    ));

    // Rejecting cluster-level admission-control DDL must leave the active
    // transaction intact: rollback still removes the pending row.
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from drop_resource_group_boundary"),
        "RS:"
    );
}

#[test]
fn do_is_explicitly_unsupported_before_expression_side_effects() {
    let mut db = Database::new();
    assert!(matches!(
        db.run(&tidb_parser::parse("do @do_boundary := 1").expect("parse DO")),
        Err(ExecError::Unsupported("DO"))
    ));

    // `DO` would evaluate the assignment in TiDB. Until the seed owns DO's
    // discard-result and warning protocol, rejection must happen before the
    // expression can alter session state.
    assert_eq!(step(&mut db, "select @do_boundary"), "RS:<nil>");
}

#[test]
fn batch_dml_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table batch_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into batch_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("batch limit 1 delete from batch_boundary")
                .expect("parse BATCH DML")
        ),
        Err(ExecError::Unsupported("BATCH DML"))
    ));

    // A rejected BATCH must neither execute the inner DELETE nor commit/clear
    // the active transaction snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from batch_boundary"), "RS:");
}

#[test]
fn import_into_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table import_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into import_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("import into import_boundary from '/file.csv'")
                .expect("parse IMPORT INTO")
        ),
        Err(ExecError::Unsupported("IMPORT INTO"))
    ));

    // Importing needs TiDB's external-storage/import-job protocol. Until the
    // executor owns it, rejection must neither start a new transaction nor
    // discard the active snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from import_boundary"), "RS:");
}

#[test]
fn load_stats_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table load_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into load_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("load stats '/stats.json'").expect("parse LOAD STATS")),
        Err(ExecError::Unsupported("LOAD STATS"))
    ));

    // Neither unsupported import path may run an implicit transaction or
    // consume the existing snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from load_boundary"), "RS:");
}

#[test]
fn grant_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table grant_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into grant_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("grant select on grant_boundary to grant_user")
                .expect("parse GRANT")
        ),
        Err(ExecError::Unsupported("GRANT"))
    ));

    // GRANT changes TiDB's account/privilege graph, which this seed does not
    // model. Its rejection must therefore not discard the active snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from grant_boundary"), "RS:");
}

#[test]
fn revoke_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table revoke_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into revoke_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("revoke select on revoke_boundary from revoke_user")
                .expect("parse REVOKE")
        ),
        Err(ExecError::Unsupported("REVOKE"))
    ));

    // REVOKE needs the durable privilege graph absent from this seed; a
    // rejected command must not discard the active transaction snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from revoke_boundary"), "RS:");
}

#[test]
fn grant_revoke_role_are_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table role_grant_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into role_grant_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("grant r1 to role_user").expect("parse GRANT ROLE")),
        Err(ExecError::Unsupported("GRANT ROLE"))
    ));
    assert!(matches!(
        db.run(&tidb_parser::parse("revoke r1 from role_user").expect("parse REVOKE ROLE")),
        Err(ExecError::Unsupported("REVOKE ROLE"))
    ));

    // Neither role-graph operation may execute or consume the active
    // transaction while this executor has no durable privilege subsystem.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from role_grant_boundary"), "RS:");
}

#[test]
fn admin_flush_plan_cache_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table plan_cache_flush_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into plan_cache_flush_boundary values (1)");

    for (sql, expected) in [
        (
            "admin flush session plan_cache",
            "ADMIN FLUSH SESSION PLAN_CACHE",
        ),
        (
            "admin flush global plan_cache",
            "ADMIN FLUSH GLOBAL PLAN_CACHE",
        ),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse ADMIN FLUSH PLAN_CACHE")),
            Err(ExecError::Unsupported(message)) if message == expected
        ));
    }

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from plan_cache_flush_boundary"),
        "RS:"
    );
}

#[test]
fn show_grants_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_grants_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_grants_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("show grants").expect("parse SHOW GRANTS")),
        Err(ExecError::Unsupported("SHOW GRANTS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_grants_boundary"), "RS:");
}

#[test]
fn admin_reload_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table admin_reload_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into admin_reload_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("admin reload opt_rule_blacklist").expect("parse ADMIN RELOAD")),
        Err(ExecError::Unsupported("ADMIN RELOAD"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from admin_reload_boundary"), "RS:");
}

#[test]
fn admin_show_bdr_role_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table admin_show_bdr_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into admin_show_bdr_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("admin show bdr role").expect("parse ADMIN SHOW BDR ROLE")),
        Err(ExecError::Unsupported("ADMIN SHOW BDR ROLE"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_show_bdr_boundary"),
        "RS:"
    );
}

#[test]
fn admin_show_slow_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table admin_show_slow_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into admin_show_slow_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("admin show slow top all 3").expect("parse ADMIN SHOW SLOW")),
        Err(ExecError::Unsupported("ADMIN SHOW SLOW"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_show_slow_boundary"),
        "RS:"
    );
}

#[test]
fn show_stats_locked_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_stats_locked_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_stats_locked_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show stats_locked where table_name = 't'")
                .expect("parse SHOW STATS_LOCKED")
        ),
        Err(ExecError::Unsupported("SHOW STATS_LOCKED"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_stats_locked_boundary"),
        "RS:"
    );
}

#[test]
fn show_stats_buckets_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_stats_buckets_boundary (id int)");
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into show_stats_buckets_boundary values (1)",
    );
    assert!(matches!(
        db.run(
            &tidb_parser::parse("show stats_buckets where col_name = 'a'")
                .expect("parse SHOW STATS_BUCKETS")
        ),
        Err(ExecError::Unsupported("SHOW STATS_BUCKETS"))
    ));
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_stats_buckets_boundary"),
        "RS:"
    );
}

#[test]
fn admin_show_next_row_id_is_explicitly_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table admin_show_next_row_id_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into admin_show_next_row_id_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("admin show admin_show_next_row_id_boundary next_row_id")
                .expect("parse ADMIN SHOW NEXT_ROW_ID")
        ),
        Err(ExecError::Unsupported("ADMIN SHOW NEXT_ROW_ID"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_show_next_row_id_boundary"),
        "RS:"
    );
}

#[test]
fn plan_replayer_dump_explain_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table plan_replayer_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into plan_replayer_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("plan replayer dump explain select id from plan_replayer_boundary")
                .expect("parse PLAN REPLAYER DUMP EXPLAIN")
        ),
        Err(ExecError::Unsupported("PLAN REPLAYER"))
    ));

    // The unsupported administration command must not implicitly commit the
    // open transaction, even though real Plan Replayer may inspect the plan.
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from plan_replayer_boundary"),
        "RS:"
    );
}

#[test]
fn show_index_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_index_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_index_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show index from show_index_boundary").expect("parse SHOW INDEX")
        ),
        Err(ExecError::Unsupported("SHOW INDEX"))
    ));

    // SHOW INDEX is an administrative read. Rejecting an unimplemented
    // metadata renderer must not commit or clear the active transaction.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_index_boundary"), "RS:");
}

#[test]
fn show_columns_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_columns_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_columns_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show columns from show_columns_boundary")
                .expect("parse SHOW COLUMNS")
        ),
        Err(ExecError::Unsupported("SHOW COLUMNS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_columns_boundary"), "RS:");
}

#[test]
fn show_tables_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_tables_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_tables_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("show tables").expect("parse SHOW TABLES")),
        Err(ExecError::Unsupported("SHOW TABLES"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_tables_boundary"), "RS:");
}

#[test]
fn show_status_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_status_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_status_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show status where Variable_name like 'Threads%'")
                .expect("parse SHOW STATUS")
        ),
        Err(ExecError::Unsupported("SHOW STATUS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from show_status_boundary"), "RS:");
}

#[test]
fn show_variables_where_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table show_variables_where_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into show_variables_where_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show global variables where Variable_name = 'autocommit'")
                .expect("parse SHOW VARIABLES WHERE")
        ),
        Err(ExecError::Unsupported("SHOW VARIABLES"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_variables_where_boundary"),
        "RS:"
    );
}

#[test]
fn show_stats_topn_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_stats_topn_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_stats_topn_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show stats_topn where table_name = 'show_stats_topn_boundary'")
                .expect("parse SHOW STATS_TOPN")
        ),
        Err(ExecError::Unsupported("SHOW STATS_TOPN"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_stats_topn_boundary"),
        "RS:"
    );
}

#[test]
fn admin_show_ddl_jobs_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table admin_show_ddl_jobs_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into admin_show_ddl_jobs_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("admin show ddl jobs 20 where id = 0")
                .expect("parse ADMIN SHOW DDL JOBS")
        ),
        Err(ExecError::Unsupported("ADMIN SHOW DDL JOBS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_show_ddl_jobs_boundary"),
        "RS:"
    );
}

#[test]
fn admin_show_ddl_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table admin_show_ddl_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into admin_show_ddl_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("admin show ddl").expect("parse ADMIN SHOW DDL")),
        Err(ExecError::Unsupported("ADMIN SHOW DDL"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_show_ddl_boundary"),
        "RS:"
    );
}

#[test]
fn admin_show_ddl_job_queries_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table admin_show_ddl_job_queries_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into admin_show_ddl_job_queries_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("admin show ddl job queries limit 3 offset 2")
                .expect("parse ADMIN SHOW DDL JOB QUERIES")
        ),
        Err(ExecError::Unsupported("ADMIN SHOW DDL JOB QUERIES"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(
            &mut db,
            "select id from admin_show_ddl_job_queries_boundary"
        ),
        "RS:"
    );
}

#[test]
fn admin_ddl_job_controls_are_explicitly_unsupported_before_mutation() {
    for (sql, command) in [
        ("admin cancel ddl jobs 1, 2", "ADMIN CANCEL DDL JOBS"),
        ("admin pause ddl jobs 1, 3", "ADMIN PAUSE DDL JOBS"),
        ("admin resume ddl jobs 1, 2", "ADMIN RESUME DDL JOBS"),
    ] {
        let mut db = Database::new();
        step(
            &mut db,
            "create table admin_ddl_job_control_boundary (id int)",
        );
        step(&mut db, "begin");
        step(
            &mut db,
            "insert into admin_ddl_job_control_boundary values (1)",
        );

        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse ADMIN DDL job control")),
            Err(ExecError::Unsupported(actual)) if actual == command
        ));

        step(&mut db, "rollback");
        assert_eq!(
            step(&mut db, "select id from admin_ddl_job_control_boundary"),
            "RS:"
        );
    }
}

#[test]
fn show_table_status_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_table_status_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_table_status_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show table status like 'show_table_status_boundary'")
                .expect("parse SHOW TABLE STATUS")
        ),
        Err(ExecError::Unsupported("SHOW TABLE STATUS"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_table_status_boundary"),
        "RS:"
    );
}

#[test]
fn show_open_tables_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_open_tables_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_open_tables_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("show open tables from test").expect("parse SHOW OPEN TABLES")),
        Err(ExecError::Unsupported("SHOW OPEN TABLES"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_open_tables_boundary"),
        "RS:"
    );
}

#[test]
fn show_table_next_row_id_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table next_row_id_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into next_row_id_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("show table next_row_id_boundary next_row_id")
                .expect("parse SHOW TABLE NEXT_ROW_ID")
        ),
        Err(ExecError::Unsupported("SHOW TABLE NEXT_ROW_ID"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from next_row_id_boundary"), "RS:");
}

#[test]
fn show_databases_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_databases_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_databases_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("show databases like 'test%'").expect("parse SHOW DATABASES")),
        Err(ExecError::Unsupported("SHOW DATABASES"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_databases_boundary"),
        "RS:"
    );
}

#[test]
fn flush_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table flush_boundary (id int)");

    for (sql, operation) in [
        ("flush status", "FLUSH STATUS"),
        ("flush tables with read lock", "FLUSH TABLES"),
        ("flush privileges", "FLUSH PRIVILEGES"),
    ] {
        step(&mut db, "begin");
        step(&mut db, "insert into flush_boundary values (1)");
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse FLUSH")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
        step(&mut db, "rollback");
        assert_eq!(step(&mut db, "select id from flush_boundary"), "RS:");
    }
}

#[test]
fn charset_session_commands_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table charset_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into charset_boundary values (1)");

    for (sql, operation) in [
        ("set names utf8mb4 collate utf8mb4_general_ci", "SET NAMES"),
        ("set char set latin1", "SET CHARSET"),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse charset command")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    // Rejection must not commit or clear the active transaction.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from charset_boundary"), "RS:");
}

#[test]
fn set_password_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table set_password_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into set_password_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("set password for current_user() = 'new' retain current password")
                .expect("parse SET PASSWORD")
        ),
        Err(ExecError::Unsupported("SET PASSWORD"))
    ));

    // Unsupported password management must not commit or discard an active
    // transaction while this executor has no user/authentication catalog.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from set_password_boundary"), "RS:");
}

#[test]
fn role_session_commands_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table role_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into role_boundary values (1)");

    for (sql, operation) in [
        ("set role all except audit", "SET ROLE"),
        ("set default role all to app", "SET DEFAULT ROLE"),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse role command")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    // Neither privilege-management command may consume the active snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from role_boundary"), "RS:");
}

#[test]
fn resource_group_session_commands_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table resource_group_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into resource_group_boundary values (1)");

    for (sql, operation) in [
        ("set resource group rg1", "SET RESOURCE GROUP"),
        (
            "set session_states '{\"rs-group\":\"rg1\"}'",
            "SET SESSION_STATES",
        ),
    ] {
        assert!(matches!(
            db.run(
                &tidb_parser::parse(sql).expect("parse resource-control command"),
            ),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    // Neither unsupported session-control command may consume the active
    // snapshot. A rollback must still remove the pending write.
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from resource_group_boundary"),
        "RS:"
    );
}

#[test]
fn sysvar_readback() {
    // Real MySQL/TiDB defaults (confirmed via `gorun`): autocommit on,
    // time_zone the literal string "SYSTEM" (NOT "+00:00") until an
    // explicit SET establishes a concrete offset.
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select @@autocommit"), "RS:1");
    assert_eq!(step(&mut db, "select @@session.autocommit"), "RS:1");
    assert_eq!(step(&mut db, "select @@global.autocommit"), "RS:1");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:SYSTEM");
    assert_eq!(step(&mut db, "select @@global.time_zone"), "RS:SYSTEM");
    assert!(step(&mut db, "select @@instance.autocommit").starts_with("Eval("));

    // `SET autocommit`/`SET time_zone` change ONLY the session-scoped
    // readback -- `@@GLOBAL.*` stays at its fixed default regardless
    // (confirmed via `gorun`: this executor has no separate global
    // variable store, and `SET GLOBAL` is already rejected elsewhere).
    step(&mut db, "set autocommit=0");
    assert_eq!(step(&mut db, "select @@autocommit"), "RS:0");
    assert_eq!(step(&mut db, "select @@global.autocommit"), "RS:1");
    step(&mut db, "set time_zone='+05:30'");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:+05:30");
    assert_eq!(step(&mut db, "select @@global.time_zone"), "RS:SYSTEM");

    // A system variable is an ordinary expression operand, usable in
    // arithmetic like any other value.
    assert_eq!(step(&mut db, "select 1 + @@autocommit"), "RS:1");

    // Any variable this executor doesn't track state for is a genuine
    // execution error (a permanent scope boundary: real MySQL/TiDB has
    // ~600 system variables, only autocommit/time_zone/tx_isolation/
    // tx_isolation_one_shot are modelled).
    assert!(step(&mut db, "select @@totally_bogus_var_xyz").starts_with("Eval("));
}

/// The source exposes this as a typed session enum, but its ON/WARN protocol
/// behavior belongs to TiDB's client connection layer rather than this
/// one-statement executor. Keep the seed boundary explicit: invalid enum
/// values and unsupported GLOBAL writes cannot partially mutate session state.
#[test]
fn multi_statement_mode_keeps_typed_session_and_global_boundaries() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "set tidb_multi_statement_mode = warn"), "OK");
    assert_eq!(
        step(&mut db, "set tidb_multi_statement_mode = 3"),
        "Unsupported(\"SET tidb_multi_statement_mode value\")"
    );
    assert_eq!(
        step(&mut db, "select @@tidb_multi_statement_mode"),
        "RS:WARN"
    );

    assert_eq!(
        step(&mut db, "set global tidb_multi_statement_mode = on"),
        "Unsupported(\"SET GLOBAL/INSTANCE variable\")"
    );
    assert_eq!(
        step(&mut db, "select @@global.tidb_multi_statement_mode"),
        "RS:OFF"
    );
}

/// Ports the bounded TypeUnsigned session contract in
/// `pkg/executor/set_test.go:TestDivPrecisionIncrement`, and proves its one
/// value reaches every decimal-division executor path. The table corpus owns
/// the complete Go golden; this direct test additionally guards the seed's
/// intentional no-global-store boundary and rejects a fractional assignment
/// before it can mutate the previously valid session value.
#[test]
fn div_precision_increment_threads_scalar_grouped_and_window_division() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table dpi_unit (a decimal(3,0), b decimal(3,0))",
    );
    step(&mut db, "insert into dpi_unit values (8, 7), (9, 7)");

    assert_eq!(
        step(
            &mut db,
            "select @@div_precision_increment, a/b from dpi_unit order by a"
        ),
        "RS:4|1.1429;4|1.2857"
    );
    assert_eq!(step(&mut db, "set div_precision_increment = 7"), "OK");
    assert_eq!(
        step(&mut db, "select avg(a) / 2, avg(a/b) from dpi_unit"),
        "RS:4.25000000000000|1.21428571350000"
    );
    assert_eq!(
        step(
            &mut db,
            "select a, avg(a/b) over () from dpi_unit order by a",
        ),
        "RS:8|1.21428571350000;9|1.21428571350000"
    );

    assert_eq!(step(&mut db, "set div_precision_increment = -1"), "OK");
    assert_eq!(
        step(&mut db, "select @@div_precision_increment, 8/7"),
        "RS:0|1.1429"
    );
    assert_eq!(step(&mut db, "set div_precision_increment = 31"), "OK");
    assert_eq!(
        step(&mut db, "select @@div_precision_increment, 8/7"),
        "RS:30|1.142857142857142857142857142857"
    );
    assert!(step(&mut db, "set div_precision_increment = 6.5")
        .starts_with("Unsupported(\"SET div_precision_increment value\")"));
    assert_eq!(step(&mut db, "select @@div_precision_increment"), "RS:30");
    assert_eq!(
        step(&mut db, "set global div_precision_increment = 4"),
        "Unsupported(\"SET GLOBAL/INSTANCE variable\")"
    );
    assert_eq!(
        step(
            &mut db,
            "select @@global.div_precision_increment, @@session.div_precision_increment",
        ),
        "RS:4|30"
    );
}

/// Ports `sql_select_limit`'s TypeUnsigned session contract and
/// `TryAddExtraLimit`'s outer-query rule
/// (`pkg/sessionctx/variable/sysvar_test.go:41`,
/// `pkg/planner/core/preprocess.go:85-123`). The differential corpus owns
/// the Go golden; this regression keeps the important structural boundaries
/// local: session status is nontransactional, explicit LIMIT wins, UNION gets
/// only a statement-level cap, and this seed's no-global-store policy cannot
/// partially mutate a session value.
#[test]
fn sql_select_limit_is_uint_nontransactional_and_only_implicit() {
    assert_eq!(
        Database::default().sql_select_limit,
        SqlSelectLimit::UNLIMITED,
        "the derived database default must preserve TiDB's no-limit sentinel"
    );
    let mut db = Database::new();
    step(&mut db, "create table sl_unit (a int)");
    step(&mut db, "insert into sl_unit values (1), (2), (3)");

    assert_eq!(
        step(
            &mut db,
            "select @@sql_select_limit, @@global.sql_select_limit limit 1",
        ),
        "RS:18446744073709551615|18446744073709551615"
    );
    assert_eq!(step(&mut db, "set sql_select_limit = -10"), "OK");
    assert_eq!(step(&mut db, "select @@sql_select_limit limit 1"), "RS:0");
    assert_eq!(step(&mut db, "select a from sl_unit order by a"), "RS:");

    assert_eq!(step(&mut db, "set sql_select_limit = 100000000000"), "OK");
    assert_eq!(
        step(&mut db, "select @@sql_select_limit limit 1"),
        "RS:100000000000"
    );
    step(&mut db, "set sql_select_limit = 2");
    assert_eq!(step(&mut db, "select a from sl_unit order by a"), "RS:1;2");
    assert_eq!(
        step(&mut db, "select a from sl_unit order by a limit 3"),
        "RS:1;2;3"
    );
    assert_eq!(
        step(&mut db, "select 1 union all select 2 union all select 3"),
        "RS:1;2"
    );
    assert_eq!(
        step(
            &mut db,
            "select 1 union all select 2 union all select 3 limit 3",
        ),
        "RS:1;2;3"
    );

    step(&mut db, "begin");
    step(&mut db, "set sql_select_limit = 1");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select @@sql_select_limit limit 1"), "RS:1");
    assert!(step(&mut db, "set sql_select_limit = 18446744073709551616")
        .starts_with("Unsupported(\"SET sql_select_limit value\")"));
    assert_eq!(step(&mut db, "select @@sql_select_limit limit 1"), "RS:1");
    assert_eq!(
        step(&mut db, "set global sql_select_limit = 1"),
        "Unsupported(\"SET GLOBAL/INSTANCE variable\")"
    );
    assert_eq!(
        step(&mut db, "select @@global.sql_select_limit limit 1"),
        "RS:18446744073709551615"
    );
}

/// Ports `LAST_INSERT_ID(expr)`'s statement-status handoff from
/// `pkg/executor/select.go:1224-1229`, not an auto-increment column feature.
/// The current-statement setter and previous-statement reader must remain
/// separate, preserve raw UInt64 bits for negative input, and survive both a
/// rollback and a later evaluation error.
#[test]
fn last_insert_id_promotes_unsigned_status_at_statement_boundaries() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "select last_insert_id(5), last_insert_id(), @@last_insert_id, @@identity",
        ),
        "RS:5|0|0|0"
    );
    assert_eq!(
        step(
            &mut db,
            "select last_insert_id(), @@last_insert_id, @@identity",
        ),
        "RS:5|5|5"
    );
    assert_eq!(
        step(&mut db, "select last_insert_id(-1)"),
        "RS:18446744073709551615"
    );
    assert_eq!(
        step(&mut db, "select last_insert_id(), @@last_insert_id"),
        "RS:18446744073709551615|18446744073709551615"
    );

    step(&mut db, "begin");
    assert_eq!(step(&mut db, "select last_insert_id(9)"), "RS:9");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select last_insert_id(), @@identity"),
        "RS:9|9"
    );

    // The setter runs before the later function error. Go carries that
    // statement context into the next statement; rollback is irrelevant.
    assert!(step(
        &mut db,
        "select last_insert_id(17), no_such_last_insert_function()",
    )
    .starts_with("Eval(Unsupported(\"unsupported function\"))"));
    assert_eq!(
        step(&mut db, "select last_insert_id(), @@last_insert_id"),
        "RS:17|17"
    );
    assert!(step(&mut db, "select @@global.last_insert_id")
        .starts_with("Eval(Unsupported(\"unknown system variable\"))"));
}

/// Go's `timeutil.ParseTimeZone` treats `SYSTEM`, `UTC`, and a fixed zero
/// offset as separately observable session settings. A transaction rollback
/// does not restore a prior session variable value.
#[test]
fn time_zone_labels_are_distinct_and_nontransactional() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "set time_zone='+00:00'"), "OK");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:+00:00");

    assert_eq!(step(&mut db, "set time_zone='UTC'"), "OK");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:UTC");

    step(&mut db, "begin");
    assert_eq!(step(&mut db, "set time_zone='SYSTEM'"), "OK");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:SYSTEM");

    assert_eq!(step(&mut db, "set time_zone='utc'"), "OK");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:utc");
    assert_eq!(step(&mut db, "select @@global.time_zone"), "RS:SYSTEM");
}

/// `foreign_key_checks` is a session switch over every FK boundary the seed
/// already owns. Disabling it does not remove constraints or repair data, so
/// re-enabling must reject only subsequent violations; like other session
/// variables, its value survives ROLLBACK.
#[test]
fn foreign_key_checks_gates_existing_fk_boundaries_without_rollback() {
    let mut db = Database::new();
    step(&mut db, "create table fkc_parent (id int primary key)");
    step(
        &mut db,
        "create table fkc_child (id int primary key, pid int, foreign key (pid) references fkc_parent(id))",
    );
    assert_eq!(
        step(&mut db, "insert into fkc_child values (1, 9)"),
        "ForeignKeyViolation"
    );

    step(&mut db, "set foreign_key_checks=0");
    assert_eq!(step(&mut db, "select @@foreign_key_checks"), "RS:0");
    assert!(step(&mut db, "set foreign_key_checks=3").starts_with("Unsupported("));
    assert_eq!(step(&mut db, "select @@foreign_key_checks"), "RS:0");
    step(&mut db, "insert into fkc_child values (1, 9)");
    step(&mut db, "set foreign_key_checks=1");
    assert_eq!(
        step(&mut db, "insert into fkc_child values (2, 8)"),
        "ForeignKeyViolation"
    );
    assert_eq!(step(&mut db, "select * from fkc_child"), "RS:1|9");

    step(&mut db, "insert into fkc_parent values (9)");
    assert_eq!(
        step(&mut db, "delete from fkc_parent where id=9"),
        "ForeignKeyViolation"
    );
    step(&mut db, "set foreign_key_checks=off");
    step(&mut db, "delete from fkc_parent where id=9");
    assert_eq!(step(&mut db, "select * from fkc_child"), "RS:1|9");

    step(&mut db, "set foreign_key_checks=default");
    assert_eq!(step(&mut db, "select @@foreign_key_checks"), "RS:1");
    step(&mut db, "create table fkc_drop_parent (id int primary key)");
    step(
        &mut db,
        "create table fkc_drop_child (id int primary key, pid int, foreign key (pid) references fkc_drop_parent(id))",
    );
    assert_eq!(
        step(&mut db, "drop table fkc_drop_parent"),
        "ForeignKeyViolation"
    );
    step(&mut db, "set foreign_key_checks=0");
    step(&mut db, "drop table fkc_drop_parent");

    step(&mut db, "begin");
    step(&mut db, "set foreign_key_checks=1");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select @@foreign_key_checks"), "RS:1");
}

/// Go registers `sql_safe_updates` in `variable/noop.go`: the session value
/// is observable, but TiDB does not apply MySQL's no-WHERE DML restriction.
#[test]
fn sql_safe_updates_is_nontransactional_compatibility_state() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:0");
    assert_eq!(step(&mut db, "select @@global.sql_safe_updates"), "RS:0");
    step(&mut db, "set sql_safe_updates=1");
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:1");
    assert!(step(&mut db, "set sql_safe_updates=3").starts_with("Unsupported("));
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:1");
    step(&mut db, "set sql_safe_updates=default");
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:0");

    // `SET_VAR` is statement-scoped: expose the hinted value during this
    // SELECT, then restore the session value. The restoration must run when
    // SELECT evaluation returns an error too.
    assert_eq!(
        step(
            &mut db,
            "select /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates",
        ),
        "RS:1"
    );
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:0");
    assert_eq!(
        step(
            &mut db,
            "select /*+ SET_VAR(sql_safe_updates=1) SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates",
        ),
        "RS:1"
    );
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:0");
    step(&mut db, "set sql_safe_updates=1");
    assert_eq!(
        step(
            &mut db,
            "select /*+ SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates",
        ),
        "RS:0"
    );
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:1");
    assert!(step(
        &mut db,
        "select /*+ SET_VAR(sql_safe_updates=0) */ no_such_column"
    )
    .starts_with("Eval("));
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:1");

    step(&mut db, "begin");
    step(&mut db, "set sql_safe_updates=1");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select @@sql_safe_updates"), "RS:1");

    step(&mut db, "create table ssu (id int)");
    step(&mut db, "insert into ssu values (1), (2)");
    step(&mut db, "update ssu set id=id+10");
    step(&mut db, "delete from ssu");
    assert_eq!(step(&mut db, "select * from ssu"), "RS:");
}

/// Go's Timestamp sysvar has one setting for both current-time functions and
/// @@timestamp readback. Its default string "0" is dynamic, while a fixed
/// value retains its normalized text exactly. The seed intentionally has no
/// warning-result surface, so the negative-value assertion covers the
/// observable clamp/state transition but not Go's accompanying warning.
#[test]
fn timestamp_is_one_source_for_clock_readback_default_and_transactions() {
    let mut db = Database::new();
    step(&mut db, "set time_zone = '+00:00'");

    step(&mut db, "set timestamp = 10.5000");
    assert_eq!(
        step(&mut db, "select @@timestamp, now(6), current_timestamp(6)",),
        "RS:10.5000|1970-01-01 00:00:10.500000|1970-01-01 00:00:10.500000"
    );
    assert_eq!(
        step(&mut db, "set timestamp = 2147483648"),
        "Unsupported(\"SET timestamp value\")"
    );
    assert_eq!(step(&mut db, "select @@timestamp"), "RS:10.5000");

    // The exact string "0" is dynamic in TiDB, but decimal zero strings
    // remain fixed values and preserve their source-normalized readback.
    step(&mut db, "set timestamp = 0.0");
    assert_eq!(
        step(&mut db, "select @@timestamp, now()"),
        "RS:0.0|1970-01-01 00:00:00"
    );
    step(&mut db, "set timestamp = 0");
    let dynamic = step(&mut db, "select @@timestamp");
    assert!(dynamic.starts_with("RS:"));
    assert_ne!(dynamic, "RS:0");
    let state = db.session_state();
    assert_eq!(dynamic, format!("RS:{}", state.timestamp));
    assert_eq!(
        state.timestamp,
        (state.now.expect("dynamic statement clock").0 as f64
            + f64::from(state.now.expect("dynamic statement clock").1) / 1e9)
            .to_string()
    );

    // Negative input clamps to source default (Go also appends a warning).
    step(&mut db, "set timestamp = -5");
    assert_ne!(step(&mut db, "select @@timestamp"), "RS:-5");
    step(&mut db, "set timestamp = default");
    assert_ne!(step(&mut db, "select @@timestamp"), "RS:0");

    // This setting is session state, not catalog transaction state.
    step(&mut db, "begin");
    step(&mut db, "set timestamp = 2.5");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select @@timestamp, now(1)"),
        "RS:2.5|1970-01-01 00:00:02.5"
    );
}

/// Parser support for generic system-variable SET must not be mistaken for a
/// global-variable implementation.  The seed executor dispatches only its
/// existing session model, and rejects unsupported scope/default/name before
/// a mixed list can partially mutate session state.
#[test]
fn generic_system_set_execution_boundary() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "set @@session.autocommit = 0, @@local.time_zone = '+05:30'",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "select @@autocommit"), "RS:0");
    assert_eq!(step(&mut db, "select @@time_zone"), "RS:+05:30");

    let mut untouched = Database::new();
    assert_eq!(
        step(
            &mut untouched,
            "set @@session.autocommit = 0, @@global.autocommit = 0",
        ),
        "Unsupported(\"SET GLOBAL/INSTANCE variable\")"
    );
    assert_eq!(step(&mut untouched, "select @@autocommit"), "RS:1");
    assert_eq!(
        step(&mut untouched, "set @@instance.tidb_mem_quota_query = 128"),
        "Unsupported(\"SET GLOBAL/INSTANCE variable\")"
    );
    assert_eq!(
        step(&mut untouched, "set @@session.autocommit = default"),
        "Unsupported(\"SET DEFAULT variable\")"
    );
    assert_eq!(
        step(
            &mut untouched,
            "set @@session.character_set_results = binary"
        ),
        "Unsupported(\"SET variable\")"
    );
}

/// The storage layout attached to an explicit primary key is unobservable in
/// this seed's in-memory, table-scan-only execution model. This mirrors the
/// exact `window_fixture` Go-result script's use of a clustered feature
/// comment: creation, DML, uniqueness, and table scans have the same
/// behavior as an ordinary primary key without pretending to model TiKV's
/// physical layout.
#[test]
fn clustered_primary_key_is_layout_neutral_for_dml_and_table_scans() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table default_pk (id int primary key, v int)",
    );
    step(
        &mut db,
        "create table clustered_pk (id int, v int, primary key(id) clustered)",
    );
    step(
        &mut db,
        "create table nonclustered_pk (id int primary key nonclustered, v int)",
    );
    for table in ["default_pk", "clustered_pk", "nonclustered_pk"] {
        step(
            &mut db,
            &format!("insert into {table} values (2, 20), (1, 10)"),
        );
        assert_eq!(
            step(&mut db, &format!("select id, v from {table} order by id")),
            "RS:1|10;2|20"
        );
        let duplicate = step(&mut db, &format!("insert into {table} values (1, 99)"));
        assert_eq!(duplicate, "DuplicateKey", "{table}");
    }
    // The real fixture's feature-comment spelling reaches the same parsed
    // mode and therefore remains executable for its following window/DML
    // workload rather than failing before the first INSERT.
    assert_eq!(
        step(
            &mut db,
            "create table fixture_pk (id int, v int, primary key(id) /*T![clustered_index] clustered */)",
        ),
        "OK"
    );
    step(&mut db, "insert into fixture_pk values (1, 10)");
    assert_eq!(step(&mut db, "select * from fixture_pk"), "RS:1|10");
}

/// TiDB deliberately exposes `tx_read_only`/`transaction_read_only` as
/// compatibility-only aliases. The governing code is
/// `pkg/sessionctx/variable/noop.go` plus `varsutil.go:checkReadOnly`, with
/// end-to-end coverage in `pkg/executor/set_test.go:TestSetVar`: the value is
/// observable but has no write-enforcement effect, and setting it ON is gated
/// by `tidb_enable_noop_functions`.
#[test]
fn transaction_read_only_noop_gate_and_aliases() {
    let mut db = Database::new();

    assert_eq!(step(&mut db, "select @@tx_read_only"), "RS:0");
    assert_eq!(step(&mut db, "select @@transaction_read_only"), "RS:0");
    assert_eq!(
        step(&mut db, "select @@tidb_enable_noop_functions"),
        "RS:OFF"
    );
    // OFF accepts false but rejects true without changing either alias.
    assert_eq!(step(&mut db, "set tx_read_only = 0"), "OK");
    assert!(step(&mut db, "set transaction_read_only = 1").starts_with("Unsupported("));
    assert_eq!(
        step(&mut db, "select @@tx_read_only, @@transaction_read_only"),
        "RS:0|0"
    );

    assert_eq!(step(&mut db, "set tidb_enable_noop_functions = on"), "OK");
    assert_eq!(
        step(&mut db, "select @@tidb_enable_noop_functions"),
        "RS:ON"
    );
    // The `SET TRANSACTION READ ONLY` syntax is parser sugar for the same
    // one stored variable. It changes readback but not DML permissions: Go
    // declares this feature a no-op explicitly, so the INSERT must remain
    // executable rather than receiving a fabricated read-only failure.
    assert_eq!(step(&mut db, "set transaction read only"), "OK");
    assert_eq!(
        step(&mut db, "select @@tx_read_only, @@transaction_read_only"),
        "RS:1|1"
    );
    step(&mut db, "create table read_only_noop (id int)");
    assert_eq!(step(&mut db, "insert into read_only_noop values (1)"), "OK");
    assert_eq!(step(&mut db, "select id from read_only_noop"), "RS:1");

    // Go rejects disabling the gate while a dependent no-op is still ON.
    assert!(step(&mut db, "set tidb_enable_noop_functions = off").starts_with("Unsupported("));
    assert_eq!(step(&mut db, "set transaction read write"), "OK");
    assert_eq!(step(&mut db, "set tidb_enable_noop_functions = off"), "OK");
    assert_eq!(
        step(&mut db, "select @@tx_read_only, @@transaction_read_only"),
        "RS:0|0"
    );

    // WARN is a distinct accepted mode in the Go enum. This executor has no
    // warning result surface, but it must preserve the mode and accept the
    // same compatibility value rather than collapsing WARN into OFF.
    assert_eq!(step(&mut db, "set tidb_enable_noop_functions = warn"), "OK");
    assert_eq!(
        step(&mut db, "select @@tidb_enable_noop_functions"),
        "RS:WARN"
    );
    assert_eq!(step(&mut db, "set tx_read_only = 1"), "OK");
    assert_eq!(step(&mut db, "set tx_read_only = 0"), "OK");
    assert_eq!(step(&mut db, "set tidb_enable_noop_functions = 0"), "OK");

    // This seed has no cluster-global variable store. Global reads therefore
    // remain TiDB's defaults and GLOBAL writes are rejected before session
    // state changes, consistent with the pre-existing sysvar boundary.
    assert_eq!(step(&mut db, "select @@global.tx_read_only"), "RS:0");
    assert_eq!(
        step(&mut db, "select @@global.tidb_enable_noop_functions"),
        "RS:OFF"
    );
    assert!(step(&mut db, "set global tx_read_only = 0").starts_with("Unsupported("));
}

#[test]
fn user_variables() {
    let mut db = Database::new();
    // An unset user variable reads as NULL, never an error -- the
    // opposite convention from `@@sysvar`'s unrecognized-name case.
    assert_eq!(step(&mut db, "select @undefined_var"), "RS:<nil>");
    assert_eq!(step(&mut db, "set @x = 5"), "OK");
    assert_eq!(step(&mut db, "select @x"), "RS:5");
    assert_eq!(step(&mut db, "select @x + 1"), "RS:6");
    assert_eq!(step(&mut db, "set @y = 'hello'"), "OK");
    assert_eq!(step(&mut db, "select @y"), "RS:hello");
    // The value expression may reference another already-set user
    // variable.
    assert_eq!(step(&mut db, "set @z = @x + 1"), "OK");
    assert_eq!(step(&mut db, "select @z"), "RS:6");
    // Go's SET executor evaluates and writes each list item before it
    // evaluates the next one. Both = and := use that same ordered rule.
    assert_eq!(
        step(
            &mut db,
            "set @ordered = 1, @next := @ordered + 1, @ordered = @next + 1"
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "select @ordered, @next"), "RS:3|2");
    // Assigning NULL unsets the variable, so it reads as NULL exactly like
    // a variable that was never assigned.
    assert_eq!(step(&mut db, "set @ordered = null"), "OK");
    assert_eq!(step(&mut db, "select @ordered"), "RS:<nil>");
    // A failure in a later item does not undo earlier user-variable writes.
    assert!(step(&mut db, "set @survives = 9, @fails = unknown_function()").starts_with("Eval("));
    assert_eq!(step(&mut db, "select @survives"), "RS:9");
    // Case-insensitive name (confirmed via `gorun`: `@X` and `@x` are
    // the SAME variable).
    assert_eq!(step(&mut db, "select @X"), "RS:5");
    // The value expression may reference a subquery.
    step(&mut db, "create table t (id int primary key, v int)");
    step(&mut db, "insert into t values (1,10),(2,20)");
    assert_eq!(step(&mut db, "set @c = (select count(*) from t)"), "OK");
    assert_eq!(step(&mut db, "select @c"), "RS:2");
    // A user variable is an ordinary expression operand, usable
    // anywhere a column reference could be, including a table-scanning
    // `WHERE` clause.
    assert_eq!(
        step(&mut db, "select id, v from t where v > @x order by id"),
        "RS:1|10;2|20"
    );
    // User variables are session-scoped, NOT transactional (confirmed
    // via `gorun`: survives a LATER `ROLLBACK`, unlike table data).
    step(&mut db, "begin");
    step(&mut db, "set @w = 99");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select @w"), "RS:99");
}

/// `@x := expr` (the inline ASSIGNMENT EXPRESSION, `tidb_ast::Expr::Assign`
/// — see `tidb_expr::Columns::set_uservar`'s own doc for the interior-
/// mutability mechanism) evaluates to the assigned value AND mutates the
/// session's user-variable store as a side effect, matching `gorun`'s own
/// `SELECT @i := 1` => `1`. A LATER select-list item in the SAME row sees
/// an EARLIER one's assignment (left-to-right, confirmed via `gorun`:
/// `SELECT @x := 1, @x + 1` => `1|2`), and — the classic MySQL running-
/// total idiom — a LATER row in the same scan sees an EARLIER row's
/// assignment too (`gorun`: `SET @y = 0; SELECT @y := @y + 1 FROM (3
/// rows)` => `1;2;3`). The write persists into the session past the end
/// of the statement, visible to a later, separate statement (`gorun`:
/// after the above, `SELECT @y` => `3`) — the SAME persistence
/// `user_variables` (the pre-existing `SET @x = value` / plain `@x`
/// read machinery) already established, now shared by BOTH assignment
/// forms.
#[test]
fn user_var_assign_eval() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select @i := 1"), "RS:1");
    // Same-row, left-to-right visibility.
    assert_eq!(step(&mut db, "select @i := 1, @i + 1"), "RS:1|2");
    // The scalar assignment function returns NULL without overwriting its
    // previous value. This is intentionally unlike top-level SET @i = NULL.
    assert_eq!(step(&mut db, "select @i := null"), "RS:<nil>");
    assert_eq!(step(&mut db, "select @i"), "RS:1");
    // Reading an unset variable is still `NULL`, unaffected.
    assert_eq!(step(&mut db, "select @unset_var"), "RS:<nil>");
    // Cross-row accumulation within one scan, then cross-statement
    // persistence of the final value.
    step(&mut db, "set @y = 0");
    assert_eq!(step(&mut db, "select @y := @y + 1"), "RS:1");
    assert_eq!(step(&mut db, "select @y"), "RS:1");
    step(&mut db, "create table uvar1 (a int)");
    step(&mut db, "insert into uvar1 values (1), (2), (3)");
    assert_eq!(
        step(&mut db, "select @y := @y + 1 from uvar1 order by a"),
        "RS:2;3;4"
    );
    assert_eq!(step(&mut db, "select @y"), "RS:4");
    // String values are copied out of each scanned row before the next row
    // is evaluated, matching Go's `TestSetVarFromColumn` buffer-reuse
    // regression rather than retaining a row-backed reference.
    step(&mut db, "create table uvar_string (v varchar(8))");
    step(&mut db, "insert into uvar_string values ('a'), ('b')");
    assert_eq!(
        step(
            &mut db,
            "select @last_string := v, @last_string from uvar_string order by v",
        ),
        "RS:a|a;b|b"
    );
    assert_eq!(step(&mut db, "select @last_string"), "RS:b");
}

/// The SAME `@w := expr` mechanism works in `WHERE` too (evaluated once
/// per scanned row, in scan order, exactly like the select-list case
/// above) — a common MySQL idiom for a manual row counter. Confirmed via
/// `gorun`.
#[test]
fn user_var_assign_where_eval() {
    let mut db = Database::new();
    step(&mut db, "create table uvar2 (a int)");
    step(&mut db, "insert into uvar2 values (1), (2), (3)");
    step(&mut db, "set @w = 0");
    assert_eq!(
        step(
            &mut db,
            "select a from uvar2 where (@w := @w + 1) > 0 order by a"
        ),
        "RS:1;2;3"
    );
    assert_eq!(step(&mut db, "select @w"), "RS:3");
}

/// `:=` also works in an AGGREGATED (`GROUP BY`) query — assignment
/// reaches `eval_group`'s own aggregate-free-leaf fallback the same way
/// any other subquery-free expression does, one evaluation per SURVIVING
/// GROUP. Deliberately NOT asserting a byte-exact match against `gorun`
/// here, unlike every other assertion in this file: probed directly,
/// real TiDB's own per-group assignment ORDER for this exact idiom is
/// driven by its hash-based grouping's internal bucket layout, not a
/// stable position (a 4-group probe assigned `a`,`b`,`c`,`d` the values
/// `4,1,3,2` — no simple forward/reverse pattern), and MySQL's own
/// documentation says the evaluation order of user-variable expressions
/// during aggregation is UNSPECIFIED. This project's own `GROUP BY`
/// groups in first-occurrence order (deterministic, matching its
/// existing convention elsewhere) — asserted here as a regression test
/// for THIS implementation's own mechanism, not a claimed `gorun` match.
#[test]
fn user_var_assign_group_by_eval() {
    let mut db = Database::new();
    step(&mut db, "create table uvar3 (a int, dept varchar(10))");
    step(
        &mut db,
        "insert into uvar3 values (1,'a'), (2,'a'), (3,'b')",
    );
    step(&mut db, "set @g = 0");
    assert_eq!(
        step(
            &mut db,
            "select dept, sum(a), @g := @g + 1 from uvar3 group by dept"
        ),
        "RS:a|3|1;b|3|2"
    );
    assert_eq!(step(&mut db, "select @g"), "RS:2");
}

/// A column reference nested inside a `:=` assignment's own `value` is
/// still subject to `ONLY_FULL_GROUP_BY` column-pinning validation — the
/// SAME `check_columns_pinned`-based regression template
/// `member_of_column_pinning` already established, confirming
/// `tidb_ast::Expr::Assign` was added to that traversal correctly (a
/// missing arm here would silently let an ungrouped column slip through
/// unnoticed) — this validation runs BEFORE evaluation, rejecting the
/// statement regardless of whether `:=` itself would go on to evaluate
/// successfully.
#[test]
fn user_var_assign_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table uvcp1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into uvcp1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from uvcp1 group by dept having @x := tag = 'y'"
    )
    .starts_with("UngroupedColumn("));
}

/// Parser support for `EXPLAIN` must not masquerade as a plan renderer or as
/// execution of its wrapped statement. The seed executor has neither TiDB's
/// optimizer nor its analyze instrumentation, so this is an explicit boundary.
#[test]
fn explain_is_explicitly_unsupported() {
    let mut db = Database::new();
    assert!(matches!(
        db.run(&tidb_parser::parse("explain analyze select 1").expect("parse EXPLAIN")),
        Err(ExecError::Unsupported("EXPLAIN"))
    ));
}

/// `DESC`/`DESCRIBE` parses through Go's `SHOW COLUMNS` normal form, but the
/// seed executor has no information schema or TiDB-compatible metadata rows.
/// It must fail explicitly rather than exposing its internal table shape.
#[test]
fn describe_table_is_explicitly_unsupported() {
    let mut db = Database::new();
    assert!(matches!(
        db.run(&tidb_parser::parse("describe t").expect("parse DESCRIBE")),
        Err(ExecError::Unsupported("DESC"))
    ));
    assert!(matches!(
        db.run(&tidb_parser::parse("explain t c").expect("parse EXPLAIN table")),
        Err(ExecError::Unsupported("DESC"))
    ));
}

/// `SHOW WARNINGS` needs the previous statement's session diagnostic area,
/// including diagnostic codes and ordered Warning/Error/Note entries. The
/// seed executor has none, so it must not turn the command into empty rows.
#[test]
fn show_warnings_is_explicitly_unsupported() {
    let mut db = Database::new();
    for sql in [
        "show warnings",
        "show warnings like 'Warn%'",
        "show warnings where Level in ('Warning', 'Error')",
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse SHOW WARNINGS")),
            Err(ExecError::Unsupported("SHOW WARNINGS"))
        ));
    }
}

/// ANALYZE must not be accepted as a no-op: real TiDB persists
/// optimizer-visible histograms and TopN statistics, a subsystem this seed
/// deliberately does not provide.
#[test]
fn analyze_table_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table at1 (a int, b int)");
    assert!(matches!(
        db.run(
            &tidb_parser::parse("analyze table at1 all columns with 2 topn")
                .expect("parse ANALYZE TABLE")
        ),
        Err(ExecError::Unsupported("ANALYZE TABLE"))
    ));
    // Unsupported ANALYZE has no DDL-like side effect: the pre-existing
    // relation remains readable and unmodified.
    assert_eq!(step(&mut db, "select * from at1"), "RS:");
}

/// Incremental ANALYZE has the same statistics-write boundary as ordinary
/// ANALYZE, while retaining its own typed parser payload.
#[test]
fn analyze_incremental_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table ai1 (a int, b int)");
    assert!(matches!(
        db.run(
            &tidb_parser::parse("analyze incremental table ai1 index")
                .expect("parse ANALYZE INCREMENTAL")
        ),
        Err(ExecError::Unsupported("ANALYZE INCREMENTAL"))
    ));
    assert_eq!(step(&mut db, "select * from ai1"), "RS:");
}

/// TiDB's ADMIN CHECK reads and compares physical index records; the seed has
/// only table scans and no secondary-index catalog. Both syntax branches must
/// reject before they can be mistaken for a successful empty check.
#[test]
fn admin_check_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table ac_boundary (id int)");
    for (sql, operation) in [
        ("admin check table ac_boundary", "ADMIN CHECK TABLE"),
        (
            "admin check index ac_boundary idx (1, 2)",
            "ADMIN CHECK INDEX",
        ),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse ADMIN CHECK")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }
    assert_eq!(step(&mut db, "select * from ac_boundary"), "RS:");
}

#[test]
fn admin_checksum_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table admin_checksum_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into admin_checksum_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("admin checksum table admin_checksum_boundary")
                .expect("parse ADMIN CHECKSUM TABLE")
        ),
        Err(ExecError::Unsupported("ADMIN CHECKSUM TABLE"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_checksum_boundary"),
        "RS:"
    );
}

#[test]
fn admin_recover_index_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table admin_recover_index_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into admin_recover_index_boundary values (1)",
    );

    assert!(matches!(
        db.run(
            &tidb_parser::parse("admin recover index admin_recover_index_boundary idx")
                .expect("parse ADMIN RECOVER INDEX")
        ),
        Err(ExecError::Unsupported("ADMIN RECOVER INDEX"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from admin_recover_index_boundary"),
        "RS:"
    );
}

#[test]
fn stats_locks_are_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table stats_lock_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into stats_lock_boundary values (1)");

    for (sql, operation) in [
        ("lock stats stats_lock_boundary", "LOCK STATS"),
        (
            "unlock stats stats_lock_boundary partition p0",
            "UNLOCK STATS",
        ),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse stats lock")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from stats_lock_boundary"), "RS:");
}

/// BDR role changes need TiDB's cluster metadata and DDL restriction policy.
/// Parsing them must not make the seed executor falsely claim a local role
/// transition or disturb an open transaction.
#[test]
fn admin_bdr_controls_are_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table bdr_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into bdr_boundary values (1)");

    for (sql, operation) in [
        ("admin set bdr role primary", "ADMIN SET BDR ROLE"),
        ("admin set bdr role secondary", "ADMIN SET BDR ROLE"),
        ("admin unset bdr role", "ADMIN UNSET BDR ROLE"),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse ADMIN BDR control")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from bdr_boundary"), "RS:");
}

/// `CREATE USER` has a parser/restore model but no corresponding user,
/// credential, or privilege catalog in this seed executor. It must reject
/// before changing transaction or relation state, rather than reporting a
/// successful no-op account creation.
#[test]
fn create_user_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table cu_boundary (id int)");
    assert!(matches!(
        db.run(
            &tidb_parser::parse(
                "create user if not exists app@localhost identified with mysql_native_password by 'secret' password expire interval 3 day password history 5 account lock failed_login_attempts 3 password_lock_time unbounded attribute '{\"role\": \"reader\"}'",
            )
            .expect("parse CREATE USER")
        ),
        Err(ExecError::Unsupported("CREATE USER"))
    ));
    assert_eq!(step(&mut db, "select * from cu_boundary"), "RS:");
}

#[test]
fn create_role_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table role_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into role_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("create role if not exists app_reader, 'ops'@localhost")
                .expect("parse CREATE ROLE")
        ),
        Err(ExecError::Unsupported("CREATE ROLE"))
    ));

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from role_boundary"), "RS:");
}

#[test]
fn alter_user_resource_group_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table resource_group_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into resource_group_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("alter user app resource group rg1")
                .expect("parse ALTER USER RESOURCE GROUP")
        ),
        Err(ExecError::Unsupported("ALTER USER"))
    ));

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from resource_group_boundary"),
        "RS:"
    );
}

/// Bindings are parser/restore-only until the executor has TiDB's binding
/// catalog and cache lifecycle. Every command family must reject before it can
/// commit or otherwise disturb an open transaction snapshot.
#[test]
fn binding_commands_are_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table binding_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into binding_boundary values (1)");

    for (sql, operation) in [
        (
            "create global binding for select * from binding_boundary using select * from binding_boundary use index(id)",
            "CREATE BINDING",
        ),
        (
            "create global binding for with cte1 as (select id from binding_boundary) update binding_boundary set id = 2 where id in (select id from cte1) using with cte1 as (select id from binding_boundary) update binding_boundary set id = 2 where id in (select id from cte1)",
            "CREATE BINDING",
        ),
        ("drop binding for sql digest 'digest'", "DROP BINDING"),
        (
            "set binding disabled for select * from binding_boundary",
            "SET BINDING",
        ),
        ("show session bindings", "SHOW BINDINGS"),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse binding command")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    // An accidental DDL-style execution path would clear the snapshot and
    // make this insert survive rollback. The explicit errors above must not.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from binding_boundary"), "RS:");
}

/// UPDATE/DELETE ORDER BY and LIMIT select which rows are mutated. Until the
/// executor implements that ordering/limit contract, parser support must not
/// silently fall through to the unrestricted mutation path.
#[test]
fn update_delete_order_limit_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table dml_tail_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into dml_tail_boundary values (1)");

    for (sql, operation) in [
        (
            "update dml_tail_boundary set id = 2 order by id limit 1",
            "UPDATE ORDER BY/LIMIT",
        ),
        (
            "delete from dml_tail_boundary order by id limit 1",
            "DELETE ORDER BY/LIMIT",
        ),
    ] {
        assert!(matches!(
            db.run(&tidb_parser::parse(sql).expect("parse DML tail")),
            Err(ExecError::Unsupported(actual)) if actual == operation
        ));
    }

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from dml_tail_boundary"), "RS:");
}

#[test]
fn alter_user_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table au_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into au_boundary values (1)");

    assert!(matches!(
        db.run(
            &tidb_parser::parse("alter user app identified by 'new' retain current password")
                .expect("parse ALTER USER")
        ),
        Err(ExecError::Unsupported("ALTER USER"))
    ));

    // The rejected account DDL must not clear the transaction snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from au_boundary"), "RS:");
}

#[test]
fn rename_user_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table rename_user_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into rename_user_boundary values (1)");

    assert!(matches!(
        db.run(&tidb_parser::parse("rename user old_user to new_user").expect("parse RENAME USER")),
        Err(ExecError::Unsupported("RENAME USER"))
    ));

    // Renaming accounts needs the privilege/user graph absent from this seed.
    // Rejecting it must not clear the active transaction snapshot.
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from rename_user_boundary"), "RS:");
}

#[test]
fn create_index_is_explicitly_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table ci_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into ci_boundary values (1)");
    assert!(matches!(
        db.run(&tidb_parser::parse("create unique index ci on ci_boundary(id)").unwrap()),
        Err(ExecError::Unsupported("CREATE INDEX"))
    ));
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from ci_boundary"), "RS:");
}
