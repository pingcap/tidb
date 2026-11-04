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

//! Go `pkg/session/bootstrap_test.go`'s upgrade cases: a bootstrapped store
//! is downgraded, then the next start upgrades it.

use super::*;
use crate::bootstrap::{TxnUpgradeStore, UpgradeStore};
use crate::upgrade_def::CURRENT_BOOTSTRAP_VERSION;
use crate::upgrade_run::{finish_bootstrap, get_start_mode, must_get_store_bootstrap_version};

fn store(stack: &UnistoreClusterStack) -> TxnUpgradeStore<'_, tidb_unistore::mock::StorePdClient> {
    TxnUpgradeStore::new(&stack.opener, Duration::from_secs(10))
}

fn server_version(session: &mut Session) -> Vec<Vec<String>> {
    displayed(rows(
        session,
        r#"SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME="tidb_server_version""#,
    ))
}

/// Go `RevertVersionAndVariables`.
fn revert_version_and_variables(session: &mut Session, ver: i64) {
    rows(
        session,
        &format!("update mysql.tidb set variable_value='{ver}' where variable_name='tidb_server_version'"),
    );
    if ver <= 195 {
        // for version <= version195, tidb_enable_dist_task should be disabled before upgrade
        rows(
            session,
            "update mysql.global_variables set variable_value='off' where variable_name='tidb_enable_dist_task'",
        );
    }
}

/// Go `BootstrapSession` over the downgraded store: the meta version picks
/// the upgrade mode, the upgrade runs on a new internal session, and
/// `finishBootstrap` stamps the meta version.
fn bootstrap_session(stack: &UnistoreClusterStack) {
    let store = store(stack);
    let ver = must_get_store_bootstrap_version(&store).unwrap();
    assert_eq!(get_start_mode(ver), tidb_ddl::StartMode::Upgrade);
    crate::domain::cluster_session_node::run_upgrade(
        &stack.factory,
        &store,
        &tidb_ddl_serverstate::MemSyncer::new(),
    )
    .expect("the upgrade runs");
    finish_bootstrap(&store).unwrap();
}

fn show_create_table(session: &mut Session, table: &str) -> String {
    displayed(rows(session, &format!("show create table mysql.{table}")))[0][1].clone()
}

#[test]
fn upgrade_from_version_zero_runs_every_upgrade_function_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut se = stack.factory.open_session(session_context(91)).expect("session opens");
    rows(&mut se, "USE mysql");

    // bootstrap with currentBootstrapVersion
    assert_eq!(server_version(&mut se), [[CURRENT_BOOTSTRAP_VERSION.to_string()]]);
    assert_eq!(
        must_get_store_bootstrap_version(&store(&stack)).unwrap(),
        CURRENT_BOOTSTRAP_VERSION
    );

    // Do something to downgrade the store.
    // downgrade meta bootstrap version
    store(&stack).finish_bootstrap(1).unwrap();
    rows(&mut se, r#"delete from mysql.TiDB where VARIABLE_NAME="tidb_server_version""#);
    rows(
        &mut se,
        "update mysql.global_variables set variable_value='off' where variable_name='tidb_enable_dist_task'",
    );
    rows(
        &mut se,
        r#"delete from mysql.global_variables where VARIABLE_NAME="tidb_distsql_scan_concurrency""#,
    );
    revert_version_and_variables(&mut se, 0);
    // Go's bootstrapped root has no password; `upgradeToVer12` hex-decodes
    // every non-empty pre-4.1 password, which a modern `*...` hash is not.
    rows(&mut se, "update mysql.user set authentication_string='' where user='root'");
    // Make sure the version is downgraded.
    assert!(server_version(&mut se).is_empty());
    let jobs_before = displayed(rows(&mut se, "admin show ddl jobs 1000")).len();

    // Create a new session then upgrade() will run automatically.
    bootstrap_session(&stack);

    let mut se2 = stack.factory.open_session(session_context(92)).expect("session opens");
    assert_eq!(server_version(&mut se2), [[CURRENT_BOOTSTRAP_VERSION.to_string()]]);
    assert_eq!(
        must_get_store_bootstrap_version(&store(&stack)).unwrap(),
        CURRENT_BOOTSTRAP_VERSION
    );

    // Verify that 'new_collation_enabled' is false.
    assert_eq!(
        displayed(rows(
            &mut se2,
            "SELECT VARIABLE_VALUE from mysql.TiDB where VARIABLE_NAME='new_collation_enabled'",
        )),
        [["False"]]
    );
    // upgradeToVer2 put the deleted variable back with its default.
    assert_eq!(
        displayed(rows(
            &mut se2,
            "SELECT VARIABLE_VALUE from mysql.global_variables where VARIABLE_NAME='tidb_distsql_scan_concurrency'",
        )),
        [["15"]]
    );

    let jobs = displayed(rows(&mut se2, "admin show ddl jobs 1000"));
    assert!(jobs.len() > jobs_before, "the upgrade ran DDL jobs");
    for job in &jobs {
        // Should not use multi-schema change in bootstrap DDL because the job arguments may be changed.
        assert!(!job[3].contains("multi-schema"), "{job:?}");
    }

    // A store at the current version is not upgraded again.
    let jobs_after_first = jobs.len();
    crate::domain::cluster_session_node::run_upgrade(
        &stack.factory,
        &store(&stack),
        &tidb_ddl_serverstate::MemSyncer::new(),
    )
    .expect("the no-op upgrade runs");
    assert_eq!(displayed(rows(&mut se2, "admin show ddl jobs 1000")).len(), jobs_after_first);
}

#[test]
fn upgrade_to_ver252_widens_the_binding_timestamps_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut se_v250 = stack.factory.open_session(session_context(93)).expect("session opens");
    store(&stack).finish_bootstrap(250).unwrap();
    revert_version_and_variables(&mut se_v250, 250);

    let create_tbl_sql = show_create_table(&mut se_v250, "bind_info");
    assert!(create_tbl_sql.contains("`create_time` timestamp(6)"), "{create_tbl_sql}");
    assert!(create_tbl_sql.contains("`update_time` timestamp(6)"), "{create_tbl_sql}");
    // revert it back to timestamp(3) for testing.
    rows(&mut se_v250, "set @@sql_mode = ''");
    rows(&mut se_v250, "alter table mysql.bind_info modify create_time timestamp(3)");
    rows(&mut se_v250, "alter table mysql.bind_info modify update_time timestamp(3)");
    let create_tbl_sql = show_create_table(&mut se_v250, "bind_info");
    assert!(create_tbl_sql.contains("`create_time` timestamp(3)"), "{create_tbl_sql}");
    assert!(create_tbl_sql.contains("`update_time` timestamp(3)"), "{create_tbl_sql}");

    // do upgrade to latest version
    bootstrap_session(&stack);
    let mut se_cur_ver = stack.factory.open_session(session_context(94)).expect("session opens");
    assert_eq!(server_version(&mut se_cur_ver), [[CURRENT_BOOTSTRAP_VERSION.to_string()]]);
    // check if the columns have been changed to timestamp(6)
    let create_tbl_sql = show_create_table(&mut se_cur_ver, "bind_info");
    assert!(create_tbl_sql.contains("`create_time` timestamp(6)"), "{create_tbl_sql}");
    assert!(create_tbl_sql.contains("`update_time` timestamp(6)"), "{create_tbl_sql}");
}

#[test]
fn upgrade_to_ver254_adds_the_runaway_watch_indexes_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut se_v253 = stack.factory.open_session(session_context(95)).expect("session opens");
    store(&stack).finish_bootstrap(253).unwrap();
    revert_version_and_variables(&mut se_v253, 253);

    // Verify the indexes exist after bootstrap
    assert!(show_create_table(&mut se_v253, "tidb_runaway_watch").contains("idx_start_time"));
    assert!(show_create_table(&mut se_v253, "tidb_runaway_watch_done").contains("idx_done_time"));

    // Remove the indexes to simulate the old version
    rows(&mut se_v253, "ALTER TABLE mysql.tidb_runaway_watch DROP INDEX idx_start_time");
    rows(&mut se_v253, "ALTER TABLE mysql.tidb_runaway_watch_done DROP INDEX idx_done_time");
    assert!(!show_create_table(&mut se_v253, "tidb_runaway_watch").contains("idx_start_time"));
    assert!(!show_create_table(&mut se_v253, "tidb_runaway_watch_done").contains("idx_done_time"));

    // Upgrade to current version
    bootstrap_session(&stack);
    let mut se_cur_ver = stack.factory.open_session(session_context(96)).expect("session opens");
    assert_eq!(server_version(&mut se_cur_ver), [[CURRENT_BOOTSTRAP_VERSION.to_string()]]);

    // Verify the indexes have been created after upgrade
    assert!(show_create_table(&mut se_cur_ver, "tidb_runaway_watch").contains("idx_start_time"));
    assert!(show_create_table(&mut se_cur_ver, "tidb_runaway_watch_done").contains("idx_done_time"));
}

#[test]
fn upgrade_to_ver242_writes_the_cluster_id_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut se = stack.factory.open_session(session_context(97)).expect("session opens");
    let cluster_id = stack.opener.cluster_id().to_string();
    let cluster_id_rows = |session: &mut Session| {
        displayed(rows(session, "select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME='cluster_id'"))
    };
    assert_eq!(cluster_id_rows(&mut se), [[cluster_id.clone()]]);

    store(&stack).finish_bootstrap(241).unwrap();
    revert_version_and_variables(&mut se, 241);
    rows(&mut se, "delete from mysql.tidb where VARIABLE_NAME='cluster_id'");
    assert!(cluster_id_rows(&mut se).is_empty());

    bootstrap_session(&stack);
    let mut se_cur_ver = stack.factory.open_session(session_context(98)).expect("session opens");
    assert_eq!(cluster_id_rows(&mut se_cur_ver), [[cluster_id]]);
}

/// Go's later `initGlobalVariableIfNotExists` upgrades backfill only absent
/// rows, keeping the old cluster's behavior; `upgradeToVer255` rewrites the
/// retired analyze version.
#[test]
fn upgrade_backfills_only_missing_global_variables_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut se = stack.factory.open_session(session_context(99)).expect("session opens");
    store(&stack).finish_bootstrap(254).unwrap();
    revert_version_and_variables(&mut se, 254);
    for name in [
        "tidb_analyze_default_num_buckets",
        "tidb_default_string_match_selectivity",
        "tidb_ignore_inlist_plan_digest",
        "tidb_enable_no_backslash_escapes_in_like",
        "tidb_analyze_distsql_scan_concurrency",
    ] {
        rows(&mut se, &format!("delete from mysql.global_variables where VARIABLE_NAME='{name}'"));
    }
    // A user-set value survives the upgrade.
    rows(
        &mut se,
        "update mysql.global_variables set VARIABLE_VALUE='7' where VARIABLE_NAME='tidb_analyze_default_num_topn'",
    );
    rows(
        &mut se,
        "update mysql.global_variables set VARIABLE_VALUE='1' where VARIABLE_NAME='tidb_analyze_version'",
    );
    rows(
        &mut se,
        "update mysql.global_variables set VARIABLE_VALUE='9' where VARIABLE_NAME='tidb_distsql_scan_concurrency'",
    );

    bootstrap_session(&stack);
    let mut se_cur_ver = stack.factory.open_session(session_context(100)).expect("session opens");
    let value = |session: &mut Session, name: &str| {
        displayed(rows(
            session,
            &format!("select VARIABLE_VALUE from mysql.global_variables where VARIABLE_NAME='{name}'"),
        ))
    };
    assert_eq!(value(&mut se_cur_ver, "tidb_analyze_default_num_buckets"), [["256"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_analyze_default_num_topn"), [["7"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_default_string_match_selectivity"), [["0.8"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_ignore_inlist_plan_digest"), [["OFF"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_enable_no_backslash_escapes_in_like"), [["OFF"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_analyze_distsql_scan_concurrency"), [["9"]]);
    assert_eq!(value(&mut se_cur_ver, "tidb_analyze_version"), [["2"]]);
}
