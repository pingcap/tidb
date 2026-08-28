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

//! Source-backed inventory for manifest batch `b145`, `pkg/session.part2`.
//!
//! The batch is the deterministic items 61--120 of the upstream
//! `pkg/session` test enumeration. The cursor package is a complete Rust
//! carrier and is exercised below. Bootstrap-version mutation, starter-file
//! reconciliation, PD/keyspace integration, and `syssession` ownership are
//! intentionally recorded as ignored gaps: this crate does not own those
//! storage/domain/server seams.

#![cfg(test)]

use crate::cursor::{CursorTracker, State};

/// `pkg/session/bootstrap_test.go:1333::TestTiDBUpgradeToVer170`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and Domain upgrade are not transcreated"]
fn test_tidb_upgrade_to_ver170() {}

/// `pkg/session/bootstrap_test.go:1367::TestTiDBUpgradeToVer176`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and Domain upgrade are not transcreated"]
fn test_tidb_upgrade_to_ver176() {}

/// `pkg/session/bootstrap_test.go:1405::TestTiDBUpgradeToVer177`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and Domain upgrade are not transcreated"]
fn test_tidb_upgrade_to_ver177() {}

/// `pkg/session/bootstrap_test.go:1443::TestTiDBUpgradeToVer209`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and Domain upgrade are not transcreated"]
fn test_tidb_upgrade_to_ver209() {}

/// `pkg/session/bootstrap_test.go:1495::TestIssue61890`.
#[test]
#[ignore = "go-parity-gap: bootstrap/session system-table repair is not transcreated"]
fn test_issue61890() {}

/// `pkg/session/bootstrap_test.go:1510::TestKeyspaceEtcdNamespace`.
#[test]
#[ignore = "go-parity-gap: keyspace metadata and etcd integration are not transcreated"]
fn test_keyspace_etcd_namespace() {}

/// `pkg/session/bootstrap_test.go:1520::TestNullKeyspaceEtcdNamespace`.
#[test]
#[ignore = "go-parity-gap: keyspace metadata and etcd integration are not transcreated"]
fn test_null_keyspace_etcd_namespace() {}

/// `pkg/session/bootstrap_test.go:1609::TestTiDBUpgradeToVer240`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and Domain upgrade are not transcreated"]
fn test_tidb_upgrade_to_ver240() {}

/// `pkg/session/bootstrap_test.go:1657::TestTiDBUpgradeToVer252`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and system-table DDL are not transcreated"]
fn test_tidb_upgrade_to_ver252() {}

/// `pkg/session/bootstrap_test.go:1720::TestTiDBUpgradeToVer254`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and system-table DDL are not transcreated"]
fn test_tidb_upgrade_to_ver254() {}

/// `pkg/session/bootstrap_test.go:1779::TestWriteClusterIDToMySQLTiDBWhenUpgradingTo242`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and mysql.tidb upgrade are not transcreated"]
fn test_write_cluster_id_to_mysql_tidb_when_upgrading_to242() {}

/// `pkg/session/bootstrap_test.go:1839::TestBindInfoUniqueIndex`.
#[test]
#[ignore = "go-parity-gap: bootstrap-version rollback and bind_info upgrade are not transcreated"]
fn test_bind_info_unique_index() {}

/// `pkg/session/bootstrap_test.go:1886::TestVersionedBootstrapSchemas`.
#[test]
#[ignore = "go-parity-gap: versioned bootstrap schema catalog is not transcreated"]
fn test_versioned_bootstrap_schemas() {}

/// `pkg/session/bootstrap_test.go:1917::TestCheckSystemTableConstraint`.
#[test]
#[ignore = "go-parity-gap: session bootstrap system-table model constraints are not transcreated"]
fn test_check_system_table_constraint() {}

/// `pkg/session/cursor/tracker_test.go:26::TestNewCursor`.
#[test]
fn test_new_cursor() {
    let tracker = CursorTracker::new();
    let first = tracker.new_cursor(State::default());
    let second = tracker.new_cursor(State::default());
    assert_eq!(first.id(), 1);
    assert_eq!(second.id(), 2);
}

/// `pkg/session/cursor/tracker_test.go:37::TestGetCursor`.
#[test]
fn test_get_cursor() {
    let tracker = CursorTracker::new();
    let cursor = tracker.new_cursor(State { start_ts: 42 });
    let found = tracker.cursor(cursor.id()).expect("cursor was registered");
    assert_eq!(found.id(), cursor.id());
    assert_eq!(found.state(), State { start_ts: 42 });
}

/// `pkg/session/cursor/tracker_test.go:45::TestRangeCursor`.
#[test]
fn test_range_cursor() {
    let tracker = CursorTracker::new();
    tracker.new_cursor(State::default());
    let mut called = false;
    tracker.range_cursor(|cursor| {
        called = true;
        assert_eq!(cursor.id(), 1);
        false
    });
    assert!(called);
}

/// `pkg/session/cursor/tracker_test.go:59::TestCursorHandleClose`.
#[test]
fn test_cursor_handle_close() {
    let tracker = CursorTracker::new();
    let cursor = tracker.new_cursor(State::default());
    let id = cursor.id();
    cursor.close();
    assert!(tracker.cursor(id).is_none());
}

/// `pkg/session/cursor/tracker_test.go:69::TestCursorTrackerConcurrentCreateDelete`.
#[test]
fn test_cursor_tracker_concurrent_create_delete() {
    let tracker = CursorTracker::new();
    std::thread::scope(|scope| {
        for _ in 0..100 {
            let tracker = tracker.clone();
            scope.spawn(move || {
                for _ in 0..100 {
                    let cursor = tracker.new_cursor(State::default());
                    cursor.close();
                }
            });
        }
        for _ in 0..100 {
            let tracker = tracker.clone();
            scope.spawn(move || {
                tracker.range_cursor(|cursor| {
                    cursor.close();
                    true
                });
            });
        }
    });
    assert!(tracker.is_empty());
}

/// `pkg/session/main_test.go:33::TestMain` is the Go goleak/common-test harness.
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust test surface"]
fn test_main() {}

/// `pkg/session/session_nextgen_test.go:103::TestUsePipelinedDMLDisabledInStarter`.
#[test]
#[ignore = "go-parity-gap: starter deployment session mode is not transcreated"]
fn test_use_pipelined_dml_disabled_in_starter() {}

/// `pkg/session/session_nextgen_test.go:120::TestUpgradeGCV2AbortUsesPostLockBootstrapVersion`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap and external workload manager are not transcreated"]
fn test_upgrade_gcv2_abort_uses_post_lock_bootstrap_version() {}

/// `pkg/session/session_nextgen_test.go:141::TestCreateSessionWithDomainOptionsAttachesExternalWorkloadManager`.
#[test]
#[ignore = "go-parity-gap: Domain/session bootstrap and external workload manager are not transcreated"]
fn test_create_session_with_domain_options_attaches_external_workload_manager() {}

/// `pkg/session/session_nextgen_test.go:159::TestBootstrapSessionWithExternalWorkloadManagerAttachesBootstrapDomain`.
#[test]
#[ignore = "go-parity-gap: Domain/session bootstrap and external workload manager are not transcreated"]
fn test_bootstrap_session_with_external_workload_manager_attaches_bootstrap_domain() {}

/// `pkg/session/session_test.go:48::TestGetStartMode`.
#[test]
#[ignore = "go-parity-carrier: start_mode is already tested in tidb-server::bootstrap_source"]
fn test_get_start_mode() {}

/// `pkg/session/session_test.go:55::TestMustGetStoreBootstrapVersionRetriesTransaction`.
#[test]
#[ignore = "go-parity-gap: session bootstrap-version storage retry path is not transcreated"]
fn test_must_get_store_bootstrap_version_retries_transaction() {}

/// `pkg/session/session_test.go:79::TestWaitSystemBootVersion`.
#[test]
#[ignore = "go-parity-gap: SYSTEM keyspace storage bootstrap wait loop is not transcreated"]
fn test_wait_system_boot_version() {}

/// `pkg/session/session_test.go:168::TestBootstrapSessionImplUserKSVersionGuard`.
#[test]
#[ignore = "go-parity-carrier: user-keyspace version decision is tested in tidb-server::bootstrap_source"]
fn test_bootstrap_session_impl_user_ks_version_guard() {}

/// `pkg/session/session_test.go:242::TestDDLTableVersionTables`.
#[test]
#[ignore = "go-parity-gap: session DDL table-version catalog is not transcreated"]
fn test_ddl_table_version_tables() {}

/// `pkg/session/session_test.go:273::TestMemArbitratorSession`.
#[test]
#[ignore = "go-parity-gap: digest identity and session memory-arbitrator wiring are not fully transcreated; token estimates have a partial carrier in lib.rs"]
fn test_mem_arbitrator_session() {}

/// `pkg/session/sessmgr/processinfo_test.go:31::TestProcessInfoShallowCP`.
#[test]
#[ignore = "go-parity-carrier: shallow ProcessInfo cloning is tested in tidb-exec::process_info_source"]
fn test_process_info_shallow_cp() {}

/// `pkg/session/starter_bootstrap_file_test.go:41::TestStarterBootstrapFileValidationAndRendering`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap-file parser and renderer are not transcreated"]
fn test_starter_bootstrap_file_validation_and_rendering() {}

/// `pkg/session/starter_bootstrap_file_test.go:68::TestStarterBootstrapFileValidationErrors`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap-file parser is not transcreated"]
fn test_starter_bootstrap_file_validation_errors() {}

/// `pkg/session/starter_bootstrap_file_test.go:114::TestStarterBootstrapFileLoadNoopOutsideStarter`.
#[test]
#[ignore = "go-parity-gap: starter deployment configuration is not transcreated"]
fn test_starter_bootstrap_file_load_noop_outside_starter() {}

/// `pkg/session/starter_bootstrap_file_test.go:136::TestStarterBootstrapFileLoadInStarter`.
#[test]
#[ignore = "go-parity-gap: starter deployment configuration is not transcreated"]
fn test_starter_bootstrap_file_load_in_starter() {}

/// `pkg/session/starter_bootstrap_file_test.go:162::TestStarterBootstrapFileBootstrapBlocks`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap SQL execution is not transcreated"]
fn test_starter_bootstrap_file_bootstrap_blocks() {}

/// `pkg/session/starter_bootstrap_file_test.go:197::TestStarterBootstrapFileInitialBootstrap`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap privilege initialization is not transcreated"]
fn test_starter_bootstrap_file_initial_bootstrap() {}

/// `pkg/session/starter_bootstrap_file_test.go:255::TestStarterBootstrapFileUpgrade`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap upgrade SQL and version persistence are not transcreated"]
fn test_starter_bootstrap_file_upgrade() {}

/// `pkg/session/starter_bootstrap_file_test.go:304::TestStarterBootstrapFileUpgradePartialFailure`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap transactional upgrade is not transcreated"]
fn test_starter_bootstrap_file_upgrade_partial_failure() {}

/// `pkg/session/starter_bootstrap_file_test.go:343::TestStarterBootstrapFileUpgradeSkipsOlderFile`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap version reconciliation is not transcreated"]
fn test_starter_bootstrap_file_upgrade_skips_older_file() {}

/// `pkg/session/starter_bootstrap_file_test.go:366::TestStarterBootstrapStoreVersionGate`.
#[test]
#[ignore = "go-parity-gap: starter bootstrap store/domain gate is not transcreated"]
fn test_starter_bootstrap_store_version_gate() {}

/// `pkg/session/starter_bootstrap_file_test.go:429::TestStarterPrivilegeResetMetadataState`.
#[test]
#[ignore = "go-parity-gap: starter privilege-reset metadata state is not transcreated"]
fn test_starter_privilege_reset_metadata_state() {}

/// `pkg/session/starter_bootstrap_file_test.go:515::TestStarterPrivilegeResetWorkflow`.
#[test]
#[ignore = "go-parity-gap: starter privilege-reset PD/domain workflow is not transcreated"]
fn test_starter_privilege_reset_workflow() {}

/// `pkg/session/starter_bootstrap_file_test.go:624::TestStarterPrivilegeReset`.
#[test]
#[ignore = "go-parity-gap: starter privilege-reset execution is not transcreated"]
fn test_starter_privilege_reset() {}

/// `pkg/session/syssession/main_test.go:24::TestMain` is the Go test harness.
#[test]
#[ignore = "go-parity-gap: Go syssession TestMain harness is not a Rust test surface"]
fn test_syssession_main() {}

/// `pkg/session/syssession/pool_test.go:38::TestNewSessionPool`.
#[test]
#[ignore = "go-parity-gap: syssession pool ownership and mock session context are not transcreated"]
fn test_new_session_pool() {}

/// `pkg/session/syssession/pool_test.go:77::TestSessionPoolGet`.
#[test]
#[ignore = "go-parity-gap: syssession pool ownership and mock session context are not transcreated"]
fn test_session_pool_get() {}

/// `pkg/session/syssession/pool_test.go:123::TestSessionPoolPut`.
#[test]
#[ignore = "go-parity-gap: syssession pool ownership and mock session context are not transcreated"]
fn test_session_pool_put() {}

/// `pkg/session/syssession/pool_test.go:318::TestSessionPoolWithSession`.
#[test]
#[ignore = "go-parity-gap: syssession pool ownership and mock session context are not transcreated"]
fn test_session_pool_with_session() {}

/// `pkg/session/syssession/pool_test.go:383::TestSessionPoolClose`.
#[test]
#[ignore = "go-parity-gap: syssession pool ownership and mock session context are not transcreated"]
fn test_session_pool_close() {}

/// `pkg/session/syssession/session_integration_test.go:31::TestDomainAdvancedSessionPoolInternalSessionRegistry`.
#[test]
#[ignore = "go-parity-gap: Domain advanced session pool and internal-session registry are not transcreated"]
fn test_domain_advanced_session_pool_internal_session_registry() {}

/// `pkg/session/syssession/session_integration_test.go:77::TestDomainAdvancedSessionPoolPutBackDirtySession`.
#[test]
#[ignore = "go-parity-gap: Domain advanced session pool and transaction cleanup are not transcreated"]
fn test_domain_advanced_session_pool_put_back_dirty_session() {}

/// `pkg/session/syssession/session_test.go:186::TestNewInternalSession`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_new_internal_session() {}

/// `pkg/session/syssession/session_test.go:229::TestResignOwnerAndCloseSctx`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_resign_owner_and_close_sctx() {}

/// `pkg/session/syssession/session_test.go:266::TestInternalSessionTransferOwner`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_transfer_owner() {}

/// `pkg/session/syssession/session_test.go:400::TestInternalSessionClose`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_close() {}

/// `pkg/session/syssession/session_test.go:526::TestInternalSessionEnterOperation`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_enter_operation() {}

/// `pkg/session/syssession/session_test.go:636::TestInternalSessionOwnerWithSctx`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_owner_with_sctx() {}

/// `pkg/session/syssession/session_test.go:691::TestInternalSessionAvoidReuse`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_avoid_reuse() {}

/// `pkg/session/syssession/session_test.go:732::TestInternalSessionCheckNoPendingTxn`.
#[test]
#[ignore = "go-parity-gap: syssession ownership state machine is not transcreated"]
fn test_internal_session_check_no_pending_txn() {}
