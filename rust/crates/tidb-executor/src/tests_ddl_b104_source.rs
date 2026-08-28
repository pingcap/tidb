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

//! Ports of the deterministic b104 window (Go declarations 241--300) from
//! `pkg/ddl` at master `25050b53f84fd14c4cfa97a7bb3826876c333c29`.
//!
//! This window is mostly the Go DDL coordinator's private test surface:
//! failpoint-controlled schema states, the DDL job/history tables, metadata
//! locks, domain schema versions, and the running-job scheduler. Those are
//! above this crate's synchronous catalog/DDL driver and are therefore kept as
//! explicit ignored mappings rather than replaced by weaker tests. The one
//! portable SQL contract, `TestDropTables`, runs against the same lifecycle
//! entry point that the executor exposes.

/// Go `pkg/ddl/db_table_test.go:876::TestDropTables` is the one synchronous
/// SQL contract in this window. Go drops existing names even when another
/// name is missing, and reports the missing names after the mutation.
#[test]
fn drop_tables() {
    let mut catalog = crate::Catalog::default();
    let drop = |sql: &str, catalog: &mut crate::Catalog| {
        crate::run_drop_table_in(sql, catalog, "test", tidb_parser::SqlMode::default(), true)
    };

    let error = drop("drop table t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    let error = drop("drop table test2.t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    assert_eq!(
        drop("drop table if exists t1, t2", &mut catalog).unwrap(),
        vec!["test.t2"]
    );
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    assert_eq!(
        drop("drop table if exists t2, t1", &mut catalog).unwrap(),
        vec!["test.t2"]
    );
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    let error = drop("drop table t1, t2", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    let error = drop("drop table t2, t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    assert!(!catalog.contains_in("test", "t1"));
}

/// Go `pkg/ddl/db_table_test.go:909::TestCreateConstraintForTable`.
///
/// The Go contract is CHECK-constraint name uniqueness across separate CREATE
/// and ALTER jobs, plus cross-database metadata visibility. This tier has no
/// DDL job queue or the Go session variable that enables CHECK enforcement;
/// the ON-model boundary is covered in `tests_ddl_check_constraints`.
#[test]
#[ignore = "go-parity-gap: CHECK constraint ON mode, duplicate-name job errors, and cross-database information_schema are outside the synchronous executor catalog"]
fn create_constraint_for_table() {}

/// Go `pkg/ddl/db_table_test.go:933::TestCreateTableHandleAutoIDOnce`.
#[test]
#[ignore = "go-parity-gap: handleAutoIncID failpoint call counts and SHOW TABLE next_row_id require the Go DDL job and session metadata surfaces"]
fn create_table_handle_auto_id_once() {}

/// Go `pkg/ddl/db_table_test.go:952::TestCreateTableWithBR`.
#[test]
#[ignore = "go-parity-gap: BR mode and CreateTableWithInfo are cluster DDL executor APIs, not synchronous catalog DDL"]
fn create_table_with_br() {}

/// Go `pkg/ddl/db_test.go:78::TestGetTimeZone`.
#[test]
#[ignore = "go-parity-gap: SET time_zone, named-zone resolution, and ddl.util.GetTimeZone are session-variable APIs"]
fn get_time_zone() {}

/// Go `pkg/ddl/db_test.go:122::TestIssue22819`.
#[test]
#[ignore = "go-parity-gap: pessimistic transaction schema validation across partition truncate needs sessions, metadata locks, and schema leases"]
fn issue_22819() {}

/// Go `pkg/ddl/db_test.go:146::TestIssue22307`.
#[test]
#[ignore = "go-parity-gap: DML issued from beforeRunOneJobStep during DROP COLUMN needs the online-DDL state machine"]
fn issue_22307() {}

/// Go `pkg/ddl/db_test.go:172::TestAddExpressionIndexRollback`.
#[test]
#[ignore = "go-parity-gap: expression-index backfill rollback, concurrent DML, failpoints, and reorg metadata are not built by this crate"]
fn add_expression_index_rollback() {}

/// Go `pkg/ddl/db_test.go:236::TestDropTableOnTiKVDiskFull`.
#[test]
#[ignore = "go-parity-gap: TiKV disk-full failpoint and mockstore RPC behavior require the storage cluster harness"]
fn drop_table_on_tikv_disk_full() {}

/// Go `pkg/ddl/db_test.go:248::TestRebaseAutoID`.
#[test]
#[ignore = "go-parity-gap: auto-ID rebase ranges, allocator failpoints, and ALTER TABLE AUTO_INCREMENT jobs require the cluster allocator"]
fn rebase_auto_id() {}

/// Go `pkg/ddl/db_test.go:281::TestProcessColumnFlags`.
#[test]
#[ignore = "go-parity-gap: Go processColumnFlags assertions inspect table.Column metadata flags after ALTER jobs; this carrier has no equivalent public flag mutation API"]
fn process_column_flags() {}

/// Go `pkg/ddl/db_test.go:320::TestForbidCacheTableForSystemTable`.
#[test]
#[ignore = "go-parity-gap: system databases, privileges, views, and ALTER TABLE CACHE are outside the executor-only catalog"]
fn forbid_cache_table_for_system_table() {}

/// Go `pkg/ddl/db_test.go:350::TestAlterShardRowIDBits`.
#[test]
#[ignore = "go-parity-gap: shard_row_id_bits and global auto-ID overflow are cluster allocator metadata"]
fn alter_shard_row_id_bits() {}

/// Go `pkg/ddl/db_test.go:386::TestDDLJobErrorCount`.
#[test]
#[ignore = "go-parity-gap: DDL history error counts and afterWaitSchemaSynced failpoint injection require the job queue"]
fn ddl_job_error_count() {}

/// Go `pkg/ddl/db_test.go:414::TestAddIndexFailOnCaseWhenCanExit`.
#[test]
#[ignore = "go-parity-gap: injected CASE WHEN index-builder failure and DDL error-count retry policy require online DDL jobs"]
fn add_index_fail_on_case_when_can_exit() {}

/// Go `pkg/ddl/db_test.go:433::TestCreateTableWithIntegerLengthWarning`.
#[test]
#[ignore = "go-parity-gap: strict integer display-width warnings are session statement diagnostics and no SHOW WARNINGS surface exists here"]
fn create_table_with_integer_length_warning() {}

/// Go `pkg/ddl/db_test.go:487::TestShowCountWarningsOrErrors`.
#[test]
#[ignore = "go-parity-gap: SHOW COUNT(*) WARNINGS/ERRORS and session diagnostic counters are outside the executor catalog driver"]
fn show_count_warnings_or_errors() {}

/// Go `pkg/ddl/db_test.go:515::TestIssue60047`.
#[test]
#[ignore = "go-parity-gap: concurrent INSERT ON DUPLICATE KEY UPDATE during partitioned ADD COLUMN needs online-DDL schema states"]
fn issue_60047() {}

/// Go `pkg/ddl/db_test.go:559::TestCancelJobWriteConflict`.
#[test]
#[ignore = "go-parity-gap: ADMIN CANCEL DDL JOB, commit retry failpoints, and add-index reorganization are not implemented in this tier"]
fn cancel_job_write_conflict() {}

/// Go `pkg/ddl/db_test.go:610::TestTxnSavepointWithDDL`.
#[test]
#[ignore = "go-parity-gap: pessimistic savepoints committing across concurrent ALTER jobs need session transactions and schema validation"]
fn txn_savepoint_with_ddl() {}

/// Go `pkg/ddl/db_test.go:667::TestSnapshotVersion`.
#[test]
#[ignore = "go-parity-gap: domain schema syncer snapshots and historical metadata require the cluster domain"]
fn snapshot_version() {}

/// Go `pkg/ddl/db_test.go:729::TestSchemaValidator`.
#[test]
#[ignore = "go-parity-gap: schema validator reload failpoints and lease timing require the domain/schema-sync service"]
fn schema_validator() {}

/// Go `pkg/ddl/db_test.go:782::TestLogAndShowSlowLog`.
#[test]
#[ignore = "go-parity-gap: asynchronous domain slow-query collection and SHOW SLOW query filtering are not executor-catalog APIs"]
fn log_and_show_slow_log() {}

/// Go `pkg/ddl/db_test.go:829::TestReportingMinStartTimestamp`.
#[test]
#[ignore = "go-parity-gap: InfoSyncer session-manager reporting and transaction timestamp oracle are cluster services"]
fn reporting_min_start_timestamp() {}

/// Go `pkg/ddl/db_test.go:857::TestBuildMaxLengthIndexWithNonRestrictedSqlMode`.
#[test]
#[ignore = "go-parity-gap: the Go test requires every charset, SQL-mode warnings, and SHOW CREATE rendering; this tier has no equivalent complete surface"]
fn build_max_length_index_with_non_restricted_sql_mode() {}

/// Go `pkg/ddl/db_test.go:971::TestTiDBDownBeforeUpdateGlobalVersion`.
#[test]
#[ignore = "go-parity-gap: simulated DDL-server failure before global schema-version update requires the distributed DDL owner"]
fn tidb_down_before_update_global_version() {}

/// Go `pkg/ddl/db_test.go:985::TestDDLBlockedCreateView`.
#[test]
#[ignore = "go-parity-gap: CREATE VIEW interleaved at ALTER TABLE StateWriteOnly needs the online-DDL job queue and session concurrency"]
fn ddl_blocked_create_view() {}

/// Go `pkg/ddl/db_test.go:1008::TestHashPartitionAddColumn`.
#[test]
#[ignore = "go-parity-gap: partitioned ADD COLUMN with DML injected at StateWriteOnly needs online-DDL schema states"]
fn hash_partition_add_column() {}

/// Go `pkg/ddl/db_test.go:1026::TestSetInvalidDefaultValueAfterModifyColumn`.
#[test]
#[ignore = "go-parity-gap: concurrent ALTER COLUMN default during MODIFY COLUMN's DeleteOnly state needs the online-DDL job queue"]
fn set_invalid_default_value_after_modify_column() {}

/// Go `pkg/ddl/db_test.go:1057::TestMDLTruncateTable`.
#[test]
#[ignore = "go-parity-gap: metadata-lock blocking and concurrent table-ID replacement require sessions, transactions, and schema leases"]
fn mdl_truncate_table() {}

/// Go `pkg/ddl/db_test.go:1130::TestTruncateTableAndSchemaDependence`.
#[test]
#[ignore = "go-parity-gap: DROP DATABASE ordered behind a truncate job's schema-sync hook requires the distributed DDL scheduler"]
fn truncate_table_and_schema_dependence() {}

/// Go `pkg/ddl/db_test.go:1169::TestInsertIgnore`.
#[test]
#[ignore = "go-parity-gap: INSERT IGNORE injected during unique-index backfill requires online DDL and reorg progress metadata"]
fn insert_ignore() {}

/// Go `pkg/ddl/db_test.go:1199::TestDDLJobErrEntrySizeTooLarge`.
#[test]
#[ignore = "go-parity-gap: injected KV entry-size failure and subsequent DDL job recovery require the DDL job queue"]
fn ddl_job_err_entry_size_too_large() {}

/// Go `pkg/ddl/db_test.go:1244::TestResumeSystemPausedDDLJobWithKVDiskFullReason`.
#[test]
#[ignore = "go-parity-gap: ADMIN RESUME DDL JOBS mutates encoded mysql.tidb_ddl_job records, which this crate does not model"]
fn resume_system_paused_ddl_job_with_kv_disk_full_reason() {}

/// Go `pkg/ddl/db_test.go:1313::TestAdminAlterDDLJobUpdateSysTable`.
#[test]
#[ignore = "go-parity-gap: ADMIN ALTER DDL JOB edits persisted reorg metadata in mysql.tidb_ddl_job"]
fn admin_alter_ddl_job_update_sys_table() {}

/// Go `pkg/ddl/db_test.go:1349::TestAdminAlterDDLJobUnsupportedCases`.
#[test]
#[ignore = "go-parity-gap: ADMIN ALTER DDL JOB parsing, validation, and job-type dispatch require the system DDL-job table"]
fn admin_alter_ddl_job_unsupported_cases() {}

/// Go `pkg/ddl/db_test.go:1414::TestAdminAlterDDLJobCommitFailed`.
#[test]
#[ignore = "go-parity-gap: injected ADMIN ALTER DDL JOB commit failure requires transactional job metadata"]
fn admin_alter_ddl_job_commit_failed() {}

/// Go `pkg/ddl/db_test.go:1440::TestGetAllTableInfos`.
#[test]
#[ignore = "go-parity-gap: comparison with meta.IterAllTables over TiKV and information_schema needs the cluster metadata store"]
fn get_all_table_infos() {}

/// Go `pkg/ddl/db_test.go:1491::TestGetVersionFailed`.
#[test]
#[ignore = "go-parity-gap: injected current-version failures during schema synchronization require domain DDL services"]
fn get_version_failed() {}

/// Go `pkg/ddl/ddl_algorithm_test.go:37::TestFindAlterAlgorithm`.
#[test]
#[ignore = "go-parity-gap: ResolveAlterAlgorithm is a private Go DDL scheduler helper and this executor exposes no equivalent algorithm resolver"]
fn find_alter_algorithm() {}

/// Go `pkg/ddl/ddl_error_test.go:32::TestTableError`.
#[test]
#[ignore = "go-parity-gap: failpoint-corrupted schema/table IDs exercise DDL job metadata validation, not synchronous catalog errors"]
fn table_error() {}

/// Go `pkg/ddl/ddl_error_test.go:60::TestViewError`.
#[test]
#[ignore = "go-parity-gap: the Go fixture is a domain/DDL error harness and this carrier has no matching job-error path"]
fn view_error() {}

/// Go `pkg/ddl/ddl_error_test.go:68::TestForeignKeyError`.
#[test]
#[ignore = "go-parity-gap: corrupted schema IDs during ADD/DROP FOREIGN KEY require DDL job failpoints"]
fn foreign_key_error() {}

/// Go `pkg/ddl/ddl_error_test.go:83::TestIndexError`.
#[test]
#[ignore = "go-parity-gap: corrupted schema IDs during index DDL require DDL job failpoints"]
fn index_error() {}

/// Go `pkg/ddl/ddl_error_test.go:100::TestColumnError`.
#[test]
#[ignore = "go-parity-gap: corrupted schema/table IDs during column DDL require DDL job failpoints"]
fn column_error() {}

/// Go `pkg/ddl/ddl_error_test.go:142::TestCreateDatabaseError`.
#[test]
#[ignore = "go-parity-gap: corrupted schema ID during CREATE DATABASE requires the Go DDL executor failpoint"]
fn create_database_error() {}

/// Go `pkg/ddl/ddl_error_test.go:153::TestCreateIndexErrTooManyKeys`.
#[test]
#[ignore = "go-parity-gap: Go's 512-index global configuration constant has no corresponding public Rust DDL limit"]
fn create_index_err_too_many_keys() {}

/// Go `pkg/ddl/ddl_history_test.go:36::TestDDLHistoryBasic`.
#[test]
#[ignore = "go-parity-gap: AddHistoryDDLJob, ScanHistoryDDLJobs, and encoded history metadata require TiDB's mysql DDL tables"]
fn ddl_history_basic() {}

/// Go `pkg/ddl/ddl_history_test.go:129::TestScanHistoryDDLJobsWithErrorLimit`.
#[test]
#[ignore = "go-parity-gap: history scanning over meta.Mutator is not represented by the executor catalog"]
fn scan_history_ddl_jobs_with_error_limit() {}

/// Go `pkg/ddl/ddl_running_jobs_test.go:79::TestRunningJobs`.
#[test]
#[ignore = "go-parity-gap: private runningJobs conflict tracking is part of the distributed DDL scheduler and is not exposed here"]
fn running_jobs() {}

/// Go `pkg/ddl/ddl_running_jobs_test.go:147::TestSchemaPolicyAndResourceGroup`.
#[test]
#[ignore = "go-parity-gap: scheduler conflicts over placement policies and resource groups require DDL owner state"]
fn schema_policy_and_resource_group() {}

/// Go `pkg/ddl/ddl_running_jobs_test.go:223::TestExclusiveShared`.
#[test]
#[ignore = "go-parity-gap: private runningJobs exclusive/shared/pending scheduler state has no executor-catalog carrier"]
fn exclusive_shared() {}

/// Go `pkg/ddl/ddl_test.go:90::TestGetIntervalFromPolicy`.
#[test]
#[ignore = "go-parity-gap: getIntervalFromPolicy is a private DDL owner backoff helper with no Rust implementation"]
fn get_interval_from_policy() {}

/// Go `pkg/ddl/ddl_test.go:128::TestModifyColumn`.
#[test]
#[ignore = "go-parity-gap: the Go test calls private checkModifyTypes over model.ColumnInfo; the Rust ALTER path has a different non-public representation"]
fn modify_column() {}

/// Go `pkg/ddl/ddl_test.go:169::TestProcessModifyColumnOptionsGenerated`.
#[test]
#[ignore = "go-parity-gap: ProcessModifyColumnOptions mutates Go table.Column generated-expression metadata directly"]
fn process_modify_column_options_generated() {}

/// Go `pkg/ddl/ddl_test.go:221::TestFieldCase`.
#[test]
#[ignore = "go-parity-gap: private checkDuplicateColumn error construction over Go model.ColumnInfo is not a public Rust helper"]
fn field_case() {}

/// Go `pkg/ddl/ddl_test.go:233::TestIgnorableSpec`.
#[test]
#[ignore = "go-parity-gap: isIgnorableSpec is a private Go ALTER scheduler classification helper"]
fn ignorable_spec() {}

/// Go `pkg/ddl/ddl_test.go:260::TestError`.
#[test]
#[ignore = "go-parity-gap: Go terror error-code conversion is not a Rust DDL scheduler API"]
fn error() {}

/// Go `pkg/ddl/ddl_test.go:273::TestCheckDuplicateConstraint`.
#[test]
#[ignore = "go-parity-gap: private checkDuplicateConstraint and Go terror message identity are not exposed by this carrier"]
fn check_duplicate_constraint() {}

/// Go `pkg/ddl/ddl_test.go:295::TestGetTableDataKeyRanges`.
#[test]
#[ignore = "go-parity-gap: getTableDataKeyRanges is a private flashback/cluster key-range helper, not the executor table scan range API"]
fn get_table_data_key_ranges() {}
