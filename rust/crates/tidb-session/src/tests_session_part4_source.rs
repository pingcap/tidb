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

//! Source-backed carriers for `pkg/session.part4`.
//!
//! The assigned slice is the deterministic items 181--240 of the
//! `pkg/session` `Test*` inventory on `origin/master`. Items 181--201 are
//! already represented by the shared bootstrap/common source carrier; this
//! module owns the remaining items 202--240. The assigned Go tests exercise
//! Go TestKit over bootstrap/domain/storage, SQL execution, failpoints,
//! protocol state, and TiKV-specific behavior. Those seams are not exposed as
//! a complete Rust session test surface yet, so each declaration is retained
//! as an explicit ignored parity carrier rather than replaced with a weaker
//! assertion.

#![cfg(test)]

/// `pkg/session/test/meta/session_test.go:45::TestInitDDLTables`.
// go-parity-gap: DDL table initialization mutates Go storage metadata and
// version state (production: pkg/session/session.go:4255).
#[test]
#[ignore = "go-parity-gap: Go DDL bootstrap tables and metadata are not transcreated"]
fn test_init_ddl_tables() {}

/// `pkg/session/test/meta/session_test.go:103::TestInitMetaTable`.
// go-parity-gap: the test compares bootstrap-created table metadata through Go
// Domain/InfoSchema (production: pkg/session/session.go:4454).
#[test]
#[ignore = "go-parity-gap: Go bootstrap catalog metadata is not transcreated"]
fn test_init_meta_table() {}

/// `pkg/session/test/meta/session_test.go:141::TestMetaTableRegion`.
// go-parity-gap: region/key assertions require Go TiKV tablecodec and mock
// storage (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go TiKV region/tablecodec integration is not transcreated"]
fn test_meta_table_region() {}

/// `pkg/session/test/meta/session_test.go:175::TestRecordTTLRows`.
// go-parity-gap: TTL row accounting is accumulated by Go session transaction
// commit handling (production: pkg/session/session.go:1011).
#[test]
#[ignore = "go-parity-gap: Go TTL transaction metrics are not transcreated"]
fn test_record_ttl_rows() {}

/// `pkg/session/test/meta/session_test.go:215::TestInformationSchemaCreateTime`.
// go-parity-gap: INFORMATION_SCHEMA create-time and timezone behavior require
// Go Domain schema metadata and SQL execution (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go information-schema/timezone behavior is not transcreated"]
fn test_information_schema_create_time() {}

/// `pkg/session/test/meta/session_test.go:247::TestNextgenBootstrap`.
// go-parity-gap: reserved bootstrap IDs are assigned by Go's bootstrap Domain
// (production: pkg/session/session.go:4454).
#[test]
#[ignore = "go-parity-gap: Go next-generation bootstrap catalog is not transcreated"]
fn test_nextgen_bootstrap() {}

/// `pkg/session/test/nontransactionaltest/main_test.go:29::TestMain`.
// go-parity-gap: this is the Go testing/goleak process harness, not a session
// behavior surface (production harness: pkg/testkit/testmain/testmain.go:1).
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust test surface"]
fn test_nontransactionaltest_main() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:32::TestNonTransactionalDMLSharding`.
// go-parity-gap: batch non-transactional DML is implemented by Go session
// nontransactional execution (production: pkg/session/nontransactional.go:428).
#[test]
#[ignore = "go-parity-gap: Go non-transactional batch DML is not transcreated"]
fn test_non_transactional_dml_sharding() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:127::TestNonTransactionalDMLErrorMessage`.
// go-parity-gap: failpoint-controlled batch DML errors come from Go's
// nontransactional worker (production: pkg/session/nontransactional.go:428).
#[test]
#[ignore = "go-parity-gap: Go non-transactional DML failpoint/error protocol is not transcreated"]
fn test_non_transactional_dml_error_message() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:258::TestNonTransactionalWithCheckConstraint`.
// go-parity-gap: batch DML consistency checks depend on Go snapshot/session
// state and the nontransactional executor (production: pkg/session/nontransactional.go:428).
#[test]
#[ignore = "go-parity-gap: Go non-transactional consistency checks are not transcreated"]
fn test_non_transactional_with_check_constraint() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:373::TestNonTransactionalDMLWorkWithForeignKey`.
// go-parity-gap: foreign-key checks across partial batch jobs require Go SQL
// execution and storage transactions (production: pkg/session/nontransactional.go:428).
#[test]
#[ignore = "go-parity-gap: Go non-transactional foreign-key DML is not transcreated"]
fn test_non_transactional_dml_work_with_foreign_key() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:429::TestNonTransactionalMetrics`.
// go-parity-gap: the tested counters are updated by Go session nontransactional
// execution (production: pkg/session/nontransactional.go:428).
#[test]
#[ignore = "go-parity-gap: Go non-transactional metrics are not transcreated"]
fn test_non_transactional_metrics() {}

/// `pkg/session/test/nontransactionaltest/nontransactional_test.go:513::TestNonTransactionalDmlIgnoreMaxExecutionTime`.
// go-parity-gap: the test depends on the Go max-execution-time failpoint in the
// batch worker (production: pkg/session/nontransactional.go:541).
#[test]
#[ignore = "go-parity-gap: Go non-transactional execution-time failpoint is not transcreated"]
fn test_non_transactional_dml_ignore_max_execution_time() {}

/// `pkg/session/test/privileges/main_test.go:29::TestMain`.
// go-parity-gap: this is the Go testing/goleak process harness, not a session
// behavior surface (production harness: pkg/testkit/testmain/testmain.go:1).
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust test surface"]
fn test_privileges_main() {}

/// `pkg/session/test/privileges/privileges_test.go:26::TestSkipWithGrant`.
// go-parity-gap: SkipWithGrant changes Go privilege-manager authorization
// (production: pkg/privilege/privileges/privileges.go:56).
#[test]
#[ignore = "go-parity-gap: Go SkipWithGrant/global privilege manager state is not transcreated"]
fn test_skip_with_grant() {}

/// `pkg/session/test/privileges/privileges_test.go:47::TestSessionAuth`.
// go-parity-gap: authentication delegates to Go privilege tables and session
// Auth (production: pkg/session/session.go:3718).
#[test]
#[ignore = "go-parity-gap: Go session authentication against privilege tables is not transcreated"]
fn test_session_auth() {}

/// `pkg/session/test/resourcegrouptest/resource_group_test.go:25::TestResourceGroupHintInTxn`.
// go-parity-gap: resource-group hints are applied during Go statement
// execution and expression evaluation (production: pkg/expression/builtin_info.go:341).
#[test]
#[ignore = "go-parity-gap: Go resource-group transaction/failpoint behavior is not transcreated"]
fn test_resource_group_hint_in_txn() {}

/// `pkg/session/test/schematest/main_test.go:29::TestMain`.
// go-parity-gap: this is the Go testing/goleak process harness, not a session
// behavior surface (production harness: pkg/testkit/testmain/testmain.go:1).
#[test]
#[ignore = "go-parity-gap: Go TestMain/goleak harness is not a Rust test surface"]
fn test_schematest_main() {}

/// `pkg/session/test/schematest/schema_test.go:57::TestPrepareStmtCommitWhenSchemaChanged`.
// go-parity-gap: prepared DML commit validation requires Go schema/domain
// coordination (production: pkg/session/session.go:1126).
#[test]
#[ignore = "go-parity-gap: Go schema-change transaction validation is not transcreated"]
fn test_prepare_stmt_commit_when_schema_changed() {}

/// `pkg/session/test/schematest/schema_test.go:83::TestRetrySchemaChangeForEmptyChange`.
// go-parity-gap: retrying schema changes across concurrent sessions requires
// Go Domain/schema lease state (production: pkg/session/session.go:1126).
#[test]
#[ignore = "go-parity-gap: Go schema-change retry lifecycle is not transcreated"]
fn test_retry_schema_change_for_empty_change() {}

/// `pkg/session/test/schematest/schema_test.go:105::TestTableReaderChunk`.
// go-parity-gap: chunk reads and region splitting require Go executor/TiKV
// storage integration (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go table-reader chunk/TiKV integration is not transcreated"]
fn test_table_reader_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:158::TestInsertExecChunk`.
// go-parity-gap: INSERT execution chunking is exercised through Go TestKit
// and record sets (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go INSERT executor chunking is not transcreated"]
fn test_insert_exec_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:193::TestUpdateExecChunk`.
// go-parity-gap: UPDATE execution chunking is exercised through Go TestKit
// and record sets (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go UPDATE executor chunking is not transcreated"]
fn test_update_exec_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:230::TestDeleteExecChunk`.
// go-parity-gap: DELETE execution chunking is exercised through Go TestKit
// and record sets (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go DELETE executor chunking is not transcreated"]
fn test_delete_exec_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:260::TestDeleteMultiTableExecChunk`.
// go-parity-gap: multi-table DELETE chunking requires Go executor and storage
// mutation state (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go multi-table DELETE chunking is not transcreated"]
fn test_delete_multi_table_exec_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:312::TestIndexLookUpReaderChunk`.
// go-parity-gap: index-lookup reader chunks and region splitting require Go
// executor/TiKV integration (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go index-lookup reader/TiKV integration is not transcreated"]
fn test_index_look_up_reader_chunk() {}

/// `pkg/session/test/schematest/schema_test.go:377::TestTxnSize`.
// go-parity-gap: transaction write-size accounting is owned by Go KV transaction
// options (production: pkg/session/txn.go:324).
#[test]
#[ignore = "go-parity-gap: Go transaction size accounting is not transcreated"]
fn test_txn_size() {}

/// `pkg/session/test/schematest/schema_test.go:393::TestValidationRecursion`.
// go-parity-gap: recursive global sysvar validation requires Go SessionVars and
// GlobalVarsAccessor (production: pkg/sessionctx/variable/session.go:1).
#[test]
#[ignore = "go-parity-gap: Go recursive sysvar validation is not transcreated"]
fn test_validation_recursion() {}

/// `pkg/session/test/session_test.go:61::TestSchemaCheckerSQL`.
// go-parity-gap: commit-time schema checking uses Go's schema-change marker
// and Domain validator (production: pkg/session/session.go:1126).
#[test]
#[ignore = "go-parity-gap: Go SQL schema-checker/domain integration is not transcreated"]
fn test_schema_checker_sql() {}

/// `pkg/session/test/session_test.go:133::TestLoadSchemaFailed`.
// go-parity-gap: schema reload failure and lease recovery require Go Domain
// lifecycle and failpoints (production: pkg/session/session.go:1126).
#[test]
#[ignore = "go-parity-gap: Go schema reload failure/recovery is not transcreated"]
fn test_load_schema_failed() {}

/// `pkg/session/test/session_test.go:188::TestWriteOnMultipleCachedTable`.
// go-parity-gap: cached-table reads and writes require Go table-cache/session
// state (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go table-cache write coordination is not transcreated"]
fn test_write_on_multiple_cached_table() {}

/// `pkg/session/test/session_test.go:229::TestFixSetTiDBSnapshotTS`.
// go-parity-gap: snapshot timestamp and schema selection require Go session
// variables plus storage timestamps (production: pkg/session/session.go:3530).
#[test]
#[ignore = "go-parity-gap: Go snapshot/schema session state is not transcreated"]
fn test_fix_set_tidb_snapshot_ts() {}

/// `pkg/session/test/session_test.go:253::TestPrepareZero`.
// go-parity-gap: zero-date prepared execution depends on Go SQL mode/type
// conversion and record-set execution (production: pkg/session/session.go:3420).
#[test]
#[ignore = "go-parity-gap: Go prepared zero-date conversion is not transcreated"]
fn test_prepare_zero() {}

/// `pkg/session/test/session_test.go:270::TestPrimaryKeyAutoIncrement`.
// go-parity-gap: auto-increment allocation and prepared parameter coercion
// require Go DML execution state (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go auto-increment/prepared DML behavior is not transcreated"]
fn test_primary_key_auto_increment() {}

/// `pkg/session/test/session_test.go:295::TestParseWithParams`.
// go-parity-gap: parameter escaping/restoration is Go RestrictedSQLExecutor
// behavior (production: pkg/session/session.go:1990).
#[test]
#[ignore = "go-parity-gap: Go ParseWithParams charset/escape behavior is not transcreated"]
fn test_parse_with_params() {}

/// `pkg/session/test/session_test.go:334::TestDoDDLJobQuit`.
// go-parity-gap: DDL cancellation requires Go Domain/DDL executor lifecycle
// and failpoints (production: pkg/session/session.go:4454).
#[test]
#[ignore = "go-parity-gap: Go DDL cancellation lifecycle is not transcreated"]
fn test_do_ddl_job_quit() {}

/// `pkg/session/test/session_test.go:362::TestProcessInfoIssue22068`.
// go-parity-gap: process inspection during a running query requires Go session
// manager/process state (production: pkg/session/session.go:5286).
#[test]
#[ignore = "go-parity-gap: Go running-query process info is not transcreated"]
fn test_process_info_issue22068() {}

/// `pkg/session/test/session_test.go:379::TestPerStmtTaskID`.
// go-parity-gap: statement task IDs are assigned in Go statement execution and
// distributed-query context (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go per-statement task-ID lifecycle is not transcreated"]
fn test_per_stmt_task_id() {}

/// `pkg/session/test/session_test.go:396::TestStmtHints`.
// go-parity-gap: hint parsing and application mutate Go StmtCtx/session vars
// during execution (production: pkg/session/session.go:1861).
#[test]
#[ignore = "go-parity-gap: Go statement-hint/session state is not transcreated"]
fn test_stmt_hints() {}
