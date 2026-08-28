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

//! Port ledger for `pkg/ddl/jobsubmit/submit_test.go` (pkg/ddl.part7 items
//! 416-419 of the local enumeration). All four drive
//! `jobsubmit.SubmitBatch` over an embedded-unistore store plus systable
//! manager; no Rust carrier exists, so all are documentary gap ports.

/// GO PORT of `pkg/ddl/jobsubmit/submit_test.go:132
/// TestSubmitBatchEnqueuesTableModeJob`.
///
/// Re-derived contract: `SubmitBatch` on one `BuildAlterTableModeJob` spec
/// allocates a fresh job ID above the current global ID, sets state to
/// Queueing with a non-zero StartTS and BDRRole none, and the persisted
/// `mysql.tidb_ddl_job` row carries type ActionAlterTableMode, schema/table
/// IDs 100/200, query "skip", and lowercased involving schema info
/// ("testdb"."t1"). Sub-cases: submitting with mixed-case SchemaName/
/// TableName/InvolvingSchemaInfo normalizes them all to lowercase (keeping
/// InvolvingAll sentinels) both in-memory and on the persisted row; a nil
/// TraceInfo is initialized to the empty `&tracing.TraceInfo{}`; and a
/// pre-set TraceInfo pointer survives submission unchanged.
#[test]
#[ignore = "go-parity-gap: jobsubmit.SubmitBatch (pkg/ddl/jobsubmit/submit.go) and the systable/mysql.tidb_ddl_job surface are not transcreated"]
fn submit_batch_enqueues_table_mode_job_with_normalized_names_and_trace() {}

/// GO PORT of `pkg/ddl/jobsubmit/submit_test.go:224
/// TestSubmitBatchAllocatesIDsAndInsertsJob`.
///
/// Re-derived contract: a JobVersion2 create-table spec whose TableInfo has
/// a two-definition partition gets job ID, table ID, and BOTH partition
/// definition IDs allocated fresh (all above the initial global ID), the
/// allocated table ID is written back into `CreateTableArgs.TableInfo.ID`,
/// and the persisted job row's table_ids column lists exactly that table ID.
#[test]
#[ignore = "go-parity-gap: SubmitBatch's ID allocation/insert transaction has no Rust carrier"]
fn submit_batch_allocates_table_and_partition_ids_before_insert() {}

/// GO PORT of `pkg/ddl/jobsubmit/submit_test.go:263
/// TestSubmitBatchChecksAndPauseState`.
///
/// Re-derived contract, four guards in order: an involving-schema-info entry
/// with an empty table name fails with "must have non-empty name" and
/// nothing is inserted; an existing flashback-cluster job (found from the
/// min job ID) blocks submission with "have flashback cluster job"; a BDR
/// primary role denies a CDC-write-source-0 DDL with a "bdr role" error and
/// inserts nothing; and while the cluster is upgrading, a non-system DDL is
/// still ENQUEUED but lands in state Pausing with AdminCommandBySystem.
#[test]
#[ignore = "go-parity-gap: SubmitBatch's flashback/BDR/upgrade guards (pkg/ddl/jobsubmit/submit.go:66-140) are not transcreated"]
fn submit_batch_guards_flashback_bdr_and_upgrade_state() {}

/// GO PORT of `pkg/ddl/jobsubmit/submit_test.go:308
/// TestSubmitBatchRetryCleanup`.
///
/// Re-derived contract: with `mockGenGIDRetryableError` failing the first
/// global-ID generation, `BeforeInsertWithAssignedIDs` runs twice — the
/// first attempt's assigned ID is recorded and its cleanup function invoked
/// exactly once, and the successful second attempt assigns a NEW ID — so
/// the assigned-IDs list has 2 entries while cleanupIDs has 1, matching the
/// first ID.
#[test]
#[ignore = "go-parity-gap: needs the retry cleanup callback contract of SubmitBatch, not transcreated"]
fn submit_batch_retry_cleans_up_previously_assigned_ids() {}
