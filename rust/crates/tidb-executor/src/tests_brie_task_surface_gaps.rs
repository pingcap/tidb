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

//! Gap tests for Go `pkg/executor/brie_test.go` and
//! `pkg/executor/brie_utils_test.go`: the BACKUP/RESTORE (BRIE) task queue
//! and its create-database/table replay helpers. The statement parses on
//! this tier (`tidb_ast::stmt::brie` mirrors `ast.BRIEStmt`) but there is no
//! BRIE executor, no `globalBRIEQueue`, and no BR glue -- `pkg/executor`
//! defines all of them Go-side (`pkg/executor/brie.go:109 brieTaskInfo`,
//! `:61 clearInterval`, `:691 tidbGlue`, `:744 GetVersion`,
//! `pkg/executor/brie_utils.go:56/:89/:115/:163`). Every Go contract in the
//! two files is recorded here rather than approximated.

/// Go `pkg/executor/brie_test.go:44::TestGlueGetVersion`:
/// `tidbGlue.GetVersion` (`pkg/executor/brie.go:744`, `"TiDB\n" +
/// printer.GetTiDBInfo()`) advertises `Release Version`, `Git Commit Hash`
/// and `GoVersion` -- the fields BR surfaces in `SHOW BR BACKUP` outputs.
#[test]
#[ignore = "go-parity-gap: no BR glue; tidbGlue.GetVersion (pkg/executor/brie.go:744) has no Rust counterpart"]
fn glue_get_version_reports_release_and_commit() {}

/// Go `pkg/executor/brie_test.go:76::TestFetchShowBRIE`:
/// `ShowExec.fetchShowBRIE` drains `globalBRIEQueue` in task-id order, one
/// row per task (`id, storage, Wait, 0, queue/exec/finish times, conn_id,
/// message|NULL`); a fetched task is removed, a re-registered task is
/// fetched again, `clearTask` (`pkg/executor/brie.go:219`) only drops tasks
/// older than `clearInterval` (`:61`, 10m) or `outdatedDuration` (`:63`).
#[test]
#[ignore = "go-parity-gap: no globalBRIEQueue or SHOW BACKUPS executor; fetchShowBRIE (pkg/executor/show.go) and brieQueue (pkg/executor/brie.go:109-229) have no Rust counterpart"]
fn fetch_show_brie_drains_the_task_queue_once_per_registration() {}

/// Go `pkg/executor/brie_test.go:151::TestBRIEBuilderOptions`:
/// `BACKUP TABLE ... CHECKSUM_CONCURRENCY = 4 IGNORE_STATS = 1
/// COMPRESSION_LEVEL = 4 COMPRESSION_TYPE = 'lz4' ENCRYPTION_METHOD =
/// 'aes256-ctr' ENCRYPTION_KEYFILE = '/tmp/keyfile'` flows from the parsed
/// options through `buildBRIE` (`pkg/executor/brie.go:249`, with the
/// `modifyStore` failpoint at `:321` forcing tikv) into `backupCfg`:
/// Checksum=false, ChecksumConcurrency=4, CompressionLevel=4,
/// IgnoreStats=true, LZ4 compression, AES256-CTR cipher.
#[test]
#[ignore = "go-parity-gap: no BRIE executor builder or backuppb/encryptionpb config; buildBRIE (pkg/executor/brie.go:249) has no Rust counterpart"]
fn brie_builder_options_reach_backup_config() {}

/// Go `pkg/executor/brie_utils_test.go:41::TestSplitBatchCreateTableWithTableId`:
/// `splitBatchCreateTable` with `ddl.WithIDAllocated(true)` keeps caller
/// table ids (`tidb_table_id` 124/125 survive) and joins the per-table
/// CREATE statements into ONE `QueryString`; without the option the reused
/// id 124 is rejected and a fresh id > the allocator's current value is
/// generated (`pkg/executor/brie_utils.go:163`).
#[test]
#[ignore = "go-parity-gap: no splitBatchCreateTable or ddl.WithIDAllocated replay path; pkg/executor/brie_utils.go:163 has no Rust counterpart"]
fn split_batch_create_table_keeps_reused_table_ids_when_allocated() {}

/// Go `pkg/executor/brie_utils_test.go:122::TestSplitBatchCreateTable`:
/// with the `RestoreBatchCreateTableEntryTooLarge` failpoint returning 1
/// after the first successful sub-batch, the remaining CREATE statements are
/// re-batched and executed one at a time in REVERSE order (tables_3 first,
/// `admin show ddl jobs` shows three public `create tables` jobs) with the
/// original ids kept (`tidb_table_id` 1234/1235/1236).
#[test]
#[ignore = "go-parity-gap: needs the ddl failpoint RestoreBatchCreateTableEntryTooLarge and admin-show-ddl-jobs; no Rust counterpart"]
fn split_batch_create_table_retries_remainder_one_by_one_after_entry_too_large() {}

/// Go `pkg/executor/brie_utils_test.go:196::TestSplitBatchCreateTableFailWithEntryTooLarge`:
/// with the failpoint returning 0, the FIRST sub-batch write hits
/// `kv.ErrEntryTooLarge` and the error propagates after exactly the first
/// CREATE landed in `QueryString`.
#[test]
#[ignore = "go-parity-gap: needs the ddl failpoint and kv.ErrEntryTooLarge transaction sizing; no Rust counterpart"]
fn split_batch_create_table_fails_with_entry_too_large() {}

/// Go `pkg/executor/brie_utils_test.go:233::TestBRIECreateDatabase`:
/// `BRIECreateDatabase` (`pkg/executor/brie_utils.go:56`) creates the
/// database with the restored schema's charset/collation, preserves the
/// caller's `QueryString` across both calls (empty `brComment` included),
/// and both `db_1`/`db_2` are usable.
#[test]
#[ignore = "go-parity-gap: no BRIECreateDatabase; pkg/executor/brie_utils.go:56 has no Rust counterpart"]
fn brie_create_database_preserves_query_string() {}

/// Go `pkg/executor/brie_utils_test.go:277::TestBRIECreateTable`:
/// `BRIECreateTable` (`pkg/executor/brie_utils.go:89`) replays a mocked
/// public table info (int pk + json + varchar) under a forced id, twice with
/// different names/ids, preserving the caller's `QueryString`; both tables
/// `DESC` cleanly.
#[test]
#[ignore = "go-parity-gap: no BRIECreateTable; pkg/executor/brie_utils.go:89 has no Rust counterpart"]
fn brie_create_table_preserves_query_string() {}

/// Go `pkg/executor/brie_utils_test.go:304::TestBRIECreateTables`:
/// `BRIECreateTables` (`pkg/executor/brie_utils.go:115`) batch-creates 100
/// tables keyed by schema name in one call, again preserving `QueryString`.
#[test]
#[ignore = "go-parity-gap: no BRIECreateTables; pkg/executor/brie_utils.go:115 has no Rust counterpart"]
fn brie_create_tables_batches_all_schema_tables() {}

/// Go `pkg/executor/brie_utils_test.go:390::TestSplitTablesQueryMatch`: with
/// a fake DDL executor failing every batch above one table (alternating
/// ErrTxnTooLarge/ErrEntryTooLarge), the QueryString sent per attempt lists
/// the batch's restored CREATE statements joined by `;`, the retried
/// remainder shrinks one table at a time (t1+t2, then t1), and each table's
/// successful attempt is recorded exactly once per schema (`test` gets 3
/// attempts, `test2` one; successes: t1, t2, t3 one each).
#[test]
#[ignore = "go-parity-gap: needs an injectable fake DDL executor behind splitTables; the Rust ddl path has no such seam"]
fn split_tables_query_match_tracks_each_tables_successful_attempt() {}
