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

//! Gap tests for the remaining Go `pkg/executor/compact_table_test.go` cases
//! in executor enumeration items 181--190. The Go tests drive
//! `CompactTableTiFlashExec` (`pkg/executor/compact_table.go:65`) through
//! mocked TiFlash `CompactRequest` responses and assert warning, retry,
//! continuation-key, store-cancellation, and partition behavior. This Rust
//! crate has no `ALTER TABLE ... COMPACT` executor, TiFlash RPC mock, or
//! session warning surface.

/// Go `pkg/executor/compact_table_test.go:208::TestCompactTableErrorInHalfway`.
/// `storeCompactTask::compactOnePhysicalTable` (`pkg/executor/compact_table.go:238`)
/// must turn a second-request busy response into a warning after a prior
/// continuation request succeeded.
#[test]
#[ignore = "go-parity-gap: TiFlash CompactRequest responses, ALTER TABLE COMPACT, and session warnings are unported"]
fn compact_table_error_in_halfway() {}

/// Go `pkg/executor/compact_table_test.go:239::TestCompactTableNoRemainingMultipleTiFlash`.
/// `storeCompactTask::work` (`pkg/executor/compact_table.go:132`) must run one
/// request on each TiFlash store and finish when both responses have no
/// remaining range.
#[test]
#[ignore = "go-parity-gap: multiple TiFlash stores and compact-task fan-out are unported"]
fn compact_table_no_remaining_multiple_tiflash() {}

/// Go `pkg/executor/compact_table_test.go:278::TestCompactTableMultipleTiFlash`.
/// `storeCompactTask::compactOnePhysicalTable` (`pkg/executor/compact_table.go:238`)
/// independently follows each store's returned end key until compaction is
/// complete.
#[test]
#[ignore = "go-parity-gap: TiFlash store RPC fan-out and continuation state are unported"]
fn compact_table_multiple_tiflash() {}

/// Go `pkg/executor/compact_table_test.go:337::TestCompactTableMultipleTiFlashWithError`.
/// `CompactTableTiFlashExec::doCompact` (`pkg/executor/compact_table.go:86`)
/// stops the other store tasks when one store returns a non-retryable compact
/// error while preserving the warning contract.
#[test]
#[ignore = "go-parity-gap: cross-store compact cancellation and TiFlash error responses are unported"]
fn compact_table_multiple_tiflash_with_error() {}

/// Go `pkg/executor/compact_table_test.go:408::TestCompactTableWithRangePartition`.
/// `storeCompactTask::work` (`pkg/executor/compact_table.go:132`) sends a
/// compaction request for every physical range-partition table.
#[test]
#[ignore = "go-parity-gap: partition DDL metadata, TiFlash physical tables, and compact RPCs are unported"]
fn compact_table_with_range_partition() {}

/// Go `pkg/executor/compact_table_test.go:522::TestCompactTableWithHashPartitionAndOnePartitionFailed`.
/// The partition loop in `storeCompactTask::work` (`pkg/executor/compact_table.go:151`)
/// records one failed partition while allowing the remaining physical tables
/// to follow the executor's cancellation policy.
#[test]
#[ignore = "go-parity-gap: hash-partition TiFlash compaction and per-partition failure handling are unported"]
fn compact_table_with_hash_partition_and_one_partition_failed() {}

/// Go `pkg/executor/compact_table_test.go:597::TestCompactTableWithTiFlashDown`.
/// `sendRequestWithRetry` (`pkg/executor/compact_table.go:338`) retries a
/// transport failure and `compactOnePhysicalTable` (`:258`) surfaces the
/// final store warning.
#[test]
#[ignore = "go-parity-gap: TiFlash transport retry/backoff and store-down warnings are unported"]
fn compact_table_with_tiflash_down() {}

/// Go `pkg/executor/compact_table_test.go:646::TestCompactTableWithSpecifiedRangePartition`.
/// `storeCompactTask::work` (`pkg/executor/compact_table.go:151`) restricts
/// compaction to the explicitly named range partitions.
#[test]
#[ignore = "go-parity-gap: specified-partition ALTER TABLE COMPACT and TiFlash RPC selection are unported"]
fn compact_table_with_specified_range_partition() {}

/// Go `pkg/executor/compact_table_test.go:713::TestCompactTableWithSpecifiedHashPartitionAndOnePartitionFailed`.
/// The selected physical tables in `storeCompactTask::work`
/// (`pkg/executor/compact_table.go:151`) preserve the hash-partition failure
/// and warning behavior.
#[test]
#[ignore = "go-parity-gap: specified hash-partition compaction and TiFlash failure warnings are unported"]
fn compact_table_with_specified_hash_partition_and_one_partition_failed() {}

/// Go `pkg/executor/compact_table_test.go:776::TestCompactTableWithTiFlashDownAndRestore`.
/// `sendRequestWithRetry` (`pkg/executor/compact_table.go:338`) retries a
/// temporarily unavailable TiFlash store and resumes continuation-key work.
#[test]
#[ignore = "go-parity-gap: TiFlash down-and-restore retry behavior has no Rust RPC or session seam"]
fn compact_table_with_tiflash_down_and_restore() {}
