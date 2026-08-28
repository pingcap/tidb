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

//! Ports of the remaining three Go sources of batch b113's
//! `pkg/ddl/tests/partition` window: `error_injection_test.go`
//! (`TestTruncatePartitionListFailuresWithGlobalIndex` :65,
//! `TestTruncatePartitionListFailures` :92), `exchange_partition_test.go`
//! (`TestExchangeRangeColumnsPartition` :26) and
//! `global_index_version_test.go` (`TestGlobalIndexVersion0` :30,
//! `TestGlobalIndexVersion1` :169, `TestGlobalIndexVersionConstants` :266,
//! `TestGlobalIndexTruncateAndDropPartition` :276,
//! `TestUpdateIndexesResetsGlobalIndexVersion` :460).
//!
//! Every test here but one needs a carrier this tier does not have: the
//! failpoint-driven DDL failure/retry machinery, EXCHANGE PARTITION, or
//! GLOBAL indexes themselves (the storage refuses to build one, so even the
//! plain read-your-rows flows of `TestGlobalIndexTruncateAndDropPartition`
//! cannot run). The one real port is the constants test — the three
//! `model.GlobalIndexVersion*` values are carried verbatim by
//! `tidb-model/src/index.rs:51-68`.

use tidb_model::index::{
    GLOBAL_INDEX_VERSION_LEGACY, GLOBAL_INDEX_VERSION_V1, GLOBAL_INDEX_VERSION_V2,
};

/// Go `TestTruncatePartitionListFailuresWithGlobalIndex`
/// (`pkg/ddl/tests/partition/error_injection_test.go:65`): `truncate
/// partition p0,p2` on a GLOBAL-indexed LIST-partitioned table under the
/// four injected failures (`truncatePartCancel1` non-recoverable rollback,
/// `truncatePartFail1` recoverable with id-changing retry, `Fail2`/`Fail3`
/// recoverable): the rollback rows, post-alter DML rows and after-recover
/// rows are pinned exactly, and failed alters leave partition/ index ids
/// untouched with empty AddingDefinitions/DroppingDefinitions.
// go-parity-gap: no failpoint injection, no DDL job retry/rollback state
// machine, and no GLOBAL index carrier.
#[test]
#[ignore]
fn truncate_partition_list_failures_with_global_index() {
}

/// Go `TestTruncatePartitionListFailures`
/// (`pkg/ddl/tests/partition/error_injection_test.go:92`): the same
/// injected-failure matrix over a local-primary-key LIST table (skips the
/// recoverable rows), pinning before/after DML row sets and the failed
/// alter's metadata invariants.
// go-parity-gap: no failpoint injection and no DDL job retry/rollback
// state machine.
#[test]
#[ignore]
fn truncate_partition_list_failures() {
}

/// Go `TestExchangeRangeColumnsPartition`
/// (`pkg/ddl/tests/partition/exchange_partition_test.go:26`): every
/// (age, name) boundary combination of a RANGE COLUMNS(age, name) table
/// exchanges out to a plain table and back, and each row refuses to
/// exchange into a non-matching partition with
/// `[ddl:1737]Found a row that does not match the partition`.
// go-parity-gap: `ALTER TABLE ... EXCHANGE PARTITION` is refused by the
// ALTER dispatcher in this tier.
#[test]
#[ignore]
fn exchange_range_columns_partition() {
}

/// Go `TestGlobalIndexVersion0`
/// (`pkg/ddl/tests/partition/global_index_version_test.go:30`): with the
/// `SetGlobalIndexVersion` failpoint forcing the legacy format, a GLOBAL
/// index over an EXCHANGE-tainted table (duplicate `_tidb_rowid`s across
/// partitions) serves only 13 of 14 rows, `admin check table` reports
/// `[admin:8223]data inconsistency`, updates fail with
/// `[tikv:8141]assertion failed`, and the index metadata reads
/// GlobalIndexVersion 0; local indexes stay 0 and unique nullable GLOBAL
/// indexes get V1.
// go-parity-gap: no failpoint forcing, no EXCHANGE PARTITION, and no
// GLOBAL index carrier.
#[test]
#[ignore]
fn global_index_version_0() {
}

/// Go `TestGlobalIndexVersion1`
/// (`pkg/ddl/tests/partition/global_index_version_test.go:169`): the V1
/// default — a GLOBAL index over the EXCHANGE-tainted table serves all 14
/// rows, updates work, metadata reads GlobalIndexVersionV1, and local
/// indexes stay at version 0.
// go-parity-gap: no EXCHANGE PARTITION and no GLOBAL index carrier.
#[test]
#[ignore]
fn global_index_version_1() {
}

// --- TestGlobalIndexVersionConstants
//     (pkg/ddl/tests/partition/global_index_version_test.go:266) ---
//
// The three version constants: LEGACY=0, V1=1, V2=2
// (`pkg/meta/model/index.go:51-68`).
#[test]
fn global_index_version_constants() {
    assert_eq!(GLOBAL_INDEX_VERSION_LEGACY, 0);
    assert_eq!(GLOBAL_INDEX_VERSION_V1, 1);
    assert_eq!(GLOBAL_INDEX_VERSION_V2, 2);
}

/// Go `TestGlobalIndexTruncateAndDropPartition`
/// (`pkg/ddl/tests/partition/global_index_version_test.go:276`): V2 GLOBAL
/// indexes survive TRUNCATE PARTITION, DROP PARTITION (twice), an
/// EXCHANGE-tainted duplicate-rowid table, and REORGANIZE PARTITION —
/// `USE INDEX` reads and `ADMIN CHECK TABLE` stay consistent throughout,
/// and unique GLOBAL indexes tolerate duplicate NULLs across partitions.
// go-parity-gap: this tier refuses to build GLOBAL indexes at all ("a
// GLOBAL index ... maintains only per-partition index entries"), and
// EXCHANGE/REORGANIZE are unsupported, so no statement of this test runs.
#[test]
#[ignore]
fn global_index_truncate_and_drop_partition() {
}

/// Go `TestUpdateIndexesResetsGlobalIndexVersion`
/// (`pkg/ddl/tests/partition/global_index_version_test.go:460`): `CREATE
/// TABLE ... KEY idx_b(b) GLOBAL ... UPDATE INDEXES (idx_b LOCAL)` must
/// store the index as local with GlobalIndexVersion reset to 0, and DML
/// over it must not raise "handle is not a PartitionHandle".
// go-parity-gap: the GLOBAL keyword is refused at CREATE TABLE in this
// tier, and UPDATE INDEXES is not carried by the DDL builder.
#[test]
#[ignore]
fn update_indexes_resets_global_index_version() {
}
