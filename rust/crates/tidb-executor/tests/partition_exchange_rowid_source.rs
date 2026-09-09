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

//! Port of the EXCHANGE-PARTITION tests of `pkg/ddl/tests/partition`:
//! `db_partition_test.go:3937::TestExchangeTiDBRowID`,
//! `db_partition_test.go:3980::TestIssue66077ExchangePartitionDifferentDefinitionsWithShardRowIDBits`
//! and `exchange_partition_test.go:26::TestExchangeRangeColumnsPartition`.
//!
//! All three pin `ALTER TABLE ... EXCHANGE PARTITION`, which this tier
//! refuses with 1105 "this ALTER TABLE action is not supported yet"
//! (measured), so they are `#[ignore]` gap tests whose contracts are
//! re-derived from the Go sources.

use tidb_executor::{run_create_table_on, Catalog};

/// Go `db_partition_test.go:3937::TestExchangeTiDBRowID`: after
/// `alter table tp exchange partition p0 with table t` (two nonclustered-PK
/// tables, one hash-partitioned by `a`), inserting `(8,8)` into BOTH tables
/// must not reuse the exchanged rows' `_tidb_rowid`s: Go asserts the
/// post-exchange rowid sets are `1..6` from `t`'s old rows plus fresh
/// `30001` for the new row (`:3954-:3962`), i.e. the exchange swaps the
/// row batches AND the global auto-rowid allocator state moves with the
/// partition, so a later insert into either side allocates beyond the
/// exchanged max (issue 64176).
// go-parity-gap: EXCHANGE PARTITION answers 1105 on this tier (measured),
// and `_tidb_rowid` is not selectable here either — the tier's rowid
// allocation is internal, with no pseudocolumn projection.
#[test]
#[ignore]
fn exchange_partition_tidb_rowid_allocator_follows_the_partition() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int, b int, primary key (a) nonclustered)",
        &mut catalog,
    )
    .unwrap();
    run_create_table_on(
        "create table tp (a int, b int, primary key (a) nonclustered) partition by hash (a) partitions 2",
        &mut catalog,
    )
    .unwrap();
    // The exchange at Go :3950 plus the two inserts at :3951/:3953 and the
    // rowid-exact result checks are the unportable body.
}

/// Go `db_partition_test.go:3980::TestIssue66077ExchangePartitionDifferentDefinitionsWithShardRowIDBits`
/// (issue 66077): with `tidb_shard_row_id_bits = 4`, exchanging a partition
/// of `sbtest.sbtest1` (shard-row-id-bits table) with a differently-shaped
/// table must not lose rows nor corrupt subsequent allocations.
// go-parity-gap: EXCHANGE PARTITION answers 1105 on this tier, and the
// preconditions have no carriers either — `tidb_shard_row_id_bits` as a
// session variable and `SHARD_ROW_ID_BITS` in metadata are unported, so the
// shard-bits-mismatch arm cannot even be framed.
#[test]
#[ignore]
fn exchange_partition_between_different_shard_row_id_bit_definitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table sbtest1 (id int not null, k int not null default '0', c char(120) not null default '', pad char(60) not null default '')",
        &mut catalog,
    )
    .unwrap();
    // Go :3985-:4011 builds the shard-row-id-bits pair, exchanges, and
    // re-checks counts and rowids; none of that is reachable here.
}

/// Go `exchange_partition_test.go:26::TestExchangeRangeColumnsPartition`:
/// a 5-partition `RANGE COLUMNS(age, name)` table (multi-column bounds with
/// NULL, MIN/MAX int and ''/'l'/'m'/'n' string bounds, `p2 values less than
/// (30, MAXVALUE)`), a full 50-row cross-product insert
/// (`:57-:75`), then for EVERY partition a routing probe (each row's
/// partition is recomputed and counted, `:77-:110`) before/after exchanging
/// each partition with a like-shaped ordinary table — proving the
/// multi-column range-columns router and the exchange agree.
// go-parity-gap: EXCHANGE PARTITION answers 1105 on this tier (measured).
// The multi-column RANGE COLUMNS routing itself builds and routes (measured
// against `partition_routing`), but the test's contract is the exchange
// equivalence, so nothing runs without the exchange carrier.
#[test]
#[ignore]
fn exchange_range_columns_partition_matches_the_multi_column_router() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t1 (id INT NOT NULL, age INT, name VARCHAR(50)) \
         PARTITION BY RANGE COLUMNS(age, name) (\
         PARTITION p0 VALUES LESS THAN (20, 'm'), \
         PARTITION p1 VALUES LESS THAN (30, 'm'), \
         PARTITION p2 VALUES LESS THAN (30, MAXVALUE), \
         PARTITION p3 VALUES LESS THAN (40, 'm'), \
         PARTITION p4 VALUES LESS THAN (MAXVALUE, MAXVALUE))",
        &mut catalog,
    )
    .expect("the multi-column range-columns table builds on this tier");
    // Go :47-:120 then exchanges p0..p4 one by one with `t1_e` and
    // re-counts the routing after every step.
}
