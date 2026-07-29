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

//! Real-PD/TiKV proof that the Stage D server write path actually persists.
//!
//! Stage B proved the transaction coordinator against real TiKV by building PD,
//! RegionCache, and the transport directly. Stage D added a different
//! composition — `ProductionReadProcessAuthority::connect` hands out a write
//! opener derived from the authority that already serves reads, and
//! `commit_configured_write` drives one prepared statement through it. That
//! composition had no real-cluster coverage, so this test drives exactly it:
//!
//! 1. One authority connects. Its read opener and its transaction opener report
//!    the same nonzero `authority_id`, so reads and writes truly share one PD
//!    worker, RegionCache, and BatchCommands transport.
//! 2. A prepared INSERT commits through the shared opener and reports one
//!    affected row.
//! 3. An arithmetic UPDATE reads the just-committed value at its own start
//!    timestamp and commits the sum.
//! 4. An UPDATE of a missing row publishes nothing and reports zero rows.
//! 5. A brand-new authority — a fresh PD client, RegionCache, and transport —
//!    reads the committed row back. The value therefore lives in TiKV, not in
//!    the first authority's process memory.
//!
//! The runner `scripts/run-realtikv-prepared-write.sh` owns the
//! tag-owned playground lifecycle. No in-memory database, mock transport, or
//! fixture row is admitted.

use std::time::Duration;

use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_exec::real_tikv_dml::{commit_configured_write, prepare_configured_write};
use tidb_exec::real_tikv_read::{ProductionReadProcessAuthority, RealOptimisticTransactionOpener};
use tidb_planner::prepared_dml::PreparedBindValue;
use tidb_planner::read_only_scan::{
    configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredScalarType, ConfiguredTable,
};
use tidb_txnkv::rpc::UnaryCallContext;

/// Wraps signed integers as the planner's bind currency (these writes are int-only).
fn int_binds(params: [i64; 2]) -> Vec<PreparedBindValue> {
    params.into_iter().map(PreparedBindValue::Int).collect()
}

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const TABLE_ID: i64 = 528_491;
const ID_COLUMN: i64 = 1;
const BALANCE_COLUMN: i64 = 2;
const HANDLE: i64 = 10;
const MISSING_HANDLE: i64 = 99;
const INSERTED_BALANCE: i64 = 100;
const ADDEND: i64 = 7;

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "campaign28",
        "accounts",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_not_null("balance", BALANCE_COLUMN),
        ],
    )
}

fn configured_catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([configured_table()]).expect("configured catalog must validate")
}

/// Reads one configured row's balance back through a fresh transaction on the
/// given opener. A new transaction takes a new PD start timestamp greater than
/// any prior commit, so the value it observes is what TiKV durably stored.
fn read_balance(opener: &RealOptimisticTransactionOpener, handle: i64) -> Option<i64> {
    let row_key = encode_row_key_with_handle(TABLE_ID, &RecordHandle::Int(handle));
    let mut transaction = opener
        .begin(1, 128)
        .expect("allocate a real readback snapshot");
    let observed = transaction
        .snapshot_get(&row_key, &UnaryCallContext::with_timeout(RPC_TIMEOUT))
        .expect("real BatchCommands Get must succeed");
    observed.value.map(|value| {
        // Decodes through the same shared row codec production reads with
        // (`tidb_tablecodec::decode_table_row_to_map`), rather than a second,
        // bespoke row decoder of this test's own.
        let field_types = std::collections::BTreeMap::from([(
            BALANCE_COLUMN,
            ConfiguredScalarType::BigInt.chunk_field_type(),
        )]);
        tidb_tablecodec::decode_table_row_to_map(&value, &field_types, None)
            .expect("row decodes")
            .remove(&BALANCE_COLUMN)
            .expect("balance column must be present")
            .as_int()
            .expect("balance column must be a signed integer")
    })
}

fn insert_sql() -> &'static str {
    "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)"
}

fn arithmetic_update_sql() -> &'static str {
    "UPDATE campaign28.accounts SET balance = balance + ? WHERE id = ?"
}

#[test]
#[ignore = "requires run-realtikv-prepared-write.sh"]
fn prepared_insert_and_update_persist_through_one_shared_authority() {
    let pd_address =
        std::env::var("PREPARED_WRITE_PD_ADDR").expect("runner must provide PREPARED_WRITE_PD_ADDR");
    let catalog = configured_catalog();

    // (1) One authority. Reads and writes must share it.
    let mut authority = ProductionReadProcessAuthority::connect(
        [pd_address.clone()],
        RPC_TIMEOUT,
        configured_table(),
    )
    .expect("connect one real read/write authority");
    let read_authority_id = authority.opener().authority_id();
    let write_opener = authority.transaction_opener();
    assert_ne!(read_authority_id, 0);
    assert_eq!(
        read_authority_id,
        write_opener.authority_id(),
        "reads and writes must derive from one authority"
    );
    let cluster_id = authority.cluster_id();
    assert_ne!(cluster_id, 0);

    // (2) A prepared INSERT commits and reports one affected row.
    let insert = prepare_configured_write(insert_sql(), &catalog)
        .expect("INSERT lowers")
        .bind(&int_binds([HANDLE, INSERTED_BALANCE]))
        .expect("INSERT binds");
    let insert_report =
        commit_configured_write(&write_opener, &insert, RPC_TIMEOUT, 0).expect("INSERT commits");
    assert_eq!(insert_report.affected_rows, 1);
    assert_eq!(insert_report.no_write, None);
    assert_eq!(
        read_balance(&write_opener, HANDLE),
        Some(INSERTED_BALANCE),
        "the committed INSERT must be visible to a later snapshot"
    );

    // (3) An arithmetic UPDATE reads the committed value and commits the sum.
    let update = prepare_configured_write(arithmetic_update_sql(), &catalog)
        .expect("UPDATE lowers")
        .bind(&int_binds([ADDEND, HANDLE]))
        .expect("UPDATE binds");
    let update_report =
        commit_configured_write(&write_opener, &update, RPC_TIMEOUT, 0).expect("UPDATE commits");
    assert_eq!(update_report.affected_rows, 1);
    let expected_balance = INSERTED_BALANCE + ADDEND;
    assert_eq!(read_balance(&write_opener, HANDLE), Some(expected_balance));

    // (4) An UPDATE of a missing row publishes nothing.
    let missing_update = prepare_configured_write(arithmetic_update_sql(), &catalog)
        .expect("UPDATE lowers")
        .bind(&int_binds([ADDEND, MISSING_HANDLE]))
        .expect("UPDATE binds");
    let missing_report = commit_configured_write(&write_opener, &missing_update, RPC_TIMEOUT, 0)
        .expect("missing UPDATE returns without publication");
    assert_eq!(missing_report.affected_rows, 0);
    assert!(missing_report.no_write.is_some());
    assert_eq!(read_balance(&write_opener, MISSING_HANDLE), None);

    // Release every write capability this test holds before shutting the
    // authority down. The production server drops its session factory the same
    // way before `shutdown_process`; a live opener clone keeps a PD handle
    // alive and the PD stage refuses to stop while any clone remains.
    let write_authority_id = write_opener.authority_id();
    drop(write_opener);

    // (5) A fresh authority — new PD client, RegionCache, transport — reads the
    // durable value. This is the library-level equivalent of restarting the
    // Rust node: nothing from the first authority survives except what TiKV
    // persisted.
    authority.shutdown().expect("dependency-ordered shutdown");
    drop(authority);

    let restarted =
        ProductionReadProcessAuthority::connect([pd_address], RPC_TIMEOUT, configured_table())
            .expect("reconnect a fresh authority");
    let restarted_opener = restarted.transaction_opener();
    let restart_authority_id = restarted_opener.authority_id();
    assert_ne!(
        restart_authority_id, write_authority_id,
        "the restarted authority must be a distinct process authority"
    );
    assert_eq!(
        read_balance(&restarted_opener, HANDLE),
        Some(expected_balance),
        "the row must survive a full authority restart"
    );

    println!(
        "campaign28_prepared_write status=passed cluster_id={cluster_id} \
         table_id={TABLE_ID} handle={HANDLE} final_balance={expected_balance} \
         write_authority_id={write_authority_id} restart_authority_id={restart_authority_id}"
    );
}
