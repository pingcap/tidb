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

//! The statement ctx's `util.ExecDetails` (`ctx.Value(util.ExecDetailsKey)`)
//! is charged by the client, as client-go charges it: every send's KV wait
//! and traffic (`sendReqToRegion`) and every Backoffer sleep
//! (`BackoffWithCfgAndMaxSleep`). TiDB's part is to make each client call of
//! a statement under that ctx: the transaction's reads (Go passes the
//! statement ctx to `KVTxn.Get`), and the Backoffers the coprocessor builds
//! (`backoff.NewBackofferWithVars(ctx, ...)` on the ctx `CopClient.Send`
//! derived with `tikv.TxnStartKey()`).

#![allow(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use tidb_copr::cop_store::NativeCopStore;
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_txnkv::{StatementKvContext, StatementKvContextCell, UnaryCallContext, UnaryCancellation};

fn unistore_store() -> TikvStore<tidb_unistore::mock::StorePdClient> {
    let (rpc, pd, cluster) = tidb_unistore::mock::new(Vec::new()).unwrap();
    tidb_unistore::cluster::bootstrap_with_single_store(&cluster);
    tidb_unistore::mock::new_test_tikv_store(&rpc, pd, tikv_client::Config::default()).unwrap()
}

fn statement_context(exec_details: &Arc<tikv_client::util::ExecDetails>) -> StatementKvContext {
    StatementKvContext {
        exec_details: Some(Arc::clone(exec_details)),
        ..Default::default()
    }
}

/// Go `KVTxn.Get(ctx, k)` under the statement ctx: the snapshot read's RPC
/// charges the statement's KV wait and its request traffic.
#[test]
fn transaction_reads_charge_the_statement_exec_details() {
    let opener = RealOptimisticTransactionOpener::from_capabilities(
        unistore_store(),
        Duration::from_secs(10),
    )
    .unwrap();
    let mut transaction = opener.begin_read_only().unwrap();
    let exec_details = Arc::new(tikv_client::util::ExecDetails::default());
    let cell = StatementKvContextCell::default();
    cell.publish(statement_context(&exec_details));
    transaction.set_statement_kv_context(cell);
    let call = UnaryCallContext::new(Duration::from_secs(10), UnaryCancellation::new());
    transaction.snapshot_get(b"absent", &call).unwrap();
    let charged = exec_details.snapshot();
    assert!(charged.wait_kv_response_duration_ns > 0, "{charged:?}");
    assert!(charged.traffic.sent_kv_total > 0, "{charged:?}");
    transaction.finish_without_writes().unwrap();
}

/// Go `copIteratorWorker`'s `bo`, made on the statement ctx with
/// `tikv.TxnStartKey()`: its sleeps are the statement's `Backoff_total`
/// and its `retry later` lines report the read's start ts.
#[test]
fn cop_backoffers_built_in_the_statement_ctx_charge_its_backoff() {
    let exec_details = Arc::new(tikv_client::util::ExecDetails::default());
    let kv_context = statement_context(&exec_details).with_txn_start_ts(42);
    let backoffer = tidb_txnkv::rpc::trace::build_in_ctx(None, kv_context.trace_context(), || {
        tikv_client::tikv::Backoffer::new(tikv_client::async_util::Cancellation::default(), 20_000)
    });
    assert_eq!(backoffer.txn_start_ts(), Some(42));
    let backoffer = Arc::new(tokio::sync::Mutex::new(backoffer));
    let store = unistore_store();
    store
        .backoff_region_miss(&backoffer, "region miss".to_owned())
        .unwrap();
    let charged = exec_details.snapshot();
    assert_eq!(charged.backoff_count, 1, "{charged:?}");
    assert!(charged.backoff_duration_ns > 0, "{charged:?}");

    // The same Backoffer made outside the statement ctx charges nothing.
    let detached = Arc::new(tokio::sync::Mutex::new(tikv_client::tikv::Backoffer::new(
        tikv_client::async_util::Cancellation::default(),
        20_000,
    )));
    store
        .backoff_region_miss(&detached, "region miss".to_owned())
        .unwrap();
    assert_eq!(exec_details.snapshot().backoff_count, 1);
}
