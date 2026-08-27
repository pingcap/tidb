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

//! A production transaction can be OWNED by the thread that uses it.
//!
//! client-go has no per-transaction worker at all: `KVTxn` holds no channel
//! and no request queue, and every read runs on the caller's own goroutine
//! over a store handle shared between transactions. This tier instead pins
//! each transaction to a borrowed thread and reaches it through a channel,
//! on the stated grounds that "the production transport is deliberately
//! worker-local (`Rc<RefCell<..>>`)"
//! (`tidb-exec/src/cluster_table_storage.rs`).
//!
//! That justification is worth a compile-time fact rather than a comment: the
//! shared halves are `Arc<Mutex<C>>` and `BackgroundRegionCache<L>`, and the
//! capability traits the opener demands are all `Send + Sync + 'static`
//! (`transaction/coordinator/opener.rs`). If this test compiles, nothing in
//! the type system forces the pin, and moving a transaction onto the
//! connection worker -- the shape client-go has -- is open.
//!
//! If a future change makes a transaction thread-affine, this test goes red
//! at the point the affinity is introduced rather than at the point someone
//! tries to move it.

fn assert_send<T: Send>() {}

#[test]
fn a_production_transaction_can_be_owned_by_the_connection_worker() {
    assert_send::<tidb_txnkv::transaction::ProductionOptimisticTransaction>();
    assert_send::<tidb_txnkv::transaction::ProductionPessimisticTransaction>();
}
