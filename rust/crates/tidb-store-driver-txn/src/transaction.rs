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

//! Go `store/driver/txn`'s transaction over the vendored client-rust engine:
//! `tikvStore.Begin` (`RealOptimisticTransactionOpener`), the transaction
//! facade (`client`) and the commit-protocol choice. The `pkg/kv` contracts
//! and client-go-level outcomes it reports through stay in
//! `tidb_txnkv::transaction`.

mod call_interceptor;
mod coordinator;

pub use coordinator::{
    CommitProtocol, ProductionTransaction, RealOptimisticTransactionOpener, RealTransaction,
    SnapshotGetResult, SnapshotScanRegion, StoreClient,
};

/// TiDB facade over the native optimistic and pessimistic transaction owner.
pub mod client;
