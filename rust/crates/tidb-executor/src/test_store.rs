// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The store a unit test's statements run over: Go's testkit gives each test
//! one mock store, and every statement of the test reads and writes it
//! (`sctx.Txn()`). Each test runs on its own thread, so the store is the
//! thread's.

use tidb_store_driver_txn::cluster_storage_test_util::SharedMemStorage;

thread_local! {
    static STORE: SharedMemStorage = SharedMemStorage::default();
}

/// This test's store; every clone shares its rows.
pub(crate) fn store() -> SharedMemStorage {
    STORE.with(Clone::clone)
}
