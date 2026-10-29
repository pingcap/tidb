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

//! Go `kv.Storage.GetLockWaits`: the wait-for edges the storage backend
//! reports for `information_schema.DATA_LOCK_WAITS`.

/// One pessimistic wait-for edge returned by the active storage backend.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataLockWait {
    /// Transaction waiting for the lock.
    pub txn: u64,
    /// Transaction currently holding the lock.
    pub wait_for_txn: u64,
    /// Encoded locked key.
    pub key: Vec<u8>,
    /// Encoded TopSQL resource-group tag.
    pub resource_group_tag: Vec<u8>,
}

/// Storage boundary for `information_schema.DATA_LOCK_WAITS`.
pub trait DataLockWaitsProvider: Send + Sync {
    /// Reads the current wait-for edges from the backend.
    fn lock_waits(&self) -> Result<Vec<DataLockWait>, String>;
}
