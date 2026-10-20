// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The store records PD's HTTP API answers (Go `pdhttp.StoresInfo`).

/// Legacy store lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdStoreState {
    /// Online according to the legacy field.
    Up,
    /// Being removed according to the legacy field.
    Offline,
}

/// Replacement store lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdNodeState {
    /// Joining but not yet fully serving.
    Preparing,
    /// Serving requests.
    Serving,
    /// Being removed.
    Removing,
}

/// Go pdhttp `StoreInfo.Store`: one store as PD's HTTP API lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PdStore {
    /// Store identity.
    pub id: u64,
    /// TiKV client address.
    pub address: String,
    /// Go `Store.StatusAddress`: the HTTP status address (TiFlash serves its
    /// `/tiflash/sync-status/...` region-sync report here).
    pub status_address: String,
    /// Legacy source state.
    pub state: PdStoreState,
    /// Replacement source state.
    pub node_state: PdNodeState,
    /// Store labels in PD metadata order.
    pub labels: Vec<(String, String)>,
}
