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

//! Go `TiFlashSystemTableRetriever`'s storage boundary: the
//! `tikvrpc.CmdGetTiFlashSystemTable` request it sends through
//! `tikvStore.GetTiKVClient().SendRequest`.

use std::time::Duration;

/// The store client the TiFlash system tables
/// (`information_schema.TIFLASH_TABLES` / `TIFLASH_SEGMENTS` /
/// `TIFLASH_INDEXES`) query each TiFlash store through.
pub trait TiFlashSystemTableClient: Send + Sync {
    /// Sends a `kvrpcpb.TiFlashSystemTableRequest{Sql: sql}` to the TiFlash
    /// store at `address` and returns the response's `Data` (TiFlash's JSON
    /// result).
    fn get_tiflash_system_table(&self, address: &str, sql: &str, timeout: Duration) -> Result<Vec<u8>, String>;
}
