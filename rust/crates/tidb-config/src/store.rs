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

//! Transcreation of Go `pkg/config/store.go`: the storage type.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The type of storage (Go `StoreType`, a string type: unknown values
/// survive decoding and fail `valid()`, exactly like the source).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct StoreType(pub String);

/// TiKV storage (Go `StoreTypeTiKV`).
pub const STORE_TYPE_TIKV: &str = "tikv";
/// UniStore storage, test only (Go `StoreTypeUniStore`).
pub const STORE_TYPE_UNISTORE: &str = "unistore";
/// MockTiKV storage, test only (Go `StoreTypeMockTiKV`).
pub const STORE_TYPE_MOCKTIKV: &str = "mocktikv";

impl StoreType {
    /// Go `Valid`.
    pub fn valid(&self) -> bool {
        matches!(
            self.0.as_str(),
            STORE_TYPE_TIKV | STORE_TYPE_UNISTORE | STORE_TYPE_MOCKTIKV
        )
    }
}

impl fmt::Display for StoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Default for StoreType {
    fn default() -> Self {
        StoreType(STORE_TYPE_UNISTORE.to_string())
    }
}

/// Go `StoreTypeList`.
pub fn store_type_list() -> [StoreType; 3] {
    [
        StoreType(STORE_TYPE_TIKV.to_string()),
        StoreType(STORE_TYPE_UNISTORE.to_string()),
        StoreType(STORE_TYPE_MOCKTIKV.to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestStoreType.
    #[test]
    fn store_type() {
        assert_eq!(store_type_list().len(), 3);
        for tp in store_type_list() {
            assert!(tp.valid());
        }
        assert!(!StoreType("bogus".to_string()).valid());
    }
}
