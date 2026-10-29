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

//! Go `kv.StoreType` (`pkg/kv/kv.go`).

/// Go `kv.StoreType` (`pkg/kv/kv.go`): the type of storage engine, with
/// Go's zero value (`TiKV`) as the default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StoreType {
    /// Go `kv.TiKV`.
    #[default]
    TiKv,
    /// Go `kv.TiFlash`.
    TiFlash,
    /// Go `kv.TiDB`.
    TiDb,
    /// Go `kv.UnSpecified`.
    Unspecified,
}

impl StoreType {
    /// Go `StoreType.Name`: the name of the store type.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            StoreType::TiFlash => "tiflash",
            StoreType::TiDb => "tidb",
            StoreType::TiKv => "tikv",
            StoreType::Unspecified => "unspecified",
        }
    }
}
