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

//! TiFlash store classification from `pkg/util/engine`.

use tidb_proto::metapb;

use crate::PdStore;

const ENGINE_LABEL: &str = "engine";
const TIFLASH: &str = "tiflash";
const TIFLASH_COMPUTE: &str = "tiflash_compute";

fn is_tiflash_label(key: &str, value: &str) -> bool {
    key == ENGINE_LABEL && (value == TIFLASH || value == TIFLASH_COMPUTE)
}

fn is_tiflash_write_label(key: &str, value: &str) -> bool {
    key == ENGINE_LABEL && value == TIFLASH
}

/// Returns whether protobuf store metadata describes either a TiFlash write
/// node or a NextGen TiFlash compute node.
#[must_use]
pub fn is_tiflash(store: &metapb::Store) -> bool {
    store
        .labels
        .iter()
        .any(|label| is_tiflash_label(&label.key, &label.value))
}

/// Returns whether normalized PD store metadata describes either a TiFlash
/// write node or a NextGen TiFlash compute node.
#[must_use]
pub fn is_tiflash_http_response(store: &PdStore) -> bool {
    store
        .labels
        .iter()
        .any(|(key, value)| is_tiflash_label(key, value))
}

/// Returns whether normalized PD store metadata describes a TiFlash write
/// node. NextGen compute-only nodes are deliberately excluded.
#[must_use]
pub fn is_tiflash_write_http_response(store: &PdStore) -> bool {
    store
        .labels
        .iter()
        .any(|(key, value)| is_tiflash_write_label(key, value))
}
