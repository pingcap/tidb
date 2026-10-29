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

//! Go `pkg/tablecodec/rowindexcodec`: a key's row/index family from its
//! minimal `t[table_id]_[ri]` prefix, which `util/resourcegrouptag` labels
//! requests by.

const TABLE_PREFIX: u8 = b't';
const TABLE_ID_LEN: usize = 8;
const KIND_PREFIX_LEN: usize = 2;
const KIND_PREFIX_OFFSET: usize = 1 + TABLE_ID_LEN;
const MIN_KEY_LEN: usize = KIND_PREFIX_OFFSET + KIND_PREFIX_LEN;

/// The row/index family identified by a TiDB table-key prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyKind {
    /// The key is too short or does not contain a recognized table-key prefix.
    Unknown,
    /// The key has the `t[table_id]_r` record prefix.
    Row,
    /// The key has the `t[table_id]_i` index prefix.
    Index,
}

/// Classifies a key from only its minimal `t[table_id]_[ri]` prefix.
///
/// The table ID bytes and any bytes after the kind prefix are intentionally
/// opaque. This is the complete observable contract of Go's
/// `rowindexcodec.GetKeyKind`; it does not validate a complete row or index key.
pub fn get_key_kind(key: &[u8]) -> KeyKind {
    if key.len() < MIN_KEY_LEN || key[0] != TABLE_PREFIX {
        return KeyKind::Unknown;
    }

    match &key[KIND_PREFIX_OFFSET..MIN_KEY_LEN] {
        b"_r" => KeyKind::Row,
        b"_i" => KeyKind::Index,
        _ => KeyKind::Unknown,
    }
}
