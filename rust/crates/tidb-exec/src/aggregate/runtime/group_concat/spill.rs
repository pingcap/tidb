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

use super::GroupConcatState;
use tidb_util::serialization::{serialize_bool, serialize_bytes_buffer, Cursor, INT_LEN};

/// Malformed source-native GROUP_CONCAT spill row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConcatSpillError;

/// Encodes Go's base partial result: native bool then native int length and bytes.
#[must_use]
pub fn encode_base_partial(state: &GroupConcatState) -> Vec<u8> {
    let Some(buffer) = state.finish() else {
        return vec![0];
    };
    let mut encoded = Vec::with_capacity(1 + INT_LEN + buffer.len());
    serialize_bool(true, &mut encoded);
    serialize_bytes_buffer(buffer, &mut encoded);
    encoded
}

/// Decodes one Go same-architecture base partial result.
pub fn decode_base_partial(
    encoded: &[u8],
    separator: impl AsRef<[u8]>,
    max_len: u64,
) -> Result<GroupConcatState, GroupConcatSpillError> {
    let mut cursor = Cursor::new(encoded);
    let has_buffer = cursor.read_bool().map_err(|_| GroupConcatSpillError)?;
    let mut state = GroupConcatState::new(separator, max_len);
    if !has_buffer {
        return Ok(state);
    }
    let value = cursor
        .read_bytes_buffer()
        .map_err(|_| GroupConcatSpillError)?;
    state.restore_buffer(Some(value));
    Ok(state)
}
