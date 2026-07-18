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

use std::mem::size_of;

use super::GroupConcatState;

/// Malformed source-native GROUP_CONCAT spill row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConcatSpillError;

/// Encodes Go's base partial result: native bool then native int length and bytes.
#[must_use]
pub fn encode_base_partial(state: &GroupConcatState) -> Vec<u8> {
    let Some(buffer) = state.finish() else {
        return vec![0];
    };
    let mut encoded = Vec::with_capacity(1 + size_of::<isize>() + buffer.len());
    encoded.push(1);
    encoded.extend_from_slice(&(buffer.len() as isize).to_ne_bytes());
    encoded.extend_from_slice(buffer);
    encoded
}

/// Decodes one Go same-architecture base partial result.
pub fn decode_base_partial(
    encoded: &[u8],
    separator: impl AsRef<[u8]>,
    max_len: u64,
) -> Result<GroupConcatState, GroupConcatSpillError> {
    let (&has_buffer, tail) = encoded.split_first().ok_or(GroupConcatSpillError)?;
    let mut state = GroupConcatState::new(separator, max_len);
    if has_buffer == 0 {
        return Ok(state);
    }
    if tail.len() < size_of::<isize>() {
        return Err(GroupConcatSpillError);
    }
    let (length, payload) = tail.split_at(size_of::<isize>());
    let length = isize::from_ne_bytes(length.try_into().map_err(|_| GroupConcatSpillError)?);
    let length = usize::try_from(length).map_err(|_| GroupConcatSpillError)?;
    let value = payload.get(..length).ok_or(GroupConcatSpillError)?.to_vec();
    state.restore_buffer(Some(value));
    Ok(state)
}
