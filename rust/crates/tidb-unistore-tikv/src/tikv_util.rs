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

//! Small byte-range and mutation-hash helpers from
//! `pkg/store/mockstore/unistore/tikv/util.go`.
//!
//! The Go source keeps these helpers private because only the in-memory TiKV
//! server uses them. The Rust owner exposes them at crate visibility so the
//! same server modules can share the exact contracts without making a new
//! public API. `safe_copy` takes an `Option` to preserve Go's distinction
//! between a nil slice and a non-nil empty slice.

use tidb_proto::KvrpcMutation;
use tidb_txnkv::{fingerprint64, Key};

/// Go `exceedEndKey`: an empty end key is unbounded; otherwise the current
/// key is outside the half-open range once it reaches the end key.
#[must_use]
pub(crate) fn exceed_end_key(current: &[u8], end_key: &[u8]) -> bool {
    !end_key.is_empty() && current >= end_key
}

/// Go `sortAndDedupHashVals`: sort in place and compact adjacent duplicates.
#[must_use]
pub(crate) fn sort_and_dedup_hash_vals(hash_vals: &mut Vec<u64>) -> &[u64] {
    if hash_vals.len() > 1 {
        hash_vals.sort_unstable();
        hash_vals.dedup();
    }
    hash_vals
}

/// Hash mutation keys with the same FarmHash fingerprint used by TiKV's Go
/// implementation, then apply [`sort_and_dedup_hash_vals`].
#[must_use]
pub(crate) fn mutations_to_hash_vals(mutations: &[KvrpcMutation]) -> Vec<u64> {
    let mut hash_vals = mutations
        .iter()
        .map(|mutation| fingerprint64(&mutation.key))
        .collect();
    let _ = sort_and_dedup_hash_vals(&mut hash_vals);
    hash_vals
}

/// Hash raw keys with the source FarmHash implementation, sorted and unique.
#[must_use]
pub(crate) fn keys_to_hash_vals(keys: &[&[u8]]) -> Vec<u64> {
    let mut hash_vals = keys.iter().map(|key| fingerprint64(key)).collect();
    let _ = sort_and_dedup_hash_vals(&mut hash_vals);
    hash_vals
}

/// Hash the user-key bytes of TiKV [`Key`] values, sorted and unique.
#[must_use]
pub(crate) fn user_keys_to_hash_vals(keys: &[Key]) -> Vec<u64> {
    let mut hash_vals = keys
        .iter()
        .map(|key| fingerprint64(key.as_bytes()))
        .collect();
    let _ = sort_and_dedup_hash_vals(&mut hash_vals);
    hash_vals
}

/// Go `safeCopy`: clone bytes while retaining nil as nil.
#[must_use]
pub(crate) fn safe_copy(bytes: Option<&[u8]>) -> Option<Vec<u8>> {
    bytes.map(ToOwned::to_owned)
}
