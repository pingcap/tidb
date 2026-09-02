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

//! Go-parity pins for `pkg/store/mockstore/unistore/tikv/util_test.go`
//! (`origin/master`), testport batch b062 (`pkg/store/mockstore.part2`).
//!
//! `TestLockwaiterBasic` / `TestLockwaiterConcurrent` (from
//! `util/lockwaiter/lockwaiter_test.go`, also in this batch's slice) remain
//! ported in [`crate::lockwaiter::tests`] as `test_lockwaiter_basic` and
//! `test_lockwaiter_concurrent`; this module owns the three `tikv/util.go`
//! helper tables.
//!
//! The three `tikv/util.go` helpers exercised by `TestExceedEndKey`,
//! `TestSortAndDedupHashVals`, and `TestSafeCopy`
//! (`exceedEndKey(current, endKey) = len(endKey) != 0 &&
//! bytes.Compare(current, endKey) >= 0`,
//! `sortAndDedupHashVals` = sort + compact for len > 1,
//! `safeCopy` = `slices.Clone`) are implemented in [`crate::tikv_util`].
//! Their callers (`mutationsToHashVals`, the `mvcc.go` scan loops,
//! `cophandler/closure_exec.go`) remain outside this crate's current
//! execution graph, but the helper contracts are executable here so a future
//! caller can reuse them without a second source re-read.

use super::tikv_util::{
    exceed_end_key, keys_to_hash_vals, mutations_to_hash_vals, safe_copy, sort_and_dedup_hash_vals,
    user_keys_to_hash_vals,
};
use tidb_proto::KvrpcMutation;
use tidb_txnkv::Key;

/// Go `TestExceedEndKey` (`tikv/util_test.go:24`), all 7 table cases, each
/// asserted against Go's expected boolean:
/// empty/absent end key → false; `current == endKey` → true;
/// `current > endKey` → true; `current < endKey` → false; both empty → false.
#[test]
fn exceed_end_key_table_vectors() {
    for (current, end_key, expected) in [
        (b"abc".as_slice(), b"".as_slice(), false),
        (b"abc".as_slice(), b"".as_slice(), false),
        (b"abc".as_slice(), b"abc".as_slice(), true),
        (b"bcd".as_slice(), b"abc".as_slice(), true),
        (b"abc".as_slice(), b"bcd".as_slice(), false),
        (b"".as_slice(), b"abc".as_slice(), false),
        (b"".as_slice(), b"".as_slice(), false),
    ] {
        assert_eq!(exceed_end_key(current, end_key), expected);
    }
}

/// Go `TestSortAndDedupHashVals` (`tikv/util_test.go:83`): sort ascending and
/// drop duplicates (Go mutates in place when `len > 1`; a 1-element or empty
/// slice is returned unchanged). All 8 table cases carry exact u64 vectors.
#[test]
fn sort_and_dedup_hash_vals_table_vectors() {
    for (input, expected) in [
        (vec![], vec![]),
        (vec![1], vec![1]),
        (vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]),
        (vec![5, 3, 1, 4, 2], vec![1, 2, 3, 4, 5]),
        (
            vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3],
            vec![1, 2, 3, 4, 5, 6, 9],
        ),
        (vec![7, 7, 7, 7], vec![7]),
        (vec![3, 3], vec![3]),
        (vec![3, 1], vec![1, 3]),
    ] {
        let mut input = input;
        assert_eq!(sort_and_dedup_hash_vals(&mut input), expected.as_slice());
    }
}

/// Go `TestSafeCopy` (`tikv/util_test.go:142`): returns a byte-for-byte equal
/// slice that is independent of the input buffer (mutating the original must
/// not affect the copy). Cases: nil→nil, empty→empty, single byte, "hello
/// world", binary `{0,1,2,255,254}`.
#[test]
fn safe_copy_table_vectors() {
    assert_eq!(safe_copy(None), None);
    assert_eq!(safe_copy(Some(&[])), Some(Vec::new()));
    for expected in [
        b"A".to_vec(),
        b"hello world".to_vec(),
        vec![0, 1, 2, 255, 254],
    ] {
        let mut source = expected.clone();
        let copy = safe_copy(Some(&source)).expect("non-nil source has a copy");
        source[0] = source[0].wrapping_add(1);
        assert_eq!(copy, expected);
    }
}

/// The remaining production helpers in `util.go` all use the same FarmHash
/// and sorted-unique pipeline. These vectors exercise the mutation, raw-key,
/// and user-key entry points, including duplicate elimination.
#[test]
fn hash_helpers_match_go_farmhash_pipeline() {
    let empty_hash = 11_160_318_154_034_397_263;
    let a_hash = 12_917_804_110_809_363_939;
    let expected = vec![empty_hash, a_hash];

    assert_eq!(keys_to_hash_vals(&[b"a", &b""[..], b"a"]), expected);

    let mutations = vec![
        KvrpcMutation {
            key: b"a".to_vec(),
            ..KvrpcMutation::default()
        },
        KvrpcMutation::default(),
        KvrpcMutation {
            key: b"a".to_vec(),
            ..KvrpcMutation::default()
        },
    ];
    assert_eq!(mutations_to_hash_vals(&mutations), expected);

    let keys = vec![
        Key::from_bytes(b"a"),
        Key::from_bytes(Vec::new()),
        Key::from_bytes(b"a"),
    ];
    assert_eq!(user_keys_to_hash_vals(&keys), expected);
}
