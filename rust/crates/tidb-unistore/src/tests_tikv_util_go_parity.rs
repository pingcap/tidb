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
//! Of that file's three tests, only `TestLockwaiterBasic` /
//! `TestLockwaiterConcurrent` (from
//! `util/lockwaiter/lockwaiter_test.go`, also in this batch's slice) have
//! transcreated subjects; they are ported in [`crate::lockwaiter::tests`] as
//! `test_lockwaiter_basic` and `test_lockwaiter_concurrent`.
//!
//! The three `tikv/util.go` helpers exercised by `TestExceedEndKey`,
//! `TestSortAndDedupHashVals`, and `TestSafeCopy`
//! (`exceedEndKey(current, endKey) = len(endKey) != 0 &&
//! bytes.Compare(current, endKey) >= 0`,
//! `sortAndDedupHashVals` = sort + compact for len > 1,
//! `safeCopy` = `slices.Clone`) are NOT yet transcreated anywhere in this
//! workspace — their callers (`mutationsToHashVals`, the `mvcc.go` scan
//! loops, `cophandler/closure_exec.go`) have no Rust counterparts yet. Each
//! gap is pinned below as an `#[ignore]`d test carrying the Go table vectors
//! verbatim so that the moment a transcreation lands, un-ignoring it yields
//! an executable parity check instead of a rewrite from memory.

/// Go `TestExceedEndKey` (`tikv/util_test.go:24`), all 7 table cases, each
/// asserted against Go's expected boolean:
/// empty/absent end key → false; `current == endKey` → true;
/// `current > endKey` → true; `current < endKey` → false; both empty → false.
#[test]
#[ignore = "go-parity-gap: tikv/util.go exceedEndKey has no Rust transcreation yet"]
fn exceed_end_key_table_vectors() {
    // ("empty end key", current=b"abc", endKey=nil, false)
    // ("empty end key with slice", current=b"abc", endKey=[]byte{}, false)
    // ("current equals end key", current=b"abc", endKey=b"abc", true)
    // ("current greater than end key", current=b"bcd", endKey=b"abc", true)
    // ("current less than end key", current=b"abc", endKey=b"bcd", false)
    // ("current empty, end key not empty", current=[], endKey=b"abc", false)
    // ("both empty", current=[], endKey=[], false)
    panic!("blocked on transcreating pkg/store/mockstore/unistore/tikv/util.go exceedEndKey");
}

/// Go `TestSortAndDedupHashVals` (`tikv/util_test.go:83`): sort ascending and
/// drop duplicates (Go mutates in place when `len > 1`; a 1-element or empty
/// slice is returned unchanged). All 8 table cases carry exact u64 vectors.
#[test]
#[ignore = "go-parity-gap: tikv/util.go sortAndDedupHashVals has no Rust transcreation yet"]
fn sort_and_dedup_hash_vals_table_vectors() {
    // [] → []
    // [1] → [1]
    // [1,2,3,4,5] → [1,2,3,4,5]
    // [5,3,1,4,2] → [1,2,3,4,5]
    // [3,1,4,1,5,9,2,6,5,3] → [1,2,3,4,5,6,9]
    // [7,7,7,7] → [7]
    // [3,3] → [3]
    // [3,1] → [1,3]
    panic!("blocked on transcreating pkg/store/mockstore/unistore/tikv/util.go sortAndDedupHashVals");
}

/// Go `TestSafeCopy` (`tikv/util_test.go:142`): returns a byte-for-byte equal
/// slice that is independent of the input buffer (mutating the original must
/// not affect the copy). Cases: nil→nil, empty→empty, single byte, "hello
/// world", binary `{0,1,2,255,254}`.
#[test]
#[ignore = "go-parity-gap: tikv/util.go safeCopy has no Rust transcreation yet"]
fn safe_copy_table_vectors() {
    // nil → nil; [] → []; [65] → [65]; "hello world" → "hello world";
    // {0,1,2,255,254} → same, with independence from the source buffer.
    panic!("blocked on transcreating pkg/store/mockstore/unistore/tikv/util.go safeCopy");
}
