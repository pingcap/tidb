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

//! Public contract tests for the complete `pkg/util/tikvutil` transcreation.

use std::sync::atomic::Ordering;

use tidb_tikvutil::COMMITTER_CONCURRENCY;

/// The Go package has no test file; this contract keeps its one exported
/// process-wide atomic observable through the public Rust crate boundary.
#[test]
fn exported_committer_concurrency_is_an_atomic_i32_with_source_default() {
    let previous = COMMITTER_CONCURRENCY.swap(128, Ordering::SeqCst);
    struct Restore(i32);
    impl Drop for Restore {
        fn drop(&mut self) {
            COMMITTER_CONCURRENCY.store(self.0, Ordering::SeqCst);
        }
    }
    let _restore = Restore(previous);

    assert_eq!(COMMITTER_CONCURRENCY.load(Ordering::SeqCst), 128);
    COMMITTER_CONCURRENCY.store(321, Ordering::SeqCst);
    assert_eq!(COMMITTER_CONCURRENCY.load(Ordering::SeqCst), 321);
    assert_eq!(
        COMMITTER_CONCURRENCY.fetch_add(1, Ordering::SeqCst),
        321,
        "the atomic must retain the source's signed 32-bit arithmetic domain"
    );
    assert_eq!(COMMITTER_CONCURRENCY.load(Ordering::SeqCst), 322);
}
