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

//! Process-wide TiKV client settings owned by `pkg/util/tikvutil`.

use std::sync::atomic::{AtomicI32, Ordering};

/// Default value of the `tidb_committer_concurrency` system variable.
pub const DEFAULT_COMMITTER_CONCURRENCY: i32 = 128;

static COMMITTER_CONCURRENCY: AtomicI32 = AtomicI32::new(DEFAULT_COMMITTER_CONCURRENCY);

/// Returns the committer concurrency used when constructing TiKV clients.
#[must_use]
pub fn committer_concurrency() -> i32 {
    COMMITTER_CONCURRENCY.load(Ordering::SeqCst)
}

/// Publishes a validated `tidb_committer_concurrency` value process-wide.
pub fn set_committer_concurrency(value: i32) {
    COMMITTER_CONCURRENCY.store(value, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Restore(i32);

    impl Drop for Restore {
        fn drop(&mut self) {
            set_committer_concurrency(self.0);
        }
    }

    #[test]
    fn committer_concurrency_is_process_wide() {
        let _restore = Restore(committer_concurrency());
        set_committer_concurrency(321);
        assert_eq!(committer_concurrency(), 321);
    }
}
