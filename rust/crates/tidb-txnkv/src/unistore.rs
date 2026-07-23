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

//! Standalone-UniStore process mode from `pkg/kv/unistore.go`.

use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this TiDB process uses standalone UniStore.
///
/// Atomic access preserves the source process-global behavior without exposing
/// a data race.
pub static STANDALONE_TIDB: AtomicBool = AtomicBool::new(false);

/// Returns the current standalone-UniStore mode.
#[must_use]
pub fn standalone_tidb() -> bool {
    STANDALONE_TIDB.load(Ordering::Acquire)
}

/// Updates standalone-UniStore mode during process configuration.
pub fn set_standalone_tidb(enabled: bool) {
    STANDALONE_TIDB.store(enabled, Ordering::Release);
}
