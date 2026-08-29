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

//! Public `pkg/util/kvcache` surface at the existing `tidb-util` boundary.

use std::sync::{Arc, OnceLock};

use crate::memory::{Tracker, LABEL_FOR_GLOBAL_SIMPLE_LRU_CACHE};

pub use tidb_kvcache::*;

/// Returns the package-global tracker exported by the source package.
#[must_use]
pub fn global_lru_memory_tracker() -> &'static Arc<Tracker> {
    static TRACKER: OnceLock<Arc<Tracker>> = OnceLock::new();
    TRACKER.get_or_init(|| Tracker::new(LABEL_FOR_GLOBAL_SIMPLE_LRU_CACHE, -1))
}
