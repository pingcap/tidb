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

//! Transcreation of Go `pkg/util/disk`: temporary-storage directory lifecycle
//! and disk-usage tracker constructors.

mod temp_dir;

pub use temp_dir::{check_and_create_dir, check_and_init_temp_dir, clean_up, initialize_temp_dir};

use std::sync::Arc;

/// Go `disk.Tracker = memory.Tracker`.
pub type Tracker = crate::memory::Tracker;

/// Go `disk.NewTracker`.
#[must_use]
pub fn new_tracker(label: i64, bytes_limit: i64) -> Arc<Tracker> {
    Tracker::new(label, bytes_limit)
}

/// Go `disk.NewGlobalTracker`.
#[must_use]
pub fn new_global_tracker(label: i64, bytes_limit: i64) -> Arc<Tracker> {
    Tracker::new_global(label, bytes_limit)
}
