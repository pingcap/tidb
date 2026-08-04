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

//! Transcreation of Go `pkg/util/disk`: the temporary-storage directory every
//! spilling operator writes into, and the disk-usage tracker.
//!
//! Go `tracker.go` is one line -- `type Tracker = memory.Tracker` -- so the
//! disk tracker here is [`crate::memory::Tracker`] re-exported under the same
//! name, and `NewTracker` is [`new_tracker`].
//!
//! NOT PORTED, named so it is not mistaken for covered: Go
//! `InitializeTempDir` takes a `gofslock` advisory lock on `<tmp>/_dir.lock`
//! so two tidb-servers configured onto the same `tmp-storage-path` refuse to
//! share it, and `CleanUp` releases it. An advisory `flock(2)` needs a libc
//! dependency this workspace does not carry, so the lock is absent here: the
//! directory is created and stale contents are swept, but a second process
//! pointed at the same directory is NOT refused. That is a misconfiguration
//! guard, not a query-path behavior; the spill path itself is unaffected
//! because each spill file is created with a unique random name.

pub mod temp_dir;

pub use temp_dir::{
    check_and_create_dir, check_and_init_temp_dir, clean_up, encode_def_temp_storage_dir,
    initialize_temp_dir, set_temp_storage_path, temp_storage_path,
};

use std::sync::Arc;

/// Go `disk.Tracker = memory.Tracker`.
pub type Tracker = crate::memory::Tracker;

/// Go `disk.NewTracker`.
#[must_use]
pub fn new_tracker(label: i64, bytes_limit: i64) -> Arc<Tracker> {
    Tracker::new(label, bytes_limit)
}
