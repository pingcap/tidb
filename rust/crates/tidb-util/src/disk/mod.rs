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
//! [`SpillStorage`] is the one immutable process authority for path,
//! encryption, quota, directory lease, stale-file cleanup, and secure file
//! creation. Keeping those decisions together prevents a query operator from
//! silently bypassing startup policy through a second mutable global.

pub mod spill_storage;

pub use spill_storage::{
    SpillEncryptionMethod, SpillEncryptionParseError, SpillStorage, SpillStorageOpenError,
    SpillStorageSpec, LOCAL_TEMPORARY_SPACE_QUOTA_ERROR,
};

use std::sync::Arc;

/// Go `disk.Tracker = memory.Tracker`.
pub type Tracker = crate::memory::Tracker;

/// Go `disk.NewTracker`.
#[must_use]
pub fn new_tracker(label: i64, bytes_limit: i64) -> Arc<Tracker> {
    Tracker::new(label, bytes_limit)
}
