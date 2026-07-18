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

//! Synchronous statistics-load worker concurrency from
//! `pkg/statistics/handle/syncload/stats_syncload.go`.
//!
//! The Go helper reads the process CPU count at runtime. This leaf accepts
//! that already-observed count so the threshold policy is deterministic and
//! testable; runtime probing, queue sizing, worker lifecycle, and storage
//! loading remain external boundaries.

/// Return the source concurrency for an already-observed CPU count.
///
/// The four inclusive ranges intentionally preserve the Go policy: up to 8
/// CPUs uses 5 workers, up to 16 uses 6, up to 32 uses 8, and larger machines
/// use 10.
#[must_use]
pub const fn sync_load_concurrency_for_cpu(core_count: usize) -> usize {
    if core_count <= 8 {
        5
    } else if core_count <= 16 {
        6
    } else if core_count <= 32 {
        8
    } else {
        10
    }
}
