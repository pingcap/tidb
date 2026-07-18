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

//! Stats-read/writer version and slow-save decisions from
//! `pkg/statistics/handle/storage/stats_read_writer.go`.
//!
//! The Go owner refreshes `stats_meta` after a slow save only when the stats
//! lease is positive and elapsed time reaches five lease intervals; its
//! failpoint can force that branch. It records historical metadata only after
//! a successful operation produces a nonzero version. This leaf keeps those
//! scalar decisions and the source error text over caller-owned inputs. SQL,
//! transaction/session/cache work, logging, failpoint plumbing, and storage
//! lifecycle remain external.

/// Source `cache.LeaseOffset` used by slow stats saving.
pub const LEASE_OFFSET: i64 = 5;

/// Error returned when the slow-save stats-meta refresh fails.
pub const SLOW_STATS_SAVE_ERROR_MESSAGE: &str =
    "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later";

/// Whether a successful operation should record historical stats metadata.
///
/// This is the deferred condition used by `UpdateStatsMetaVersionForGC` and
/// the other stats-read/writer save paths: failures and version zero suppress
/// the record.
#[must_use]
pub const fn historical_stats_meta_record_required(
    operation_succeeded: bool,
    stats_version: u64,
) -> bool {
    operation_succeeded && stats_version != 0
}

/// Whether slow stats saving should refresh `stats_meta`.
///
/// `lease_nanos` and `elapsed_nanos` are signed nanosecond values matching Go's
/// `time.Duration`. The source explicitly disables the elapsed check for a
/// non-positive lease, while `force` models the `slowStatsSaving` failpoint.
/// The multiplication wraps like Go's signed duration arithmetic.
#[must_use]
pub const fn slow_stats_saving_requires_meta_update(
    lease_nanos: i64,
    elapsed_nanos: i64,
    force: bool,
) -> bool {
    force || (lease_nanos > 0 && elapsed_nanos >= lease_nanos.wrapping_mul(LEASE_OFFSET))
}
