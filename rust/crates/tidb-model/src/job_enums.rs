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

//! DDL-job enums from `pkg/meta/model/job.go`: `JobState`, `JobVersion`, and
//! `ModifyColumnType`. Extracted into their own module (Go keeps them in the
//! large `job.go`).

use std::sync::atomic::{AtomicI64, Ordering};

/// Go `JobState` (an `int32`): the state of a DDL job. A newtype over `i32`
/// so any stored value round-trips; [`Display`](std::fmt::Display) yields
/// `"none"` for the zero/unknown value, matching Go's `switch` default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobState(pub i32);

impl JobState {
    /// The job is absent (Go `JobStateNone`, zero value).
    pub const NONE: JobState = JobState(0);
    /// The job is running (Go `JobStateRunning`).
    pub const RUNNING: JobState = JobState(1);
    /// The job is rolling back (Go `JobStateRollingback`).
    pub const ROLLINGBACK: JobState = JobState(2);
    /// The rollback finished (Go `JobStateRollbackDone`).
    pub const ROLLBACK_DONE: JobState = JobState(3);
    /// The job is done (Go `JobStateDone`).
    pub const DONE: JobState = JobState(4);
    /// The job was cancelled (Go `JobStateCancelled`).
    pub const CANCELLED: JobState = JobState(5);
    /// The job is done and synchronized to all servers (Go `JobStateSynced`).
    pub const SYNCED: JobState = JobState(6);
    /// The client cancelled the job but the worker hasn't handled it
    /// (Go `JobStateCancelling`).
    pub const CANCELLING: JobState = JobState(7);
    /// The job hasn't started yet (Go `JobStateQueueing`).
    pub const QUEUEING: JobState = JobState(8);
    /// The job is paused (Go `JobStatePaused`).
    pub const PAUSED: JobState = JobState(9);
    /// The job is being paused (Go `JobStatePausing`).
    pub const PAUSING: JobState = JobState(10);

    // The state predicates from Go's `Job.Is*` methods that depend only on the
    // job state. `Job.IsRunning()` etc. are `self.state.is_running()`; the
    // Type/SchemaState-dependent predicates (IsPausable/IsRollbackable/...)
    // live on the Job struct.

    /// Go `Job.IsRunning`.
    #[must_use]
    pub fn is_running(self) -> bool {
        self == JobState::RUNNING
    }
    /// Go `Job.IsCancelling`.
    #[must_use]
    pub fn is_cancelling(self) -> bool {
        self == JobState::CANCELLING
    }
    /// Go `Job.IsDone`.
    #[must_use]
    pub fn is_done(self) -> bool {
        self == JobState::DONE
    }
    /// Go `Job.IsCancelled`.
    #[must_use]
    pub fn is_cancelled(self) -> bool {
        self == JobState::CANCELLED
    }
    /// Go `Job.IsSynced`.
    #[must_use]
    pub fn is_synced(self) -> bool {
        self == JobState::SYNCED
    }
    /// Go `Job.IsPaused`.
    #[must_use]
    pub fn is_paused(self) -> bool {
        self == JobState::PAUSED
    }
    /// Go `Job.IsPausing`.
    #[must_use]
    pub fn is_pausing(self) -> bool {
        self == JobState::PAUSING
    }
    /// Go `Job.IsQueueing`.
    #[must_use]
    pub fn is_queueing(self) -> bool {
        self == JobState::QUEUEING
    }
    /// Go `Job.IsRollingback`.
    #[must_use]
    pub fn is_rollingback(self) -> bool {
        self == JobState::ROLLINGBACK
    }
    /// Go `Job.IsRollbackDone`.
    #[must_use]
    pub fn is_rollback_done(self) -> bool {
        self == JobState::ROLLBACK_DONE
    }
    /// Go `Job.NotStarted`: the job is absent or queued.
    #[must_use]
    pub fn not_started(self) -> bool {
        self == JobState::NONE || self == JobState::QUEUEING
    }
    /// Go `Job.IsFinished`: done, rolled back, or cancelled.
    #[must_use]
    pub fn is_finished(self) -> bool {
        self == JobState::DONE || self == JobState::ROLLBACK_DONE || self == JobState::CANCELLED
    }
    /// Go `Job.InFinalState`: synced, cancelled, or paused.
    #[must_use]
    pub fn in_final_state(self) -> bool {
        self == JobState::SYNCED || self == JobState::CANCELLED || self == JobState::PAUSED
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            JobState::RUNNING => "running",
            JobState::ROLLINGBACK => "rollingback",
            JobState::ROLLBACK_DONE => "rollback done",
            JobState::DONE => "done",
            JobState::CANCELLED => "cancelled",
            JobState::CANCELLING => "cancelling",
            JobState::SYNCED => "synced",
            JobState::QUEUEING => "queueing",
            JobState::PAUSED => "paused",
            JobState::PAUSING => "pausing",
            // JobStateNone and any unknown value.
            _ => "none",
        })
    }
}

/// Go `JobVersion` (an `int64`): the storage version of a DDL job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobVersion(pub i64);

impl JobVersion {
    /// The first version: job args stored as an untyped array (pre-v8.4.0).
    pub const V1: JobVersion = JobVersion(1);
    /// The second version: job args stored as typed structs (v8.4.0+).
    pub const V2: JobVersion = JobVersion(2);
}

impl std::fmt::Display for JobVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            JobVersion::V1 => f.write_str("v1"),
            JobVersion::V2 => f.write_str("v2"),
            JobVersion(v) => write!(f, "unknown({v})"),
        }
    }
}

/// Go's `jobVerInUse`: the DDL-job version new jobs use on this node.
static JOB_VER_IN_USE: AtomicI64 = AtomicI64::new(0);

/// Go `SetJobVerInUse`.
pub fn set_job_ver_in_use(ver: JobVersion) {
    JOB_VER_IN_USE.store(ver.0, Ordering::SeqCst);
}

/// Go `GetJobVerInUse`.
#[must_use]
pub fn get_job_ver_in_use() -> JobVersion {
    JobVersion(JOB_VER_IN_USE.load(Ordering::SeqCst))
}

/// The kind of a modify-column job (Go's `ModifyColumnType`, a `byte`).
///
/// Value 6 (`mysql.TypeNull`) is intentionally skipped for compatibility
/// with older TiDB versions.
pub mod modify_type {
    /// No modification (Go `ModifyTypeNone`).
    pub const NONE: u8 = 0;
    /// No reorganization or check needed (Go `ModifyTypeNoReorg`).
    pub const NO_REORG: u8 = 1;
    /// No reorg, but the existing data must be checked
    /// (Go `ModifyTypeNoReorgWithCheck`).
    pub const NO_REORG_WITH_CHECK: u8 = 2;
    /// Only the index needs reorganizing (Go `ModifyTypeIndexReorg`).
    pub const INDEX_REORG: u8 = 3;
    /// Both row and index data need reorganizing (Go `ModifyTypeReorg`).
    pub const REORG: u8 = 4;
    /// A varchar->char conversion with a data pre-check
    /// (Go `ModifyTypePrecheck`).
    pub const PRECHECK: u8 = 5;
}

/// Go `ModifyTypeToString`: the label of a modify-column type. Unknown values
/// yield `""`.
#[must_use]
pub fn modify_type_to_string(tp: u8) -> &'static str {
    match tp {
        modify_type::NONE => "none",
        modify_type::NO_REORG => "modify meta only",
        modify_type::NO_REORG_WITH_CHECK => "modify meta only with range check",
        modify_type::INDEX_REORG => "reorg index only",
        modify_type::REORG => "reorg row and index",
        modify_type::PRECHECK => "prechecking",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestState: every listed job state stringifies non-empty.
    #[test]
    fn job_state_strings_non_empty() {
        for state in [
            JobState::RUNNING,
            JobState::DONE,
            JobState::CANCELLED,
            JobState::ROLLINGBACK,
            JobState::ROLLBACK_DONE,
            JobState::SYNCED,
        ] {
            assert!(!state.to_string().is_empty());
        }
    }

    #[test]
    fn job_state_exact() {
        assert_eq!(JobState::NONE.to_string(), "none");
        assert_eq!(JobState::ROLLBACK_DONE.to_string(), "rollback done");
        assert_eq!(JobState::QUEUEING.to_string(), "queueing");
        assert_eq!(JobState(123).to_string(), "none");
        assert_eq!(JobState::default(), JobState::NONE);
    }

    #[test]
    fn job_state_predicates() {
        assert!(JobState::RUNNING.is_running());
        assert!(JobState::DONE.is_done());
        assert!(JobState::CANCELLED.is_cancelled());
        assert!(JobState::SYNCED.is_synced());
        assert!(JobState::PAUSED.is_paused());
        assert!(JobState::PAUSING.is_pausing());
        assert!(JobState::QUEUEING.is_queueing());
        assert!(JobState::ROLLINGBACK.is_rollingback());
        assert!(JobState::ROLLBACK_DONE.is_rollback_done());
        assert!(JobState::CANCELLING.is_cancelling());

        // not_started: None or Queueing.
        assert!(JobState::NONE.not_started());
        assert!(JobState::QUEUEING.not_started());
        assert!(!JobState::RUNNING.not_started());

        // is_finished: Done, RollbackDone, or Cancelled.
        assert!(JobState::DONE.is_finished());
        assert!(JobState::ROLLBACK_DONE.is_finished());
        assert!(JobState::CANCELLED.is_finished());
        assert!(!JobState::SYNCED.is_finished());

        // in_final_state: Synced, Cancelled, or Paused.
        assert!(JobState::SYNCED.in_final_state());
        assert!(JobState::CANCELLED.in_final_state());
        assert!(JobState::PAUSED.in_final_state());
        assert!(!JobState::DONE.in_final_state());
    }

    #[test]
    fn job_version_string_and_accessor() {
        assert_eq!(JobVersion::V1.to_string(), "v1");
        assert_eq!(JobVersion::V2.to_string(), "v2");
        assert_eq!(JobVersion(7).to_string(), "unknown(7)");

        set_job_ver_in_use(JobVersion::V2);
        assert_eq!(get_job_ver_in_use(), JobVersion::V2);
        set_job_ver_in_use(JobVersion::V1);
        assert_eq!(get_job_ver_in_use(), JobVersion::V1);
    }

    #[test]
    fn modify_type_strings() {
        assert_eq!(modify_type_to_string(modify_type::NONE), "none");
        assert_eq!(
            modify_type_to_string(modify_type::NO_REORG),
            "modify meta only"
        );
        assert_eq!(
            modify_type_to_string(modify_type::NO_REORG_WITH_CHECK),
            "modify meta only with range check"
        );
        assert_eq!(
            modify_type_to_string(modify_type::INDEX_REORG),
            "reorg index only"
        );
        assert_eq!(
            modify_type_to_string(modify_type::REORG),
            "reorg row and index"
        );
        assert_eq!(modify_type_to_string(modify_type::PRECHECK), "prechecking");
        assert_eq!(modify_type_to_string(99), "");
    }
}
