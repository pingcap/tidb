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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_datatype::GoString;
use tidb_model::job::{JOB_PAUSE_REASON_KV_DISK_FULL, JOB_RESUME_REASON_KV_DISK_FULL};
use tidb_model::{AdminCommandOperator, Job, JobState};

/// GO PORT of `pkg/ddl/index_nokit_test.go:35 TestShouldAutoPauseExistingKVDiskFullTask`
/// (job-state half only; the task predicate half is the gap port below).
///
/// Re-derived contract from the Go test's assertions over `model.Job`
/// (pkg/meta/model/job.go:713 IsPausedBySystem, :753-760
/// IsPausingOrPausedBySystemForKVDiskFull, :1293-1300 the reason constants):
/// setting the resume reason makes `HasResumeReason("tikv_disk_full")` true
/// (which is what forces `shouldAutoPauseExistingKVDiskFullTask` to false),
/// `ClearResumeReason` drops it; after the auto-pause shape — state Pausing,
/// `AdminCommandBySystem`, pause reason `tikv_disk_full` with a message
/// naming the storage node — the job reports
/// `IsPausingOrPausedBySystemForKVDiskFull()` true, the pause message carries
/// the store-type text ("TiFlash disk full"), and `ResumeReason` is nil
/// because `autoPauseAddIndexJobOnKVDiskFull` clears it
/// (pkg/ddl/index.go:3115-3128).
#[test]
fn kv_disk_full_pause_round_trips_the_job_reason_carriers() {
    // The Go test seeds `job.ResumeReason` and expects the auto-pause gate to
    // close: `HasResumeReason(JobResumeReasonKVDiskFull)` must be observable.
    let mut job = Job::default();
    assert!(!job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
    job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
    assert!(job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));

    // `autoPauseAddIndexJobOnKVDiskFull` (index.go:3122) clears the resume
    // reason; after that the gate would open again.
    job.clear_resume_reason();
    assert!(!job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
    assert!(job.resume_reason.is_none());

    // The pause shape applied by index.go:3118-3121: Pausing state, system
    // operator, durable `tikv_disk_full` reason whose message embeds the
    // store-type and task text ("... hit TiFlash disk full: ...").
    job.state = JobState::PAUSING;
    job.admin_operator = AdminCommandOperator::BY_SYSTEM;
    job.set_pause_reason(
        JOB_PAUSE_REASON_KV_DISK_FULL,
        "DXF add-index task 127 hit TiFlash disk full: the remaining storage capacity of TiFlash(127.0.0.1:3930) is less than 10%",
    );
    assert!(job.is_pausing_or_paused_by_system_for_kv_disk_full());
    let pause = job.pause_reason.as_ref().unwrap().clone();
    let pause_message = pause.read().message.to_utf8_lossy_go();
    assert!(pause_message.contains("TiFlash disk full"));
    // The Go test also pins `require.NotContains(err, "because TiKV disk is
    // full")` / "hit TiKV disk full": the message names the TiFlash store.
    assert!(!pause_message.contains("TiKV disk full"));

    // A user-paused job (not BY_SYSTEM) with the same reason must NOT count
    // as a system KV-disk-full pause (job.go:757-760 requires BY_SYSTEM).
    let mut user_paused = Job::default();
    user_paused.state = JobState::PAUSING;
    user_paused.admin_operator = AdminCommandOperator::BY_END_USER;
    user_paused.set_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL, "user asked");
    assert!(!user_paused.is_pausing_or_paused_by_system_for_kv_disk_full());

    let _ = GoString::from("type check: reason fields are Go strings");
}
