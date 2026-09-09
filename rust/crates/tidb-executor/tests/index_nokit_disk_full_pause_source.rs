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

//! Port ledger for `pkg/ddl/index_nokit_test.go` (pkg/ddl.part7 items 362-363).
//!
//! ONE test half is a functional port: the job-state assertions of
//! `TestShouldAutoPauseExistingKVDiskFullTask` run over the transcreated
//! `model.Job` reason carriers (tidb_model::job, `pkg/meta/model/job.go:713`,
//! `:753-760`, `:1293-1300`). The task-side predicate and the pause applier
//! have no Rust carrier, and `modifyTaskParamLoop` needs the systable/DXF
//! task-manager pair; both stay documentary gap ports.

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

/// GO PORT of `pkg/ddl/index_nokit_test.go:35
/// TestShouldAutoPauseExistingKVDiskFullTask` (task predicate + applier half).
///
/// Re-derived contract: `shouldAutoPauseExistingKVDiskFullTask` is true iff
/// the DXF task is paused AND its error is a KV-disk-full error AND the job
/// has no `tikv_disk_full` resume reason (pkg/ddl/index.go:3144-3151, using
/// `errdef.IsKVDiskFullError`); a running task or a non-disk-full error both
/// give false. `autoPauseAddIndexJobOnKVDiskFull` (index.go:3115-3128) sets
/// the job to Pausing under `AdminCommandBySystem`, stores the pause reason
/// with the "DXF add-index task %d hit %s disk full: %s" message (store type
/// decided by `kvDiskFullStoreType`, index.go:3130-3142: "TiFlash" when the
/// error mentions tiflash, "TiKV" when it mentions tikv, else "storage
/// node"), clears the resume reason, and returns
/// `ErrDDLAutoPausedByKVDiskFull` wrapping job ID and message.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for shouldAutoPauseExistingKVDiskFullTask/autoPauseAddIndexJobOnKVDiskFull (pkg/ddl/index.go:3115-3151) or errdef.IsKVDiskFullError task-error classification"]
fn should_auto_pause_gates_on_paused_task_state_and_kv_disk_full_error() {}

/// GO PORT of `pkg/ddl/index_nokit_test.go:70 TestModifyTaskParamLoop`.
///
/// Re-derived contract (pkg/ddl/index.go:3359-3437): the loop wakes on
/// `UpdateDDLJobReorgCfgInterval` (backfilling.go:944-945, shrunk to 10ms by
/// the test) or `done`, then re-reads the job by ID — a plain error retries,
/// `systable.ErrNotFound` returns; it derives required slots from the job's
/// ReorgMeta concurrency via `adjustConcurrency` (errors retry), diffs
/// concurrency/batch-size/max-write-speed against the last applied triple,
/// and continues when nothing changed; a changed triple fetches the task —
/// `storage.ErrTaskNotFound` returns, other errors retry, a state that
/// cannot move to modifying retries, and only then does it call
/// `ModifyTaskByID` with `{PrevState, [ModifyRequiredSlots, ModifyBatchSize,
/// ModifyMaxWriteSpeed]}`; a modify error retries, success updates the
/// last-applied triple so the same values do not modify twice. The Go test
/// drives all of this through gomock doubles of the systable and DXF-task
/// managers, asserting `ctrl.Satisfied()` per scenario.
#[test]
#[ignore = "go-parity-gap: modifyTaskParamLoop (pkg/ddl/index.go:3359-3437) needs the systable.Manager and DXF storage.Manager abstraction pair plus proto.Task/ModifyParam carriers, none transcreated"]
fn modify_task_param_loop_pushes_reorg_meta_changes_to_the_task_manager() {}
