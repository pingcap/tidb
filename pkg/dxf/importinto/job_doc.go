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

package importinto

// IMPORT INTO job and DXF task transaction notes.
//
// This file documents the important transaction boundaries between the
// user-facing import job row and the DXF task row. It is intentionally kept
// close to the importinto code because the behavior depends on several
// helpers in this package plus the DXF framework.
//
// Two rows describe one IMPORT INTO job:
//
//   - mysql.tidb_import_jobs stores the user-visible import job status,
//     step, summary, and error. In nextgen user keyspace mode this row lives
//     in the user keyspace.
//   - mysql.tidb_global_task stores the DXF task state, step, task key, and
//     keyspace. In nextgen user keyspace mode this row lives in the SYSTEM
//     keyspace, because DXF runs as a SYSTEM service.
//
// The normal nextgen submit path is intentionally not atomic across these two
// rows:
//
//   1. Create the import job in the current/user keyspace with
//      storage.GetTaskManager().WithNewTxn.
//   2. Commit the import job row as status=pending, step=''.
//   3. Open the DXF service task manager with storage.GetDXFSvcTaskMgr().
//   4. Create the SYSTEM keyspace DXF task in a second WithNewTxn.
//   5. Commit the DXF task row as state=pending, step=StepInit.
//
// Classic kernel mode and SYSTEM keyspace mode do not have this split: job
// creation and task creation are in the same transaction there. The split only
// exists for nextgen user keyspace imports.
//
// This split is a consequence of how nextgen DXF service and IMPORT INTO
// evolved from the Classic kernel implementation. Classic kernel DXF and
// IMPORT INTO ran in one keyspace, so the import job and DXF task could be
// submitted atomically in one transaction. Nextgen moved DXF service work into
// SYSTEM while keeping the user-facing IMPORT INTO job in the user keyspace.
// That creates a split-brain-style consistency window between the job row and
// the task row. The current design is not the cleanest possible solution, but
// it is the practical one for now because it preserves the existing IMPORT
// INTO job model while allowing DXF to run as a SYSTEM service.
//
// A cleaner long-term design would make the ownership boundary explicit: DXF
// service in SYSTEM owns only DXF tasks, and TiDB/IMPORT INTO owns import jobs
// inside the user keyspace. The two sides would communicate through a clearer
// contract instead of relying on partially coordinated table updates across
// keyspaces. That would avoid this class of split-brain issue more directly,
// but it is a larger design change and is not part of the current
// implementation.
//
// Transaction and statement nodes used below:
//
//   J
//     The user-keyspace import job transaction commits. The job is externally
//     visible as pending.
//
//   T
//     The SYSTEM-keyspace DXF task transaction commits. The task is externally
//     visible as pending/StepInit.
//
//   C0
//     CANCEL IMPORT JOB runs checkPrivilegeAndStatus. This reads the import
//     job row and allows cancellation only when the job status is pending or
//     running and the caller has privilege.
//
//   C1
//     CANCEL IMPORT JOB runs CancelTaskByKeySession in a SYSTEM-keyspace
//     transaction. It updates a live DXF task to cancelling only when the
//     current DXF task state is pending, running, or awaiting-resolution.
//     The affected row count is not checked, so "no task", "task not
//     committed yet", "already terminal", and "state not matched" all look
//     like success to this step.
//
//   C2
//     CANCEL IMPORT JOB runs WaitTaskDoneByKey. The first lookup checks live
//     and history task tables by task key. If no task is found, it returns
//     ErrTaskNotFound. If a task is found, it polls by task ID until the DXF
//     task is succeed, reverted, or failed. It does not verify that the task
//     was actually cancelled.
//
//   C3
//     If C2 returns ErrTaskNotFound, cancelDanglingImportJob runs
//     CancelPendingJob in the user keyspace. It directly changes the import
//     job from pending to cancelled. Unlike most scheduler writes, this path
//     checks affected rows and returns "job state changed during cancel,
//     please try again later" if the job is no longer pending.
//
//   S0
//     The import scheduler runs checkImportJobNotCancelled. It reads only the
//     import job row. A normal visible-task cancel is not visible to this
//     check until the scheduler later writes the import job to cancelled.
//
//   S1
//     The import scheduler runs StartJob. It updates pending -> running and
//     sets the first import job step. The SQL predicate requires status
//     pending, but the affected row count is ignored.
//
//   S2
//     The import scheduler runs Job2Step. It updates the import job step when
//     status is running. The affected row count is ignored.
//
//   SF, SE, SC
//     The import scheduler runs FinishJob, FailJob, or CancelJob. These are
//     terminal import-job writes guarded by the current status. Their affected
//     row counts are ignored, so the first terminal writer wins and later
//     terminal writers silently no-op.
//
// Submit and cancel interleavings.
//
// The following cases partition the externally different time sequences for
// nextgen user keyspace submit and user cancellation:
//
//   1. C0 < J
//      The job row is not visible. CANCEL IMPORT JOB returns job-not-found.
//      This normally requires an out-of-band or guessed job ID.
//
//   2. J commits, but T never commits
//      The import job remains pending without a DXF task. This can happen if
//      the SYSTEM DXF task submission fails after the user job transaction has
//      already committed. A later dangling cancel can still cancel the pending
//      import job.
//
//   3. J < C0 < C1(0 rows) < C2(no task) < C3, and T never commits
//      CancelTaskByKeySession misses the absent DXF task. WaitTaskDoneByKey
//      also finds no task. cancelDanglingImportJob changes the import job from
//      pending to cancelled. No DXF task exists.
//
//   4. J < C0 < C1(0 rows) < C2(no task) < C3 < T
//      The user sees cancel success because the import job row was directly
//      marked cancelled. The submitter can still later commit the SYSTEM DXF
//      task because the submit path does not recheck the job status before T.
//      The scheduler should then see the cancelled import job at S0 and drive
//      the DXF task to revert before import work starts.
//
//   5. J < C0 < C1(0 rows) < T < C2(found task)
//      This is the known missed-cancel window. C1 ran before the task was
//      committed, so no durable cancel marker was recorded in the DXF task
//      table. C2 then finds the task, so the dangling-job fallback does not
//      run. The cancel command waits for the task's natural terminal state;
//      without another cancel, the task can finish or fail normally, and the
//      command can still return nil because WaitTaskDoneByKey accepts any done
//      state.
//
//   6. J < C0 < C1(0 rows) < C2(no task) < T < S1 < C3
//      The dangling fallback races with a newly committed task and scheduler
//      start. If S1 changes the import job to running first, C3 affects zero
//      rows and returns "job state changed during cancel, please try again
//      later". The task continues unless another cancel succeeds.
//
//   7. J < T < C1, and the DXF task is pending, running, or
//      awaiting-resolution
//      This is normal visible-task cancellation. C1 changes the DXF task to
//      cancelling. The DXF scheduler later changes cancelling -> reverting ->
//      reverted, and the import scheduler maps the cancellation error to the
//      import job status cancelled.
//
//   8. J < T < C1, but the DXF task is already cancelling, reverting,
//      succeed, failed, reverted, or otherwise outside C1's predicate
//      C1 affects zero rows and does not report that fact. If C2 finds a live
//      or history task, the command waits for or observes terminal completion.
//      This can look like cancel success even though this cancel did not
//      change the task.
//
//   9. C0 reads a terminal import job, or the caller lacks privilege
//      Cancellation stops before touching the DXF task.
//
// Scheduler and DXF framework interleavings after T is visible.
//
//   - Normal visible-task cancellation changes only the DXF task first. The
//     import job row stays pending or running until the scheduler reaches the
//     cancellation OnDone path and runs SC.
//
//   - S0 checks only the import job row. Therefore C1 can happen before S0 and
//     S0 can still pass, because C1 changed the DXF task state but did not
//     change the import job status.
//
//   - Between S0 and S1 or S2, a direct import-job cancellation makes S1/S2
//     affect zero rows. A normal visible-task cancellation does not; S1/S2 can
//     still advance the import job status or step before the DXF cancellation
//     is processed.
//
//   - During business-step planning, the order is:
//
//         S0 -> S1 or S2 -> generate subtask metadata -> SwitchTaskStep
//
//     If C1 wins before SwitchTaskStep, the task-step update no-ops and no
//     subtasks are inserted in the normal non-batch path. If SwitchTaskStep
//     wins first, C1 can still match running and cancellation proceeds.
//
//   - The batch step-switch path inserts subtask batches before the task
//     state/step update. A concurrent cancel can leave pending subtasks while
//     the task is already cancelling. Executors should not run those subtasks
//     as normal work while the task is cancelling; once the task becomes
//     reverting, executor managers cancel and drain pending/running subtasks.
//
//   - If a running subtask fails and manual recovery is enabled, the framework
//     tries to move the DXF task to awaiting-resolution. C1 is allowed from
//     awaiting-resolution. If C1 wins first, the awaiting-resolution update
//     affects zero rows; if awaiting-resolution wins first, C1 changes it to
//     cancelling.
//
//   - On successful completion, the import scheduler's OnDone hook runs before
//     the DXF framework persists succeed. If FinishJob commits before a user
//     cancel, the import job can become finished. If the cancel then changes
//     the DXF task to cancelling before SucceedTask commits, SucceedTask can
//     affect zero rows and the DXF task can later become reverted while the
//     import job remains finished. The later CancelJob sees a terminal import
//     job and silently no-ops.
//
//   - On failure or cancellation completion, the first terminal import-job
//     writer wins. FailJob and CancelJob both match pending/running jobs and
//     ignore affected rows. A losing terminal writer silently no-ops.
//
// Other import-job-related actors.
//
//   - Active-job precheck is a separate read before CreateJob. Two concurrent
//     imports can both observe zero active jobs before either creates a job.
//
//   - Non-detached IMPORT INTO connection cancellation calls the same
//     cancelAndWaitImportJob path and inherits the same races.
//
//   - The IMPORT SDK and Lightning do not directly update tidb_import_jobs.
//     They enter through SQL front ends such as IMPORT INTO, CANCEL IMPORT JOB,
//     SHOW IMPORT JOB, and SHOW IMPORT GROUP. Lightning group cancellation has
//     an extra stale-snapshot window: it reads group jobs and then cancels each
//     non-completed job one by one.
//
//   - SHOW IMPORT JOBS and SHOW IMPORT GROUPS are observers, not writers. They
//     read import job rows first and then read DXF runtime/subtask information
//     separately for running jobs. Their result can mix snapshots from
//     different moments.
//
//   - awaiting-resolution is not stored as an import job status. The import
//     job row remains running; SHOW IMPORT JOB overlays awaiting-resolution
//     from the DXF task state when runtime information is available.
//
//   - IMPORT INTO ... FROM SELECT returns through the import-from-select path
//     before submitTask and does not use this import-job/DXF-task table path.
//
// Practical risks that need to be fixed later.
//
//   1. Highest: J < C0 < C1(0 rows) < T < C2(found task). The cancel request
//      is missed, the dangling fallback is skipped, and the command waits for
//      natural task completion.
//
//   2. High: success hook race. FinishJob can commit before a later cancel
//      prevents SucceedTask from committing, leaving an import job marked
//      finished while the DXF task later reverts.
//
//   3. Medium: C2(no task) < T < S1 < C3. The dangling fallback loses because
//      the import job is no longer pending and reports retry-later.
//
//   4. Medium: step/status drift. Normal visible-task cancellation does not
//      mark the import job cancelled until scheduler OnDone, so job status and
//      step can advance after the user requested cancellation.
//
//   5. Observer-only: SHOW IMPORT, SDK, and Lightning can observe mixed
//      snapshots, but they do not introduce new direct import-job writers.
