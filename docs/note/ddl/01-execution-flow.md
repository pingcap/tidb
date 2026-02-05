# DDL Execution Flow (SQL → Job → Owner → Workers → Done)

This doc explains the *execution skeleton* of TiDB DDL and the responsibility boundary of each layer.

## 1) SQL executor layer: `pkg/executor.DDLExec`

Entry: `pkg/executor/ddl.go` (`type DDLExec`, `(*DDLExec).Next`)

Responsibilities:

- End the previous transaction and start a new internal DDL transaction context.
- Perform statement-level branching (AST switch) and call `ddl.Executor` methods.
- Convert “schema outdated” errors into `ErrInfoSchemaChanged` so the caller can retry safely (`(*DDLExec).toErr`).

Non-responsibilities (avoid putting logic here):

- Persisting schema changes directly (should go through DDL jobs).
- Any logic that must survive owner transfer / restart (belongs to job execution).

## 2) DDL executor (DDL module): statement → job(s) → wait

Entry: `pkg/ddl/executor.go` (`type Executor`, `(*executor).DoDDLJobWrapper`)

The DDL module has its own “executor” (different from `pkg/executor/`). Its job is:

1. Validate/normalize statement-level details that are *DDL-module specific* (e.g. build `TableInfo`, expand options).
2. Build a persistent `model.Job` (or multiple jobs) and wrap it in `ddl.JobWrapper`.
3. Submit the job via `JobSubmitter` by sending it into a bounded channel (`deliverJobTask` → `limitJobCh`).
4. Block the SQL session until the job finishes, using:
   - an etcd notification channel (fast path), and
   - a ticker-based polling fallback (lease-based) for robustness.

The waiting loop is in `(*executor).DoDDLJobWrapper` and includes:

- Attaching `StmtCtx.DDLJobID` for `KILL` to cancel the job.
- Periodic job state checking (to fetch final result or detect errors).
- Cancellation path when the connection is killed.

## 3) Job submission: batching, ID allocation, writing `mysql.tidb_ddl_job`

Entry: `pkg/ddl/job_submitter.go` (`type JobSubmitter`, `submitLoop`, `addBatchDDLJobs`)

Key ideas:

- Jobs are collected from `limitJobCh` and submitted in batch.
- The submitter allocates IDs (schema/table/index IDs) if needed.
- Jobs are inserted into the DDL job table (`mysql.tidb_ddl_job`), then the submitter notifies the owner scheduler (`notifyNewJobSubmitted`).
- When `tidb_enable_fast_create_table` is enabled, multiple create-table jobs may be merged into a single “batch create table” job.

## 4) Owner election: only the owner runs scheduler + workers

Entry: `pkg/ddl/job_scheduler.go` (`ownerListener.OnBecomeOwner`)

- DDL uses an owner manager (`owner.Manager`, etcd-backed) to elect exactly one owner.
- When a node becomes owner, it starts `jobScheduler` and worker pools.
- When it retires, scheduler/workers are stopped; another node will take over.

This is why DDL execution must be resumable and driven by persistent job state.

## 5) Scheduling and execution: worker pools and job step transitions

Entrypoints:

- Scheduler: `pkg/ddl/job_scheduler.go` (`jobScheduler.start`, `scheduleLoop`)
- Worker core: `pkg/ddl/job_worker.go` (`transitOneJobStep`, `runOneJobStep`)

The owner scheduler:

- Pulls runnable jobs from job table.
- Classifies jobs into categories (general vs reorg/backfill) and dispatches them into worker pools.

Workers run each job in *small, persistent steps*:

- Update meta in a transaction.
- Move job forward by changing schema state and/or job state.
- If schema version changes, update global schema version and wait followers to sync (`OwnerUpdateGlobalVersion` + `WaitVersionSynced`).
- For reorg/backfill, run reorg logic repeatedly, updating progress in the job record so it can resume.

## 6) Schema version sync: safety boundary for online DDL

Entrypoints:

- `pkg/ddl/job_worker.go:updateGlobalVersionAndWaitSynced`
- `pkg/ddl/schema_version.go:waitVersionSynced`
- `pkg/ddl/schemaver/syncer.go` (`Syncer.WaitVersionSynced`)

After each schema state change, the owner:

1. Updates the global schema version.
2. Waits until all relevant TiDB instances have reloaded to (≥) that version.

This mechanism is fundamental to online DDL correctness: it ensures that once the job advances to the next schema state, all nodes have a consistent view (or are at least caught up enough to respect compatibility).

## 7) Completion and unblocking the SQL session

On completion:

- The job is moved to history and removed from the active queue.
- A per-job “done channel” is closed/triggered so the submitter-side wait loop returns quickly.
- The SQL session returns success/error based on the history job record.

## “Where should I implement this change?”

Rule of thumb:

- **Statement mapping / job creation / waiting**: `pkg/ddl/executor.go`
- **Persistent step execution and state transitions**: `pkg/ddl/job_worker.go` + specific DDL action files (`table.go`, `schema.go`, `index.go`, …)
- **SQL session boundaries / generic dispatch**: `pkg/executor/ddl.go` (keep thin)

If the change modifies *schema state transitions*, *job steps*, or *meta writes*, it almost certainly belongs in `pkg/ddl/`.

