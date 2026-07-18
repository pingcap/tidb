# DDL Job Lifecycle (state machine, versions, schema sync, owner failover)

This doc focuses on the persistent “job” as the unit of DDL execution.

## Job record and versions

DDL jobs are persisted as `model.Job` (`pkg/meta/model/job.go`).

- The job record is durable: it lives in system tables and survives owner transfer / restart.
- Job arguments have *versions*:
  - `JobVersion1`: legacy, args stored as an untyped array (pre-v8.4.0).
  - `JobVersion2`: typed args structs (from v8.4.0).
  - See `pkg/meta/model/job.go` (`type JobVersion`, `JobVersion1`, `JobVersion2`, `GetJobVerInUse`).

**Practical implication for developers:** when adding/changing job arguments, check whether the job type is already migrated to v2; ensure decoding/encoding is compatible and test both versions if needed.

## Job state vs schema state

There are two related but distinct “state” dimensions:

1. **Job state** (conceptually): whether the job is running/rolling back/done, etc.  
   Implemented by `model.JobState` and helper methods like `job.IsRunning()` / `job.IsRollingback()`.

2. **Schema state** (online DDL state machine): how visible the schema change is to SQL.  
   For many DDLs, the state sequence follows (simplified):

   - `none`
   - `delete only`
   - `write only`
   - `reorg` (reorganization / backfill)
   - `public`

The submitter-side wait loop assumes this simplified sequence (see the comment in `pkg/ddl/executor.go:DoDDLJobWrapper`).

**Why schema states exist:** to keep DML and queries safe while the schema is changing, by gradually changing allowed operations and ensuring all nodes see each transition before the next.

## Owner election and failover (why “job-based” matters)

- Only the **DDL owner** runs scheduler + workers (`pkg/ddl/job_scheduler.go:OnBecomeOwner`).
- Non-owners can still accept DDL statements: they create jobs and submit them.
- If the owner changes mid-job:
  - the new owner reloads runnable jobs from system tables,
  - continues from the persisted job state/progress.

**Developer implication:** each job step must be:

- idempotent (safe to retry),
- resumable (persist progress needed for the next step),
- safe under partial failures (e.g., crash after meta write but before schema sync).

## Schema version + diff and cluster-wide synchronization

After schema changes, TiDB relies on a global schema version + per-job synchronization to coordinate all nodes.

Key pieces:

- Global version update + wait: `pkg/ddl/job_worker.go:updateGlobalVersionAndWaitSynced`
- Waiting logic: `pkg/ddl/schema_version.go:waitVersionSynced`
- Sync mechanism: `pkg/ddl/schemaver/syncer.go` (`Syncer.WaitVersionSynced`)

At a high level:

1. Owner updates global schema version in etcd (`OwnerUpdateGlobalVersion`).
2. Followers watch global schema version, reload InfoSchema, and report their own versions (`UpdateSelfVersion`).
3. Owner waits until all relevant followers report version ≥ target (`WaitVersionSynced`).

When MDL is disabled, some paths fall back to lease-based waiting (see `waitVersionSyncedWithoutMDL` in `pkg/ddl/schema_version.go`).

## Job tables and storage access

DDL job persistence is backed by system tables under `mysql.*` (job queue, history, MDL info, …).

The storage access layer is abstracted by `pkg/ddl/systable/manager.go` (`type Manager`), used by scheduler/worker to read job and MDL information.

**Tip:** if you need to change how jobs are stored/loaded, start from `systable.Manager` and track call sites from scheduler/worker.

## Waiting and notifications back to SQL sessions

The submitting session waits in `pkg/ddl/executor.go:DoDDLJobWrapper` via:

- A per-job notification channel stored in `ddlJobDoneChMap` (wired in `pkg/ddl/ddl.go:NewDDL`).
- A ticker polling loop as a fallback.

This dual approach is important:

- The in-process channel makes owner-local jobs return quickly without waiting for the next polling tick.
- Polling is required for cross-node execution (submitter is not owner), and also covers cases where in-process notifications can’t be delivered (e.g. owner transfer).

## Common lifecycle pitfalls (checklist)

- Job args not persisted / not decodable after restart → owner failover breaks the DDL.
- Meta change committed but global schema version not updated → followers don’t reload; SQL sees inconsistent schema.
- Schema version updated but `WaitVersionSynced` skipped incorrectly → nodes observe incompatible states.
- Reorg progress not persisted → reorg restarts from scratch after retry.
