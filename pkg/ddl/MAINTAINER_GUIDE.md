# DDL Maintainer Guide

This doc records non-obvious design choices, invariants, and maintainer workflows for TiDB DDL.

It intentionally:
- prioritizes relatively complex logic and explains why it is designed this way
- skips content that is easiest to learn by reading the code
- stays concise and action-oriented for maintainers/reviewers/oncall

Status: describes current behavior as implemented in `pkg/ddl/` and its boundary modules
(`pkg/executor/ddl.go`, `pkg/meta/model/job.go`, `pkg/domain/domain.go`, `pkg/metrics/ddl.go`).
If you change a state machine, persistent encoding, or recovery behavior, update this doc in the same patch.

## Contents
1. Scope And Non-Negotiable Invariants
2. Responsibility Boundaries (Where Changes Belong)
3. Durable State Model (Jobs, System Tables, Compatibility)
4. Execution Skeleton (Submit -> Schedule -> One Step -> Sync -> Done)
5. Schema State Machine And Version Sync (Why "Two Versions" Exist)
6. Reorg / Backfill (Long-Running DDL) Invariants
7. Operation-Specific Sharp Edges (Add Index, Modify Column, Partition DDL)
8. Concurrency, Ordering, Idempotency, Cancellation
9. Observability, Debugging, And Common Failure Modes
10. Maintainer Playbooks (Common Change Types)
11. Glossary

## 1. Scope And Non-Negotiable Invariants

This guide covers the job-based DDL framework and its safety boundaries:
- SQL entrypoint and statement boundary handling: `pkg/executor/ddl.go` (`DDLExec`)
- Statement -> job(s) -> wait: `pkg/ddl/executor.go` (`Executor`, `DoDDLJobWrapper`)
- Job submission and system table persistence: `pkg/ddl/job_submitter.go`, `pkg/ddl/systable/*`
- Owner scheduler + worker pools: `pkg/ddl/job_scheduler.go`, `pkg/ddl/ddl_workerpool.go`
- Worker job-step execution and schema version sync: `pkg/ddl/job_worker.go`, `pkg/ddl/schema_version.go`, `pkg/ddl/schemaver/*`
- Job model + argument encoding: `pkg/meta/model/job.go` and `pkg/meta/model/job_args.go`

It intentionally skips:
- SQL syntax / parser AST details (see `pkg/parser/*`)
- planner mapping details (see `pkg/planner/*`)
- InfoSchema internals beyond the schema-version sync boundary

Top invariants a DDL maintainer must not break:
1. DDL MUST be job-based and resumable (owner transfer / restart safe).
2. Only the DDL owner runs scheduler + workers; non-owners only submit + wait.
3. Schema state transitions MUST be fenced by: commit meta -> update schema version -> wait follower sync.
4. Job steps MUST be idempotent and MUST persist enough progress to retry/resume.
5. Persistent metadata (job args, system tables, reorg checkpoints) MUST remain compatible across rolling upgrades.

Maintainer preflight (write these down when making a non-trivial DDL change):
- Is it job-based (persist/resume/failover) or a metadata-only fast path?
- Which schema state transitions exist (if any), and where are the sync boundaries?
- Does it require reorg/backfill? What is the persisted checkpoint?
- What are cancel/rollback semantics (which steps are reversible vs GC/delete-range cleanup)?
- Which durable tables/fields are touched (`mysql.tidb_ddl_job`, `mysql.tidb_ddl_reorg`, ...), and what is the upgrade story?

## 2. Responsibility Boundaries (Where Changes Belong)

DDL is job-based and owner-driven:
- A SQL DDL statement becomes a durable `model.Job`.
- Jobs are persisted into system tables under `mysql.*`.
- Exactly one TiDB node is elected as the DDL owner; only the owner schedules and executes job steps.
- Workers run jobs step-by-step, updating metadata and synchronizing schema version across the cluster.

Keep these boundaries sharp:
- `pkg/executor/ddl.go` MUST stay thin: generic dispatch + statement/txn boundary management.
- `pkg/ddl/executor.go` owns statement -> job translation and the submitter-side wait loop.
- `pkg/ddl/job_submitter.go` owns batching, ID allocation, and inserting job records into `mysql.tidb_ddl_job`.
- `pkg/ddl/job_scheduler.go` owns owner-side job selection/routing; `pkg/ddl/job_worker.go` owns durable execution.
- `pkg/ddl/schema_version.go` and `pkg/ddl/schemaver/*` are the online DDL safety boundary: schema version update + wait.

Rule of thumb:
- If logic must survive restart/owner transfer, it belongs in the job execution path (worker + action handlers), not in `pkg/executor/ddl.go`.

"Where should I implement this change?" (quick map):
| Change type | Correct place |
|---|---|
| SQL syntax / AST | `pkg/parser/*` |
| planner mapping | `pkg/planner/*` |
| statement boundary + generic AST dispatch | `pkg/executor/ddl.go` |
| statement -> job args / submission / wait | `pkg/ddl/executor.go` |
| durable step execution, state transitions, meta writes | `pkg/ddl/job_worker.go` + action handlers (e.g. `pkg/ddl/table.go`, `pkg/ddl/index.go`, `pkg/ddl/modify_column.go`, `pkg/ddl/partition.go`) |
| job args encoding/decoding and compatibility | `pkg/meta/model/job.go`, `pkg/meta/model/job_args.go` |
| schema sync and versioning mechanisms | `pkg/ddl/schema_version.go`, `pkg/ddl/schemaver/*` |
| job/MDL system table access | `pkg/ddl/systable/*` |

## 3. Durable State Model (Jobs, System Tables, Compatibility)

### 3.1 Jobs Are Durable Execution Units

- A DDL statement MUST be represented as a durable `model.Job` (`pkg/meta/model/job.go`).
- Any step that affects correctness MUST be derivable from persisted state (job meta + args + reorg meta/checkpoints),
  because the owner can change at any time.

Why it matters:
- Owner transfer / crash is a normal path, not an edge case.
- "Works on my node" DDL bugs almost always come from non-durable state.

### 3.2 Job Encoding And Argument Versions

Jobs have an explicit job-arg versioning mechanism (see `pkg/meta/model/job.go`):
- `JobVersion1`: legacy, args stored as an untyped array.
- `JobVersion2`: typed args structs (newer actions migrate here).

Rules for maintainers:
- When adding/changing job args, identify the job version in use and keep decode/encode backward compatible.
- New fields MUST have safe defaults for old decoders (mixed-version upgrade window is part of correctness).
- Avoid changing the meaning of existing fields without a version gate.

### 3.3 System Tables Are A Durable API Surface

DDL persistence relies on `mysql.*` system tables (definitions live in `pkg/meta/metadef/system_tables_def.go`):
- `mysql.tidb_ddl_job`: active job queue
- `mysql.tidb_ddl_history`: history jobs (submitter-side completion source-of-truth)
- `mysql.tidb_ddl_reorg`: reorg/backfill checkpoints (range + element + reorg meta)
- `mysql.tidb_mdl_info`: metadata lock (MDL) information for schema sync gating

Maintainer rule:
- Treat these as a durable API surface. Any schema/encoding change requires explicit compatibility reasoning and tests.

Implementation note:
- Scheduler/worker access these tables through `pkg/ddl/systable/manager.go` (`type Manager`).

## 4. Execution Skeleton (Submit -> Schedule -> One Step -> Sync -> Done)

This section focuses on ordering constraints that are easy to break.

### 4.1 Submitter Side: Statement -> Job(s) -> Wait

Flow (high level):
1. `pkg/executor/ddl.go` (`DDLExec`) ends the previous txn and dispatches the DDL AST into the DDL module.
2. `pkg/ddl/executor.go` validates/normalizes statement details, builds a durable `model.Job` (or multiple jobs),
   and submits it via `JobSubmitter` (bounded channel + batching).
3. `pkg/ddl/job_submitter.go` allocates IDs if needed, writes job rows into `mysql.tidb_ddl_job`,
   and notifies the owner scheduler (`notifyNewJobSubmitted`).
4. The submitting session blocks in `DoDDLJobWrapper` until completion.

Non-obvious design choice: "channel + polling" wait loop.
- The submitter uses an in-process done channel as a best-effort fast path (returns quickly when the job completes locally).
- It MUST still poll persisted state (history) as the correctness path:
  - cross-node execution requires polling (submitter is not owner)
  - owner transfer can drop in-process notifications
- `DoDDLJobWrapper` also wires `StmtCtx.DDLJobID` so `KILL` can cancel the job; keep cancellation behavior correct under polling.

Operational footnote:
- When `tidb_enable_fast_create_table` is enabled, the submitter may merge multiple create-table jobs into one batch-create-table job.
  This changes job granularity but MUST NOT change durability/sync invariants.

### 4.2 Owner Side: Election -> Scheduler -> Worker Pools

- DDL owner election is etcd-backed (see owner callbacks in `pkg/ddl/job_scheduler.go`).
- Only the owner runs the scheduler and worker pools. Non-owners still accept DDL statements (submit + wait).

Why it matters:
- Correctness depends on a single scheduler driving state transitions and schema version sync.

### 4.3 One Persistent Step Per Transaction

The owner runs a "step machine":
- Scheduler picks a runnable job from system tables.
- A worker runs exactly one step inside a transaction (`pkg/ddl/job_worker.go:transitOneJobStep`):
  - validate job bytes (detect concurrent cancel/pause changes)
  - run step logic (`runOneJobStep`)
  - persist job changes (including args when needed) and MDL info
  - commit
- After commit, scheduler updates global schema version and waits for follower sync
  (`pkg/ddl/job_scheduler.go:transitOneJobStepAndWaitSync`).

Key ordering rule:
- Meta/job changes MUST be committed before schema version is announced to followers.

Why it matters:
- Followers reload InfoSchema based on schema version; announcing too early makes them observe incomplete metadata.

### 4.4 Reorg Routing (Why Some Jobs Go To The Reorg Pool)

- The submitter marks jobs as "may need reorg" when inserting into `mysql.tidb_ddl_job` (see `MayNeedReorg` usage in
  `pkg/ddl/job_submitter.go` and the `reorg` routing flag in the system table).
- The scheduler uses this flag to route jobs into a dedicated reorg worker pool.

Why it matters:
- It prevents long-running backfills from starving metadata-only DDL steps and bounds concurrency for scan/write workloads.

## 5. Schema State Machine And Version Sync (Why "Two Versions" Exist)

### 5.1 Job State vs Schema State

Maintain the distinction:
- Job state (`model.JobState`): running / rolling back / done / cancelled.
- Schema state (`model.SchemaState` on job and schema objects): compatibility window visible to SQL
  (commonly `none` -> `delete only` -> `write only` -> `reorg` -> `public`).

The submitter-side wait loop (`pkg/ddl/executor.go:DoDDLJobWrapper`) assumes a simplified state progression for many jobs.
Mixing job state and schema state tends to produce "job finished" and "online DDL compatibility" bugs.

### 5.2 The Two-Version Safety Invariant

Invariant:
- The owner MUST NOT advance a DDL object across multiple schema states without synchronizing followers in between.

Mechanism:
- After each state-changing step, the owner commits the step transaction, then:
  - updates the global schema version, and
  - waits for followers to reach that version (or a compatible one)

Why it exists:
- Different TiDB nodes observe different schema versions during rollout.
- Requiring "adjacent states only" prevents incompatible combinations from coexisting (wrong results, DML failures, corruption).

### 5.3 MDL vs Lease-Based Waiting

`WaitVersionSynced` is not optional:
- With MDL enabled, MDL info participates in gating and wait logic (`mysql.tidb_mdl_info`).
- When MDL is disabled, the system falls back to lease-based waiting (`waitVersionSyncedWithoutMDL` in `pkg/ddl/schema_version.go`).

Maintainer rule:
- Any schema-sync change MUST consider both MDL and non-MDL paths, and MUST preserve the commit-then-announce ordering.

## 6. Reorg / Backfill (Long-Running DDL) Invariants

Reorg/backfill is the "data change" phase for many DDLs (add index, some modify column, partition reorganize/drop/truncate, ...).

### 6.1 Resumability: Progress Must Be Persisted

- Reorg MUST persist progress to durable state so retry/owner transfer resumes from checkpoint.
- The common checkpoint store is `mysql.tidb_ddl_reorg` plus job meta/reorg meta persisted in `mysql.tidb_ddl_job`.

If you change what is stored for progress:
- ensure it's written transactionally with the step's meta updates
- ensure it is compatible across rolling upgrades
- ensure it is sufficient to continue without scanning from zero

### 6.2 Reorg Type Selection Must Be Stable Once Chosen

Some reorgs pick a backfill implementation and persist it in `job.ReorgMeta.ReorgTp`:
- `ReorgTypeTxn`: traditional transactional backfill
- `ReorgTypeTxnMerge`: fast reorg pipeline (no Lightning), uses a temp index + merge
- `ReorgTypeIngest`: Lightning/ingest accelerated pipeline

Maintainer rule:
- Once a job persists its reorg type, retries/resumes MUST continue with the same type (switching mid-flight breaks idempotency).

### 6.3 Distributed Backfill (Dist-Task) Is An Extension, Not A Replacement

When dist-task is enabled:
- the DDL job still drives schema states, persistence, and owner transfer semantics
- the backfill work may be delegated to distributed executors via the dist-task framework

Anchors:
- task type registration around `proto.Backfill` in `pkg/ddl/ddl.go`
- design docs: `docs/design/2022-09-19-distributed-ddl-reorg.md`, `docs/design/2023-04-11-dist-task.md`

### 6.4 Ingest / Lightning Acceleration Has Extra Sharp Edges

Anchors:
- code: `pkg/ddl/ingest/*`
- design doc: `docs/design/2022-06-07-adding-index-acceleration.md`

When touching ingest paths, be explicit about:
- disk space and temp-dir lifecycle
- checkpointing and resume
- fallback rules (ingest -> non-ingest) and their persistence implications

## 7. Operation-Specific Sharp Edges (Add Index, Modify Column, Partition DDL)

This section lists invariants that have historically produced correctness bugs when misunderstood.

### 7.1 Add Index / Add Primary Key

Add index is a reorg-heavy online DDL (see `pkg/ddl/index.go:onCreateIndex`):
- Index state transitions are part of the online contract:
  - `StateNone` -> `StateDeleteOnly` -> `StateWriteOnly` -> `StateWriteReorganization` -> `StatePublic`
- Each boundary MUST be followed by schema version sync (two-version invariant).

Other sharp edges (review hotspots):
- Expression/generated-key indexes may introduce hidden columns; these must be published early enough to keep multi-version reads/writes correct.
- Partial indexes are only supported with fast reorg; do not relax this without reasoning about the backfill type and online write rules.
- Optional index `SPLIT` options pre-split index regions; keep it idempotent and ordered so retries do not double-apply side effects.

Fast reorg pipelines use a persisted backfill-merge state machine on the index:
- `IndexInfo.BackfillState` (`pkg/meta/model/reorg.go`) is persisted and MUST be monotonic:
  - `BackfillStateRunning` -> `BackfillStateReadyToMerge` -> `BackfillStateMerging` -> `BackfillStateInapplicable`
- Why it exists: online writes continue while backfill runs, and schema propagation is multi-version across nodes.
  "ReadyToMerge" MUST be published and synced before merge begins so all nodes apply the correct double-write semantics.

Reorg meta and feature gates:
- `job.ReorgMeta` is initialized from session/global variables.
- `tidb_enable_dist_task` requires fast reorg; otherwise the job rejects (dist-task assumes the fast-reorg pipeline).

Reorg stage is not only backfill:
- add index may run an optional `ANALYZE` phase after backfill; the job does not become `public` until analyze is done/skipped/timeout/failed.
  The analyze progress is persisted (see `AnalyzeState` usage in `pkg/ddl/index.go`).

Persistence invariants:
- Backfill progress and reorg handles live in `mysql.tidb_ddl_reorg` and job meta; losing them causes restart-from-zero or double-writes.

### 7.2 Modify Column (Modify / Change / Rename Column)

Modify column is tricky because it may be metadata-only, validation-only, index-only reorg, or full row+index reorg.
The behavior is driven by a persisted "modify type" (see `pkg/ddl/modify_column.go:getModifyColumnType`).

Key invariants:
- `noReorgDataStrict(...)` is the strong predicate for "never needs reorg"; do not weaken it casually.
- `NULL -> NOT NULL` often uses "no-reorg with check": validate existing rows before publishing NOT NULL.
  The check is implemented as a restricted SQL query (built by `buildCheckSQLFromModifyColumn`, executed with `LIMIT 1`).
- Partitioned tables and TiFlash replica paths are conservatively forced into full reorg in some cases; do not assume the fast path applies.
- `VARCHAR -> CHAR` has a special precheck path that may downgrade from reorg to no-reorg depending on data.

Index-only reorg:
- Used when row data stays but index keys/values must be rewritten.
- It creates temporary "changing indexes" and reuses the add-index reorg engine; it still uses the schema state machine
  (`none` -> `delete only` -> `write only` -> `reorg` -> `public`).

Full row+index reorg:
- Uses a persisted stage machine in `job.ReorgMeta.Stage`:
  - update column (row rewrite) -> recreate index -> completed
- It introduces a hidden "changing column" and temporary indexes; job args MUST persist the new identifiers (column/index IDs)
  before doing irreversible work so the job can resume after retry/owner transfer.

Maintainer tip:
- If you touch rollback logic, confirm behavior for all modify types (no-reorg / index reorg / full reorg), not just the happy path.

Useful failpoints and tests (when you need deterministic coverage):
- Failpoints:
  - `github.com/pingcap/tidb/pkg/ddl/afterModifyColumnStateDeleteOnly`
  - `github.com/pingcap/tidb/pkg/ddl/afterReorgWorkForModifyColumn`
  - `github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData`
  - `github.com/pingcap/tidb/pkg/ddl/getModifyColumnType`
- Tests:
  - `pkg/ddl/modify_column_test.go`
  - `pkg/ddl/column_modify_test.go`
  - `pkg/ddl/column_change_test.go`

### 7.3 Partition DDL (Add / Drop / Truncate / Reorganize / Exchange)

Partition DDL correctness depends heavily on multi-version semantics: different TiDB nodes run with different schema versions.
To stay correct, partition DDL persists intermediate metadata on the table/partition info, not "state per partition":
- `tblInfo.Partition.DDLState` / `DDLAction`
- `tblInfo.Partition.AddingDefinitions`
- `tblInfo.Partition.DroppingDefinitions`
- reorganize-partition mapping: `tblInfo.Partition.DDLChangedIndex`

These fields are part of the correctness contract:
- they drive "double write / filter reads" behaviors under mixed schema versions
- they MUST NOT be cleared too early (or followers will apply the wrong semantics)

Schema diff note:
- Partition operations use specialized schema diff helpers in `pkg/ddl/schema_version.go` for apply-diff paths; keep schema diff updates
  consistent with the intermediate partition metadata above.

ADD PARTITION:
- state machine is intentionally short: `StateNone` -> `StateReplicaOnly` -> Done.
- In `StateNone`, new definitions are moved into `AddingDefinitions` and `DDLState` is set before writes start.
- `StateReplicaOnly` may wait for TiFlash replicas for new partitions (`checkPartitionReplica`) and related PD side effects.

DROP/TRUNCATE PARTITION:
- state machine includes an extra cleanup stage: `StatePublic` -> `StateWriteOnly` -> `StateDeleteOnly` -> `StateDeleteReorganization` -> Done.
- Why: if the table has global indexes, entries that still point to dropped partitions must be cleaned up in the delete-reorg stage.
  Skipping this leaves orphan global-index entries.
- Job completion usually relies on delete-range GC to clean old partition data later; finished args MUST include old physical IDs.

REORGANIZE PARTITION:
- copies data into a new set of partitions and may recreate indexes.
- it forces transactional reorg (`ReorgTypeTxn`) in the partition reorg worker; do not assume fast reorg/ingest behavior.
- it uses a `StateDeleteReorganization` stage to switch reads to new definitions while keeping double-write for one more version.
- `ActionAlterTablePartitioning` / `ActionRemovePartitioning` reuse this pipeline and may drop+create the table with a new table ID in the `StatePublic` branch.

EXCHANGE PARTITION:
- uses an interim `StateWriteOnly` schema version to make the non-partitioned table enforce the partition constraint window.
- persists `ExchangePartitionInfo` before the swap; optional `WITH VALIDATION` runs an explicit validation path.

Placement rules / label rules / TiFlash:
- Partition DDL often triggers PD side effects (placement bundles, label rules, TiFlash placement/config).
- These SHOULD happen before the schema state that starts writing to the new partitions so retries/rollbacks remain safe and idempotent.

Good starting tests:
- `pkg/ddl/partition_test.go`
- `pkg/ddl/notifier/testkit_test.go`

## 8. Concurrency, Ordering, Idempotency, Cancellation

### 8.1 Concurrency Model

- Only one owner schedules jobs, but multiple workers can execute steps concurrently (general DDL vs reorg pool, etc).
- The worker step transaction is optimistic and checks persisted job bytes to detect concurrent modifications
  (cancel/pause requests) before committing.

### 8.2 Idempotency Checklist (Review Gate)

When reviewing a change to job step logic, explicitly check:
- Is the step safe to run twice (same input) without corrupting metadata?
- Is there a persisted checkpoint for any long-running scan/backfill work?
- If the step can be partially applied, can the retry detect and finish safely?
- Can the step be interrupted and resumed without leaking goroutines/resources?
- Is rollback/cancel behavior correct for partially applied changes?

### 8.3 Ordering Checklist Around Schema Sync

For any step that changes schema state/version:
- Do we return a non-zero schema version from the step so the scheduler waits?
- Do we preserve the two-version invariant across retries/owner transfer (unsynced version handling)?
- Are MDL and non-MDL (lease-based) paths both safe?
- Are we committing meta/job updates before announcing the new schema version?

## 9. Observability, Debugging, And Common Failure Modes

### 9.1 SQL-Level Tools (First-Line Triage)

- `ADMIN SHOW DDL JOBS`
- `ADMIN SHOW DDL JOB QUERIES`
- `ADMIN CANCEL DDL JOBS <job_id>`

System tables (when you need ground truth):
- `mysql.tidb_ddl_job` (queue)
- `mysql.tidb_ddl_history` (final result)
- `mysql.tidb_ddl_reorg` (reorg checkpoints)
- `mysql.tidb_mdl_info` (MDL gating state)

### 9.2 Logs And Metrics

Logs:
- DDL logs usually use `logutil.DDLLogger()` (search under `pkg/ddl/*`).
- Useful fields to grep for: job ID, schema version, schema state, owner transitions, reorg progress.

Metrics (`pkg/metrics/ddl.go`):
- `tidb_ddl_waiting_jobs` (gauge): queued jobs
- `tidb_ddl_running_job_count` (gauge): running jobs by pool type
- `tidb_ddl_worker_operation_duration_seconds` (histogram): step/sync timing (`run_one_step`, `wait_schema_synced`, ...)
- `tidb_ddl_retryable_error_total` (counter): retryable errors (often indicates instability or flaky dependencies)

### 9.3 Common Failure Modes (Symptom -> Likely Cause -> First Look)

- Job stuck in running:
  - likely cause: schema version sync wait; owner changed mid-job; reorg/backfill blocked/slow.
  - first look: owner logs (`pkg/ddl/job_scheduler.go`), `waitVersionSynced` logs/metrics, `mysql.tidb_mdl_info`.

- Job finishes on owner but submitter session keeps waiting:
  - likely cause: submitter is on a different node and relies on polling; history write missing/delayed; owner transfer dropped notifications.
  - first look: `mysql.tidb_ddl_history` row for the job ID; submitter wait loop (`pkg/ddl/executor.go:DoDDLJobWrapper`).

- Schema version sync takes very long:
  - likely cause: a follower is slow/unhealthy; etcd issues; MDL/lease mode mismatch.
  - first look: `pkg/ddl/schemaver/syncer.go`, `pkg/ddl/schema_version.go`, and related metrics.

- Reorg restarts from 0 after retry:
  - likely cause: checkpoint/progress not persisted correctly (or persisted incompatibly).
  - first look: `mysql.tidb_ddl_reorg`, job reorg meta fields, `pkg/ddl/reorg*.go`.

- Cancel/pause does not take effect:
  - likely cause: step ignores context/job state; long loop without checking; cancellation polling missing for that code path.
  - first look: `pkg/ddl/job_worker.go` (job polling + step lifecycle) and the reorg loop.

## 10. Maintainer Playbooks (Common Change Types)

### 10.1 Add A New DDL Action

Checklist:
1. Confirm the correct layer (parser/planner/executor/DDL module). Do not "fix it in `pkg/executor/`".
2. Implement statement -> job args in `pkg/ddl/executor.go` (prefer v2 typed args when supported by the action).
3. Implement durable step execution in `pkg/ddl/job_worker.go` + action handler(s).
4. Preserve commit-then-announce ordering and schema sync boundaries.
5. Define cancel/rollback semantics (including GC/delete-range when needed).
6. Add a targeted unit test and an integration test if behavior is user-visible.

### 10.2 Change Job Arguments / Persistent Encoding

Checklist:
- Identify job version (v1 vs v2 typed args) and keep decoding backward compatible.
- New fields must be defaultable (old jobs) and ignorable (new jobs on old nodes) in the mixed-version window.
- If you touch system tables (`mysql.tidb_ddl_*`, `mysql.tidb_mdl_info`), treat it as a compatibility change with explicit tests.

### 10.3 Change Schema State Machine / Schema Sync Rules

Checklist:
- Update reasoning in terms of schema state (online compatibility), not only job state.
- Preserve the two-version invariant (sync before advancing).
- Validate both MDL and non-MDL paths in `pkg/ddl/schema_version.go`.

### 10.4 Change Reorg/Backfill Logic

Checklist:
- Persist progress/checkpoints; prove resumability on retry/owner transfer.
- Confirm idempotency (including temp artifacts like temp indexes/changing columns).
- Verify cancellation/pause/resume paths do not leak resources.
- Watch `tidb_ddl_worker_operation_duration_seconds` (`wait_schema_synced`, `run_one_step`) for regressions.

### 10.5 Tests And Determinism (Workflow Notes)

- Prefer targeted package unit tests under `pkg/ddl/*_test.go`.
- Use integration tests (`tests/integrationtest`) when results are user-visible or cross-module.
- When debugging state-machine timing, look for existing failpoints in `pkg/ddl/` before adding new ones.

For exact commands, follow the repository root `AGENTS.md` testing sections (failpoint enable/disable + tag selection).

## 11. Glossary

- DDL owner: the elected TiDB node that runs the scheduler + workers (`pkg/ddl/job_scheduler.go`).
- Job: durable DDL execution record (`model.Job`), persisted in `mysql.tidb_ddl_job`.
- Job state: running/rolling back/done/etc (`model.JobState`).
- Schema state: online DDL visibility state (`delete only`, `write only`, `reorg`, `public`, ...).
- Schema version: the global version used to fence InfoSchema reloads and online DDL transitions.
- Reorg/backfill: data rewrite phase (scan existing data, write new structures).
- MDL: metadata lock mechanism used to gate schema sync (see `mysql.tidb_mdl_info` and `pkg/ddl/schema_version.go`).
- Dist-task: distributed backfill framework used by some DDL reorg modes.
- Ingest: Lightning-based acceleration path for backfill (`pkg/ddl/ingest/*`).
