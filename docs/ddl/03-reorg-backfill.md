# Reorg / Backfill Overview (Add index, modify column, distributed backfill)

Many DDLs are not “metadata-only”. They require scanning existing data and writing new data structures (indexes, column rewrites, partition changes, …). TiDB calls this phase **reorganization** (“reorg” / backfill).

## What counts as “reorg”

Typical examples:

- Add index / add primary key (index backfill).
- Modify column that requires data rewrite.
- Some partition operations that move/transform data.

Deep dive:

- Add index: `docs/ddl/06-add-index.md`
- Modify column: `docs/ddl/07-modify-column.md`
- Partition DDL (reorg-heavy operations): `docs/ddl/08-partition-ddl.md`

These jobs usually enter the schema state `reorg`, where:

- the schema change is partially visible/compatible, and
- background workers perform data backfill while DML continues (with restrictions).

## Where reorg runs in the framework

High-level hooks:

- Worker step driving: `pkg/ddl/job_worker.go` (`runOneJobStep`, `transitOneJobStep`)
- Reorg context and utilities: `pkg/ddl/reorg.go`, `pkg/ddl/reorg_util.go`
- Worker pools: `pkg/ddl/job_scheduler.go` creates a dedicated reorg worker pool (see `reorgCnt` sizing logic).

The key requirement is **resumability**:

- Reorg must persist progress into the job record (row count / ranges / checkpoints).
- On retry/owner transfer, the worker continues from persisted progress.

## Distributed backfill (dist task)

TiDB supports distributing backfill work using the dist-task framework (especially for large backfills).

Anchors:

- Registration happens in `pkg/ddl/ddl.go:NewDDL` via `taskexecutor.RegisterTaskType(proto.Backfill, ...)` and scheduler factories.
- Design docs:
  - `docs/design/2022-09-19-distributed-ddl-reorg.md`
  - `docs/design/2023-04-11-dist-task.md`

When reading code, treat this as an *extension* of the same DDL job lifecycle:

- The DDL job still drives schema states and persists progress.
- The backfill “work” may be delegated to distributed executors via dist-task.

## Ingest / acceleration path (Lightning backend)

For some workloads, backfill can be accelerated using ingest/Lightning-based pipelines.

Anchors:

- Code: `pkg/ddl/ingest/*`
- Design doc: `docs/design/2022-06-07-adding-index-acceleration.md`

When touching ingest paths, be extra careful about:

- disk space and temp dir lifecycle,
- checkpointing and resume,
- switching between ingest and non-ingest modes based on config/cluster capability.

## Practical dev guidance for reorg changes

When you change reorg/backfill code, always verify:

- Progress is persisted (and is compatible across job version / restarts).
- Work can be safely retried (idempotent writes or safe dedup).
- Cancellation/pause/resume semantics are respected (don’t ignore job state transitions).
- Schema version sync is still correct at state boundaries.

If the change is large or introduces a new reorg mode, prefer adding a short design doc under `docs/design/` and link it from `docs/ddl/README.md`.
