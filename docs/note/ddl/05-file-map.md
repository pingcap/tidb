# DDL Codebase File Map (quick “where is it implemented?”)

This is a pragmatic map from common DDL areas to `pkg/ddl/` files. It’s intentionally non-exhaustive, but aims to cover the most frequently touched parts.

## Core framework (read these first)

- DDL wiring & lifecycle: `pkg/ddl/ddl.go`
- Statement → job(s) → wait: `pkg/ddl/executor.go`
- Job batching/submission: `pkg/ddl/job_submitter.go`
- Owner scheduler + worker pools: `pkg/ddl/job_scheduler.go`, `pkg/ddl/ddl_workerpool.go`
- Worker job-step execution: `pkg/ddl/job_worker.go`
- Schema version sync (owner↔followers): `pkg/ddl/schema_version.go`, `pkg/ddl/schemaver/*`
- System table access (jobs/MDL): `pkg/ddl/systable/*`

## Common DDL domains

| Topic / statement family | Where to look |
|---|---|
| Create/Drop/Alter database | `pkg/ddl/schema.go` |
| Create/Drop/Truncate/Rename table | `pkg/ddl/table.go` |
| Add/Drop index | `pkg/ddl/index.go`, reorg helpers in `pkg/ddl/reorg*.go` |
| Modify column | `pkg/ddl/modify_column.go` |
| Partition DDL | `pkg/ddl/partition.go` |
| Multi-schema change (`ALTER TABLE ...` multi spec) | `pkg/ddl/multi_schema_change.go` |
| Placement policies | `pkg/ddl/placement_policy.go`, `pkg/ddl/placement/*` |
| Resource group DDL | `pkg/ddl/resource_group.go`, `pkg/ddl/resourcegroup/*` |
| Sequence | `pkg/ddl/sequence.go` |
| Table locks | `pkg/ddl/table_lock.go` |
| Table mode (e.g. cache) | `pkg/ddl/table_mode.go` |
| TTL | `pkg/ddl/ttl.go` |
| Split region for table/index | `pkg/ddl/split_region.go`, `pkg/ddl/table_split_test.go` |
| Repair table | `pkg/ddl/repair_table*.go` |
| Rolling back / error handling helpers | `pkg/ddl/rollingback*.go` |

## Reorg / backfill & acceleration

- Reorg framework: `pkg/ddl/reorg.go`, `pkg/ddl/reorg_util.go`
- Distributed backfill hooks: look for `proto.Backfill` registration in `pkg/ddl/ddl.go`
- Ingest / Lightning acceleration: `pkg/ddl/ingest/*`

## Events / notifications

- Schema change event publish/subscribe: `pkg/ddl/notifier/*`

## Deep dives

- Add index: `docs/note/ddl/06-add-index.md`
