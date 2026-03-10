# Partition DDL Deep Dive (add/drop/truncate/reorganize/exchange)

This doc focuses on TiDB **partition DDL** job types:

- `model.ActionAddTablePartition`
- `model.ActionDropTablePartition`
- `model.ActionTruncateTablePartition`
- `model.ActionReorganizePartition`
- `model.ActionExchangeTablePartition`
- Partitioning changes that reuse reorganize-partition pipeline: `model.ActionAlterTablePartitioning`, `model.ActionRemovePartitioning`

Partition DDL is subtle because correctness depends on **multi-version semantics** (different TiDB nodes observe different schema versions), plus **global indexes**, **placement rules**, and (optionally) **TiFlash replicas**.

## Entry points (start here)

SQL → DDL module:

- SQL dispatch: `pkg/executor/ddl.go` (`type DDLExec`, `(*DDLExec).Next`)
- DDL executor for partition ops: `pkg/ddl/executor.go` (search `Action*Partition` job creation)

Job execution (owner worker):

- Worker dispatch: `pkg/ddl/job_worker.go` (`case model.ActionAddTablePartition`, `...Drop...`, `...Truncate...`, `...Reorganize...`, `...Exchange...`)
- Handlers live in: `pkg/ddl/partition.go`

Schema diff helpers (used by schema sync/apply diff):

- `pkg/ddl/schema_version.go:SetSchemaDiffForExchangeTablePartition`
- `pkg/ddl/schema_version.go:SetSchemaDiffForTruncateTablePartition`
- `pkg/ddl/schema_version.go:SetSchemaDiffForDropTablePartition`
- `pkg/ddl/schema_version.go:SetSchemaDiffForReorganizePartition`

## Shared mental model: intermediate partition metadata

Partition DDL usually does **not** store “state per partition definition”. Instead, it persists intermediate state via:

- `job.SchemaState` (drives the worker state machine)
- `tblInfo.Partition.DDLState` / `tblInfo.Partition.DDLAction` (persisted so other components can interpret current behavior)
- `tblInfo.Partition.AddingDefinitions` (new partitions being introduced)
- `tblInfo.Partition.DroppingDefinitions` (partitions being removed)
- For reorganize partition: `tblInfo.Partition.DDLChangedIndex` (old/new index mapping)

Many correctness rules are implemented as “double write / filter reads” behaviors keyed off these fields (see comments inside `pkg/ddl/partition.go`).

## ADD PARTITION (`ActionAddTablePartition`)

- Handler: `pkg/ddl/partition.go:onAddTablePartition`
- Job args: `model.TablePartitionArgs` (`pkg/meta/model/job_args.go`)

State machine (`job.SchemaState`):

- `StateNone` → `StateReplicaOnly` → Done (table becomes `StatePublic`)

Key steps:

- In `StateNone`, TiDB:
  - Validates partition definition (count/name/value constraints)
  - Moves new defs into `tblInfo.Partition.AddingDefinitions` (`updateAddingPartitionInfo`)
  - Persists `Partition.DDLState = StateReplicaOnly` and placement/label updates before writes start
- In `StateReplicaOnly`, TiDB may block until TiFlash replicas for new partitions are ready:
  - Replica check loop: `pkg/ddl/partition.go:checkPartitionReplica`
  - PD/TiFlash configuration: `infosync.ConfigureTiFlashPDForPartitions`
- Finalize:
  - Merge `AddingDefinitions` into `Definitions` (`updatePartitionInfo`)
  - Optional `preSplitAndScatter`
  - Clear `Partition.DDLState`/`DDLAction` and finish the job

## DROP PARTITION (`ActionDropTablePartition`)

- Handler: `pkg/ddl/partition.go:onDropTablePartition`
- Job args: `model.TablePartitionArgs`

State machine (`job.SchemaState`):

- `StatePublic` → `StateWriteOnly` → `StateDeleteOnly` → `StateDeleteReorganization` → Done (`StateNone`)

Why so many states:

- Drop partition must preserve correctness under:
  - sessions that still see the old partitioning scheme,
  - sessions that see the new partitioning scheme,
  - global indexes that may still point to dropped partitions.

Global-index cleanup (only when the table has global indexes):

- Cleaner: `pkg/ddl/partition.go:cleanGlobalIndexEntriesFromDroppedPartitions`
- Reorg info builder: `pkg/ddl/partition.go:getReorgInfoFromPartitions`
- Runs through shared reorg engine: `pkg/ddl/reorg.go` / `pkg/ddl/column.go:updatePhysicalTableRow` (partition worker type differs by job)

Finalization:

- Clears `DroppingDefinitions`, sets `args.OldPhysicalTblIDs` (for schema diff / delete-range)
- Emits notifier event: `notifier.NewDropPartitionEvent` (via `asyncNotifyEvent`)
- Job finishes and delete-range GC will clean old partitions later: `pkg/ddl/delete_range.go` (`ActionDropTablePartition` cases)

## TRUNCATE PARTITION (`ActionTruncateTablePartition`)

- Handler: `pkg/ddl/partition.go:onTruncateTablePartition`
- Job args: `model.TruncateTableArgs` (uses `OldPartitionIDs`/`NewPartitionIDs`)

State machine (`job.SchemaState`):

- `StatePublic` → `StateWriteOnly` → `StateDeleteOnly` → `StateDeleteReorganization` → Done (`StateNone`)

Key behaviors:

- It replaces old partition IDs with new ones in table metadata:
  - ID replacement helper: `pkg/ddl/partition.go:replaceTruncatePartitions`
- During intermediate states, it sets flags to filter global index reads/writes correctly:
  - `pi.NewPartitionIDs` and `pi.DroppingDefinitions` are used by global-index filtering logic (see comments near `onTruncateTablePartition`)
- If global indexes exist, it reuses the same cleanup as drop-partition:
  - `cleanGlobalIndexEntriesFromDroppedPartitions`
- Job finishes and delete-range GC cleans old partition data:
  - `pkg/ddl/delete_range.go` (`ActionTruncateTablePartition` cases)

## REORGANIZE PARTITION (`ActionReorganizePartition`)

This is the “heavy” partition DDL: it copies data into a new set of partitions and may recreate (global/unique) indexes.

- Handler: `pkg/ddl/partition.go:onReorganizePartition`
- Reorg worker: `pkg/ddl/partition.go:doPartitionReorgWork` (forces `job.ReorgMeta.ReorgTp = ReorgTypeTxn`)

State machine (`job.SchemaState`):

- `StateNone`
  - Create `AddingDefinitions` + `DroppingDefinitions`
  - Create replacement indexes when required (`Partition.DDLChangedIndex`)
- `StateDeleteOnly` → `StateWriteOnly` → `StateWriteReorganization`
  - Ensure all nodes are in the correct “double write” window
- `StateWriteReorganization`
  - Data copy + index creation: `doPartitionReorgWork` → `pkg/ddl/partition.go:doPartitionReorgWork`
- `StateDeleteReorganization`
  - Switch reads to new definitions but keep double-write for one more schema version
- `StatePublic`
  - Mark replaced indexes `StateDeleteOnly` to avoid orphan inserts racing with delete-range GC
- Done (`StateNone`)
  - Remove old partitions and old indexes from `TableInfo`
  - Populate finished args (`OldPhysicalTblIDs`, `OldGlobalIndexes`, `NewPartitionIDs`) for delete-range and stats update
  - Emit notifier event: `newStatsDDLEventForJob`

Partitioning changes:

- `ActionAlterTablePartitioning` / `ActionRemovePartitioning` reuse the same pipeline and may **Drop+Create** the table with a new table ID:
  - See the `StatePublic` branch in `onReorganizePartition` where `metaMut.DropTableOrView` + `CreateTableOrView` is used.

## EXCHANGE PARTITION (`ActionExchangeTablePartition`)

- Handler: `pkg/ddl/partition.go:onExchangeTablePartition`
- Job args: `model.ExchangeTablePartitionArgs`

High-level flow:

- An interim schema version (`StateWriteOnly`) is used to make the non-partitioned table enforce the partition constraint window:
  - It sets `ExchangePartitionInfo` on involved tables before the swap
  - Optional validation path: `checkExchangePartitionRecordValidation` (when `WITH VALIDATION`)
- Then it swaps metadata/auto-IDs/replica bookkeeping and finalizes, with explicit rollback handling (`rollbackExchangeTablePartition`)

## Placement, labels, and TiFlash (don’t forget these)

Partition DDL often needs PD side effects:

- Placement bundle updates: `pkg/ddl/partition.go:alterTablePartitionBundles`, `pkg/ddl/partition.go:droppedPartitionBundles`
- Label rule updates: `pkg/ddl/partition.go:alterTableLabelRule` / `dropLabelRules`
- TiFlash partition placement + replica status:
  - `infosync.ConfigureTiFlashPDForPartitions`
  - Replica check loop: `checkPartitionReplica`

These should generally happen **before** the state that starts writing to the new partitions, so a retry/rollback doesn’t leave partial placement state.

## Practical debugging anchors

SQL:

- `ADMIN SHOW DDL JOBS`
- `ADMIN SHOW DDL JOB QUERIES`
- `ADMIN CANCEL DDL JOBS <job_id>`

Tables:

- `mysql.tidb_ddl_job`
- `mysql.tidb_ddl_history`
- `mysql.tidb_ddl_reorg`

Tests (good starting points):

- `pkg/ddl/partition_test.go`
- `pkg/ddl/notifier/testkit_test.go` (covers multiple partition actions)

## Common pitfalls checklist

- Dropping/truncating partitions with **global indexes** requires the extra “delete reorg” stage; skipping it leaves orphan global-index entries.
- `AddingDefinitions` / `DroppingDefinitions` and `Partition.DDLState` are part of multi-version correctness; don’t clear them too early.
- `doPartitionReorgWork` forces transactional reorg (`ReorgTypeTxn`); don’t assume fast-reorg/ingest behavior.
- PD side effects (placement/labels/TiFlash) must be ordered so rollback/retry remains safe and idempotent.

