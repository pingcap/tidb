# DDL Special Handling During Log Meta Restore

This document summarizes **only the DDL-related behaviors that are handled specially** during log/meta restore. All items below are confirmed by code paths in the current repository (no inference beyond what the code checks).

## 0) Recorder lifecycles (delRangeRecorder / ingestRecorder / TiflashRecorder)

This section describes **when each recorder is created, populated, and consumed**.

### delRangeRecorder (GC delete-range)

- Created:
  - `br/pkg/task/stream.go` → `buildSchemaReplace`
    - `stream.NewSchemasReplace(..., recordDeleteRange=client.RecordDeleteRange, ...)`
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `NewSchemasReplace`
    - `delRangeRecorder: newDelRangeExecWrapper(globalTableIdMap, recordDeleteRange)`

- Populated:
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `SchemasReplace.RewriteMetaKvEntry`
    - On Default CF + `utils.IsMetaDDLJobHistoryKey(e.Key)`: decode `model.Job` and call `processIngestIndexAndDeleteRangeFromJob(job)`
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `processIngestIndexAndDeleteRangeFromJob`
    - `if ddl.JobNeedGC(job)` → `ddl.AddDelRangeJobInternal(..., sr.delRangeRecorder, job)`
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `brDelRangeExecWrapper.ConsumeDeleteRange`
    - calls `recordDeleteRange(*PreDelRangeQuery)` (wired to `LogClient.RecordDeleteRange`).

- Buffered / thread-safety:
  - `br/pkg/restore/log_client/batch_meta_processor.go` → `RestoreMetaKVProcessor.RestoreAndRewriteMetaKVFiles`
    - starts loader: `rp.client.RunGCRowsLoader(ctx)`
  - `br/pkg/restore/log_client/client.go`:
    - `RecordDeleteRange` pushes into `deleteRangeQueryCh`
    - `RunGCRowsLoader` drains `deleteRangeQueryCh` into `rc.deleteRangeQuery`

- Consumed:
  - `br/pkg/task/stream.go` → `restoreStream` (after KV restore)
    - `client.InsertGCRows(ctx)`
  - `br/pkg/restore/log_client/client.go` → `InsertGCRows`
    - closes channel, waits loader, and inserts into `gc_delete_range`.

### ingestRecorder (ingest index repair)

- Created:
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `NewSchemasReplace`
    - `ingestRecorder: ingestrec.New()`

- Populated:
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `SchemasReplace.RewriteMetaKvEntry`
    - On Default CF + `mDDLJobHistory`: decode `model.Job` → `processIngestIndexAndDeleteRangeFromJob(job)`
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `tryRecordIngestIndex`
    - For `ActionMultiSchemaChange`: expands subjobs
    - Otherwise: `sr.ingestRecorder.TryAddJob(job, ...)`
  - `br/pkg/restore/ingestrec/ingest_recorder.go` → `TryAddJob`
    - only records ingest reorg jobs for `ActionAddIndex` / `ActionAddPrimaryKey` / `ActionModifyColumn` (and state constraints).

- Rewritten (table ID mapping after meta restore):
  - `br/pkg/task/stream.go` → `restoreStream`
    - `ingestRecorder := schemasReplace.GetIngestRecorder()`
    - `rangeFilterFromIngestRecorder(ingestRecorder, rewriteRules)`
  - `br/pkg/task/stream.go` → `rangeFilterFromIngestRecorder`
    - `ingestRecorder.RewriteTableID(...)` based on `rewriteRules`.

- Consumed:
  - `br/pkg/task/stream.go` → `restoreStream` (after KV restore)
    - `client.RepairIngestIndex(ctx, ingestRecorder, cfg.logCheckpointMetaManager, g)`
  - `br/pkg/restore/log_client/client.go` → `RepairIngestIndex`
    - calls `ingestRecorder.UpdateIndexInfo(..., InfoSchema)` then `Iterate(...)` to generate and execute SQL.
    - may load/save generated SQLs via checkpoint meta manager (see `generateRepairIngestIndexSQLs`).

### TiflashRecorder (TiFlash replica stripping + later restore)

- Created:
  - `br/pkg/task/stream.go` → `RunPointInTimeRestore`
    - `cfg.tiflashRecorder = tiflashrec.New()`

- Populated and applied during meta KV replay:
  - Hook wiring:
    - `br/pkg/task/stream.go` → `buildSchemaReplace`
      - sets `schemasReplace.AfterTableRewrittenFn = func(deleted bool, tableInfo *model.TableInfo) { ... }`
  - Hook invocation:
    - `br/pkg/stream/rewrite_meta_rawkv.go` → `rewriteTableInfo`
      - calls `sr.AfterTableRewrittenFn(false, &tableInfo)` on normal rewrite
    - `br/pkg/stream/rewrite_meta_rawkv.go` → `rewriteEntryForTable`
      - on deletion calls `sr.AfterTableRewrittenFn(true, &model.TableInfo{ID: newTableID})`
  - Hook behavior:
    - `br/pkg/task/stream.go` → `buildSchemaReplace`
      - records current `tableInfo.TiFlashReplica` into `cfg.tiflashRecorder` (or `DelTable` when deleted / nil)
      - **removes** replica info from restored meta: `tableInfo.TiFlashReplica = nil`

- Checkpoint persistence:
  - Save:
    - `br/pkg/restore/log_client/client.go` → `LoadOrCreateCheckpointMetadataForLogRestore`
      - `CheckpointMetadataForLogRestore.TiFlashItems = tiflashRecorder.GetItems()`
  - Load:
    - `br/pkg/task/stream.go` → `RunPointInTimeRestore`
      - when skipping full restore due to checkpoint: `cfg.tiflashRecorder.Load(taskInfo.CheckpointInfo.Metadata.TiFlashItems)`

- Consumed:
  - SQL generation:
    - `br/pkg/restore/tiflashrec/tiflash_recorder.go`
      - `GenerateAlterTableDDLs(InfoSchema)` generates `ALTER TABLE ... SET TIFLASH REPLICA ...`
  - Execution:
    - `br/pkg/task/stream.go` → `restoreStream`
      - `sqls := cfg.tiflashRecorder.GenerateAlterTableDDLs(mgr.GetDomain().InfoSchema())`
      - `client.ResetTiflashReplicas(ctx, sqls, g)`

## 1) DDL job history extraction (mDDLJobHistory)
**What is special:** DDL job history entries are *not restored as meta KV*; instead they are decoded and used for two special cases (ingest index repair and delete-range GC).

**Path:**
- `br/pkg/stream/rewrite_meta_rawkv.go`
  - `SchemasReplace.RewriteMetaKvEntry`
    - Checks `utils.IsMetaDDLJobHistoryKey(e.Key)` on **Default CF**
    - Decodes `model.Job` and routes to `processIngestIndexAndDeleteRangeFromJob`

**Behavior:**
- DDL job history is parsed and **not written back** as meta KV (`return nil, ...`).

## 2) Ingest index DDL jobs (repair by replay)
**What is special:** Ingest-mode index builds are *not included in log backup KV*, so they are recorded and later repaired via SQL.

**Paths:**
- Capture from DDL job history:
  - `br/pkg/stream/rewrite_meta_rawkv.go`
    - `processIngestIndexAndDeleteRangeFromJob`
      - `tryRecordIngestIndex`
        - For `ActionMultiSchemaChange`: expands to sub-jobs
        - Otherwise: `ingestrec.IngestRecorder.TryAddJob`
- Recording logic:
  - `br/pkg/restore/ingestrec/ingest_recorder.go`
    - `TryAddJob`
      - **Only records** when all conditions are true:
        - `job.ReorgMeta.ReorgTp == model.ReorgTypeIngest`
        - `job.Type` is **one of** `ActionAddIndex`, `ActionAddPrimaryKey`, `ActionModifyColumn`
        - Job is synced (or sub-job done)
- Repair execution:
  - `br/pkg/restore/log_client/client.go`
    - `RepairIngestIndex`
    - `generateRepairIngestIndexSQLs`

**Behavior:**
- Only the job types above are recorded for ingest repair.
- The repair uses **latest InfoSchema** to build ADD INDEX/PRIMARY KEY SQL.

## 3) DDL jobs that require GC delete-range
**What is special:** For DDL jobs where TiDB normally relies on GC to clean ranges, the delete-range is recorded and executed explicitly after restore.

**Paths:**
- Capture from DDL job history:
  - `br/pkg/stream/rewrite_meta_rawkv.go`
    - `processIngestIndexAndDeleteRangeFromJob`
      - `if ddl.JobNeedGC(job)` → `ddl.AddDelRangeJobInternal(..., brDelRangeExecWrapper, job)`
- Delete-range recording:
  - `br/pkg/stream/rewrite_meta_rawkv.go`
    - `brDelRangeExecWrapper` (captures SQL + params)
- Execution after restore:
  - `br/pkg/restore/log_client/client.go`
    - `RunGCRowsLoader` / `InsertGCRows`

**Behavior:**
- **Only jobs where** `ddl.JobNeedGC(job)` is true are handled.
- The code does **not** list job types explicitly; the DDL package decides.

## 4) Table deletion / recreation tracking (DDL effects)
**What is special:** Deleted tables are tracked to refresh metadata in a dependency-safe order, and a re-created table is removed from the delete list.

**Paths:**
- `br/pkg/stream/rewrite_meta_rawkv.go`
  - `rewriteEntryForTable`
    - When write CF indicates deletion: add to `deletedTables`
    - When a new table meta is written after deletion: remove from `deletedTables`
    - Comment references **RENAME TABLE** and **EXCHANGE PARTITION** sequences
- `br/pkg/restore/log_client/client.go`
  - `RefreshMetaForTables` uses `deletedTables` to refresh meta in order

**Behavior:**
- Delete + re-create sequences (e.g., rename/exchange partition) are handled to avoid stale refresh.

## 5) DDL/meta filtering to only restore DB + DDL history
**What is special:** During meta KV restore, only `mDB` and `mDDLJobHistory` keys are considered; other meta keys are skipped.

**Path:**
- `br/pkg/restore/log_client/log_file_manager.go`
  - `ReadFilteredEntriesFromFiles`
    - `if !utils.IsDBOrDDLJobHistoryKey(txnEntry.Key) { continue }`

**Behavior:**
- This limits meta restore scope to database info and DDL job history.

## 6) TiFlash replica (ALTER TABLE ... SET TIFLASH REPLICA)

**What is special:** During PiTR, TiFlash replica config is **intentionally stripped out of restored table meta** and restored later via SQL.

This impacts any historical/meta changes whose effect is persisted into `TableInfo.TiFlashReplica`, including the DDL `ALTER TABLE ... SET TIFLASH REPLICA ...`.

**Paths:**

- Strip replica info while replaying meta KV (and record it):
  - `br/pkg/task/stream.go` → `buildSchemaReplace`
    - assigns `SchemasReplace.AfterTableRewrittenFn`
    - in callback:
      - updates `cfg.tiflashRecorder` via `AddTable/DelTable`
      - then `tableInfo.TiFlashReplica = nil`
  - `br/pkg/stream/rewrite_meta_rawkv.go` → `rewriteTableInfo`
    - calls `AfterTableRewrittenFn(false, &tableInfo)` during table meta rewrite

- Persist to checkpoint (so retries keep the same intended replica config):
  - `br/pkg/restore/log_client/client.go` → `LoadOrCreateCheckpointMetadataForLogRestore`
    - saves `CheckpointMetadataForLogRestore.TiFlashItems`
  - `br/pkg/task/stream.go` → `RunPointInTimeRestore`
    - loads `cfg.tiflashRecorder.Load(...TiFlashItems...)` when resuming from checkpoint

- Restore replica config after PiTR finishes:
  - `br/pkg/task/stream.go` → `restoreStream`
    - `cfg.tiflashRecorder.GenerateAlterTableDDLs(InfoSchema)`
    - `client.ResetTiflashReplicas(ctx, sqls, g)`
  - `br/pkg/restore/log_client/client.go` → `ResetTiflashReplicas`

---

## Summary (no inference)
From the current code paths, DDL-related special handling during log/meta restore is limited to:
1) **Ingest index repair** (specific ingest reorg DDL types only).
2) **Delete-range GC** for DDL jobs where `ddl.JobNeedGC(job)` is true.
3) **Table delete/recreate tracking** affecting meta refresh order (rename/exchange patterns).
4) **Meta filtering** to `mDB` + `mDDLJobHistory` keys only.
5) **TiFlash replica stripping + later restore** (restore-time `ALTER TABLE ... SET TIFLASH REPLICA ...`).

No other DDL job types are explicitly enumerated or handled beyond these code paths.
