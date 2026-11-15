# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**3 unresolved: 0 open, 3 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| D02 | partial | The ingest (fast-reorg) backfill runs: `ddl.Start` initializes Go's ingest environment (`InitGlobalLightningEnv` on TiKV, `CleanUpTempDirLoop`), ADD INDEX picks `ReorgType::INGEST` and, under `tidb_enable_dist_task`, Go's DXF backfill task (`tidb-ddl` `real_tikv_ddl/backfilling_dist.rs`, `backfilling_dist_{executor,scheduler}.rs`), whose local backend (`tidb-ddl-ingest`, `tidb-ingestor-ingestctrl`) sorts on disk and imports through TiKV's ImportSST, then merges the temporary index as its own DXF step and checks a unique index's remote duplicates; `autoPauseAddIndexJobOnKVDiskFull` (`tidb-ddl` `index.rs`), the status port's `/ingest/*` routes (`tidb-server` `ingest_handler.rs`) and the backfill executor's Top SQL attach are Go's; the testkit's mock store runs `InitDistTaskLoop` as Go's `BootstrapSession` does. A live TiKV ADD INDEX / UNIQUE INDEX / duplicate case answers as Go's (job comment `ingest, DXF, max_node_count=3`, Go's 1062 text). The local backend (`tidb-ingestor-ingestctrl`) follows Go's `ingestctrl` for split/scatter in batches with retries, region job generation, the store balancer, the retryer, write/ingest with MultiIngest detection and its single-SST fallback, the store write limiter, engine management and the duplicate iterator, SQL/TiKV checksum with GC TTL ownership, gzip and the TiKV mode switcher, pinned by 69 of Go's 79 `ingestctrl` tests; extsort replaces Pebble, so Go's Pebble-internal SST block-size test does not apply. `ADMIN ALTER DDL JOBS` (planner `buildAdminAlterDDLJob`, `AlterDDLJobExec`) rewrites the job row, `modifyTaskParamLoop` carries it to the DXF task, and `readIndexStepExecutor.ResourceModified` / `TaskMetaModified` retune the running pipeline's reader/writer pools, batch size and write limit. `estimateTableRowSize` fills the task's `EstimateRowSize`; `Backend.ImportEngine` follows Go's order: TiKV's region-split config, the empty-engine skip, `forceTableSplitRange` at 100+ regions and the table-scope scheduler pause (`br/pkg/pdutil.PauseSchedulersByKeyRange`, `tidb-br` `pdutil`). Left: global sort (`pkg/ingestor/globalsort`, `engineapi`, `ingestcli`) with `ingestctrl`'s external-engine and cloud-worker tests; duplicate conflict conversion and lazy duplicate-controller init (Lightning backend/kv/errormanager); Go's `pkg/ddl/ingest` integration tests are ported except two that name structural gaps: the local ingest's partition loop does not persist each next partition (Go `updateReorgInfo`, so an empty partition's progress is never recorded; `TestAddIndexWithEmptyPartitions`), and the ingest scan's snapshot is not an internal session of the session manager (`wrapInBeginRollback` on a pooled session; `TestAddIndexSetInternalSessions`): GC's min-start-ts still sees it, through `ACTIVE_START_TS`, which every storage transaction holds; the realtikv add-index inventory (`tests/realtikvtest/addindextest*`) is not ported | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest`, `pkg/ingestor/ingestctrl` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| E05 | partial | IMPORT INTO still refuses execution. Go's assignment visitor/input-variable checks and the ImportInto planner handoff now run, including target metadata, rewritten options, generated columns, result schema, target privileges, temporary/cached-table rejection and SELECT width/physical optimization (Go TestImportIntoBuildPlan and testkit captures). Importer option/default retention, column mappings, CSV interpretation/skip handling, the SQL dump tokenizer/row/chunk reader and local file/glob discovery helpers are ported and Go-pinned; they are not yet an executable import pipeline. Remaining planner work: URL parse errors, SEM v1/nextgen S3 handling and complete snapshot/privilege pins. Remaining execution work: ImportIntoExec, complete importer plan/session capture and controller integration, compressed/remote discovery and Parquet parsing, chunk processing, prechecks, physical KV encoding/ingest, and DXF task/job lifecycle, cleanup, cancellation and result rows. The in-scope prerequisite ports include pkg/lightning/backend/kv and pkg/ingestor/ingestctrl. No successful local or remote import is claimed. | `pkg/executor/importer`, `pkg/executor/import_into.go`, `pkg/dxf/importinto`, `pkg/planner/core` (`buildImportInto`) |
| E07 | partial | Local `SHOW BACKUP METADATA` executes over Go V1/V2 backupmeta (including indexed schema checksums; the BR reader also reads encrypted V2 fixtures). Local atomic writes, Go V1/V2 MetaWriter (including encryption, indexes and archive sizes), DDL reading, restore schema/file loading with partition assignment, checksums, and statistics payloads are ported and tested against Go tests/captured output. BRIE's queue/progress, SHOW BACKUPS/RESTORES, SHOW BR JOB QUERY, CANCEL BR JOB, dynamic privileges, SEM/store builder guards and SQL option/configuration fields are ported. Remaining: `BRIEExec.Next` does not register or execute BACKUP/RESTORE; `task.RunBackup`/`RunRestore`, the backup client and snapshot restore client, PD/GC/checkpoint orchestration, TiKV backup/SST import, tidbGlue and restore DDL are absent. Queue execution is tested directly with registered jobs, not physical backup jobs. Complete destination URL parsing/canonicalization, remote backend construction/credential extraction, complete task configuration/CLI parsing and remaining BR metadata paths (compatibility checks, statistics-file writing/downloading and restore integration) remain unported. No database/table backup-drop-restore roundtrip is established. | `pkg/executor/brie.go`, `br/pkg/task`, `br/pkg/backup`, `br/pkg/restore/snap_client` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
