# Modify Column Deep Dive (type change, nullability, online reorg)

This doc focuses on **`model.ActionModifyColumn`**, covering `ALTER TABLE ... MODIFY COLUMN`, `ALTER TABLE ... CHANGE COLUMN`, and `ALTER TABLE ... RENAME COLUMN` (implemented as a special case of modify-column).

Modify column is tricky because it can be:

- **Metadata-only** (no scan / no reorg).
- **Metadata + validation** (check existing rows, but no backfill).
- **Index-only reorg** (rebuild affected indexes because key encoding changes).
- **Row + index reorg** (rewrite table rows + rebuild indexes).

## Entry points (start here)

SQL → DDL module:

- SQL dispatch: `pkg/executor/ddl.go` (`type DDLExec`, `(*DDLExec).Next`)
- Modify column: `pkg/ddl/executor.go:ModifyColumn`
- Change column: `pkg/ddl/executor.go:ChangeColumn`
- Rename column: `pkg/ddl/executor.go:RenameColumn` (builds a `model.ActionModifyColumn` job)

Job creation / args:

- Build job+args: `pkg/ddl/modify_column.go:GetModifiableColumnJob`
- Typed job args: `model.ModifyColumnArgs` (`pkg/meta/model/job_args.go`)

Job execution (owner worker):

- Worker dispatch: `pkg/ddl/job_worker.go` (`case model.ActionModifyColumn:`)
- Main handler: `pkg/ddl/modify_column.go:onModifyColumn`

## Modify type selection (what decides “reorg”)

Modify-column behavior is driven by `args.ModifyColumnType`, with all conversion
validation, storage/index classification, execution-time restrictions, and
`VARCHAR -> CHAR` precheck selection centralized in `pkg/ddl/modify_column_change.go`:

- Submission analysis: `pkg/ddl/modify_column_change.go:analyzeModifyColumn`
- Initial execution resolution: `pkg/ddl/modify_column_change.go:resolveModifyColumnType`
- Post-precheck resolution: `pkg/ddl/modify_column_change.go:resolveModifyColumnTypeAfterPrecheck`
- Fast-path predicate: `pkg/ddl/modify_column_change.go:isMetaOnlyChange` (strong check: “no reorg no matter what data is”)

Submission calls `analyzeModifyColumn` and initial execution calls
`resolveModifyColumnType`, each using its own metadata snapshot. Conversion
analysis is structured into three layers:

1. **Storage & index classification (`classifyColumnChange`)**: checks safe
   charset compatibility (`isCharsetMetadataCompatible`: ENUM/SET ordinals and
   UTF8/Latin1 -> UTF8MB4 widening), then classifies row storage by type family
   in `classifyByRowValue` / `classifyByStringRowValue` (integer ranges ignore
   display width; DECIMAL requires identical precision/scale/sign; ENUM/SET
   preserve member ordinals; BINARY length changes require padding changes;
   vector dimensions must match unless target is unconstrained). Nonbinary
   CHAR/VARCHAR to TEXT/BLOB can skip data validation if the maximum source byte
   length fits the target byte limit. With the same charset, a conversion that
   requires validation checks the byte length of existing rows. For indexed
   columns it checks index key/restored-data encoding via `needsIndexReorg`
   (including incompatible collation changes under new collation and
   `NeedRestoredData` changes such as `CHAR -> VARCHAR` with `_bin` collations,
   routing non-integer/non-char types like `TEXT` with prefix indexes to
   `ModifyTypeReorg`). `isMetaOnlyChange`
   uses the same classification for dependent-object checks; nullability remains
   a separate check.
2. **Support validation (`validateModifyColumn`)**: validates the target
   charset/collation pair, cross-type support (`isCrossTypeChangeSupported`), the
   GBK charset/collation change restriction, and primary-key restrictions
   (`change != ModifyTypeNoReorg`). Non-GBK charset and collation changes are
   allowed and routed by `classifyColumnChange` to the appropriate execution
   path instead of being gated by type-only heuristics.
3. **Execution-time restrictions & precheck (`applyExecutionRestrictions` +
   `needsVarcharToCharPrecheck`)**: promotes `ModifyTypeNoReorg` to
   `ModifyTypeNoReorgWithCheck` on `NULL -> NOT NULL`, applies legacy
   `mysql.TypeNull` compatibility, partition/TiFlash, stats-encoding, and
   non-strict SQL mode fallbacks, and wraps `VARCHAR -> CHAR` validation/index
   paths into `ModifyTypePrecheck` on initial resolution.

New MODIFY/CHANGE COLUMN jobs set `NeedReorg=true`, including metadata-only changes,
so scheduling and reorg settings do not rely on the submission snapshot. This also
applies to MODIFY COLUMN sub-jobs in multi-schema changes. Initial execution still
selects the actual conversion from current metadata; the flag does not force data
rewriting, and distributed-reorg validation (`IsDistReorg && !IsFastReorg`) is
deferred to `initForReorgIndexes` so metadata-only changes keep working when
`tidb_enable_dist_task=ON` and `tidb_ddl_enable_fast_reorg=OFF`. Lightweight
modifications can therefore wait behind reorg jobs. Already queued jobs retain
their stored scheduling flags. Pure RENAME COLUMN jobs retain general-pool
scheduling.

High-level rules (read `applyExecutionRestrictions` for the exact gates):

- If a legacy job (`ModifyTypeNone` or `mysql.TypeNull`) already has `ChangingColumn != nil` → stay on `ModifyTypeReorg` in `onModifyColumn` so both resume and rollback clean up its temporary column and indexes
- If `classifyColumnChange(...) == ModifyTypeNoReorg`:
  - Not NULL → NOT NULL → `ModifyTypeNoReorg`
  - NULL → NOT NULL → `ModifyTypeNoReorgWithCheck` (requires validating existing rows)
- Otherwise:
  - Legacy `mysql.TypeNull` stored type → force `ModifyTypeReorg`
  - Partitioned tables / TiFlash replica → force `ModifyTypeReorg` (see the `FIXME` gate in `applyExecutionRestrictions`)
  - Incompatible stats encoding (integer sign toggle / incompatible collation) → force `ModifyTypeReorg`
  - Non-strict SQL mode → `ModifyTypeReorg`
  - Otherwise keep base classification (`ModifyTypeNoReorgWithCheck`, `ModifyTypeIndexReorg`, or `ModifyTypeReorg`), wrapping `VARCHAR -> CHAR` check/index-reorg into `ModifyTypePrecheck` on initial resolution.

Special-case: `VARCHAR` → `CHAR` may start as `ModifyTypePrecheck` and then “downgrade” to a no-reorg type if data is safe:

- Precheck path: `pkg/ddl/modify_column.go:precheckForVarcharToChar`

## Partition-column compatibility gate

`ALTER TABLE ... MODIFY/CHANGE COLUMN` on a partition column is still handled by
`model.ActionModifyColumn`; it does **not** become a partition-specific DDL job.
What changes is that TiDB applies an extra compatibility gate before the modify
can proceed:

This gate covers both:

- `PARTITION BY ... COLUMNS(...)` tables, where the partition column names are in
`tblInfo.Partition.Columns`
- Expression partitioning, where TiDB extracts referenced columns from
`tblInfo.Partition.Expr`

Core guardrails in `pkg/ddl/modify_column.go:checkPartitionColumnModifiable`:

- Renaming a partition column is rejected.
- The new type must still be a legal partitioning-column type
(`isColTypeAllowedAsPartitioningCol`).
- `EvalType`, charset, and collation must stay unchanged.
- Flag changes are narrowly scoped:
  - `NULL -> NOT NULL` is rejected.
  - `NOT NULL -> NULL` is allowed.
  - default-only and comment-only changes are allowed.

After the allowlist check passes, TiDB still rebuilds partition definitions from
generated partition metadata (`AppendPartitionInfo` +
`buildPartitionDefinitionsInfo`), which also runs additional partition-definition
validation. This is the final safety net that catches partition-definition
values that no longer match the modified column type.

The exact allowlist lives in
`pkg/ddl/modify_column.go:checkPartitionColumnTypeChangeAllowlist`. Keep the
doc-level rule simple and use the code as the source of truth:

- `KEY` partition accepts a set of safe changes, such as integer widening,
  `CHAR`/`VARCHAR`/`VARBINARY` length extension, and `ENUM`/`SET` tail-append.
- `RANGE/LIST COLUMNS` accepts a set of safe changes, such as integer widening,
  time/datetime fractional-seconds extension, and variable-length string length extension.
- `RANGE/LIST/HASH` expression partition classifies each target-column usage
  path in the partition expression and only accepts a few known-safe forms
  (`col`, `TO_DAYS(col)`, `EXTRACT(... FROM col)`) and a few safe changes.

This gate decides whether the change is allowed. For allowed changes,
`applyExecutionRestrictions` returns early for a `ModifyTypeNoReorg` base
classification, with validation for a `NULL` to `NOT NULL` change. On partitioned
tables, other base classifications require `ModifyTypeReorg`.

## No-reorg paths (metadata-only / metadata+check)

No-reorg jobs still run through the DDL job framework, but avoid the backfill engine.

- Metadata-only: `pkg/ddl/modify_column.go:doModifyColumnNoCheck`
- Metadata + check: `pkg/ddl/modify_column.go:doModifyColumnWithCheck`
  - Data check query generator: `pkg/ddl/modify_column.go:buildCheckSQLFromModifyColumn`
  - Check executor: `pkg/ddl/modify_column.go:checkModifyColumnData` (runs a restricted SQL `SELECT ... LIMIT 1`)

Finish / schema diff:

- Finalize meta + schema version: `pkg/ddl/modify_column.go:finishModifyColumnWithoutReorg`

## Index-only reorg (rebuild affected indexes)

This path is used when row data can stay as-is, but **index keys/values must be rewritten** (e.g. unsigned/signed integer toggles, collation encoding changes).

- Entry: `pkg/ddl/modify_column.go:doModifyColumnIndexReorg`
- Temporary “changing indexes” creation: `pkg/ddl/modify_column.go:initializeChangingIndexes`
- Index backfill engine: `pkg/ddl/index.go:doReorgWorkForCreateIndex`

State machine (job.SchemaState) is similar to add-index:

- `StateNone` → `StateDeleteOnly` → `StateWriteOnly` → `StateWriteReorganization` → `StatePublic` → Done

## Row + index reorg (changing column + rewrite)

This is the “full” modify-column pipeline:

- Entry: `pkg/ddl/modify_column.go:doModifyColumnTypeWithData`
- Create a hidden “changing column” + temp indexes: `pkg/ddl/modify_column.go:getChangingCol`
- Reorg stages persisted in `job.ReorgMeta.Stage` (`pkg/meta/model/reorg.go`):
  - `ReorgStageModifyColumnUpdateColumn` (row rewrite)
  - `ReorgStageModifyColumnRecreateIndex` (index rebuild)
  - `ReorgStageModifyColumnCompleted`

Schema state machine (online compatibility window):

- Changing column + temp indexes:
  - `StateNone` → `StateDeleteOnly` → `StateWriteOnly` → `StateWriteReorganization` → `StatePublic`
- Old column + old indexes:
  - After publish: `StateWriteOnly` → `StateDeleteOnly` → removed

Row rewrite (reorg/backfill) is implemented via the shared reorg engine:

- Reorg driver: `pkg/ddl/modify_column.go:doReorgWorkForModifyColumn`
- Row update worker: `pkg/ddl/column.go:updatePhysicalTableRow` (uses `typeUpdateColumnWorker` for `ActionModifyColumn`)
- Record rewrite loop: `pkg/ddl/column.go:updateColumnWorker` (casts and writes new row values)

Index recreation reuses the add-index reorg path:

- `pkg/ddl/index.go:doReorgWorkForCreateIndex`

## Persistence (where progress and “mid-flight” state live)

Durable progress matters because owner transfer / retry can re-run steps.

Job args (`model.ModifyColumnArgs`) persist mid-flight identifiers so the job can resume:

- `OldColumnID` and `OldColumnName`
- `ChangingColumn` / `ChangingIdxs` (full reorg)
- `RedundantIdxs` (GC old temp indexes created by earlier modify-column jobs)
- Finished args populate delete-range inputs: `IndexIDs`, `PartitionIDs`, etc.

Reorg checkpoint/progress:

- Job record: `mysql.tidb_ddl_job` (`job_meta`, `reorg`, `processing`)
- Reorg handle table: `mysql.tidb_ddl_reorg` (checkpoint range + element)

Reorg meta (`job.ReorgMeta`) persists:

- `ReorgTp` (backfill type selection for index rebuild): `pkg/ddl/index.go:initForReorgIndexes` → `pkg/ddl/index.go:pickBackfillType`
- `Stage` (modify-column stage machine): `pkg/meta/model/reorg.go`
- `AnalyzeState` (optional analyze after reorg)

## Rolling back / cancellation (keep invariants)

Rollback handling depends on the modify type:

- No-reorg rollback: `pkg/ddl/modify_column.go:rollbackModifyColumnJob`
- Full reorg rollback: `pkg/ddl/modify_column.go:rollbackModifyColumnJobWithReorg`
- Index-only rollback: `pkg/ddl/modify_column.go:rollbackModifyColumnJobWithIndexReorg`

Key invariant: a step may be retried/replayed, so make each step **idempotent** and ensure all “new identifiers” are persisted before doing irreversible work.

## Practical debugging anchors

SQL:

- `ADMIN SHOW DDL JOBS`
- `ADMIN SHOW DDL JOB QUERIES`
- `ADMIN CANCEL DDL JOBS <job_id>`

Tables:

- `mysql.tidb_ddl_job` (queue)
- `mysql.tidb_ddl_history` (history)
- `mysql.tidb_ddl_reorg` (reorg handle/progress)

Failpoints (tests/debug):

- `github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData`
- `github.com/pingcap/tidb/pkg/ddl/afterModifyColumnStateDeleteOnly`
- `github.com/pingcap/tidb/pkg/ddl/afterReorgWorkForModifyColumn`
- `github.com/pingcap/tidb/pkg/ddl/getModifyColumnType`

Tests (good starting points):

- `pkg/ddl/modify_column_test.go`
- `pkg/ddl/column_modify_test.go`
- `pkg/ddl/column_change_test.go`
- `pkg/ddl/tests/partition/modify_column_test.go` (partition-column allowlist,
nullability, default/comment, and pruning regressions)

## Common pitfalls checklist

- When `baseType != ModifyTypeNoReorg`, `applyExecutionRestrictions` forces full
  reorg for partitioned tables and tables with a positive TiFlash replica count.
- Partition-column modify has an extra allowlist plus partition-definition revalidation; if you change allowed cases, update `checkPartitionColumnModifiable` and the partition regression matrix together.
- Forgetting to persist mid-flight identifiers (changing column/index IDs) → job can’t resume after retry/owner transfer.
- Missing schema-sync boundary between state transitions → other nodes may apply incompatible DML rules.
- Job args compatibility: `ModifyColumnType` and args decoding must remain backward compatible (`applyExecutionRestrictions`’s compat comments).
