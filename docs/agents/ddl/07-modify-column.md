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

Modify-column behavior is driven by `args.ModifyColumnType`:

- Selection logic: `pkg/ddl/modify_column.go:getModifyColumnType`
- Fast-path predicate: `pkg/ddl/modify_column.go:noReorgDataStrict` (strong check: “no reorg no matter what data is”)

High-level rules (read `getModifyColumnType` for the exact gates):

- If `noReorgDataStrict(...)` is true:
  - Not NULL → NULL? / type range superset, etc. → `ModifyTypeNoReorg`
  - NULL → NOT NULL → `ModifyTypeNoReorgWithCheck` (requires validating existing rows)
- Otherwise:
  - Partitioned tables / TiFlash replica → force `ModifyTypeReorg` (see the `FIXME` gate in `getModifyColumnType`)
  - Non-strict SQL mode → `ModifyTypeReorg`
  - If row rewrite is required → `ModifyTypeReorg` (`needRowReorg`)
  - Else:
    - No related indexes or index rewrite not needed → `ModifyTypeNoReorgWithCheck`
    - Index rewrite needed → `ModifyTypeIndexReorg` (`needIndexReorg`)

Special-case: `VARCHAR` → `CHAR` may start as `ModifyTypePrecheck` and then “downgrade” to a no-reorg type if data is safe:

- Precheck path: `pkg/ddl/modify_column.go:precheckForVarcharToChar`

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

## Common pitfalls checklist

- Partitioned table / TiFlash replica paths are conservatively forced into full reorg (`getModifyColumnType`); don’t assume “fast path” applies.
- Forgetting to persist mid-flight identifiers (changing column/index IDs) → job can’t resume after retry/owner transfer.
- Missing schema-sync boundary between state transitions → other nodes may apply incompatible DML rules.
- Job args compatibility: `ModifyColumnType` and args decoding must remain backward compatible (`getModifyColumnType`’s compat comments).

