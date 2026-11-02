# RECOVER TABLE, FLASHBACK TABLE and FLASHBACK DATABASE, as Go's

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

In Go TiDB a dropped (or truncated) table can be brought back while its data
is still inside the GC safe point:

- `RECOVER TABLE t` or `RECOVER TABLE BY JOB <id>`;
- `FLASHBACK TABLE t [TO t2]`;
- `FLASHBACK DATABASE d [TO d2]` (Go `RecoverSchema`).

The Rust rewrite parses and plans these statements (`tidb-ast`
`DdlStmt::RecoverTable` / `FlashbackTable` / `FlashbackDatabase`), but
nothing executes them: there is no model argument type, no DDL job handler
for `ActionRecoverTable` / `ActionRecoverSchema`, and no executor half.

After this plan, `drop table t; flashback table t;` gives back `t` with its
rows and auto-increment state, under the same checks and errors as Go.

## The Go code to port (the source of truth)

- `pkg/executor/ddl.go`:
  - `executeRecoverTable`;
  - `getRecoverTableByJobID`, which reads the history job, runs
    `gcutil.ValidateSnapshot`, and takes the table from the snapshot
    infoschema at `GetRecoverSnapshotTS(job)`;
  - `getRecoverTableByTableName`, which walks `ddl.IterHistoryDDLJobs` with
    `GetDropOrTruncateTableInfoFromJobsByStore` and the GC safe point, and
    refuses a GLOBAL temporary table;
  - `executeFlashbackTable` (rename with `TO`);
  - `executeFlashbackDatabase` with `getRecoverDBByName`;
  - the auto-ids come from snapshot meta (`GetAutoIDAccessors().Get()`).
- `pkg/ddl/executor.go`:
  - `RecoverTable`, which builds `ActionRecoverTable` with
    `model.RecoverArgs{RecoverInfo, CheckFlag: recoverCheckFlagNone}`, plus
    the involving schema info when renamed;
  - `RecoverSchema`.
- `pkg/ddl/table.go`:
  - `onRecoverTable`, in two steps. NONE to WRITE_ONLY records whether
    GC was enabled (`CheckFlag`). WRITE_ONLY to PUBLIC disables GC, checks
    the safe point, and runs `recoverTable`, then
    `updateVersionAndTableInfo` and `FinishTableJob`.
  - `recoverTable` removes the table's delete ranges
    (`removeFromGCDeleteRange`), clears placement bundles, and calls
    `CreateTableAndSetAutoID` (TTL disabled, TiFlash replica rules).
- `pkg/ddl/schema.go`: `onRecoverSchema`, the same two steps for every
  table of the schema.
- `pkg/ddl/job_worker.go` / `ddl.go`: `finishRecoverTable` /
  `finishRecoverSchema` re-enable GC when the flag says GC was on.
- `pkg/meta/model/job_args.go`: `RecoverArgs`, `RecoverSchemaInfo` and
  `RecoverTableInfo`, with their JSON (v1 and v2 forms).

Oracle: `./gorun` refuses FLASHBACK because its store has no
`tikv_gc_safe_point` row. Insert one first:
`insert into mysql.tidb values ('tikv_gc_safe_point','20000101-00:00:00 +0000','')`.

## Rust pieces that exist

- `tidb-gcutil`: `check_gc_enable`, `disable_gc`, `enable_gc`,
  `validate_snapshot`, `get_gc_safe_point`.
- `tidb_meta::transaction::Mutator::create_table_and_set_auto_id`.
- `tidb-ddl` `ddl_delete_range::DelRangeTable` (the `gc_delete_range` rows).
- History jobs: `tidb_ddl::ddl_history_table::DdlHistoryTable`.
- Snapshot meta: `SnapshotMetaSnapshot` over
  `ClusterTransactions::open_snapshot_at`, as in `sessions.rs`.

## Progress

- [x] M1: model. `tidb-model` `job_args_recover.rs` holds `AutoIdGroup`
  (moved from `tidb-meta`, which re-exports it), `RecoverTableInfo`,
  `RecoverSchemaInfo` (its embedded `*DBInfo` is hand-serialized, Go's
  keys first and in order) and `RecoverArgs` (`JobArgsValue::Recover`,
  v1/v2, `get_recover_args`). Test `recover_args_encode_and_decode_as_go`
  compares against `testdata/recover_args_go.txt`, the bytes Go's own
  model package writes.
- [ ] M2: job handlers. `onRecoverTable` and `recoverTable` in a new
  `cluster_ddl/recover_table.rs`, dispatched from `cluster_ddl.rs` (next to
  the CREATE TABLE arms), plus the finish hook that re-enables GC.
- [ ] M3: executor. Lower the three statements to `DdlStatement` variants
  that build Go's job (the history walk, the snapshot `TableInfo`, the
  snapshot auto-ids), and submit through
  `submit_persisted_job_with_retry`.
- [ ] M4: `onRecoverSchema` / FLASHBACK DATABASE.
- FLASHBACK CLUSTER (`ActionFlashbackCluster`) is out of scope. It needs
  TiKV's flashback RPCs.

## Surprises & Discoveries

- `serde_json::Value` sorts object keys and cannot hand over raw JSON
  members. The embedded `*DBInfo` is therefore written as text and read
  back from text.

## Decision Log

(None yet.)

## Outcomes & Retrospective

(Pending.)
