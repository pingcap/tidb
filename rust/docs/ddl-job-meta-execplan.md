# DDL job steps write through `meta.Mutator`

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

In Go TiDB a DDL job step (`runOneJobStep` and the `onXxx` handlers in
`pkg/ddl`) runs inside the job's transaction and writes the schema through
`jobCtx.metaMut`, a `meta.Mutator` over that transaction: `CreateDatabase`,
`CreateTableOrView`, `UpdateTable`, `DropTableOrView`, and, through
`updateSchemaVersion`, `GenSchemaVersion` plus `SetSchemaDiff`. Reads in the
step see the step's own writes (the transaction's memory buffer).

The Rust rewrite plans a step against a read-only snapshot and returns a list
of raw key mutations (`DdlWrite.mutations`, `tidb_txnkv::transaction::
BufferMutation`) that the worker commits. Each handler encodes the meta keys
itself (`tidb_meta::key::table_kv_key`, `value::serialize_table_info`), and
the schema version is `catalog.schema_meta_version() + 1` instead of
`GenSchemaVersion` (meta's counter plus one). About 90 raw meta writes in
`crates/tidb-ddl/src` (71 sets, 15 deletes, one lock) do this, plus the
shared helper `add_index::update_version_and_table_info` (Go
`updateVersionAndTableInfo`), which has about 73 callers.

After this plan every handler writes through `tidb_meta::transaction::Mutator`
over the job's transaction view (`crate::job_txn::JobTxn`: the snapshot with
the step's own writes over it), and the step's mutations are that view's
writes. Observable behavior is unchanged except where Go's mutator differs:
`UpdateTable` bumps `TableInfo.Revision` and checks that the database and
table exist; `GenSchemaVersion` increments the stored counter, so a counter
ahead of the loaded catalog (an empty diff the loader stepped back over) no
longer yields a reused version.

## Plan of Work

1. (done) `crate::job_txn`: `JobTxn` (snapshot plus buffered writes, read-your-
   writes `get` / `scan_prefix` / `iterate_range`), `job_meta` (Go
   `meta.NewMutator(txn)`), `job_mutations`, `job_meta_error`;
   `cluster_ddl::update_schema_version` (Go `updateSchemaVersion`).
   `onCreateSchema` converted.
2. One `JobMeta` per step, created where Go creates `jobCtx.metaMut`
   (`plan_persisted_action`), passed to every handler in place of the raw
   snapshot. Raw reads that are not meta reads go through the same
   transaction view. The step's `DdlWrite.mutations` are the view's writes
   followed by whatever the handler still pushes by hand, so handlers convert
   one file at a time.
3. `update_version_and_table_info` / `table_step_with_diff` write through the
   mutator (`UpdateTable`, `update_schema_version`); its 73 callers take the
   meta argument (compiler-driven).
4. The remaining handlers, file by file: `create_table` / `create_object`
   (`CreateTableOrView`), `drop_object` and `cluster_ddl` drop paths
   (`DropTableOrView`, `DropDatabase`), `rename_table`, `truncate`,
   `exchange_partition`, `alter_schema` (`UpdateDatabase`),
   `placement_policy` (`CreatePolicy` / `UpdatePolicy` / `DropPolicy`),
   `resource_group`, `multi_schema`, `worker.rs`.
5. Delete the hand encoders the handlers no longer use.

## Validation and Acceptance

Per step: `cargo test -p tidb-ddl` (lib and `tests/`), then the scratchpad
`binrun.py` set (planner, executor, session, server corpus gate:
"157 topics, 3 divergences (3 stale recordings), 0 refused").

## Progress

- [x] (2026-11-02) Step 1.
- [x] (2026-11-02) Step 2: `plan_persisted_action` and the cancel path run
  their handler through `run_in_job_txn`; a multi-schema change's sub-jobs
  run nested in the step's `JobTxn` (the old `MetaOverlay` is gone: it was
  this view), their raw writes staged into it, and the combined diff is
  `SetSchemaDiff` through the mutator. Handlers that took the snapshot and
  now need the transaction read through it instead (the snapshot parameter
  is gone from them).
- [x] (2026-11-02) Step 3: `update_version_and_table_info` /
  `table_step_with_diff` write through the mutator; `txn` threaded through
  their callers (compiler-driven); parameters left unused (the catalog was
  only read for `schema_meta_version() + 1`) removed.
- [x] (2026-11-02) Step 4: CREATE SCHEMA (`CreateDatabase`), DROP SCHEMA
  (`UpdateDatabase`, `DropDatabase`), ALTER DATABASE (`UpdateDatabase`),
  CREATE TABLE (`CreateTableOrView`; a table with foreign keys is created
  delete-only directly, as `createTableWithForeignKeys` does), CREATE
  TABLES, CREATE VIEW / SEQUENCE (`CreateSequenceAndSetSeqValue`), DROP
  TABLE / VIEW / SEQUENCE (`UpdateTable`, `DropTableOrView` + auto-ID
  `Del`, `DropSequence`), TRUNCATE, RENAME TABLE(S) (`DropTableOrView` /
  `CreateTableOrView` / `UpdateTable`), EXCHANGE PARTITION (auto-ID groups
  `Get` / `Put`, drop / update / create as Go orders them), PARTITION BY /
  REMOVE PARTITIONING (`DropTableOrView`, auto-IDs moved,
  `CreateTableOrView` before the version step), check constraints,
  placement policies and resource groups all write through the mutator, and
  every version is `GenSchemaVersion` + `SetSchemaDiff`. Left as raw writes
  staged into the transaction: CREATE TABLE's starting AUTO_INCREMENT /
  AUTO_RANDOM counters (Go rebases them after the job, `handleAutoIncID`),
  and the notifier-table rows (`append_schema_change_mutations`).
- [ ] Step 5: delete the encoders no handler uses; the behavior differences
  found on the way (below) each land on their own.

## Surprises & Discoveries

- (2026-11-02) Nothing in `crates/tidb-ddl` called `Mutator` before this
  plan; the port of Go's meta writers in `tidb_meta` was unused by the DDL
  owner.

- (2026-11-02) Go's `updateTable` sets `UpdateTS` only for a public table
  outside truncate / rename / exchange (`needUpdateTs`); Rust's helper sets
  it always. Kept as is in step 3 (a behavior change of its own).

- (2026-11-02) PARTITION BY / REMOVE PARTITIONING deleted the old table key
  as a raw mutation and then `UpdateTable`d the new id, which meta did not
  have: the step failed and the job retried until its error limit (the
  corpus topic `globalindex/misc` hung). Go drops, moves the auto-IDs and
  `CreateTableOrView`s first; ported.
- (2026-11-02) A multi-schema change's revertible sub-jobs share the step's
  transaction in Go, so a failed sub-job leaves the earlier ones' writes in
  it; Rust's rebase sub-jobs write their counter in that phase (Go's only
  mark the job non-revertible), so the revertible loop runs in a nested
  transaction that reaches the step only when every sub-job stepped,
  keeping `multi_schema_change_failed_alter_keeps_rebases_*` as it was.
- (2026-11-02) Not yet Go: `convertAddIdxJob2RollbackJob` bumps the version
  (`updateVersionAndTableInfo`) where Rust writes the table alone.

## Decision Log

- (2026-11-02) The job view keeps the step's writes in a sorted map and
  answers them as `BufferMutation`s, so the worker's commit path (and its
  assertions) is unchanged while handlers migrate.
