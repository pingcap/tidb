# DXF Single-Table Export — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `EXPORT TABLE db.t TO 's3://…'` — a distributed single-table export that runs as a DXF task, writing PK/handle-ordered SQL/CSV/parquet files to an object store, byte-compatible with Dumpling output.

**Architecture:** A new DXF task type `Export` (`pkg/dxf/export`) modeled on `pkg/dxf/importinto`: a `Dump` step splits the table into key-ordered spans (reusing backfill's region split) and runs a per-subtask read→encode→upload pipeline (cop scan at a fixed TSO snapshot → `DumpTextRow`/`dumpformat` encode → object-store multipart writer, cutting files at `FileSize`); a `PostProcess` step writes schema + metadata files; a revert step cleans the bucket synchronously on failure/cancel. A user-facing job lives in `mysql.tidb_export_jobs`; the statement/`SHOW`/`CANCEL` wiring mirrors IMPORT INTO.

**Tech Stack:** Go, TiDB DXF framework (`pkg/dxf/framework`), coprocessor/distsql (`pkg/store/copr`), `pkg/objstore` (S3 multipart), `pkg/parser`, `pkg/server/internal/column` (DumpTextRow), Dumpling (`dumpling/export`).

**Design reference:** Feishu — *Distributed Single-Table Export on DXF — Design* (`wiki/Kuf0wT7QKiZQTTkZY8xcC39tn8e`).

---

## Milestone overview (implement in order; each is a PR-sized, testable unit)

- **M0 — Formatting foundations (the blocker).** Extract `DumpTextRow` value formatting out of `pkg/server/internal/*` into a shared package; create `pkg/dumpformat` (CSV/SQL framing reused by Dumpling and the exporter). *Nothing else compiles against the exporter until this lands.*
- **M1 — Statement + job table.** `mysql.tidb_export_jobs`; `EXPORT TABLE` / `SHOW EXPORT JOB[S]` / `CANCEL EXPORT JOB` parser + executor wiring; validation; submit/poll/detached.
- **M2 — DXF task: split + dump pipeline.** Register `Export` task type; `Dump` step splitting (reuse backfill); subtask read→encode→write pipeline; file naming; GC barrier.
- **M3 — PostProcess + revert + job progress.** Schema/metadata files; synchronous revert cleanup; progress aggregation into the job row; `SHOW EXPORT JOB` 15 columns.
- **M4 — Parquet encoder.** Separate columnar encoder (type→schema mapping, row-group `FileSize` cutting).
- **M5 — Metrics + e2e tests.** Grafana metrics; real-TiKV round-trip via IMPORT INTO.

Each milestone's tasks below are TDD bite-sized where the logic is self-contained (naming, encoding, cleanup, splitting). Where a task threads through an existing TiDB subsystem (parser grammar, scheduler interface), it names the **exact reference implementation to copy** and the **exact verification** — these are not placeholders; follow the cited pattern.

---

## File structure (created / modified)

**M0**
- Create `pkg/format/textrow/` (or `pkg/util/format`): the extracted text-protocol value serializer (`DumpTextRow`, `ResultEncoder`, `AppendFormatFloat`, length-encode helpers) — importable by both `pkg/server` and `pkg/dxf/export`.
- Modify `pkg/server/internal/column/column.go:151` → delegate to the new package (keep the server's API; move the body).
- Create `pkg/dumpformat/` — `FileFormat` enum, CSV/SQL framing+escaping, output-file naming, compression glue, a chunk-backed `RowSource`.
- Modify `dumpling/export/writer_util.go` / `sql_type.go` → re-use `pkg/dumpformat` (keep Dumpling behavior).

**M1**
- Modify `pkg/meta/metadef/system_tables_def.go` (add `CreateTiDBExportJobsTable`), `pkg/meta/metadef/system.go` (reserve `TiDBExportJobsTableID`), `pkg/session/bootstrap.go` (register table), `pkg/session/upgrade_def.go` (upgrade step).
- Modify `pkg/parser/ast/dml.go` (AST: `ExportTableStmt`, `ShowExportJobs`, `ExportJobActionStmt`), `pkg/parser/parser.y` + regen.
- Create `pkg/executor/export/` — submit/validate/poll executor + `SHOW`/`CANCEL` executors + job-table accessors (`pkg/dxf/export/job.go`, mirroring `pkg/dxf/importinto/job.go`).

**M2–M3**
- Create `pkg/dxf/export/`: `register.go`, `proto.go` (task/subtask meta), `scheduler.go`, `task_executor.go`, `subtask_executor.go`, `clean_up.go`, `pipeline.go`, `naming.go`.

**M4** — Create `pkg/dumpformat/parquet.go` (or `pkg/dxf/export/parquet.go`).
**M5** — `metrics/grafana/*.json` additions; `tests/realtikvtest/exporttest/`.

> After any added/moved/renamed Go file or new `TestXxx`, run `make bazel_prepare` and include the Bazel metadata (AGENTS.md Quick Decision Matrix).

---

## M0 — Formatting foundations

### Task 0.1: Extract the text-protocol value serializer into a shared package

**Why first / two problems:** (a) `DumpTextRow` (`column.go:151`) and `ResultEncoder` (`result_encoder.go:66`) live in `pkg/server/internal`, so `pkg/dxf/export` can't import them; (b) **`DumpTextRow` emits length-encoded *protocol* bytes** (each value prefixed by a length-encoded int, NULL = `0xfb`), but the exporter needs the **raw per-value text** to then apply CSV/SQL escaping. So we extract the **per-value formatter** (the type switch that produces one value's bytes), refactor the server's `DumpTextRow` to wrap it with length-encoding, and let the exporter / `dumpformat` call the per-value formatter directly.

**Files:**
- Create: `pkg/format/textrow/textrow.go` (and `result_encoder.go`, `dump_helpers.go`)
- Modify: `pkg/server/internal/column/column.go` (delegate `DumpTextRow` to the new package; keep the `Info`/`ResultEncoder` server-facing API or re-export)
- Test: `pkg/format/textrow/textrow_test.go`

- [ ] **Step 1 — Write the failing differential test.** Two assertions over a mixed-type `chunk.Row` (built via `chunk.MutRowFromDatums`): (a) the extracted per-value formatter `AppendValueText(dst, row, i, col, enc)` produces, for every non-null type, the bytes that sit *inside* `DumpTextRow`'s length-encoding; (b) the refactored server `DumpTextRow` (now built on `AppendValueText`) is byte-identical to a golden of the pre-refactor output. Cover the divergence-prone types: float (`AppendFormatFloat`), datetime fsp, decimal, unsigned, NULL, enum/set, charset.

```go
// pkg/format/textrow/textrow_test.go (package textrow_test)
func TestAppendValueTextMatchesProtocolInner(t *testing.T) {
    cols, row := buildMixedTypeRow(t) // int, unsigned bigint, decimal(20,4), datetime(fsp=3),
                                      // varchar w/ escape chars, enum, set, json, blob, NULL
    enc := textrow.NewResultEncoder(charset.CharsetUTF8MB4)
    for i, col := range cols {
        if row.IsNull(i) { continue } // exporter maps NULL itself; DumpTextRow writes 0xfb
        got := textrow.AppendValueText(nil, row, i, col, enc)
        require.Equal(t, protocolInnerValue(t, cols, row, i), got) // value inside the length-encoding
    }
}

func TestDumpTextRowUnchangedAfterRefactor(t *testing.T) {
    cols, row := buildMixedTypeRow(t)
    require.Equal(t, goldenPreRefactor(t, cols, row), mustDumpTextRow(t, cols, row))
}
```

- [ ] **Step 2 — Run, verify it fails** (`go test ./pkg/format/textrow/ -run AppendValueText`): FAIL — `AppendValueText` not defined.
- [ ] **Step 3 — Extract the per-value formatter and refactor.** Pull the per-type byte production out of `DumpTextRow`'s loop into `AppendValueText(dst []byte, row chunk.Row, i int, col *Info, enc *ResultEncoder) []byte` (one value, charset-encoded via `enc`, **no length prefix**, NULL handled by the caller). Move `AppendValueText`, `Info` (the fields it reads: `Type/Flag/Decimal/Charset`), `ResultEncoder`, and `AppendFormatFloat` (`pkg/server/internal/util/util.go:159`) into `pkg/format/textrow`. Rewrite the server `DumpTextRow` as: for each col, `IsNull → append 0xfb`, else `dump.LengthEncodedString(buf, AppendValueText(...))` — so it still emits identical protocol bytes.
- [ ] **Step 4 — Re-point the server.** In `pkg/server/internal/column/column.go`, make `DumpTextRow` call `textrow.DumpTextRow` (and `Info`/`ResultEncoder` alias the shared types). Run the existing server tests: `go test ./pkg/server/... -run TestDumpText` — Expected: PASS (no behavior change).
- [ ] **Step 5 — Run the differential test:** PASS.
- [ ] **Step 6 — `make bazel_prepare`; commit.** `git commit -m "format: extract text-protocol value serializer into pkg/format/textrow"`

### Task 0.2: `pkg/dumpformat` — CSV/SQL framing reused by Dumpling and the exporter

**Files:**
- Create: `pkg/dumpformat/format.go` (`FileFormat` enum, `RowSource` interface over `chunk.Row`), `csv.go`, `sql.go`, `naming.go`, `compress.go`
- Modify: `dumpling/export/writer_util.go`, `sql_type.go` to call `pkg/dumpformat` for the framing/escaping
- Test: `pkg/dumpformat/csv_test.go`, `sql_test.go`, `naming_test.go`

- [ ] **Step 1 — Failing test for the file name.** Encodes the `<db>.<table>.<ordinal><writer><file>.<ext>[.<suffix>]` scheme with fixed widths; asserts lexicographic order == (ordinal, writer, file) order and that compression adds the suffix.

```go
func TestFileNameSortsByOrdinalWriterFile(t *testing.T) {
    n := dumpformat.FileName{DB: "mydb", Table: "orders", Ordinal: 1, Writer: 2, File: 3, Ext: "csv", Compress: dumpformat.Zstd}
    require.Equal(t, "mydb.orders.0000010020003.csv.zst", n.String())
    require.Less(t, dumpformat.FileName{Ordinal:1,Writer:2,File:3}.String(),
                   dumpformat.FileName{Ordinal:1,Writer:2,File:4}.String())
    require.Less(t, dumpformat.FileName{Ordinal:1,Writer:9,File:0}.String(),
                   dumpformat.FileName{Ordinal:2,Writer:0,File:0}.String())
}
```

- [ ] **Step 2 — Run, fail.** `go test ./pkg/dumpformat/ -run FileName` — FAIL.
- [ ] **Step 3 — Implement `FileName.String()`** with widths `Ordinal:%07d`, `Writer:%03d`, `File:%04d` (sized in Task 2.2's notes), plus `Ext` and optional compression suffix.
- [ ] **Step 4 — Run, pass.**
- [ ] **Step 5 — CSV/SQL writers over `chunk.Row`.** A `RowSource` yields `chunk.Row` + `[]*textrow.Info`; the CSV writer emits each non-null value via `textrow.AppendValueText` then applies Dumpling's CSV escaping (`dumpling/export/sql_type.go` `escapeCSV`), and writes the CSV null token for nulls; the SQL writer emits `INSERT` framing (quote+escape strings, `NULL` for nulls). Test: encode a chunk row to CSV, assert byte-identity vs the Dumpling path for the same values (incl. a NULL).
- [ ] **Step 6 — Re-point Dumpling** to `pkg/dumpformat` for framing; run dumpling unit tests: `go test ./dumpling/export/ -run TestWrite` — Expected: PASS (unchanged output).
- [ ] **Step 7 — `make bazel_prepare`; commit.** `git commit -m "dumpformat: extract CSV/SQL framing shared by dumpling and export"`

---

## M1 — Statement + job table

### Task 1.1: `mysql.tidb_export_jobs` system table

**Files:** Modify `pkg/meta/metadef/system_tables_def.go` (add `CreateTiDBExportJobsTable`, mirroring `CreateTiDBImportJobsTable` at `:681`), `pkg/meta/metadef/system.go` (reserve `TiDBExportJobsTableID = ReservedGlobalIDUpperBound - <next free>`), `pkg/session/bootstrap.go` (register, like `:320`), `pkg/session/upgrade_def.go` (new bootstrap version + `mustExecute(s, metadef.CreateTiDBExportJobsTable)`).

- [ ] **Step 1 — Test:** bootstrap a test store, `SELECT` from `mysql.tidb_export_jobs`, assert columns exist (`id, create/start/update/end_time, table_schema, table_name, table_id, destination, format, created_by, parameters, total_size, status, step, summary, error_message`).
- [ ] **Step 2 — Run, fail** (table missing).
- [ ] **Step 3 — Add the DDL + table id + bootstrap + upgrade step** (copy `tidb_import_jobs` and adapt columns; add `destination`, `format`, `total_size`).
- [ ] **Step 4 — Run, pass.**
- [ ] **Step 5 — `make bazel_prepare`; commit.**

### Task 1.2: Parser — `EXPORT TABLE … TO … WITH (…)`

**Files:** Modify `pkg/parser/ast/dml.go` (`ExportTableStmt` AST, model on `ImportIntoStmt`), `pkg/parser/parser.y` (grammar), then `make parser`. Test: `pkg/parser/parser_test.go`.

- [ ] **Step 1 — Parser test:** `EXPORT TABLE db.t TO 's3://b/p' WITH (FORMAT='csv', FILE_SIZE='256MiB', COMPRESSION='zstd', DETACHED)` parses to an `ExportTableStmt` with the right table, URI, and options; restore round-trips.
- [ ] **Step 2 — Run, fail.**
- [ ] **Step 3 — Add AST + grammar** following `ImportIntoStmt` (the `WITH (...)` option list parsing is identical; reuse it). `make parser` to regenerate.
- [ ] **Step 4 — Run, pass.**
- [ ] **Step 5 — `make parser_unit_test`; `make bazel_prepare`; commit.**

### Task 1.3: `SHOW EXPORT JOB[S]` / `CANCEL EXPORT JOB <id>`

**Files:** Modify `pkg/parser/ast/dml.go` (`ShowExportJobs` flag + `ExportJobID`, like `ShowImportJobs` at `:3148`), `parser.y`; regen. Test in `parser_test.go`.

- [ ] Steps mirror Task 1.2: failing parser test for the three forms → grammar → regen → pass → commit.

### Task 1.4: EXPORT executor (validate, snapshot, job row, submit/poll) + SHOW/CANCEL executors

**Files:** Create `pkg/executor/export/export.go` (the `EXPORT` exec), `show_export.go`, `cancel_export.go`; create `pkg/dxf/export/job.go` (insert/update/get on `tidb_export_jobs`, mirroring `pkg/dxf/importinto/job.go`). Wire into `pkg/executor/builder.go`.

- [ ] **Step 1 — Validation test:** EXPORT on a view → error; bad `FORMAT` → error; missing credentials in URI → error. (No DXF submit yet; stub the submit.)
- [ ] **Step 2 — Run, fail.**
- [ ] **Step 3 — Implement:** validate (base table via infoschema, options), resolve snapshot TS (oracle), insert a `tidb_export_jobs` row, then submit `handle.SubmitTask("Export", …)` (returns task id; store on the job row). `DETACHED` → return job id; else poll the job row to terminal and return the summary. `SHOW EXPORT JOB[S]` reads the job table; `CANCEL` sets the job to canceling and cancels the DXF task.
- [ ] **Step 4 — Run, pass.** Commit (with `make bazel_prepare`).

---

## M2 — DXF task: split + dump pipeline

### Task 2.1: Register the `Export` task type + scheduler skeleton

**Files:** Create `pkg/dxf/export/register.go` (`RegisterSchedulerFactory`/`RegisterTaskType`/`RegisterSchedulerCleanUpFactory`, like `importinto`), `scheduler.go` (steps `Dump`, `PostProcess`; `GetNextStep`; `OnDone` handling `reverting`), `proto.go` (task meta = table, dest, format, snapshot TS; subtask meta = key range, `Ordinal` carries naming, file-index base). Reference: `pkg/dxf/importinto/scheduler.go`.

- [ ] **Step 1 — Test:** `GetNextStep` returns `StepInit → Dump → PostProcess → StepDone`. Marshal/unmarshal subtask meta round-trips.
- [ ] **Step 2-4 — fail → implement (copy importinto skeleton, two steps) → pass.**
- [ ] **Step 5 — `make bazel_prepare`; commit.**

### Task 2.2: `Dump` step splitting — reuse backfill, emit spans in key order

**Files:** `pkg/dxf/export/scheduler.go` (`OnNextSubtasksBatch` for `Dump`). Reference: `pkg/ddl/backfilling_dist_scheduler.go:301` `generateReadIndexPlan` (region load + **continuity check** + `CalculateRegionBatch`).

- [ ] **Step 1 — Test (determinism + continuity + order):** given a stubbed continuous region set over `[start,end]`, the produced subtask metas (a) cover `[start,end]` with no gap/overlap, (b) are in key order, (c) are deterministic across two calls. Given a region set with a gap, splitting errors `"regions are not continuous"`.
- [ ] **Step 2 — Run, fail.**
- [ ] **Step 3 — Implement:** `getTableRange(snapshotVer)` for `[start,end]`; `regionCache.LoadRegionsInKeyRange`; **sort + continuity check (keep it — do not drop)**; group into coarse spans (`CalculateRegionBatch`); emit metas in key order so the framework's `Ordinal = i+1` follows the handle (scheduler.go:567). Set field widths: `Ordinal` 7 digits, `Writer` 3, `File` 4 — assert `#spans < 10^7`, `Rw ≤ 999`, files/writer `< 10^4` given the span size; widen if a check trips. Partitioned: iterate physical partitions, emit partition-then-handle.
- [ ] **Step 4 — Run, pass; commit.**

### Task 2.3: Subtask executor — read → encode → write pipeline

**Files:** Create `pkg/dxf/export/task_executor.go`, `subtask_executor.go`, `pipeline.go`, `naming.go`. Reference: backfill `pkg/ddl/backfilling_operators.go` (cop `buildTableScan` over a key range, `pkg/dxf/operator` pools), `pkg/objstore` multipart writer (`objstore.Create` with `Concurrency>1, PartSize`).

- [ ] **Step 1 — Determinism test (idempotent retry):** run a subtask twice over the same in-memory rows at the same fake snapshot ts + same `FileSize`; assert identical file names and identical bytes (same cut points). This guards the "retry overwrites" invariant.
- [ ] **Step 2 — Run, fail.**
- [ ] **Step 3 — Implement the pipeline:**
  - divide the subtask span into sub-ranges (one per writer; `writer_id` = sub-range ordinal, fixed at subtask start — *not* the live pool size);
  - per sub-range: cop scan `[start,end)` in handle order at the **fixed snapshot `start_ts`** (`buildTableScan(..., snapshotTS, start, end, ...)` — region splits mid-read are handled by the cop client, no special code);
  - encode rows via `pkg/dumpformat` (CSV/SQL via `textrow`) ;
  - stream to the object-store multipart writer (`objstore.Create(Concurrency=N, PartSize=P)`), cutting a new file at `FileSize` on a row/complete-statement boundary, `file_id` from 0;
  - name files via `dumpformat.FileName{Ordinal, Writer, File, …}`.
  - pools: encode = `#cores`; `Rr`/`Rw` from `GetResource().CPU.Capacity()` tunable via `ResourceModified`.
- [ ] **Step 4 — Run, pass.**
- [ ] **Step 5 — `make bazel_prepare`; commit.**

### Task 2.4: GC barrier for the task lifetime

**Files:** `pkg/dxf/export/scheduler.go` (acquire on first Dump batch / task start; TTL-refresh goroutine; release in `OnDone`). Reference: Dumpling `runGCProtectionUpdater` (`dumpling/export/dump.go:1842`) — use the keyspace `SetGCBarrier` / service safepoint via the PD client; `protectTS = snapshotTS-1`, TTL 5m, refresh every TTL/2.

- [ ] **Step 1 — Test:** on task start a barrier is registered at the snapshot ts; on `OnDone` it is released; a refresh fires within TTL/2 (use a fake clock/PD).
- [ ] **Steps 2-5 — fail → implement → pass → commit.**

---

## M3 — PostProcess + revert + progress

### Task 3.1: `PostProcess` — schema + metadata files (Dumpling layout)

**Files:** `pkg/dxf/export/task_executor.go` (PostProcess StepExecutor). Reference: Dumpling schema dump + `dumpling/export/metadata.go:53` (`Started/Finished dump at`, `SHOW MASTER STATUS:` with the TSO).

- [ ] **Step 1 — Test:** PostProcess writes `<db>-schema-create.sql`, `<db>.<table>-schema.sql` (from infoschema **at the snapshot TS**), and a `metadata` file whose bytes match Dumpling's format (timestamps + `SHOW MASTER STATUS` TSO).
- [ ] **Steps 2-5 — fail → implement (one subtask, runs after all Dump subtasks succeed) → pass → commit.**

### Task 3.2: Revert — synchronous bucket cleanup in `OnDone`

> **Framework constraint (verified):** DXF does **not** dispatch subtasks during `reverting`. `BaseScheduler.onReverting` (`pkg/disttask/framework/scheduler/scheduler.go:332-358`) only *cancels* the existing subtasks, waits for them to drain, then calls the single-node `OnDone(ctx, h, task)` and finally `RevertedTask`. So `reverted` is reached **after** `OnDone` returns — cleanup placed in `OnDone` is synchronous-before-rollback, but it is **single-node**, not a per-writer fan-out. This mirrors IMPORT INTO, which cleans up in `OnDone → failJob/cancelJob` (`pkg/disttask/importinto/scheduler.go:369-389`), not via revert subtasks.

**Files:** `pkg/dxf/export/scheduler.go` (`OnDone`: when `task.Error != nil`, prefix-sweep the destination before returning), `clean_up.go`. Reference: `pkg/dxf/importinto/clean_up.go` (prefix sweep) + `objstore` `WalkDir`/`DeleteFiles` + `s3store` `ListMultipartUploads`/`AbortMultipartUpload`.

- [ ] **Step 1 — Test:** after a forced Dump failure, `OnDone` deletes all `<db>.<table>.*` objects under the destination and aborts incomplete multipart uploads (via `ListMultipartUploads` over the prefix); the task reaches `reverted` only after the sweep completes (assert via a mock object store: bucket empty before `RevertedTask`).
- [ ] **Steps 2-5 — fail → implement the single-node prefix sweep in `OnDone` (list `<db>.<table>.*` → `DeleteFiles`; list+abort lingering multiparts). Additionally, each Dump `StepExecutor.Cleanup` best-effort aborts the multipart upload it currently holds when its subtask is cancelled (per-node teardown of the *existing* subtask, not a new revert subtask) → pass → commit.** Add an S3 lifecycle-rule note (doc/runbook) as the backstop for node-death-mid-sweep.

### Task 3.3: Progress aggregation + `SHOW EXPORT JOB` columns

**Files:** `pkg/dxf/export/scheduler.go` (`OnTick`/`OnNextSubtasksBatch` aggregate subtask summaries into `tidb_export_jobs.summary`), `pkg/executor/export/show_export.go` (the 15 columns).

- [ ] **Step 1 — Test:** as Dump subtasks report rows/bytes/files (via the framework summary), the job row's `summary` reflects the totals; `SHOW EXPORT JOB <id>` returns exactly the 15 columns (Job_ID, Source_Table, Table_ID, Destination, Phase, Status, Total_Size, Exported_Size, Exported_Rows, Exported_Files, Result_Message, Create/Start/End_Time, Created_By).
- [ ] **Steps 2-5 — fail → implement → pass → commit.**

---

## M4 — Parquet encoder

### Task 4.1: Parquet columnar encoder

**Files:** Create `pkg/dumpformat/parquet.go`. Note (from design): parquet has **no** `DumpTextRow`/Dumpling output/existing writer to reuse — it's new. Map TiDB column types → a parquet schema, batch rows into row groups with per-column compression, and cut a new parquet **file** at `FileSize` on a **row-group boundary** (never mid-row-group). The byte-identical rule does **not** apply (no Dumpling parquet to match).

- [ ] **Step 1 — Test:** encode a chunk of mixed types to parquet; read it back (existing `mydump` parquet reader) and assert the rows round-trip; assert a new file starts at a row-group boundary when `FileSize` is crossed.
- [ ] **Steps 2-5 — fail → implement (choose the parquet lib already vendored; map types) → pass → commit.**

---

## M5 — Metrics + e2e

### Task 5.1: Metrics

**Files:** add export counters/gauges (rows/bytes/files exported, per-stage throughput, subtask states) and a Grafana panel JSON. Reference IMPORT INTO's metering/metrics.

- [ ] Add metrics + a Grafana panel; commit. (Resource metering for next-gen billing reuses IMPORT INTO's `SendRowAndSizeMeterData` path — wire it in `clean_up`/`OnDone` for succeeded tasks.)

### Task 5.2: Real-TiKV round-trip e2e

**Files:** `tests/realtikvtest/exporttest/export_test.go`. Per AGENTS.md RealTiKV flow (start playground → run → cleanup).

- [ ] **Step 1 — Test:** create a table with mixed types (clustered int PK, then a non-clustered case), `EXPORT TABLE … TO 's3://…'` (local/minio), assert: files sort by name into handle order, no overlap, and **`IMPORT INTO` the exported files reproduces the table** (row count + checksum). For CSV/SQL, also assert byte-identity vs a `dumpling` run of the same table.
- [ ] **Step 2-3 — run via the realtikv runner; commit.**

---

## Self-review — spec coverage

| Design section | Task(s) |
|---|---|
| Statement, options, credentials, validation | 1.2, 1.4 |
| Configuration (FORMAT/FILE_SIZE/COMPRESSION/SNAPSHOT/DETACHED) | 1.2, 2.3, 4.1 |
| Job lifecycle, `tidb_export_jobs`, SHOW/CANCEL, progress | 1.1, 1.3, 1.4, 3.3 |
| Output data & invariants (handle order, disjoint, monotonic name) | 2.2 (ordinal in key order), 2.3 (per-writer cut), 0.2 (naming) |
| Data format (byte-identical CSV/SQL via DumpTextRow + charset) | 0.1, 0.2, 2.3, 5.2 |
| Task model (Export type, Dump/PostProcess, no merge-sort, GC) | 2.1, 2.2, 2.4, 3.1 |
| Range splitting (reuse backfill, continuity, partitioned) | 2.2 |
| Subtask pipeline (read/encode/upload pools, multipart writer) | 2.3 |
| Errors/idempotency/cleanup (revert, abort multiparts) | 2.3 (idempotent), 3.2 |
| Parquet | 4.1 |
| Auto-scaling (formula TBD post-benchmark) | *deferred — sizing constants from M5 benchmark; not blocking* |
| Metrics | 5.1 |
| Testing | 0.x/1.x/2.x unit + 5.2 e2e |

**Open items carried from the design (not blocking M0–M3):** the cleanup-policy question (kept open in the doc); the auto-scaling formula (constants after benchmark); metering detail (reuse IMPORT INTO). These are noted at their tasks, not silently dropped.
