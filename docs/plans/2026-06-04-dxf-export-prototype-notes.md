# DXF Single-Table Export — Prototype Status & Perf Notes

Companion to `2026-06-04-dxf-single-table-export.md` (the full implementation
plan). This records what the prototype covers, the perf findings from the
10GiB benchmarks, and the items that must not be forgotten at productization.

## Prototype status

Working end-to-end (`EXPORT TABLE db.t TO '<uri>' [FORMAT 'csv'] WITH
thread=, writers_per_encoder=, file_size=, subtask_regions=, detached`):

- parser / planner / executor wiring; DXF task type `Export` with a single
  `Dump` step; region-based split (per add-index's `CalculateRegionBatch`,
  cloud branch); per-writer sub-range bounds fixed at schedule time
  (`SubtaskMeta.WriterBounds`) so retries rewrite identical files.
- per-subtask pipeline: n encoders (n = task concurrency = `thread`,
  default min(8, node CPU)), each serving `writers_per_encoder`(default 2)
  reader/writer pairs; m readers share one
  channel into the encoder, the encoder routes encoded buffers to per-writer
  channels; buffers and chunks recycled via `sync.Pool`.
- CSV encoding via `pkg/format/textrow.AppendValueText`; S3/local output via
  `pkg/objstore` multipart (`PartSize` 8MiB × 4 concurrent per writer).
- per-task Prometheus counters `tidb_export_{rows,bytes,files}_total`
  (label `task_id`); `rate()` gives the live export speed.
- service safepoint at the snapshot TS: set at submit (TTL 10min), kept
  alive by the scheduler, deleted in `OnDone`.
- verified on real TiKV: clustered int PK (global file order across 8
  regions), non-clustered (`_tidb_rowid`), common handle, and a gated 10GiB
  test (`tests/realtikvtest/exporttest`, `EXPORT_LARGE_TEST=1`, optional
  `EXPORT_LARGE_THREAD` / `EXPORT_LARGE_DEST` (S3 URI) /
  `EXPORT_LARGE_CPUPROF`).

Not implemented (see the main plan): job table & SHOW/CANCEL, PostProcess
(schema/metadata files), failure cleanup (partial files remain), parquet,
compression, charset other than utf8mb4, virtual generated columns
(rejected at submit), WHERE filters.

## NextGen adaptation status

Target deployment: submit from a user keyspace, execute on DXF service
(SYSTEM keyspace) tidb-workers, write to S3 only.

| Concern | Status |
| --- | --- |
| Submit/wait from user KS | OK — `handle.SubmitTask` / `WaitTaskDoneOrPaused` go through `storage.GetDXFSvcTaskMgr()`, which returns the SYSTEM-KS task manager on nextgen |
| Target scope | OK — `handle.GetTargetScope()` returns `dxf_service` on nextgen |
| Scheduler store | OK — `scheduler.Param.TaskStore` is the task-keyspace store (framework guarantee) |
| Executor store | OK — the framework resolves the task-keyspace store (#68824), use `BaseTaskExecutor.TaskStore` |
| Region lookup / cop scan with keyspace codec | OK — region cache PD client and distsql client apply the store codec |
| GC barrier per keyspace | OK — `gc.NewManager(pd, store.GetCodec().GetKeyspaceID())` |
| MaxNodeCount | fixed to 1 for now (single executor node); computing it from estimated table size like `CalcMaxNodeCountForImportInto` is the follow-up for auto-scaling |
| SEM / S3 external ID | OK — same checks as IMPORT INTO: deny local destination, require explicit S3 credentials, deny user external ID, inject keyspace external ID on nextgen |
| Not verified on a real nextgen cluster | run a real submit→execute round before benchmarking |

## Perf findings (10GiB / 10M rows × ~1KiB, thread=16 → 32 writers, single tidb + single tikv-slim, 32C host)

Throughput plateaued at ~520–610 MiB/s regardless of writer count or
local-vs-MinIO destination. Block profile: 32 readers spent ~80% of their
time waiting in `distsql.selectResult.Next` — **the single TiKV's scan
delivery is the bottleneck in this setup**, encoders/writers are starved.
On nextgen the read side scales with tikv-workers, so the export node's CPU
efficiency decides whether the NIC can be saturated.

CPU profile of the export window (~108 core-seconds / 9.72GiB ≈ 11 cs/GiB):

| Cost | Share | Note |
| --- | --- | --- |
| SigV4 payload SHA256 + body re-buffering (`checksum.computeStreamChecksum`) | ~51% | **plain-HTTP artifact**: the AWS SDK only signs the payload over non-TLS endpoints (our MinIO test). Over HTTPS S3 it defaults to UNSIGNED-PAYLOAD — this cost disappears on nextgen, replaced by much cheaper AES-NI TLS |
| `appendEscaped` byte-at-a-time | 11% | fixed — bulk segment copy via `IndexByte`, now <1% |
| fresh chunk per `Next` | part of memclr | fixed — chunk pool |
| `httputil.DumpRequestOut` via smithy `RequestResponseLogger` | ~7% | `pkg/objstore/s3store/store.go:71` sets `aws.LogRequest\|LogResponse` unconditionally; the dump cost is paid even when the DEBUG log line is discarded. Gate it on log level |
| post-encoder copy chain | ~10% | see below |

## Productization TODO (do not forget)

1. **Disable SDK request/response logging unless debug** —
   `pkg/objstore/s3store/store.go:71` / `:125`; ~7% CPU for free.
2. **Post-encoder copy chain** (encoder buf → `objectio.BufferedWriter`
   interceptBuffer → io.Pipe → `manager.Uploader` part buffer → TCP):
   - phase 1 (cheap): bypass `BufferedWriter` for already-large writes; the
     pipe doesn't need PartSize-aligned input, `manager.Uploader` re-chunks
     anyway.
   - phase 2: drive multipart ourselves — `CreateMultipartUpload` + N
     in-flight `UploadPart` calls with caller-owned seekable bodies (the
     SDK does not copy a `bytes.Reader` body; see `s3store.multipartWriter`)
     + `CompleteMultipartUpload`. Encoder buffers then reach the socket with
     zero extra copies. Requires a small seekable multi-slice reader for
     retry rewind. Decide after profiling on a real nextgen cluster shows
     CPU is the NIC-saturation limit (~10% CPU at stake).
3. **MaxNodeCount for auto-scaling** — currently fixed at 1 node; estimate
   table size at submit (region sizes, as the design doc's progress section
   already requires) and compute like `CalcMaxNodeCountForImportInto`.
4. TiKV-side read tuning: with scan supply fixed (multiple tikv-workers),
   revisit `channelBufSize`, readers-per-encoder (`writersPerEncoder`), and
   distsql concurrency per sub-range.
5. **File-name field widths** — `<file>` is `%04d`; with a tiny `file_size`
   and a huge per-writer range (>9999 files per writer) lexicographic order
   breaks. Validate/widen at schedule time (the main plan's naming task).
6. **Metric label cardinality** — the `task_id`-labeled export counters are
   never unregistered (IMPORT INTO unregisters per task); fine for tests,
   needs lifecycle management before production.
7. Carry-overs already tracked in the main plan: revert/cleanup, job table,
   PostProcess files, parquet, compression, region-split helper shared with
   add-index (`pkg/ddl/backfilling_dist_scheduler.go:generatePlanForPhysicalTable`).

## Benchmark numbers (for reference only — shared dev box, ±15% noise)

| Run | Dest | thread | Result |
| --- | --- | --- | --- |
| fresh cluster | local disk | 8 (16 writers) | 9.72GiB / 17.7s ≈ 561 MiB/s |
| fresh cluster | local disk | 16 (32 writers) | 16.2s ≈ 613 MiB/s |
| fresh cluster | MinIO S3 (HTTP) | 16 | 16.2s ≈ 613 MiB/s |
| fresh cluster, after escape+chunk-pool fixes | MinIO S3 | 16 | 19.2s ≈ 518 MiB/s (within env noise; CPU-profile-level wins are the reliable signal) |

Load side: 10M rows in ~65–75s through 16 insert sessions.
