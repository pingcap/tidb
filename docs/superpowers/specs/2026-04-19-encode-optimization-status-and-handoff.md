# Encode-Step Optimization — Status & Handoff

**Date:** 2026-04-19 (revised same day after Branch 1 cluster validation)
**Author:** Ruihao Chen (w/ Claude)

This doc captures the current state of the IMPORT INTO encode-step perf
investigation, decisions made so far, work landed in code, and what remains.
Future sessions should start here.

Companion docs (context):
- `2026-04-17-s3-benchmark-and-encode-profiling-design.md` — observability spec
- `2026-04-17-encode-read-path-optimization.md` — read-path design
- `2026-04-17-encode-optimization-experiment-branches.md` — three-branch experiment plan
- `2026-04-19-encode-write-path-optimization.md` — write-path design (Options A and C, detailed)

## Baseline

Observed run: 5000-file sbtest, 650 GiB total, 3 nodes × 7 concurrency = 21 workers,
PK-only schema.

Schema:
```sql
CREATE TABLE test.sbtest (
  id bigint NOT NULL PRIMARY KEY,
  k bigint NOT NULL DEFAULT 0,
  c char(60),
  pad char(30)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

Measurements:
- Encode step wall time: ~32 min
- Per-chunk `takeTime`: bimodal
  - Regime A (~75%): `takeTime ≈ 6 s`, `readDur ≈ 4 s`, `encodeDur ≈ 1.6 s`, `sendDur < 10 ms`
  - Regime B (~25%): `takeTime ≈ 14 s`, `sendDur ≈ 8 s`, `deliverDur ≈ 8 s`
- Per-worker S3 read (single connection): ~34 MiB/s
- Per-worker S3 write (multipart, 950 MiB buffer flush): ~119 MiB/s
- Per-worker CPU (parse+encode combined): ~57 MiB/s
- Per-chunk KV output: ~136 MB (1M rows × ~136 bytes/row, matches schema; no secondary indexes = 1 KV per row, data writer only)

Key architectural facts discovered:
- `dataWriter` buffer is ~950 MiB per worker (= 50% × memory-per-core = 13 GiB/7/2)
- Memory allocation: (N+1) writers × per-writer buffer (where N = # secondary indexes). Scales badly with index count.
- `IndexRouteWriter` (at `pkg/executor/importer/chunk_process.go:710`) routes each index to its own `external.Writer`
- Regime B's 8 s flush stall = dataWriter's 950 MiB buffer being flushed to S3; encode goroutine's `sendFn` blocks because the single deliver goroutine is in the middle of that flush
- `skipMergeSort` threshold = `min(250 × concurrency, 4000)`; with 680 files overlapping and concurrency=7, threshold=1750, 680 ≤ 1750 ⇒ **merge step is skipped** for sbtest
- Ingest subtask count: `ceil(totalBytes / DefaultBatchSize)` per kvGroup, where `DefaultBatchSize = 100 GiB` (`pkg/lightning/config/const.go:49`). For sbtest PK-only 650 GiB: **~7 ingest subtasks**, not hundreds. Ingest is under-parallelized relative to the 21 workers.

## Design consensus

Two independent bottlenecks:

1. **Read-side serialization**: per chunk, the encode goroutine serializes S3 read → parse → encode → send. S3 read is single-connection (~34 MiB/s) and CPU is faster (~57 MiB/s), so CPU stays idle waiting for bytes. Additionally, the sum `readDur + encodeDur` is dominated by read time.

2. **Write-side flush stalls**: dataWriter's 950 MiB buffer fills every ~7 chunks. When it flushes (~8 s to S3), the deliver goroutine is locked and the encode goroutine's `sendFn` blocks. This causes the bimodal `takeTime` distribution (regime B).

### Read-path decision

Enable existing `pkg/util/prefetch.Reader` (async double-buffered, already in tree) for encode-step source reads, AND extend it with parallel range reads when `PrefetchSize > 16 MiB` and the underlying reader is offset-aware.

Rationale:
- Async prefetch alone hides CPU behind read (~7 min saving) but doesn't speed up the 34 MiB/s ceiling.
- Parallel range reads push per-worker throughput toward the pod NIC ceiling (100+ MiB/s), saving another ~8–10 min.
- Total projected: ~13–15 min saving, bringing cluster time from 32 → ~17–22 min.

### Write-path decision

Two options evaluated; consensus is:

**Option A (primary): one unified writer, one physical file per flush, logical kvGroup segments inside**
- Memory: 60% of memory-per-core, split 2 × 30% halves (double-buffered)
- On flush: each kvGroup's accumulated rows sorted independently, segments written consecutively into one physical S3 file
- Stats file format extended: `[kvGroup, physicalFile, startOffset, endOffset, minKey, maxKey, rowCnt]`
- Reader-side refactor required: `MultipleFilesStat`, `RangeSplitter`, `MergeOverlappingFiles[V2]`, ingest, checkpoint all thread offsets
- One multipart upload per flush regardless of N (good multipart efficiency always)
- Estimated engineering cost: 2–3 weeks

**Option C (fallback if reader-side refactor too costly): per-writer double-buffered, parallel deliverer goroutines, SAME memory model as today (no double buffer, no memory growth)**
- Keeps current N+1 writer structure
- Replaces single deliver goroutine with N+1 parallel deliverer goroutines (one per writer)
- Flushes from different writers happen concurrently → eliminates the `max(data_flush + N × index_flush)` serialization
- Per-writer buffer unchanged (same memory as today)
- Reader-side: unchanged
- Caveat: for PK-only (sbtest), this is essentially a no-op since there are no index writers to parallelize with data
- Estimated engineering cost: 3–5 days

Discarded alternatives:
- **Option B** (separate data and index pools): user identified that Option C is a cleaner formulation with independent per-index writers instead of a shared index pool
- **Shared chunk pool across kvGroups** (generic Proposal 3): over-engineered relative to Option B/C
- **Per-writer double-buffering at today's memory**: doesn't scale — for 16 indexes, per-writer buffer halves to ~28 MiB, below S3 multipart `PartSize` (32 MiB), losing write efficiency

### Three-branch experiment plan (superseded — see "Decision" below)

Originally planned:
- **Branch 1**: read-side only (`PrefetchSize = 32 MiB` + parallel range reads)
- **Branch 2**: Branch 1 + write Option A
- **Branch 3**: Branch 1 + write Option C

Ablation comparisons were to decide whether Option A's reader-side refactor is worth the engineering cost versus Option C's simpler parallel-deliverer fix.

Projected cluster wall time for 5000-file sbtest:
- Today: 32 min
- Branch 1: ~21–22 min
- Branch 3 (Branch 1 + Option C): ~18–20 min on sbtest (Option C doesn't help PK-only)
- Branch 2 (Branch 1 + Option A): ~14–15 min

### Decision (2026-04-19, after Branch 1 validation)

Going straight to **Option A**, skipping Branch 3 as a separate experiment.

Reasoning:
- Branch 1 cluster measurement showed +35% throughput (333 → 450 MiB/s) and regime-A chunks dropped to ~3.5 s, but regime-B send stalls (9–10 s periodic) persist and now dominate the remaining gap to the ~15 min projection.
- Option C is a no-op for PK-only workloads like sbtest, which is our dominant test workload. Shipping Option C and then still needing Option A to hit the sbtest number adds engineering churn without accelerating the answer.
- Option A's reader-side refactor is a one-time structural investment; committing now is cheaper than committing after a partially-satisfying Option C ship.

See `2026-04-19-encode-write-path-optimization.md` for the full Option A / Option C spec.

## What's landed in code

### Part A + B (observability) — PR #67860 (branch `feature/encode-step-timers`)

- Encode-step fine-grained histograms: `tidb_lightning_chunk_process_operation_seconds{operation=...}` with labels `s3_read`, `decompress`, `parse`, `encode`, `send`, plus existing `write_data`, `write_index`, `deliver`
- Per-chunk `encode subtask done` INFO log line with breakdown
- `ADMIN BENCHMARK S3 READ|WRITE 'url' [WITH (...)]` SQL statement wrapping existing `tools/objstore-perf` core into `pkg/objstore/perfbench`
- Parser grammar, planner, executor wiring

### Branch 1 read-path — commit `46fd45466d` on `joechenrh:feature/encode-step-timers`

Pushed 2026-04-19. Files changed (12 total):

| File | Change |
|---|---|
| `pkg/lightning/mydump/parser.go` | `OpenReader` takes new `prefetchSize int` param; plumbs into `ReaderOption.PrefetchSize` |
| `pkg/executor/importer/import.go` | Added `DefaultEncodeReadPrefetchSize = 32 MiB` constant; encode path passes it |
| `pkg/executor/importer/table_import.go` | Second encode call site passes the constant |
| `pkg/executor/importer/sampler.go` | Passes `0` (sampler unchanged) |
| `lightning/pkg/importer/chunk_process.go`, `get_pre_info.go` | Pass `0` (legacy lightning unchanged) |
| `pkg/util/prefetch/parallel_reader.go` | **new**: `ParallelReader` + `RangeOpener` type. Bounded reorder buffer, in-order delivery, parallel range fetching. |
| `pkg/util/prefetch/parallel_reader_test.go` | **new**: 7 unit tests (in-order delivery under jitter, non-aligned tail, small files < 1 block, error propagation, close cancellation, bounded outstanding fetches). All pass. |
| `pkg/objstore/s3like/store.go` | Constants `parallelPrefetchThreshold=16MiB`, `parallelPrefetchBlockSize=8MiB`, `parallelPrefetchMaxConcurrency=8`. New `newParallelRangeReader` helper. `Open` dispatches to parallel when `PrefetchSize > 16 MiB` AND `RangeSize > 8 MiB`. |
| `pkg/objstore/s3like/io.go` | Same dispatch in the Seek-reopen path |
| `pkg/objstore/gcs.go` | Same constants/helper, same dispatch in `Read` (lazy-open) and `Seek` paths |
| `pkg/objstore/s3store/ks3.go` | Same constants/helper, same dispatch in `Open` and `Seek` paths |

Commit message:
```
*: enable parallel range prefetch for IMPORT INTO encode-step reads

Branch 1 of the encode-step optimization plan: reduce per-worker S3 read
time by issuing concurrent range GETs instead of a single sequential
connection. Encode CPU work also hides behind the async prefetch buffer.
```

### Activation semantics

- `PrefetchSize = 0` (default everywhere except encode): raw reader, no prefetch, no change
- `0 < PrefetchSize ≤ 16 MiB`: existing sequential `prefetch.NewReader` double-buffered async
- `PrefetchSize > 16 MiB` AND range > 8 MiB: new `ParallelReader` with `concurrency = min(PrefetchSize/8MiB, 8)`

At `DefaultEncodeReadPrefetchSize = 32 MiB`: 4 parallel 8 MiB range GETs per encode source file.

### Retry path deliberately left on sequential prefetch

In s3like and ks3, the `Read()` retry path (after a transient read error) re-opens the underlying reader and wraps with sequential `prefetch.NewReader`. This is intentional — retries are rare and the parallel-reader re-instantiation would add complexity without material benefit. Only `Open()` and `Seek()` paths use parallel.

## Open items and known risks

### HIGH PRIORITY — Compression guard

The current parallel-reader code in `s3like`, `ks3`, `gcs` does NOT check whether the source is compressed. If `PrefetchSize > 16 MiB` is passed and the file is zstd/gzip, the parallel ranges will feed out-of-order bytes to a decompressor that requires sequential input, corrupting the stream.

**For sbtest (uncompressed CSV) this is safe.** For case-ux-7 / case-csv-7 zstd workloads it will break.

**Current mitigation**: `objstore.WithCompression.Open` is the wrapper that adds decompression; it calls the raw backend's `Open` with `ReaderOption`. If we want compressed sources to fall back to sequential, the check must happen either:
- Inside the storage backend (harder — it doesn't know if the caller wraps with decompression)
- At the `mydump.OpenReader` level (set `prefetchSize = min(prefetchSize, threshold)` when `fileMeta.Compression != CompressionNone`)

**Proposed fix** (to land before testing beyond sbtest):
In `pkg/lightning/mydump/parser.go` `OpenReader`:
```go
if fileMeta.Compression != CompressionNone && prefetchSize > parallelPrefetchThreshold {
    prefetchSize = parallelPrefetchThreshold
}
```
Clamps to below the parallel threshold so only sequential double-buffered prefetch is used.

Where to source `parallelPrefetchThreshold`: either export from `s3like` or mirror as a constant in mydump. Mirroring is simpler.

### ~~MEDIUM PRIORITY — Write-path design doc not yet written~~ DONE 2026-04-19

Written: `docs/superpowers/specs/2026-04-19-encode-write-path-optimization.md`. Covers Option A's unified writer + stats V2 + 7-file reader-side refactor, and Option C's fan-out deliverer + errgroup lifecycle. Decision flipped to A-first after Branch 1 cluster validation (see "Decision" section above).

**Next**: write the Option A implementation plan at `docs/superpowers/plans/2026-04-19-encode-write-path-option-a.md`.

### MEDIUM PRIORITY — Reader-path integration test

Branch 1 has unit tests for `ParallelReader` but no end-to-end test that runs the encode pipeline with `PrefetchSize > 0` and verifies correct row output. To add under `pkg/executor/importer/chunk_process_testkit_test.go` or similar.

### LOW PRIORITY — ingest under-parallelization

Independent finding from the `generateWriteIngestSpecs` read: `DefaultBatchSize = 100 GiB` per ingest subtask. For 650 GiB, this yields ~7 ingest subtasks to spread across 21 workers — most workers idle during ingest. Tuning this knob could meaningfully reduce ingest wall time but is out of scope for the encode-step plan. File separately if/when measured.

### LOW PRIORITY — AWS SDK connection pool

21 workers × 4 parallel ranges = up to 84 concurrent S3 GETs per cluster. Default SDK pool is usually 100+, but verify under load. If the pool saturates, the parallel reader will serialize anyway (workers block on connection).

### LOW PRIORITY — Bazel regeneration

Branch 1 added new Go files (`pkg/util/prefetch/parallel_reader.go`, `parallel_reader_test.go`) and a new top-level test function. Per `AGENTS.md` Quick Decision Matrix, `make bazel_prepare` is required before the PR ships. Not run yet — pending live-cluster validation first.

## Branch 1 cluster validation (2026-04-19, post-image-build)

Ran image built from commit `46fd45466d` on the nextgen test cluster with 5000-file sbtest.

Observed:
- **Cluster throughput: ~450 MiB/s** (baseline ~333 MiB/s, +35%)
- `s3_read` per chunk: ~1.0–1.7 s (baseline ~3.7 s) — parallel prefetch working
- Regime-A chunks: `takeTime ≈ 3.5 s` (baseline ~6 s) — consistent with projection
- **Regime-B chunks still present**: periodic `sendDur ≈ 9–10 s`, `takeTime ≈ 13 s` — write-path untouched, as expected
- File size confirmed 136 MB per chunk (1M rows × 136 bytes)

Interpretation:
- Read-side projection was accurate. Remaining gap to ~15 min projection is entirely write-side.
- The persistent regime B on this PK-only workload rules out Option C as a sufficient fix (C is a no-op for N=0). Going to Option A.

## User's next action

Write the Option A implementation plan (see "Pointers for picking up" below).

## Conversation summary — how we got here

1. Started with perf motivation: user tried go pprof-driven optimizations on encode CPU (especially sorting), saw no end-to-end wall-time improvement. Hypothesis: S3 I/O is the hidden cost.

2. Designed observability (Part A: `ADMIN BENCHMARK S3` statement; Part B: fine-grained encode-step timers). Landed PR #67860.

3. User ran benchmarks + live IMPORT INTO, reported:
   - `ADMIN BENCHMARK S3 READ` at 256 MiB objects, 8 workers: 256 MiB/s aggregate (~32 MiB/s per worker counted, ~39 MiB/s raw traffic via prefetch overcount)
   - `ADMIN BENCHMARK S3 WRITE` at 256 MiB, 4 workers: 102 MiB/s aggregate (~25 MiB/s per worker — misleadingly low because single-PUT per object, 4 parts only)
   - Real IMPORT INTO encode subtask log: `readDur≈3.7s, encodeDur≈1.6s, parseDur≈0.6s, s3_read_pct≈61%`

4. Analysis iteration: initial estimate of ~69 MiB/s per worker was based on wrong file-size assumption (256 MiB). User corrected: files are 125 MB each (not 256 MiB). Real per-worker rate: 34 MiB/s. This matched the gateway benchmark closely.

5. Ran 5000-file full import: 32 min total. From logs, bimodal distribution observed. Deeper analysis via full log dump:
   - Regime A (normal chunks, `takeTime ~6s`): `sendDur < 10 ms`
   - Regime B (slow chunks, `takeTime ~14s`): `sendDur ~8s`, `deliverDur ~8s`
   - Interpretation evolved: initially thought regime B was generic delivery backpressure; user clarified buffer size is 950 MiB = half of memory-per-core. Correctly re-interpreted as periodic dataWriter flush stalls.

6. Discussed write-path architectures:
   - Multi-round debate over Options A (one writer, logical segments), B (separate data+index pools), C (per-writer parallel deliverers)
   - User converged: Option A primary + Option C fallback
   - Rejected Option B (user correctly noted it's equivalent to per-writer independence with extra scheduler)

7. User challenged: "does 680 files with 10 indexes cost the same to read as 1 file with 10 logical segments?" — answer: approximately yes at S3 layer; no read-side penalty for Option A; the real cost of Option A is reader-side code migration.

8. User asked about `generateMergeSortSpecs` and `generateWriteIngestSpecs` — read the code, corrected my earlier estimate. Found: merge step is skipped for sbtest; ingest creates ~7 subtasks for 650 GiB (not hundreds). Ingest under-parallelization noted.

9. Wrote three-branch experiment plan (read-only; read+A; read+C) for ablation.

10. User directed: implement Branch 1 with `PrefetchSize = 32 MiB`. Started implementation.

11. User challenged: "read speed < encode speed, so just PrefetchSize won't work." Correctly noted that Change 1 (enable async prefetch) hides CPU (~7 min saving) but doesn't speed up reads. Change 2 (parallel ranges) is the real lever (~10 min additional).

12. Implemented full Branch 1: new `prefetch.ParallelReader`, integration into s3like / ks3 / gcs Open and Seek paths. All unit tests pass. Pushed to `joechenrh:feature/encode-step-timers` as commit `46fd45466d`.

13. User is now building an image.

## Pointers for picking up

- Current branch: `feature/encode-step-timers` at `joechenrh/tidb` and local `worktrees/encode-timers/`
- Leftover worktree: `worktrees/encode-read-prefetch/` (branch `feature/encode-read-prefetch`) — can be removed; its changes were cherry-picked to `feature/encode-step-timers`
- Next priority (functional): add compression guard in `mydump.OpenReader` before testing beyond sbtest
- Next priority (planning): write `docs/superpowers/plans/2026-04-19-encode-write-path-option-a.md` — task-by-task implementation plan for Option A, derived from the spec
- Next priority (implementation): execute the Option A plan (unified writer + stats V2 + reader-side refactor across `split.go`, `merge.go`, `merge_v2.go`, `byte_reader.go`, `reader.go`, `planner.go`, checkpoints)
