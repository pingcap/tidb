# IMPORT INTO Encode Write-Path Optimization Design

**Status:** Design
**Date:** 2026-04-19
**Author:** Ruihao Chen

Companion docs:
- `2026-04-17-s3-benchmark-and-encode-profiling-design.md` — observability
- `2026-04-17-encode-read-path-optimization.md` — read-path design
- `2026-04-17-encode-optimization-experiment-branches.md` — three-branch plan (high-level write-side comparison)
- `2026-04-19-encode-optimization-status-and-handoff.md` — current state

## Motivation

Branch 1 (read-path) is live. First cluster measurement (sbtest, 5000 files × 136 MB, 3 × 7 workers):

- Encode throughput rose from ~333 MiB/s to ~450 MiB/s (+35%)
- Regime-A chunks now ~3.5 s (down from ~6 s); `s3_read` ~1.0–1.7 s (down from ~4 s)
- **Regime-B chunks still appear**: `sendDur ≈ 9–10 s` periodically, `takeTime ≈ 13 s`

Read-side is no longer the dominant bottleneck. The remaining gap to the ~15 min projection is the periodic ~8–10 s send stalls. Root cause: the dataWriter buffer (~950 MiB per worker for sbtest) fills every ~7 chunks; when it flushes, the single deliverer goroutine blocks the encode pipeline via the `sendFn` channel.

This spec covers the two write-side designs discussed and pre-agreed (Option A primary, Option C fallback) at the level of detail needed to implement either one.

## Current architecture (baseline)

### Pipeline shape

```
encode goroutine → sendFn → kvBatch chan → deliverLoop (single goroutine)
                                              ├─ dataWriter.AppendRows(data KVs)
                                              └─ IndexRouteWriter.AppendRows(index KVs by indexID)
                                                    └─ per-index external.Writer.WriteRow (N writers)
```

### Key call sites

| Symbol | Location |
|---|---|
| `dataDeliver` struct | `pkg/executor/importer/chunk_process.go:485` |
| `sendEncodedData` (=sendFn) | `pkg/executor/importer/chunk_process.go:499` |
| `deliverLoop` | `pkg/executor/importer/chunk_process.go:508` |
| `IndexRouteWriter` struct | `pkg/executor/importer/chunk_process.go:645` |
| `IndexRouteWriter.AppendRows` | `pkg/executor/importer/chunk_process.go:661` |
| `external.Writer` struct | `pkg/lightning/backend/external/writer.go:427` |
| `Writer.WriteRow` | `pkg/lightning/backend/external/writer.go:467` |
| `Writer.flushKVs` | `pkg/lightning/backend/external/writer.go:559` |
| `MultipleFilesStat` | `pkg/lightning/backend/external/writer.go:357` |
| Memory formula (`getWriterMemorySizeLimit`) | `pkg/dxf/importinto/encode_and_sort_operator.go:200` |
| WriterBuilder construction | `pkg/dxf/importinto/encode_and_sort_operator.go:128` |

### Memory sizing formula (today)

In `encode_and_sort_operator.go:200–217`:

```go
indexKVGroupCnt := GetNumOfIndexGenKV(plan.DesiredTableInfo)  // N
memPerCore      := resource.MemoryPerCore()
memPerShare     := memPerCore * 0.5 / (indexKVGroupCnt + 3)
dataKVMemSize   := memPerShare * 3
perIndexMemSize := memPerShare * 1
```

Shape:
- data writer gets 3 shares; each of N index writers gets 1 share
- Total writer memory per worker = `0.5 × memPerCore`
- For sbtest PK-only (N=0): data = 50% of memPerCore ≈ 950 MiB per worker (matches observed)
- For N=16 indexes: data ≈ `3/19 × 50%` ≈ 7.9% ≈ 150 MiB; each index ≈ 50 MiB

### What "single deliverer" means functionally

`deliverLoop` drains `kvBatch` with one goroutine. Per batch:

1. Calls `dataWriter.AppendRows(dataKVs)` — may trigger `flushKVs` (S3 multipart upload, ~8 s for 950 MiB)
2. Then calls `indexWriter.AppendRows(indexKVs)` — may trigger per-index flushes (sequential inside IndexRouteWriter loop)
3. Returns to `kvBatch` receive

While step 1 or 2 is flushing, the encode goroutine's `sendFn` blocks on the unbuffered (or saturated) `kvBatch` channel. This is the send stall observed in regime B.

### Stats file format (current)

`MultipleFilesStat` (writer.go:357–364):

```go
type MultipleFilesStat struct {
    MinKey            tidbkv.Key  `json:"min-key"`
    MaxKey            tidbkv.Key  `json:"max-key"`
    Filenames         [][2]string `json:"filenames"`  // [dataFile, statFile]
    MaxOverlappingNum int64       `json:"max-overlapping-num"`
}
```

Each flush appends one `[dataFile, statFile]` pair. Every 500 flushes, `build()` aggregates into a new `MultipleFilesStat` entry. Readers (`split.go`, `merge.go`) treat each `Filenames` entry as a whole file with its own key range.

## Option A — Unified writer, one physical file per flush

### Architecture

Replace `dataWriter + IndexRouteWriter` with a single unified writer:

```
encode goroutine → sendFn → kvBatch chan → deliverLoop (single goroutine)
                                              └─ unifiedWriter.AppendRows(dataKVs, indexKVs map)
                                                    └─ 2 × 30% of memPerCore, ping-pong halves
```

Memory:
- Total writer memory = `0.6 × memPerCore` (bumped from 0.5 to buy the second half)
- Split = `2 × 0.3 × memPerCore` (two halves for double-buffering)
- For sbtest at 13 GiB/7 cores: each half ≈ 560 MiB
- For N=16 indexes: no change in per-kvGroup share — both halves serve all kvGroups; there is no per-kvGroup pre-allocation

On flush (one half at a time):

1. Separate accumulated KVs by kvGroup (data group + N index groups)
2. Sort each kvGroup's slice independently (parallelizable across kvGroups if CPU budget allows)
3. Open one S3 multipart upload for one physical file
4. Append each sorted kvGroup as a consecutive byte range in the file
5. Emit one stat file recording per-segment offsets + per-segment key range
6. Swap halves; deliverLoop continues consuming into the other half while flush runs in a goroutine

Critical invariant: the front-buffer half is always available for writes, so `deliverLoop` (and therefore encode's `sendFn`) does not block on flush. This is what eliminates regime B.

### Writer type

New file `pkg/lightning/backend/external/unified_writer.go`:

```go
type UnifiedWriter struct {
    // Two halves; front receives writes, back is either idle or flushing
    halves   [2]*writerHalf
    frontIdx atomic.Int32
    flushCh  chan int  // send back-half index when ready to flush
    flushWG  sync.WaitGroup

    // Ordering state — see "Segment ordering" below
    nextPhysicalFileSeq atomic.Int64

    // Output
    store       storage.ExternalStorage
    prefix      string
    memSizeHalf uint64

    // Stats
    multiFileStats []MultipleFilesStatV2
}

type writerHalf struct {
    kvBuffer *membuf.Buffer          // sized to memSizeHalf
    // Per-kvGroup locations within kvBuffer. groupID 0 = data; 1..N = secondary indexes.
    locations map[int64][]membuf.SliceLocation
    sizeByGroup map[int64]int64
    totalSize uint64
}
```

Key methods:

- `AppendRows(ctx, groupID int64, kvs)`: writes into `halves[frontIdx].kvBuffer`, records location under `groupID`. On buffer-full → trigger flush (send `frontIdx` to `flushCh`, atomically swap `frontIdx`). Returns without waiting — backpressure is handled by `flushWG`'s capacity (see below).
- `FlushLoop`: a single background goroutine consumes `flushCh`, sorts per-kvGroup, writes one multipart S3 upload, emits one stat file, clears the half and makes it reusable. Two halves means at most one flush in flight; if a second flush is queued while the first is running, encode must wait (but this is the "both buffers full" edge case — rare if sizing is correct).

### Extended stats format

`pkg/lightning/backend/external/writer.go` (new type):

```go
// MultipleFilesStatV2 extends MultipleFilesStat with per-segment info.
// Format versioning: a new byte at stat-file start distinguishes V1 from V2.
type MultipleFilesStatV2 struct {
    MinKey            tidbkv.Key         `json:"min-key"`
    MaxKey            tidbkv.Key         `json:"max-key"`
    Segments          []KVGroupSegment   `json:"segments"`
    MaxOverlappingNum int64              `json:"max-overlapping-num"`
}

type KVGroupSegment struct {
    KVGroupID   int64      `json:"kv-group"`       // 0 = data, >0 = indexID
    DataFile    string     `json:"data-file"`      // physical file (shared across segments in one flush)
    StatFile    string     `json:"stat-file"`      // physical stat file (shared across segments)
    StartOffset int64      `json:"start-offset"`   // byte range of this segment inside DataFile
    EndOffset   int64      `json:"end-offset"`
    StatStart   int64      `json:"stat-start"`     // byte range inside StatFile
    StatEnd     int64      `json:"stat-end"`
    MinKey      tidbkv.Key `json:"min-key"`
    MaxKey      tidbkv.Key `json:"max-key"`
    RowCount    int64      `json:"row-count"`
}
```

Backwards-compat: a format-version byte at the head of the stat blob. V1 readers refuse V2 and error out with a clear message — we do NOT attempt cross-version reads, because the change is an offline migration tied to a schema-version bump.

### Reader-side refactor

The following call sites all treat `Filenames [][2]string` as "opaque file, read from byte 0". They must all thread per-segment offsets:

| File | Change |
|---|---|
| `pkg/lightning/backend/external/split.go:122` (`NewRangeSplitter`) | Accept `[]KVGroupSegment` per kvGroup. `MergePropIter` iterates across segments, not files. |
| `pkg/lightning/backend/external/split.go:183` (`SplitOneRangesGroup`) | Returns `(exhaustedSegments, activeSegments)` — same data structure swap. |
| `pkg/lightning/backend/external/merge.go:181` (`MergeOverlappingFiles`) | Accept `[]SegmentRef{Path, StartOffset, EndOffset}` instead of `paths []string`. |
| `pkg/lightning/backend/external/merge_v2.go` (`MergeOverlappingFilesV2`) | Same signature change. |
| `pkg/lightning/backend/external/byte_reader.go:85` (`openStoreReaderAndSeek`) | Add `endOffset` parameter; reader reports EOF when hitting endOffset. |
| `pkg/lightning/backend/external/reader.go` (KV readers) | Pass segment bounds down. |
| `pkg/dxf/importinto/planner.go` (`generateWriteIngestSpecs`) | Build `WriteIngestStepMeta` with segment refs instead of filenames. |
| Ingest subtask executor | Open file at segment offset via byte range; existing `concurrentReader` already supports start offset. |
| `pkg/lightning/checkpoints/*` | Checkpoint records segment refs instead of filenames. Format version bump. |

`byteReader` already partially supports range via `initFileOffset` (byte_reader.go:85). Adding an `endOffset` is straightforward: add a field, have the reader return `io.EOF` when `curBufOffset + consumed >= endOffset`. The storage-layer GET can also be bounded by setting HTTP Range end — minor efficiency win for readers that only want a prefix of the physical file.

### Flush concurrency model

Writer-side:
- At most two halves in flight (one receiving writes, one flushing)
- If encode produces faster than flush completes, `AppendRows` blocks when front-half fills and back-half is still flushing — this is the throttle, not a surprise stall
- Sizing guidance: `memSizeHalf ≥ (encode_rate × expected_flush_duration)`. With encode ~57 MiB/s/worker and ~5 s flush for 560 MiB: `57 × 5 = 285 MiB < 560 MiB`. Safe margin.

Within a flush:
- Sorting per kvGroup can run in parallel if worker CPU is idle; serial is also acceptable (sort cost ~200 ms for 560 MiB is negligible vs 5 s upload)
- S3 multipart upload is naturally parallel (up to `PartSize=32 MiB` parts in flight) regardless of segment count — one physical file = one multipart session with good per-part concurrency

### Files produced

Per worker: `totalBytes / 560 MiB` physical files. For sbtest 650 GiB / 21 workers ≈ 31 GiB per worker → ~55 files per worker → ~1160 files cluster-wide (vs ~680 today). Still far below the `skipMergeSort` threshold (`min(250 × 7, 4000) = 1750`), so merge-step behavior unchanged.

For high-N workloads: file count is unchanged (segment count grows, but those are logical, not separate S3 objects).

### Engineering cost

~2–3 focused weeks including:

- UnifiedWriter + FlushLoop: ~3–4 days
- Stats V2 format + encoder/decoder: ~2 days
- Reader-side refactor across 7 files: ~5–7 days
- Checkpoint format bump: ~2 days
- Tests (correctness + roundtrip + ingest integration): ~3–4 days
- Cluster validation + perf regressions: ~2 days

## Option C — Parallel deliverer goroutines

### Architecture

Keep N+1 writers unchanged. Replace the single `deliverLoop` with a fan-out + per-writer deliverer goroutines:

```
encode goroutine → sendFn → kvBatch chan → fanOutLoop (1 goroutine)
                                              ├─ dataCh    (buffered 8) → dataDrainLoop   → dataWriter
                                              ├─ idx[0]Ch  (buffered 8) → idx[0]DrainLoop → writers[indexID₀]
                                              ├─ ...
                                              └─ idx[N-1]Ch                              → writers[indexID_{N-1}]
```

Each drain goroutine owns its writer's `AppendRows` calls. When a writer flushes, only its own drain goroutine blocks; other writers continue receiving.

### Implementation

In `pkg/executor/importer/chunk_process.go`:

```go
type dataDeliver struct {
    // existing fields ...
    kvBatch       chan *encodedKVGroupBatch
    dataCh        chan []common.KvPair
    indexChannels map[int64]chan []common.KvPair   // keyed by indexID
    errGroup      *errgroup.Group
    // ...
}

func (p *dataDeliver) fanOutLoop(ctx context.Context) error {
    defer func() {
        close(p.dataCh)
        for _, ch := range p.indexChannels {
            close(ch)
        }
    }()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case batch, ok := <-p.kvBatch:
            if !ok {
                return nil  // encode done
            }
            // fan out
            p.dataCh <- batch.dataKVs   // blocks only this writer's path
            for indexID, kvs := range batch.indexKVs {
                ch, ok := p.indexChannels[indexID]
                if !ok {
                    ch = make(chan []common.KvPair, 8)
                    p.indexChannels[indexID] = ch
                    p.errGroup.Go(func() error {
                        return p.drainIndexLoop(ctx, indexID, ch)
                    })
                }
                ch <- kvs
            }
            batch.reset()
        }
    }
}

func (p *dataDeliver) drainDataLoop(ctx context.Context, ch <-chan []common.KvPair) error {
    for kvs := range ch {
        if err := p.dataWriter.AppendRows(ctx, kvs, ...); err != nil {
            return err
        }
    }
    return p.dataWriter.Close(ctx)
}

func (p *dataDeliver) drainIndexLoop(ctx context.Context, indexID int64, ch <-chan []common.KvPair) error {
    writer := p.writerFactory(indexID)  // same factory as IndexRouteWriter today
    defer writer.Close(ctx)
    for kvs := range ch {
        for _, kv := range kvs {
            if err := writer.WriteRow(ctx, kv.Key, kv.Val, nil); err != nil {
                return err
            }
        }
    }
    return nil
}
```

The existing `IndexRouteWriter` becomes redundant; it can be deleted or simplified into a factory function.

### Lifecycle and error propagation

- `errGroup` has context tied to the subtask
- Any drainLoop returns error → `errGroup.Wait` returns that error → subtask fails
- On encode side: when encode finishes, `close(kvBatch)` signals `fanOutLoop` to return, which closes per-writer channels; each drainLoop sees `range ch` exit, calls `Close` on its writer (which emits final stats file), returns nil
- On cancellation: context.Done fires in all goroutines; each returns `ctx.Err`; errGroup surfaces the first

Channel buffer size: 8 per sub-channel. Each `kvBatch` is ~96 KiB (per encoded batch size today), so 8 batches = ~768 KiB — small enough that backpressure is responsive, large enough to absorb ~100 ms of encode burst.

### What C does and does NOT fix

**Fixes**: when data is flushing AND indexes need to flush (or vice versa), their flushes run in parallel instead of serially. For an N-index table, per-flush-cycle wall time drops from `sum(data_flush + Σ index_flushes)` to `max(data_flush, max_index_flush)`.

**Does NOT fix**: the data writer's own flush stall. When `dataWriter` is mid-flush (950 MiB / ~8 s), `dataDrainLoop` is blocked inside `AppendRows`, so `dataCh` backs up to 8 batches and then `fanOutLoop` blocks on `dataCh <-`, which blocks `kvBatch` receive, which blocks `sendFn`. Regime B persists for PK-only workloads.

For sbtest (N=0): essentially a no-op. The only benefit is that N=0 means there's no index serialization to worry about — which was already the case.

For N≥1 workloads: benefit grows with N, with diminishing returns once indexes parallelize beyond the write-pipe bandwidth of the pod NIC.

### Files to touch

Implementation changes are confined to one file:

| File | Change |
|---|---|
| `pkg/executor/importer/chunk_process.go` | Rewrite `deliverLoop` as `fanOutLoop` + drain goroutines. Remove or simplify `IndexRouteWriter`. |

Test changes:
- `pkg/executor/importer/chunk_process_testkit_test.go` — add fan-out error propagation tests, lifecycle ordering tests

### Reader-side: no changes

Same N+1 physical files, same `MultipleFilesStat` per writer, same stat-file format. `RangeSplitter`, `MergeOverlappingFiles`, ingest, and checkpoint are all untouched.

### Engineering cost

~3–5 days:
- Core rewrite: ~2 days
- Error/lifecycle tests: ~1 day
- Cluster validation: ~1 day

## Decision criteria

Run Branch 3 (Branch 1 + Option C) first on a representative multi-index workload (N ≥ 3). Measure:

- Cluster encode wall time
- `tidb_lightning_chunk_process_operation_seconds{operation="send"}` P50 and P99

Then decide:

1. **If Branch 3 wall time on multi-index ≈ Branch 2 projection (~15 min)** → ship Branch 3. Defer Option A indefinitely. The reader-side refactor's risk and cost aren't justified.

2. **If Branch 3 wall time noticeably exceeds Branch 2 projection** (gap > ~3 min) → commit to Option A. Regime-B on the data writer is unavoidable with the per-writer-buffer model, and only double-buffering eliminates it.

3. **If Branch 3 doesn't help sbtest at all** (expected, N=0) but the team already plans a regime where sbtest is the dominant workload → skip Option C entirely, go straight to Option A.

For the near term: Option C is the recommended next implementation. It ships in days, not weeks, and it covers the realistic multi-index case. Option A is a strategic refactor to hold in reserve based on measured gaps.

## Out of scope

- `DefaultBatchSize = 100 GiB` (ingest subtask sizing) — tracked separately; not a write-path issue
- `skipMergeSort` threshold changes
- Cross-writer sort-merge optimizations (partitioning input ranges across writers)
- Object-store-specific multipart tuning (PartSize, MaxInflight) — already tuned

## Risks

### Option A

- **Stats format migration**: a mixed cluster (some TiDB nodes running V1, some V2) must be avoided during upgrade. Mitigation: the format-version byte refuses cross-version reads loudly, and rolling upgrade docs note that encode subtasks produce the newer format only after all nodes are on the new binary.
- **Reader-side correctness**: 7 touch-points, each with edge cases (empty segment, cross-segment merges, segment at file-end). Mitigation: high unit-test coverage per file + integration test with synthetic multi-segment stats.
- **Ingest opening segment offsets**: current ingest already uses `concurrentReader` with `initFileOffset`; endOffset support is a small extension but must be correct to avoid reading across into the next segment.
- **Performance regression on low-N workloads**: 60% vs 50% memory-per-core means slightly less memory for the encode goroutine's scratch. Should be negligible but measure.

### Option C

- **Error propagation**: N+1 goroutines → first error must cancel all others via errgroup context. Straightforward with Go's `errgroup`, but tests must include mid-flush failures in each goroutine type.
- **Channel sizing**: 8 batches per sub-channel is a guess. If encode bursts > 8 batches before draining, encode blocks on the fan-out. Measure on real workload; bump to 16 or 32 if needed.
- **Backpressure fairness**: if one index writer is much slower than others (e.g., a wide index), that writer's channel fills first and backs up the whole fan-out. Not a bug per se — it's correct backpressure — but worth characterizing.
- **Writer lifecycle ordering**: on success, each drain goroutine must call `Close` exactly once after its channel drains. On error, `Close` still needs to run to avoid leaked multipart uploads. Test both paths.

## Open questions

1. **Can Option C be extended with per-writer double-buffering cheaply?** (i.e., C+half-of-A). Each writer gets its own 2-half ping-pong at today's per-writer size. Eliminates data-side regime B without the reader-side refactor. Memory cost: 2× per-writer sizes, which hits the per-index-halves-to-28-MiB problem at N=16. Only sensible at N ≤ 4. Worth prototyping as "Option C++" if Branch 3 measurement is promising but shows residual regime B.

2. **Does Option A make secondary-index sort parallelization easier?** A single writer with typed kvGroups could sort all N+1 groups in parallel using a worker pool within the flush goroutine. Potential ~2× speedup on sort portion for high-N workloads. Defer — not a primary motivator for Option A.

3. **Should stats format V2 also inline per-segment overlap stats across segments of the same kvGroup?** Today's `MaxOverlappingNum` is per-`MultipleFilesStat` entry (across ~500 files). With segments, the right granularity is likely per-kvGroup-per-file. This affects `skipMergeSort` heuristics. Keep today's granularity to minimize scope; revisit if merge-step behavior regresses.

## Appendix — code snippets referenced

### Current `deliverLoop` shape (to replace in Option C)

```go
// pkg/executor/importer/chunk_process.go:508
func (p *dataDeliver) deliverLoop(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case batch, ok := <-p.kvBatch:
            if !ok {
                return nil
            }
            if err := p.dataWriter.AppendRows(ctx, batch.dataKVs, ...); err != nil {
                return err
            }
            if err := p.indexWriter.AppendRows(ctx, batch.indexKVs, ...); err != nil {
                return err
            }
            batch.reset()
        }
    }
}
```

### Current memory formula (to replace in Option A)

```go
// pkg/dxf/importinto/encode_and_sort_operator.go:200
func getWriterMemorySizeLimit(plan *importer.Plan) (data uint64, idx uint64) {
    indexKVGroupCnt := GetNumOfIndexGenKV(plan.DesiredTableInfo)
    memPerCore := resource.MemoryPerCore()
    memPerShare := uint64(float64(memPerCore) * 0.5 / float64(indexKVGroupCnt+3))
    return memPerShare * 3, memPerShare * 1
}
// Option A replacement:
//   memPerHalf := uint64(float64(memPerCore) * 0.3)
//   return memPerHalf, memPerHalf  // both halves carry all kvGroups
```
