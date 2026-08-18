# Drive IMPORT INTO global-sort encode to its workload-specific throughput ceiling

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. Maintain this plan according to that file. Repository policy in `AGENTS.md` and operational commands in `docs/agents/testing-flow.md` take precedence when they are stricter.

## Purpose / Big Picture

`IMPORT INTO` with global sort currently leaves throughput on the table for two independently observed reasons. A source reader normally consumes one object-store stream at a time, and a global-sort writer stops accepting encoded KVs while a full in-memory batch is sorted and uploaded. After those waits are removed, row parsing, SQL-to-KV encoding, checksumming, routing, copying, and sorting may become the limit, especially for tables with many indexes or expensive expressions.

The completed work will make the encode step behave like a balanced pipeline. Each source reader will be able to issue ordered parallel range reads, each encode writer will continue accepting KVs while the preceding batch is flushed, and later CPU changes will be selected from profiles rather than guesses. The goal is not an unconditional 400 MiB/s per pod. The goal is for each representative workload to run close to the smallest measured capacity of source reading, CPU encoding, global-sort writing, and pod networking, without changing SQL semantics or the global-sort file contract.

Users will observe success in `IMPORT INTO` job duration, source rows per second, source and emitted-KV MiB/s, low encode `send` wait, and stable CPU/network utilization. A PK-only table, a moderate-index table, and a sixteen-index table will intentionally have different ceilings.

## Progress

- [x] (2026-08-18) Reconstructed the current encode pipeline, existing metrics, reader behavior, writer memory formula, and historical cluster measurements.
- [x] (2026-08-18) Wrote the initial self-contained execution and cluster-validation plan.
- [x] (2026-08-18) Selected `upstream/master` at `454d7010a4` and created a clean dedicated worktree without carrying unrelated CSV parser changes into the performance series.
- [ ] Milestone 0: establish reproducible workload manifests, stage-level observability, baseline ceilings, and baseline profiles.
- [ ] Milestone 1: implement ordered parallel 8 MiB source range reads and pass local correctness tests.
- [ ] Milestone 1 cluster gate: measure reader-only behavior on uncompressed and compressed inputs and record the result here.
- [ ] Milestone 2: implement opt-in asynchronous ping-pong flushing for IMPORT INTO writers while preserving the existing external-file format.
- [ ] Milestone 2 cluster gate: measure reader plus writer behavior on PK-only, moderate-index, and sixteen-index workloads and record the result here.
- [ ] Milestone 3: capture post-I/O CPU/heap/block profiles, rank encoding costs, and complete only profile-justified CPU optimizations.
- [ ] Run Ready-profile validation for each production PR and the final RealTiKV global-sort regression set.
- [ ] Complete the final workload matrix, calculate achieved efficiency against each measured ceiling, and write the retrospective.

## Surprises & Discoveries

- Observation: the current encode source path passes a nil `storeapi.ReaderOption`, so the prefetch support already present in object-store backends is disabled for this path.
  Evidence: `pkg/lightning/mydump/parser.go`, function `OpenReader`, calls `Open(..., nil)` for compressed and uncompressed files.

- Observation: prior experimental evidence already validates ordered parallel range reads as a useful direction. Four 8 MiB ranges per reader increased a three-node cluster from about 333 MiB/s to about 450 MiB/s, reduced per-chunk raw S3 read time from about 3.7 seconds to 1.0-1.7 seconds, and left periodic writer-side `send` stalls intact.
  Evidence: `docs/superpowers/specs/2026-04-19-encode-optimization-status-and-handoff.md`, section `Branch 1 cluster validation`.

- Observation: for the measured 7-core, roughly 13 GiB worker, the PK-only data writer owns about 950 MiB and synchronously sorts and uploads it when full. The encode queue then fills and `sendFn` waits for about 8-10 seconds.
  Evidence: `pkg/dxf/importinto/task_executor.go` sets writer memory from `MemoryPerCore`; `pkg/dxf/importinto/encode_and_sort_operator.go` assigns 50% of memory per core to writers; `pkg/ingestor/simplesst/writer.go` runs `flushKVs` synchronously.

- Observation: total writer memory does not grow linearly with index count. The fixed 50% writer budget is divided into three shares for data and one share for each generated index KV group. More indexes make each writer buffer smaller and can hurt multipart/file efficiency, but the formula keeps the aggregate planned buffer budget approximately fixed.
  Evidence: `getWriterMemorySizeLimit` in `pkg/dxf/importinto/encode_and_sort_operator.go` divides by `indexKVGroupCnt + 3`.

- Observation: the current batch writer uses a 5 MiB minimum multipart part size, not the 32 MiB assumption found in an older design note. A half-buffer for a narrow index can therefore still form several upload parts, but high-index concurrency must be bounded.
  Evidence: `MinUploadPartSize` and `createStorageWriter` in `pkg/ingestor/simplesst/writer.go`.

- Observation: `simplesst.Writer` is also used by DDL global sort and merge code. Changing its default scheduling semantics would enlarge the blast radius beyond IMPORT INTO.
  Evidence: callers include `pkg/ddl/backfilling_operators.go`, `pkg/ingestor/globalsort/merge.go`, and `pkg/dxf/importinto/encode_and_sort_operator.go`.

- Observation: the present worktree contains unrelated user changes in `pkg/lightning/mydump/csv_parser.go`, `pkg/lightning/mydump/csv_parser_test.go`, and `pkg/lightning/mydump/parser.go`, plus unrelated untracked files. Implementation must start in a clean worktree from an explicitly agreed base rather than editing over those files.
  Evidence: `git status --short` on 2026-08-18.

## Decision Log

- Decision: define performance success relative to a workload-specific measured ceiling instead of 400 MiB/s per pod.
  Rationale: encode consumes ingress and egress simultaneously, emitted KV size depends on schema and index count, and complex schemas have lower CPU ceilings. A fixed source-byte target would reward or penalize workloads for the wrong reason.
  Date/Author: 2026-08-18 / Codex and user.

- Decision: treat the documented AWS EC2 network bandwidth as available independently and simultaneously in each direction; do not spend a cluster experiment deciding whether ingress and egress share one bucket.
  Rationale: the AWS EC2 network-bandwidth documentation explicitly says an instance with a specified bandwidth can use that bandwidth for inbound traffic and simultaneously for outbound traffic, and that burstable instances have separate inbound and outbound network-credit buckets. M0 still observes receive/transmit rates and ENA allowance-exceeded counters to detect instance, CNI, connection, or platform constraints, but those observations are not used to reinterpret the documented directionality. Reference: `https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html`.
  Date/Author: 2026-08-18 / user correction, verified by Codex against AWS documentation.

- Decision: implement reader concurrency as ordered 8 MiB range reads, initially four ranges per active reader, with a maximum of eight and a process-level in-flight range cap.
  Rationale: 8 MiB has prior positive cluster evidence; four ranges use about 32 MiB of prefetch memory per reader and 28 range requests for seven encode workers, which is enough to test saturation without starting at excessive concurrency.
  Date/Author: 2026-08-18 / Codex and user.

- Decision: preserve byte order before decompression, so compressed input is supported by the same parallel raw-range reader rather than being categorically disabled.
  Rationale: gzip/zstd decoders require an ordered compressed byte stream, not a single HTTP connection. Parallel ranges are valid when exact bytes are reassembled by offset before reaching the decoder. Tests must prove this for supported compression formats.
  Date/Author: 2026-08-18 / Codex.

- Decision: prototype writer overlap by splitting the existing writer budget into two equal 25% halves, not by adding an extra 20% allocation on top of the existing 50% budget.
  Rationale: two equal halves keep total planned writer memory unchanged, simplify repeated swaps, produce consistent file sizes, and shorten each flush. The measured rates suggest a roughly 475 MiB half takes materially less time to flush than the other half takes to fill. A 30%/20% split remains a tuning experiment only if cluster data disproves the symmetric choice.
  Date/Author: 2026-08-18 / Codex after comparing 50%+20%, 30%/20%, and 25%/25% variants.

- Decision: make asynchronous flush opt-in for IMPORT INTO, while leaving existing `simplesst.Writer` callers synchronous by default.
  Rationale: DDL and merge paths also use this writer. An opt-in builder mode preserves compatibility and lets unit tests compare the old and new scheduling behavior over an identical file format.
  Date/Author: 2026-08-18 / Codex.

- Decision: preserve the current per-KV-group data/stat file format in the first writer prototype.
  Rationale: the performance problem can be tested without introducing logical segments into shared files or changing range splitting, merge, ingest, checkpoints, keyspace encoding, and duplicate handling together. A unified physical-file format is a fallback only if the simpler writer cannot reach the measured ceiling because of object count or small-part inefficiency.
  Date/Author: 2026-08-18 / Codex.

- Decision: optimize encoding CPU only after reader and writer waits are independently measured, but create reproducible CPU benchmarks and profiles in Milestone 0.
  Rationale: profiling infrastructure is useful immediately; changing hot code before I/O stalls are removed can improve a microbenchmark without affecting wall time. Each CPU change must retain its own ablation result.
  Date/Author: 2026-08-18 / Codex and user.

- Decision: use one immutable observability baseline derived from a pinned master SHA; do not require a noisy cluster comparison against a bare-master image.
  Rationale: repeated runs of the same image can differ by more than 2%, so a fixed 2% observability-overhead gate would not be statistically meaningful without many expensive repetitions. Every reader, writer, and CPU experiment retains identical observability code, so that cost cancels in the ablation. A bare master image is an optional historical anchor only. Milestone 1, Milestone 2, and every retained CPU optimization still use separate immutable image digests so each comparison changes one behavior variable.
  Date/Author: 2026-08-18 / revised after user correction.

## Outcomes & Retrospective

No production milestone has been implemented yet. The initial outcome is a staged plan with two explicit design gates: first prove ordered reader concurrency, then prove format-preserving asynchronous writer flushing. Update this section after every cluster gate with achieved throughput, limiting stage, regressions, and whether the next milestone remains justified.

## Context and Orientation

The distributed execution framework package `pkg/dxf/importinto` owns the global-sort encode subtask. `importStepExecutor.Init` in `pkg/dxf/importinto/task_executor.go` sets encode worker concurrency to the assigned CPU capacity. On the target worker this is seven workers, even though the Kubernetes pod request is nominally eight cores. `newEncodeAndSortOperator` in `pkg/dxf/importinto/encode_and_sort_operator.go` creates one persistent `chunkWorker` per concurrency slot. Each worker owns a data writer and lazily-created per-index writers.

For every source chunk, `importMinimalTaskExecutor.Run` in `pkg/dxf/importinto/subtask_executor.go` calls `importer.ProcessChunkWithWriter`. In `pkg/executor/importer/chunk_process.go`, a chunk uses two goroutines. `chunkEncoder.encodeLoop` reads and parses a row, encodes it through `TableKVEncoder`, accumulates about 96 KiB or 4096 rows, calculates per-KV-group checksums, and sends an `encodedKVGroupBatch`. `dataDeliver.deliverLoop` receives the batch, appends data KVs, then index KVs. Read and encode operations inside `encodeLoop` are sequential; only the encoder and deliver loops overlap.

`pkg/lightning/mydump/parser.go` opens the source object. Object backends such as `pkg/objstore/s3like/store.go`, `pkg/objstore/gcs.go`, and `pkg/objstore/s3store/ks3.go` already understand `ReaderOption.PrefetchSize`, but the IMPORT INTO path currently passes nil. An ordered parallel reader means multiple object-store byte ranges are fetched concurrently and buffered by their absolute offsets, while calls to `Read` still return the exact sequential byte stream.

`pkg/ingestor/simplesst/writer.go` owns the global-sort batch writer. It copies encoded key/value bytes into a `membuf.Buffer`, stores lightweight slice locations, sorts those locations by key when the buffer fills, writes one data object and one range-property object, updates duplicate information and min/max statistics, then resets the buffer. A ping-pong writer has two such batch states: a front state receiving writes and a back state being sorted and uploaded. Backpressure is correct only when both states are unavailable.

The term source throughput means bytes consumed from input objects per second. Wire amplification, written as `A_wire`, is object-store output bytes divided by source bytes and includes key/value framing and stat objects. AWS documents EC2 instance bandwidth independently for inbound and outbound traffic and allows both directions simultaneously. Therefore the source-rate network limit for this project is `min(B_rx, B_tx / A_wire)`. If the worker limit is 400 MiB/s in each direction, this becomes `min(400 MiB/s, 400 MiB/s / A_wire)`. Pod RX/TX and ENA allowance counters remain observability signals, not a test of whether AWS combines the directions.

For a fixed workload, define the achievable source throughput ceiling as:

    R_ceiling = min(R_read, R_cpu, R_sort_write / A_wire, R_network)

`R_read` comes from object-store read tests with the chosen concurrency. `R_cpu` comes from an in-memory/no-network encode pipeline benchmark for the same table and row shape. `R_sort_write` is emitted wire bytes per second from a writer benchmark. `R_network` uses the applicable formula above. The primary efficiency result is `R_actual / R_ceiling`; also report rows/s and KV pairs/s because source-byte throughput is misleading for compressed files and wide indexes.

The existing metric families include `tidb_import_bytes{state=...}`, `tidb_import_chunk_process_operation_seconds{operation=...}`, and `tidb_global_sort_write_to_cloud_storage_*`. Histogram `_sum` rates represent accumulated worker-seconds, not wall time. For example, a `send` `_sum` rate of 2 on a seven-worker pod means about two worker-seconds per second are blocked sending, or roughly 29% of encode-worker capacity.

## Workload Contract

Four workload manifests are required. Every manifest must record the exact `CREATE TABLE`, `IMPORT INTO`, source URI with credentials removed, compression, file count, min/median/max file size, total source bytes, expected row count, expected generated KV groups, and a stable data checksum. Store concise manifests beside this plan or append them under `Artifacts and Notes`; do not commit credentials.

W0 is the existing large uncompressed PK-only sbtest shape. It isolates source reads and the data-writer flush cliff. The historical reference is roughly 5000 files and a long enough encode step to reach steady state.

W1 is a realistic moderate-index table with three secondary indexes, including at least one nontrivial multi-column index. It shows whether writer scheduling across KV groups introduces new bottlenecks.

W2 has sixteen generated secondary-index KV groups. Rows should be large enough and numerous enough to run steadily for at least five minutes. This is the high-amplification, CPU-heavy guardrail rather than a workload expected to reach W0's source MiB/s.

W3 is a compressed CSV workload, preferably zstd plus one gzip smoke data set. Its decoded schema may reuse W0 or W1. It proves that ordered parallel raw reads preserve decompressor semantics and highlights why rows/s must accompany source MiB/s.

Use a smaller 50-100 GiB calibration set for concurrency and buffer tuning, then the large W0 data for final comparisons. Interleave baseline and candidate runs when practical so time-dependent cluster noise does not always favor one image. Run one warm-up and at least two measured trials for cluster gates. If repeated runs show more than 5% spread, run a third, use the median for the directional decision, and do not claim improvements smaller than the observed noise band.

## Plan of Work

### Milestone 0: reproducible baselines and observability

Start from an agreed base commit in a dedicated worktree. Do not reuse the dirty root worktree. Record the base SHA and all image digests in this plan.

Build one initial immutable `observe` image from the agreed master SHA plus only the metrics, summary logs, and benchmark harness required by this milestone. This is the authoritative baseline for every later ablation. A bare-master image may be run once to connect new results to historical numbers, but it is not required and no small fixed delta between bare master and `observe` is an acceptance gate. Validate instrumentation overhead with focused local benchmarks and profile the metric/log code itself if it appears in CPU or allocation profiles.

Port or implement fine-grained reader timing so raw object-store read, decompression, parse, encode, send wait, data append, index append, sort, upload, and total chunk time can be distinguished. `pkg/lightning/metric/metric.go` owns the task-scoped operation histogram. Put read-timing wrappers under `pkg/executor/importer` so Lightning's unrelated paths do not silently change. Extend the existing per-chunk summary log rather than logging per row or per range.

Add writer observations for front-buffer bytes, completed flush bytes, active flushes, time blocked because both halves are unavailable, flush error count, and file count. Existing `sort`, `write`, and `sort_and_write` histograms remain authoritative for flush service time. Avoid a second metric that counts the same duration under a different name.

Add or reuse microbenchmarks for the three schema shapes. Extend the existing CSV parser benchmarks in `pkg/lightning/mydump/csv_parser_test.go`; do not create duplicate parser scaffolding. Add encode pipeline benchmarks alongside `pkg/executor/importer/chunk_process_testkit_test.go` or in a new `chunk_process_bench_test.go`, reusing existing test helpers to construct tables and encoders. Benchmarks must report rows, source bytes, encoded KV bytes, and KV pairs. Use an in-memory source and object store so the CPU benchmark includes routing, checksum, copy, and sort but excludes remote network.

Run `tools/objstore-perf` inside a worker pod to establish one-stream and concurrent read/write service rates against the same region and storage classes used by the job. Run read-only and write-only benchmarks; do not add a simultaneous read/write experiment merely to determine bandwidth directionality, because AWS already specifies separate simultaneous inbound and outbound capacity. Pod network receive/transmit metrics and ENA allowance-exceeded counters during a real encode run show whether another practical constraint prevents use of that capacity.

Capture a 120-second CPU profile, heap profile, and block profile during steady-state baseline W0 and W2 runs. Capture task ID, pod name, start/end timestamps, and the exact commit. Raw large profiles and logs may live in external artifact storage; record stable references and a short top table in this plan.

Milestone 0 is accepted when every workload can be reproduced from its manifest, stage worker-seconds explain the dominant wait, `A_wire` can be calculated, and `R_read`, `R_cpu`, `R_sort_write`, `R_network`, and `R_actual` can be filled into the baseline table under `Artifacts and Notes`.

### Milestone 1: ordered parallel source reads

Create `pkg/util/prefetch/parallel_reader.go` and tests beside it. Define a `RangeOpener` callback that opens a half-open byte range `[start,end)`. The reader assigns monotonically increasing 8 MiB ranges to workers, holds completed data in a bounded reorder map, and exposes an ordinary sequential `io.ReadCloser`. It must never hold more than the configured window, must return short final ranges correctly, and must cancel and join every worker on `Close`.

Integrate this reader into S3-like, GCS, and KS3 source open and seek/reopen paths. Extract shared selection logic so the threshold, range size, concurrency calculation, and cap do not drift between backends. With an encode prefetch window of 32 MiB, use four 8 MiB requests; cap one reader at eight requests. A process-level limiter initially caps active range requests at 32. Range retries must preserve the requested offsets and must not duplicate bytes.

Add an IMPORT INTO encode-read setting internal to the implementation, initially 32 MiB. Do not expose a user-facing SQL option in the first PR. Pass zero for legacy Lightning/sampling callers unless their behavior is explicitly covered. Preserve existing behavior for local files and object stores that cannot provide independent ranges.

Tests must cover out-of-order completion, partial final blocks, files smaller than 8 MiB, an error in a non-first range, cancellation, repeated close, bounded memory, seek/reopen, compressed zstd input, compressed gzip input, and exact byte identity against the sequential reader. Extend the nearest existing tests and keep new top-level tests grouped; benchmarks do not count toward shard limits.

The reader cluster gate compares current baseline and reader-only images on W0 and W3. Test reader concurrency 2, 4, and 8 on the calibration set, but promote one default only. Four remains the default unless another value improves median end-to-end throughput by at least 5% without increasing object-store errors, throttling, or peak process memory materially.

Milestone 1 is accepted when W0 raw-read worker time falls by at least 30% or is no longer among the top two limits, median W0 encode throughput improves by at least 20% unless another measured ceiling is reached first, W3 imports exact rows under zstd and gzip, and no range leak, retry corruption, or process-memory growth is observed.

### Milestone 2: opt-in asynchronous ping-pong writer

Keep `WriterBuilder.Build` synchronous by default. Add an opt-in async-flush configuration used only by `pkg/dxf/importinto/encode_and_sort_operator.go`. The public writer file format, partitioned object naming, `WriterSummary`, `MultipleFilesStat`, duplicate files, TiKV codec application, and close callback remain unchanged.

Refactor the mutable per-flush fields in `pkg/ingestor/simplesst/writer.go` into a batch state containing its `membuf.Buffer`, slice locations, KV size, framed batch size, and range-property state. In async mode, construct two batch states, each limited to half of the memory limit passed to the builder. `WriteRow` appends only to the front state. When front is full, enqueue that detached state to one flush loop and switch to the available state. If the other state is still flushing when the new front fills, record `both_buffers_wait` and wait; this is the only expected encode backpressure.

Only the flush loop mutates sequence numbers, min/max keys, total size/count, multi-file stats, and duplicate summary. It runs sort, duplicate handling, data/stat upload, and state reset in order, which preserves deterministic summary ordering. The next `WriteRow`, explicit flush, or `Close` must surface any background error. `Close` enqueues the last nonempty state, waits for all flushes, destroys both buffers, and calls `onClose` only after final stats are complete. Cancellation must stop new writes, close/abort in-flight object writers through existing error paths, and avoid goroutine leaks.

Because data and sixteen index writers can flush independently, share a bounded flush limiter among writers created by one import executor. Start with four active flushes per pod/executor, based on the historical roughly 119 MiB/s per-flush write rate and roughly 400 MiB/s pod bandwidth. Keep the limit internal for the prototype. Measure limits 2, 3, and 4 on W1/W2 before promoting a default. Multipart concurrency inside one file stays unchanged during the first experiment so the ablation isolates buffering and scheduling.

Recalculate adjusted block sizes against a half-buffer, particularly for sixteen-index cases. The pool may allocate lazily, but the combined aligned block capacity of both halves across data and index writers must stay within the existing 50% writer budget plus documented allocator overhead.

Add deterministic writer tests using a controllable fake storage writer. Block the first upload after a batch becomes full and prove that writes continue into the second state. Fill both states and prove the next write blocks until one flush finishes. Cover async upload error propagation, close during an active flush, retry behavior, duplicate modes, keyspace codec, min/max stats, file count, context cancellation, and unchanged synchronous behavior when async mode is not enabled.

Extend IMPORT INTO unit coverage to construct data and index writers in async mode and read every emitted file back through the existing reader. Reuse `tests/realtikvtest/importintotest4` for end-to-end global-sort correctness. `TestGlobalSortBasic` covers multiple indexes, forced merge, cleanup, and failure; `TestGlobalSortMultiFiles` covers multiple source files and three secondary indexes. Add a focused case only if existing methods cannot assert async close/error behavior.

The writer cluster gate compares reader-only and reader-plus-writer images. W0 isolates the data writer. W1 and W2 test scheduling and amplification. Record send worker-seconds, both-buffer wait, sort/upload service time, active flushes, file count, whether merge sort was skipped, peak RSS, GC CPU, receive/transmit throughput, S3/GCS request errors, and the whole IMPORT INTO duration including any changed merge cost.

Milestone 2 is accepted when `send` plus both-buffer wait is below 5% of encode-worker capacity in steady-state W0, or when its remaining value is explained by the measured write/network ceiling; W0 median encode duration improves by at least 15% over reader-only unless it already reaches another ceiling; no W1/W2 correctness or cleanup regression occurs; peak process memory stays below 90% of the 12.9 GiB limit with no rising trend; and extra files do not make total encode-plus-merge duration worse. If the simple design fails specifically because small per-index objects or object counts dominate W2, record the evidence and open a new decision about unified physical files. Do not introduce segments within this milestone.

### Milestone 3: profile-driven encoding CPU work

After Milestone 2, repeat steady-state CPU, allocation, block, and mutex profiles for W0, W1, and W2. Use profiles from the combined reader-plus-writer image. Rank costs by CPU-seconds per million rows, allocation bytes per row, and share of wall-clock worker capacity. Separate generic costs from schema-dependent costs.

Generic candidates include the copy from encoder-owned KV buffers into the sort buffer, CRC64 checksum scans, `tablecodec.DecodeIndexID` plus map routing, repeated key decoding during comparisons, per-KV framing copies, and temporary slice/map allocation. Schema-dependent candidates include type casts, collation, generated expressions, JSON handling, and the number/width of secondary indexes. This list is a routing guide, not permission to change all of them.

Select one optimization at a time. A candidate qualifies when it owns at least 8% of CPU in one representative workload or at least 10% of allocation bytes, and the proposed change has a correctness test surface. Before editing, record the expected invariant and failure modes in `Decision Log`. Add a benchmark that fails the performance hypothesis or captures the old allocation count, implement the smallest change, and retain an A/B commit or build for the cluster test.

Promote a CPU optimization only when its focused benchmark improves the targeted cost by at least 10%, the affected cluster workload improves end-to-end encode throughput by at least 3% or CPU-seconds per row by at least 5%, and no other representative workload regresses by more than 3%. Smaller wins may be kept only when they materially simplify code or remove allocations with essentially zero risk; record that exception.

Stop CPU work for a workload when actual throughput reaches at least 85% of its measured ceiling, or when the remaining gap is fully assigned to network/write/read capacity rather than avoidable CPU idle time. A sixteen-index workload is successful at its own ceiling even if its source MiB/s is far below W0.

### Milestone 4: final integration and rollout evidence

Run the final workload matrix using one immutable image. For every workload, run at least two measured trials and report the median. Verify imported row counts, table checksums, `ADMIN CHECK TABLE` where applicable, duplicate behavior, cleanup after success/failure, and no orphaned multipart uploads. Compare encode-only time and total IMPORT INTO time so an encode improvement cannot hide a merge or ingest regression.

Prepare separate PRs for observability, reader behavior, writer behavior, and each CPU optimization. Every PR description must contain its ablation, exact tests, resource impact, and rollback behavior. Reader and writer internal defaults must be easy to revert independently until final validation is complete.

Final acceptance requires at least 85% efficiency on W0 and W1, at least 80% on W2/W3 or a documented external ceiling, no correctness regression, no OOM or persistent memory growth, and no greater than 3% total-duration regression in any representative workload. Use the documented per-direction AWS limit in the ceiling model; use observed plateaus only to diagnose a lower practical constraint such as S3 service rate, ENA allowance, CNI shaping, packet rate, connections, or CPU.

## Concrete Steps

Run all repository commands from the repository root unless a command explicitly changes directory.

First create a clean implementation worktree after agreeing on the base SHA:

    git worktree add worktrees/import-encode-opt -b feature/import-encode-opt <base_commit>
    cd worktrees/import-encode-opt
    git status --short

Expect no output from `git status --short`. Record `<base_commit>` in `Artifacts and Notes` before editing. Do not delete or reset the existing dirty root worktree.

Before any build or test after Go edits, inspect the Bazel gate and run `make bazel_prepare` because this plan expects new Go files, import changes, and new top-level tests:

    git status --short
    git diff --name-status
    git diff --name-status --cached
    git ls-files --others --exclude-standard
    git diff -U0 -- '*.go'
    git diff -U0 --cached -- '*.go'
    make bazel_prepare

Review every generated Bazel diff. The repository contains unrelated user files in other worktrees; only metadata belonging to this feature may enter a PR.

Use the WIP profile during coding. Expected targeted commands are listed below; update test names in this plan if implementation chooses clearer behavior-based names.

For reader utility tests, the package does not currently use failpoints:

    go test ./pkg/util/prefetch -run 'TestParallelReader' -tags=intest,deadlock -count=1

For object-store integration and importer tests, use the failpoint-aware runner because those packages use failpoints:

    ./tools/check/failpoint-go-test.sh pkg/objstore/s3like -run 'TestParallelPrefetch' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'Test.*ParallelPrefetch' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run 'Test.*ParallelRead' -count=1

For writer and encode integration tests:

    ./tools/check/failpoint-go-test.sh pkg/ingestor/simplesst -run 'TestWriterAsyncFlush' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run 'Test.*AsyncFlush' -count=1
    ./tools/check/failpoint-go-test.sh pkg/dxf/importinto -run 'TestEncodeAndSort.*AsyncFlush' -count=1

Run CPU benchmarks without running unit tests. Always pin count and benchtime in recorded comparisons:

    ./tools/check/failpoint-go-test.sh pkg/lightning/mydump -run '^$' -bench 'Benchmark(ReadRowUsingMydumpCSVParser|CSVParserUnescape)$' -benchmem -count=5 -benchtime=3s
    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run '^$' -bench 'BenchmarkEncodePipeline(PKOnly|ThreeIndexes|SixteenIndexes)$' -benchmem -count=5 -benchtime=3s

For a Ready-profile gate on a code PR, rerun all targeted tests affected by that PR, then run:

    make lint

Do not run `make bazel_lint_changed`; repository policy forbids it unless explicitly requested.

For the final RealTiKV checks, use the lifecycle in `.agents/skills/tidb-realtikv-runner` and `docs/agents/testing-flow.md`. A cleanup-safe command shape is:

    PD_ADDR=127.0.0.1:2379
    (
      cleanup() {
        [ -n "${PLAYGROUND_PID:-}" ] && kill "${PLAYGROUND_PID}" 2>/dev/null || true
        [ -n "${PLAYGROUND_PID:-}" ] && wait "${PLAYGROUND_PID}" 2>/dev/null || true
        rm -rf "${HOME}/.tiup/data/realtikvtest"
      }
      trap cleanup EXIT INT TERM
      tiup playground --mode tikv-slim --tag realtikvtest &
      PLAYGROUND_PID=$!
      until curl -sf "http://${PD_ADDR}/pd/api/v1/version" >/dev/null; do sleep 1; done
      ./tools/check/failpoint-go-test.sh tests/realtikvtest/importintotest4 \
        -run 'TestImportInto/TestGlobalSort(Basic|MultiFiles)$' \
        -tags=intest,deadlock,nextgen -count=1
    )
    ! curl -sf "http://${PD_ADDR}/pd/api/v1/version"

If the runner requires a non-default PD port, pass `-args -tikv-path 'tikv://127.0.0.1:<pd_port>?disableGC=true'` and update `PD_ADDR`. Expect both suite methods to pass and the final curl check to fail, proving cleanup.

Inside a worker pod, build or copy `tools/objstore-perf` from the exact image commit. Example benchmark shapes are:

    ./objstore-perf -mode read -url '<source_store_url>' -prefix '<run_id>/read-1' -workers 7 -duration 2m -object-size 256MiB -block-size 8MiB -prefetch-size 0
    ./objstore-perf -mode write -url '<sort_store_url>' -prefix '<run_id>/write-1' -workers 4 -duration 2m -object-size 512MiB -block-size 8MiB -writer-concurrency 20 -part-size 5MiB -cleanup

Never put credentials in shell history, committed manifests, logs, or this plan. Use the cluster's workload identity or secret injection. The benchmark prefix must be unique and the write benchmark must either use `-cleanup` or be removed through an explicitly scoped storage cleanup operation.

## Cluster Experiment Handoff

The user will provide or help provision the cluster and datasets. Before asking for a run, the implementer must provide one immutable image tag/digest, commit SHA, exact SQL, exact workload manifest, expected duration, metrics time window, and cleanup expectations. Do not ask the user to choose tuning values ad hoc; each experiment request must name one controlled variable and the baseline it compares with.

For every run, collect the following bundle:

- Run ID, task/job ID, image digest, commit SHA, cluster and region, start/end UTC timestamps.
- Pod CPU request/limit, observed available CPU, memory limit, EC2 instance type, and the documented per-direction network baseline/burst limit.
- Source and sort-store provider/region/storage class, with credentials removed.
- Workload manifest ID, SQL, total source bytes, row count, compression, index count, and expected KV groups.
- Encode step wall time, total IMPORT INTO wall time, source rows/s and MiB/s, emitted KV MiB/s, `A_wire`, and data/stat/duplicate object counts.
- Per-pod receive/transmit rates, CPU usage and throttling, RSS/working set, GC CPU/pause/heap, active range requests, active flushes, object-store error/throttle counts.
- Operation histogram `_sum` rates for raw read, decompress, parse, encode, send, data append, index append, sort, upload, and both-buffer wait.
- Flush P50/P95/P99 size and duration, merge-step decision/duration, final row/checksum result, and cleanup result.
- CPU/heap/block profiles for baseline and the final run of each milestone.

Append a concise result table and links to durable artifacts under `Artifacts and Notes`. If a run is invalid because another workload, pod restart, autoscaling, or object-store incident overlaps the time window, mark it invalid rather than averaging it into the result.

## Validation and Acceptance

Correctness has priority over throughput. Parallel reads must produce byte-identical streams under ordinary reads, seek, retry, cancellation, gzip, and zstd. Async flush must produce the same sorted file framing, range properties, duplicate records, keyspace-prefixed keys, min/max summaries, and cleanup behavior as synchronous flush. Tests must make the old implementation fail the new overlap assertion before the new code is accepted; performance-only changes still require deterministic behavior tests.

Local validation uses the WIP profile while iterating and the Ready profile before claiming a PR complete. Packages `pkg/objstore`, `pkg/executor/importer`, `pkg/ingestor/simplesst`, and `pkg/dxf/importinto` use failpoints, so their unit tests must run through `tools/check/failpoint-go-test.sh`. `make bazel_prepare` is mandatory for the anticipated import, new-file, and new-test changes. `make lint` is mandatory for Ready code changes.

Cluster acceptance is based on median steady-state results and the ceiling formula, not a single peak chart. Reader and writer milestones have their own gates so an unsuccessful idea can be revised or discarded without contaminating the next comparison. Final acceptance criteria are stated in Milestone 4 and must be copied into the final PR or project summary with actual numbers.

What cannot be proven locally must be stated: real S3/GCS range behavior, pod network interpretation, multipart concurrency limits, large-data memory high-water marks, and actual workload ceilings require the user-provided cluster.

## Idempotence and Recovery

Unit tests, benchmarks, Bazel preparation, and read-only cluster metrics collection are safe to rerun. Object-store benchmarks must use a unique run prefix. Read tests may reuse prepared immutable objects; write tests must clean only their exact prefix.

The reader implementation remains disabled outside the explicit encode prefetch setting, so rollback is one constant/configuration change while the code is under evaluation. The writer implementation remains opt-in in `WriterBuilder`, so rollback changes only IMPORT INTO construction back to synchronous mode and leaves the external files readable.

If a cluster run fails, preserve job/task IDs, logs, and the exact image before retrying. Do not reuse a possibly partially imported table; drop/recreate the test database or use a new table name. Confirm global-sort temporary objects are cleaned. If cleanup fails, list the exact task prefix and remove only that prefix through the approved storage procedure.

If `make bazel_prepare` generates unrelated metadata because of files in another worktree or dirty root, stop and create or repair the clean feature worktree. Do not reset or delete user changes. If a new async writer corrupts output, disable the opt-in mode first; existing synchronous files and readers remain the recovery path.

## Artifacts and Notes

Record the agreed implementation base here:

    Base commit: 454d7010a4 (upstream/master when selected on 2026-08-18)
    Feature worktree: worktrees/import-encode-opt
    Observability baseline image digest: <not built>
    Optional bare-master historical image digest: <not built>

Record the workload manifests here or link repository-relative manifest files:

    W0 PK-only uncompressed: <pending>
    W1 three-index realistic: <pending>
    W2 sixteen-index: <pending>
    W3 compressed: <pending>

Use this result shape after every valid run:

    Run ID | Build | Workload | Encode wall | Rows/s | Source MiB/s | KV MiB/s | A_wire | Read cap | CPU cap | Write cap/source | Network cap | Efficiency | Send % | Both-buffer wait % | Peak RSS | Files | Merge wall | Total wall

Historical evidence, not a substitute for a fresh baseline on the agreed base:

    Baseline: about 333 MiB/s cluster encode throughput, about 32 minutes on W0-like data.
    Reader prototype: about 450 MiB/s cluster throughput; S3 read about 1.0-1.7 seconds/chunk; normal chunks about 3.5 seconds; periodic send stalls about 9-10 seconds remained.

## Interfaces and Dependencies

Milestone 1 should leave `pkg/util/prefetch` with an interface equivalent to:

    type RangeOpener func(ctx context.Context, start, end int64) (io.ReadCloser, error)

    type RangeLimiter struct {
        // implementation-private process-level semaphore
    }

    func NewRangeLimiter(limit int) *RangeLimiter

    func NewParallelReader(
        ctx context.Context,
        opener RangeOpener,
        totalSize int64,
        concurrency int,
        blockSize int,
        limiter *RangeLimiter,
    ) io.ReadCloser

Names may change for local convention, but the contract is mandatory: half-open exact ranges, ordered output, bounded memory, shared concurrency control, cancellation, and no goroutine leak.

Milestone 2 should add an opt-in builder dependency equivalent to:

    type FlushLimiter struct {
        // implementation-private semaphore
    }

    func NewFlushLimiter(limit int) *FlushLimiter

    func (b *WriterBuilder) SetAsyncFlush(limiter *FlushLimiter) *WriterBuilder

`SetMemorySizeLimit` continues to describe total KV-buffer memory for one writer. In async mode each of the two states receives approximately half that limit. `Build` continues returning `*Writer`, so `IndexRouteWriter` and DDL callers do not require a new writer interface. When async mode is absent, observable behavior and scheduling remain synchronous.

Task-scoped metrics continue to use the existing IMPORT INTO metric registry and task ID labels. New labels must have bounded cardinality; do not label metrics by file name, index ID, worker UUID, or object path.

The object-store SDKs, `membuf`, TiKV codec, checksum, duplicate modes, and global-sort readers remain existing dependencies. Do not add a third-party concurrency or sorting dependency for these milestones.

## Plan Revision Notes

2026-08-18: Created the initial plan from current repository evidence and historical cluster measurements. Chose ordered 4x8 MiB reads, a format-preserving opt-in 25%/25% writer prototype, workload-specific ceilings, and profile-gated CPU optimization. Added explicit local validation, Bazel/failpoint rules, cluster handoff fields, rollback boundaries, and the gate before considering unified physical files.

2026-08-18: Removed the proposed simultaneous read/write experiment for deciding whether the 400 MiB/s AWS limit is shared. AWS documents instance bandwidth as available to inbound and outbound traffic simultaneously, with separate credit buckets. Retained RX/TX and ENA allowance monitoring only to find lower practical constraints.

2026-08-18: Initially proposed a bare-master control plus observability image with a 2% overhead gate, then removed that gate after the user pointed out that same-image cluster runs can vary by more than 2%. The authoritative baseline is now one immutable observability image from a pinned master SHA. All candidates retain identical observability code, and improvements smaller than the measured run-to-run noise are not claimed.
