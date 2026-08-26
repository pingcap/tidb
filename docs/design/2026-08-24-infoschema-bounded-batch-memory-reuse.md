# Bound and account reusable InfoSchema batch memory

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan is maintained according to that document.

## Purpose / Big Picture

Large scans of `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`, and `INFORMATION_SCHEMA.TIDB_INDEXES` create short-lived row batches while decoding table metadata. The user needs those batches to reuse a bounded amount of query-owned memory instead of repeatedly growing garbage that waits for Go GC. After this change, the large metadata retrievers retain reusable row storage only up to a fixed byte limit, account the retained capacity in the statement memory tracker, and discard oversized storage instead of keeping a worst-case batch alive until the query ends.

The result is observable through targeted unit tests and through the existing 100,000-table benchmark under `local_250k_tables`: SQL results must remain identical while allocation, TiDB RSS peak, CPU, and latency are compared against the saved baseline binary.

## Progress

- [x] (2026-08-24) Located the large InfoSchema retrievers, chunk reuse path, statement memory tracker, existing 100,000-table data, saved baseline binary, and prior heap profiles.
- [x] (2026-08-24) Captured three-run metadata-projection baselines and a `TABLES` no-GC heap profile with `/private/tmp/tidb-server-infoschema-baseline` under the preserved 100,000-table topology.
- [x] (2026-08-24) Implemented a 1,024-row query-scoped reusable row buffer with a 16 MiB retained-capacity limit, oversized-buffer eviction, and statement memory tracking.
- [x] (2026-08-24) Added regression tests for normal reuse, oversized-buffer eviction, tracker release, and one persistent MetaKV iterator spanning multiple output batches.
- [x] (2026-08-24) Passed scoped failpoint-aware tests, the 282-case `executor/infoschema_reader` integration test, `make bazel_prepare`, and built `/private/tmp/tidb-server-infoschema-bounded-reuse-v2`.
- [x] (2026-08-24) Repeated the three-run benchmark and isolated heap profiles with identical SQL, data, topology, and sampling settings.
- [x] (2026-08-24) Passed the Ready validation gate (`make lint`, final serial failpoint-aware executor regression, and `git diff --check`), reviewed the focused diff, and recorded the outcome.

## Surprises & Discoveries

- Observation: The destination `chunk.Chunk` already preserves its backing storage across `Reset`, but `MemTableReaderExec.Next` still receives `[][]types.Datum` and copies each row into that chunk.
  Evidence: `pkg/executor/memtable_reader.go` calls `req.GrowAndReset` after `retriever.retrieve` returns a datum row slice.

- Observation: The current large-table special retriever is used only for `INFORMATION_SCHEMA.COLUMNS`; `TABLES` and `TIDB_INDEXES` still use the generic retriever and materialize their result rows.
  Evidence: `pkg/executor/builder.go` constructs `hugeMemTableRetriever` only for `TableColumns`.

- Observation: The worktree contains unrelated diagnostic API and resumable MetaKV iteration changes. They must be preserved.
  Evidence: `git status --short` lists changes under `pkg/server`, `pkg/meta`, and `pkg/structure` that predate this plan.

- Observation: The baseline `TABLES` profile allocated 1.92 GiB during one scan, with roughly 450-472 MiB each attributed to gRPC materialization, `KvPair.Unmarshal`, and `slices.Clone`.
  Evidence: `../local_250k_tables/results/bounded_reuse_baseline_profile_tables` contains the before/after heap profiles; the query raised uncollected `HeapAlloc` from about 244 MiB to 1.19 GiB.

- Observation: The saved three-run metadata baseline returned 100,834 `TABLES` rows, 1,205,386 `COLUMNS` rows, and 300,198 `TIDB_INDEXES` rows.
  Evidence: `../local_250k_tables/results/bounded_reuse_baseline/system_table_metadata_scan_100000_20260824T124729Z.json` records the exact row counts and per-run measurements.

- Observation: Recreating the MetaKV scanner at every 1,024-row executor boundary made the first implementation allocate more than the baseline for `TABLES` and `COLUMNS`.
  Evidence: First-implementation `alloc_space` was 2.19 GiB for `TABLES`, 7.03 GiB for `COLUMNS`, and 3.50 GiB for `TIDB_INDEXES`; the scanner-resume path alone allocated 328.15 MiB, 535.51 MiB, and 433.34 MiB in `slices.Clone`.

- Observation: Keeping the scanner alive across output batches removes the raw-value clone path and most RPC prefetch amplification.
  Evidence: Final `alloc_space` is 1.75 GiB, 2.88 GiB, and 1.68 GiB. Metadata-path `slices.Clone` is zero for `COLUMNS` and `TIDB_INDEXES`, while final latency is 6.676 s, 9.957 s, and 6.917 s.

- Observation: Low live heap after forced GC does not guarantee low settled RSS.
  Evidence: Final after-GC `inuse_space` deltas are within sampling noise of zero, but the isolated `COLUMNS` process still retained about 980 MiB of RSS after 20 seconds. Go heap page retention and client pools remain outside the bounded row-buffer policy.

## Decision Log

- Decision: Use query-scoped bounded reuse rather than a process-global `sync.Pool`.
  Rationale: Query ownership makes memory accounting deterministic, avoids retaining an outlier allocation globally, and avoids `sync.Pool` being cleared nondeterministically by GC.
  Date/Author: 2026-08-24 / Codex

- Decision: Start with row-batch ownership and accounting in `pkg/executor`; do not introduce off-heap memory or change MetaKV value ownership in this change.
  Rationale: This keeps the correctness and compatibility surface local while directly testing the user's requested retained-capacity policy. MetaKV borrowed values and partial JSON decoding remain separate larger optimizations.
  Date/Author: 2026-08-24 / Codex

- Decision: Retain at most 16 MiB per retriever, with a 1,024-row target batch, and discard a backing store whose estimated capacity exceeds that limit.
  Rationale: Sixteen MiB is inside the requested 1-16 MiB range and is large enough for normal metadata batches while preventing a single unusually wide table from becoming a long-lived per-query allocation.
  Date/Author: 2026-08-24 / Codex

- Decision: Keep one borrowed-value MetaKV iterator alive for the lifetime of a schema scan instead of reopening a resumable scanner for every output batch.
  Rationale: The executor consumes returned rows synchronously before requesting the next batch, so the iterator can safely preserve its scanner while decoded `TableInfo` values remain independently owned. This removes repeated clone and prefetched-RPC waste without changing SQL result ordering.
  Date/Author: 2026-08-24 / Codex

## Outcomes & Retrospective

The final implementation preserves all result row counts and reduces mean latency versus the original baseline by 20.9% for `TABLES`, 47.0% for `COLUMNS`, and 31.1% for `TIDB_INDEXES`. Mean TiDB CPU falls by 13.4%, 9.0%, and 9.8%.

The persistent scanner fixes the first implementation's cumulative-allocation regression. Final `alloc_space` is 1.75 GiB for `TABLES`, 2.88 GiB for `COLUMNS`, and 1.68 GiB for `TIDB_INDEXES`: 20.0%, 59.0%, and 51.9% below the first implementation. Statement Summary `MAX_MEM` is 1.19 MiB, 4.86 MiB, and 1.05 MiB, confirming that query-owned reusable Datum memory is tracked below the 16 MiB retained-capacity ceiling.

The remaining allocation cost is dominated by full `TableInfo` JSON decoding, `KvPair` unmarshalling, and gRPC receive/materialization buffers. RSS can remain high after `COLUMNS` even when forced-GC live heap returns to baseline, so immediate operating-system RSS release is explicitly not an acceptance claim for this change. Detailed measurements are stored in `local_250k_tables/results/infoschema_bounded_reuse_v2_ab_comparison_20260824.md`.

## Context and Orientation

`pkg/executor/infoschema_reader.go` builds datum rows for virtual Information Schema tables. `hugeMemTableRetriever` already returns `COLUMNS` in roughly 1,024-row batches, but the outer and per-row slice capacities are not governed by a byte limit and are not fully represented in statement memory tracking. `pkg/executor/memtable_reader.go` copies returned datum rows into a reusable columnar `chunk.Chunk`. `pkg/executor/builder.go` attaches selected retriever memory trackers to `StmtCtx.MemTracker`.

A retained capacity is memory whose slice length has been reset to zero but whose backing array remains reachable for the next batch. The Go GC cannot reclaim it, so the SQL tracker must count its capacity rather than only its current length. Oversized eviction means replacing the reusable rows slice with a small fresh slice after returning the oversized batch, once the executor no longer needs the previous rows.

The benchmark directory is `../local_250k_tables` relative to this repository. It owns a preserved TiUP playground tag containing 100,000 empty tables and scripts that capture process CPU, RSS, Go runtime metrics, and heap profiles. `/private/tmp/tidb-server-infoschema-baseline` is the saved pre-change TiDB binary.

## Plan of Work

First run a baseline metadata scan and heap capture using the saved binary and existing TiUP tag. Then add a small internal reusable-row-buffer abstraction beside the large retriever. It will estimate retained capacity from the outer row slice, each row's datum slice, and the datum payload estimate already used by TiDB. It will update a dedicated statement child tracker whenever retained capacity changes. At the start of the next retrieval call it will reuse the backing arrays when retained capacity is at most 16 MiB; otherwise it will drop all row references and reinitialize a small batch.

Integrate the tracker in `executorBuilder` for large metadata retrievers and ensure `Close` releases all tracked retained capacity. Extend an existing executor test file rather than adding a new Go test file. Tests will exercise the buffer directly and run representative Information Schema queries to ensure rows and batching remain correct.

After scoped tests pass, build a new server binary and repeat the exact baseline benchmark/profile. Compare result row counts, latency, baseline-adjusted CPU, TiDB RSS delta, peak HeapAlloc, `TotalAlloc` delta, GC count, and Statement Summary memory. Finally run the repository Ready verification profile required for changed Go code.

## Concrete Steps

From repository root, baseline and optimized runs use the existing scripts with `DB_BINPATH` set to the corresponding binary. Each run starts the playground, waits for SQL readiness, invokes `benchmark_system_tables.py` or `profile_tables_memory.py`, and stops the playground before switching binaries.

Targeted tests run through the executor package's failpoint-aware test wrapper because `pkg/executor` uses failpoints:

    ./tools/check/failpoint-go-test.sh pkg/executor -run '<new focused test names>' -count=1

The optimized binary is built from the current worktree after formatting and targeted tests. Because this change will modify an existing Go import section and add a top-level test, repository policy requires:

    make bazel_prepare

The final Ready gate includes the required scoped tests and:

    make lint

## Validation and Acceptance

The change is accepted when all focused tests pass, all three metadata queries return exactly the baseline row counts, retained-capacity tracking returns to zero on retriever close, and no buffer larger than 16 MiB remains retained for reuse. The A/B report must show measured results even if RSS or CPU does not improve; no performance claim is made without the data.

## Idempotence and Recovery

Benchmark scripts only read the existing 100,000-table dataset. Starting and stopping the playground is repeatable and preserves the tagged data. If a run fails, stop the playground, retain its partial output directory, restart with the same binary, and rerun into a new timestamped directory. Existing dirty-worktree changes are not reverted or overwritten.

## Artifacts and Notes

Baseline and optimized JSON, CSV, Markdown, and heap profile artifacts will be stored under `local_250k_tables/results` in separately named directories. The saved binaries stay under `/private/tmp`.

## Interfaces and Dependencies

The implementation uses existing `types.EstimatedMemUsage`, `memory.Tracker`, and `chunk.Chunk` APIs. It adds no third-party dependency and changes no SQL-visible schema or result ordering.
