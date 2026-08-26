# Attribute InfoSchema scan allocations to one statement

This ExecPlan is a living document maintained according to `PLANS.md`.

## Purpose

The 100,000-table `INFORMATION_SCHEMA.COLUMNS` benchmark reports roughly
848 MiB of process allocation per scan, while its post-GC live working set is
only about 10-14 MiB. Heap `alloc_space` profiles identify the allocation
sites, but shared gRPC reader goroutines and background workers make statement
ownership ambiguous. Add an opt-in, statement-owned diagnostic counter set so
the benchmark can correlate exact data-flow counts with sampled allocator
bytes without changing connection reuse, scan semantics, or default behavior.

## Progress

- [x] (2026-08-26) Reproduced the `alloc_space` delta from the existing batch-32 profiles.
- [x] (2026-08-26) Identified gRPC `NopBufferPool.Get` as the dominant sampled site.
- [x] (2026-08-26) Implemented opt-in counters from reusable scan response through metadata decode and row construction, plus process `MemStats` deltas for the retriever lifetime.
- [x] (2026-08-26) Added and passed focused client-go, meta, InfoSchema, and executor tests.
- [x] (2026-08-26) Ran `make bazel_prepare` in an ASCII APFS working copy because Bazel cannot represent the original Chinese workspace path; compared and applied only the generated BUILD deltas.
- [x] (2026-08-26) Built the integrated TiDB binary and ran three clean 100,000-table scans plus a final smoke after the nil-observer fast-path rebuild.
- [x] (2026-08-26) Compared exact counters, process-window `TotalAlloc`, and `alloc_space` samples; recorded overlap rules and the remaining background/sample residual in the benchmark report.

## Decisions

- Enable attribution with the session user variable
  `@tidb_diag_infoschema_scan_stats=1`; publish JSON in
  `@tidb_diag_infoschema_scan_stats_result` after the retriever closes.
- Keep counters query-owned and pass them through the existing metadata scan
  context. When the variable is absent, no observer is attached and the hot
  path performs no counter updates.
- Count exact logical bytes and allocation requests at owned boundaries. Use
  heap `alloc_space` for runtime allocator bytes because Go size classes and
  gRPC's shared reader are not exactly attributable from the SQL goroutine.
- Do not add a dedicated connection, a transport buffer pool, or a global
  `grpc-shared-buffer-pool` change.

## Validation

Targeted tests must verify counter propagation, accumulation, JSON publication,
and unchanged scan results. The integrated test must return 1,205,386 rows and
report the same `GOGC=500` and `grpc-shared-buffer-pool=false` controls as the
existing benchmark.

## Outcomes

Three clean scans allocated 832.40-836.37 MiB in the retriever process window,
with an average of 834.24 MiB. Their baseline-subtracted `alloc_space` profiles
averaged 832.43 MiB. Mutually exclusive profile categories attribute 79.4% to
MetaKV transport/protobuf, 8.3% to row materialization/output, 7.1% to
TableInfo scan/decode, 3.9% to other execution/client work, and 1.3% to
monitoring/background activity.

The dominant flat allocation site is gRPC `NopBufferPool.Get`, averaging about
654.78 MiB. The query-owned counters independently observed 658.12 MiB of Scan
responses across 3,132 RPCs, which explains the dominant sampled site without
mistaking the overlapping 653.92 MiB `TableInfo` JSON input for another
allocation. The final rebuilt-binary smoke returned all 1,205,386 rows and
reported 834.32 MiB of process-window allocation with identical data-flow
counters.

The detailed benchmark report and raw profiles are under
`local_250k_tables/results/infoschema_allocation_attribution_gogc500_20260826`.
