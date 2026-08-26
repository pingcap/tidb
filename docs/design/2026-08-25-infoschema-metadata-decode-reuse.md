# Bound metadata scan memory at GOGC=500

This ExecPlan is a living document maintained according to `PLANS.md`.

## Current status after connection rollback

On 2026-08-25, the dedicated reusable-scan connection map and the explicit
16 MiB gRPC transport buffer pool were rolled back to reduce client-go
connection-management scope. `ReusableScanResponse` remains enabled, but it
now uses the ordinary TiKV connection pool and bypasses BatchCommands only for
that request.

The current three-run `COLUMNS` result is 434.89 MiB average and 510.34 MiB
maximum query-induced peak RSS at GOGC=500. The authoritative report is at
`../../local_250k_tables/results/metadata_columns_no_dedicated_conn_query_peak_noforce_gogc500_20260825/report.md`.
Sections below that describe the dedicated bounded connection pool are retained
as the historical experiment that preceded this rollback.

## Purpose

The preserved test has 100,000 empty tables, 12 columns per table, and two
secondary indexes per table. A full `INFORMATION_SCHEMA.COLUMNS` scan returns
1,205,386 rows. The goal is to reduce query-induced TiDB peak RSS below 200 MiB
while keeping runtime `GOGC=500`, without using an earlier GC, an explicit GC,
or the process-wide `tikv-client.grpc-shared-buffer-pool` switch as the
production solution.

The final design reuses memory at four ownership boundaries:

1. The executor keeps bounded typed result rows instead of building full Datum
   rows for columns that are not projected.
2. The MetaKV iterator partially decodes the ordinary base-table fields needed
   by `COLUMNS` and reuses scratch state.
3. `ReusableScanResponse` owns and reuses the decoded scan response graph.
4. A dedicated client-go connection for reusable scans uses an explicit
   16 MiB bounded gRPC transport buffer pool. Ordinary transaction, Cop, and
   batch RPC connections retain their existing behavior.

## Progress

- [x] (2026-08-25) Established the 100,000-table correctness and resource baseline.
- [x] (2026-08-25) Kept one MetaKV scanner alive across executor output batches.
- [x] (2026-08-25) Added bounded typed row reuse and FieldType decode scratch reuse.
- [x] (2026-08-25) Added a `COLUMNS`-specific partial `TableInfo` decoder with safe fallback for uncommon metadata.
- [x] (2026-08-25) Added caller-owned reusable scan response decoding in client-go.
- [x] (2026-08-25) Added a per-RPCClient, 16 MiB bounded gRPC buffer pool and one dedicated reusable-scan connection per TiKV address.
- [x] (2026-08-25) Removed the benchmark's global `grpc-shared-buffer-pool=true` experiment.
- [x] (2026-08-25) Passed scoped and full client-go tests and built the integrated TiDB server.
- [x] (2026-08-25) Ran three isolated optimized scans and one same-protocol control at exact `GOGC=500`, with no forced GC in the measured interval.
- [x] (2026-08-25) Passed the final client-go race/vet checks and TiDB `make lint`; restored and verified the optimized local cluster.

## Discoveries

- The original scanner lifetime was too short. Recreating it every 1,024 output
  rows repeated MetaKV reads and response allocation.

- Reusing executor rows alone did not control RSS. The dominant remaining
  allocations were gRPC receive buffers, batch response materialization, full
  protobuf response graphs, and full `TableInfo` JSON decoding.

- A normal `encoding/json` partial destination still allocates heavily because
  it builds strings and nested model values. A narrow gjson fast path for
  ordinary base tables removes that reflection-heavy path while preserving a
  standard JSON fallback for views, non-empty defaults, ENUM/SET/ARRAY, and
  other uncommon metadata.

- Enabling gRPC's shared pool globally proved that transport reuse was valuable,
  but it also changed every TiKV connection and could retain buffers for
  unrelated business traffic. The useful boundary is the reusable metadata
  scan request, not the whole process.

- A pre-query `pprof?gc=1` is not a neutral RSS baseline on macOS. In the first
  attempt, asynchronous scavenging released about 290 MiB during the query even
  though the automatic GC counter did not change. Final RSS measurements
  therefore use a fresh restart, a 20-second settle period, and no forced GC.

- Heap pprof allocation samples may remain stale when no GC occurs. The final
  measurement uses `go_gc_heap_allocs_bytes_total` for cumulative allocation and
  runtime GC cycle counters for GC attribution. A separate post-query diagnostic
  GC is used only to classify allocations after performance points are captured.

## Decisions

- Keep `GOGC=500` exact during the test by disabling the TiDB GOGC tuner and
  setting the runtime value to 500. This is a measurement control, not a memory
  optimization.

- Do not enable `tikv-client.grpc-shared-buffer-pool` globally. The benchmark
  and restored local cluster must report this setting as `false`.

- Select the dedicated connection only when `req.ReusableScanResponse != nil`.
  A normal request still uses the existing connection pool, batching, codec,
  compression, and global configuration semantics.

- Use one dedicated HTTP/2 connection per TiKV address. One connection can
  multiplex requests and avoids multiplying retained transport buffers by the
  normal gRPC connection count.

- Share one explicit buffer pool across all dedicated connections owned by an
  `RPCClient`. Its size classes match gRPC's useful classes: 256 B, 4 KiB,
  16 KiB, 32 KiB, and 1 MiB. Idle buffer capacity is capped at 16 MiB; oversized
  and unknown-capacity buffers are dropped.

- Use a mutex-backed pool rather than `sync.Pool`. The retention limit and
  lifetime are explicit and are not reset nondeterministically by GC.

- Preserve shared connection-version ordering. `CloseAddrVer` closes either
  normal or reusable-scan pools only when that pool's version is not newer than
  the reported error version.

## Implementation

TiDB changes are concentrated in `pkg/meta`, `pkg/infoschema`, and
`pkg/executor`. The simple decoder clones borrowed MetaKV strings that must
survive the iterator call, interns low-cardinality charset/collation values for
the query lifetime, and writes only projected `COLUMNS` values into bounded
executor storage.

Client-go adds `internal/client/bounded_buffer_pool.go`. `RPCClient` owns a
`reusableScanConnPools` map and one `reusableScanBufferPool`. `connPool` accepts
an optional `mem.BufferPool`; only the reusable-scan pool passes the bounded
pool to `experimental.WithBufferPool`. Close paths manage both pool maps.

Regression tests cover buffer reuse, the 16 MiB retention limit, oversized
buffer eviction, concurrent retention, normal/reusable connection isolation,
and connection-version close semantics.

## Results

All scans returned 1,205,386 rows. The optimized runs kept GOGC at 500 and had
zero automatic and zero forced GC cycles during the query.

| Configuration | Run | Peak RSS delta | Allocated bytes | TiDB CPU | Duration | Automatic GC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| No dedicated gRPC pool | control | 500.25 MiB | 827.05 MiB | 4.52 s | 11.481 s | 1 |
| Dedicated bounded pool | 1 | 90.47 MiB | 168.67 MiB | 4.03 s | 11.242 s | 0 |
| Dedicated bounded pool | 2 | 71.67 MiB | 170.10 MiB | 4.10 s | 11.575 s | 0 |
| Dedicated bounded pool | 3 | 72.19 MiB | 169.89 MiB | 4.09 s | 11.570 s | 0 |

Optimized average peak RSS growth is 78.11 MiB and the maximum is 90.47 MiB.
Average allocation is 169.55 MiB. Relative to the same-protocol control, peak
RSS falls 84.39%, allocation falls 79.50%, and TiDB CPU falls 9.88%; latency is
effectively unchanged.

The diagnostic allocation profile attributes 11.39 MiB to MetaKV transport and
protobuf, including 3.55 MiB in `boundedBufferPool.Get`, 4.17 MiB in reusable
response growth, and 3.52 MiB in `KvPair.Unmarshal`. The previous 650+ MiB
`NopBufferPool.Get` and batch-response materialization hotspots are absent from
the query delta. Remaining sampled cost is led by row materialization/output
(82.28 MiB) and TableInfo scan/decode (41.25 MiB).

## Validation

Client-go functional validation:

    go test ./internal/client -run 'Test(BoundedBufferPool|ReusableScanUsesIsolatedConnPool|CloseAddrVerHandlesReusableScanConnPool|Conn$)' -count=1
    go test ./internal/client ./tikvrpc -count=1
    go test -race ./internal/client -run 'Test(BoundedBufferPool|ReusableScanUsesIsolatedConnPool|CloseAddrVerHandlesReusableScanConnPool)' -count=1
    go vet ./internal/client ./tikvrpc

TiDB Ready validation:

    make lint

The full client tests require local loopback listeners for mock TiKV servers.
The integrated TiDB binary is built from the workspace containing both the TiDB
and client-go modules.

The final local cluster must satisfy all of the following:

- `/debug/gogc` returns `500`.
- `@@GLOBAL.tidb_enable_gogc_tuner` is `0` during the controlled benchmark.
- `SHOW CONFIG` reports `tikv-client.grpc-shared-buffer-pool = false`.
- A full `COLUMNS` scan returns 1,205,386 rows.
- Query-induced peak RSS remains below 200 MiB in every optimized run.
- No optimized run invokes or triggers GC during the measured query interval.

## Artifacts

The detailed result report is at
`../../local_250k_tables/results/metadata_columns_bounded_grpc_pool_noforce_gogc500_20260825/report.md`.
Raw optimized runs are stored beside that report under `run1`, `run2`, and
`run3`. The same-protocol control is under
`../../local_250k_tables/results/metadata_columns_simple_decoder_noforce_gogc500_20260825/control`.

The restored local playground uses
`/private/tmp/tidb-server-columns-bounded-grpc-gogc500-20260825`. Its global
shared gRPC buffer setting is false.
