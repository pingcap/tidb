# Reuse MetaKV gRPC receive buffers without changing ordinary TiKV traffic

This ExecPlan is a living document maintained according to `PLANS.md`.

## Purpose

The optimized 100,000-table `INFORMATION_SCHEMA.COLUMNS` scan still allocates
about 834 MiB per execution at `GOGC=500`. Query-owned counters observe
658.12 MiB of MetaKV Scan responses, while allocation profiles attribute about
654.78 MiB to gRPC `NopBufferPool.Get`. Reuse those transport buffers without
enabling `tikv-client.grpc-shared-buffer-pool` for ordinary transaction, Cop,
batch, or streaming requests.

## Progress

- [x] (2026-08-26) Created branch `codex/infoschema-metakv-grpc-buffer-reuse` from the current bounded metadata streaming worktree.
- [x] (2026-08-26) Confirmed that gRPC buffer pools are connection-scoped, so request-local reuse requires an isolated connection.
- [x] (2026-08-26) Implemented one isolated reusable-scan connection per TiKV address and one RPCClient-scoped bounded transport buffer pool.
- [x] (2026-08-26) Added deterministic unit coverage for reuse and clearing, retention bounds, concurrent access, request routing, address-version closure, and client closure. The non-race targeted suite passes.
- [x] (2026-08-26) Completed targeted client-go tests, race validation, vet, and a compile-only check of `txnkv/txnsnapshot`.
- [x] (2026-08-26) Built the integrated TiDB binary at `/private/tmp/tidb-server-infoschema-metakv-grpc-buffer-reuse-20260826`.
- [x] (2026-08-26) Ran three alternating cold-start baseline/optimized rounds on the verified 100,000-table fixture. Every query returned 1,205,371 rows.
- [x] (2026-08-26) Compared query-window RSS peak, allocation, GC, CPU, duration, and clean allocation profiles.
- [x] (2026-08-26) Completed the TiDB Ready gate: `GOWORK=off make bazel_prepare` in an ASCII-path mirror produced no content changes, and `make lint` passed.

## Surprises & Discoveries

- Observation: TiDB and client-go are separate Git repositories and both now use
  branch `codex/infoschema-metakv-grpc-buffer-reuse`. The parent
  `implementation/go.work` joins them for the integrated TiDB build.
- Observation: the first optimized allocation run was contaminated by two
  unrelated status-server schema requests. The profile attributed about
  736 MiB cumulatively to `SchemaHandler.ServeHTTP`, including JSON encoding,
  TableInfo loads, ordinary batch RPC, and `NopBufferPool.Get`; those stacks are
  not part of the measured SQL. The run is retained as `optimized/allocation_run1`
  but excluded from comparison. Subsequent runs use status port 10180 through
  `local_250k_tables/config/tidb-benchmark-isolated.toml` and
  `local_250k_tables/tidb_status_port_wrapper.sh`. The wrapper is necessary
  because TiUP passes `--status=10080` on the command line, which otherwise
  overrides the config file.
- Observation: an isolated no-forced-GC peak run allocated only about 185 MiB
  during the query but started with about 517 MiB of Go HeapAlloc close to its
  NextGC target. Touching previously nonresident heap pages produced a much
  larger RSS rise and one automatic GC. This run is retained as a GC-phase
  sensitivity case; primary A/B peak measurements force GC before the query
  and wait 20 seconds for scavenging to settle before taking the baseline.
- Observation: Darwin TiKV v8.5.7 crashed while the 100,000-table fixture was
  being completed in `WriteCompactionFilter::flush_pending_writes_if_need`.
  Both benchmark groups therefore set `gc.enable-compaction-filter=false`.
  The measured workload is a read-only MetaKV scan, so the workaround does not
  alter the path under test.
- Observation: the Chinese workspace path is interpreted as `?????` by parts
  of the Bazel toolchain. The final Bazel gate ran in a fresh ASCII-path mirror
  with `GOWORK=off`; this also prevented Gazelle from pruning the external
  client-go repository because of the parent workspace replacement.

## Decision Log

- Select the isolated connection only when `req.ReusableScanResponse != nil`.
- Keep the existing global `tikv-client.grpc-shared-buffer-pool=false` setting.
- Use one HTTP/2 connection per TiKV address for reusable scans. A connection
  can multiplex concurrent requests, and this avoids multiplying transport
  buffers by the ordinary gRPC connection count.
- Share one mutex-backed buffer pool across all reusable-scan connections owned
  by an `RPCClient`.
- Use 256 B, 4 KiB, 16 KiB, 32 KiB, and 1 MiB size classes. Cap total retained
  backing-array capacity at 16 MiB and drop oversized or excess buffers.
- Clear a retained buffer before reuse to preserve the default gRPC pool's
  data-isolation behavior.
- Keep connection versions ordered across ordinary and reusable-scan pools so
  `CloseAddrVer` cannot close a newer connection after an older error.

## Outcomes & Retrospective

The isolated transport pool removed the dominant short-lived allocation source
without changing the amount of metadata read. Both allocation-profile runs made
3,132 Scan RPCs and received 690,084,907 bytes. The baseline allocated 936.22
MiB in the pprof window, including 686.35 MiB in
`grpc/internal/mem.NopBufferPool.Get`; the optimized build allocated 259.81 MiB,
including 14.61 MiB in that function. MetaKV transport/protobuf allocations
fell from 717.50 MiB to 46.79 MiB.

Across three alternating cold-start rounds, query-window incremental RSS fell
from 371.19 MiB average / 481.56 MiB maximum to 85.97 MiB average / 91.98 MiB
maximum, a 76.84% average reduction. Process `TotalAlloc` fell from 941.73 MiB
to 279.24 MiB, TiDB CPU from 4.893 to 4.267 core-seconds, and duration from
11.064 s to 10.470 s. Each baseline round triggered one automatic GC during
the query; optimized rounds triggered none.

This meets the fixed-shape goal of keeping incremental RSS below 200 MiB. It is
not a universal production upper bound: multi-TiKV routing, concurrent metadata
scans, mutex contention in the shared pool, connection churn, and tail latency
remain explicit follow-up test surfaces. The complete evidence is in
`local_250k_tables/results/infoschema_metakv_grpc_buffer_reuse_ab_20260826/report.md`.

## Validation

Completed validation:

- `GOWORK=off go test ./internal/client ./tikvrpc -run 'Test(BoundedBufferPool|ReusableScan)' -count=1`
- `GOWORK=off go test -race ./internal/client ./tikvrpc -run 'Test(BoundedBufferPool|ReusableScan)' -count=1`
- `GOWORK=off go vet ./internal/client ./tikvrpc`
- `GOWORK=off go test ./txnkv/txnsnapshot -run '^$' -count=1`
- `GOWORK=off make bazel_prepare` in a fresh ASCII-path TiDB mirror; generated Bazel metadata was content-identical to the working tree.
- `make lint` in the same mirror with its parent `go.work` selecting the matching client-go branch.
- Three alternating cold-start A/B rounds plus one clean allocation-profile run per binary, using the preserved 100,000-table dataset, `GOGC=500`, no in-query status polling, and the same projected `COLUMNS` query.
