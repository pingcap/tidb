# Bound memory used by large InfoSchema metadata scans

This ExecPlan is a living document maintained according to `PLANS.md` at the
repository root.

## Purpose / Big Picture

Full scans of `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`, and
`INFORMATION_SCHEMA.TIDB_INDEXES` over a schema with 100,000 tables create a
large amount of short-lived Go memory. The scan repeatedly reads `TableInfo`
JSON from MetaKV, decodes metadata, constructs row-oriented `Datum` values,
and receives TiKV Scan responses through gRPC.

After this change, these three virtual tables use one bounded metadata pipeline:
the executor writes only projected fields into reusable row batches, the
MetaKV scanner stays alive across executor output batches, `COLUMNS` decodes
only the required `TableInfo` fields for ordinary base tables, and client-go
reuses both decoded Scan responses and transport receive buffers on isolated
metadata-scan connections. Ordinary user-table KV traffic keeps its existing
connection pools and gRPC buffer policy.

## Progress

- [x] (2026-08-24) Added bounded query-owned `Datum` row reuse and statement memory tracking.
- [x] (2026-08-24) Kept one borrowed-value MetaKV scanner alive across executor output batches.
- [x] (2026-08-25) Added `COLUMNS`-specific partial `TableInfo` decoding with a full JSON fallback.
- [x] (2026-08-25) Reused `TableInfo`, column, index, and row storage across batches with a 16 MiB retained-capacity limit.
- [x] (2026-08-26) Added caller-owned reusable TiKV Scan response decoding in client-go.
- [x] (2026-08-26) Added an isolated reusable-scan connection per TiKV address and an RPCClient-scoped 16 MiB gRPC buffer pool.
- [x] (2026-08-26) Removed the unrelated diagnostic HTTP API and experimental allocation-counter plumbing from the review diff.
- [x] (2026-08-26) Removed test-only resume APIs and the process-wide `FieldType` JSON pool from the production diff.
- [x] (2026-08-26) Ran the focused TiDB and client-go suites, race checks, vet, Bazel generation, and TiDB Ready lint.
- [x] (2026-08-26) Prepared the final Draft PR description and recorded the review footprint.

## Surprises & Discoveries

- Observation: Reopening the MetaKV scanner every 1,024 output rows reread
  prefetched data and recreated response objects.
  Evidence: allocation profiles showed repeated `slices.Clone`, protobuf
  materialization, and scanner-resume work until the iterator lifetime was
  extended across output batches.

- Observation: Reusing executor rows alone did not control peak RSS.
  Evidence: the dominant remaining allocation was gRPC receive buffering,
  followed by Scan response graphs and complete `TableInfo` JSON decoding.

- Observation: a normal `encoding/json` partial destination still allocates
  nested model values and strings that are not needed by `COLUMNS`.
  Evidence: a narrow decoder for ordinary base-table metadata substantially
  reduced the decode portion, while uncommon metadata shapes still require the
  standard decoder for correctness.

- Observation: gRPC buffer pools are connection-scoped.
  Evidence: request-local transport reuse therefore requires an isolated
  connection; enabling the global shared pool would also change ordinary
  transaction, Cop, batch, and streaming traffic.

## Decision Log

- Decision: Keep projection, partial decode, persistent scanning, reusable
  executor storage, reusable Scan responses, and bounded gRPC buffers in one
  stacked optimization change.
  Rationale: the measured peak is the result of allocations across the whole
  pipeline, and reviewing only one layer would not demonstrate the intended
  end-to-end memory bound.
  Date/Author: 2026-08-26 / Codex

- Decision: Retain at most 16 MiB for each query-owned executor buffer and 16
  MiB in the RPCClient-owned transport pool; discard oversized storage.
  Rationale: reuse removes allocator and GC pressure, while explicit limits
  prevent one unusually wide response or table definition from becoming a
  permanent high-water mark.
  Date/Author: 2026-08-26 / Codex

- Decision: Select the isolated client-go connection only when a Scan request
  carries caller-owned reusable response storage.
  Rationale: ordinary TiKV traffic must preserve its current connection count,
  BatchCommands behavior, codec, compression, and buffer-pool configuration.
  Date/Author: 2026-08-26 / Codex

- Decision: Remove statement allocation counters from the production patch.
  Rationale: they were useful for the local experiment but added cross-package
  API surface and hot-path bookkeeping without contributing to memory reuse.
  Date/Author: 2026-08-26 / Codex

- Decision: Do not add a process-wide pool to `parser/types.FieldType` and do
  not retain callback-based resume APIs once the persistent iterator is in
  place.
  Rationale: neither is required by the metadata scan. Removing them keeps the
  ordinary JSON decode path unchanged and reduces the public review surface.
  Date/Author: 2026-08-26 / Codex

## Outcomes & Retrospective

The preserved 100,000-table fixture has 12 columns and two secondary indexes
per table. In the prior alternating cold-start A/B run for the projected full
`COLUMNS` scan, the combined optimized build reduced average query-window RSS
growth from 371.19 MiB to 85.97 MiB and maximum growth from 481.56 MiB to 91.98
MiB. Process allocation fell from 941.73 MiB to 279.24 MiB, TiDB CPU from 4.893
to 4.267 core-seconds, and duration from 11.064 seconds to 10.470 seconds.

These measurements establish behavior for the fixed local topology and
`GOGC=500`; they are not a universal production upper bound. Multi-TiKV
routing, concurrent metadata scans, isolated-connection churn, and tail
latency remain explicit follow-up test surfaces.

The final review footprint is 21 files with 2,836 additions and 158 deletions
in TiDB, including this ExecPlan, and 10 files with 751 additions and 20
deletions in client-go. Combined, the stacked change is 31 files with 3,587
additions and 178 deletions. The largest file is
`pkg/executor/infoschema_reader.go`, because it contains the shared projected
row builder plus the three table-specific batched emitters. The client-go
portion is isolated behind an explicit reusable Scan response pointer; normal
transactional and coprocessor requests do not select the new connection pool.

## Context and Orientation

`pkg/executor/infoschema_reader.go` owns row construction and batching for the
three large virtual tables. `pkg/infoschema/infoschema_v2.go`, `pkg/meta`, and
`pkg/structure` form the persistent MetaKV iteration and partial decode path.
`pkg/store/driver/txn/snapshot.go` opts these scans into client-go response
reuse. In client-go, `txnkv/txnsnapshot` attaches caller-owned response storage,
`tikvrpc` decodes into it, and `internal/client` routes the request through an
isolated connection using a bounded gRPC `mem.BufferPool`.

A borrowed value is valid only until the underlying iterator advances. The
metadata decoder therefore copies or interns every string retained in a
returned `TableInfo`, while avoiding the previous unconditional clone of the
complete MetaKV value. A retained capacity is a reachable backing array kept
for the next batch; every query-owned retained capacity is charged to the SQL
memory tracker and released when the retriever closes.

## Plan of Work

Keep the three large Information Schema tables on a shared batch retriever.
Build a projection map once, reuse row and `TableInfo` destinations within a
bounded query lifetime, and skip statistics-cache work unless a projected
`TABLES` column needs it. For InfoSchema V2 full scans, open one table metadata
iterator per schema and keep it alive across executor calls. Decode only the
base `TableInfo` and column fields required by `COLUMNS`, falling back to the
complete compatible decoder for uncommon shapes.

For the client boundary, attach reusable Scan response storage only to these
metadata iterators. Route such requests through one reusable-scan connection
per TiKV address. Share a mutex-protected, size-classed transport pool across
those connections and cap its total retained capacity. Preserve connection
version ordering and close both ordinary and reusable pools on client shutdown.

## Concrete Steps

Run focused client-go validation from the client-go repository:

    GOWORK=off go test ./internal/client ./tikvrpc -run 'Test(BoundedBufferPool|ReusableScan|CloseAddrVerHandlesReusableScanConnPool|GetConnAfterClose)' -count=1
    GOWORK=off go test -race ./internal/client ./tikvrpc -run 'Test(BoundedBufferPool|ReusableScan|CloseAddrVerHandlesReusableScanConnPool|GetConnAfterClose)' -count=1
    GOWORK=off go test ./internal/client ./tikvrpc -count=1
    GOWORK=off go test ./txnkv/txnsnapshot -run '^$' -count=1
    GOWORK=off go vet ./internal/client ./tikvrpc ./txnkv/txnsnapshot

Run focused TiDB validation from the TiDB repository. The executor package uses
failpoints, so use its wrapper for targeted tests. Import and dependency changes
also require `make bazel_prepare` before the final Ready profile.

    GOWORK=off ./tools/check/failpoint-go-test.sh pkg/executor -run 'Test(BoundedDatumRows|HugeMemTableRetrieverKeepsTableInfoIteratorAcrossBatches)$' -count=1
    GOWORK=off go test -tags=intest,deadlock ./pkg/meta ./pkg/infoschema ./pkg/structure -run 'Test(Meta|SimpleColumnsTableInfoDecoder|V2Basic|Hash)$' -count=1
    GOWORK=off go test -tags=intest,deadlock ./pkg/planner/core/operator/logicalop/logicalop_test -run 'Test(Columns|InfoSchemaTableExtract)$' -count=1
    GOWORK=off ./tools/check/failpoint-go-test.sh pkg/executor/test/infoschema -run 'Test(TablesTable|ColumnTable|InfoschemaTablesSpecialOptimizationCovered)$' -count=1
    make bazel_prepare
    make lint

## Validation and Acceptance

All focused tests must pass, all three SQL queries must preserve their result
rows and ordering, retained statement memory must return to zero on retriever
close, and ordinary TiKV requests must continue using the existing connection
pool. Reusable metadata Scan responses must reuse capacity after warm-up and
drop capacity above the configured limit. The 100,000-table benchmark must
record peak RSS, cumulative allocation, CPU core-seconds, duration, and GC
cycles under the same binary, topology, SQL projection, and `GOGC` controls.

## Idempotence and Recovery

The focused tests and read-only benchmark are repeatable. The pre-cleanup TiDB
and client-go heads are preserved locally on branch
`codex/infoschema-metakv-grpc-buffer-reuse-full-experiment`. If scope cleanup
removes a required behavior, compare against that branch and restore only the
smallest owning change.

## Artifacts and Notes

The detailed A/B evidence is stored outside the repository under
`local_250k_tables/results/infoschema_metakv_grpc_buffer_reuse_ab_20260826`.
The client-go dependency used by this change is commit
`9796e6f3c6c06a034b5fe041852341a67e5138ce`, represented in TiDB by pseudo-version
`v2.0.0-20260826115000-9796e6f3c6c0`.

## Interfaces and Dependencies

TiDB adds a `kv.ScanResponseRetainedSize` snapshot option and persistent
`TableInfoIterator` interfaces used only by the large InfoSchema retriever.
The partial decoder uses `github.com/tidwall/gjson`, with the standard JSON
decoder as a compatibility fallback. Client-go adds `ReusableScanResponse`, a
per-snapshot retained-size opt-in, an isolated reusable-scan connection map,
and a bounded implementation of gRPC's `mem.BufferPool` contract.

Revision note (2026-08-26): consolidated the five experimental plans into one
end-to-end plan, removed diagnostic and allocation-attribution scope, narrowed
the exported iterator surface, and recorded the final validation and review
footprint.
