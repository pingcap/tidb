# Region group lifecycle evidence, 2026-09-29

This is a seed integration repair within the **whole** upstream package
`pkg/store/mockstore/unistore/cophandler`, not a package completion claim.
`source-inventory.json` retains every file in that package, including build and
test artifacts. The preceding aggregation receipt inventories its dependencies.

## Reference and root cause

Go master: `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
Rust baseline: `17ad39d246ace8a787e6e32ce5ab9687688a14c5`.

Follow the live call chain: `HandleCopRequest` → `handleCopDAGRequest` →
`buildAndRunMPPExecutor` → `buildMPPAgg` → `aggExec`. Both TypeAggregation and
TypeStreamAgg share typed `PBToExpr` group expressions, `codec.EncodeValue` keys,
a lookup map, and first-seen group order. Empty or fully filtered input produces
no groups. `Aggregation.streamed` does not select a different implementation.

Rust instead chose a contiguous-group path from that deprecated field, sorted
hash groups by key, rejected computed keys, and used collation keys/hash codes
with an ad hoc separator. The repair replaces those alternatives with typed
expressions and one insertion-ordered group lifecycle for table and index scans.
It reuses the request's typed row and shared codec/timezone, with one hash-map
entry lookup per input row and aggregate state allocation only for new groups.

The initial investigation followed the older closure executor. Full request
capture exposed that this is no longer the live dispatch path. Consequently,
legacy special COUNT and empty StreamAgg NULL behavior was **not** introduced.

## Capture and regression

`go-oracle.txt` is an overlay test for the owning Go package. It uses its existing
test store helpers, writes rowcodec or nonunique-index KVs, and executes real
coprocessor requests. It closes readers, transactions and its temporary store.

The initial execution archive was `8936d7bdcb`. Before publication, it was
refreshed to master including its current client-go module pin. All 7,726 tracked
files were verified against master by Git blob hash, with failpoints disabled.
The final Go run passes and produces byte-identical responses for every request.
See the inventory for exact references.

The executable fixture is
`../../../crates/tidb-unistore/testdata/region-group-go.tsv.gz`. Its 11,232 rows
contain distinct requests. Tab-separated columns are:

1. Scenario name.
2. Encoded `coprocessor.Request`, hex.
3. Comma-separated input KV pairs, `keyhex:valuehex`.
4. Concatenated `SelectResponse.Chunks.RowsData`, hex.
5. Error, UTF-8 hex, empty for success.
6. Comma-separated warning strings, UTF-8 hex.

Coverage includes COUNT(constant), COUNT(column), MIN, grouping without aggregate
functions, global/direct/constant/computed/compound keys, NULL, signed floating
zero, strings with case-insensitive collation, decimal, datetime, timestamp,
duration and JSON. Table and index scans run in both directions; both executor
types run with absent/false/true streamed metadata, empty/populated ranges, and
absent/false/true selection. Group-only requests require at least one group key.

The absent field is removed at its protobuf field path before Go handles the
request: gogo's bool otherwise marshals explicit false. This prevents duplicate
default/false cases. All captured requests succeed without warnings. The Rust
regression compares row bytes, errors and warnings; execution timing/statistics
and the complete protobuf response envelope are outside this fixture's scope.

`rust-before.json.gz` records all 5,824 baseline differences. The baseline was
tested with only the regression module attached to unchanged production source.
All 11,232 cases pass after the repair. The preceding 3,129 aggregate-function
cases still pass. Run `python3 rust/docs/planner/region-group-lifecycle-20260929/verify.py`
to verify the fixture, failure inventory and artifact hashes.

## Remaining structural work and risks

The whole package remains incomplete. Rust still flattens a restricted executor
composition instead of building Go's live executor tree. This affects composition
with projection, aggregation, caps, and other executor types. Also, live Go
`aggExec` calls `GetResult` and materializes declared result FieldTypes; Rust's
existing distributed finalization still calls `GetPartialResult`, notably a
different AVG contract. These require a separate complete lifecycle investigation
with live-request fixtures; this grouping receipt does not close them.

MPP exchange, paging, intermediate output, all expression diagnostics, complete
Go test/support/variant validation and the previously documented unsupported
aggregate functions remain open. No full-package parity claim is made.

First-seen order and interleaved-group reuse intentionally change incorrect Rust
results. Both executor types now retain one state per distinct key, as Go does;
that uses more memory than the removed contiguous-only path. Hash lookup replaces
ordered-tree lookup, but sysbench/TPC-C/TPC-H/YCSB throughput and real TiKV have not
been measured in this change.
