# DistSQL context workstream

This workstream owns request metadata and detach-safe execution state at the
boundary before TiKV RPC. The Go source of truth is `pkg/distsql/context/**`
and its context tests, beginning with `context_test.go:38 TestContextDetach`.

The first leaf is `crates/tidb-distsql`: request/session identity, replica
read and priority values, paging, warning collection, cancellation/kill
handles, copied CPU/KV state, and fresh max-keys counters on detach. Its
source evidence is
`corpus/coverage/evidence/tests/distsql-context-source-wave.tsv` and its
focused tests live in `crates/tidb-distsql/src/tests.rs`.

Full client variables, runtime statistics, resource groups, protobuf request
encoding, region routing, and raw response decoding remain separate domains.
The dependency-closed `select_iter` leaf now ports the serial `SelectResult`
row/close contract and `channel_iter` ports per-response-channel chunk/row
ordering over owned generic rows. `response_channel` additionally owns ordered
result/warning/error/close events with idempotent lifecycle before raw tipb or
TiKV transport exists. `tidb-proto` now owns a source-checked,
dependency-closed `SelectResponse`/`StreamResponse` projection with exact
field numbers and TiFlash summary dependencies; it is a wire contract only,
not a typed executor decoder. `chunk_decode` and `stream_decode` now validate
response-level encoding and `Chunk.rows_meta` byte ranges; `tidb-codec::value`
and `tidb-codec::column` preserve raw default-row and TypeChunk fixed/variable
framing while preserving default/columnar/CHBlock payloads and StreamResponse
presence as opaque. A bounded `decode_column_datums` leaf now converts
source-proven integer, float, and raw string/binary columns while returning
explicit errors for temporal, decimal, JSON, enum/set, vector, and unknown
types. Native CHBlock decoding, default-row typed conversion, and
intermediate-output routing remain separate. The first real consumer is
`tidb-exec::Session::execute_framed_query`, which writes
the original SQL into `DistSqlContext::request`; `ReadRequestBuilder` and
`RequestEnvelope` now preserve request metadata and source concurrency/limit
policy for the future TiKV request consumer without pretending to execute an
RPC. `TransportRequest` now makes the post-build ownership boundary explicit:
metadata is immutable and unbound until an opaque transport marker claims it;
send-before-bind and repeated bind are errors. It carries no fake endpoint,
protobuf, region, or RPC state. Cargo/workspace and ledger edits belong to the
evidence/workspace steward.

The adjacent `KvRequestBuilder` leaf preserves caller-supplied opaque
`Request.Data` bytes and ordered TiFlash `PartitionIDAndRanges` metadata before
protobuf marshaling, region splitting, or RPC. `tidb-codec::column` supplies
the raw TypeChunk framing consumed by this boundary, derives the source-owned
`FieldType` physical widths, and exposes the bounded scalar Datum leaf;
`CoprocessorRequestEnvelope` now projects the exact tipb request fields while
preserving opaque payload/context and ordered ranges before region/RPC
ownership. `RegionTaskEnvelope` likewise preserves StoreBatchTask region
epoch/peer/range/task/bucket metadata before region lookup, retries, endpoint
selection, or RPC. Packed temporal and BinaryJSON framing are separate from
SQL semantics; opaque Datum codecs and native CHBlock ownership remain
separate.

`RawChBlockChunk` is now the native CHBlock boundary. It validates the
source-shaped envelope and retains borrowed payload/row metadata; typed
ClickHouse Datum decoding and TiFlash execution remain unimplemented and must
not be approximated in this crate.

The TypeDefault path now consumes the source-proven scalar Datum tag subset
with exact payload boundaries. Temporal/Duration, JSON, vector, enum/set, and
schema-aware conversion still return explicit unsupported errors.

The fixed `RawDuration` codec is owned by `tidb-codec`, not this transport
crate: it preserves the eight-byte signed nanosecond payload and source
`MaxFsp` metadata. DistSQL still must not infer SQL duration or timezone
semantics from the raw bytes.

Validation:

```bash
CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-distsql
cargo fmt --all -- --check
```
