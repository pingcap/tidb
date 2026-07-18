# Codec and row-identity workstream

Owns TiDB's byte-compatible scalar/key encoding and the row-handle contracts
that consume it. The authoritative Go sources begin at
`pkg/util/codec/{number,bytes,decimal,codec}.go` and `pkg/kv/key.go`; later
row/table work expands through `pkg/util/rowcodec/**` and
`pkg/tablecodec/**` only as real consumers arrive.

This is a dependency-closed vertical lane. A codec worker reads the complete
owning Go routine and its original tests, implements a production Rust API,
and proves the bytes against a Go-generated fixture or differential oracle.
Round-trip-only tests are insufficient: two mutually wrong encoders/decoders
can agree, and comparable encodings must match Go byte-for-byte at every sign,
length, and decimal-group boundary.

`tidb-datatype` owns scalar meaning and exact decimal storage parts;
`tidb-codec` owns serialization; `tidb-txnkv` owns `Handle` identity and map
semantics. Do not duplicate a scalar type in the codec, expose Go's base-1e9
decimal word layout, represent common handles as unchecked opaque fixtures,
or reproduce Go `unsafe.Sizeof`/map-capacity accounting. Cross-crate additions
must land with an immediate consumer and executable original-test evidence.

The first slices are intentionally narrower than full codec parity:
comparable number/byte/float/decimal datum keys feed
Int/Common/Partition Handle, and `bytes.go`'s raw plus compact byte APIs are
complete. Natural and fixed-schema decimal encoding, exact sizing, decoding,
and typed truncation/overflow are also complete at the codec boundary. The
minimal row/index table-key classifier is a complete source leaf as well. The
bounded BinaryJSON type/value framing and duration payload boundary are now
source-backed, but JSON operations and typed Datum/session behavior remain
open. The broader value codec, SQL temporal/Duration semantics (the packed integer
boundary is isolated in `tidb-datatype::PackedTime` and
`tidb-codec::temporal`), JSON/vector flags, general/unicode collation weights,
row formats, table-key layout, portable memory accounting, and
reproducible allocation benchmarks remain separate source obligations until
their dependencies exist.

`pkg/util/rowcodec/common.go` is not dependency-closed: it couples the physical
row format to full temporal/enum/set/bit/JSON/vector datatypes, schema column
metadata, timezone conversion, and runtime keyspace policy. Build those typed
owners first, then move `common.go`, `row.go`, `encoder.go`, and `decoder.go`
atomically with their original codec/checksum tests. Do not grow a partial row
format around placeholder scalar kinds.

The bounded `tidb-codec::RowLayout` leaf now isolates the source-owned row
header, small/large column-ID and offset widths, sorted ID lookup, null/default
decision, opaque value ranges, and checksum trailer metadata from
`pkg/util/rowcodec/{common,row}.go`. It deliberately does not claim row
encoding/decoding, typed schema conversion, handles, runtime keyspace-prefix
removal, or checksum calculation; those seams remain explicit before the full
rowcodec move.

`tidb-codec::encode_raw_row` now owns the inverse metadata decision without
pretending to be a typed row encoder: it accepts schema-independent
`RawRowColumn` entries whose non-null bytes are already opaque, sorts not-null and
null ID partitions independently, chooses one-/two-byte or four-byte metadata
when an ID or total payload crosses the source thresholds, and preserves caller
buffer prefixes. `encode_raw_int` and `encode_raw_uint` preserve rowcodec's
compact little-endian 1/2/4/8-byte payload widths; they are deliberately
distinct from the fixed-width mem-comparable integer helpers. Datum conversion,
timezone and error policy, checksum/handle calculation, schema defaults, and
decoder reuse remain open owners.

`tidb-codec::RowDecoder` now adds the dependency-closed inverse seam from
`pkg/util/rowcodec/decoder.go`: it delegates physical framing to
`RowLayout`, returns the source not-null/null/missing distinction with borrowed
opaque value bytes, and decodes the source compact signed/unsigned 1/2/4/8-byte
payload widths. Invalid widths are classified as typed errors instead of
reaching Go's panic-prone default 8-byte branch. The schema-aware Datum,
chunk, handle, timezone, old-row, and default-value paths in `decoder.go`
remain explicit gaps until their owners can move together.

`DecimalWireMetadata` is the current decimal framing hand-off: it reads the
source precision/scale header and packed payload length, reports the exact
consumed/remainder boundary, and never rounds or materializes a SQL decimal.
`RawDuration` similarly preserves only the signed nanosecond payload and FSP
metadata. SQL range checks, rounding, timezone conversion, and warning policy
belong to typed/session owners and must not be inferred from these wire leaves.
`RawDuration::parts` adds Go's sign/hour/minute/second/microsecond decomposition
with sub-microsecond truncation; SQL TIME range, rounding, and warning policy
remain outside this physical owner.
The typed `tidb-datatype::truncate_overflow_mysql_time` owner now clamps raw
duration values to MySQL TIME endpoints and reports overflow direction without
constructing session warnings or errors.
`tidb-datatype::round_duration_fsp` owns FSP normalization and half-away-from-
zero rounding, including carry and negative values; warning/session error
construction remains outside the datatype primitive.
`tidb-datatype::parse_duration` now owns the bounded signed `HH:MM[:SS]` and
day-prefixed grammar, fraction parsing/carry, FSP normalization, and TIME
endpoint clamp with typed overflow. Compact `HHMMSS` forms, including short and
leading-zero forms, are covered too; date/datetime fallback, Unicode trimming,
and statement warning/session policy remain open.
`classify_duration_datetime_fallback` now preserves Go's compact-12,
compact-14, and separated date/time routing decision as a typed signal without
performing calendar conversion or attaching session warnings.
`DurationParseEvent` exposes overflow, datetime-fallback, and truncation events
without warning text or session mutation; the session owner still chooses
warning versus statement-error policy.
`RawJsonTemporal` extends the BinaryJSON boundary for DATE/DATETIME/TIMESTAMP
values by preserving Go's type code and little-endian packed calendar bits.
FSP, timezone, calendar validation, and SQL temporal conversion remain outside
this physical codec owner.

Focused checks run from `rust/` with 12 jobs:

```sh
cargo test --locked -j 12 -p tidb-codec -p tidb-txnkv -p difftest-transaction-tests
cargo clippy --locked -j 12 -p tidb-codec -p tidb-txnkv -p difftest-transaction-tests --all-targets -- -D warnings
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --queue tidb-codec
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue transaction
```
