# Protocol workstream

This workstream owns the serialized MySQL transport contract, not session
semantics. The Go source of truth is `pkg/server/internal/packetio.go` and
its source tests in `pkg/server/internal/packetio_test.go`.

The first leaf is `crates/tidb-protocol`: uncompressed four-byte headers,
continuation frames, sequence validation, flush, and the incoming
`max_allowed_packet` guard. Its source evidence is
`corpus/coverage/evidence/tests/protocol-packetio-source-wave.tsv` and its
focused tests are `crates/tidb-protocol/tests/packetio_source.rs`.

Compressed zlib/zstd framing, TLS, authentication, command execution, and
executor-owned result-field derivation are separate source domains. The leaf
now owns the source-shaped command-byte split (`decode_command`, including
TiDB's one trailing-NUL `COM_QUERY` rule), length-encoded integer, text-row,
column-definition, OK/EOF, and logical result-set sequencing primitives. Its
`textrow` module also ports the bounded numeric `AppendFormatFloat` behavior,
dependency-closed decimal text, and byte-preserving scalar formatting;
temporal/JSON/enum/set/vector Datum conversion remain separate source domains.
The `ResultEncoder` leaf now ports Go's registered result-charset precedence
and binary/ASCII/Latin1/UTF-8/GBK byte policy, while the full session charset
registry and encoder lifecycle remain explicit boundaries. The `error_packet`
leaf also ports `clientConn.writeError`'s raw ERR payload ordering for
protocol-41 and legacy clients. `error_conversion` now owns the typed,
source-backed error-kind to MySQL errno/SQLSTATE table without inventing
executor/session message context. The generated `tidb-proto` SelectResponse/
StreamResponse projection is the
corresponding source-checked response-side contract, while raw tipb/chunk
decoding remains in `tidb-distsql::chunk_decode`; typed codecs, packet framing,
flush, and server integration remain caller responsibilities.
The current consumers are
`tidb-exec::Session::execute_framed_query` and its bounded
`execute_framed_query_text_result_set` response seam. Cargo/workspace and
ledger edits belong to the evidence/workspace steward.

Validation:

```bash
CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-protocol
cargo fmt --all -- --check
```
