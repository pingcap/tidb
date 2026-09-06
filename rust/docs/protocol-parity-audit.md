# pkg/server/internal parity audit (baseline a85e0fd5df)

Full audit of Go `pkg/server/internal` (packetio, parse, resultset+cursor,
column+convert, advertisedstatus, util, dump, handshake) against
`rust/crates/tidb-protocol` plus the tidb-server seam.

## Fixed this batch (behavior)

1. `command.rs`: `COM_SHUTDOWN` (0x08) and `COM_CHANGE_USER` (0x11) now
   decode to owned `Command::Shutdown`/`Command::ChangeUser` variants
   instead of falling into `Unknown`; Go answers both
   (conn.go:1554/1567). The read-only Rust SQL node answers them with its
   declared unsupported-commands error, next to FieldList/ResetConnection.
2. `error_conversion.rs` + `resultset_stream.rs`: an unrenderable result
   datum now maps to `ErrorKind::InvalidType` (Go `err.ErrInvalidType`,
   8057, column.go:175/238) via the new
   `ResultSetStreamError::error_kind()`, instead of the 1105 unknown
   fallback. Regression: `invalid_type_maps_to_go_err_invalid_type`.

## Documented obligations and narrowings

- `advertisedstatus/checker.go` (5 s-timeout HTTP identity poll) has no
  Rust counterpart and is now NAMED in the crate-header obligation list
  next to TLS/auth/metrics/dispatch.
- zstd encoder level scale differs (klauspost 1-4 vs zstd-crate 1-22);
  frames stay wire-compatible.
- Header-overflow rejection (Rust) and decompressed-length validation
  (Rust) are defense-in-depth additions over Go's silent truncation.
- Type-vector reuse reports MissingPreviousTypeVector up front where Go
  fails later as MalformPacket (same error outcome); the
  utf8mb4-hardcoded execute wrapper is fallback-only — the low-level
  decode keeps the client-charset parameter.

## Verified matching (highlights)

Packet framing with 0xffffff continuation and the exact-multiple
zero-length terminator; per-frame sequence inc; compressed envelope
(7-byte header, 1 MiB batch, >50 min-compress, zlib level 6, zstd
default, zero-len verbatim); sequence mismatch semantics incl. MariaDB
compressed-mode ignore; dump length-encoded helpers, BinaryTime
1-byte-days quirk, BinaryDateTime shapes; column dump (def/meta, 256
truncation, 0x0c, charset policy, dumpType/dumpFlag, decimal pad);
DumpBinaryRow bitmap math and type arms; text row NULL/type matrix and
float E-format rules; StmtFetch len==8 + min(fetch,1024); COM_QUERY/
STMT_PREPARE NUL trim; parseBinaryParams full matrix incl. the
NULL-bitmap quirk; STMT_EXECUTE split gates and new-params-bound
semantics; prepare response layout; ERR/OK/EOF packets incl.
DEPRECATE_EOF OK-shaped EOF.

## Validation

- `cargo test -p tidb-protocol` (110 incl. the new 8057 mapping),
  `cargo build -p tidb-server`, `cargo fmt`, `git diff --check`,
  `make lint`.
