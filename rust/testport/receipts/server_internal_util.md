# `pkg/server/internal/util` parity receipt

Status: complete inventory and mixed ownership boundary; no production edit was
required. This receipt covers the complete Go server utility package and does
not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, all four tracked artifacts were read in full: 467
total lines. There is no package `doc.go`, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 25 | library/test targets and dependencies |
| `buffered_read_conn.go` | 84 | buffered net.Conn wrapper and liveness probe |
| `util.go` | 188 | length-encoded/null-terminated protocol helpers, charset input decoder, CORS/test config |
| `util_test.go` | 170 | length-encoded and null-terminated helper tests |

The four files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

`rust/crates/tidb-protocol/src/binary_params.rs` owns the protocol helpers
`ParseLengthEncodedInt`, `ParseLengthEncodedBytes`, and `ParseNullTermString`;
its source-derived `server_internal_util_source` test covers NULL, width,
truncation, and terminator behavior. The same crate's binary-parameter path
owns the charset input-decoder consumer.

The remaining Go helpers (`BufferedReadConn`/`IsAlive`, `CorsHandler`, and
`NewTestConfig`) are adapters over Go's `net.Conn`, `net/http`, and full server
configuration. Rust has no dependency-closed server listener/config owner for
these APIs. Adding substitutes would invent transport or test bootstrap
behavior. No Rust-only behavior was found to remove and no missing Go behavior
can be implemented safely outside the server/session migration unit.

## Validation

Profile: **Ready** for this documentation-only mixed boundary.

- `go test ./pkg/server/internal/util -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol --test all server_internal_util -- --test-threads=1` — passed the source-derived protocol helper tests.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

Protocol helper correctness is covered by the Go and Rust source tests. Live
TCP liveness deadlines, HTTP CORS handling, server test configuration, and
non-host platform builds remain outside the explicit Rust ownership boundary.
