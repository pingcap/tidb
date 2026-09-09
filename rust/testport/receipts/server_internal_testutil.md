# `pkg/server/internal/testutil` parity receipt

Status: complete inventory and test-support ownership boundary; no production
edit was required. This receipt covers the complete Go test utility package and
does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, both tracked artifacts were read in full: 79 total
lines. There is no package `doc.go`, test, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 8 | test-support library target |
| `testutil.go` | 71 | no-op `net.Conn` over a byte buffer and TCP-port helper |

The two files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

`rust/crates/tidb-protocol/tests/server_internal_testutil_source.rs` provides
the Rust test-only counterpart. It preserves byte-buffer reads, no-op writes,
close/deadline/address methods, and IPv4/IPv6 port extraction without exposing
an unrelated production socket abstraction. The packet-reader assertion covers
the actual server-test usage.

No Rust-only behavior or missing Go behavior was found. Keeping this as a
test-only support type avoids polluting production crates with a Go-specific
mock connection.

## Validation

Profile: **Ready** for this documentation-only test-support boundary.

- `go test ./pkg/server/internal/testutil -count=1` — package compile check (no
  Go test files).
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol --test all server_internal_testutil -- --test-threads=1` — passed both source-derived tests.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

The support type is intentionally test-only; live server test harnesses,
generated Bazel execution, and non-host platform builds remain outside this
boundary.
