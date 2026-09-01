# `pkg/server/internal/dump` parity receipt

Status: complete inventory and explicit Rust protocol-owner boundary; no
production edit was required. This receipt covers the complete Go wire-dump
helper package and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, all three tracked artifacts were read in full: 303
total lines. There is no package `doc.go`, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 25 | library/test targets and dependencies |
| `dump.go` | 154 | length-encoded, integer, binary-time, and binary-datetime framing |
| `dump_test.go` | 124 | protocol-width and temporal serialization vectors |

The three files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

`rust/crates/tidb-protocol` is the dependency-closed wire owner. Its result
and prepared-statement modules implement Go's length-encoded integer framing,
`BinaryTime`, and `BinaryDateTime` branches; source-derived tests retain the
zero/negative/fractional duration vectors, date/timestamp shape selection, and
little-endian width cases from `dump_test.go`. The owner is shared with the
column/result-row serializers because these helpers are protocol primitives,
not server-session policy.

No Rust-only behavior or missing Go behavior was found in this package. Adding
a duplicate `dump` facade would create two protocol encoders and risk wire
divergence.

## Validation

Profile: **Ready** for this documentation-only protocol boundary.

- `go test ./pkg/server/internal/dump -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol --test all prepared_statement_protocol_source -- --test-threads=1` — passed the source-derived binary row and temporal dump suite.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

Wire-level dump behavior is covered by the focused Rust and Go vectors. Live
server result-set integration, generated Bazel execution, and non-host platform
builds remain outside this leaf protocol boundary.
