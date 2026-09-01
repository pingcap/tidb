# `pkg/server/internal/testserverclient` parity receipt

Status: complete inventory and explicit integration-harness boundary; no
production edit was required. This receipt covers the complete Go test-server
client support package and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, both tracked artifacts were read in full: 3,159
total lines, including all 55 methods/helpers and every SQL, TLS, load-data,
status, metrics, schema-state, and failpoint scenario in the source file. There
is no package `doc.go`, test file, fixture, generated source, platform variant,
benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 33 | test-support library target and integration dependencies |
| `server_client.go` | 3,126 | database/sql client, HTTP status client, and integration scenario harness |

The two files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

This package is a Go integration harness, not a server runtime owner. It binds
`database/sql` drivers, TiDB `testkit`, failpoints, TLS certificates, DDL job
state, Prometheus text metrics, and hundreds of SQL assertions into one
server-process test lifecycle. Rust has no dependency-closed equivalent of the
Go testkit/server bootstrap or this scenario suite. Its protocol source tests
cover individual wire contracts, but cannot replace the live Go integration
environment without inventing a second harness and database lifecycle.

No Rust-only behavior was found to remove and no missing Go behavior can be
implemented safely in this test-support package outside the server/testkit
migration unit.

## Validation

Profile: **Ready** for this documentation-only integration boundary.

- `go test ./pkg/server/internal/testserverclient -count=1` — package compile
  check (no Go test files).
- `cmp` against Go master for `server_client.go` and `BUILD.bazel` — passed.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required;
  failpoint enable/disable was not applicable because no test command ran a
  failpoint-bearing test function.

## Risks and unverified scope

Live SQL, TLS, load-data, DDL failpoint, metrics, and status API scenarios are
intentionally not rerun here; they require a running TiDB integration server.
Generated Bazel execution and non-host platform builds remain outside this
explicit harness boundary.
