# `pkg/server/handler/optimizor` parity receipt

Status: complete inventory and explicit HTTP/domain boundary; no production edit
was required. This receipt covers the complete Go `optimizor` package and does
not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, all seven tracked package artifacts were read in
full: 2,219 total lines. There is no package `doc.go`, fixture, generated
source, platform variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 76 | production/test target, ten-way test sharding, and failpoint dependency |
| `main_test.go` | 72 | common server, TopSQL, failpoint, metrics, and leak-check bootstrap |
| `optimize_trace.go` | 59 | optimizer-trace download HTTP adapter |
| `plan_replayer.go` | 353 | plan-replayer download/forwarding, capture-zip historical-stat enrichment, TOML/schema parsing, and in-memory zip rewrite |
| `plan_replayer_test.go` | 1,072 | plan-replayer dump/load, capture, historical-stat, panic, FK, and regression scenarios |
| `statistics_handler.go` | 169 | current, historical, and priority-queue statistics HTTP handlers |
| `statistics_handler_test.go` | 418 | stats dump/history, partition, priority queue, correlation, and null-file scenarios |

The seven files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

The package is a Go HTTP composition layer. `OptimizeTraceHandler` and
`PlanReplayerHandler` depend on the Go status router, `InfoSyncer` topology
forwarding, external storage, and the `Domain`; the stats handlers additionally
depend on `statistics/handle`, snapshot infoschema lookup, and HTTP response
encoding. Rust has leaf owners for the underlying decisions—
`tidb-domain::optimize_trace`, `tidb-domain::plan_replayer`, historical-stats
and statistics JSON code, and auto-analyze priority-queue snapshots—but no
dependency-closed Rust HTTP/router/domain composition owner for these endpoints.
`tidb-domain` explicitly leaves `plan_replayer_dump.go` and the `Domain` root at
the boundary, which are required by this package's zip and stats paths.

No Rust-only behavior was found to remove and no missing Go behavior can be
implemented safely outside the server/domain/external-storage migration unit.
Adding a parallel HTTP facade would invent a second routing and lifecycle
contract rather than establish parity.

## Validation

Profile: **Ready** for this documentation-only integration boundary.

- `TMPDIR=/tmp PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> ./tools/check/failpoint-go-test.sh pkg/server/handler/optimizor -count=1` — passed; the short temp root avoids the macOS Unix-socket path limit seen under the default temporary path.
- `PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> ./tools/check/failpoint-go-test.sh pkg/server/handler/optimizor -run '^TestDumpPlanReplayerAPI$' -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-domain` with bundled OpenSSL environment — 143 passed, 0 failed; this exercises the Rust optimize-trace, plan-replayer, historical-stats, and related domain owners.
- `cmp` against Go master for all seven artifacts — passed.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

The complete Go package tests pass, including failpoint-enabled panic and
historical-stat paths. Rust live HTTP routing, external-storage streaming,
full `Domain` composition, and non-host platform builds remain outside this
explicit boundary; these are dependency gaps, not silently substituted
behavior.
