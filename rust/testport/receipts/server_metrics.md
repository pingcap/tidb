# `pkg/server/metrics` parity receipt

Status: complete inventory and explicit ownership boundary; no production edit
was required. This receipt covers the complete Go server-metrics package and
does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding the boundary, both tracked artifacts were read in full: 135
total lines. The package has no `doc.go`, test, fixture, generated source,
platform variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 14 | library target and Prometheus/metrics dependencies |
| `metrics.go` | 121 | command-name mapping and server metric-vector initialization |

The two files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

The package is a server-facing Prometheus wiring layer: it binds Go's global
`pkg/metrics` vectors to command constants and resource-group labels, and
exports mutable package variables consumed by the Go server. The Rust tree has
no dependency-closed server connection loop or equivalent Prometheus registry
owner for these vectors; its existing metrics crates cover unrelated planner,
statistics, and execution surfaces. Adding a facade would fabricate consumers
and duplicate global registration. No Rust-only behavior was found to remove,
and no missing Go behavior can be implemented safely without the server
transport owner.

## Validation

Profile: **Ready** for this documentation-only ownership boundary.

- `cmp` against Go master for `metrics.go` and `BUILD.bazel` — passed.
- `go test ./pkg/server/metrics -count=1` — package compile check (no Go test
  files).
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Bazel or Go source/build artifact changed, so `make bazel_prepare` was not
  required.

## Risks and unverified scope

Correctness risk is low because this batch changes no runtime code. Live
Prometheus registration, server command dispatch, generated Bazel execution,
and non-host platform builds remain outside this explicit Rust ownership
boundary.
