# `pkg/ttl/metrics` parity receipt

Status: Audited; no dependency-closed Rust implementation was added. This
receipt covers the complete Go package inventory and records its current
boundary; it is not a repository-wide parity claim.

Comparison source: Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust owner: none in the hparser-integration workspace. The Rust TTL session
owner keeps a narrow `PhaseTracer` trait boundary for the three phases it
uses, but no Rust metric registry owns this package.

## Complete Go inventory

All three tracked artifacts in `pkg/ttl/metrics` were read in full before
editing: 353 lines total. There is no package `doc.go`, fixture or `testdata`
directory, generated source or input, platform/build-tag variant, benchmark,
fuzz target, README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 21 | Go library/test targets |
| `metrics.go` | 262 | TTL Prometheus metrics and phase tracer |
| `metrics_test.go` | 70 | deterministic phase-duration test |

The Go production source, test, and BUILD metadata are byte-identical to
current Go master. The package's only imports are `pkg/metrics` and the
Prometheus client library; neither the complete TTL registry nor its Go
context-value wiring has a Rust owner in this branch.

## Parity finding and boundary decision

No safe source-level fix is available inside this package's dependency
closure. Porting `InitMetricsVars`, all metric vectors, worker-specific
Prometheus counters, and `context.Context` tracer lookup would require a
transcreated `pkg/metrics` registry and its process-wide registration
semantics. The existing Rust session `PhaseTracer`/`NoopPhaseTracer` boundary
therefore remains explicit and limited to `RunInTxn`'s three phase names; no
Rust-only metric behavior or speculative registry was introduced.

The Go `WaterMarkScheduleDelayNames` values, including their source-defined
labels and thresholds, were preserved as source behavior and are not changed
without a Go-master bug-fix request.

## Validation

Profile: **Ready** for this no-code audit.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/ttl/metrics -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate.
- `git diff --check` — passed.

No Rust owner changed and no Go/Bazel artifact changed, so Rust cargo checks
and `make bazel_prepare` were not applicable to this documentation-only audit.

## Risks and unverified scope

- Correctness risk is unchanged: the Go implementation remains authoritative
  for TTL Prometheus metrics and phase durations.
- Compatibility risk is limited to the documented Rust boundary; no API or
  metric labels were altered.
- Performance is unchanged.
- Not verified locally: cross-runtime metric export, process-wide Prometheus
  registration, non-host platforms, and repository-wide integration suites.

The rolling repository audit continues with the remaining package checklist.
