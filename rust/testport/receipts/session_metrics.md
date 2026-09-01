# `pkg/session/metrics` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains two tracked artifacts and 158 lines. Every production
source and Bazel target was read in full before comparing the Rust workspace.
There is no `doc.go`, test file, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `78cf4465a8c4b658e2a939bfce95ca29034f0dc6` | `eababdcb872e00f4bb73813c73fb61147dd325fbd37b0c5445eabfe6754c6ffc` | public session-metrics target and Prometheus dependency |
| `metrics.go` | 146 | `31aa904291bce69df76ccf7bf9411f187670e34f` | `88b50dacd6d4d43d36e21759ad58cefde13d9fc9bbed92a358ea127f5550e41b` | session metric handles and label-bound initialization |

The production surface defines two functions (`init` and
`InitMetricsVars`) and initializes 49 exported Prometheus counter/observer
handles. The handles bind non-transactional DML, per-transaction statement
and duration outcomes, retries, parse/compile timing, CTE and partition
telemetry, account-lock telemetry, index-merge use, and batched-store use to
the shared `pkg/metrics` families. There are no package-local tests; all
declarations and both build artifacts were checked individually.

## Rust ownership and explicit boundary

Rust currently owns only a small label-level leaf in
`tidb-exec::session_metrics`: the three non-transactional DML kinds and
their exact `delete`/`insert`/`update` labels. It deliberately does not
register Prometheus families, expose observer handles, initialize session
metrics, or cover the remaining statement/transaction and telemetry
families. Those behaviors cross the session, executor, metrics, and
telemetry consumers and have no dependency-closed Rust owner today.

No Rust-only behavior was found to remove, and no safe missing behavior can
be implemented by adding isolated counters or a second metrics registry.
That would risk duplicate registration, label cardinality/order changes, and
observable telemetry drift. This complete Go package is therefore recorded
as an explicit SEED/boundary; future parity requires one coordinated
Prometheus/session metrics owner and its consumers.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were
not required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/session/metrics -count=1)
# passed: pkg/session/metrics [no test files]
```

The package was compiled from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, full Go repository tests, live Prometheus registration/scraping,
or a future dependency-closed Rust implementation of all session metrics.

This receipt certifies the bounded `pkg/session/metrics` inventory and
ownership decision; it is not a repository-wide transcreation claim.
