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

The unused tidb-exec label-only model and its private harness were retired.
It had no production caller and never registered or incremented metrics. Its
three label strings did not validate Go's session instrumentation. This dated
Go inventory remains historical evidence; complete shared metric ownership and
consumer validation must be established from live sources before acceptance.
See the current audit index for later connected repairs and remaining findings.

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
