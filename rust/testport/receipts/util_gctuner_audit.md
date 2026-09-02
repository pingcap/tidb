# `pkg/util/gctuner` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at
`94eb995357f34b7bab4889a82f0405797046447d` (2026-09-02). No Rust crate is a
dependency-closed owner for this package. The package's direct Go behavior is
runtime- and lifecycle-bound, so this audit records the complete boundary
without adding a detached Rust timer or GC policy.

## Complete inventory

All nine Go artifacts were read in full before making the integration
decision:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 41 | `16de85d2c84f0c5d3a2c6ab91b7a4929221d63f6` | `b92b2f8f24f50a08e92de2f245307e5d78b99058d24ff2b66e0e002bad05d503` | library/test targets, five-way shard and race metadata |
| `finalizer.go` | 58 | `3a08a5d3a9057b59a38ce8f3f415fd2fa8267293` | `777ff693485b557b0136c05efe13260314bde006f6c323716cb6bbde6774e136` | repeating GC finalizer and stop flag |
| `finalizer_test.go` | 58 | `8c641c06c7882b5157597da4d6d905e5096d11c4` | `1471a917ebbc64db7143f202c290987a741dac9f72ff13c514b1417b6e9599ff` | eight-cycle callback/stop regression |
| `mem.go` | 24 | `e1a4fff9607995ad30d05e251658ca4b436f2a79` | `77bb13d95d52ae22d451d0a9dc41ec20989e7db24e43b5f6a7b57a75e2167605` | HeapInuse read boundary |
| `mem_test.go` | 31 | `4cf0b7504ae0803c7741b89d9c47b48ee6f8201c` | `54c8bea10417b18c29ed66aef852c0d1e4c262b1411af007da60391e51063b3d` | 100 MiB live-allocation assertion |
| `memory_limit_tuner.go` | 202 | `b3580c0eeef4277d37031e524b9bb85c18e3cedc` | `f308c3ebe342f09aadf091484b661ed2683c99428c0e6f0da8fbfd32bec286f7` | memory-limit finalizer, adjustment window, and fallback policy |
| `memory_limit_tuner_test.go` | 275 | `4064a949858ddbe774d93fb8f317c0749ad453c8` | `d7f9133a7124722dbb727471045bce18e86bfe57bba571cb06c745715ecdd355` | global tuner, issue-48741, and disable/enable tests |
| `tuner.go` | 190 | `9324cda85f703d9f89b770a1c556f25fd7fc43cb` | `08652f74d67bd8936bb06dd8d17e1532aa8ab8abdcbab499739243a044d7d370` | GOGC threshold tuner and percent clamps |
| `tuner_test.go` | 114 | `ebd1cf9ca2bdd3d7e2a3e7445616a13f825b6e95` | `c7afffc5997249108d19bcb62eae9a79c0ba0cb02c9aca2ec076fed34952e7ee` | live tuner and arithmetic table tests |

The baseline contains 993 Go/Bazel lines, 30 production function/method
declarations, four exported tuner APIs, seven source tests, and no
`doc.go`, generated file, fixture tree, platform variant, benchmark, example,
or nested package. The test target also embeds Go's common runtime setup via
the repository test harness.

## Go behavior

`finalizer` uses a self-reinstalling `runtime.SetFinalizer` callback to invoke
the tuner on every GC until an atomic stop flag is set. `tuner` reads
`runtime.MemStats.HeapInuse`, computes
`floor((threshold-inuse)/inuse*100)`, and clamps to the package min/max
percentages while publishing GOGC through the ordinary util setter. The
singleton `Tuning` API starts, retunes, or stops that finalizer; `GetGOGC`
returns the effective tuned or environment default percentage.

`memoryLimitTuner` tracks the configured server limit and trigger percentage,
uses Go's `runtime/debug.SetMemoryLimit`, and recognizes the previous GC's
memory-limit trigger. It temporarily raises the limit to 110% for one minute
(three seconds in tests), records the GC counters, and then restores the
configured limit. Disable/enable nesting and both failpoint-controlled races
are part of the Go-master source contract. `readMemoryInuse` is the only
package helper; `mem_test.go` deliberately exercises the actual runtime
allocator.

The current Go-master arbitration delta is included in this package batch:
`calcMemoryLimit` caps the percentage at the server limit while global
arbitration is active, and the obsolete callback/reset path is removed. The
focused regression covers both the capped fallback and restoration of the
configured server limit.

## Rust ownership and integration decision

Rust has supporting pieces in `tidb-util::memory` (process statistics and
global arbitration), `tidb-util::memoryusagealarm`, and
`tidb-util::servermemorylimit`, but none owns this package's repeating
finalizer, Go runtime GOGC/SetMemoryLimit controls, or singleton state. The
Rust server's memory-limit path is a separate configuration/runner contract;
it cannot stand in for Go's GC trigger tuner.

The source consumers are cross-cutting: `pkg/domain`, `pkg/resourcemanager`,
BR restore, session variables, and server startup all coordinate the tuner and
global memory arbitrator. A partial Rust finalizer, synthetic GC percentage,
or independent memory-limit thread would have no ordinary consumer and would
create Rust-only scheduling/GC policy. The package remains explicitly
unclaimed until those runtime, metrics, server, and test dependencies can land
atomically. The Go production/test behavior is restored in this batch; no Rust
source changed.

## Validation

Profile: **Ready** for this package-level parity batch. The focused regression
failed before the fix because arbitration allowed a 110% memory limit and
passes afterward; the complete package suite also passes with failpoints
enabled and disabled by the repository wrapper.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/util/gctuner -run '^TestSetMemoryLimit$' -count=1
# FAIL before fix: expected 1073741824, got 1181116006

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/util/gctuner -run '^TestSetMemoryLimit$' -count=1
# PASS

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/util/gctuner -count=1 -vet=off
# PASS; all package tests in 15.927s
```

The existing focused tuner, finalizer, memory, and issue-48741 tests were
also rerun in the canonical failpoint wrapper. `make lint` and
`git diff --check` pass. `make bazel_prepare` was attempted because Go source
and test behavior changed, but is blocked by the unavailable local Bazel
executable.

## Risks and unverified behavior

- Correctness: arithmetic and lifecycle tests pass through the canonical
  failpoint workflow; no Rust implementation is claimed.
- Compatibility: Go runtime finalizers, GOGC, and SetMemoryLimit have no
  portable Rust equivalent. The global-memory-arbitration and server-limit
  owners must not be silently substituted for this package.
- Performance: no runtime code changed. A future owner must preserve
  process-global singleton state, GC cadence, and fallback reset intervals.
- Not verified locally: race-enabled Bazel execution, non-Go runtimes,
  server/BR restore end-to-end wiring, and cross-platform runtime behavior.
