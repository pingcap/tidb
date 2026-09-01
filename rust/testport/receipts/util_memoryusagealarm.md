# `pkg/util/memoryusagealarm` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
The source is unchanged from the earlier extraction point; this receipt
re-audits the complete current package and its existing Rust seed owner.

## Complete inventory

All three Go-master artifacts were read in full before the ownership decision.
There is no `doc.go`, generated output, platform variant, fixture, or nested
package.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 37 | `1fe7ccb8899182ada14e25cc24ddfc32ad8fa750` | `c58ab2e9cb5ead68b8942eef9e4885cf4ab1a6570e05cef701d9dfb597aa0916` | public library plus flaky short race-enabled test target and all session/memory/profile deps inventoried |
| `memoryusagealarm.go` | 467 | `9698dd5ffb361837441ce57e45440631465ff7e8` | `f54ef8a8a4392a6c47e2a3875c0b21bd08dcc46e8d9c12b8f291f450454ee989` | configuration provider, 100ms monitor, threshold state machine, SQL/profile records, and retention logic inventoried |
| `memoryusagealarm_test.go` | 240 | `82f341a292b5da0df8fdd202310573b43026d8ad` | `c3cdd98649bb67c3d39c14c924fa4e1c65213774118f69a13d9bf9a2c8684f83` | threshold, sorting/formatting, variable-refresh, goroutine-profile test, and benchmark fixtures inventoried |

The package totals 744 textual lines: 23 production function/method
declarations, the `ConfigProvider`, `TiDBConfigProvider`, `Handle`,
`memoryUsageAlarm`, `AlarmReason`, and profile item carriers, plus 12 named
test/benchmark helpers. `Handle.Run` ticks every 100ms until `exitCh`, skips
global memory arbitration, refreshes configuration at most once per minute,
chooses server-limit heap bytes or system memory, records only over-threshold
growth/interval events, writes the top ten SQLs by memory/time, dumps heap and
64MiB-bounded goroutine profiles, and removes older `oom_record` directories.
The source tests cover the 70% threshold, 60-second and 10% growth rules,
ordering/formatting, and variable refresh; the goroutine-profile test and
benchmark intentionally exercise Go runtime stack text.

## Rust ownership and integration decision

`tidb-util::memoryusagealarm` is an existing source-shaped seed owner with
three passing tests for threshold, top-ten formatting, and variable refresh.
It narrows Go's ticker/clock/global-memory reads behind explicit providers and
uses a `ProfileRecorder` seam. `tidb-server` and
`tidb-util::servermemorylimit` share the session-manager snapshot, but no
ordinary Rust startup path constructs/runs the alarm handle, implements the
Go `TiDBConfigProvider`, or records real heap and goroutine profiles. The
source's `util.GenLogFields`/session statement context, runtime/pprof files,
global vardef/config wiring, and race-enabled lifecycle therefore remain
outside one dependency-closed owner. No partial runtime handler or detached
profile sampler is justified; the package remains explicitly unclaimed.

## Validation

Profile: **WIP**. This is a complete inventory and explicit boundary audit
with no code change, so `make bazel_prepare` and the Ready lint gate are not
triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/memoryusagealarm -count=1
# ok

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --offline --locked -p tidb-util memoryusagealarm --lib -- --test-threads=1
# 3 passed
```

## Risks and unverified behavior

- Correctness: threshold, interval, growth, sorting, formatting, and variable
  refresh source cases pass in both the Go package and Rust seed tests.
- Compatibility: profile bytes, SQL log fields, session-manager enumeration,
  global config/vardef updates, and 100ms goroutine lifecycle are still Go
  runtime contracts and are not claimed as Rust parity.
- Performance: Go's monitor allocates a 64MiB stack buffer only on alarm; no
  runtime code changed.
- Not verified locally: race-enabled/Bazel execution, actual heap or goroutine
  profile files, server startup wiring, and live OOM retention under real
  sessions. Rust compiler warnings in unrelated crates remain present.
