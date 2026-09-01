# `pkg/util/cpuprofile` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package owns
the process-wide runtime/pprof sampler and its HTTP/test adapters; no
dependency-closed Rust owner exists.

## Complete inventory

All six Go-master artifacts were read in full before deciding ownership. The
top-level package and its nested test-support package are inventoried
together because the test target imports the latter directly:

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 33 | `b129a73dd6d9b8a6089f9fa27b60218d069c0ca3` | public profiler library and flaky short test target |
| `cpuprofile.go` | 226 | `80a26f12841f3d19c05d58267fc747281f08aa2d` | process-global sampler, registration, lifecycle, and consumer delivery |
| `cpuprofile_test.go` | 250 | `eebb7e0e89b2f1258836431e3d37f4a1c0fda1d8` | lifecycle, parallel-consumer, profile parsing, and HTTP regressions |
| `pprof_api.go` | 208 | `24cc487d33b39a0d1b7e0f5337d7551d5d971401` | collector, profile merge/label filtering, and HTTP adapter |
| `testutil/BUILD.bazel` | 8 | `b7a148358fb6b245d96ea6cb538fbeeb00a38033` | public test-support target |
| `testutil/util.go` | 65 | `7e3567869e25c0ecdb494af396856be68f48243a` | labelled CPU-load goroutine harness (`MockCPULoad*`) |

The package totals 790 textual lines: three production files (including the
nested test utility), one source test, and two Bazel targets. There is no
`doc.go`, README, generated output, platform-specific source, fixture,
benchmark, fuzz target, or additional nested package. The test harness runs
common setup and goleak, exercises duplicate/start-stop and closed-channel
behavior, parses merged Google pprof data, verifies TopSQL's `sql` label is
retained while digest labels are removed, and checks the HTTP timeout/error
contract.

## Go behavior

`StartCPUProfiler` starts one process-global `parallelCPUProfiler`; a second
start returns the exact already-started error and stop is idempotent. The
sampler ticks at `DefProfileDuration` (one second by default), immediately
publishes the previous profile, and only starts runtime/pprof sampling while
at least one registered channel exists. Consumer sends are non-blocking and
panic-safe, while registration/unregistration is mutex-protected. A failed
`pprof.StartCPUProfile` is delivered as `ProfileData.Error` without stopping
the loop.

`Collector` registers a one-slot channel, waits for the first sample, merges
successive Google pprof profiles, removes every sample label except `sql`,
and writes the result on stop. `ProfileHTTPHandler` mirrors Go's pprof
endpoint, applies the request/server `WriteTimeout` guard, and returns the
source status, content headers, and error text. `MockCPULoad` and
`MockCPULoadV2` create labelled busy goroutines solely for those tests.

## Rust ownership and integration decision

Rust has no equivalent process-wide runtime profiler, Google pprof decoder or
profile merge path, labelled goroutine sampler, HTTP pprof handler, or
TopSQL/profile-table consumer. `tidb-server::http_status` explicitly leaves
pprof endpoints as 404, while `tidb-util::memoryusagealarm` only injects a
profile-recorder boundary for heap/alarm side effects. Existing TopSQL and
SEM code therefore cannot satisfy this package's lifecycle and result-data
contract.

Adding a detached sampler, parser, or HTTP endpoint would be Rust-only
behavior without the missing server/infoschema/logging owners and would
duplicate runtime profiler ownership. The package remains explicitly
unclaimed; no Rust production or supplemental test change is justified by
this audit.

## Validation

Profile: **WIP**. This is a complete inventory and explicit boundary audit,
not a package-completion or final repository-readiness claim.

Passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/cpuprofile -count=1
# ok github.com/pingcap/tidb/pkg/util/cpuprofile
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. No Rust
test was run because there is no Rust owner or executable consumer for this
package. The package's source tests are flaky/host-runtime-sensitive and its
full Bazel target was not run.

## Risks and unverified scope

- Correctness: the Go lifecycle, pprof merge, labels, and HTTP tests pass; no
  Rust profiler behavior is claimed.
- Compatibility: runtime/pprof output, labelled SQL samples, HTTP pprof
  availability, and profile-table integration remain unported Rust behavior.
- Performance: no runtime code changed. A future owner must preserve global
  profiler exclusivity, non-blocking consumers, and the one-second sampling
  cadence without adding a duplicate sampler.
- Not verified locally: race/flaky Bazel execution, live pprof HTTP under a
  running Rust server, Linux/macOS runtime profile details, and cross-service
  TiKV/PD profile fetches.
