# `pkg/util/profile` — Go-master parity boundary receipt

Status: complete inventory; no dependency-closed Rust owner exists, so no
partial profiling implementation is claimed.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has seven direct artifacts, all read in full:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 49 | public library plus flaky short test target and fixture glob |
| `flamegraph.go` | 189 | profile samples to sorted tree rows and percentage formatting |
| `flamegraph_test.go` | 104 | 40-row fixture-backed flamegraph regression |
| `main_test.go` | 33 | common test setup and goroutine-leak exclusions |
| `profile.go` | 154 | pprof/CPU collection, profile dispatch, and goroutine parsing |
| `profile_test.go` | 82 | end-to-end performance-schema profile table queries |
| `testdata/test.pprof` | 1,206 bytes | gzip-compressed pprof fixture (SHA-256 `892f2aef3b5e42ab0b82d8fac8be5c3063858f3c054f08e02aea5c68fec5680b`) |

There is no `doc.go`, platform/build-tag variant, generated source, benchmark,
fuzz target, nested package, or additional fixture. The two production files
contain 20 function/method declarations (including private flamegraph and
collector helpers); the tests contain `TestMain`, `TestProfileToDatum`, and
`TestProfiles`. Go master adds only test/build wiring: CPU profiling is
started for the CPU table and each profile query now asserts its structured
"profiling request received" log entry.

## Rust owner and boundary

Rust currently has no profile collector, pprof/flamegraph decoder, CPU
profiler adapter, goroutine text parser, or performance-schema profile table
executor. `tidb-util::sem` and `sem_compat` preserve the six table names, but
that name registry is not executable profile behavior. No Rust call site
constructs `Collector`, and no ordinary SQL path can provide the Go
`ProfileGraph`/`ParseGoroutines` contract.

Implementing a standalone parser or sampling thread in `tidb-util` would
create a cache/test-only path without the missing `infoschema/perfschema`,
session result-set, `cpuprofile`, and logging consumers. The Go fixture and
both tests are therefore retained as the source boundary; this audit makes no
Rust production or supplemental test change.

## Validation

Profile: WIP boundary audit in the continuing package loop; no Ready fix
claim applies.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/profile -run '^TestProfileToDatum$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest
  ./pkg/util/profile -run '^TestProfiles$' -count=1` — passed.
- `git diff --check` — passed for the audit changes.

No Go or Bazel file changed, so `make bazel_prepare` is not required. No Rust
test was run because no Rust owner or executable consumer exists. The Go
embedded SQL test remains host/runtime-specific and does not prove a Rust
implementation.

## Risks and unverified scope

Correctness and compatibility remain unimplemented at the Rust boundary:
profile rows, goroutine parsing, CPU sampling, and request logging are not
available to a Rust SQL session. Performance is unchanged because no sampler
or thread was added. A future atomic package must include the pprof decoder,
cpuprofile lifecycle, six table result schemas, ordinary infoschema dispatch,
and source fixture/test harness before it can claim parity.
