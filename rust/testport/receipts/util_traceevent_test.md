# `pkg/util/traceevent/test` — complete package boundary receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The nested integration-test package has exactly two tracked artifacts and 461
lines. All test/support and Bazel lines were read before the ownership
decision. It has eight function declarations and four top-level test entries;
there are no production files, fixtures, generated/platform variants,
benchmarks, fuzz targets, or additional build artifacts.

| Go-master artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 21 | `3ff24035421fb3f0db2ea9498dc1ca6de17daa8b` | flaky integration test target and dependencies |
| `integration_test.go` | 440 | `26c5bd023804e8264a76d7fb58e9549ad3dc704e` | next-gen session/flight-recorder integration tests |

## Rust ownership and boundary

The tests require a complete next-gen TiDB session, mock store, trace-event
flight recorder, logger, and client-go trace-control hooks. Rust's
`tidb-util::traceevent` source tests cover the recorder and adapter at the
unit/owner level, but there is no dependency-closed SQL-session integration
harness equivalent to this package. The root traceevent receipt records the
production owner; this nested package remains an explicit integration
boundary. No Rust-only behavior was removed and no speculative test harness
was added.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
Inventory/read pass: 2 artifacts, 461 lines, 8 declarations, 4 test entries
go test ./pkg/util/traceevent -tags=intest,deadlock -count=1: PASS; 0.468s (root package)
go test ./pkg/util/traceevent/test -tags=intest,deadlock -run '^$' -count=1: interrupted during linker build; no test execution
cargo +nightly-2026-08-22 test -p tidb-util --lib traceevent::tests -- --test-threads=1: PASS; 12 tests
cargo +nightly-2026-08-22 fmt --all -- --check: PASS
Pinned-Go make lint: PASS
git diff --check: PASS
```

The nested integration test was not run because its link step exceeded the
local time budget; live next-gen session, logger, and embedded-store behavior
remain unverified. No Go/Bazel/module source changed, so `make bazel_prepare`
is not required.

## Risks and next boundary

- Correctness: integration assertions depend on session bootstrap, trace
  category propagation, and recorder timing.
- Compatibility: the package is next-gen-only and its `kerneltype` skips are
  part of the Go contract.
- Performance: no production path changed; live recorder volume and SQL
  integration overhead remain unmeasured.

