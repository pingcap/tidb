# `pkg/server/handler/tests` Go-parity receipt

## Complete inventory

- Go comparison source: fetched `origin/master` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02 rolling baseline).
- This test-only package contains exactly five artifacts and 3,591 lines after
  restoration: `BUILD.bazel` (82), `dxf_test.go` (673),
  `http_handler_serial_test.go` (855), `http_handler_test.go` (1,909), and
  `main_test.go` (72). Every production-equivalent test, fixture/harness, and
  build file was read completely before editing. There is no production file,
  generated/platform variant, package-local testdata, benchmark, fuzz target,
  or nested package.
- Go master has the same five artifacts and 3,630 lines. The remaining 39-line
  difference is intentional branch context: profiling-log assertions track a
  server-side logger not present on this branch, the row encoder uses the
  current collate-aware API, and the locally pinned older kvproto exposes
  `KeyspaceMeta.Id` rather than Go master's oneof wrapper.

## Restored coverage

The hparser branch had removed the DXF cleanup-size API subtest, the next-gen
user-keyspace maintenance-route check, and the complete TiFlash replica summary
HTTP test. This batch restores those Go-master scenarios and keeps the existing
DXF history consumer regression: sensitive task-error text is never returned,
while stable `ErrorCode` and `ErrorCategory` fields remain available. The tests
now exercise GET/POST validation, memory-only response shape, keyspace route
isolation, schema reload behavior, stale-summary semantics, columnar-storage
state, dropped-table leftovers, and method/query errors.

Rust has no dependency-closed Go session-backed HTTP test server or TiFlash/DXF
handler integration owner. No Rust-only test behavior was found to remove and
no disconnected Rust harness was added; this package remains Go-native consumer
coverage for the restored HTTP contracts.

## Validation

Failpoints are used by the test server, so the canonical wrapper enabled and
disabled them around each run:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server/handler/tests \
  -run 'Test(TiFlashReplicaSummary|DXFAPI)$' -count=1
./tools/check/failpoint-go-test.sh pkg/server/handler/tests \
  -run '^TestDXFMaintenanceAPINotAvailableInUserKeyspace$' -count=1
```

Both focused runs pass (the maintenance test is skipped on the classic kernel).
The full package suite, external-etcd test, and Bazel regeneration are not run
locally; `make bazel_prepare` was attempted and is blocked because `bazel` is
not installed in this workspace. The existing focused history test and Ready
lint/diff evidence are retained in the prior receipt history.
