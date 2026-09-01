# `pkg/server/handler/tests` HTTP test-package ExecPlan

## Progress

- [x] (2026-09-02) Pinned Go master and inventoried all five package
  artifacts, including the complete HTTP test harness.
- [x] (2026-09-02) Restored the DXF cleanup-size API subtest, next-gen
  user-keyspace maintenance-route check, and TiFlash replica summary lifecycle
  test that were missing from the hparser branch.
- [x] (2026-09-02) Preserved the existing storage-history error-code/category
  regression and adapted the keyspace fixture to the locally pinned kvproto
  API without changing module dependencies.
- [x] (2026-09-02) Ran focused failpoint-aware tests for DXF and TiFlash routes;
  both pass, with the maintenance case skipped on classic.
- [ ] Complete the shared Ready gates, publish the combined batch, verify and
  pull `origin/hparser-integration`, then continue the rolling audit.

## Scope

This is test-only consumer coverage for Go session-backed HTTP behavior. Rust
has no dependency-closed HTTP handler server or TiFlash/DXF integration owner;
the tests remain a Go integration boundary rather than a Rust test-port claim.

## Inventory

Read all five artifacts before editing: `BUILD.bazel`, `dxf_test.go`,
`http_handler_serial_test.go`, `http_handler_test.go`, and `main_test.go` (3,591
lines after the restoration). There are no production, fixture, generated,
platform, benchmark, fuzz, nested, or package-local testdata artifacts. The
remaining profiling-log and collate/old-kvproto differences are explicit
branch boundaries.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server/handler/tests \
  -run 'Test(TiFlashReplicaSummary|DXFAPI)$' -count=1
./tools/check/failpoint-go-test.sh pkg/server/handler/tests \
  -run '^TestDXFMaintenanceAPINotAvailableInUserKeyspace$' -count=1
```

`make bazel_prepare` is required for the changed Go test imports and remains
blocked locally by the unavailable `bazel` executable. The package receipt
records the complete inventory, ownership boundary, and validation evidence.
