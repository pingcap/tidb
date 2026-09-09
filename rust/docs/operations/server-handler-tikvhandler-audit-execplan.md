# `tikvhandler` Go-parity restoration ExecPlan

## Objective

Restore the complete `pkg/server/handler/tikvhandler` behavior that exists at
the fetched Go `master` revision, while keeping the package boundary honest for
the Rust transcreation. The hparser branch had removed live TiFlash summary and
DXF cleanup-size HTTP behavior and renamed the deprecated TiFlash handler.

## Scope and inventory

The package was inventoried file-by-file before implementation: `BUILD.bazel`,
`dxf.go`, `dxf_test.go`, `flash_replica.go` (missing on the branch and read from
Go master), and `tikv_handler.go`. There is no package doc, fixture/testdata,
generated/platform variant, benchmark/fuzz artifact, or nested package. The
batch adds one focused regression file. Supporting `vardef/tidb_vars.go` and
`variable/sysvar.go` entries are restored because the summary endpoint reads
the Go global sysvar.

## Implementation steps

1. Restore Go-master `flash_replica.go` and its BUILD source entry.
2. Restore `DXFTaskCleanupBatchSizeHandler` and its memory-only GET/POST
   contract; restore the deprecated handler's Go-master name.
3. Register `/tiflash/replica` and
   `/dxf/schedule/task_cleanup_batch_size`, and restore their API documentation.
4. Add focused parser, sysvar-registration, and HTTP regression tests.
5. Run failpoint-aware package tests, Ready validation, and record the Bazel
   prerequisite failure when `bazel` is unavailable.

## Validation and exit criteria

The focused and full Go package tests, server compile, supporting vardef/
variable checks, `make lint`, and `git diff --check` must pass. Production
package files must match Go master byte-for-byte; any Rust owner remains a
lower-level model/InfoSchema boundary because no dependency-closed Rust HTTP
server owner exists. Receipt:
`rust/testport/receipts/server_handler_tikvhandler.md`.
