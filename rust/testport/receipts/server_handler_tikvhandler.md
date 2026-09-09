# `pkg/server/handler/tikvhandler` Go-parity receipt

## Source and inventory

- Go comparison source: fetched `origin/master` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- The complete package inventory is six tracked/source artifacts after this
  batch: `BUILD.bazel` (84 lines), `dxf.go` (533), `dxf_test.go` (47),
  `flash_replica.go` (129), `flash_replica_test.go` (86), and
  `tikv_handler.go` (2,421), for 3,300 lines.
- The Go-master package inventory is five artifacts (the same package files
  except the focused regression file) and 3,211 lines. There is no `doc.go`,
  fixture/testdata directory, generated output, platform-specific variant,
  benchmark, fuzz target, nested package, or additional build artifact.
- Every production and existing test artifact was read before editing. The
  production files and `dxf_test.go` are now byte-identical to Go master;
  `flash_replica_test.go` is the added focused regression source.

## Gap and implementation

The hparser branch had deleted Go master's `flash_replica.go` and its BUILD
entry, removed `DXFTaskCleanupBatchSizeHandler`, and renamed the deprecated
handler from `FlashReplicaDeprecatedHandler` to `FlashReplicaHandler`. The
server routes and HTTP API documentation were missing the corresponding live
TiFlash summary and cleanup-size endpoints. The batch restores the complete Go
behavior, including the global `tidb_columnar_storage_enabled` vardef/sysvar
needed by the summary response, and registers the exact Go-master routes while
preserving unrelated server changes.

Rust inventory found model/InfoSchema and lower-level TiFlash replica owners,
but no dependency-closed Rust HTTP router/domain owner for these handlers. No
Rust facade or cache-only substitute was invented; the HTTP/domain boundary is
recorded explicitly.

## Regression and validation

`TestParseFlashReplicaReloadQuery` covers omitted, `false`, `true`, `1`, `0`,
and invalid values. `TestFlashReplicaSummarySysvar` guards registration and
the default value. `TestDXFTaskCleanupBatchSizeHandler` covers GET, malformed
and out-of-range POST values, valid updates, and memory-only JSON output. Before
restoration the package could not compile because the missing summary source
referenced an absent `vardef.TiDBColumnarStorageEnabled` symbol; the focused
tests now pass.

Ready-profile commands run:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server/handler/tikvhandler -count=1
go test ./pkg/server/handler/tikvhandler -count=1
go test ./pkg/server -run '^$' -count=1
go test ./pkg/sessionctx/vardef -count=1
go test ./pkg/sessionctx/variable -run '^TestMaxExecutionTime$' -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The focused and full tikvhandler suites, server compile, vardef suite, variable
target, lint, and diff checks pass. The required `make bazel_prepare` was also
attempted with the pinned Go environment and is blocked only because this
workspace has no `bazel` executable.

## Risks and boundary

- The summary is intentionally best-effort and follows Go's non-linearizable
  InfoSchema snapshot/reload semantics; `can_disable` is not a distributed
  lock.
- The cleanup size and columnar-storage setting are process/global-sysvar
  state, respectively, and are not persisted by these HTTP APIs.
- Full server integration tests that require a running TiDB/TiFlash cluster,
  Bazel regeneration, and Rust HTTP integration are not run locally.
