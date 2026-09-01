# `pkg/ingestor/errdef` parity receipt

Status: Completed the missing-Go-behavior fix and recorded the complete
package inventory. This receipt covers the error-definition package; it is
not a repository-wide parity claim.

Comparison source: Go `origin/master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the pre-fix commit.

## Complete inventory

Before editing, the two original tracked artifacts in `pkg/ingestor/errdef`
were read in full: 76 lines. The focused regression added a third artifact,
making the post-fix package 110 lines. There is no package `doc.go`, fixture or
`testdata` directory, generated source or input, platform/build-tag variant,
benchmark, fuzz target, README, or ownership artifact.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 9 | — | — | public library target (post-fix includes the test target) |
| `errors.go` | 67 | — | — | normalized ingest/global-sort errors and HTTP status error |
| `errors_test.go` | — | — | — | `ErrTooManyDataFiles` message/RFC-code regression |

The branch had removed Go master's `ErrTooManyDataFiles` sentinel even though
the current global-sort merge planner uses it. The fix restores its exact
message template and `GlobalSort:TooManyDataFiles` RFC code and adds the
focused contract test. The pre-fix test failed to compile with an undefined
symbol; the post-fix test verifies both formatted error text and code identity.

## Rust ownership and boundary

Rust has TiDB/MySQL error catalogs and DXF step metadata, but no ingestor error
owner, global-sort merge planner, or write-and-ingest HTTP/RPC client. None of
the Go RFC codes or `IsKVDiskFullError`/`HTTPStatusError` contracts is consumed
by a Rust ingest path. Adding only an unused Rust error constant—especially the
new target-file-limit error without its planner—would invent a disconnected
API and would not implement the Go behavior.

No dependency-closed Rust ingestor error-definition owner or global-sort merge
planner exists. The Go definitions remain authoritative until the ingest path
is ported as a complete package.

## Validation and risk

Profile: **Ready** for this restoration batch.

```text
Pre-fix: `go test ./pkg/ingestor/errdef -run TestTooManyDataFilesErrorContract -count=1`
# failed as expected with undefined ErrTooManyDataFiles

Post-fix: `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/ingestor/errdef -count=1`
# passed

`make bazel_prepare` was attempted for the new test target but is unavailable
locally (`bazel: No such file or directory`). `make lint` and `git diff --check`
pass under the Ready profile.
```

Not verified here: Bazel generation, global-sort merge planning end-to-end,
TiKV ingest RPC behavior, non-host platforms, or full workspace tests.
