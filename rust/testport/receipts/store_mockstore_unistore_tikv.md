# `pkg/store/mockstore/unistore/tikv` Go-parity receipt

## Source and inventory

- Go comparison source: fetched `origin/master` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the behavior repair is upstream
  commit `2964713e267`.
- The complete package inventory after this batch is 16 artifacts and 9,286
  lines: `BUILD.bazel`; ten production Go files (`deadlock.go`, `detector.go`,
  `inner_server.go`, `mock_region.go`, `mvcc.go`, `region.go`, `server.go`,
  `server_batch.go`, `util.go`, and `write.go`); and five Go test/support files
  (`detector_test.go`, `main_test.go`, `mock_pd_test.go`, `mvcc_test.go`, and
  `util_test.go`).
- There is no `doc.go`, fixture/testdata directory, generated output,
  platform-specific variant, benchmark, fuzz target, nested package, or
  additional build artifact. Every artifact was inventoried before editing.
- Fifteen artifacts were already byte-identical to Go master. `server.go` was
  missing only the 11-line deterministic deadlock failpoint and is now also
  byte-identical.

## Gap and implementation

The hparser branch lacked the `pessimisticLockReturnDeadlock` failpoint used to
exercise a retryable single-statement deadlock after a client-side LOAD DATA
stream has been consumed. The exact Go-master failpoint is restored. Its
consumer fix in `pkg/executor/adapter.go` refuses to retry
`LOAD DATA LOCAL INFILE`, because reopening the executor would send a second
`LOCAL_INFILE_REQUEST` and desynchronize the MySQL packet stream.

The focused Go-master regression in
`pkg/server/tests/commontest/tidb_test.go` is restored as supporting consumer
evidence. Rust models LOAD DATA statement classification and pessimistic retry,
but has no equivalent client-local infile retry/reopen owner; no Rust-only
retry shim was introduced.

## Regression and validation

Before the repair, `TestLoadDataLocalPessimisticRetryDesync` failed because no
deadlock was injected and the LOAD DATA statement succeeded. After restoring
the failpoint and the no-retry guard, the original deadlock is returned, no row
is committed, and the connection remains synchronized:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server/tests/commontest \
  -run '^TestLoadDataLocalPessimisticRetryDesync$' -count=1
```

Ready validation and the Bazel prerequisite outcome are recorded in the
ExecPlan. `make lint` and `git diff --check` pass. The required
`make bazel_prepare` was attempted with the pinned Go environment and failed
only because this workspace has no `bazel` executable.

## Risks and boundary

- Only client-local streams bypass pessimistic retry. Server and remote files
  remain reopenable and retain existing retry behavior.
- The failpoint is test-only and is inert in production.
- This receipt completes the mock TiKV package, not the much larger
  `pkg/executor` or server commontest packages; their remaining deltas stay
  explicit for their atomic audits.
