# `pkg/store/mockstore/unistore/tikv` Go-parity receipt

## Source and inventory

- Go comparison source: fetched `origin/master` at
  `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`; the behavior repair is upstream
  commit `2964713e267`. The package has no source diff between the previous
  audited authority (`c6054025ed4c32ab3672a2a24ea46892714d21ec`) and this
  current master authority.
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

## Rust follow-up: utility helper alignment

The Rust owner now implements the remaining `util.go` behavior in
`rust/crates/tidb-unistore/src/tikv_util.rs`: half-open end-key comparison,
in-place sorted de-duplication, mutation/raw-key/user-key FarmHash pipelines,
and nil-preserving byte cloning. The existing source-backed FarmHash
implementation in `tidb-txnkv` is re-exported for reuse rather than copied;
the 64-bit values therefore remain identical to `github.com/dgryski/go-farm`.
The Go table vectors are live Rust regressions in
`tests_tikv_util_go_parity.rs`, including empty, duplicate, binary, and
mutation/key entry points.

Before this batch those three table tests were ignored parity stubs and there
was no Rust utility owner. The focused Rust utility tests now pass (four tests).
The ordinary `tidb-unistore` test target still has an unrelated pre-existing
compile blocker in its parent `InProcessClient` test seam: the full transport
construction requires `SynchronousBatchRequestDispatcher`, which that client
does not yet implement. The focused utility run used a temporary validation
only implementation of that parent seam and removed it before this commit;
the production diff remains limited to the utility owner and shared FarmHash
export.

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
- The Rust utility functions have no caller in the current unistore execution
  graph, so `cargo check --lib` reports dead-code warnings; keeping them
  crate-visible preserves the complete helper contract for the pending scan
  and coprocessor caller alignments.
