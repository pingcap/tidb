# Scalar-subquery statement-boundary Go-parity receipt

## Source and inventory

- Go comparison source: fetched `origin/master` at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the exact upstream repair is
  commit `2c5dbbe51bbe4809abc62b315857c274035547ba`.
- The affected top-level `pkg/executor` inventory is 165 artifacts and 96,694
  lines: 87 production Go files, 76 Go tests, one `BUILD.bazel`, and one
  ownership artifact. The affected top-level `pkg/session` inventory is 24
  artifacts and 17,521 lines: 14 production Go files, eight Go tests, one
  `BUILD.bazel`, and one ownership artifact. Both inventories include every
  top-level production/test/build artifact; nested directories remain separate
  Go packages and are not claimed by this supporting repair.
- This receipt records a focused cross-package Go behavior restoration, not a
  complete transcreation claim for either large package. Their remaining
  Go-master deltas stay explicit for later atomic package audits.

## Gap and implementation

`SessionVars.MapScalarSubQ` was reset only from logical-plan construction.
Fast PointGet, cached plans, and transaction replay can bypass that path, so a
later statement could inherit scalar-subquery plan nodes from an earlier one.
The batch restores Go master's statement-boundary reset in
`executor.ResetContextOfStmt` and the retry-history reset immediately before
`RebuildPlan`.

The focused Go-master tests are restored in `pkg/executor/adapter_test.go` and
`pkg/session/tidb_test.go`, with the required session BUILD dependencies. The
server consumer regression restored in the following `pkg/server` batch also
exercises a scalar statement followed by a prefetched PointGet.

Rust has planner/executor scalar-subquery ownership, but it does not expose the
Go process-global `MapScalarSubQ` registry or Go transaction-history replay
path. No Rust-only cache or reset facade was added.

## Regression and validation

Before the fix, the server consumer failed with `expected []int{1, 0}` and
`actual []int{1, 1}` for both the registry and flattened scalar trees. After
the two resets were restored, the executor lifecycle suite, transaction replay
suite, and the exact server consumer case pass under failpoint lifecycle
management:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/executor \
  -run '^TestScalarSubqueryRegistryLifecycle$' -count=1

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/session \
  -run '^TestScalarSubqueryRegistryTxnReplay$' -count=1

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp \
./tools/check/failpoint-go-test.sh pkg/server \
  -run '^TestPrefetchPointKeys4Delete/scalar_followed_by_a_prebuilt_PointGet$' \
  -count=1
```

Ready validation and the Bazel prerequisite result are recorded in the batch
ExecPlan. `make lint` and `git diff --check` pass. The required
`make bazel_prepare` was attempted with the pinned Go environment and failed
only because this workspace has no `bazel` executable.

## Risks and boundary

- The reset occurs before every statement plan selection, including fast and
  cached plans; the current statement repopulates the registry as needed.
- Retry clears the registry per history item, preventing cross-statement plan
  retention without changing transaction retry semantics.
- Full `pkg/executor` and `pkg/session` sweeps are intentionally outside this
  focused repair; they remain mandatory when those complete packages are
  claimed.
