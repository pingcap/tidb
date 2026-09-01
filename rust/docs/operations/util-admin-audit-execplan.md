# `pkg/util/admin` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the four-artifact package is unchanged from the previous pin.

## Inventory and decision

All four Go artifacts (412 lines) were read in full: `BUILD.bazel`,
`admin.go`, `admin_integration_test.go`, and `main_test.go`. There are no
generated/platform/fixture/benchmark/fuzz artifacts or nested packages.

The dependency-closed Rust executor/session owners now preserve count checks,
row/index consistency, clustered-primary omission, partition handles,
corruption errors, and SQL refusal/output paths. The prior clustered-primary
selection gap was fixed with a focused regression. No Rust-only behavior
remains in this package; the Bazel target and embedded-etcd/live storage seams
remain explicitly unverified.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read all production, test, harness, and Bazel artifacts.
- [x] Re-ran the tagged Go integration package test in current and detached
      latest-master worktrees.
- [x] Recorded the current authority and exact artifact hashes in the receipt.
- [x] Pushed the prior source fix and this receipt refresh to
      `hparser-integration`.
- [ ] Run Bazel/sharded and live TiKV/race integration when those environments
      are available.

## Validation

Profile: Ready evidence remains valid for the source fix; this refresh adds no
Go/Rust source and requires no `make bazel_prepare`.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/admin
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/admin
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/util/admin -count=1
# all passed; Go test ran in current and /tmp/tidb-go-latest-c605
git diff --check
# passed
```

Full Rust owner tests, formatting, and pinned lint are recorded in the source
fix receipt. Bazel execution, live TiKV, and race/flaky integration remain
unverified.

