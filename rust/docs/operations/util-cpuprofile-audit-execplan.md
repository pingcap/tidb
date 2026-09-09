# `pkg/util/cpuprofile` parity boundary ExecPlan

This living ExecPlan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the six-artifact package is unchanged from the previous audit pin.

## Purpose

Inventory the complete process-wide CPU profiler package, including its nested
labelled load harness, and decide whether all production, test, and Bazel
behavior can move as one dependency-closed Rust owner.

## Progress

- [x] Read all six Go artifacts (790 lines): the root BUILD target, two
      profiler production files, source test, nested BUILD target, and
      labelled CPU-load helper.
- [x] Read the full source tests and map lifecycle, duplicate registration,
      profile merge/label filtering, HTTP timeout/error, and goleak behavior.
- [x] Searched Rust owners and consumers for runtime/pprof, profile merge,
      labelled goroutine, HTTP, TopSQL, and profile-table equivalents.
- [x] Pulled the latest Go master and confirmed no package drift.
- [x] Ran the complete Go package test in the current and exact detached
      latest-master worktrees.
- [x] Recorded the explicit dependency boundary in the receipt.
- [ ] Land a future atomic owner when runtime profiler, pprof parser/merge,
      HTTP, metrics, and server/profile-table dependencies are available.

## Decision

Keep `pkg/util/cpuprofile` explicitly unclaimed. The Rust branch has no
dependency-closed process-wide runtime profiler, Google pprof decoder/merge
path, labelled goroutine sampler, HTTP pprof endpoint, or TopSQL/profile-table
consumer. A detached sampler or endpoint would duplicate runtime ownership and
create Rust-only behavior without the required server and logging contracts.

## Validation

Profile: WIP boundary audit (no code fix and no completion claim).

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/cpuprofile
# passed: exactly six artifacts

git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/cpuprofile
# passed: no package drift

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/cpuprofile -count=1
# passed in current and /tmp/tidb-go-latest-c605

git diff --check
# passed
```

The source test is host-runtime-sensitive and flaky/Bazel execution was not
run. Linux/macOS profile details, pprof HTTP integration under a running Rust
server, and cross-service profile consumers remain unverified.

