# `pkg/util/cgmon` parity boundary ExecPlan

This living ExecPlan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package is unchanged from the earlier audit pin.

## Purpose

Inventory the complete three-artifact Go package and decide whether its
Linux-only process monitor can move as one dependency-closed Rust unit. The
claim must include the ten-second refresh loop, process-global lifecycle,
metrics, panic recovery, cgroup readers, server startup wiring, fallback test,
and Bazel target.

## Progress

- [x] Read all three Go artifacts (229 lines): `BUILD.bazel`, `cgmon.go`,
      and `cgmon_test.go`.
- [x] Read the complete Rust `tidb-util::cgmon` owner and its cgroup,
      memory, metrics, and server consumer seams.
- [x] Compared the latest Go master tree with the Rust branch; no package
      source drift was found.
- [x] Ran the deterministic Go fallback test in both the current worktree and
      an exact detached latest-master worktree.
- [x] Recorded the dependency-closed boundary and refreshed the receipt.
- [ ] Land a future atomic monitor owner when Rust metrics, server lifecycle,
      panic-recovery, and scheduler dependencies are available together.

## Go contract

`StartCgroupMonitor` and `StopCgroupMonitor` are explicitly non-thread-safe
process-global lifecycle calls. Linux starts an immediate refresh followed by a
ten-second ticker and recovers panics in the loop. CPU refresh starts at
`runtime.NumCPU`, applies a positive cgroup quota/period ceiling only when it
is lower, and publishes `metrics.MaxProcs`. Memory refresh starts at physical
memory total, takes a smaller cgroup limit when available, and publishes
`metrics.MemoryLimit`. Read errors are returned to the test seam but do not
terminate the monitor.

## Decision

Keep `pkg/util/cgmon` explicitly unclaimed. `tidb-util::cgroup` and
`memory::process` provide supporting authorities, but no Rust owner currently
contains the Go monitor's scheduler cadence, process-global start/stop
semantics, metric registration/publication, panic recovery, and server
startup/shutdown wiring. Adding a second timer or metric path would be
Rust-only behavior and would split the package claim.

## Validation

Profile: WIP boundary audit (no code fix and no completion claim).

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/cgmon
# passed: exactly three artifacts

git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/cgmon
# passed: no package drift

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/cgmon -count=1
# passed in current and /tmp/tidb-go-latest-c605

git diff --check
# passed
```

Linux cgroup live execution, race/flaky Bazel scheduling, and Rust server
integration remain unverified and outside this explicit boundary.

