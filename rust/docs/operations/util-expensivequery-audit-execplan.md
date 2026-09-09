# `pkg/util/expensivequery` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the three-artifact package is unchanged from the previous pin.

## Inventory and decision

All three Go artifacts (220 textual lines) were read in full:
`BUILD.bazel`, `expensivequery.go`, and `expensivequery_test.go`. The package
has no `doc.go`, README, generated output, platform-specific source, fixture,
benchmark, fuzz target, or nested package. The test file is only the common
`TestMain`/goleak harness and defines no source tests.

The Go owner is a process-wide server/session monitor: it polls every 100 ms,
reloads expensive-query and transaction thresholds, records ongoing-transaction
histograms, throttles warning logs, enforces execution and auto-analyze time
limits, applies resource-group runaway kills, and handles bootstrap memory
quota logging. Rust currently owns the threshold variables and session kill
signals, but not the dependency-closed polling worker, session-manager
enumeration, metrics/logging consumers, kill policies, or domain bootstrap
registration. Adding a timer or threshold-only helper would be Rust-only
behavior, so no source change is justified.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Ran the package test in current and detached latest-master worktrees.
- [x] Updated the receipt and top-level parity plan with the current authority.
- [ ] Port the monitor only after a dependency-closed Rust server/session owner
      and focused lifecycle/kill/metric regressions exist.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix or package-completion claim. No `make bazel_prepare` or Ready lint
gate is triggered by this documentation-only refresh.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/expensivequery
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/expensivequery
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/expensivequery -count=1
# all passed; no tests to run, in current and /tmp/tidb-go-latest-c605
git diff --check
# passed
```

## Risks and unverified scope

- Correctness: no Rust monitor is claimed; the Go harness has no executable
  source tests.
- Compatibility: process-list fields, threshold reload cadence, histogram
  labels, logging intervals, and kill flags remain Go-only integration
  contracts.
- Performance: no runtime code changed; a future owner must preserve the
  100 ms polling cadence and throttling windows.
- Not verified locally: domain bootstrap wiring, live session/auto-analyze and
  runaway kills, metric contents, race/flaky Bazel execution, and server
  end-to-end behavior.
