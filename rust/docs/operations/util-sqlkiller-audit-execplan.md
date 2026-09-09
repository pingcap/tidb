# `pkg/util/sqlkiller` parity audit ExecPlan

This living ExecPlan records the complete Go-package audit and restoration of
the concurrent-reset SQLKiller behavior. The repository-wide rolling audit
continues after this package.

## Purpose / Big Picture

Keep kill-signal state, event-channel generations, kill reasons, and reset
ordering aligned with current Go `master`. Signal writers and reset must share
one lock so a concurrent reset cannot close or clear the wrong event generation
or lose a newly arriving kill reason.

## Progress

- [x] (2026-09-02) Read and inventoried all three Go-master artifacts at
      `origin/master` `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 277-line
      `sqlkiller.go`, 110-line `sqlkiller_test.go`, and 30-line `BUILD.bazel`
      (417 lines total). There is no package doc, fixture, generated/platform
      variant, benchmark, or additional harness.
- [x] (2026-09-02) Compared the complete package with the hparser branch. The
      Go-master delta is the 93-line SQLKiller lock/order fix, the 110-line
      `TestSQLKillerConcurrentReset` regression, and its Bazel test target.
- [x] (2026-09-02) Restored locked signal/event transitions, reason capture,
      reset ordering, and the source failpoint interleaves in Go; added the
      source test and BUILD metadata.
- [x] (2026-09-02) Demonstrated the regression failed before the fix in a
      detached worktree (old `getKillError` signature), then passed in focused
      and full failpoint-wrapped package tests.
- [x] (2026-09-02) Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next package.

## Surprises & Discoveries

- The Rust `tidb-util::sqlkiller` owner already carried the lock and event
  generation contract, while the Go hparser branch was missing the current
  source implementation and all Go regression/build artifacts.
- `pkg/util/sqlkiller` uses production failpoints and the new test enables
  call-site hooks, so the canonical failpoint wrapper is required for Go
  validation.

## Decision Log

- Decision: restore the exact Go-master state machine and test/build artifacts
  rather than changing Rust consumers. Rationale: Rust owner parity already
  exists; the missing behavior is in the Go package and the fix is dependency-
  closed to these three artifacts. Date: 2026-09-02, Codex.

## Validation

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/util/sqlkiller -run '^TestSQLKillerConcurrentReset$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/util/sqlkiller -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make bazel_prepare

Expected results are passing focused/full failpoint-wrapped Go tests, passing
lint, and clean diff hygiene. `make bazel_prepare` is required because the
BUILD target and a new top-level Go test were added; it is blocked locally by
the missing `bazel` executable.

## Risks

- Correctness: lock scope intentionally covers signal CAS, reason, event
  trigger, and reset; logging remains outside the lock as in Go master.
- Compatibility: all existing SQLKiller APIs remain source-compatible; the
  new test target is marked flaky like the source target.
- Performance: uncontended signal paths add one mutex critical section, while
  logging and error construction remain outside it.

## Outcomes & Retrospective

The Go SQLKiller package now has the current concurrent-reset state machine,
source regression, and Bazel test metadata. Rust owner tests and downstream
consumer checks remain covered by the existing parity receipt.
