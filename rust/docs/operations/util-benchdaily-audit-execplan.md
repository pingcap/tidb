# `pkg/util/benchdaily` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the four-artifact package is unchanged from the previous pin.

## Inventory and decision

All four Go artifacts (261 textual lines) were read in full:
`BUILD.bazel`, `bench_daily.go`, `bench_daily_test.go`, and `main_test.go`.
There is no `doc.go`, fixture, generated or platform variant, fuzz target, or
nested package. The production owner adapts `testing.B` benchmarks into a JSON
array, while the test-only daily combiner scans the repository for result files
and writes a date/commit envelope. The common harness supplies setup and
goleak exclusions.

Rust has ordinary benchmark targets and source-derived fixed-workload tests,
but no equivalent `testing.B` reflection adapter, `-outfile`/`-date`/`-commit`
flags, repository-wide result-file scan, or CI JSON envelope. This is CI
tooling rather than a database runtime contract; adding a Rust serializer
would create a second Rust-only reporting path. No source change or regression
test is justified.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Ran the package test in current and detached latest-master worktrees.
- [x] Updated the receipt and top-level parity plan with the current authority.
- [ ] Port the harness only if a dependency-closed Rust CI owner is requested.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix or package-completion claim. No `make bazel_prepare` or Ready lint
gate is triggered by this documentation-only refresh.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/benchdaily
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/benchdaily
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/benchdaily -count=1
# all passed in current and /tmp/tidb-go-latest-c605; no-op default path
git diff --check
# passed
```

## Risks and unverified scope

- Correctness: no Rust benchmark-reporting owner is claimed; the default Go
  test intentionally executes no benchmark when flags are absent.
- Compatibility: CI result JSON shape, reflection-derived names, and file
  discovery remain Go tooling contracts.
- Performance: no runtime code changed; a future port must avoid scanning the
  repository during ordinary unit tests.
- Not verified locally: populated CI benchmark files, output encoding errors,
  cross-platform path behavior, and a Rust tooling owner.
