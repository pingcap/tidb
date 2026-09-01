# `pkg/util/selection` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package is unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Inventory and decision

All four Go artifacts (433 textual lines) were read in full:
`BUILD.bazel`, `selection.go`, `selection_test.go`, and `main_test.go`. There
is no package doc, README, fixture, generated/platform source, or nested
package. The source tests cover empty/duplicate/random/serial selection; the
test file also registers seven sizes across introselect, quickselect, and sort
benchmarks.

The `tidb-util::selection` owner already preserves the signed `-1` empty result,
introselect and median-of-medians fallback, source quickselect comparison,
target-width index behavior, and the HashAgg percentile consumer. Earlier work
removed Rust-only `Option`, saturation, `is_empty`, diagnostics, and duplicate
tests. No current Go drift or new Rust-only behavior was found, so no source
change is justified in this authority refresh.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Re-ran Go package tests in current and detached latest-master worktrees.
- [x] Updated the receipt and top-level parity plan with the current authority.
- [ ] Re-run the full Ready profile when a source change is made.

## Validation

Profile: **WIP** for this documentation-only refresh; no Go/Rust source or
Bazel metadata changed, so `make bazel_prepare` is not required.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/selection
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/selection
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/selection -count=1
# all passed in current and /tmp/tidb-go-latest-c605
git diff --check
# passed
```

## Risks and unverified scope

- Correctness: the four Go source tests and existing Rust owner tests remain
  the focused evidence; no new behavior changed.
- Compatibility: signed empty results, duplicate handling, fallback depth, and
  benchmark registration remain cross-language contracts.
- Performance: no runtime code changed; benchmark data remains Go-owned CI
  tooling and Rust's native benchmark harness is unchanged.
- Not verified locally: Bazel execution, ten-million-element benchmark runs,
  and race-enabled selection tests.
