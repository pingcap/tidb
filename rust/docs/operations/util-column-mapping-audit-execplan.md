# `pkg/util/column-mapping` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package is unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Inventory and decision

All four package artifacts (888 textual lines) were read in full:
`BUILD.bazel`, `README.md`, `column.go`, and `column_test.go`. There is no
`doc.go`, generated/platform source, fixture, benchmark, fuzz target, example,
or nested package. The seven source tests cover rule validation, mapping/cache
lifecycle, DDL handling, partition-ID layout and errors, value rewriting, and
case sensitivity.

The Rust `tidb-util::column_mapping` owner already preserves the Go selector,
partition-ID arithmetic, accepted dynamic numeric types, DDL return tuple, and
simple Unicode lowercasing. Earlier parity work removed checked partition-size
policy, Rust-only clone/error/diagnostic surfaces, and supplemental tests; the
retained source-derived tests cover the focused gaps. No current Go drift or
new missing behavior was found, so no source change is justified in this
authority refresh.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, README, and Bazel artifact.
- [x] Re-ran Go package tests in current and detached latest-master worktrees.
- [x] Updated the receipt and top-level parity plan with the current authority.
- [ ] Run the full Ready profile again when a source change is made.

## Validation

Profile: **WIP** for this documentation-only refresh; no Go/Rust source or
Bazel metadata changed, so `make bazel_prepare` is not required.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/column-mapping
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/column-mapping
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/column-mapping -count=1
# all passed in current and /tmp/tidb-go-latest-c605
git diff --check
# passed
```

## Risks and unverified scope

- Correctness: all seven Go tests and existing Rust source-derived regressions
  remain the focused evidence; no new behavior changed.
- Compatibility: partition-ID bit layout, signed overflow probes, dynamic
  numeric conversion, and DDL tuple semantics remain cross-language contracts.
- Performance: no runtime code changed; mapping cache synchronization remains
  the native implementation detail.
- Not verified locally: Bazel execution, live importer/DDL consumers, and
  race-enabled mapping tests.
