# `pkg/util/deeptest` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the three-artifact package is unchanged from the previous pin.

## Inventory and decision

All three Go artifacts (503 textual lines) were read in full:
`BUILD.bazel`, `statictesthelper.go`, and `statictesthelper_test.go`. There is
no `doc.go`, generated/platform variant, fixture, benchmark, fuzz target, or
nested package. The helper exposes recursive reflection assertions with glob
path options for ignored fields and pointer identity; the source test covers
structs, pointers, slices, arrays, maps, interfaces, functions, channels,
invalid values, and expected failures.

Rust test modules contain local assertions and comments derived from this
helper, but no reusable reflection comparator with its path-glob, pointer
identity, and intentional-failure semantics. This is test-only infrastructure;
adding a generic Rust comparator would create Rust-only test policy without a
production contract. No source change or regression test is justified.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Ran the complete Go test package in current and detached latest-master
      worktrees.
- [x] Updated the receipt and top-level parity plan with the current authority.
- [ ] Port a shared comparator only if a dependency-closed Rust test owner is
      requested.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix or package-completion claim. No `make bazel_prepare` or Ready lint
gate is triggered by this documentation-only refresh.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/deeptest
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/deeptest
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/deeptest -count=1
# all passed in current and /tmp/tidb-go-latest-c605
git diff --check
# passed
```

## Risks and unverified scope

- Correctness: the full Go failure matrix passes; no Rust comparator owner is
  claimed.
- Compatibility: pointer alias, map/slice storage, invalid-value, and glob
  path semantics remain Go test infrastructure contracts.
- Performance: no runtime code changed; a future test-only port must preserve
  reflection-walk termination and avoid production dependencies.
- Not verified locally: Bazel execution, architecture-specific reflect
  behavior, and any Rust suite that would consume a shared comparator.
