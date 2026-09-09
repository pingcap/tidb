# `pkg/util/context` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package is unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Inventory and decision

All five Go artifacts (757 textual lines) were read in full:
`BUILD.bazel`, `context.go`, `plancache.go`, `warn.go`, and `warn_test.go`.
There is no package doc, generated/platform source, fixture, benchmark, fuzz
target, example, or additional harness. The source tests cover JSON warning
round trips, the no-op singleton, static-handler copy/truncate/retention, and
the complete warning storage surface.

The Rust `tidb-util::context` owner remains dependency-closed for value-store,
warning, plan-cache, and range-fallback behavior. The audit found five
Rust-only compile-time diagnostics on return values that Go permits callers to
discard: two static-handler constructors and three plan-cache accessors. A
focused `#[deny(unused_must_use)]` regression failed with five errors before
the change and passes after removing only those annotations. Existing warning,
plan-cache, and consumer tests remain unchanged.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Added the focused return-value regression; it failed before the fix.
- [x] Removed the five Rust-only `#[must_use]` diagnostics and passed focused
      Rust/Go tests, formatting, and the pinned Go lint gate.
- [ ] Run broader workspace and Bazel validation when those environments are
      available; this package-level fix is otherwise Ready.

## Validation

Profile: **Ready** for this focused parity fix; no Go or Bazel source changed,
so `make bazel_prepare` is not required.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/context
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/context
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/context -count=1
# all passed in current and /tmp/tidb-go-latest-c605

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test context_contract --offline --locked -- --test-threads=1
# six passed, including context_return_values_may_be_ignored_like_go

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
# all passed
```

The pre-fix focused regression failed with five `unused_must_use` errors; it
passes after the annotations are removed.

## Risks and unverified scope

- Correctness: warning state transitions, JSON, and plan-cache behavior are
  unchanged; only compile-time diagnostics were relaxed to match Go.
- Compatibility: callers may discard the five results as in Go; all current
  consumers remain type-checked.
- Performance: unchanged; no locking or warning-storage path changed.
- Not verified locally: Bazel execution, full workspace tests, and live SQL
  consumers beyond the focused owner/consumer suites.
