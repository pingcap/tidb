# `pkg/util/format` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package remains unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Inventory and decision

All four Go artifacts (318 textual lines) were read in full:
`BUILD.bazel`, `format.go`, `format_test.go`, and `main_test.go`. There is no
`doc.go`, generated/platform source, fixture, benchmark, fuzz target, example,
or nested package. The source test covers the indent/flat state machine and
the complete SQL display escape set; the harness only provides common setup
and leak checking.

The formatter state machine remains owned by `tidb-datatype::format`, with
`tidb-util::format` adding the util-specific backslash escape. The audit found
one Rust-only contract: the util `output_format` wrapper was marked
`#[must_use]`, unlike Go's discardable return. A focused deny-lint test failed
before the change and passes after removing only that annotation. Earlier work
already removed unconsumed Rust-only formatter accessors and typed-fragment
traits while preserving the source behavior.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, test, and Bazel artifact.
- [x] Added the focused return-value regression; it failed before the fix.
- [x] Removed the Rust-only `#[must_use]` annotation and passed focused Go/Rust
      tests, formatting, and the pinned Go lint gate.
- [ ] Run broader workspace and Bazel validation when those environments are
      available; this package-level fix is otherwise Ready.

## Validation

Profile: **Ready** for this focused parity fix; no Go or Bazel source changed,
so `make bazel_prepare` is not required.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/format
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/format
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/format -count=1
# all passed in current and /tmp/tidb-go-latest-c605

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test format_contract --offline --locked -- --test-threads=1
# five passed, including output_format_return_value_may_be_ignored_like_go

cd rust && cargo +nightly-2026-08-22 test --manifest-path Cargo.toml -p tidb-datatype --test all parser_format_package_source:: --offline --locked -- --test-threads=1
# eight passed
cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
# all passed
```

The pre-fix focused regression failed with one `unused_must_use` error from
`output_format`; after the annotation removal it passes.

## Risks and unverified scope

- Correctness: the wrapper's output bytes and formatter state machine are
  unchanged; only compile-time diagnostics were relaxed to match Go.
- Compatibility: callers may now discard `output_format` results, while all
  existing consumers remain type-checked.
- Performance: unchanged; no allocation or formatting path changed.
- Not verified locally: Bazel execution, full workspace tests, and live SQL
  consumers beyond the focused formatter source tests.
