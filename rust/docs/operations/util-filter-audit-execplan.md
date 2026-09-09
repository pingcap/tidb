# `pkg/util/filter` parity ExecPlan

This living plan follows `PLANS.md`. Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02) is the current
authority; the package remains unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Inventory and decision

All six Go artifacts (914 textual lines) were read in full:
`BUILD.bazel`, `README.md`, `filter.go`, `schema.go`, `filter_test.go`, and
`schema_test.go`. There is no package doc, generated/platform source, fixture,
benchmark, fuzz target, example, or additional harness. The source tests cover
schema/table precedence, regex/glob combinations, case handling, invalid
regexes, schema-only statements, nil rules, and all system-schema rows.

The `tidb-util::filter` owner remains dependency-closed for the selector,
regex, cache, and system-schema behavior, and its wired session consumers are
unchanged. The audit found four Rust-only compile-time diagnostics that Go
does not enforce (`is_system_schema`, `apply_on`, `apply`, and `matches`). A
focused `#[deny(unused_must_use)]` regression failed with four errors before
the change and passes after removing only those annotations. Earlier work
already deleted the duplicate `tidb-exec::filter` owner and source-absent
error/test surfaces.

## Progress

- [x] Compared the latest Go master tree with the Rust branch; no package drift.
- [x] Re-read every production, README, test, and Bazel artifact.
- [x] Added the focused return-value regression; it failed before the fix.
- [x] Removed the four Rust-only `#[must_use]` diagnostics and passed focused
      Rust/Go tests, formatting, and the pinned Go lint gate.
- [ ] Run broader workspace and Bazel validation when those environments are
      available; this package-level fix is otherwise Ready.

## Validation

Profile: **Ready** for this focused parity fix; no Go or Bazel source changed,
so `make bazel_prepare` is not required.

```text
git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/filter
git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/filter
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/filter -count=1
# all passed in current and /tmp/tidb-go-latest-c605

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test table_filter_contract --offline --locked -- --test-threads=1
# four passed, including filter_return_values_may_be_ignored_like_go

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
# all passed
```

The pre-fix focused regression failed with four `unused_must_use` errors; it
passes after the annotations are removed.

## Risks and unverified scope

- Correctness: selector, regex, cache, and system-schema behavior are
  unchanged; only compile-time diagnostics were relaxed to match Go.
- Compatibility: callers may discard the four results as in Go; all current
  consumers remain type-checked.
- Performance: unchanged; matching and cache synchronization are untouched.
- Not verified locally: Bazel execution, full workspace tests, and live BR/
  replication consumers beyond the focused owner and Go suites.
