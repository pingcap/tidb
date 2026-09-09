# `pkg/util/queue` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the prior extraction and byte-identical to the exact detached
latest-master worktree.

## Complete inventory

All three Go artifacts (198 textual lines) were read in full before editing:

- `BUILD.bazel` (17 lines) — public library and flaky short test target;
- `queue.go` (94 lines) — generic circular buffer, zero value, growth, pop,
  clear, expansion, length, emptiness, and capacity;
- `queue_test.go` (87 lines) — `TestQueue` with four ordered source subtests.

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, `testdata`, benchmark, fuzz target, example test, or nested package.
The current checkout matches `origin/master` for every artifact.

## Rust ownership and parity result

`rust/crates/tidb-util/src/queue.rs` is the sole production owner. Its four
Go-named tests reproduce every source assertion, while retained source-derived
cases cover wrapped growth, zero-value versus `NewQueue(0)`, and Go's
retained-slot behavior after `Clear`.

The authority refresh found four Rust-only `#[must_use]` diagnostics on
`Queue::new`, `len`, `is_empty`, and `cap`. Go permits callers to discard all
four return values, so the annotations were removed. The focused
`return_values_may_be_ignored_like_go` regression is denied against
`unused_must_use`: before the fix it failed with four compiler errors, and
after the fix it passes with all nine inline Rust tests (four Go-named tests,
four source-derived boundary regressions, and the new parity regression).

The earlier audit's removal of the unused `tidb-exec::queue` duplicate remains
in force; no second consumer-specific implementation is added.

## Validation

Profile: **Ready** for this focused parity fix within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git -c maintenance.auto=false -c gc.auto=0 fetch origin master --prune` —
  passed; `origin/master` is `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- `git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/queue` — passed; no Go source drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/queue` and SHA-256 inventory — passed; exactly the three listed artifacts.
- `cmp -s /tmp/tidb-go-latest-c605/pkg/util/queue/<file> pkg/util/queue/<file>` for all three artifacts — passed.
- Pre-fix focused Rust compile with the new deny-lint regression — failed with
  the expected four `unused_must_use` errors.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/queue -count=1` — passed in the current and exact detached latest-master worktrees.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib queue::tests --offline --locked -- --test-threads=1` — passed; nine tests.
- `cd rust && OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go, Bazel, module, or Cargo manifest file changed, so `make bazel_prepare`
was not required.

## Risk

- Correctness: current and detached Go tests plus all nine Rust queue tests
  pass; FIFO, growth, panic, zero-value, and retained-slot behavior remain
  covered.
- Compatibility: removing compile-time diagnostics restores Go's discardable
  return contract without changing queue state transitions.
- Performance: unchanged; this removes diagnostics only.
