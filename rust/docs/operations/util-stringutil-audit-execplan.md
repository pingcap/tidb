# `pkg/util/stringutil` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while
the rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

TiDB's string utility package defines SQL quoting, wildcard matching,
identifier escaping, memoized display values, UTF-8 helpers, and ASCII
normalization. This audit keeps the dependency-closed Rust owner aligned with
Go `master`, including the explicit LIKE escape byte, while ensuring callers
may discard helper results exactly as Go callers do.

## Progress

- [x] (2026-09-02) Read all four Go-master artifacts in full:
      `BUILD.bazel`, `main_test.go`, `string_util.go`, and
      `string_util_test.go` (927 lines total). Confirmed there are no package
      docs, fixtures, generated/platform variants, nested packages, or extra
      build artifacts.
- [x] (2026-09-02) Compared Go `origin/master` at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` with the hparser checkout.
      The master-only source delta is the explicit `CompileLike2Regexp` escape
      byte; the Rust owner already implements and tests that contract.
- [x] (2026-09-02) Read the 694-line Rust owner and 74-line benchmark before
      editing. Preserved its source-shaped seven Go tests, three benchmark
      families, and existing custom-escape regression.
- [x] (2026-09-02) Added
      `return_values_may_be_ignored_like_go`, which failed before the edit with
      15 `unused_must_use` errors, then removed all 15 explicit Rust-only
      `#[must_use]` annotations.
- [x] (2026-09-02) Ran current and detached latest-master Go tests, the
      focused Rust regression, all nine Rust stringutil tests, Rust formatting,
      pinned repository lint, and diff hygiene.
- [ ] Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next boundary.

## Surprises & Discoveries

- The hparser integration checkout still has the one-argument Go helper,
  whereas current Go `master` supplies an escape byte and updates its caller.
  The Rust helper already follows current `master`; no second conversion path
  is needed in this batch.
- Go strings are arbitrary byte sequences. The Rust owner consequently uses
  byte slices for quoting, binary matching, copying, and trailing-space logic,
  and decodes invalid UTF-8 one byte at a time to Go's replacement rune.
- The Rust owner carried explicit `#[must_use]` attributes on 15 helpers even
  though Go permits discarded results. A local deny-lint test reproduces every
  diagnostic before the attributes are removed.

## Decision Log

- Decision: keep `tidb-util::stringutil` as the complete owner and remove only
  explicit return-use diagnostics. Rationale: the owner already preserves the
  complete source behavior and the Go-master escape-byte delta; adding
  compatibility wrappers or a second regex path would be Rust-only behavior.
  Date/Author: 2026-09-02, Codex.
- Decision: retain the custom escape-byte API in Rust because it is the direct
  translation of Go `CompileLike2Regexp(str, escape)` at current `master`.
  Rationale: the hparser checkout's older one-argument source is behind the
  requested authority, while the detached master test and Rust regression both
  exercise the explicit byte. Date/Author: 2026-09-02, Codex.

## Validation

Run from the repository root unless a command says otherwise:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/stringutil -count=1
    (cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/stringutil -count=1)
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib stringutil::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib stringutil::tests --offline --locked -- --test-threads=1
    (cd rust && cargo +nightly-2026-08-22 fmt --all -- --check)
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Expected results are two passing Go suites, one focused Rust regression, nine
Rust stringutil tests, clean formatting, successful pinned lint, and no
whitespace errors. No Go or Bazel artifact changed in this batch, so
`make bazel_prepare` and failpoint toggling are not required.

## Outcomes & Retrospective

The Rust stringutil owner now accepts Go-style discarded results while keeping
all source-shaped matching, quoting, UTF-8, memoization, and benchmark
behavior. Current-master and hparser source differences are recorded rather
than hidden. The receipt and top-level testport plan contain the exact
inventory and validation evidence; broader planner/executor consumer parity
remains outside this leaf plan.
