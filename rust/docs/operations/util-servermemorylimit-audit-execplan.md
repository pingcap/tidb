# `pkg/util/servermemorylimit` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while
the rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

TiDB's server-memory-limit package monitors process heap use, coordinates
top-session termination, and exposes the last 50 kill operations to SQL. This
bounded audit keeps the dependency-closed Rust owner aligned with Go and
ensures constructing a handler may be discarded like the corresponding Go
constructor call.

## Progress

- [x] (2026-09-02) Read all three Go-master artifacts in full:
      `BUILD.bazel`, `servermemorylimit.go`, and
      `servermemorylimit_test.go` (375 lines total). Confirmed no package doc,
      fixture, benchmark, generated/platform variant, nested package, or extra
      build artifact.
- [x] (2026-09-02) Confirmed the package is byte-identical at Go
      `origin/master` authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the 499-line Rust owner and its history-ring test
      before editing. Preserved live 100ms checking, global memory authorities,
      session-manager integration, kill state, failpoint handling, and exact
      12-column history rows.
- [x] (2026-09-02) Added
      `return_values_may_be_ignored_like_go`, reproduced one pre-fix
      `unused_must_use` error with the attribute temporarily restored, then
      removed the Rust-only constructor annotation.
- [x] (2026-09-02) Ran current failpoint-wrapped Go tests, detached latest
      Go unit tests, both focused Rust tests, Rust formatting, pinned
      repository lint, and diff hygiene.
- [ ] Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next boundary.

## Surprises & Discoveries

- The Rust owner already removed the earlier cache-like sampler and duplicate
  spill policy, leaving only one explicit return-use mismatch in the public
  handler constructor.
- The package uses `failpoint.Inject("issue42662_2", ...)` in production, so
  the canonical Go failpoint wrapper is required even for the focused history
  test. It restored the source tree's failpoint state on exit.
- The process-global history manager and memory authorities are shared with
  the ordinary memory package; no new test-only snapshot or alternate kill
  loop is needed.

## Decision Log

- Decision: keep `tidb-util::servermemorylimit` as the complete owner and
  remove only `new_server_memory_limit_handle`'s explicit `#[must_use]`.
  Rationale: the live controller, history ring, and consumers already match
  Go; adding a second handler API would be Rust-only behavior. Date/Author:
  2026-09-02, Codex.
- Decision: use a compile-time deny-lint regression with a disposable channel
  receiver. Rationale: it isolates the constructor contract without starting a
  background checker goroutine or mutating process-memory state. Date/Author:
  2026-09-02, Codex.

## Validation

Run from the repository root unless a command says otherwise:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/servermemorylimit -run '^TestMemoryUsageOpsHistory$' -count=1
    (cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/servermemorylimit -count=1)
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib servermemorylimit::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib servermemorylimit::tests::test_memory_usage_ops_history --offline --locked -- --exact
    (cd rust && cargo +nightly-2026-08-22 fmt --all -- --check)
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Expected results are passing failpoint-wrapped current Go tests, detached Go
unit tests, both Rust regressions, clean formatting, successful pinned lint,
and no whitespace errors. No Go or Bazel artifact changed, so
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The server-memory-limit owner now accepts Go-style discarded construction while
retaining the live controller and history behavior. The receipt records the
complete inventory, failpoint decision, source authority, and Ready evidence.
Cross-platform runtime behavior and the remaining package audits are outside
this plan.
