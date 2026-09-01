# `pkg/util/disk` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while
the rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

TiDB's disk utility owns temporary-storage directory setup, process-global
locking, stale-entry cleanup, and disk-memory tracker constructors. This audit
keeps those observable contracts in the dependency-closed Rust owner and
ensures Rust does not make valid Go-style discarded tracker construction fail
under `unused_must_use`.

## Progress

- [x] (2026-09-02) Read all five Go-master artifacts in full:
      `BUILD.bazel`, `main_test.go`, `tempDir.go`, `tempDir_test.go`, and
      `tracker.go` (283 lines total). Confirmed no package doc, fixture,
      testdata, benchmark, fuzz target, generated source, or platform variant.
- [x] (2026-09-02) Confirmed the package is unchanged at Go
      `origin/master` authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read both Rust owner files (`disk/mod.rs` and
      `disk/temp_dir.rs`) and their tests before editing. Preserved directory
      locking, asynchronous cleanup, tracker aliases, and spill consumers.
- [x] (2026-09-02) Added
      `disk::tests::return_values_may_be_ignored_like_go`, which failed before
      the edit with two `unused_must_use` errors, then removed both explicit
      Rust-only constructor annotations.
- [x] (2026-09-02) Ran current and detached latest-master Go tests, both
      focused Rust regressions, Rust formatting, pinned repository lint, and
      diff hygiene.
- [ ] Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next boundary.

## Surprises & Discoveries

- The Rust disk owner has already consolidated the Go temporary-directory and
  spill-storage seams; the remaining explicit mismatch was only return-use
  diagnostics on `new_tracker` and `new_global_tracker`.
- Go's `CheckAndInitTempDir` serializes the complete check/init operation and
  retries after the configured directory is removed. The existing Rust test
  covers ten concurrent reinitializers and remains unchanged.
- Windows and unsupported-target lock implementations cannot be executed on
  this macOS host; their source branches remain in the owner and are recorded
  as a validation boundary.

## Decision Log

- Decision: keep `tidb-util::disk` as the complete owner and remove only the
  two explicit `#[must_use]` attributes. Rationale: all Go production and test
  behavior is already represented, while adding a second spill policy or
  tracker abstraction would be Rust-only. Date/Author: 2026-09-02, Codex.
- Decision: use a local deny-lint regression rather than changing workspace
  lint settings. Rationale: it proves the public constructor boundary without
  suppressing unrelated diagnostics. Date/Author: 2026-09-02, Codex.

## Validation

Run from the repository root unless a command says otherwise:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disk -count=1
    (cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/disk -count=1)
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib disk::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib disk::temp_dir::tests::test_remove_dir --offline --locked -- --exact --nocapture
    (cd rust && cargo +nightly-2026-08-22 fmt --all -- --check)
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Expected results are two passing Go suites, two passing Rust regressions,
clean Rust formatting, successful pinned lint, and no whitespace errors. No Go
or Bazel artifact changed, so `make bazel_prepare` and failpoint toggling are
not required for this Rust/docs-only batch.

## Outcomes & Retrospective

The disk owner now matches Go's constructor return contract while retaining
directory lock, cleanup, tracker, and spill behavior. The receipt records the
complete artifact inventory and current authority. Cross-platform lock runtime
coverage and the remaining repository package audits are outside this plan.
