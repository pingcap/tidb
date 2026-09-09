# `pkg/keyspace` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while
the package remains part of the rolling Go-to-Rust audit.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

TiDB users and operators should see one keyspace contract regardless of
whether the implementation path is Go or Rust. This bounded audit verifies
that keyspace-name resolution, etcd namespace construction, API context
selection, logging fields, and Starter username qualification retain Go's
observable behavior. It also prevents Rust's `#[must_use]` lint from making
otherwise valid Go-style discarded calls fail to compile.

## Progress

- [x] (2026-09-02) Read all five Go-master `pkg/keyspace` artifacts in full:
      `BUILD.bazel`, `doc.go`, `keyspace.go`, `keyspace_test.go`, and
      `username_policy.go` (404 lines total). Confirmed there are no generated,
      platform-specific, fixture, nested-package, or extra build artifacts.
- [x] (2026-09-02) Compared the package with Go `origin/master` at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec`; no Go source delta exists.
- [x] (2026-09-02) Read the dependency-closed Rust owner
      `rust/crates/tidb-util/src/keyspace.rs` and its tests before editing.
- [x] (2026-09-02) Added
      `return_values_may_be_ignored_like_go`, which failed before the edit
      with 11 `unused_must_use` errors, then removed all 11 Rust-only
      `#[must_use]` annotations from the owner.
- [x] (2026-09-02) Ran current and detached latest-master Go tests, the
      focused Rust regression, all seven Rust keyspace tests, Rust formatting,
      pinned repository lint, and diff hygiene.
- [ ] Push the batch to `origin/hparser-integration`, verify local/remote SHAs,
      and fetch the newest target branch before the next package boundary.

## Surprises & Discoveries

- Go's complete package is small and source-identical at the current authority,
  but the Rust owner carried 11 explicit return-use requirements that Go does
  not have. The deny-lint regression reproduced each diagnostic before the
  change.
- `get_keyspace_name_bytes_by_settings` intentionally returns Go's nil-slice
  equivalent (`None`) on the classic kernel and computes the configured bytes
  once on NextGen. The audit leaves that process-wide initialization contract
  unchanged.
- Client-go's codec, PD's API context, and zap's logger core are unavailable in
  the Rust crate. The existing `KeyspaceCodec`, `ApiContext`, and canonical
  `keyspaceName` field are carrier adaptations of the source values, not new
  runtime policy.

## Decision Log

- Decision: keep `tidb-util::keyspace` as the complete owner for Go's
  `pkg/keyspace` and remove only the Rust-only `#[must_use]` annotations.
  Rationale: the owner already preserves every package behavior and has
  source-shaped tests; adding fake client or logger implementations would be
  speculative Rust-only behavior. Date/Author: 2026-09-02, Codex.
- Decision: use a compile-time `#[deny(unused_must_use)]` regression rather
  than weakening lint settings globally. Rationale: it proves the public
  boundary accepts Go-style discarded results while keeping unrelated Rust
  diagnostics intact. Date/Author: 2026-09-02, Codex.

## Validation

Run from the repository root unless a command says otherwise:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/keyspace -count=1
    (cd /tmp/tidb-go-latest-c605 && PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/keyspace -count=1)
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib keyspace::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib keyspace::tests --offline --locked -- --test-threads=1
    (cd rust && cargo +nightly-2026-08-22 fmt --all -- --check)
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Expected results are two passing Go suites, one focused Rust regression, seven
Rust keyspace tests, clean Rust formatting, successful pinned lint, and no
whitespace errors. No Go source, import section, or Bazel metadata changed,
so `make bazel_prepare` and failpoint toggling are not required for this
Rust/docs-only batch.

## Outcomes & Retrospective

The keyspace owner now matches Go's return-value contract while retaining all
existing namespace, configuration, API-context, logger-field, and username
policy behavior. The bounded receipt records the exact inventory and current
authority. Broader server, PD, logger integration, and the remaining
repository packages are outside this plan and must not be described as
complete until their own dependency-closed audits finish.
