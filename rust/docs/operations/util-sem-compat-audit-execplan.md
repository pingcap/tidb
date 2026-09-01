# `pkg/util/sem/compat` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while
the rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Security Enhanced Mode (SEM) compatibility helpers select the active SEM v1
or v2 policy for invisible schemas/tables/variables/status values and
restricted privileges. This audit keeps the dependency-closed Rust dispatcher
behavioral with Go and avoids adding Rust-only compile-time obligations to its
predicate return values.

## Progress

- [x] (2026-09-02) Read all five Go-master artifacts in full: `BUILD.bazel`,
      `sem.go`, `testhelper.go`, `compat_test.go`, and
      `sem_integration_test.go` (522 lines total). Confirmed no package doc,
      fixture, benchmark, generated/platform variant, nested package, or extra
      build artifact.
- [x] (2026-09-02) Confirmed the package is byte-identical at Go
      `origin/master` authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the 331-line Rust owner and all existing tests before
      editing. Preserved the six wrappers, SEM v1/v2 exclusivity assertion,
      configured v2 test registry, and five source-derived predicate tests.
- [x] (2026-09-02) Added
      `return_values_may_be_ignored_like_go`, which failed before the edit with
      six `unused_must_use` errors, then removed all six explicit Rust-only
      `#[must_use]` annotations.
- [x] (2026-09-02) Ran the failpoint-enabled current Go integration suite,
      the focused Rust regression, all six Rust compatibility tests, Rust
      formatting, pinned repository lint, and diff hygiene. A detached
      latest-master full Go run was stopped after two minutes because it did
      not terminate on this host.
- [ ] Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next boundary.

## Surprises & Discoveries

- `sem_integration_test.go` calls `testfailpoint.EnableCall`, so the canonical
  failpoint wrapper is required even though the six direct predicate tests are
  otherwise ordinary unit tests. The wrapper restored failpoint state on exit.
- The detached latest-master package suite is integration-heavy and did not
  terminate twice within two minutes without the wrapper; the current checkout
  passed when run through the wrapper, while the source inventory is
  byte-identical at the requested authority.
- Rust's existing test support uses a process-global SEM lock and a configured
  v2 registry, preserving Go's mutually exclusive global policy without a
  second compatibility mechanism.

## Decision Log

- Decision: keep `tidb-util::sem_compat` as the complete owner and remove only
  its six explicit return-use diagnostics. Rationale: the dispatcher already
  preserves Go's active-version routing and integration seams; adding another
  policy table would be speculative Rust-only behavior. Date/Author:
  2026-09-02, Codex.
- Decision: test discarded predicates under the existing SEM global lock with
  both policies disabled. Rationale: this isolates the return contract and
  avoids races with the source-derived v2 tests. Date/Author: 2026-09-02,
  Codex.

## Validation

Run from the repository root unless a command says otherwise:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/sem/compat -run '^(TestInvisibleSchema|TestIsInvisibleTable|TestIsRestrictedPrivilege|TestIsInvisibleStatusVar|TestIsInvisibleSysVar|TestRestrictedSQL)$' -count=1
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib sem_compat::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib sem_compat::tests --offline --locked -- --test-threads=1
    (cd rust && cargo +nightly-2026-08-22 fmt --all -- --check)
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
    git diff --check

Expected results are a passing failpoint-wrapped current Go suite, one focused
Rust regression, six Rust compatibility tests, clean formatting, successful
pinned lint, and no whitespace errors. No Go or Bazel artifact changed, so
`make bazel_prepare` is not required. The detached full Go run remains an
explicit unverified boundary due its non-termination.

## Outcomes & Retrospective

The SEM compatibility owner now accepts Go-style discarded predicate results
while retaining active v1/v2 routing and source-derived policy coverage. The
receipt records the complete inventory, failpoint decision, current authority,
and detached-run limitation. Broader session integration and remaining
repository package audits are outside this leaf plan.
