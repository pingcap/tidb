# `pkg/sessionctx/variable/tests` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Read and inventory every Go-master artifact in the nested
`pkg/sessionctx/variable/tests` package, compare each test and helper with its
Rust owner, remove redundant ignored carriers when an executable owner exists,
and record the remaining cross-crate boundary without inventing behavior.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all four artifacts (1,904 lines), including the
  47-shard BUILD target, TestMain/goleak harness, all 18 session tests and
  helper, and all 29 variable tests. Confirm no fixtures, generated,
  platform-specific, fuzz, benchmark, or generator-input artifacts exist.
- [x] (2026-09-01) Compare every test with the Rust carriers and executable
  owners in `tidb-session` and `tidb-exec`.
- [x] (2026-09-01) Remove six empty ignored vardef stubs whose behaviors now
  have executable owner tests; update receipts `b011.md` and `b012.md`.
- [x] (2026-09-01) Run the focused vardef suite, Rust formatting, `make lint`,
  and diff hygiene checks. Record the exact Go-master suite failures.
- [ ] Fetch immediately before staging, create one meaningful cleanup batch
  commit, push it to `origin/hparser-integration`, and verify all remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded package.

## Scope and decisions

The package is test-only but not leaf-scoped: it exercises SessionVars,
StmtContext, TestKit/Domain, executor bootstrap, transaction context,
slow-log accessors, user-variable synchronization, and cloud-storage state.
Its Rust concerns are therefore split across crates. Existing executable
owners are the source of truth for slow-log formatting, sysvar metadata and
dependency ordering; the remaining empty carriers are retained as explicit
parity evidence until their dependency-closed owners exist.

The Go suite's asynchronous `TestHookContext` callback and the default-value
dependency assertion are recorded as source-suite failures. They are not
silenced or “fixed” in Rust because doing so would alter test semantics rather
than remove Rust-only behavior.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable/tests -count=1

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-vardef --lib
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The Go command is expected to reproduce the two source-level failures listed
above. The Rust vardef command passes with 43 tests and 107 ignored. The
OpenSSL/pkg-config environment limitation prevents running the broader focused
Rust owner command locally.

## Outcome

The complete inventory and ownership map are recorded in
`rust/testport/receipts/sessionctx_variable_tests.md`. This batch is a
meaningful cleanup of stale ignored carriers, not a package-complete Rust
transcreation. The rolling audit continues after the batch is pushed.
