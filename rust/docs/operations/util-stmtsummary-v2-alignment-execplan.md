# Align the Rust `pkg/util/stmtsummary/v2` owner with its Go contracts

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained
according to that file and the package-atomic transcreation rule in
`AGENTS.md`.

## Purpose / Big Picture

The native Rust statement-summary v2 owner should behave like the Go package
it represents. After this batch, callers may ignore ordinary Go-shaped return
values without Rust-only compiler diagnostics, a full result channel cannot
grow the worker stack until the process aborts, normal log rotation is silent,
and an external caller can construct and cancel the token accepted by the
public history-reader constructor. The focused tests make each behavior
observable, while the owner-wide test and compile gates show that existing
statement aggregation and persistence behavior remains intact.

This is a corrective package batch, not a completed transcreation claim.
`pkg/util/stmtsummary/v2/logger.go` is still only partly represented, and the
nested `pkg/util/stmtsummary/v2/tests` directory is a separate Go package.

## Progress

- [x] (2026-09-07 06:00Z) Reused and verified the complete prior Go inventory
  for the direct `pkg/util/stmtsummary/v2` package without reopening Go source,
  as explicitly requested by the user.
- [x] (2026-09-07 06:05Z) Read every Rust v2 module, test, manifest, workspace
  and lock entry, downstream dependency edge, and caller before editing.
- [x] (2026-09-07 06:12Z) Added focused regressions and captured four distinct
  pre-fix failures: 36 discard diagnostics, stack overflow, stderr leakage,
  and an inaccessible public-constructor parameter type.
- [x] (2026-09-07 06:20Z) Removed the 36 Rust-only return annotations, made the
  cancellation token public, changed retry recursion to iteration, removed
  debug stderr writes, and cleaned warnings in the edited owner.
- [x] (2026-09-07 06:24Z) Passed all seven focused regressions, all 74 owner
  tests, owner all-target compilation, and caller all-target compilation.
- [x] (2026-09-07 06:29Z) Passed scoped format checking, `git diff --check`,
  and the Ready-profile repository `make lint` command.
- [x] (2026-09-07 06:38Z) Updated the shared receipt and root test-port
  ExecPlan, self-reviewed one package-scoped diff, created one commit, rebased
  it over nine newer remote commits, and reran the complete Ready evidence.
  The rebased commit containing this plan is the normal-push publication unit.

## Surprises & Discoveries

- Observation: the Rust v2 owner had 38 explicit `#[must_use]` annotations,
  but only two describe native optional-result seams.
  Evidence: temporarily compiling four `#[deny(unused_must_use)]` regressions
  before the edit emitted exactly 36 diagnostics. The retained annotations are
  `column_factory -> Option<ColumnFactory>` and
  `global_stmt_summary -> Option<Arc<StmtSummary>>`.
- Observation: `send_with_cancel` looked like a loop but recursively called
  itself after every full-channel poll, retaining the 12 KiB value in each
  debug-build frame.
  Evidence: the focused child-process test aborted with `thread
  'stmt-send-bounded-stack' has overflowed its stack` before the fix. It now
  holds the channel full for 1.5 seconds on a bounded stack and completes.
- Observation: normal size rotation and backup pruning wrote unconditional
  `DEBUG rename` and `DEBUG prune` lines to process stderr.
  Evidence: the pre-fix child-process regression captured both lines; after
  the edit its stderr is empty.
- Observation: public `HistoryReader::new` accepted a crate-private
  `CancelToken`, so a downstream crate could not supply parent cancellation.
  Evidence: the new integration test failed before the fix with Rust error
  `E0603: struct CancelToken is private`.
- Observation: the crate-level v2 status text still said `reader.go` was
  absent even though `v2/reader.rs` is a complete module.
  Evidence: `rust/crates/tidb-stmtsummary/src/lib.rs` disagreed with the v2
  module header and implementation inventory.

## Decision Log

- Decision: honor the user's Rust-only instruction by reusing the complete Go
  package inventory already recorded in
  `rust/testport/receipts/util_stmtsummary_audit.md`; do not edit or execute Go.
  Rationale: the package boundary and all Go artifacts were already enumerated,
  and the requested work is removal of independently verifiable Rust-only
  behavior.
  Date/Author: 2026-09-07, Codex.
- Decision: remove `#[must_use]` only from ordinary values corresponding to Go
  returns, while retaining the two native `Option` boundaries.
  Rationale: this removes the language-only diagnostic without weakening
  Rust APIs where ignoring absence can hide a control-flow decision.
  Date/Author: 2026-09-07, Codex.
- Decision: keep `send_with_cancel` synchronous and poll-based, but carry the
  returned value through one iterative frame.
  Rationale: this preserves the existing channel capacity and cancellation
  cadence while matching Go's iterative `select` behavior.
  Date/Author: 2026-09-07, Codex.
- Decision: expose `CancelToken` and its `new`, `is_done`, and `cancel`
  methods instead of reducing `HistoryReader::new` visibility.
  Rationale: Go's history-reader constructor is exported and accepts caller
  cancellation; a public Rust constructor must expose a usable equivalent.
  Date/Author: 2026-09-07, Codex.
- Decision: remove unconditional rotation diagnostics without changing the
  existing best-effort rename and prune error policy.
  Rationale: the debug writes are Rust-only production output; changing error
  propagation would be a separate behavioral decision outside this fix.
  Date/Author: 2026-09-07, Codex.
- Decision: do not run `make bazel_prepare`.
  Rationale: this batch changes only Rust source, a Rust integration test, and
  Markdown; it changes no Go file, Go import, Bazel target, or module metadata.
  Date/Author: 2026-09-07, Codex.

## Outcomes & Retrospective

The behavioral fixes and all local Ready gates are complete. Six inline
regressions and one external API regression now pass, as do 74/74 owner tests,
owner all-target compilation, and the only production caller crate's
all-target compilation, scoped formatting, diff hygiene, and repository lint.
The single package commit was rebased cleanly over the latest fetched remote
branch and the same complete gate passed again. The commit containing this
plan is published by normal push immediately after this document snapshot.
The important lesson is that a loop-shaped function can still hide unbounded
recursion when ownership recovery is expressed through a recursive return.

The batch deliberately leaves the unported portions of `v2/logger.go` and the
separate `v2/tests` package explicit. No package-complete parity claim follows
from these corrections.

## Context and Orientation

The direct Go package has twelve tracked artifacts. Production consists of
`pkg/util/stmtsummary/v2/column.go`, `logger.go`, `reader.go`, `record.go`, and
`stmtsummary.go` (3,366 lines). Tests and benchmarks consist of
`column_test.go`, `main_test.go`, `reader_test.go`, `record_test.go`,
`stmtsummary_benchmark_test.go`, and `stmtsummary_test.go` (1,398 lines), plus
`BUILD.bazel`. It has no `doc.go`, fixtures, generated source, or
platform-specific variant. The two Go files and BUILD target below
`pkg/util/stmtsummary/v2/tests` belong to a separate package and are not part
of this implementation unit.

The corresponding Rust owner is the `tidb-stmtsummary` crate. Before editing,
its complete v2 source inventory was:

- `rust/crates/tidb-stmtsummary/src/v2/column.rs`, 821 lines;
- `rust/crates/tidb-stmtsummary/src/v2/mod.rs`, 49 lines;
- `rust/crates/tidb-stmtsummary/src/v2/reader.rs`, 2,174 lines;
- `rust/crates/tidb-stmtsummary/src/v2/record.rs`, 1,709 lines;
- `rust/crates/tidb-stmtsummary/src/v2/stmtsummary.rs`, 2,122 lines.

Those 6,875 lines contained 255 functions or methods, 27 inline tests, and 38
explicit `#[must_use]` annotations. The audit also read the crate manifest and
root, workspace registration in `rust/Cargo.toml`, relevant entries in
`rust/Cargo.lock`, the `tidb-session` dependency and proxy callers, its tests,
the `tidb-workloadrepo` dependency edge, and every other Rust caller found by
repository search. There is no owner `build.rs`, fixture tree, generated
source, platform variant, or custom build output. Cargo's files under
`rust/target` are derived artifacts and are not edited or committed.

A cancellation token is the Rust replacement for Go's `context.Context`
cancellation signal. A synchronous channel is bounded; `try_send` returns the
owned value when the channel is full. The fixed implementation keeps that
value in one mutable local and retries after the same 20 ms poll interval.

## Plan of Work

First add four deny-on-discard tests, one per v2 module that owns ordinary
returns. Compile them while the annotations remain and count the diagnostics.
Then remove exactly those 36 annotations and retain the two `Option` seams.

Next add a child-process regression in `v2/reader.rs`. It fills a bounded
channel, invokes `send_with_cancel` on a bounded-stack thread, waits long
enough for the old recursive implementation to overflow, then drains both
values. Replace the recursive full-channel arm with iterative ownership
recovery. Add a second child-process regression in `v2/stmtsummary.rs` that
forces rotation and requires empty stderr, then delete the two unconditional
debug writes.

Add `rust/crates/tidb-stmtsummary/tests/v2_public_api.rs` so the API is checked
from outside the crate. Make `CancelToken` and its three operations public and
document them. Remove warning-only leftovers in touched code, and correct the
stale v2 crate documentation without claiming complete logger parity.

Finally run focused, complete-owner, caller, formatting, and Ready gates.
Update this plan and the shared receipt with exact evidence, stage only the
package batch, commit once, fetch and rebase on
`origin/hparser-integration`, rerun the focused tests, and push normally.

## Concrete Steps

Run all commands from the repository root. Rust commands that link OpenSSL use
the bundled native runtime:

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
      -p tidb-stmtsummary --lib go_v2_alignment_ -- --nocapture --test-threads=1

Expect six inline tests to pass. Then run the external API test:

    OPENSSL_DIR=... OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=... \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
      -p tidb-stmtsummary --test v2_public_api -- --nocapture --test-threads=1

Expect one test to pass. Run the complete owner and compile gates:

    OPENSSL_DIR=... OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=... \
    cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
      --offline --locked -p tidb-stmtsummary --no-fail-fast --test-threads=1

    OPENSSL_DIR=... OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=... \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      --offline --locked -p tidb-stmtsummary --all-targets

    OPENSSL_DIR=... OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=... \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      --offline --locked -p tidb-session --all-targets

Expect 74 owner tests and both checks to pass. Dependency-crate warnings may
appear, but no diagnostic may point into `tidb-stmtsummary`.

Run formatting, diff hygiene, and the repository Ready lint:

    rustfmt +nightly-2026-08-22 --edition 2021 --check \
      rust/crates/tidb-stmtsummary/src/lib.rs \
      rust/crates/tidb-stmtsummary/src/v2/column.rs \
      rust/crates/tidb-stmtsummary/src/v2/reader.rs \
      rust/crates/tidb-stmtsummary/src/v2/record.rs \
      rust/crates/tidb-stmtsummary/src/v2/stmtsummary.rs \
      rust/crates/tidb-stmtsummary/tests/v2_public_api.rs
    git diff --check
    mkdir -p /tmp/tidb-codex
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint

## Validation and Acceptance

Acceptance requires all of the following observable results:

- temporarily restoring the 36 annotations makes the four focused return
  tests fail with exactly 36 diagnostics, and the corrected code compiles;
- the old retry implementation aborts the bounded-stack child, while the
  iterative implementation sends after the receiver frees capacity;
- the old writer emits the two debug lines, while the corrected writer's child
  process has empty stderr;
- the external API test changes from `E0603` to a passing cancel-state check;
- all six inline regressions, the external regression, 74 owner tests, both
  all-target checks, formatting, diff hygiene, and `make lint` pass;
- only one commit is created for the direct Go package boundary, and it is
  pushed without force after rebasing on the latest remote branch.

## Idempotence and Recovery

Every test, check, and formatting command is safe to rerun. The child-process
tests use process-local environment variables and temporary directories, so
they leave no repository state. If Cargo is interrupted, rerun the same
command; `rust/target` is only a local cache. If the remote moves before push,
fetch the explicit remote ref and rebase again. Resolve only overlap in this
package batch, rerun the focused and formatting checks, and never force-push.
Do not stage the unrelated untracked `.zcode/` directory.

## Artifacts and Notes

Pre-fix return evidence is in
`/tmp/tidb-stmtsummary-v2-return-prefx.log`: the focused command exited 101
with exactly 36 `unused return value` errors. The retry regression exited 101
with a stack-overflow abort. The writer regression exited 101 and displayed
both `DEBUG rename` and `DEBUG prune`. The external test exited 101 with
`E0603`.

Post-fix focused evidence is in
`/tmp/tidb-stmtsummary-v2-focused-postfix.log` and
`/tmp/tidb-stmtsummary-v2-api-postfix.log`: six inline tests and one external
test pass. `/tmp/tidb-stmtsummary-v2-nextest.log` records 74/74 tests passing,
and `/tmp/tidb-stmtsummary-v2-check.log` contains no owner-path diagnostic.

## Interfaces and Dependencies

At completion, `rust/crates/tidb-stmtsummary/src/v2/reader.rs` exposes:

    pub struct CancelToken(Arc<AtomicBool>);

    impl CancelToken {
        pub fn new() -> Self;
        pub fn is_done(&self) -> bool;
        pub fn cancel(&self);
    }

`HistoryReader::new` continues to accept `Option<CancelToken>`. Internal
`send_with_cancel<T>` keeps the `SyncSender<T>`, owned `T`, and token inputs but
uses one iterative frame. No public data representation, Cargo dependency,
feature, Go source, Bazel target, or generated artifact changes.

Revision note (2026-09-07): created this living plan after implementation and
the Rust owner/caller gates, recording the complete reused inventory, all
four fail-before signals, decisions, remaining validation, and publication
steps. Updated it after scoped formatting, diff hygiene, and the Ready lint
all passed, then finalized it after the package commit rebased over nine
incoming remote commits and the complete Ready evidence passed again.
