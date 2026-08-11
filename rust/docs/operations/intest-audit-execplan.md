# Align and certify `pkg/util/intest` as one Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's `intest` utility makes internal assertions conditional on Go build tags and one startup failpoint. Rust code uses the same switches for assertion-only invariants and for shorter test timing. Completion means all seven direct Go package artifacts, the original Go test, all three build-tag shapes, the failpoint startup path, the Rust owner, and live consumers are accounted for. A caller can observe matching enablement, laziness, panic text, nil/error/function handling, and `InTest` behavior through public Rust tests built under the corresponding Cargo features.

## Progress

- [x] (2026-08-12) Fixed the seven-artifact Go inventory and accepted source pin `5ea5a54cd3239cf534cfd171422ee676eb7f2934`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, generated input, fixture, benchmark, fuzz target, example, `go:generate`, or `go:embed`; identified the `intest`, `enableassert`, and default build variants and the package-init failpoint.
- [x] (2026-08-12) Read all Go sources, the original Go test, Bazel metadata, the Rust owner, historical receipt, Cargo feature declarations, and live `tidb-util` consumers.
- [x] (2026-08-12) Ran the failpoint-managed Go authority normally and under race, and probed all build-tag/failpoint startup shapes through an overlay without changing accepted source files.
- [x] (2026-08-12) Added a public Rust contract, reproduced the old startup-switch mismatch, and replaced assertion-only initialization with shared lazy initialization visible through the public switches.
- [x] (2026-08-12) Added the compact semantic receipt and passed WIP owner, default/feature contract, and focused consumer validation.
- [x] (2026-08-12) Completed Ready Go/Rust validation, source/inventory/evidence gates, formatting, three Clippy feature shapes, repository lint, and staged-diff self-review.
- [ ] Rebase one package commit onto a fresh target tip if needed, push without force, and verify fresh local, remote-tracking, and remote-advertised SHAs.

## Surprises & Discoveries

- Observation: Rust's existing owner test is compiled with `cfg(test)`, which deliberately makes `IN_TEST`, `ENABLE_ASSERT`, and `ENABLE_INTERNAL_CHECK` true even when no Cargo feature is selected.
  Evidence: `rust/crates/tidb-util/src/intest/mod.rs` initializes all three from `cfg!(... test)`. This accurately supports the repository's canonical test mode but cannot prove the default library build.

- Observation: Go evaluates the failpoint during package initialization, while the old Rust code checked `GO_FAILPOINTS` only on the first public assertion call; direct reads of both public switches therefore returned false in a failpoint-enabled process.
  Evidence: the Go overlay probe reported `initial=false,true,true` with `GO_FAILPOINTS=/enableInternalCheck=return(true)`. The pre-fix Rust child failed at `assertion failed: intest::ENABLE_ASSERT.load(Ordering::Relaxed)` before calling any assertion helper.

- Observation: The same overlay probe established all source build matrices and callable laziness without modifying a tracked Go file.
  Evidence: default reported `false,false,false` with `called=false`; `enableassert` reported `false,true,true`; `intest` reported `true,true,true`; the enabled shapes both reported `called=true` and panicked as expected.

## Decision Log

- Decision: Use `5ea5a54cd3239cf534cfd171422ee676eb7f2934` as the complete Go package pin.
  Rationale: it is the last source commit for the package, is an ancestor of the target branch, contains exactly the same seven direct artifacts, and every current package byte matches it.
  Date/Author: 2026-08-12 / Codex

- Decision: Put build-shape and failpoint coverage in `rust/crates/tidb-util/tests/intest_contract.rs`, not only in the owner's `#[cfg(test)]` module.
  Rationale: an integration test links `tidb-util` as an ordinary dependency, so the no-feature build exposes the real default constants. The same file can be rebuilt with `intest` and `enableassert` to prove each source variant without adding a test-only production interface.
  Date/Author: 2026-08-12 / Codex

- Decision: Validate startup failpoint behavior in an exact child-test invocation of the integration-test executable.
  Rationale: the Rust environment parser is intentionally one-time and lazy. A new process with `GO_FAILPOINTS` already set isolates that contract from mutable test globals and test execution order, while preserving the public API boundary.
  Date/Author: 2026-08-12 / Codex

- Decision: Replace the two public `AtomicBool` statics with `LazyLock<AtomicBool>` values backed by one shared environment snapshot.
  Rationale: retaining the old `Once` in the assertion path cannot fix direct public reads, while platform startup constructors would require unsafe and platform-specific machinery. `LazyLock` makes the first public observation reflect the startup environment, snapshots both switches consistently, preserves existing `.load` and `.store` use through dereference, and adds no dependency.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat Go variadic formatting, reflection-based typed nil, mutable non-atomic booleans, and nil callable values as native language boundaries.
  Rationale: Rust uses preformatted strings, `Option<T>`, atomics, and typed optional closures. The contract will cover every representable source result without introducing untyped or data-racy interfaces.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

Inventory, source pinning, Go baseline/probe evidence, the fail-before-fix regression, the minimal production correction, public contract, compact receipt, WIP checks, and Ready validation are complete. Publication and fresh-remote verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/intest/BUILD.bazel`, `assert.go`, `assert_common.go`, `assert_test.go`, `in_unittest.go`, `no_assert.go`, and `not_in_unittest.go`. With the `intest` build tag, both `InTest` and `EnableAssert` start true. With only `enableassert`, `InTest` is false and assertions start enabled. With neither tag, both are false until `EnableInternalCheck` is set or the package-init failpoint `/enableInternalCheck=return(true)` sets both assertion switches true. Disabled `AssertFunc` calls must not invoke their function.

The Rust owner is `rust/crates/tidb-util/src/intest/mod.rs`. Cargo features `intest` and `enableassert` correspond to the Go tags. Exported Go booleans map to lazily initialized `AtomicBool` values; Go `fmt.Sprintf` arguments map to caller-preformatted Rust strings; reflection-based nil values map to `Option<T>`. Live consumers are `mathutil/math.rs`, `redact.rs`, `sem.rs`, and `sqlkiller.rs`. The first three exercise assertion helpers; `sqlkiller` consumes `IN_TEST` to choose a one-millisecond rather than one-second liveness interval.

The original Go `TestAssert` covers true and false conditions, string and formatted messages, scalar and pointer nil checks, true/false/panicking/nil functions, and nil/non-nil errors. The package has no benchmarks or external package-specific integration harness.

## Milestones

The authority milestone runs `TestAssert` through the mandatory failpoint wrapper normally and under race. An overlay-only Go probe, or equivalently scoped commands that do not alter the accepted package, records initial flags and disabled-function laziness under default, `enableassert`, `intest`, and startup-failpoint builds.

The parity milestone adds one public integration-test target. The default build proves `IN_TEST == false`, both switches initially false, and skipped functions remain lazy until the internal switch is enabled. Feature builds prove the exact initial flag matrix and assertion panic behavior. A subprocess proves the startup environment enables both switches before the first assertion. Public panic checks preserve the Go messages for ordinary, caller-formatted, not-nil, callable, and error cases.

The consumer milestone reruns the focused owner tests plus `mathutil`, `redact`, `sem`, and `sqlkiller` tests that exercise or depend on the package contract. The completion milestone runs the complete owning crate, formatting, owner Clippy across all targets and relevant feature variants, the semantic package gate, repository lint, and atomic-diff checks.

The publication milestone fetches `hparser-integration` with an explicit refspec, rebases the one-package commit if the remote advanced, repeats Ready after any rebase, pushes normally, and verifies the local, remote-tracking, and `git ls-remote` SHAs agree. Force push is forbidden.

## Plan of Work

Keep Go and Bazel artifacts unchanged. Use `rust/crates/tidb-util/tests/intest_contract.rs` as the independent public contract. Serialize all mutations of the exported atomics within one test per selected feature build and restore their source defaults before returning. Launch the current test executable with one exact child test and a marker environment variable so `GO_FAILPOINTS` is present before any assertion. Use one shared `LazyLock<bool>` environment snapshot to initialize the two public `LazyLock<AtomicBool>` switches. Bind the owner, contract, feature manifest, module export, and focused consumers to the accepted source pin in `rust/crates/tidb-util/tests/intest.semantic.toml`.

## Concrete Steps

From repository root, run the Go source authority only through the failpoint-safe wrapper:

    ./tools/check/failpoint-go-test.sh pkg/util/intest -run '^TestAssert$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/util/intest -race -run '^TestAssert$' -count=1

Use a temporary Go overlay or probe to record the three build matrices and startup failpoint without editing any tracked package artifact. The expected initial matrices are `(false,false,false)` for default, `(false,true,true)` for `enableassert`, and `(true,true,true)` for `intest`; the default build with `GO_FAILPOINTS=/enableInternalCheck=return(true)` must start `(false,true,true)`.

From `rust`, set `CARGO_INCREMENTAL=0` and use `/tmp/tidb-package-audit.DnxFlT/rust/target` as `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-util --lib 'intest::'
    cargo test --offline --locked -j12 -p tidb-util --test intest_contract
    cargo test --offline --locked -j12 -p tidb-util --test intest_contract --features intest
    cargo test --offline --locked -j12 -p tidb-util --test intest_contract --features enableassert
    cargo test --offline --locked -j12 -p tidb-util --lib 'mathutil::math::tests::test_divide_2_batches' -- --exact
    cargo test --offline --locked -j12 -p tidb-util --lib 'redact::tests::redact_string' -- --exact
    cargo test --offline --locked -j12 -p tidb-util --lib 'sem::tests::restricted_privilege' -- --exact
    cargo test --offline --locked -j12 -p tidb-util --lib 'sqlkiller::tests::'
    cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets --features intest -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets --features enableassert -- -D warnings

From repository root, recover the removed semantic gate runner read-only from `3353b29fb^`, install the cached `revive` binary into ignored `tools/bin`, and run the receipt, lint, and atomic-boundary checks. Do not run `make bazel_prepare` unless the final diff triggers the repository gate.

## Validation and Acceptance

Go `TestAssert` must pass normally and under race with failpoints enabled and disabled by the wrapper. All observed build matrices must match their Go tag definitions, and disabled `AssertFunc` must not invoke its callable. Rust owner and public contracts must pass under all three feature shapes. The child process must prove the startup failpoint enables assertions and emits `assert failed` on a false condition. Focused assertion and timing consumers, the complete owning crate, formatting, all relevant Clippy variants, semantic source/inventory/evidence/command gates, and `make lint` must pass.

The accepted seven Go artifacts must remain byte-identical to the pin. The final staged diff may contain only the public contract, compact receipt, this plan, and a narrowly proven production fix if one is necessary. Publication is accepted only after a normal push and fresh explicit fetch show all local and remote SHAs equal.

## Idempotence and Recovery

The failpoint wrapper serializes rewriting and always disables failpoints during cleanup; rerunning it is safe. Tests and semantic checks are safe to repeat. Temporary Go probes and recovered gate scripts live under `/tmp` and must be moved to Trash or removed by exact path after evidence is captured. Cargo uses a shared target directory with incremental compilation disabled; do not clean it wholesale. If disk space becomes insufficient, remove only a verified regenerable cache path that is not used by the user's main workspace. If the target branch advances, abort publication, fetch explicitly, rebase the single package commit, and rerun Ready before pushing. Never force push.

## Artifacts and Notes

Failpoint decision:

    pkg/util/intest/assert_common.go invokes failpoint.Inject, and BUILD.bazel depends on @com_github_pingcap_failpoint//:failpoint. All package unit tests therefore use tools/check/failpoint-go-test.sh.

Build metadata decision before edits:

    make bazel_prepare is not required for the intended Rust-only contract, receipt, and plan diff. No Go file, Go import block, Go test function, Bazel file, or module dependency is planned to change. This decision will be repeated against the final diff.

Source evidence:

    git diff --exit-code 5ea5a54cd3239cf534cfd171422ee676eb7f2934..HEAD -- pkg/util/intest

The command exited successfully and `git ls-tree`/`git ls-files` both enumerate the same seven direct artifacts.

WIP evidence:

    Go TestAssert: passed normally and under race through the failpoint wrapper.
    Go build probes: all four source matrices and disabled-call laziness passed.
    Old Rust regression: 3 passed, 1 failed at the direct startup-switch read.
    Fixed Rust default contract: 4 passed; intest and enableassert: 3 passed each.
    Rust owner and four focused consumers: passed.

Ready evidence:

    Go TestAssert passed normally and under race through the failpoint wrapper; cleanup returned the failpoint refcount to zero.
    Rust public contract passed 4 tests by default and 3 tests under each of the intest and enableassert features.
    Complete tidb-util passed 346 owner tests with 1 existing ignored test, every integration test, and its compile-fail doc test.
    cargo fmt --all --check and default/intest/enableassert all-target Clippy with -D warnings passed.
    Semantic package gate passed 1 package and 8 unique owner/feature/consumer commands.
    make -o tools/bin/revive lint passed.
    The final Bazel prepare gate found no trigger because no Go, Bazel, module, manifest, or Go test artifact changed.

The temporary Go overlay probe was moved to the user's Trash as `tidb-intest-probe-20260812` after its evidence was captured.

## Interfaces and Dependencies

The public Rust interfaces remain `IN_TEST`, `ENABLE_ASSERT`, `ENABLE_INTERNAL_CHECK`, `assert`, `assert_with_message`, `assert_no_error`, `assert_no_error_with_message`, `assert_not_nil`, `assert_not_nil_with_message`, `assert_func`, and `assert_func_with_message`. The two mutable switches are now `LazyLock<AtomicBool>` and preserve existing `.load`/`.store` consumers through dereference. The contract uses only Rust's standard `std::panic`, `std::process`, environment, lazy-lock, and atomic APIs. No dependency or Cargo manifest change is required.

Plan revision note: created after complete package inventory, source pinning, owner/consumer reads, historical-receipt recovery, and validation design.

Plan revision note (2026-08-12): recorded Go matrix probes, the public startup-switch defect and fail-before-fix evidence, the shared `LazyLock` correction, compact receipt, and WIP validation.

Plan revision note (2026-08-12): recorded complete Ready evidence, the Bazel prepare decision, repository lint, semantic gate result, and recoverable cleanup of the temporary probe.
