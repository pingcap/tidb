# Audit `pkg/util/mathutil` against the Rust owner

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to
it.

## Purpose / Big Picture

Certify the complete Go package `pkg/util/mathutil` against the Rust owner
`rust/crates/tidb-util/src/mathutil`. The Go package and its unit tests define
the behavior contract. Completion means every production source, test/support
file, build input, edge case, and real Rust consumer has an explicit mapping or
adaptation decision, and one package-scoped commit is published normally to
`hparser-integration`.

## Progress

- [x] (2026-08-12) Fixed the package boundary and source pin
  `0eb881e406a4df7f7e44f94f6c12be1616aeffd7`; all eight top-level Go package
  files are byte-identical to that pin.
- [x] (2026-08-12) Inventoried the Go production sources, tests, `TestMain`,
  Bazel input, Rust owner modules, and Rust consumers.
- [x] (2026-08-12) Confirmed there is no `doc.go`, failpoint, generated input,
  build/platform variant, fixture, benchmark, fuzz target, or example.
- [x] (2026-08-12) Ran the Go package normally and with the race detector, the
  16-test Rust mathutil slice, and all five current Rust consumer slices on the
  clean target base; all pass.
- [x] (2026-08-12) Added a regression for the usable Go zero value of
  `ExponentialMovingAverage`, proved it failed before the fix with E0599, and
  implemented the minimal Rust `Default` equivalent; a Go probe fixed the
  expected infinity behavior before the test was finalized.
- [x] (2026-08-12) Reconciled integer wrapping and panic order, IEEE-754
  propagation, RNG seed truncation and mutex behavior, and EMA constructor and
  warmup behavior. No second production gap was found.
- [x] (2026-08-12) Added the whole-package semantic receipt with the owner,
  Cargo integration points, and all current Rust consumers.
- [x] (2026-08-12) Completed the Ready validation gate: Go normal/race,
  19-test Rust owner slice, full `tidb-util`, all receipt consumers, Clippy,
  formatting, semantic gate, repository lint, source pin, inventory,
  failpoint/Bazel decisions, and diff checks pass.
- [ ] Publish exactly one package-scoped commit to `hparser-integration`, then
  verify local, remote-tracking, and `ls-remote` SHAs.

## Surprises & Discoveries

- A historical commit `fede31609dc2c477360e67cdf23a869d5a2c4c9b`
  described mathutil as complete, but changed several consumer packages in the
  same commit and pinned the older Go tree
  `e14a77f78d457d27c88d5892e1a173c28a586823`. It is seed evidence rather than
  a valid current atomic package claim.
- The Go `ExponentialMovingAverage` type has a usable all-zero value. Finite
  `Add` calls remain in the EMA branch with factor zero and keep positive zero,
  while an infinite sample produces NaN through IEEE-754 `infinity * 0`. Rust
  keeps the fields private and originally provided only `new`, so that source
  construction state could not be represented.
- Go's `MysqlRng` zero value has a nil mutex pointer and panics on every method.
  Existing Go and Rust consumers construct the RNG through the seed/time
  constructors; Rust's always-valid `Mutex<State>` deliberately excludes this
  unusable state.
- The source package has only eight direct files. `main_test.go` installs Go
  process setup and leak detection; the Rust tests create no persistent worker
  and need no corresponding global harness.
- `Divide2Batches` creates its result capacity before performing division. The
  Rust implementation does the same: non-representable or negative capacity
  fails before division, while zero capacity succeeds and division by zero is
  the next failure. Valid positive inputs cannot overflow `size + 1`.
- `Clamp` uses the same ordered branches as Go rather than a total-order helper,
  so NaN is returned unchanged and equal signed zeros select the supplied bound.
  `Abs`, `NextPowerOfTwo`, and both RNG update equations use explicit wrapping
  where Go machine integers wrap.

## Decision Log

- Decision: Treat all direct files under `pkg/util/mathutil` as one atomic Go
  package and keep every consumer source unchanged.
  Rationale: repository policy defines the complete upstream Go package as the
  minimum claim and commit unit; consumer tests are integration evidence, not
  additional package ownership.
  Date/Author: 2026-08-12 / Codex

- Decision: Add `Default` for `ExponentialMovingAverage` with the all-zero
  source state.
  Rationale: Go exposes a usable zero value without calling the constructor.
  `Default` is Rust's direct, idiomatic representation and does not weaken the
  validated `new(factor, warmup_window)` constructor.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep `MysqlRng` constructor-only and recover a poisoned Rust mutex.
  Rationale: reproducing Go's nil-pointer zero value would introduce
  `Option<Mutex<_>>` solely to preserve an unusable state. All real consumers
  use constructors. Recovering poison preserves Go mutex availability after a
  panic while retaining serialized state access.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep Go's documented preconditions on `NextPowerOfTwo` rather than
  replace wrapping operations with validation or saturation.
  Rationale: callers must provide a positive, non-overflowing value. The Rust
  implementation still uses explicit wrapping operations so out-of-contract
  machine arithmetic does not accidentally differ between debug and release.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The EMA construction gap is fixed with a regression, every reviewed boundary
is either source-equivalent or an explicit adaptation, and the package receipt
replays successfully. The complete Ready gate passes. Publication and remote
SHA verification remain.

## Context and Orientation

The complete Go package inventory is:

    pkg/util/mathutil/BUILD.bazel
    pkg/util/mathutil/exponential_average.go
    pkg/util/mathutil/exponential_average_test.go
    pkg/util/mathutil/main_test.go
    pkg/util/mathutil/math.go
    pkg/util/mathutil/math_test.go
    pkg/util/mathutil/rand.go
    pkg/util/mathutil/rand_test.go

The Rust owner consists of `rust/crates/tidb-util/src/mathutil/mod.rs`,
`math.rs`, `rand.rs`, and `exponential_average.rs`. The Go tests
`TestExponential`, `TestStrLenOfUint64Fast`, `TestClamp`,
`TestNextPowerOfTwo`, `TestDivide2Batches`, `TestRandWithTime`,
`TestRandWithSeed`, and `TestRandWithSeed1AndSeed2` map to tests beside those
Rust modules. Extra Rust tests cover source behavior not asserted by the Go
suite, including signed overflow, IEEE-754 special values, constructor
boundaries, warmup transitions, and concurrent RNG serialization.

The current consumers use `MysqlRng` through `tidb-expr`, `tidb-executor`, and
`tidb-session`, and use `clamp` through `tidb-stats`. Their focused tests are
part of the receipt so the owner-to-consumer contract is rechecked without
including consumer changes in this package commit.

## Plan of Work

First add an EMA regression that constructs `ExponentialMovingAverage` through
`Default`, observes positive-zero before updates, confirms finite samples keep
positive zero, and confirms an infinite sample produces NaN. Run that test
before implementing `Default` and record the compile failure. Then derive the
exact all-zero state and rerun the focused test.

Next compare each Go operation to Rust at architecture and language boundaries.
For `Divide2Batches`, preserve capacity creation before division so invalid
batch counts fail in source order. For `Abs`, `NextPowerOfTwo`, and RNG seed
math, retain explicit wrapping. For floating-point paths, check NaN, infinity,
signed zero, constructor comparisons, nonpositive warmup, and the mean-to-EMA
transition. Record intentional language adaptations in this plan.

Finally add `rust/crates/tidb-util/tests/mathutil.semantic.toml`, run the Ready
profile, self-review the exact diff, fetch and rebase on the latest target if
needed, repeat affected gates, create one commit, and push without force.

## Concrete Steps

Run commands from the repository root. Use the pinned Go toolchain and the
shared isolated Cargo target:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      go test -count=1 -tags=intest,deadlock ./pkg/util/mathutil
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      go test -race -count=1 -tags=intest,deadlock ./pkg/util/mathutil
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target \
      cargo test -p tidb-util mathutil -- --test-threads=1

The receipt also runs the focused `tidb-expr`, `tidb-executor`, `tidb-session`,
and `tidb-stats` consumers. Ready validation adds the full `tidb-util` suite,
all-target Clippy with warnings denied, workspace formatting, the historical
semantic package gate, and repository `make lint`.

## Validation and Acceptance

No failpoint lifecycle is required because source, tests, and BUILD metadata
contain no failpoint references. `make bazel_prepare` is not required when the
final diff contains only Rust, TOML, and Markdown files and changes no Go import
or top-level Go test, Bazel target, or module dependency.

Acceptance requires the Go normal/race suites, the Rust owner suite, all listed
consumer suites, full `tidb-util`, Clippy, formatting, semantic receipt, and
repository lint to pass. The pinned package inventory and bytes must still
match, `git diff --check` must pass, and the single commit must be a normal
fast-forward whose local, tracking, and server SHAs agree.

## Idempotence and Recovery

All package inventory, source-pin, test, lint, and formatting checks are safe
to rerun. If the remote target advances, rebase the one local package commit,
repeat the affected Ready gates, and push normally. Never force-push.

## Artifacts and Notes

Clean-base evidence at `3a7798a933e8b428d6263f477e21ceaeece38510`:

    Go normal: pass
    Go race: pass
    Rust mathutil: 16 passed
    tidb-expr RAND consumer: 2 passed
    tidb-executor RAND consumer: 5 passed
    tidb-executor default-expression consumer: 1 passed
    tidb-session RAND consumer: 2 passed
    tidb-stats clamp consumer: 5 passed

EMA regression evidence:

    Pre-fix Rust: E0599, no `ExponentialMovingAverage::default`
    Go zero-value probe bits: +0, +0 after finite 8, NaN after +Inf, NaN thereafter
    Post-fix Rust focused regression: 1 passed
    Go maximum setter seeds: output +0, then seed1=0 and seed2=32
    Go Clamp: equal signed zero selects the bound; NaN value is returned unchanged

Ready evidence on target base `3a7798a933e8b428d6263f477e21ceaeece38510`:

    go test -count=1 -tags=intest,deadlock ./pkg/util/mathutil (pass)
    go test -race -count=1 -tags=intest,deadlock ./pkg/util/mathutil (pass)
    cargo test -p tidb-util mathutil -- --test-threads=1 (19 passed)
    cargo test -p tidb-util (363 passed, 1 ignored; integration/doctests pass)
    semantic-package-gate.py mathutil.semantic.toml (1 package, 7 commands)
    cargo clippy -p tidb-util --all-targets -- -D warnings (pass)
    cargo fmt --all --check (pass)
    make -o tools/bin/revive lint (pass)
    source pin, inventory, failpoint, Bazel, and diff checks (pass)

Plan revision note (2026-08-12): created after complete package inventory,
historical-claim review, clean-base tests, and consumer mapping.

## Interfaces and Dependencies

`ExponentialMovingAverage` remains exported from `tidb_util::mathutil` and gains
`Default<Output = ExponentialMovingAverage>` semantics through Rust's standard
`Default` trait. `MysqlRng` retains `new_with_seed`, `new_with_time`, `gen`, and
its seed getters/setters. The arithmetic helper signatures remain unchanged.
