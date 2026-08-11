# Complete and certify `pkg/util/intset` as one Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's `intset` utility is a small-integer-optimized set used by expression, statement-context, and planner code. Completion means all four Go package artifacts, six original tests, six benchmarks, bitmap and sparse representations, integer boundaries, copy behavior, and every public operation are bound to a Rust owner and independently observable public contract. A Rust caller can construct, mutate, iterate, compare, shift, format, and combine sets with the same representable results as the Go source.

## Progress

- [x] (2026-08-12) Fixed the four-artifact Go inventory and accepted source pin `56d06365eae71a692e538986d84003565f880103`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, failpoint, generated input, fixture, fuzz target, example, platform/build-tag variant, `go:generate`, or `go:embed`; inventoried six tests and six benchmarks.
- [x] (2026-08-12) Read every Go source/test/benchmark line, Bazel metadata, the complete Rust owner, historical receipt, prior mutation commits, case map, and Rust consumers.
- [x] (2026-08-12) Ran all six Go tests normally and under race, all six benchmarks once, and the Rust owner baseline.
- [x] (2026-08-12) Added a three-test external Rust public contract and compact semantic receipt without changing production code.
- [x] (2026-08-12) Completed WIP and Ready validation: Go normal/race tests and all six benchmarks, Rust owner/public/full-crate tests, formatting, Clippy, semantic source/inventory/command gates, repository lint, and atomic source checks passed.
- [ ] Rebase one complete package commit onto a fresh target tip if needed, push without force, and verify fresh local, remote-tracking, and remote-advertised SHAs.

## Surprises & Discoveries

- Observation: The earlier `52423bab5` commit fixed a real empty-set shift mismatch but changed only one production file and one internal test.
  Evidence: its complete diff is 16 added lines in `rust/crates/tidb-util/src/intset.rs`; it has no package inventory, benchmark evidence, receipt, public contract, or ExecPlan. Repository policy therefore treats it as seed evidence rather than a complete package claim.

- Observation: Go intentionally retains sparse representation after a set becomes large, including after `Clear` or `CopyFrom` from a small-only target.
  Evidence: a large receiver copied from `{1}` reports `Len()==0`, `Has(1)==true`, an empty `SortedArray`, semantic equality with the target, and an error from `GetSmallUInt64`. Existing Rust owner tests preserve this source behavior.

- Observation: Go's `intsets.MaxInt` is both an insertable value for length/membership/sorted-array purposes and the `Next`/`ForEach` termination sentinel.
  Evidence: the owner contract records `Len()==1`, `Has(MaxInt)==true`, and a sorted array containing MaxInt, while `Next(MaxInt)` reports false, iteration visits nothing, and string formatting produces `()`.

## Decision Log

- Decision: Use `56d06365eae71a692e538986d84003565f880103` as the complete Go package pin.
  Rationale: it is the historical package receipt pin, is an ancestor of the target branch, enumerates the same four direct artifacts, and every current byte matches it.
  Date/Author: 2026-08-12 / Codex

- Decision: Complete the package with an external public contract and receipt even though the prior owner contains extensive internal tests.
  Rationale: the prior empty-shift patch is valuable production evidence but cannot establish a complete atomic package claim. An integration test proves the exported API independently of private fields and `cfg(test)`, while the receipt binds every accepted Go artifact and validation command.
  Date/Author: 2026-08-12 / Codex

- Decision: Exclude planner and functional-dependency consumers from this package gate.
  Rationale: the user explicitly excluded optimizer work. The Rust package has no non-optimizer production consumer beyond its owner; public API coverage and the complete owning crate prove this utility without broadening the claim into optimizer code.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat Go nil slices, shallow struct assignment, mutable iteration callbacks, and platform `int` width as native Rust boundaries.
  Rationale: Rust exposes empty `Vec<i64>`, move/deep-copy ownership, borrow-checked iteration, and the target's explicit `i64` domain. Public source methods and every representable result remain covered without reproducing nil identity, aliasing, or architecture-dependent types.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

Complete inventory, source pinning, implementation reads, boundary analysis, Go normal/race tests, benchmark support, Rust owner coverage, the external public contract, compact receipt, and the full Ready gate are complete. Publication and fresh-remote verification remain.

## Context and Orientation

The accepted package is exactly `pkg/util/intset/BUILD.bazel`, `fast_int_set.go`, `fast_int_set_test.go`, and `fast_int_set_bench_test.go`. Go stores values from zero through 63 in a `uint64`. The first negative or at-least-64 value creates an `intsets.Sparse` representation containing every prior small value; that representation is retained after removals and clears. `Next` seeks only non-negative values and uses `intsets.MaxInt` as its not-found sentinel.

The Rust owner is `rust/crates/tidb-util/src/intset.rs`. It uses the same `u64` fast path and a `BTreeSet<i64>` sparse representation. `MAX_INT` and `MIN_INT` bind the 64-bit Go target's integer domain. Existing owner tests map all six Go tests and additionally capture wraparound shifts, empty shifts, the MaxInt sentinel, representation retention, full bitmap ranges, panic text, string pair/range formatting, and idiomatic iteration.

The six Go benchmarks are original support artifacts. They compare map, `intsets.Sparse`, and `FastIntSet` difference and insertion behavior. They are not performance acceptance thresholds, but every benchmark must compile and execute at least once.

## Milestones

The authority milestone fixes the four accepted artifacts, six tests, six benchmarks, source pin, owner, historical partial patch, and optimizer exclusion. It passes Go normal/race tests, one-iteration benchmarks, and the Rust owner before any new evidence file.

The public-contract milestone adds `rust/crates/tidb-util/tests/intset_contract.rs`. It exercises construction, bitmap-to-sparse transitions, retained sparse state, source sentinel behavior, public copy independence, mixed-representation equality and algebra, iteration order/domain, wrapping shifts, inclusive ranges, error text, panic text, and formatting. No production edit is planned because current owner behavior matches the source evidence.

The completion milestone adds `rust/crates/tidb-util/tests/intset.semantic.toml`, runs the public and owner tests plus the complete owning crate, formatting, owner Clippy, semantic source/inventory/evidence/command gates, repository lint, and atomic diff checks.

The publication milestone fetches `hparser-integration` with an explicit refspec, rebases the one-package commit if the remote advanced, repeats Ready after any rebase, pushes normally, and verifies the local, remote-tracking, and `git ls-remote` SHAs agree. Force push is forbidden.

## Plan of Work

Keep Go, Bazel, production Rust, Cargo manifests, optimizer modules, and transaction modules unchanged. Add a public integration contract that uses only exported `FastIntSet`, `MAX_INT`, and `MIN_INT` interfaces. Add a compact semantic receipt binding `rust/crates/tidb-util/src/intset.rs`, its module export, and the public contract to the accepted pin. Keep the commit limited to this contract, receipt, and ExecPlan unless a new fail-before-fix regression proves a production mismatch.

## Concrete Steps

From repository root, run the Go authority and benchmark support:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/intset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^Test(FastIntSet.*|GetSmallUInt64)$' -tags=intest,deadlock -count=1 ./pkg/util/intset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^Test(FastIntSet.*|GetSmallUInt64)$' -tags=intest,deadlock -count=1 ./pkg/util/intset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^$' -bench '^Benchmark(MapIntSet_Difference|IntSet_Difference|FastIntSet_Difference|IntSet_Insert|Sparse_Insert|FastIntSet_Insert)$' -benchtime=1x -count=1 -tags=intest,deadlock ./pkg/util/intset

From `rust`, set `CARGO_INCREMENTAL=0` and use `/tmp/tidb-package-audit.DnxFlT/rust/target` as `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-util --lib 'intset::tests::'
    cargo test --offline --locked -j12 -p tidb-util --test intset_contract
    cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, recover the removed semantic gate runner read-only from `3353b29fb^`, install the cached `revive` binary into ignored `tools/bin`, and run the receipt, lint, and atomic-boundary checks. Do not run `make bazel_prepare` unless the final diff triggers the repository gate.

## Validation and Acceptance

Go must list exactly six tests and six benchmarks; normal/race tests and every one-iteration benchmark must pass. Rust must pass all owner and public contracts, the complete owning crate, formatting, Clippy, the semantic package gate, repository lint, and diff checks. The accepted four Go artifacts must remain byte-identical to the pin. The final staged diff must contain only the external public contract, compact receipt, and this ExecPlan unless a proven production mismatch requires a regression and correction.

Publication is accepted only after a normal push and fresh explicit fetch show local HEAD, `origin/hparser-integration`, and `git ls-remote` at the same SHA.

## Idempotence and Recovery

Tests, benchmarks, and semantic checks are safe to rerun. Cargo uses a shared target directory with incremental compilation disabled; do not clean it wholesale. The clone tracks only `origin/master` by default, so always fetch the target branch with an explicit refspec. If the remote advances, rebase and repeat Ready. Never force push.

## Artifacts and Notes

Failpoint decision:

    No accepted package file references failpoint or testfailpoint, and BUILD.bazel has no failpoint dependency. Ordinary targeted Go tests are correct.

Build metadata decision before edits:

    make bazel_prepare is not required for the intended Rust-only contract, receipt, and plan diff. No Go file, Go import block, Go test function, Bazel file, module dependency, or Cargo manifest is planned to change. This decision will be repeated against the final diff.

Baseline evidence:

    Go listed 6 tests and 6 benchmarks; normal and race tests passed.
    All 6 benchmarks executed successfully with benchtime=1x.
    Rust owner 16/16 and external public contract 3/3 passed; the initial broad substring filter also selected 4 disjointset tests, so completion uses the narrower intset::tests:: filter.
    git diff --exit-code 56d06365eae71a692e538986d84003565f880103..HEAD -- pkg/util/intset exited successfully.

Ready evidence:

    cargo test --offline --locked -j12 -p tidb-util --lib 'intset::tests::': 16 passed.
    cargo test --offline --locked -j12 -p tidb-util --test intset_contract: 3 passed.
    cargo test --offline --locked -j12 -p tidb-util: 347 passed, 1 existing ignored; all integration tests and doctests passed.
    cargo fmt --all --check: pass.
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings: pass.
    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/intset.semantic.toml: 1 package, 3 unique commands.
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint: pass.
    Source pin and four-artifact inventory gate: pass.

The final diff remains Rust-only evidence: one public contract, one compact receipt, and this ExecPlan. It changes no Go source, Go import block, Go test function, Bazel file, module dependency, or Cargo manifest, so `make bazel_prepare` is not required.

## Interfaces and Dependencies

The public Rust interfaces remain `FastIntSet`, `FastIntSetIter`, `MAX_INT`, `MIN_INT`, `new`, `of`, `len`, `is_empty`, `only1_zero`, `insert`, `next`, `remove`, `clear`, `has`, `sorted_array`, `iter`, `for_each`, `copy`, `copy_from`, `equals`, `get_small_uint64`, `difference`, `difference_with`, `union`, `union_with`, `intersection`, `intersection_with`, `intersects`, `subset_of`, `shift`, and `add_range`, plus `Display`, `PartialEq`, `Eq`, and borrowed `IntoIterator`. No dependency or manifest change is planned.

Plan revision note: created after complete inventory, source pinning, source/owner/history/consumer reads, Go baseline/race/benchmark execution, and Rust owner baseline.

Plan revision note (2026-08-12): recorded the public contract, compact receipt, narrow owner count, WIP evidence, and complete Ready results with no production change required.
