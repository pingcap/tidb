# Complete and certify `pkg/util/disjointset` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's disjoint-set package groups values that are transitively related. The dense implementation is used for continuous integer indexes; the sparse generic implementation assigns indexes to arbitrary keys. A complete Rust package must preserve the Go source tests, exact root direction, missing-value insertion, root-to-original-value lookup, reset behavior, invalid-index behavior, and the live chunk column-alias consumer. This plan inventories the complete Go package, adds a public contract for source behavior not directly asserted by its two tests, validates the live consumer, and publishes the evidence as one Go-package commit.

## Progress

- [x] (2026-08-11 19:08Z) Fixed the complete six-file Go inventory and accepted source pin `59dfa4d3b214ded26f957249efbda21f95149bb5`; current package bytes match the pin.
- [x] (2026-08-11 19:09Z) Confirmed there is no `doc.go`, generated input, fixture, testdata, build/platform variant, benchmark, fuzz target, example, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 19:12Z) Read every Go owner/test/build artifact, every Rust owner module, the historical port/integration corrections, and the live chunk consumer and tests.
- [x] (2026-08-11 19:13Z) Passed both Go source tests normally and under `-race`, four Rust owner tests, and three focused chunk consumer tests.
- [x] (2026-08-11 19:14Z) Used a public Go probe to fix exact dense/sparse root numbering, union direction, reset, duplicate-union, invalid-index, negative-size, and NaN-key behavior.
- [x] (2026-08-11 19:15Z) Added and passed the two-test public Rust contract and four-command atomic semantic receipt; no production defect appeared.
- [x] (2026-08-11 19:18Z) Completed Ready validation and self-reviewed the final three-file one-package diff.
- [x] (2026-08-11 19:27Z) Rebased the single package commit onto final fresh remote `28c40f1be` and repeated the complete Ready profile successfully.
- [ ] Push without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: the source package entered this Rust branch at `59dfa4d3b214ded26f957249efbda21f95149bb5`, even though upstream history attributes the last content change to `2ef60523a2aa2d3a1b445b836bf42a0b3a7fa89b`.
  Evidence: the former commit adds all six package paths to this branch and its bytes match HEAD; the latter is the last upstream package-content commit and has the same bytes. The receipt uses the branch's complete source-introduction commit.

- Observation: an older semantic receipt existed but was removed with the repository-wide semantic gate machinery.
  Evidence: `d8f037925` added `disjointset.semantic.toml`; `3353b29fb` later removed the gate machinery and receipts. Current package certifications use the restored compact receipt form, so the old deleted receipt is seed evidence, not a current atomic claim.

- Observation: sparse root-to-value lookup was once absent from the chunk integration and is now live.
  Evidence: `d8f037925` replaced a scan-based column owner lookup with `Set<usize>::find_value`; two owner identity tests and the public chunk-util contract exercise alias merging and cached reuse.

- Observation: Go's generic `comparable` constraint includes floating-point NaN keys, while Rust's hash map requires `Eq + Hash` and therefore excludes `f64`.
  Evidence: two Go `FindRoot(math.NaN())` calls return indexes 0 and 1 because NaN is not equal to itself. The only live Rust consumer uses `usize`; retaining `Clone + Eq + Hash` preserves hash-map complexity and makes this problematic key domain unrepresentable.

- Observation: signed sizes and indexes are a language boundary rather than a runtime branch in Rust.
  Evidence: Go panics for negative constructor sizes and invalid signed indexes. Rust accepts `usize`, so negative inputs cannot be constructed; out-of-domain nonnegative indexes still panic like Go.

- Observation: the compact semantic gate requires every evidence path to be known to Git before commit.
  Evidence: the first gate run rejected the new untracked public contract. Registering it with `git add -N` (intent-to-add, without staging contents) let the gate validate one package and four unique commands.

- Observation: the remote advanced once during the final publication check.
  Evidence: the first synchronized base was `39e2b57ae`; a fresh pre-push fetch found `28c40f1be`, which changed only expression/session SQL-compress files. The one disjointset commit rebased without conflict and the full Ready profile passed again.

## Decision Log

- Decision: Retain the current dense and sparse production implementations.
  Rationale: every Go source assertion and every representable public probe row matches, union direction is exact, and the live consumer passes. No source-observed production defect remains to fix.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `usize` indexes and `Clone + Eq + Hash` keys as native Rust boundaries.
  Rationale: Rust indexes cannot be negative, ownership requires cloning a value returned by value, and `HashMap` requires stable equality and hashing. The live `usize` consumer is inside this domain; accepting NaN would require a slower or user-defined key abstraction not demanded by Go tests or consumers.
  Date/Author: 2026-08-11 / Codex

- Decision: Add public contract tests without duplicating the two source tests.
  Rationale: private owner tests already map `TestIntDisjointSet` and `TestDisjointSet` line for line. The integration contract pins only source-observed public edges missing from those tests: exact roots, reset, root values, duplicate union, and invalid index.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete inventory, source pin, owner/source/build reads, history review, Go normal/race baseline, Rust owner baseline, public source probe, type-boundary decision, live-consumer baseline, public contract, current receipt, pre-sync Ready validation, final self-review, synchronization, and post-sync Ready validation are complete. Only the external non-force push and fresh-remote verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/disjointset/BUILD.bazel`, `int_set.go`, `int_set_test.go`, `main_test.go`, `set.go`, and `set_test.go`. `SimpleIntSet` stores one parent index per continuous integer and makes the second union operand's root the dense successor. `Set[T]` maps sparse values to insertion-order indexes, makes the first union operand's root the sparse successor, and maps any member index back to its current root value. `TestMain` configures TiDB's common Go test process and leak checker; Rust owner tests create no package-owned background worker, so it needs no runtime analogue.

Rust owns the package in `rust/crates/tidb-util/src/disjointset/{mod,int_set,set}.rs`. The only live Rust consumer is `rust/crates/tidb-chunk/src/chunk_util.rs`, where `ColumnSwapHelper` groups input columns that share one allocation and then asks the sparse set for the original owner index. Owner-private tests map the two Go tests; `rust/crates/tidb-util/tests/disjointset_contract.rs` holds public boundary evidence; chunk owner and integration tests prove the live use.

## Milestones

The source-oracle milestone inventories all six artifacts, lists exactly two Go tests, passes normal and race runs, and records the public probe. Acceptance is the literal dense roots `[1 1 1 3]`, reset roots `[0 1 2]`, sparse insertion indexes 0 and 1, sparse successor values `b` then `c`, one panic for an invalid index, constructor panics for negative sizes, and NaN indexes 0 then 1.

The parity milestone adds a public Rust contract without changing already-correct production code. Acceptance is four existing owner tests plus two new public tests, with exact representable outputs matching the probe.

The integration milestone validates both cached/concurrent chunk owner tests and the public chunk-util contract. Acceptance is correct four-output alias identity and one cached owner class.

The publication milestone adds the compact semantic receipt, runs the complete Ready profile, synchronizes one commit to current `hparser-integration`, pushes without force, and verifies matching local and freshly fetched remote SHAs.

## Plan of Work

Add `rust/crates/tidb-util/tests/disjointset_contract.rs` with one dense and one sparse public test. The dense test fixes exact root direction, clear invalidation, and grow reset. The sparse test fixes insertion indexes, first-operand successor direction, root-value lookup after a chained union, duplicate-union stability, and invalid-index panic.

Add `rust/crates/tidb-util/tests/disjointset.semantic.toml` with the accepted Go pin, three owner modules, the public contract, and the chunk consumer evidence. Do not edit Go, Bazel, Cargo manifests, production Rust, or chunk consumer code unless a new failing regression proves a defect.

## Concrete Steps

From repository root, run the Go authority and public probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/disjointset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestIntDisjointSet|TestDisjointSet)$' -tags=intest,deadlock -count=1 ./pkg/util/disjointset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestIntDisjointSet|TestDisjointSet)$' -tags=intest,deadlock -count=1 ./pkg/util/disjointset
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-disjointset-probe.go

From `rust`, run owner and consumer gates:

    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'disjointset::'
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --test disjointset_contract
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-chunk --lib 'chunk_identity_tests::column_swap_helper_'
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-chunk --test chunk_util_contract column_swap_identity_and_cache_contract -- --exact
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 --no-deps -p tidb-chunk --lib -- -D warnings

From repository root, validate the receipt and repository lint:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | CARGO_INCREMENTAL=0 /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/disjointset.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

Go must list exactly `TestIntDisjointSet` and `TestDisjointSet`; both must pass normally and under `-race`. Rust must pass all four owner tests, both public contract tests, both focused chunk owner tests, and the chunk integration contract. The full owning crate, every integration target, doctest, formatting, owner all-target Clippy, direct consumer Clippy, compact semantic receipt, repository lint, and `git diff --check` must pass. The final commit may contain only the public contract, receipt, and this plan unless a failing regression proves a production correction is necessary.

## Idempotence and Recovery

All tests and checks are safe to rerun. The Go probe lives only under `/tmp` and must be moved to Trash after its evidence is recorded. Cargo uses the explicit shared target with incremental compilation disabled; do not clean that shared target. If the remote advances, rebase the one package commit and repeat Ready before pushing.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    test list: TestIntDisjointSet, TestDisjointSet
    both source tests: pass normally and under -race
    dense roots: [1 1 1 3]; clear invalidates; grow roots: [0 1 2]
    sparse insert: b=0, a=1; union(b,a) root value b; union(c,b) root value c
    duplicate union retains root 2; invalid index panics
    negative dense and sparse constructor sizes panic
    repeated NaN lookup inserts roots 0 and 1

Initial Rust evidence:

    disjointset owner: 4 passed
    chunk alias owner consumers: 2 passed
    chunk public consumer contract: 1 passed

WIP evidence after adding the contract and receipt:

    disjointset owner: 4 passed
    public disjointset contract: 2 passed
    chunk alias owner consumers: 2 passed
    chunk public consumer contract: 1 passed
    semantic package gate: 1 package, 4 unique commands

Pre-sync Ready evidence:

    Go test list: exactly 2 named owner tests
    Go targeted owner tests: pass normally and under -race
    Go public probe: every recorded dense/sparse row remains exact
    disjointset owner: 4 passed
    public disjointset contract: 2 passed
    chunk alias owner consumers: 2 passed
    chunk public consumer contract: 1 passed
    full tidb-util: 344 passed, 1 ignored subprocess helper; every integration target and doctest passed
    cargo fmt --all --check: pass
    tidb-util all-target Clippy with -D warnings: pass
    direct tidb-chunk --no-deps library Clippy with -D warnings: pass
    semantic package gate: 1 package, 4 unique commands
    repository make lint with revive 1.2.1: pass
    git diff --check and three-file atomic-boundary self-review: pass

Post-sync Ready evidence on final remote base `28c40f1be`:

    Go list, targeted tests, -race tests, and public probe: pass with exact outputs
    semantic package gate: 1 package, 4 unique commands
    full tidb-util: 344 passed, 1 ignored subprocess helper; every integration target and doctest passed
    cargo fmt --all --check: pass
    tidb-util all-target Clippy and direct tidb-chunk --no-deps Clippy with -D warnings: pass
    repository make lint with revive 1.2.1: pass

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match in the package

Build metadata decision:

    make bazel_prepare is not required: no Go/Bazel/module/manifest edit, Go import change, or new Go test is planned

## Interfaces and Dependencies

The public `SimpleIntSet::{new,union,find_root,clear,grow_new_int_set,len,is_empty}` and `Set::{new,in_same_group,union,find_root,find_value,len,is_empty}` interfaces remain unchanged. The owner keeps standard `Vec` and `HashMap`; the consumer keeps `Set<usize>`. No new dependency or manifest change is planned.

Plan revision note: created after the complete inventory, source pin, all owner/test/build reads, port-history review, Go normal/race baseline, public Go probe, Rust owner baseline, and live chunk consumer baseline.
