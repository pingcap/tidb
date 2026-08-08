# Lock down `pkg/util/ranger/detacher.go` without reopening existing owners

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while executing it.

Reference: `PLANS.md` at repository root. The repository-wide completion unit remains one complete Go package; this plan produces only one explicitly incomplete file-lockdown seed.

## Purpose / Big Picture

After this work, every production AST obligation in `pkg/util/ranger/detacher.go` and every obligation in its directly coupled Go tests, benchmarks, and helpers has one checked-in `PORTED` or `DECLINED` verdict. A verifier regenerates the Go AST census, pins every owning Go artifact by hash, checks every compiled Rust symbol, checks mutation source paths and hashes, and validates a content-addressed receipt. The existing locked `index_range.rs` and existing planner detacher are consumed without modification.

This does not claim that Go package `pkg/util/ranger` is complete. `checker.go`, `points.go`, `ranger.go`, `types.go`, and their remaining test/support artifacts retain independent ownership and package-level completion work.

## Progress

- [x] (2026-08-08) Verified both accepted remote refs at `6fa49fb9112c850ffd9861651792cd043b830a8d` and created an isolated `tidb-executor` worktree and target directory.
- [x] (2026-08-08) Proved no existing `detacher.go` lockdown; the existing `index_range.inventory.tsv` claims only `ranger.go` and explicitly excludes `detacher.go`.
- [x] (2026-08-08) Inventoried 676 production obligations and 1,412 direct test/benchmark/helper obligations.
- [x] (2026-08-08) Added only the missing native `removeConditions` and `AppendConditionsIfNotExist` rules, with compile/behavior anchors to the existing planner and executor owners.
- [x] (2026-08-08) Killed and restored 12 boundary mutants across six suites.
- [x] (2026-08-08) Generated and validated the content-addressed receipt after all owned implementation content stabilized.
- [x] (2026-08-08) Passed Ready scoped Go/Rust checks, strict clippy, `make -j12 lint`, direct ratchet grep, and the clean detached full Rust workspace test.
- [x] (2026-08-08) Prepared the final local commit on the preserved task branch for coordinator handoff without pushing.

## Surprises & Discoveries

- Observation: the first DNF test hypothesis was false. A branch containing both an access predicate and a residual filter remains usable for range construction; it sets `hasResidual`. Only a branch with zero access predicates invalidates the whole DNF.
  Evidence: the initial `dnf_detachment_rejects_filter_only_branch` test failed because the native result retained the branch's access predicate. The corrected filter-only branch passes and the mutation changing it to access is killed.

- Observation: the native implementation is intentionally split across crates.
  Evidence: `tidb_planner::range_detacher::{detach_cnf_predicates, detach_dnf_predicates}` owns normalized boolean traversal, while `tidb-executor/src/index_range.rs` owns executable column/index ranges and explicitly defers some full-Go seams.

- Observation: Go `AppendConditionsIfNotExist` does not deduplicate new candidates against one another.
  Evidence: Go checks each candidate only against the original `conditions` slice before appending all of `shouldAppend`; the Rust boundary test pins `[1,2] + [2,3,3] == [1,2,3,3]` and kills a growing-result deduplication mutant.

## Decision Log

- Decision: keep `tidb-executor` as the receipt owner and compile the public planner seam through its existing dependency.
  Rationale: this preserves one-crate ownership and avoids reopening or duplicating the existing planner implementation.
  Date/Author: 2026-08-08, Codex.

- Decision: classify only exact isolated rules as `PORTED`; do not treat a whole complex Go function as ported merely because `index_range.rs` implements part of its behavior.
  Rationale: the Go functions also own session context, collation, memory fallback, partition recursion, mutable `valueInfo`, and residual-expression reconstruction that the native seam does not represent.
  Date/Author: 2026-08-08, Codex.

- Decision: include exactly 13 direct Go caller tests, two direct benchmarks, `TestBenchDaily`, three ranger test helpers, and five benchmark helpers.
  Rationale: the set is the transitive helper closure of exact calls to symbols owned by `detacher.go`; unrelated package tests and the unrelated `BenchmarkBuildColumnRangeLongIN` are excluded.
  Date/Author: 2026-08-08, Codex.

## Outcomes & Retrospective

The file receipt is complete: 676 production obligations and 1,412 direct test/support obligations are classified exactly once, with 42 `PORTED`, 2,046 measured `DECLINED`, no `UNREACHABLE`, and no unclassified rows. Twelve boundary mutations were killed and restored. The initial DNF brief was falsified and corrected before the ledger was finalized.

The Ready gates passed: the exact direct Go test set, all 534 `tidb-executor` unit tests plus integration targets, strict all-target clippy, `make -j12 lint`, the content-addressed verifier, direct ratchet constants `0/100/1/78`, and `cargo test --offline --locked -j12 --workspace` in a separate clean detached worktree. No oracle ratchet moved; that is a successful lockdown outcome because completeness, not ratchet movement, is the deliverable.

The remaining gap is intentional and explicit: this file seed does not complete Go package `pkg/util/ranger`, and its 2,046 declines require the absent Go expression/session/collation/value/range interfaces before promotion.

## Context and Orientation

The Go source `pkg/util/ranger/detacher.go` separates predicates used to build table/index ranges from predicates retained as filters. Its direct Go support is in `pkg/util/ranger/ranger_test.go` and `pkg/util/ranger/bench_test.go`.

The Rust planner's normalized CNF/DNF traversal is `rust/crates/tidb-planner/src/range_detacher.rs`. The executable index/column derivation is the already-locked `rust/crates/tidb-executor/src/index_range.rs`, whose owning inventory is `ranger.go`, not this file. The new `rust/crates/tidb-executor/src/ranger_detacher_lockdown.rs` owns this receipt and the two condition-list rules that had no native symbol.

The verifier `rust/scripts/pkg-util-ranger-detacher-lockdown.py` copies only the three pinned Go artifacts into an isolated temporary root, invokes `rust/difftests/tools/go_package_lockdown_inventory`, filters the exact direct support owner set, regenerates every verdict, verifies compiled symbols in their real source files, verifies mutation paths and hashes, and checks the final receipt.

## Plan of Work

Maintain the exact artifact manifest and AST inventory beside the new Rust owner. Keep the classifier conservative: every `PORTED` row must name one compiled symbol and killed mutation suite; every `DECLINED` row must include its Go AST quote/hash and a measured missing interface. Never add a whole-function `PORTED` verdict for a partially represented range path.

Exercise normalized CNF/DNF semantics, executable DNF/composite-index ranges, single-column intersection and residual retention, ordered removal, and original-slice-only append semantics. For every mutation, change only the new receipt owner, run its named test, record the nonzero exit, restore the owner, and pass the full receipt suite.

When content is stable, regenerate the receipt, run the verifier, run targeted Go owners under the repository failpoint policy, then execute Ready Rust checks and clean full-workspace validation. Do not push any branch.

## Concrete Steps

From repository root, regenerate and validate the ledger:

    python3 rust/scripts/pkg-util-ranger-detacher-lockdown.py --write-inventory
    python3 rust/scripts/pkg-util-ranger-detacher-lockdown.py --inventory-only
    python3 rust/scripts/pkg-util-ranger-detacher-lockdown.py --write-receipt
    python3 rust/scripts/pkg-util-ranger-detacher-lockdown.py

The final verifier must print 3 artifacts, 2,088 AST obligations, 42 `PORTED`, 2,046 `DECLINED`, zero `UNREACHABLE`, and 12 killed mutations.

Run direct Go tests from `pkg/util/ranger` after confirming no failpoint references or Bazel failpoint dependency:

    go test -run '^(TestTableRange|TestIndexRangeForUnsignedAndOverflow|TestColumnRange|TestIndexRangeForYear|TestPrefixIndexRangeScan|TestIndexRange|TestShardIndexFuncSuites|TestRangeFallbackForDetachCondAndBuildRangeForIndex|TestRangeFallbackForBuildTableRange|TestRangeFallbackForBuildColumnRange|TestPrefixIndexRange|TestMinAccessCondsForDNFCond|TestBinCollationRangeForIndex|TestBenchDaily)$' -tags=intest,deadlock

From `rust`, using the worktree-exclusive target:

    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-executor --all-targets
    cargo clippy --offline --locked -j12 -p tidb-executor --all-targets -- -D warnings

From repository root:

    git diff --check
    make -j12 lint

Finally create a fresh detached worktree at the local final commit and run from its `rust` directory with another exclusive target:

    cargo test --offline --locked -j12 --workspace

## Validation and Acceptance

Acceptance requires that deleting or renaming any `PORTED` symbol fails the verifier; changing any pinned Go source or direct support AST fails the verifier; changing any mutation source path/hash or marking a survivor killed fails the verifier; and every named boundary test passes after restoration.

The production census must remain exactly 676 and the direct support census exactly 1,412. No row may have a blank or unknown verdict. The receipt must say `whole_go_package_complete: false`.

No Go or Bazel file is changed, so `make bazel_prepare` is not required. No failpoint references are present in `pkg/util/ranger`, so the scoped Go tests run directly rather than changing global failpoint state.

## Idempotence and Recovery

Inventory and receipt generation are deterministic and safe to rerun. Mutation probes must restore `ranger_detacher_lockdown.rs` and pass its complete six-test suite before recording `restore_status=PASS`. If an expensive gate fails, keep the branch and artifacts intact, record the exact failure, fix only an in-scope cause, and rerun the affected gate plus final verifier.

## Artifacts and Notes

The exact ledger lives in `rust/crates/tidb-executor/src/ranger_detacher_lockdown.inventory.tsv`. Its adjacent artifact manifest, mutation plan/results, and receipt are part of the same content-addressed unit.

The accepted parent is `6fa49fb9112c850ffd9861651792cd043b830a8d`. Remote publication is prohibited for this unit; only the coordinator may integrate and push after independent gating.

## Interfaces and Dependencies

The new native helpers are:

    pub(crate) fn remove_conditions<T: Clone + PartialEq>(conditions: &[T], conditions_to_remove: &[T]) -> Vec<T>

    pub(crate) fn append_conditions_if_not_exist<T: Clone + PartialEq>(conditions: &[T], conditions_to_append: &[T]) -> Vec<T>

The receipt compiles and behavior-tests `tidb_planner::range_detacher::{detach_cnf_predicates, detach_dnf_predicates}` and `crate::index_range::{detach_conds_for_column, detach_cond_and_build_range_for_index}` without editing their source files.

Revision note (2026-08-08): created the plan after the source/test census and mutation findings were known, so it records the measured file boundary and the corrected DNF rule rather than the falsified initial hypothesis.
