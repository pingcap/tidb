# Complete and certify `pkg/util/generic` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's generic utility package provides a fixed-capacity best-N heap and a mutex-protected generic map. The heap is live in statistics TopN construction; the map's Go users coordinate DDL and resource-management state. A complete Rust package must preserve all eight Go source tests, heap replacement and snapshot behavior, map overwrite/delete/keys behavior, public language boundaries, and the live statistics consumer. This plan inventories the complete package, probes untested public edges, adds only missing source-backed evidence, and publishes one Go-package commit.

## Progress

- [x] (2026-08-12) Fixed the complete five-file Go inventory and accepted source pin `59dfa4d3b214ded26f957249efbda21f95149bb5`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, generated input, fixture, testdata, build/platform variant, benchmark, fuzz target, example, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-12) Read every Go source/test/build artifact, both Rust owner modules and tests, the historical package claim, and the live statistics TopN consumer.
- [x] (2026-08-12) Ran Go normal/race baselines, the Rust owner baseline, and the focused statistics consumer baseline.
- [x] (2026-08-12) Recorded a public Go probe and resolved every representable source behavior and native Rust boundary.
- [x] (2026-08-12) Added minimal public contract evidence and a compact semantic receipt; no source-backed production defect was found.
- [x] (2026-08-12) Completed Ready validation and self-reviewed the final one-package diff.
- [ ] Publish and verify the one package commit (completed: rebased onto `3f323cc16` and repeated Ready; remaining: normal push and fresh-remote SHA verification).

## Surprises & Discoveries

- Observation: `BoundedMinHeap` moved from a private statistics implementation to the shared util owner before this audit.
  Evidence: commit `ada3473c3db604356d04c55c9ae9ef7d1d06326e` removed `tidb-stats/src/bounded_min_heap.rs`, made `tidb-stats/src/builder.rs` consume `tidb_util::generic::BoundedMinHeap`, and added the first whole-package receipt.

- Observation: the old receipt used non-branch source pin `e14a77f78d457d27c88d5892e1a173c28a586823` and was removed with repository-wide gate machinery.
  Evidence: current branch commit `59dfa4d3b214ded26f957249efbda21f95149bb5` adds all five accepted paths and matches HEAD exactly; `3353b29fb` later removed the old receipt.

- Observation: Go's negative map capacity is accepted as a preallocation hint, while its zero-value map is readable and deletable but panics on store.
  Evidence: the public probe printed `negative-map panic=<nil>` and `zero-map load=(0,false) delete=(0,false) ... store-panic=assignment to entry in nil map`.

- Observation: Go permits comparator magnitudes that Rust's `Ordering` deliberately cannot represent.
  Evidence: a comparator returning `math.MinInt` or `math.MaxInt` produced `[3 2 1]`; the Go implementation negates the comparator during sorting, while Rust reverses an `Ordering` without integer overflow.

- Observation: every representable public edge matched without a production edit.
  Evidence: zero-capacity add made zero comparator calls, an equal fourth item did not replace the first three, mutating a returned snapshot did not mutate the heap, and map delete returned the overwritten value followed by absence.

- Observation: the isolated worktree did not initially contain the repository's ignored `tools/bin/revive`, and its attempt to install the pinned version hit a network timeout.
  Evidence: the first lint attempt reported a missing binary; the normal install then timed out at `proxy.golang.org`. Copying the main clone's verified `revive v1.2.1` into the isolated ignored tools directory allowed the exact lint recipe to pass.

- Observation: the target branch advanced after Ready validation, but the new package commit does not overlap this audit.
  Evidence: explicit fetch moved `origin/hparser-integration` from `cb224fb4b` to `3f323cc16` (`rust: match selector wildcards by bytes`), whose diff changes only `rust/crates/tidb-util/src/filter/tests.rs` and `rust/crates/tidb-util/src/table_rule_selector.rs`.

## Decision Log

- Decision: Use the branch source-introduction commit `59dfa4d3b214ded26f957249efbda21f95149bb5` as the complete Go package pin.
  Rationale: it is an atomic five-artifact snapshot on the target branch and its package bytes match current Go source exactly.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat `usize` capacity, non-null comparator values, `Ordering`, and `Option` as candidate native Rust boundaries pending public probes.
  Rationale: Rust excludes negative capacities and nil callable values by type, models comparison sign without arbitrary integer magnitudes, and fuses Go's zero-value-plus-presence pair into `Option`. The audit must still prove all representable outputs and live-consumer behavior before accepting these boundaries.
  Date/Author: 2026-08-12 / Codex

- Decision: Accept `usize`, callable values, `Ordering`, `Option`, and initialized `RwLock<HashMap<...>>` as native Rust boundaries.
  Rationale: probes confirmed the excluded Go states are negative capacities, nil comparators, arbitrary comparator magnitudes, and the partially usable zero-value map. No live Rust consumer depends on them; representable heap and map behavior matches, and reproducing Go's zero-map panic or unsynchronized `Keys` length read would weaken the Rust API.
  Date/Author: 2026-08-12 / Codex

- Decision: Do not claim a stable order among distinct equal-ranked heap items.
  Rationale: Go's `slices.SortFunc` does not promise stable ordering for equal elements. The public contract instead proves the source-backed strict replacement rule by checking that the fourth equal item is absent without fixing an unspecified snapshot order.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The complete inventory, source pin, source/build/test reads, owner/history review, baselines, public probe, native-boundary decisions, minimal public contract, receipt, live-consumer mapping, staged-diff self-review, rebase, and post-rebase Ready repetition are complete. No production defect was found. Normal publication and fresh-remote verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/generic/BUILD.bazel`, `bounded_min_heap.go`, `bounded_min_heap_test.go`, `sync_map.go`, and `sync_map_test.go`. `BoundedMinHeap` keeps the comparator-worst retained item at the root, replaces it only for a strictly better arrival, and returns a best-to-worst copy. `SyncMap` wraps a Go map with an `RWMutex`, returning `(value, present)` from load/delete and an unordered key snapshot. There are seven heap tests and one map test.

Rust owns these APIs in `rust/crates/tidb-util/src/generic/{mod,bounded_min_heap,sync_map}.rs`. Its private owner tests map all eight Go tests and add concurrent map coverage. `rust/crates/tidb-stats/src/builder.rs` is the only live Rust consumer: it retains TopN candidates by count before pruning and histogram construction. The consumer's source-oriented tests are aggregated under the `tidb-stats` Cargo target `all`.

## Milestones

The source-oracle milestone passes all eight Go tests normally and under the race detector and records a temporary public probe for empty/nil snapshots, strict equal-item rejection, non-mutating snapshots, comparator calls and panics, negative/nil construction, zero-value and constructed maps, delete return values, overwrite, key snapshots, and negative capacity.

The parity milestone adds an external Rust contract only for public behavior not already demonstrated through the private owner tests. Any production correction must first fail against the old implementation while the Go probe supplies the expected row.

The integration milestone runs the complete `builder_source` statistics consumer surface and checks that TopN count ordering and pruning still use the generic heap.

The publication milestone adds the compact receipt, completes Ready, rebases one commit onto an explicitly fetched `hparser-integration` tip, repeats Ready after any rebase, pushes without force, and verifies local, remote-tracking, and `ls-remote` SHAs.

## Plan of Work

Run the Go test list, targeted source tests, race tests, and a temporary public probe outside the repository. Compare each representable row with both Rust owner modules. Add `rust/crates/tidb-util/tests/generic_contract.rs` only for external API boundaries missing from owner tests and `rust/crates/tidb-util/tests/generic.semantic.toml` for the complete owner/public/consumer evidence. Do not edit Go, Bazel, Cargo manifests, or production Rust unless a failing source-backed regression proves a defect.

## Concrete Steps

From repository root, run the Go authority and probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/generic
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestBoundedMinHeap.*|TestNewBoundedMinHeapSafetyChecks|TestSyncMap)$' -tags=intest,deadlock -count=1 ./pkg/util/generic
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestBoundedMinHeap.*|TestNewBoundedMinHeapSafetyChecks|TestSyncMap)$' -tags=intest,deadlock -count=1 ./pkg/util/generic
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-generic-probe.go

From `rust`, run owner, public, and consumer gates with `CARGO_INCREMENTAL=0` and the shared `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-util --lib 'generic::'
    cargo test --offline --locked -j12 -p tidb-util --test generic_contract
    cargo test --offline --locked -j12 -p tidb-stats --test all 'builder_source::'
    cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    cargo clippy --offline --locked -j12 --no-deps -p tidb-stats --all-targets -- -D warnings

From repository root, validate the receipt, lint, and atomic diff:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/generic.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint
    git diff --check

## Validation and Acceptance

Go must list exactly the seven heap tests and `TestSyncMap`; all must pass normally and under `-race`. Every public probe row must map to an exact Rust assertion or an explicit native type/API boundary justified against live consumers. Rust must pass the complete generic owner surface, public contract, focused statistics consumer, full owning crate, fmt, owner/consumer Clippy, semantic gate, repository lint, and atomic diff checks. Publication is accepted only after a normal push and a fresh explicit fetch show matching local and remote SHAs.

## Idempotence and Recovery

All tests and checks are safe to rerun. The Go probe lives only under `/tmp` and must be moved to Trash after its evidence is recorded. Cargo uses the shared target with incremental compilation disabled; do not clean it. This clone tracks only `origin/master` by default, so publication fetches must use the explicit hparser refspec. If the remote advances, rebase the one package commit and repeat Ready. Never force push.

## Artifacts and Notes

Failpoint decision:

    No package file contains failpoint imports, calls, or Bazel failpoint dependencies; use ordinary targeted Go tests.

Build metadata decision:

    make bazel_prepare is not required: the planned diff changes only Rust tests, one receipt, and this plan, with no Go/Bazel/module/manifest edit, Go import change, or new Go test.

Ready evidence:

    Go listed exactly 8 tests; targeted normal and race runs passed.
    Rust owner tests: 9 passed; public contract: 2 passed; statistics consumer: 24 passed.
    Before rebase, complete tidb-util: 344 passed, 1 existing ignored; all integration and doc tests passed.
    After rebase, complete tidb-util: 346 passed, 1 existing ignored; the two added selector tests and all integration/doc tests passed.
    cargo fmt, tidb-util Clippy, no-deps tidb-stats Clippy, semantic package gate, and make lint passed.
    The staged diff contains only the public contract, compact receipt, and this ExecPlan; no Go, Bazel, Cargo manifest, optimizer, transaction, or production file changed.
    The temporary Go probe was moved to the user's Trash as tidb-generic-probe-20260812.go.

## Interfaces and Dependencies

The public Rust interfaces are `BoundedMinHeap::{new,len,is_empty,add,to_sorted_slice}` and `SyncMap::{new,store,load,delete,keys}`. The heap uses only `Vec` and `Ordering`; the map uses `HashMap` and `RwLock`. The live consumer remains `tidb-stats` TopN construction. No new dependency or manifest change is planned.

Plan revision note: created after the complete inventory, source pin, all source/test/build reads, historical claim review, and live-consumer mapping.

Plan revision note (2026-08-12): recorded baseline and probe completion, corrected the negative map-capacity assumption, accepted the probed native Rust boundaries, and documented why equal-item snapshot order is not a contract.

Plan revision note (2026-08-12): recorded the completed Ready evidence and the recoverable pinned-linter bootstrap after the network install timeout.

Plan revision note (2026-08-12): recorded staged-diff self-review and recoverable cleanup of the temporary probe.

Plan revision note (2026-08-12): recorded the unrelated target-branch advance that requires rebase and full Ready repetition before publication.

Plan revision note (2026-08-12): recorded successful rebase onto `3f323cc16` and the complete post-rebase Ready repetition.
