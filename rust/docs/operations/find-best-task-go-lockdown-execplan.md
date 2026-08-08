# Lock down `find_best_task.go` in `tidb-executor`

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept current while the work proceeds.

This document follows the repository contract in `PLANS.md`. It is deliberately self-contained: a future contributor should be able to reproduce the claim from this file and the named checked-in artifacts without relying on a local agent environment.

## Purpose / Big Picture

The Go planner implementation in `pkg/planner/core/find_best_task.go` is the source of truth for choosing a physical task and, for a `DataSource`, choosing and converting an access path. The Rust implementation is distributed across `tidb-executor` rather than living in a same-named file. This lockdown establishes one source-owned claim for that Go file: every production AST obligation and every direct test/support obligation in `find_best_task_test.go` receives exactly one auditable verdict, every `PORTED` verdict names a compile-anchored Rust symbol and a boundary receipt, and every non-ported verdict has measured or structural evidence. A drift gate regenerates the source/test AST identities so a future Go change cannot silently leave this claim stale.

No oracle ratchet is required to move. The deliverable is classification and behavioral completeness at the reachable Rust boundary. A falsified assumption or a zero-movement result is success when it is recorded precisely.

`pkg/executor/distsql.go` is not owned by this unit. It remains queued and unclassified here even where a physical scan eventually calls distributed execution.

## Progress

- [x] (2026-08-08) Created a fresh worktree at accepted SHA `5d4e8dccbe4e9b9a450f57b31db59f8e0447ffe4` on `codex/task325-tidb-executor-find-best-task-lockdown`.
- [x] (2026-08-08) Pushed the untouched base branch to both `origin` and `ngaut` and verified both refs resolve to the accepted SHA.
- [x] (2026-08-08) Rejected the mixed-source L6 inventory as a completion artifact; retained its pushed-cap test only as seed evidence and did not cherry-pick its series.
- [x] (2026-08-08) Generated and checked in the exact 1,667 production plus 61 direct test/support obligation ledger: 854 PORTED, 126 DECLINED, 748 UNREACHABLE.
- [x] (2026-08-08) Completed the source-order reachability audit. Ported the forced-path `preferRange` bypass and `matchProperty` Case 2 fixed-prefix rule; made parsed TABLESAMPLE fail closed rather than execute an ordinary scan.
- [x] (2026-08-08) Added source, AST, direct-test, verdict, compile-anchor, evidence, and mutation-receipt gates; all three central gate tests pass.
- [x] (2026-08-08) Killed eight independent boundary mutations and restored every production site. One initial point-get mutation survived the old test, causing a new closed/low-open/high-open receipt to be added before the rerun killed it.
- [ ] Run Ready validation, clean-worktree locked-workspace validation, ratchet checks, dual-push, ref verification, and reclaim this unit's worktree and target directory.

## Surprises & Discoveries

- Observation: the accepted tip already contains most of the access-path work that the old L6 branch described; L6's inventory nevertheless records `skylinePruning` as ported while explicitly documenting that forced paths are dropped by the `preferRange` post-filter. This is a false completion verdict, not a harmless note.
  Evidence: `rust/crates/tidb-executor/src/skyline.rs` carries no forced-path coordinate in `Candidate`, while Go appends forced paths unconditionally in the post-filter.

- Observation: Rust applies `matchProperty` as a pre-pruning filter for the only non-empty property path it exposes, making Go's pairwise `matchResult` dimension exactly zero for the candidates that reach Rust skyline comparison. The full Go matcher still contains planner surfaces Rust cannot construct (TiFlash/vector/common-handle merge-sort/index-merge/partial-order), so those branches must be classified individually rather than claiming the whole function from the reachable subset.
  Evidence: `rust/crates/tidb-executor/src/driver/leaf_access.rs` filters order-incompatible paths before `skyline_pruning`; `rust/crates/tidb-executor/src/skyline.rs` fixes the comparison coordinate to zero.

- Observation: the direct Go source uses a failpoint in the index-merge conversion path, so even the narrowly selected `TestFindBestTaskSuite` must run through the repository failpoint wrapper.
  Evidence: `failpoint.Inject("forceIndexMergeKeepOrder", ...)` at `find_best_task.go:2351`; `./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestFindBestTaskSuite$' -count=1` passed and restored the failpoint refcount to zero.

- Observation: the existing high-level point-get acceptance test did not kill removal of the open-endpoint guard. A passing mutation was a finding about the test, not evidence for the code.
  Evidence: after adding `point_get_rejects_either_open_endpoint`, the same mutation returned `Some(Int(7))` for the low-open boundary and was killed.

- Observation: the existing Go executor oracle proves TABLESAMPLE is a distinct physical sampling path, including a three-row table returning only its region sample, rather than an ordinary scan.
  Evidence: `./tools/check/failpoint-go-test.sh pkg/executor -run '^TestTableSampleBasic$' -count=1` passed and restored the failpoint refcount to zero.

## Decision Log

- Decision: own only `pkg/planner/core/find_best_task.go` and its direct `find_best_task_test.go` support, never the neighboring `pkg/executor/distsql.go` source.
  Rationale: one source-owned lockdown must not smuggle an unowned source into its verdicts.
  Date/Author: 2026-08-08 / Codex.

- Decision: generate obligation identities from the repository's Go AST inventory tool, then filter by exact source path and exact direct-test path.
  Rationale: text/line matching is not an AST drift gate and would miss closures, short-circuits, switch cases, and moved nodes. The established tool yields the accepted 1,667/61 category totals.
  Date/Author: 2026-08-08 / Codex.

- Decision: use owner-level Rust behavior boundaries but keep one row for every Go AST obligation. A row inherits a verdict only after the owning function is audited branch by branch; branch-specific exceptions override the owner mapping.
  Rationale: the ledger must be exhaustive without pretending that 1,728 separate Rust functions are required. The AST anchor and node hash preserve branch identity.
  Date/Author: 2026-08-08 / Codex.

- Decision: do not cherry-pick L6.
  Rationale: it mixes two Go sources, contains incomplete verdicts, and its production behavior is largely already present at the accepted tip. Useful tests may be re-expressed only after their exact owning rule is established.
  Date/Author: 2026-08-08 / Codex.

## Context and Orientation

The authoritative production source is `pkg/planner/core/find_best_task.go`; its direct Go test/support file is `pkg/planner/core/find_best_task_test.go`. The accepted production source is 3,329 lines, 136,221 bytes, SHA-256 `4f98311980c38ca56f98e21925c45e4a412c1481f61bd33711ca31058eabf25d`. The AST inventory has 1,667 production obligations: 68 functions, 7 closures, 5 declarations, 18 fields, 918 branches, 124 loops, 510 short-circuits, and 17 switch cases. The direct test/support inventory has 61 obligations: one test, 26 assertions, three helpers, three helper closures, and 28 test rows.

The Rust crate is `rust/crates/tidb-executor`. Its relevant implementation is spread across `src/driver.rs`, `src/driver/from.rs`, `src/driver/access.rs`, `src/driver/leaf_access.rs`, `src/access_cost.rs`, `src/skyline.rs`, `src/index_range.rs`, `src/plan_trace.rs`, and the existing driver tests. The checked-in lockdown ledger will live beside the Rust driver at `src/driver/find_best_task.inventory.tsv`. Supporting evidence and mutation receipts will use the same basename. A Rust test module in `src/driver/find_best_task_lockdown.rs` will gate the artifacts and aggregate compile-anchor registries exposed under `cfg(test)` by the owning implementation modules.

The inventory verdicts mean:

* `PORTED`: the Go rule is reachable and reproduced at the named Rust behavior boundary. The named symbol must appear in the compile-anchor registry and the evidence table must name at least one passing boundary test.
* `DECLINED`: Rust deliberately does not implement the reachable Go feature. The evidence table must quote the Go-side precondition and include a measured probe that demonstrates the Rust refusal or bounded behavior. A general architecture statement is insufficient.
* `UNREACHABLE`: no Rust input or plan node can satisfy the Go branch precondition. The proof must name the missing type/state/construction boundary and a checked test or static assertion that would fail if it became constructible.

## Plan of Work

First, generate the full AST ledger from the accepted Go source and direct test file. Check hashes, byte/line counts, exact category totals, unique obligation ids, allowed verdicts, and exact one-row coverage. Make the generator deterministic and check in the generator command/logic so regenerating against a changed Go source fails with a reviewable diff.

Second, audit each Go function in source order. Map generic logical-task search to Rust's property-driven builders only where the same state is constructible. Map `DataSource` skyline, property matching, access-path conversion, point/batch-point get, index/table scan, pushed selection, and empty-range behavior to their concrete Rust boundaries. Split verdicts at branch granularity whenever a Go function mixes reachable and absent planner surfaces. Port all reachable missing behavior, beginning with forced paths surviving the `preferRange` post-filter. Do not use a broad function-level `PORTED` verdict to hide TiFlash, vector, index-merge, MPP, common-handle, partition, or memo branches that Rust cannot or does not implement.

Third, recreate the direct Go tests at Rust boundaries. `testCostOverflow`, `testEnforcedProperty`, and `testHintCannotFitProperty` must either be behaviorally ported with boundary assertions or classified row-by-row with measured/structural proof. The top-level `TestFindBestTaskSuite` is a support dispatcher and must map to the exact Rust receipt set rather than being silently omitted.

Fourth, add compile anchors and gates. Every unique `PORTED` Rust symbol in the ledger must be present in the registry, and the registry must contain no extra symbol. Private functions stay private: each owning module exposes a test-only anchor function or registry that takes an actual function item/type reference, and the lockdown test aggregates the exported names. The gate also checks the evidence identities, source/test hashes, category totals, mutation-plan/results identity, clean restoration markers, and absence of TODO verdicts.

Fifth, mutation-probe every independent reachable rule at its boundary. Each mutation must change a condition or boundary, not a recorded expected answer. Run the named test and record that it fails for the intended reason, restore the production source, rerun it green, and record the exact commands plus concise observed failure. Independent non-ported rules receive proof probes rather than fake mutations of unreachable code.

Finally, format all Rust, run `cargo test -p tidb-executor --all-targets`, strict clippy for the crate, `make -j12 lint`, and the full locked-workspace gate in a separate clean worktree with its own target directory. Directly verify the current ratchets are query 0, catalog 100, table 1, integration 78. Commit, dual-push the exact final SHA, verify both remote refs, then reclaim only this unit's exact worktrees and target directories.

## Concrete Steps

All commands below run from the isolated worktree root unless explicitly stated.

    cd /private/tmp/codex-task325-tidb-executor-find-best-task-lockdown
    go run ./rust/difftests/tools/go_package_lockdown_inventory --root . --package pkg/planner/core

The checked-in generator filters the result to `pkg/planner/core/find_best_task.go` and `pkg/planner/core/find_best_task_test.go` and writes the four `find_best_task.*.tsv` artifacts. The source gate reruns the same derivation and compares it byte-for-byte.

Before Go tests or probes, scan the exact package and selected test dependencies for failpoint usage as required by `.agents/skills/tidb-failpoint-test-runner`. Use `./tools/check/failpoint-go-test.sh` if the scan finds failpoint usage; otherwise run only the exact named test/probe. No Go source or test file is committed by this unit, so `make bazel_prepare` remains unnecessary under `.agents/skills/tidb-bazel-prepare-gate`.

Rust validation uses a worktree-exclusive target:

    CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-executor-find-best-task-lockdown cargo fmt --all -- --check
    CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-executor-find-best-task-lockdown cargo test -p tidb-executor --all-targets
    CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-executor-find-best-task-lockdown cargo clippy -p tidb-executor --all-targets -- -D warnings
    make -j12 lint

The full locked-workspace gate is run from a separate clean worktree at the final commit, never from the main checkout and never sharing the target directory.

## Validation and Acceptance

Acceptance requires all of the following:

1. The generated production ledger has exactly 1,667 unique obligations with category totals 68/7/5/18/918/124/510/17 and the direct support ledger has exactly 61 with totals 1/26/3/3/28.
2. Every obligation has exactly one allowed verdict and no TODO, blank verdict, or silent omission exists.
3. Source file, direct test file, AST-node identities, hashes, sizes, counts, and test identities drift-gate against the accepted Go tree.
4. The set of unique `PORTED` Rust symbols equals the compile-anchor registry exactly, and every such symbol has a checked Rust behavior receipt.
5. Every `DECLINED` row points to measured refusal/bounded-behavior evidence; every `UNREACHABLE` row points to a structural proof and gate.
6. The forced-index `preferRange` mismatch and any other reachable mismatch discovered in the source-order audit are fixed and boundary-tested.
7. Mutation plan and results cover every independent rule, every mutation fails the intended test, every site is restored, and the final green run is recorded.
8. Ready-profile validation and the separate clean-worktree full locked-workspace validation pass; current ratchets read exactly 0/100/1/78.
9. The final commit is identical on both remotes and this unit's exact worktree/target directories are reclaimed.

## Idempotence and Recovery

The ledger generator and gates are deterministic and safe to rerun. Mutation steps are performed one at a time and the mutation result names the exact file and expected production text; before each mutation the script verifies the original text, and after the test it restores that exact text and verifies the worktree diff. If interrupted, compare the mutation results against the source and rerun any entry lacking a `restored` marker. Never use `git reset --hard` or read/repair from the divergent main checkout.

Remote pushes are safe to repeat after verifying the local branch SHA. Worktree reclamation happens only after both remotes resolve to the final SHA and validation artifacts are committed.

## Artifacts and Notes

The authoritative checked-in artifacts are:

* `rust/crates/tidb-executor/src/driver/find_best_task.inventory.tsv`
* `rust/crates/tidb-executor/src/driver/find_best_task.evidence.tsv`
* `rust/crates/tidb-executor/src/driver/find_best_task.mutation-plan.tsv`
* `rust/crates/tidb-executor/src/driver/find_best_task.mutation-results.tsv`
* `rust/crates/tidb-executor/src/driver/find_best_task.mutation-results.tsv`
* `rust/crates/tidb-executor/src/driver/find_best_task_lockdown.rs`
* a deterministic generator/gate under `rust/scripts/`

No artifact may include or classify `pkg/executor/distsql.go`. Its continued absence is itself asserted by the gate.

## Interfaces and Dependencies

The lockdown should not widen production visibility. Test-only anchor functions may be added under `#[cfg(test)]`; each must take real references to the private functions/types it claims and return the stable inventory names. The central gate consumes only those test-only registries plus the checked-in TSVs.

The existing Go AST inventory tool at `rust/difftests/tools/go_package_lockdown_inventory` is the parser dependency. The Rust gate may invoke it as a subprocess for drift checking, following existing source-lockdown tests in the workspace. No local-agent-only dependency or absolute developer path may appear in checked-in artifacts.

## Outcomes & Retrospective

The lockdown closed 1,728 obligations with no unclassified row: 854 PORTED, 126 DECLINED, and 748 UNREACHABLE. Three accepted-tip defects or silent gaps were resolved: forced paths now survive the `preferRange` post-filter, a single constant index prefix is skipped only after Case 1 has had the chance to match the requested fixed column, and TABLESAMPLE refuses rather than returning the ordinary table scan. The direct Go `TestFindBestTaskSuite` passed through the failpoint wrapper. Eight boundary mutations were killed and restored.

No result-oracle ratchet moved; direct grep still reads query 0, catalog 100, table 1, integration 78. This is explicitly a successful completeness increment rather than ratchet movement.

The old L6 whole-series transplant was falsified as unsafe: its mixed inventory did not own a complete source and documented forced-path and property-matcher gaps under broad PORTED verdicts. No L6 commit was cherry-picked. `pkg/executor/distsql.go` remains explicitly queued and unclaimed, exactly as required.

Ready and clean-worktree validation evidence is filled in at the final checkpoint; if a later command changes this source, the progress entry and receipt must be updated rather than leaving this outcome stale.
