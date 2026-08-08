# Lock down `pkg/executor/distsql.go` without inventing Rust parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows that file and the repository requirement that a file lockdown is seed evidence, not completion of the owning Go package.

## Purpose / Big Picture

The accepted Rust tree does not contain an implementation equivalent to Go's distributed `IndexReaderExecutor` and `IndexLookUpExecutor` lifecycle in `pkg/executor/distsql.go`. This unit makes that absence explicit and mechanically complete. Afterward, every AST obligation in the owning Go file and its direct tests has exactly one checked-in verdict, and a gate fails if the Go source, test evidence, verdict set, receipt, or a claimed Rust symbol drifts. This is successful even though no oracle ratchet moves and no production obligation is claimed PORTED.

## Progress

- [x] (2026-08-08) Verified exact accepted parent `842867801eaddcffc25e4de15aabb391f02b1968` on both remotes and announced the task branch at that SHA.
- [x] (2026-08-08) Inspected the stale `L6driver` delta as evidence and found no distsql-specific patch to transplant.
- [x] (2026-08-08) Chose a fresh file-lockdown seed rather than importing unrelated access-path work or asserting false parity.
- [x] (2026-08-08) Checked in the six-artifact manifest, 1,461-row AST ledger, mutation receipt, independent Rust gate, and checker.
- [x] (2026-08-08) Killed and restored all nine boundary mutations: path, hash, AST, test-support, row census, blank verdict, false symbol, and receipt drift.
- [x] (2026-08-08) Completed failpoint Go oracles, crate all-target tests, strict scoped Clippy, `make -j12 lint`, and the clean detached workspace replay at candidate `af69d58a0670712705ebe7adb4dc21e4576d0dcf`.
- [ ] Commit this final evidence note, replay the clean workspace at that final SHA, publish it to both task refs, verify, and reclaim only this unit's worktree and caches.

## Surprises & Discoveries

- Observation: `L6driver` is not a distsql implementation branch.
  Evidence: `git diff --stat 842867801eaddcffc25e4de15aabb391f02b1968...91b00c81f53baff28f068826e287c2c077143e55` lists only `access_cost.rs`, `driver` access files/tests, and `skyline.rs`; it lists neither `pkg/executor/distsql.go` nor a Rust distsql module.
- Observation: the generic package inventory cannot run directly over all of `pkg/executor` at this accepted tree because unrelated package files produce a duplicate obligation ID.
  Evidence: the command exits nonzero with `duplicate obligation id O5b7bb17b0c0ae508`. The lockdown checker therefore isolates only the owned Go source/test artifacts before invoking the same AST collector.
- Observation: direct owning test/support extends beyond the dedicated `distsql_test.go` file.
  Evidence: symbol-reference review found `IndexReaderExecutor` in `table_readers_required_rows_test.go`, executor type labels in `internal/exec/executor_test.go`, and `LookupTableTaskChannelSize` in `test/seqtest/seq_executor_test.go`; the complete isolated census is 904 production plus 557 direct test/support obligations.

## Decision Log

- Decision: do not merge, rebase, cherry-pick, edit, or delete the stale `L6driver` worktree or branch.
  Rationale: its changes belong to a different source surface, and importing them would violate the one-owner/file scope.
  Date/Author: 2026-08-08 / Codex.
- Decision: classify the accepted seed honestly with zero PORTED production obligations.
  Rationale: Rust has scan planning and storage seams but no compiled owner matching the Go file's distributed executor lifecycle. Mapping those seams as complete Go functions would be a false whole-function verdict.
  Date/Author: 2026-08-08 / Codex.
- Decision: use exact Go AST anchors and node hashes as the source quote for every DECLINED verdict, and require a compiled-symbol allowlist before any row can become PORTED.
  Rationale: this gives each obligation individual, drift-checked evidence without pretending that a generic prose reason proves behavior.
  Date/Author: 2026-08-08 / Codex.
- Decision: scope the claim as `file-lockdown-seed-not-package-completion`.
  Rationale: `pkg/executor` is much larger than one Go file; repository policy forbids reporting a partial file as a transcreated package.
  Date/Author: 2026-08-08 / Codex.

## Outcomes & Retrospective

The checked-in seed classifies all 1,461 obligations without claiming a production port: 904 production obligations and 557 direct test/support obligations are DECLINED with exact Go AST quote anchors. Nine gate mutations are killed and restored. Scoped failpoint-enabled Go oracles, the `tidb-executor` all-target Rust suite, strict scoped Clippy, `make -j12 lint`, and the clean locked workspace test pass. Direct ratchets remain `0/100/1/78`. Only the final documentation-SHA replay, publication, and exact artifact reclamation remain.

## Context and Orientation

`pkg/executor/distsql.go` is a 2,292-line Go source file implementing distributed index readers, index lookup workers, request construction, handle extraction, ordering, consistency checks, runtime statistics, cleanup, and concurrency/rate-limit policy. Its dedicated test owner is `pkg/executor/distsql_test.go`. The exported pure helper `CalculateBatchSize` also has direct assertions in `pkg/executor/test/issuetest/executor_issue_test.go`.

The Rust crate `rust/crates/tidb-executor` has planning and scan seams such as `access_path.rs` and `remote_scan.rs`, but it has no equivalent distributed executor module. This unit therefore owns only the new integration-test lockdown module and its adjacent receipt directory. It must not touch `find_best_task`, the stale L6 worktree, or another crate.

An AST obligation is one deterministic row emitted by `rust/difftests/tools/go_package_lockdown_inventory`: declarations, fields, functions, branch outcomes, loops, switch/select cases, short-circuit outcomes, and original test/support assertions. A DECLINED verdict means the Rust seed does not claim that obligation. It is not a waiver; the exact Go AST quote remains pinned so future work must consciously reclassify it.

## Plan of Work

Add `rust/crates/tidb-executor/tests/distsql_lockdown.rs` as an independent Rust gate. Store `artifacts.tsv`, `inventory.tsv`, `mutation-plan.tsv`, `mutation-results.tsv`, and `receipt.json` under the adjacent `tests/distsql_lockdown/` directory. Add `rust/scripts/pkg-executor-distsql-lockdown.py` to isolate the exact source/test files, rerun the generic Go AST collector, reconstruct every expected classification, validate content hashes, validate mutation evidence, and reconstruct the receipt.

Every inventory row includes the upstream obligation ID/category/source/anchor/node hash/owner, one verdict, one Rust symbol field, one exact Go AST quote reference, one reason, and one mutation policy. At this seed all 1,461 rows are DECLINED, the Rust symbol is `-`, and the symbol gate rejects a fabricated PORTED row. Future work may extend the same ledger only by adding real compiled owners and mutation evidence.

Run boundary mutations for missing/wrong source paths, stale hashes, AST drift after updating the artifact hash, deleted obligations, blank verdicts, fabricated PORTED symbols, stale direct-test support, and stale receipt fields. Restore every mutation and require both the Python and independent Rust gates to pass.

## Concrete Steps

From repository root in the task worktree:

    python3 rust/scripts/pkg-executor-distsql-lockdown.py
    ./tools/check/failpoint-go-test.sh pkg/executor -run '^(TestGetLackHandles|TestIndexLookUpStats)$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor/test/issuetest -run '^TestCalculateBatchSize$' -count=1
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo test --offline --locked -j12 -p tidb-executor --test distsql_lockdown
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo test --offline --locked -j12 -p tidb-executor --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo clippy --offline --locked -j12 -p tidb-executor --all-targets -- -D warnings <existing-workspace-allowances>
    git diff --check
    make -j12 lint

For the completion replay, create a second clean detached worktree at the final commit with its own Cargo target and run:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<clean-target> cargo test --offline --locked -j12 --workspace

Then rerun the checker, formatter, direct ratchet greps, diff check, and clean-status/SHA checks before publishing the task ref to both remotes.

## Validation and Acceptance

Acceptance requires exactly six source/test artifacts and exactly 1,461 AST obligations: 904 production plus 557 direct test/support. Every row must be DECLINED at this seed; there must be no PORTED, UNREACHABLE, blank, placeholder, or unclassified verdict. The checker and Rust gate must both reject missing evidence, stale hashes, deleted rows, fabricated PORTED symbols, and stale mutation/receipt data.

The Go package uses failpoints, proven by `failpoint.`/`testfailpoint.` matches and the Bazel failpoint dependency, so targeted Go tests use `tools/check/failpoint-go-test.sh`. No Go or Bazel file is added, removed, renamed, or import-edited, so `make bazel_prepare` is not triggered. Completion uses the Ready profile plus the explicitly requested clean full workspace test. The four direct ratchets must remain exactly `0/100/1/78`; no movement is required.

## Idempotence and Recovery

All check commands are read-only except normal build caches. Mutation probes edit only this unit's files or the owned Go source temporarily, use `apply_patch`, immediately restore the inverse patch, and verify the accepted hash afterward. If a probe survives, do not record it as killed; strengthen the gate first. Worktree reclamation targets only the exact temporary paths created for this unit.

## Artifacts and Notes

Accepted parent and both base task refs: `842867801eaddcffc25e4de15aabb391f02b1968`.

Stale evidence-only branch: `L6driver` at `91b00c81f53baff28f068826e287c2c077143e55`; no changes are transplanted.

Candidate clean-gate evidence: `CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-gate-task325-tidb-executor-distsql.w2eHt7 cargo test --offline --locked -j12 --workspace` passed at `af69d58a0670712705ebe7adb4dc21e4576d0dcf`; the detached worktree was clean, the checker reported six artifacts and 1,461 DECLINED obligations, and direct greps found ratchets `0/100/1/78`.

## Interfaces and Dependencies

The Python checker uses only the Python standard library, the repository's Go AST collector, Git, and checked-in files. The Rust gate uses the crate's existing `sha2` dev dependency and the standard library. It intentionally exposes no production API and adds no dependency.

Revision note (2026-08-08): initial plan records the falsified L6 assumption, exact claim boundary, isolated AST strategy, zero-PORTED decision, and required gates.

Revision note (2026-08-08): self-review added three omitted direct test/support owners, increasing the exact census from 1,143 to 1,461; all affected mutations and tests were rerun before the clean candidate gate was recorded.
