# Restore static partition unique batch reads

This living ExecPlan follows root PLANS.md. It is a separate failed-case category after dynamic PointGet commit d260cb5cd8.

## Purpose / Big Picture

Restore Go's PartitionUnion of two BatchPointGet branches for the KEY-partitioned residual LIKE query in tests_partition.rs. The result must remain exactly abc,2, and branch read estimates must be 2.00 and 1.00.

## Progress

- [x] Capture Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85 output on real TiKV and unistore.
- [x] Reproduce wrong P1 row estimate and P2 TableFullScan; capture candidate costs.
- [x] Implement Go stats.go heuristic selection before skyline/cost and restore CountAfterAccess estimates.
- [x] Original regression, 98 partition tests, 4 candidate tests, 310 session integrations and Ready lint pass.
- [x] Real access-path passes on nightly PD/TiKV; owned nodes and playground data cleaned up.
- [x] Full session run detects a new account-table regression. Narrow run confirms it; diagnose missing common-key width check in the heuristic facts.
- [x] Add the exact util/path.go OnlyPointRange full-key-width condition and remove probes.
- [x] Final 310 session integrations and RealTiKV access-path pass after the common-key correction; serial full session suite is 1652 passed / 63 failed / 209 ignored, with the account-table regression restored.
- [x] Commit and integrate incoming IndexJoin change a5527590c4 without conflicts; preserve shared-statistics WIP.
- [x] Rebased serial session suite remains 1652/63/209 with the identical failure set; final 310 integrations and lint pass. Independent publication is the closing action.

## Surprises & Discoveries

Go removes the full table candidate before comparing costs; lowering a cost constant would not repair the missing phase. A complete point range on only the visible suffix of a common key is not a point path: all declared primary-index columns must be present. Without this condition, selecting mysql.user by User alone can choose an invalid pruned common-handle scan. The account regression is a required guard for the final change.

## Decision Log

Apply the heuristic only to unordered root enumerations whose native range facts are complete. Runtime IndexJoin and ordered-property enumerations can omit paths and must not run this selection over an incomplete set. Move tasks instead of cloning physical trees. Preserve the existing known failures and shared-statistics WIP.

## Milestones

The first milestone proves the failure with actual Go output and Rust candidate costs. The second implements candidate selection and separates point access estimates from residual filtering. The final milestone validates the original partition query, common-key safety, relevant integration and real access-path, then publishes an independent commit and Markdown receipt.

## Validation and Acceptance

From /tmp/tidb-hparser-current, use RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 with cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib, the --test all integration target, and -p tidb-planner --lib find_best_task::candidate::. Run make lint and git diff --check. Exact commands and log paths are in STATIC_PARTITION_BATCH_POINT_FIX.zh-CN.md. A nonzero full-suite status must be compared by failed test name, never relabeled green.

## Outcomes & Retrospective

The original partition failure and the discovered common-key regression are resolved and verified, including after integrating the concurrent IndexJoin change. Full-suite coverage was necessary because the new rule selects candidates for ordinary unpartitioned tables as well. The overall remaining Rust failures are not a readiness blocker and are not claimed complete here.
