# Retire the disconnected sort pipeline


This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture


Remove the disabled substitute parallel sort and its private harness as one dependency closure. Sorting continues through the existing serial partitions and result heap; developers no longer compile disconnected workers or run tests claiming those workers are active. Go's parallel sort remains a missing obligation, not a repaired finding.

## Progress


- [x] Confirmed clean Cloud checkout, refreshed Go master, inspected callers and disabling commit.
- [x] Removed the disconnected worker, spill coordinator, no-op concurrency setting and private partition adapters; retained active single-chunk spill coverage.
- [x] Removed their private merger/cursor harness and worker task counter; corrected current inventory.
- [x] 48 grouped sort/TopN/panic tests plus three existing SQL sort consumers passed; all-target checking and lint passed.
- [x] Locked server build passed.
- [x] Self-reviewed caller closure and unchanged finding dispositions; recorded source hashes and validation receipt.
- [ ] Commit through the actual hook, rebuild immediately before push, verify remote SHA; final publication results live in `/workspace/.cloud-setup/sort-cleanup/final-handoff.json` to avoid self-referential commit metadata.

## Context and Orientation


Work in `/workspace/tidb`, branch `hparser-integration`, starting at `e3d660a68ac746966d63efc0838fe5329db5015f`. `/workspace/client-rust` remains unchanged at `8b752f9638ad157931725b66ffdc57e0465432a9`. Fresh Go master is `7a3dacb52efe58d28db360ae8639d8838c376544`; comparison sources are `/workspace/.cloud-setup/go-master/pkg/executor/sortexec`. A sorted run is a sequence of rows already in ORDER BY order; the result heap merges such runs. Rust's active owners are `rust/crates/tidb-executor/src/sort.rs`, `sort_partition.rs`, `topn.rs` and `topn_spill.rs`. The disabled pipeline lives in sort.rs and parallel_sort_spill_helper.rs; its generic merger in multi_way_merge.rs has no other production callers. Shared panic recovery in sort_util.rs remains live across executors.

## Milestones and Concrete Steps


First remove the whole disconnected graph and its private tests, including no-op SortExec::with_parallelism and its physical builder caller. Keep TopN's live concurrency. Remove unused cursor/message adapters and test-only pool task counting. Keep active sorting, spill cleanup, comparison errors, quota cancellation and panic recovery tests. Correct module docs and the complete sortexec artifact inventory; historical receipts keep their dated meaning.

Then, from `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh`, set `CARGO_BUILD_JOBS=1`, and run:

    cargo test --locked -p tidb-executor --lib -- sort::tests:: sort_partition::tests:: sort_util::tests:: topn::tests:: topn_spill::tests:: topn_chunk_heap::tests:: --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-server
    cargo build --locked -p tidb-server

Run `make lint` from `/workspace/tidb`. Keep logs in `/workspace/.cloud-setup/sort-cleanup`. The actual precommit hook and fresh pre-push locked build remain mandatory. Do not force-push.

## Validation and Acceptance


Retained tests must verify ordered rows with NULLs/ties, spilled multi-run results, memory cancellation, temporary-file cleanup, TopN behavior and panic recovery. All-target checking catches external callers of retired APIs. No runtime behavior fix is claimed, so no new implementation-mirroring regression is added. Retarget the existing single-chunk test to the active serial owner and exclude its empty trailing partition from the spilled-run assertion. Record exact results and source hashes in `rust/docs/parity/current-audit/sort-cleanup-validation.json`. Finding statuses remain unchanged; no Go suite, live multi-node or performance acceptance is inferred.

## Surprises & Discoveries


Commit `b00685bdd413629f7a37b82b24fb593b5cd80f06` removed the only dispatch to fetch_and_sort_parallel as an invented implementation. It retained the whole dependency graph and tests still asserting multiple workers. Current inventory incorrectly describes that pipeline as active. The single-chunk spill test also counts the serial loop's empty trailing partition as a required spilled run.

## Decision Log


2026-10-07: finish the earlier retirement instead of reconnecting an incomplete parallel substitute. Remove only the proven disconnected closure and record missing Go obligations. Keep shared panic recovery and live TopN behavior. Removing stale harnesses must not be described as completing parallel sort.

## Outcomes & Retrospective


Removed 2,856 net Rust lines and 37 private tests across the disconnected worker/spill/merger/cursor graph. All 51 retained targeted tests, affected all-target checking and lint pass. The locked server build passed. Actual-hook and fresh pre-push build results are recorded in the external final handoff. No source behavior or structural finding closure is claimed.

## Idempotence and Recovery


Use git diff before each mutation; preserve unrelated changes. Deleted source is recoverable from the starting commit. Never rerun the one-time editing script against already edited files. Compiler dependency artifacts remain cached; no cargo clean.

## Interfaces and Dependencies


SortExec retains its constructor and Executor lifecycle; only its no-op concurrency setter disappears. TopN retains its separate concurrency setting. No Cargo dependencies or native-client source change. The panic helpers retain their signatures.
