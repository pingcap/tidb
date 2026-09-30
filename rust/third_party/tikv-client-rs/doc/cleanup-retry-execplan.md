# Preserve cleanup retry ownership after a query kill

This living ExecPlan follows the TiDB workspace PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose and source


The complete pinned client-go transaction package is the comparison unit: `github.com/tikv/client-go/v2@v2.0.8-0.20260928031501-8edb23f6c7ee/txnkv/transaction`, used by TiDB master `51a1a4abfc192a91f98fe968ad87eced9221f663`. Native baseline is `8b890e2b0e1c40842f91dd865783431cf986a811`. `cleanup-retry-inventory.json` inventories every artifact of that Go package, including original tests. No package-level doc.go exists. This repairs the existing native transaction package; it is not a new transcreation or complete behavioral parity claim.

Go `txn.go:newCleanupBackoffer` copies Variables, clears Killed and KillSignalHandler, and retains the caller's context and retry policy. Ordinary pessimistic compensation, explicit lock rollback, failed prewrite cleanup and initialization cleanup use that constructor. Pipelined lock finishing and secondary commit deliberately use ordinary backoffers and remain interruptible by their configured policy.

## Progress


- [x] Pull native master and TiDB integration/master refs; audit Go cleanup call sites.
- [x] Reproduce killed-query suppression of each native ordinary cleanup retry path.
- [x] Share cleanup backoffer construction without changing foreground or explicit retry limits.
- [x] Verify metadata/context preservation, regression suites, formatting and lint.
- [x] Review the native repair and complete the publication gates for master.

## Context and implementation


The production paths are `Transaction::pessimistic_lock_rollback` and `Committer::rollback` in `src/transaction/transaction.rs`. Both currently create source retry owners with the original transaction Variables. A transient region error then invokes the killed-query check and prevents rollback from retrying, leaving potentially acquired locks behind. Tests use the existing injected MockPdClient/MockKvClient transport and produce one retryable region error before success.

Use one private Go-shaped cleanup-backoffer constructor in the transaction package. Clone the entire Variables object, replace only its atomic kill signal with an independent zero value and remove its kill handler. The Rust kill field is mandatory, so an independent zero atomic represents Go's nil. Reuse the existing retry-option admission check: `RetryOptions::none()` and finite custom policies must not be replaced with source defaults. Carry any supplied cancellation handle unchanged. Do not modify foreground, secondary commit or pipelined retry behavior, or manually edit generated output.

## Milestones


The reproduction milestone added three transport-level tests and ran them against unchanged production code. All three failed with `Static(QueryInterrupted)` after the first retryable region response (log `/private/tmp/native-cleanup-retry-red.log`).

The implementation milestone introduces `new_cleanup_backoffer` and centralizes the decision to use default cumulative retry limits in `TransactionOptions::source_retry_owner`. Cleanup passes the cleanup constructor to that decision; ordinary operations retain their constructor. Existing custom retry policies remain on their original path. The acceptance milestone requires the three reproductions to pass, explicit no-retry and finite limits to remain bounded, cancellation and other Variables fields to survive copying, and all native library tests to pass before publication.

## Validation


Run native commands from `/Users/qiliu/projects/client-rust`. First run `cargo test --locked --lib source_cleanup_retries_ignore_query_kill` and retain failing RPC evidence before production edits. Rerun it after the repair, then `cargo test --locked --lib -- --test-threads=1` for the existing native package tests and `cargo clippy --locked --lib -- -D warnings` for static validation. Format only changed Rust sources with the repository rustfmt configuration. Inspect `git diff --check` and the final diff. No Go, protobuf or Bazel inputs change. No benchmark benefit is claimed; the successful first-attempt path must remain equivalent.

## Surprises & Discoveries


TiDB's latest branch deliberately reverted the earlier native owner integration in `2471be70e9` without a reason. The user subsequently explicitly authorized removal of all reviewed duplicates and updating the TiDB dependency. The restoration and dependency refresh are now authorized; the historical repair below was independently validated before that decision. Its caller/store/background bridge lifetime inference remains a separate gap.

## Decision Log


Use the source's shared cleanup constructor rather than suppressing all kill checks in RetryBackoffer: foreground operations and ordinary secondary/pipelined work retain Go's kill behavior. Clone all settings rather than constructing defaults, to preserve future Variables fields. Never clear the shared atomic itself, which would incorrectly revive the foreground query.

## Outcomes & Retrospective


The three reproductions failed before the production change with `Static(QueryInterrupted)` and pass afterward. All 10 tests selected by `source_cleanup` pass, including existing cumulative-budget coverage and the new no-retry/finite-budget cases across all three cleanup paths. The complete native library suite passes 1,399 tests with two previously ignored tests. Strict library Clippy, changed-source rustfmt, the diff whitespace check and SHA-256 verification of all 16 pinned Go package artifacts pass.

Exact validation commands run from the native repository:

    cargo test --locked --lib source_cleanup_retries_ignore_query_kill
    cargo test --locked --lib source_cleanup
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    rustfmt --edition 2021 --check src/transaction/transaction.rs
    git diff --check

Logs are `/private/tmp/native-cleanup-retry-red.log`, `native-cleanup-retry-green.log`, `native-cleanup-retry-lib.log` and `native-cleanup-retry-clippy.log`. The initial finite-budget test fixture incorrectly returned success after its first failure; it was corrected to return repeated region failures when testing exhaustion. The final finite-budget tests stop after exactly one RPC for no retries and two RPCs for one permitted retry. The repaired default cleanup path succeeds after two RPCs without invoking the killed-query handler or resetting the caller's atomic signal.

This repository has no `make lint` target; strict Clippy is the native lint gate. TiDB files, protobuf generation inputs, Go files and Bazel metadata were not changed. TiDB server builds and Go lint were not rerun for this independent native repair. Real TiKV faults, feature-matrix builds, sysbench, TPC-C, TPC-H and YCSB were not verified. The change copies Variables and creates an independent kill atomic only when creating a default cleanup owner; ordinary successful transaction paths are unchanged. Transport/store cancellation ownership remains a separate structural gap, and the TiDB integration is being restored in the authorized follow-up. No full-parity or benchmark-improvement claim is made.

## Recovery


Keep native changes separate from the reverted TiDB integration. The final `git fetch origin master` found no concurrent native updates. Commit the reviewed source and the two evidence documents, then push native HEAD to origin/master without force and verify the remote commit. Do not manufacture a TiDB vendor patch for a native algorithm.
