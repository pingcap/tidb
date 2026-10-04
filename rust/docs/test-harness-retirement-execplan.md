# Retire non-behavioral checks and consolidate test targets

This living ExecPlan follows PLANS.md. Work stays in Codex Cloud on hparser-integration; no pushes or push dry runs are authorized.

## Purpose / Big Picture

Future test runs should compile fewer integration binaries and spend time on Go behavior and Rust correctness. Five module-safe standalone suites move into existing aggregate targets with behavioral cases retained. Four catalog cases replace six stale total-database assumptions with fixture-relative deltas; the other moved bodies remain unchanged. Remove three assertions that inspect source/doc text, two canned DDL-runner self-checks, and obsolete guard documentation. Production code, Go fixtures and known failing parity regressions remain intact.

## Progress

- [x] Inspect instructions and ownership from clean b68c1ec85c9b08ee322ac7ec4b880888d8d24490; compare Go LRU tests at master 93a01d31f6da205ae4bf376825293903a6899fdb.
- [x] Consolidate five suites containing 57 cases; retire the documentation guard binary, source-name assertion and two mocked DDL scripts.
- [x] Final aggregate/driver/LRU selection: 74 pass, zero failed/ignored. The original standalone planner baselines retain one pass and three failures. All-target checking, make lint and self-review pass.
- [x] Record inventory and validation; the local commit must pass the actual locked-build hook. Final commit, bundle verification and draft readback are recorded in /workspace/.cloud-setup/test-cleanup-batch/final-handoff.json.

## Context and Orientation

`rust/scripts/aggregate-tests.rs` includes integration modules unless they have a standalone marker. The five selected suites previously opted out; they were not duplicates. Removing both markers and Cargo targets registers each exactly once in `all`. Selected modules cover catalog reload, scan row caps, CTE storage, engine classification and resource-group tags. Snapshot lock-wait, transaction-size settings and lock-resolver metrics retain isolated targets because they mutate global configuration/counters. Session TopN/collation and async statistics-loading also remain isolated for their global mode/queues.

The kvcache receipt binary only read documentation filenames. The transaction guard searched four retired symbol spellings. The two DDL self-checks returned canned SQL/log events without running DDL. The Go LRU contract, transaction behavior tests, live DDL runners and package receipts remain.

## Plan of Work and Milestones

Remove registrations and markers atomically, preserving retained test bodies. Update current guidance to `cargo test --test all -- <module>` and retire guard narratives. Compile affected aggregates as one batch and select retained modules plus transaction-driver behavior. Run the small Go-derived LRU suite separately. Finish with all-target checking and required lint/build gates. This is infrastructure maintenance, not completion of a Go package or behavioral finding.

## Concrete Steps and Validation

From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh` and set CARGO_BUILD_JOBS=1. Build `--test all --no-run` for tidb-exec, tidb-executor, tidb-pd-client and tidb-txnkv together; execute selected modules with --test-threads=1. Run `cargo test --locked -p tidb-kvcache --test simple_lru_test`. Check affected crates with --all-targets. From repository root run `make lint` and `git diff --check`. A normal commit must pass hooks/pre-commit's `cd rust && cargo build --locked -p tidb-server`.

All 57 moved cases must be discovered exactly once in their aggregate. Eight existing transaction-driver behavior cases and nine LRU cases remain runnable. Confirm counts with actual harness lists/results, not syntax alone.

## Idempotence and Recovery

`/workspace/.cloud-setup/test-cleanup-batch/inventory.json` records old/new source hashes and deletions. Git retains deleted files. Disk cleanup may remove only identified inactive regenerable executables after recording hashes. Preserve current binaries, known failures and recovery evidence. Native client stays unchanged. Final local commit/bundle/draft receipts live in the same Cloud directory.

## Surprises & Discoveries

Remaining standalone registrations already had exclusion markers: this consolidates five link targets rather than claiming duplicate executions. Some targets need global-state isolation and remain separate. An initial plan write used a repository-relative path from the Rust directory and failed; corrected with an absolute path. That shell continued to the authorized batch build.

## Decision Log

- Decision: remove text/path guards and canned DDL checks; retain Go-derived behavior and Rust-specific correctness. Rationale: file layout and symbol spellings do not establish parity. Date: 2026-10-04.
- Decision: preserve historical command evidence in validation JSON while updating current guidance. Rationale: old executions must not be rewritten as current outcomes. Date: 2026-10-04.

## Outcomes & Retrospective

Final edits and validation complete. The 74 selected behavioral cases pass; three genuine standalone planner failures remain as documented. The initial aggregate run had four stale catalog-total failures and three genuine Go cardinality-fixture failures. Catalog loading now includes METRICS_SCHEMA, so six assumptions of one/two total schemas were obsolete; the tests now assert the diff's delta and dropped identity. Each stale case failed independently in a fresh process before this correction. The two session suites were initially considered for consolidation, then restored byte-for-byte after verifying their global-state requirements. Their three Go-estimate failures reproduce in the original standalone binaries and remain enabled. No expectation is changed to match incorrect planner estimates. Five link targets and one documentation-only binary are retired. No measured build-time/runtime speedup or whole-package acceptance is claimed.

## Interfaces and Dependencies

Only Cargo test registrations and test/document/script files change. Shared aggregate generation, production APIs, dependencies, lockfile and mandatory hooks remain unchanged.
