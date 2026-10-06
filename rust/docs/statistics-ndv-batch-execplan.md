# Preserve statistics uniqueness and sketches through collection and reload

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture


Follow Go master `3ca96b1d5df8da123e7a650512654eedab12c861` across one statistics lifecycle: schema uniqueness determines exact non-NULL distinct counts during ANALYZE; local unique partition counts add without sketch estimation; statistics JSON and LOAD STATS retain the sketches needed for later partition merging. NDV means number of distinct values. An FM sketch estimates that number for values not known to be unique.

## Progress


- [x] Confirm clean TiDB `hparser-integration` at `16d92bb829d7ba842d70d380c4f6f4c279b78936`, native master at `cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1`, current Go reference and actual locked-build hook.
- [x] Locate collection, blocking/asynchronous merge, JSON conversion and statement-level storage owners; confirm four connected live mismatches.
- [x] Five Rust regressions fail before repair; expanded original-server probe has 12 passing and three failing checks. Implement shared schema/NDV policy, both production merge modes, the in-process caller and complete sketch conversion/write/dump carriers. Remove duplicate in-process global sketch merging.
- [x] 220 Rust tests and 15 real MySQL/unistore TCP checks pass. All-target checks, lint, changed-region formatting and the locked server build pass.
- [x] Self-review and publication preparation complete. Normal hook, fresh immediate-prepush build, remote SHA and Cloud readback results for this immutable commit are recorded afterward in `/workspace/.cloud-setup/stats-ndv-batch/final-handoff.json`; verify that receipt before treating publication as complete.

## Context and Orientation


`tidb-stats` owns histogram building, `tidb-executor/src/analyze.rs` consumes row collectors, and `tidb-exec/src/cluster_analyze.rs` supplies catalog metadata. `real_tikv_analyze.rs` owns both partition merge paths. `tidb-executor/src/load_stats.rs` converts canonical statistics to/from JSON. `tidb-exec/src/cluster_load_stats.rs` carries them into the durable statement writer in `cluster_stats_write.rs`. Both production stores share these owners.

## Milestones and Plan of Work


First extend the existing suites with underestimated unique NDV, JSON sketch round-trip and durable LOAD STATS sketch replacement cases. Capture their failures together. Then centralize Go's public, single-column, non-prefix, unconditional unique-index policy in statistics; feed exact non-NULL counts to the histogram owner. Thread current table metadata to both global merge routes so only local unique indexes use summed partition NDVs, bounded by current rows. Preserve FM sketches through column/index conversion and implement Go's separate delete/insert statements before histogram replacement. Retain absence semantics, existing transaction boundaries and ordinary cache loading behavior.

Finally validate all changed callers in a consolidated pass. This is maintenance of existing partial implementations, not acceptance of a complete upstream package. Existing structural finding counts must not imply that these focused repairs complete unrelated owners.

## Concrete Steps and Validation


All commands execute in Codex Cloud. Source `/workspace/.cloud-setup/env.sh` before Cargo, lint and server commands. From `/workspace/tidb/rust`, use `CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-executor -p tidb-exec --lib --no-fail-fast -- stats_ndv_batch --test-threads=1` for regressions, then the existing relevant ANALYZE/LOAD STATS suites. Use the existing `tidb-exec --test all` carrier for durable storage tests. Run all-target checks for statistics, executor, exec, session and server. From the repository root run `make lint`; commit through `hooks/pre-commit`; immediately before pushing rerun `cd rust && cargo build --locked -p tidb-server`. Verify the destination `pingcap/tidb:hparser-integration` SHA.

## Surprises & Discoveries


Go master moved uniqueness admission into statistics and made unique histogram NDV exact. Rust suppresses TopN but still uses the sketch estimate. The original running server also reports global index NDV zero for six unique rows with blocking merging; asynchronous merging is retained as a control. Index JSON conversion and LOAD STATS storage also drop sketches even though canonical carriers already support them. The filesystem has less than 1 GiB free; only proven inactive reproducible build outputs may be pruned, with receipts outside the checkout.

## Decision Log


Use existing test carriers and shared production paths, rather than introducing another harness. Keep Go's global-index exclusion: moved rows can remain in an old partition's statistics. Keep separate FM delete and insert operations because storage error/transaction ordering is observable. Date: 2026-10-06, Codex.

## Idempotence and Recovery


Retain the clean parent commit and external before/after logs in `/workspace/.cloud-setup/stats-ndv-batch`. Never reset concurrent work, purge dependency caches or force-push. Do not run the known unrelated failing DDL/parser suites merely to rediscover their failures. Save and read back Cloud configuration without claiming it has been published or tested in a restored instance.

## Outcomes & Retrospective


Implementation is complete. Final validation passes 220 distinct Rust cases and all 15 real MySQL/unistore TCP assertions. Two compile errors (a missing model type qualification and a test-only CiString constructor) were corrected; all diagnostic logs are retained. No performance or complete-package claim is made. The original blocking-merge index NDV zero and dropped column/index sketches are repaired. Global sketches are absent while physical partition sketches remain. Detailed commands, source hashes and log identities are in `rust/docs/parity/current-audit/statistics-ndv-batch-validation.json`.


## Integrated lock-policy follow-up


The rebuilt real server exposed an additional T01 consumer mismatch: the first LOAD STATS succeeds, but replacing an existing sketch aborts on its unique key. Go `pkg/table/tables/index.go::Create` sets `lazyCheck` only on a local miss, not a local tombstone (a recorded deletion). `lock_pessimistic_statement_with` previously selected flags from detached planned mutations, while final staging already used the shared MemDB correctly. Pass the same MutationBuffer into all six production callers and consult it before lock selection. The new unit regression fails before repair; it retains a fresh-key duplicate check and verifies that the existing deletion's AssertExist survives reinsertion. Keep the failed real-server result in `before-reinsert-wire.json`. Rerun the affected lock/writer units, exec/server all-target checks, lint and real-server probe before publication. T01 remains partial for its other index-transition obligations.

Exact final validation commands (activate the Cloud environment first):

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-exec --lib -- cluster_table_storage::tests:: system_row_write::tests:: --test-threads=1

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-executor -p tidb-exec --lib --no-fail-fast -- stats_ndv_batch cluster_analyze:: cluster_load_stats:: cluster_stats_dump:: real_tikv_analyze::tests:: load_stats:: analyze:: --test-threads=1

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-stats -p tidb-exec --test all --no-fail-fast -- stats_ndv_batch builder_source:: index_source:: global_stats_source:: pkg_statistics_go_tests_source:: analyze_commit_size_source:: analyze_generated_column_source:: analyze_added_column_source:: --test-threads=1

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib -- tests_analyze:: --test-threads=1

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-stats -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets

    cd /workspace/tidb && make lint

    cd /workspace/tidb/rust && CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server

    PYTHONPATH=/workspace/.cloud-setup/python python3 /workspace/.cloud-setup/stats-ndv-batch/wire.py after

