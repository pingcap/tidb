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

## Canonical variance and runner cleanup continuation

This continuation starts at local e66d8d43dd1b45d059abda6f35469d7c1c48fa33. Remove four standalone variance facades, the VarPopState wrapper and three exported finalizer adapters. Their only consumers were nine adapter tests. The existing variance_live_aggregate_source suite already covers their Go vectors, NULL/sample thresholds, partial merges and tuple size through the canonical runtime, with DISTINCT, SQL dispatch and metadata coverage as well. Preserve ordinary reset in that existing suite. Retire adapter tests, including fabricated negative intermediate states that cannot be supplied through the live state API. Go references remain pkg/executor/aggfuncs/func_{varpop,varsamp,stddevpop,stddevsamp}{,_test}.go at master 93a01d31f6da205ae4bf376825293903a6899fdb. This changes workspace API layout without changing SQL behavior or claiming a package complete.

Retire test-access-path-readiness.sh, test-access-path-stats-counts.sh and test-convergence-global-grant.sh: these scrape shell snippets and feed artificial timing or SQL output. Keep the actual shared readiness/statistics helpers and live runners. Keep cleanup-path safety checks, authentication wire test and protobuf synchronization tests, which cover Rust tooling obligations.

Milestones are caller/coverage migration and removal, followed by one combined live variance/core aggregate selection, affected all-target checking, make lint and the actual locked-build commit hook. Run from rust/: cargo test --locked -p tidb-exec --test all -- variance_live_aggregate_source core_aggregate_runtime_source --test-threads=1; cargo check --locked -p tidb-exec --all-targets. Run bash -n for retained runners/helpers and execute both cleanup-path safety scripts from repository root. Keep logs and hashes in /workspace/.cloud-setup/variance-cleanup. Git retains retired code for recovery; do not reset concurrent work. No pushes.

### Progress

- [x] Remove four facades, their adapters and nine redundant cases; migrate ordinary reset to existing live coverage and retire three shell self-tests.
- [x] Final retained selection: 14 pass, zero failed/ignored; all-target checking, lint, formatting, safety checks and self-review pass. Normal commit hook and recovery/draft outcomes are recorded in /workspace/.cloud-setup/variance-cleanup/final-handoff.json.

### Surprises, decisions and outcomes

The initial edit moved nine cases beside the private finalizer, and that intermediate unit run passed. A broader comparison with the existing live variance suite showed those cases duplicated its Go obligations. Delete the intermediate migration and validate the retained integration owner instead. The final receipt must distinguish this intermediate nine-pass result from final retained coverage. Canonical production implementation stays byte-identical before the removed compatibility suffix. No extra binary retirement or measured speedup is claimed in this continuation.

Final outcome: four facade source files, one compatibility state and three finalizer APIs, nine duplicate tests and three shell snippet checks are removed. The six existing variance cases and eight core aggregate cases pass. No finding is closed; 86 tracked, 29 repaired and 57 unresolved remain. No measured speedup, full suite, live-cluster acceptance or push is claimed.

## Retire unused window models


Start at 1d8a6f02e13db9a8e89e27535bc4ca5b07e946d2. Remove tidb-exec's cume_dist, ntile, lead_lag, window_value_int, window and window/ranking_runtime modules after confirming all consumers are the four leaf-test suites. The live owner is tidb-executor/src/window.rs, whose per-function comparison timing follows Go; the private geometry's claim to be the live runtime is stale. Preserve CUME_DIST peer/empty vectors, NTILE bucket vectors, LEAD/LAG offsets/defaults/wrap and integer value/NULL selection by grouping them into the existing window_executor_source suite against the actual chunk executor. Retire memory assertions about unused model layouts; complete Go memory/accounting obligations remain unverified.

### Progress


- [x] Remove six unused source modules, their four test files and stale crate documentation; migrate 43 meaningful vectors into one live-suite case.
- [x] All 14 window tests pass (43 migrated vectors, eight executions each); all-target checking, lint, formatting and self-review pass. Final normal-hook, bundle and draft outcomes are in /workspace/.cloud-setup/window-leaf-cleanup/final-handoff.json.

### Plan, validation and recovery


Go source/reference: pkg/executor/aggfuncs/func_{cume_dist,ntile,lead_lag,value}.go and their tests at master 93a01d31f6da205ae4bf376825293903a6899fdb. First migrate vectors with existing ChunkedSource and WindowExec; exercise ordinary/pipelined modes, chunk boundaries and reopen. Delete the unreferenced models and declarations together. From rust/ run cargo test --locked -p tidb-executor --test all -- window_executor_source --test-threads=1 and cargo check --locked -p tidb-exec -p tidb-executor --all-targets. Then make lint, formatting/diff review, and a normal commit running cd rust && cargo build --locked -p tidb-server. Source /workspace/.cloud-setup/env.sh; use one build job. Logs, deletion hashes and final handoff belong in /workspace/.cloud-setup/window-leaf-cleanup. Git preserves deleted files. Do not push or change native sources. This removes unused APIs, not a live SQL implementation; no full package, finding closure or measured speedup is claimed.

### Surprises & Discoveries / Decision Log


The old ranking geometry describes a shared WindowPartitionRuntime that no longer exists there. The actual executor owns independent ranking cursors and Go comparison timing. Remove the stale copy rather than connecting it to the live path. Memory tests comparing an unused model's size to itself cannot establish Go memory parity; keep that obligation open. Date: 2026-10-04.

### Outcomes & Retrospective


Six source modules and four model suites are removed. Live production window code and all 13 existing cases are byte-identical; one grouped live case carries the useful vectors. Initial helper compilation required a constructor closure because WindowFunction is not Clone/Debug; production types stay unchanged. No known failing regression is removed, no finding is closed, and no full-suite or performance claim is made.
