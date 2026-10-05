# Retire disconnected analyze models and compatibility exports

This living ExecPlan follows root PLANS.md. Git preserves removed files;
historical receipts remain indexed under parity/current-audit.

## Purpose / Big Picture


Remove unused analyze result/identity models and a legacy statistics collector,
plus an obsolete panic-recovery re-export. Keep current row sampling, histograms,
auto-analyze policy, persistence and the shared panic owner intact. Observe
absent private registrations and unchanged retained test bodies, then validate
the grouped change. Reduced inputs do not establish measured speedup.

## Context and Orientation


Cloud base d8f414241052b47cc5f98652a211e0a14bb5e828 on hparser-integration.
Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed and native master
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 remain unchanged after refresh.
Remove tidb-stats analyze_results, analyze_table_id, sample_collector and their
three private harnesses. Public type/function tracing shows no live callers.
Go AnalyzeResults carries actual sampled results into persistence and owns their
cleanup; Rust's generic histogram-probe model never carried those results.
Go SampleBuilder owns a legacy collection path; the Rust LegacySampleBuilder
is used only by private tests. Current RowSampleCollector and independent-index
analysis remain live. Remove tidb-exec analyze_panic_error's re-export and point
its two retained tests directly at tidb-executor::analyze::panic_recovery.

## Progress


- [x] Refresh refs and trace all public identifiers, including root re-exports.
- [x] Remove three models/three harnesses and obsolete export; preserve eight useful tests.
- [x] Verify retained bytes and registration; run grouped all-target check and lint.
- [ ] Commit through actual hook, fresh pre-push build, remote verification and setup save.

## Milestones and Plan of Work


Retire the three statistics modules and exports together. In the mixed
pkg_statistics_go_tests_source harness, delete only the final TestSampleSerial
section (three private legacy tests and their fixture/imports), keeping the six
row-sampling/histogram/version tests unchanged. Delete the panic re-export only
after moving its two test callers directly to the same implementation. Verify
all retained source/test hashes and fresh registrations. Record exact results
in the cleanup receipt and latest links without changing finding dispositions.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in every build shell. From rust/:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-stats -p tidb-exec -p tidb-server --all-targets

Run root make lint and git diff --check. Fresh statistics registration must omit
the three retired harnesses and retain pkg_statistics_go_tests_source. The exec
aggregate must retain analyze_panic_error_source. Six mixed test bodies and two
panic test bodies must match the baseline. No broad runtime sweep for unused
model deletion and an import redirected to the identical owner. Normal commit
must run the actual hook's cd rust && cargo build --locked -p tidb-server;
repeat immediately before push, verify remote SHA, preserve concurrent changes
and never bypass hooks or force push.

## Surprises & Discoveries


Namespace-only scans missed live root exports: analysis_policy, analyze-version
policy, DatumMapCache and SortedHistogramBuilder all remain in use and are kept.
The mixed statistics harness has six real-owner tests beside three legacy-model
tests; deleting the whole file would discard useful coverage.

## Decision Log


Retire the unused analyze model chain and compatibility namespace as one batch.
Keep original Go sampling/lifecycle obligations, including TestSampleSerial;
private model removal does not discharge them. No new dependency or harness.
Date: 2026-10-05 America/Los_Angeles.

## Outcomes & Retrospective


Removed 1650 source/test lines and 29 private tests; two panic tests now import
the same owner directly. Evidence and before-images reside outside the checkout
at /workspace/.cloud-setup/analyze-model-cleanup. Recover individual files with
git show d8f4142410:<path> into temporary files for review. External
final-handoff.json records post-commit gates, remote SHA and setup persistence.
Counts remain 86 tracked / 30 repaired / 56 unresolved. No full Go/live TiKV
suite, benchmark or complete package acceptance is claimed.

Grouped all-target checking, root lint and diff checks pass. Fresh aggregates
omit the three retired statistics harnesses while retaining the mixed statistics
and panic tests. All 3530 other source/manifest/script inputs are byte-identical.
