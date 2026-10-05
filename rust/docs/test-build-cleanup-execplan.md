# Retire disconnected DDL and restore models

This living ExecPlan follows root PLANS.md. Historical receipts remain indexed
by parity/current-audit/README.md; Git preserves retired source and tests.

## Purpose / Big Picture


Remove five unused metadata/restore implementations and three private harnesses.
Keep actual durable DDL, placement delivery, table/catalog and MDL owners intact.
Trim only stale comments from the mixed kill/cancellation harness, preserving
its four real tests. Fewer compiled inputs do not establish measured speedup.

## Context and Orientation


Cloud base 1304987c5b34ca8ed87328acf57605fd6e90561e on hparser-integration includes
concurrent MDL commits 568b317459 and 1304987c5b, safely fast-forwarded from
7926aafbbe. All five changed files must remain byte-identical. Refreshed Go master
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed; native cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1
is unchanged. Retire tidb-exec ddl_job_comments, ddl_job_merge, placement_labels,
schematracker_info_store and tidb-executor tiflash_recorder. Full Rust reference
tracing finds only module exports, private tests and stale documentation.

## Progress


- [x] Refresh refs, preserve concurrent MDL changes and inspect Go/Rust ownership.
- [x] Delete five models and three harnesses, retain mixed-harness real tests, correct stale claims.
- [x] Verify retained/concurrent inputs, fresh registration, grouped checks and two incoming MDL tests.
- [ ] Self-review and actual hook/build/push gates with verified remote SHA.

## Milestones and Plan of Work


Remove source files and root declarations as a group. Remove their three private
harnesses and internal tests. The Go job submitter, schema tracker and BR recorder
have actual ownership/lifecycle consumers absent from these Rust copies; do not
claim their complete packages or original tests are satisfied by private models.
Keep Go source/tests unchanged. Correct b105/b110, schematracker and mathutil
receipts; record removed hashes and retained evidence in the current audit receipt.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From rust/ run the grouped check:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server --all-targets

Validate the incoming concurrent change once, without a broad runtime sweep:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib tests_mdl_related:: -- --test-threads=1

Run root make lint and git diff --check. Verify hashes of all retained Rust
source/manifests/scripts except the two module declaration files and comment-only
mixed harness. Its four test bodies and all five incoming MDL files must remain
unchanged. Check fresh aggregate registration. Normal commit must execute actual
hooks/pre-commit and cd rust && cargo build --locked -p tidb-server. Repeat the
same locked build immediately before each push; verify remote SHA. Never force
push, bypass hooks, or overwrite concurrent changes.

## Surprises & Discoveries


The executor killflag harness only mentions ddl_job_merge in stale comments;
its four actual tests call retained DDL/killer owners. TiFlashRecorder has no
restore consumer despite its complete-package comment. Retiring these copies
does not complete durable DDL or BR restore integration.

## Decision Log


Delete disconnected ownership groups together; preserve useful real-owner tests
and source package obligations. No new harness/script/dependency. Run the two
new MDL tests because upstream changes joined the Cloud baseline in this batch.
Date: 2026-10-05 UTC.

## Outcomes & Retrospective


External evidence and before-images: /workspace/.cloud-setup/ddl-model-cleanup.
Final-handoff.json records post-commit gates, remote verification, recovery bundle
and setup draft. Restore selected files through git show 1304987c5b:<path> into
a temporary file before review. Counts remain 86 tracked / 30 repaired / 56
unresolved. No full Go/live TiKV/BR suite, benchmark or package acceptance claim.

Grouped all-target checking and root lint pass. Both incoming MDL regressions pass
(2 passed, 0 failed/ignored). Fresh aggregates omit the three retired harnesses
and retain the real cancellation harness. Post-commit publication results are
recorded outside the checkout, avoiding a self-referential evidence commit.
