# Remove duplicate behavioral tests

This living ExecPlan follows root PLANS.md.

## Purpose and outcome

Remove repeated Rust test implementations while retaining one executable owner for each Go behavior. This reduces test source and repeated execution without changing production semantics, deleting unique cases, or accepting any incomplete Go package. Work in /workspace/tidb on hparser-integration; baseline c98418ea90ed2ef495188a3a99147430331aa549. Refreshed Go master remains 3ca96b1d5df8da123e7a650512654eedab12c861.

## Milestones and validation

First compare duplicate bodies and their helpers in the lexer, model, transaction, planner, expression, executor and session crates. Record removed-to-retained mappings in the cleanup receipt. Then delete redundant functions, empty modules and registrations together. Preserve distinct cases and process isolation. Retire the completed planner cost plan whose commands and test path are obsolete, retaining its immutable Git archive and original acceptance limits.

Run the retained tests at their current owners with locked Cargo and one build job, grouped by crate and existing target. Source /workspace/.cloud-setup/env.sh before every command; run Cargo from /workspace/tidb/rust. Run make lint from the repository root and git diff --check. Review the final diff. The actual pre-commit hook must pass cargo build --locked -p tidb-server; repeat that build immediately before an authorized normal push and verify the remote SHA. No Go/Bazel or dependency declarations change, so bazel_prepare is not required.

## Progress

- [x] Refresh Go, confirm clean baseline and identify duplicate bodies.
- [x] Remove reviewed copies and migrate stale references.
- [x] Validate retained behavior and review the diff.
- [x] Prepare publication with the actual hook and fresh locked-build gates; final remote and Cloud results are recorded in the external final-handoff.json.

## Surprises & Discoveries

The grouped library run exposed a pre-existing EXP test expecting FloatOverflow. Current Go TestExp requires the detailed DOUBLE overflow for exp(100000), and the unchanged Rust implementation already produces DataOutOfRange with those fields. Preserve the case and replace the stale variant assertion with the exact structured error; rerun the affected expression group after that edit. The initial log retains the failure. The integration link later exhausted disk and exited with linker SIGBUS; five obsolete unit/aggregate executables were retired after hash, single-link, process and readable mapping checks, reclaiming 746051120 bytes. Dependency caches, libraries and current successful executables remain. Retry the interrupted integration group after cleanup.

Some exact copies are attached to different test-port batches, so their names exaggerate distinct coverage. The planner factor suites differ slightly: preserve the unknown-name vector in the retained primitive suite before retiring its duplicate. Tests with different feature gates or shared-state assumptions are not interchangeable merely because their bodies match.

## Decision Log

Retain Go-derived coverage and Rust lifetime/concurrency checks. Remove only reviewed duplication in this batch; absence of an identically named Go test does not establish uselessness. Preserve historical receipts as historical evidence, and update current references without rewriting prior results. Recovery uses the baseline commit; never reset concurrent work or force-push.

## Outcomes & Retrospective

Removed 19 duplicates, four test files, one empty module and unused fixture code; migrated the distinct cost-factor vector and corrected the stale EXP assertion. All 158 distinct selected tests and lint pass. Workspace-wide formatting drift remains outside scope; changed assertion formatting and diff checks pass. Detailed mappings, commands and limits are recorded in parity/current-audit/duplicate-test-cleanup-validation.json; external logs belong in /workspace/.cloud-setup/duplicate-test-cleanup. Finding dispositions remain unchanged.

## Continuation: consolidate lexer and information-test owners

Baseline 7b1c2d0b19752441589009054414b24ab7c4bddc; freshly fetched Go master is unchanged. Retire lexer_parser_privileges_source.rs and field_type_terror_source.rs after merging scanner assertions into lexer_source.rs, escape vectors into escape.rs, and feature combinations into features.rs. Consolidate the scalar information fixture into builtin_info_json_math_source.rs. Preserve parser-owned obligations and all unique vectors; historical receipts continue to describe their original commit.

- [x] Compare keyword arrays: retained 190/188 rows contain all retired 189/185 rows, including the four additional retained spellings.
- [x] Merge raw/decoded string and integer assertions, ANSI quoting, unique lexical contracts, all 15 escape vectors and feature combinations; remove 12 repeated test registrations and two files.
- [x] Validate 71 lexer tests and 60 information/JSON/math tests (131 distinct passes); lint, changed formatting, source preservation and deletion review pass.
- [x] Prepare publication through the actual locked-build hook and fresh prepush gate; final commit, remote and Cloud checkpoint results are recorded externally in final-handoff.json.

This batch changes test ownership only. It does not close any production finding or establish complete upstream package acceptance. Commands, mappings, logs and limits belong in parity/current-audit/lexer-info-cleanup-validation.json and /workspace/.cloud-setup/lexer-info-cleanup.

## Continuation: table-key owners and partition placeholders


Baseline 84e786d24085d2589c76d10d47f3f4a65a416de3; Go master 3ca96b1d5df8da123e7a650512654eedab12c861. Consolidate the two codec table-key suites into tidb-tablecodec's existing package suite, preserving unique raw-handle, malformed-key, nonunique-index, range and metadata vectors. Remove twelve ignored partition placeholders and the duplicate global-index constant test. Retire go-divergence-plan.md; the current structural plan and audit own sequencing. Historical Git contents preserve recovery without resetting concurrent work. Missing Go obligations remain explicitly unverified in the cleanup receipt; no finding is repaired by deleting a placeholder.

- [x] Review and apply the complete cleanup batch; preserve distinct byte/error vectors.
- [x] Validate both codec suites (208 passes) and the four retained partition modules (10 passes) in grouped test runs; make lint and diff checks pass.
- [x] Record mappings and results and self-review. Publication must execute the actual locked-build hook and fresh locked prepush build; final commit/remote results are recorded externally in /workspace/.cloud-setup/test-owner-cleanup/final-handoff.json.

From rust/, run cargo test --locked -p tidb-codec -p tidb-tablecodec --test all -- --test-threads=1 and cargo test --locked -p tidb-executor --test all -- partition_exchange_global_index_source:: partition_modify_column_allowlist_source:: partition_pk_global_index_source:: partition_truncate_issue57780_source:: --test-threads=1. Expected: retained tests pass; removed placeholders cannot imply new coverage. No production implementation or finding disposition changes.

Outcome: three test files, twelve ignored placeholders, fourteen net running registrations, 537 net Rust lines and the 251-line stale plan retired. All 218 selected tests and lint pass. Production behavior, finding dispositions and unverified Go obligations remain unchanged.

## Continuation: benchmark and retired-gap cleanup


Baseline 3a0580dd045bee6ea47ea5908090ad141478006b; fetched Go master remains 3ca96b1d5df8da123e7a650512654eedab12c861. Remove five wrappers that only invoke an already registered test, eight fixed-loop benchmark tests and a duplicate collation flag marker. Migrate distinct tablecodec vectors into their current owner and make the maintained benchmark call tidb_txnkv::Key::prefix_next. Retire stale comments referring to deleted ignored tests and the superseded unit-test-infrastructure plan; preserve historical evidence through the baseline archive and current audit. No production code or finding disposition changes.

- [x] Review the shared Go owners, preserve distinct inputs, remove duplicate registrations/helpers and stale guidance together.
- [x] Original private prefix helper fails the Go overflow vector (exit 101); all 73 retained tests pass, including shared-key overflow. Nine dev-profile benchmark cases, make lint, benchmark formatting and diff checks pass.
- [x] Record mappings/limits and prepare publication through the actual hook and fresh locked server build. Final commit/remote results belong in /workspace/.cloud-setup/retired-gap-cleanup/final-handoff.json.

Run Cargo from rust/ after sourcing the Cloud environment, with one build job and --locked. Use existing --test all owners for parser auth and codec, and --lib filters for expression string cases and txnkv key tests. The benchmark command is cargo bench --locked -p tidb-tablecodec --bench tablecodec --profile dev. Preserve actual benchmarks and Rust-specific correctness checks. Missing vectorized/performance/Go package obligations stay unverified; one correctness invocation never discharges a benchmark. Recovery uses the baseline commit, without resetting concurrent work. Exact logs and publication results belong in /workspace/.cloud-setup/retired-gap-cleanup.

Outcome: 14 redundant registrations and two private prefix helpers removed; unique vectors preserved, 19 stale comment blocks corrected, and the 566-line superseded plan retired. Surviving executable lines in all 16 comment-only files and both wrapper owners are verified unchanged after removing the five pure wrappers. All selected validation passes; no production finding or Go package accepted.

## Continuation: unused DDL leaf implementations


Baseline 40e6598856a1290c01c0f44eec7481f4e2f7a5a6. Retire tidb-exec/src/storage_class.rs and tidb-executor/src/ddl/mview_helpers.rs plus their module declarations and twenty private tests. Every exported function has no reference outside its own file in tracked Rust sources/manifests; neither module registers initializers or callbacks. These are disconnected models, not the live storage-option, MV admission, metadata, job or reorg owners. Preserve pinned Go obligations and all old vectors through immutable baseline archives in unused-carrier-cleanup-validation.json. No missing Go behavior is repaired by retirement. The live MV build seed has persisted-job callers and remains explicitly unaccepted under D09.

Retire obsolete-tooling-removal-execplan.md: its completed task and original evidence remain in obsolete-tooling-removal-validation.json; its no-push, old-ref and obsolete workspace instructions must not guide current work. Keep all other DDL/session/model source, tests and fixtures unchanged.

- [x] Review package documentation, all external symbol references, live dispatch and the removal boundaries.
- [x] Remove the two leaves and stale plan; all 41 selected retained tests, affected all-target checking, lint and diff review pass.
- [x] Record exact results and prepare publication through the actual hook and fresh locked server build. Final commit, remote SHA and saved Cloud checkpoint results are recorded externally in /workspace/.cloud-setup/unused-carrier-cleanup/final-handoff.json.

Activate env.sh, use one build job, and run Cargo from rust/. Validate the retained cluster_ddl_source::materialized_view cases, executor ddl::mview_schedule_expr cases and model engine_attribute/materialized_view cases. Run cargo check --locked --all-targets -p tidb-exec -p tidb-executor -p tidb-server. Source/body comparison must prove all other tracked executable files unchanged. No Go/Bazel/dependency changes require regeneration. Recover individual retired paths from the baseline archive if a future complete Go implementation needs them; never reset concurrent work.

Outcome: 2,157 lines in two unused implementations and their twenty private tests are retired, plus the two module declarations and a 55-line completed plan. All 41 selected retained tests, all-target checking, lint and source comparison pass. The ignored_table_option filter matched no case; it does not establish storage-option coverage. Existing MV build seeds have callers and remain in place with D09 unaccepted. The old vectors and 25 current Go storage-class test obligations remain documented in the retirement receipt. No SQL behavior, finding disposition or complete Go package acceptance changes.
