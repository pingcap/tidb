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
