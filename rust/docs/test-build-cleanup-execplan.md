# Retire unused execution-context and RU wrappers

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the disconnected statement-context lifecycle and RU EXPLAIN wrapper,
and call native detail merging directly. Work in /workspace/tidb on
hparser-integration from d2c2d406864c55820be4da8886c68845db41dbe0.
Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
No live session constructs the removed wrappers. Current Go separately owns
RURuntimeStats and ExplainRURuntimeStats; the retired Rust combined wrapper
was neither a live consumer nor evidence of completed integration.

## Progress


- [x] Trace every removed type and context helper across non-cache Rust sources.
- [x] Remove disconnected context/RU code and RU-private tests and fixtures.
- [x] Migrate merge consumers to native methods and retain snapshot assertions.
- [x] Validate runtime, execution-detail and canonical RU tests; lint and review.
- [ ] Pass hook and fresh pre-push builds; verify remote and Cloud checkpoint.

## Milestones and Plan of Work


In tidb-exec/src/exec_details.rs retire StmtExecDetails and its private context
keys, context initialization/inheritance/sync accessors and unconsumed snapshot
wrapper. Keep the existing atomic-field regression, calling the native
ExecDetails.snapshot directly; remove only the obsolete wrapper's None case.
In runtime_stats.rs remove RuRuntimeStats, its version/kind constants, private
fixtures and ten tests. Preserve basic/root/cop/commit collectors and tests.
Replace merge_commit_details and merge_lock_keys_details forwarding functions
with equivalent native .merge calls at every consumer. Correct module docs
and mark the original execdetails audit receipt as historical ownership.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-exec -p tidb-util --lib -- runtime_stats::tests:: exec_details::tests:: tiflash_stats::tests:: ruv2_metrics::tests:: --test-threads=1

Expect retained collector, commit/lock merge, TiFlash, canonical RU and atomic
snapshot cases to pass. Verify all snapshot field assertions remain and every
merge substitution calls the same native implementation as the old wrapper.
Search for retired symbols including imports/aliases. From repository root
run make lint and git diff --check. Normal commit must run the actual hook's
cd rust && cargo build --locked -p tidb-server; repeat immediately before push
and verify remote SHA. No Go/Bazel/dependency changes are needed.

## Surprises & Discoveries


The context lifecycle had no production caller. RU version selection mixed
roles which current Go assigns to distinct wrappers. Removing it does not
implement the missing live context or EXPLAIN integration. The snapshot test
covers canonical native fields and remains useful without its forwarding API.

## Decision Log


Preserve useful native correctness tests and live collectors. Remove only
unconsumed context/RU ownership and forwarding code. Keep commit/concurrency
collector extensions and their root-stat contract tests intact. Do not claim
whole Go package acceptance or close broad parity findings.

## Outcomes & Retrospective


Implementation and validation complete: 22 retained-owner tests passed,
along with lint and continuity/diff checks. Removed 545 net Rust lines and
ten private tests. Publication gates remain pending here; external
final-handoff.json records their later results. No full Go suite,
live cluster run, SQL replay or measured speedup is claimed.

## Recovery, Artifacts and Dependencies


Before-images: git show d2c2d40686:<path>. Restore individual files only and
preserve concurrent work. Logs and before-inventory live under
/workspace/.cloud-setup/exec-details-cleanup. Durable receipt:
rust/docs/parity/current-audit/exec-details-cleanup-validation.json.
Native sources and dependencies remain unchanged. Draft save, Publish and
future restore are separate. Revision: replace completed reader/resolver
cleanup with execution-detail ownership cleanup.
