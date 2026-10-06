# Retire disconnected slow-log models

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the unused slow-log model chain and its private checks as one batch.
Keep the actual session statement-summary policy and canonical metric owners.
Work in /workspace/tidb on hparser-integration from
47fecadae45f28d9d7491195192fbaac901f48fb. Fresh Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Go slow-log rules operate on live
SessionVars; the removed Rust parser, formatter and boolean models have no
runtime callers. Live threshold-based server recording remains untouched.

## Progress


- [x] Trace every exported symbol and compare actual Go owners and live callers.
- [x] Retire six unused slow-log modules, three carriers and unused adapter branches.
- [x] Preserve summary policy and seven tests; move the canonical RU formatting test.
- [x] Remove two obsolete audit plans and mark dated receipts as historical.
- [x] Validate retained summary/exec-detail/RU owners, source continuity and lint.
- [ ] Pass actual hook, fresh pre-push build, remote verification and Cloud checkpoint.

## Milestones and Plan of Work


Delete tidb-exec/src/slow_log_{float,format,match,parse,rules,threshold}.rs
and the match/rules/threshold integration carriers and registrations. Remove
TaskTimeStats.render from exec_details.rs after tracing its only consumer to
the retired formatter; retain all metric snapshots and structured logging.
Trim adapter.rs to SummaryStmtKind, SummaryGate, SummaryAction, is_internal_sql,
decide_summary_stmt and their seven unchanged tests. Session observation.rs
continues to consume that policy without source changes.

Move format_ruv2_summary_arm_coverage intact from the unused formatter to
its existing tidb-util/src/ruv2_metrics.rs owner. Retain all Go obligations;
removing detached models does not implement rule validation or publication.
Remove the two completed operations slow-log audit plans, repair their link,
and mark their original testport receipts as historical ownership evidence.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-exec -p tidb-util --lib -- adapter::tests:: exec_details::tests:: ruv2_metrics::tests:: --test-threads=1

The seven retained summary cases must keep internal-user classification,
PREPARE exclusion, disabled-summary predecessor clearing and COMMIT attribution.
Execution detail snapshots and canonical RU formatting must pass. Compare
retained policy, tests and consumer files against before-images; no live
behavior may change. From repository root run make lint and git diff --check.
The real hook must execute cd rust && cargo build --locked -p tidb-server;
repeat that build immediately before normal push and verify the remote SHA.

## Surprises & Discoveries


The formatter copied CPU and keyspace snapshots despite existing canonical
owners, but no live writer used it. Its RU helper test does exercise an actual
shared implementation, so that test moves instead of being deleted. The
TaskTimeStats text renderer is only called by the retired formatter; its
structured logging and aggregation remain live and are retained.

## Decision Log


Keep source-backed tests for live owners. Delete checks that only demonstrate
unused model behavior, plus a constant-concatenation test. Preserve historical
upstream inventories and explicitly revoke their executable-owner descriptions.
This cleanup makes no whole-package acceptance or broad finding-repair claim.

## Outcomes & Retrospective


Implementation and validation complete: 20 retained-owner tests passed,
including the seven summary tests and migrated RU formatting test. Lint and
continuity/diff checks passed. Removed 3348 net Rust lines and 36 private tests.
Publication gates remain pending here; external final-handoff.json records
their later results. No measured
speedup, full Go suite, SQL replay or live cluster result is claimed.

## Recovery, Artifacts and Dependencies


Use git show 47fecadae4:<path> for before-images. Restore individual files
only; preserve concurrent work. Inventory and logs live in
/workspace/.cloud-setup/slow-log-model-cleanup. Durable evidence belongs in
rust/docs/parity/current-audit/slow-log-model-cleanup-validation.json.
Native client and dependencies remain unchanged. Cloud draft save, Publish
and future restore are separate. Revision: replace the completed session
policy cleanup plan with the connected slow-log retirement batch.
