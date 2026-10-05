# Retire disconnected session and executor helper copies

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed by parity/current-audit/README.md; Git retains the deleted artifacts.

## Purpose / Big Picture


Remove eleven unused tidb-exec helper modules and their ten private harnesses,
including internal utility tests, as one batch. Live session state, retry IDs,
authentication, variable dispatch and SQL tests remain unchanged. Success is
fewer compiled inputs with retained behavior intact, not a measured speedup.

## Context and Orientation


Cloud base 532d64ada38a378dbbc8c26d398dced23da1f931 on hparser-integration.
Refreshed Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed and unchanged native
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. Remove effective_auth_plugin,
mock_global_accessor, plan_cache_params, privilege_set, removed_sysvar,
retry_info, sequence_state, session_metrics, status_registry, system_db_filter
and executor_utils from tidb-exec/src. Full Rust reference tracing finds no
production consumer. LASTVAL's real Session SQL test only cites Go's filename;
it does not use the retired sequence map and must remain byte-identical.

## Progress


- [x] Refresh refs, trace consumers and identify existing live session owners.
- [x] Delete eleven helper copies, ten harnesses and the stale metrics audit plan.
- [x] Verify retained inputs, fresh aggregate registration, grouped all-target checks and lint.
- [x] Self-review and unchanged remote base; required hook/build/push results are recorded after commit in the external final handoff.

## Milestones and Plan of Work


Delete the modules and root declarations together. Remove private harnesses and
internal tests with their dead owners. Retire the metrics-only audit plan and
correct stale Rust coverage claims in metrics, variable and TLS receipts.
Keep original Go tests, source inventories and live Rust owners unchanged.
Record hashes, removed test names and exact checks in
parity/current-audit/session-helper-cleanup-validation.json.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets

Run root make lint and git diff --check. Verify all retained .rs/.toml/.sh inputs
are byte-identical except tidb-exec/src/lib.rs module declarations. Fresh
aggregate registration must exclude all ten deleted harnesses. No runtime test
rerun is required for unreachable deletion with unchanged executable consumers.
Normal commit must run actual hooks/pre-commit including cd rust && cargo build
--locked -p tidb-server. Repeat that locked build immediately before every push;
verify remote SHA. Preserve concurrent edits and never bypass hooks or force-push.

## Surprises & Discoveries


executor_utils duplicates string-set/auth/value helpers and owns a private worker
pool tested only inside its own module. The session_metrics module contains only
three strings and no counters. Real retry IDs belong to tidb-executor::RetryAutoIds;
Session owns live sequence values. Filename matches in real SQL tests are source
citations rather than calls into the dead models.

## Decision Log


Retire disconnected copies and their private tests together; preserve real SQL,
owner and Rust correctness tests. Correct claims of live provider/metric behavior
without claiming those full Go packages are now complete. No permanent script,
new harness or dependency edit. Date: 2026-10-05 UTC.

## Outcomes & Retrospective


External evidence and before-images: /workspace/.cloud-setup/session-helper-cleanup.
The final-handoff.json records post-commit hook/build/push/recovery/configuration
results. Recover selected files with git show 532d64ada3:<path> into temporary
files before restoration. Counts remain 86 tracked / 30 repaired / 56 unresolved.
No full Go/live TiKV suite, whole-package acceptance or benchmark is claimed.

Validation result: 30 private tests and 2,446 source/test lines removed; 3,573
retained inputs are byte-identical. Fresh aggregate registration excludes all
ten retired harnesses. Grouped all-target checking, make lint and diff checks pass.
