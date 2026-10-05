# Retire disconnected session and statement models

This living ExecPlan follows root PLANS.md. Historical receipts remain in
parity/current-audit; Git preserves removed source and tests.

## Purpose / Big Picture


Remove nine unused models and their nine private harnesses, leaving actual
session/transaction state and Domain slow-query ownership intact. Observe the
result through absent registrations, unchanged live source/test hashes and
successful grouped compilation. Reduced inputs do not prove measured speedup.

## Context and Orientation


Cloud base b843662dc1655b4e695a01a161a7b656a7ca0d7b on hparser-integration.
Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed and native master
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 remain unchanged after refresh.
Under rust/crates/tidb-exec remove stale_tso, stmt_cache, session_status,
topn_slow_query, txn_read_ts, isolation_state, noop_read_only, sysvar_scope,
sysvar_type and their tests/*_source.rs harnesses. These private models have
no production consumers. ScopeFlag is consumed only by the retired noop model.
Go keeps these states with StatementContext, SessionVars and Domain. Rust live
owners remain in tidb-session::vars/sysvar, server wire status and tidb-domain.

## Progress


- [x] Refresh refs and trace all nine namespaces and their real Go/Rust owners.
- [x] Remove models/harnesses and correct stale ownership documentation.
- [x] Verify retained executable bytes, aggregate registration and grouped gates.
- [ ] Commit through actual hook, run fresh pre-push build, verify remote and save setup.

## Milestones and Plan of Work


First retire all nine ownership groups together, including their declarations.
Correct comments in session varsutil and vardef and stale b012/isolation receipts
and architecture claims. Preserve Go files and all live tests, including Domain
heap/expiry/queue cases. Next verify executable bytes of every retained Rust
input (only two documentation headers and lib declarations change), and run the
single grouped check below plus root lint. Finally record real outcomes in the
current audit receipt and both registers without changing finding dispositions.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in each shell. From rust/:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-session -p tidb-vardef -p tidb-server --all-targets

From root run make lint and git diff --check. Verify freshly generated tidb-exec
all_tests.rs excludes all nine retired harnesses. No broad runtime sweep is
needed for unreachable deletion with retained executable code byte-identical.
Normal git commit must execute hooks/pre-commit and its locked server build.
Immediately before push rerun cd rust && cargo build --locked -p tidb-server.
Never bypass hooks or force push; preserve concurrent commits and verify remote
SHA. Do not claim Go package acceptance or close findings from deletion alone.

## Surprises & Discoveries


The duplicate tidb-exec slow-query store claimed completeness while excluding
the Domain lifecycle. The real Domain store already owns the original heap,
expiry and queue tests. The isolation architecture document incorrectly claimed
the disconnected model was reached from SET. Namespace matches in a method name
or documentation are not executable consumers.

## Decision Log


Retire private models as one batch, retaining all real owners and useful tests.
Original Go obligations remain open where the live owner is incomplete. No new
harness, dependency or build script is introduced. Date: 2026-10-05 America/Los_Angeles.

## Outcomes & Retrospective


Removed 2289 source/test lines and 27 private tests. Counts remain 86 tracked,
30 repaired, 56 unresolved. Evidence and before-images live outside the checkout
at /workspace/.cloud-setup/session-model-cleanup. Restore individual files from
git show b843662dc1:<path> into a temporary file for review before reuse.
Post-commit gates and remote/draft receipts belong in external final-handoff.json,
avoiding a self-referential evidence commit. No full Go/live TiKV suite, measured
speedup or complete package acceptance is claimed.

Grouped all-target checking, make lint and diff checks pass. The fresh aggregate
contains 86 modules, excluding all nine retired harnesses. All retained live
owner and test hashes match the baseline.
