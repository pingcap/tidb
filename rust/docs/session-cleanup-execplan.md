# Remove unused server models and empty Rust harnesses


This living ExecPlan follows root PLANS.md. Work in `/workspace/tidb` on
`hparser-integration`, starting at `8f8ba0562056682dcc2e4f76a42d0cac939b4625`.
No push or push dry run is authorized.

## Purpose and context


Reduce compiled scaffolding without reducing coverage of the running server.
The server's `auth_session`, `auth_token`, `bootstrap` and `listener` models
have no production callers. Actual authentication goes through
`mysql_connection` and `ConfiguredUserStore`; listening belongs to `SqlNode`;
bootstrap publication belongs to `cluster_session_node/boot.rs`. Go master
`93a01d31f6da205ae4bf376825293903a6899fdb` owns these effects in
`pkg/session/session.go`, `pkg/server/server.go` and
`pkg/privilege/privileges/tidb_auth_token.go`. Pure Rust simulations do not
implement Go's signature verification, durable upgrades or server lifecycle.

## Milestones and work


First enumerate callers, empty tests and original Go declarations. Preserve
source identities and historical obligations in the compact companion ledger
`parity/current-audit/session-cleanup-obligations.json`; historical reasons do
not establish that an owner is still absent today. Remove the four unused
models, their exports and 18 tests. Remove 250 empty session functions
(including 34 fake benchmark tests) and 11 now-empty source modules. Some
files were already unregistered: source removals and compiled test reductions
must be reported separately.

Then make authentication identity, native-password and secure-transport tests
import the actual library instead of compiling source copies. Remove one
secure-transport test whose assertions exactly repeat the preceding test.
Correct active finding references, move stale candidate-index rows to the
ledger, and mark historical carrier claims as superseded. Retain complete Go
source/fixtures and every meaningful session test body.

Finally compare retained harness identities, run related behavioral tests,
affected all-target checking and root lint once at the batch boundary. Commit
normally through the actual locked server-build hook; retain recovery locally.
No defect fix or new Go package acceptance is claimed.

## Progress


- [x] Inspected callers and Go effect owners; captured existing compiled test lists.
- [x] Removed unused models, empty functions and duplicate source compilation.
- [x] Reconcile active documentation and verify exact harness changes.
- [x] Run grouped tests, affected all-target checks and lint; self-review passed.
- [x] Previous batch: actual hook, local commit d558cba172 and external final receipt completed.
- [x] Previous batch: recovery verified and cloud draft33 read back; no push.

## Surprises & Discoveries


Three session files were already unregistered (`part4`, `part5`, `part6`);
51 removed functions therefore do not reduce the live harness. Eight
historical Go declaration references were absent at their recorded paths in
current master; the ledger records this instead of asserting current coverage.
The retained native-auth fixture panicked on the production handshake SET NAMES,
then blocked because recorded contexts retained a socket clone. Isolated execution
reproduced the panic and timed out. The fixture now checks initialization order
and bounds socket I/O; all three cases pass.

An initial inventory script stopped at the unregistered module; only its own
partial edits were restored before the corrected batch was applied.

## Decision Log


Remove test-only models after exhaustive identifier searches across both Rust
repositories, not merely because a Rust filename lacks a Go twin. Retain
actual protocol, authorization, TLS and lifecycle regressions. Preserve
historical before-images in Git rather than duplicating entire deleted files
in docs. Date/author: 2026-10-04, Codex.

## Concrete steps and validation


Activate `/workspace/.cloud-setup/env.sh` in each shell. From `rust/`, with
`CARGO_BUILD_JOBS=1`, run:

    cargo test --locked -p tidb-session --lib -- tests_session_part1_source tests_session_part2_source --test-threads=1
    cargo test --locked -p tidb-server --test all -- auth_identity_source native_password_source secure_transport_source configured_user_store_source sql_node_lifecycle_source mysql_native_auth_lifecycle_source --test-threads=1
    cargo check --locked -p tidb-session -p tidb-server --all-targets

From the repository root run `make lint` and `git diff --check`. Capture
`--list` from old and new session-unit/server-aggregate binaries, prove exact
set differences against the ledger, and assert all other identities remain.
Run a normal commit with `TERM=xterm`, `core.hooksPath=hooks`: the executable
`hooks/pre-commit` must run `cd rust && cargo build --locked -p tidb-server`.
Logs and final hook/recovery evidence live under
`/workspace/.cloud-setup/session-cleanup/`.

## Idempotence and recovery


Do not rerun the edit script over an edited tree. Recover individual deleted
files with `git show 8f8ba0562056682dcc2e4f76a42d0cac939b4625:<path>` if needed,
without resetting concurrent changes. Preserve remote integration changes and
both exact destinations. Disk is constrained; prune only documented inactive
failed temporary or superseded build outputs, never broad `cargo clean`.

## Outcomes & Retrospective


Implementation and 46 behavioral cases pass. Session harness: 2,192 to 1,993
(exactly 199 compiled empty tests removed); server aggregate: 220 to 205
(14 unused-model tests and one duplicate removed), plus four tests removed
with the standalone target. Every other compiled identity is preserved. All
17 nonempty bodies in mixed session files are byte-identical; 11 are in the
already-unregistered part5 file and receive no execution credit. Affected all-target checking and make lint pass after the fixture repair.
The previous normal commit hook and local recovery completed successfully;
see /workspace/.cloud-setup/session-cleanup/final-handoff.json. Cleanup changes no finding status:
86 tracked, 29 repaired, 57 unresolved (36 open, 21 partial). Full Go suites,
full Rust suites, live multi-node and measured performance remain unverified.

## Interfaces and artifacts


Remove the unused public model exports without replacement; both repository
call inventories contain no consumers. Supported server entrypoints remain.
No dependency, lockfile, generated source, Go source, real workload runner or
native client changes are needed. The obligation ledger records removed
identities, file hashes and relocated candidate rows. A validation receipt
will distinguish exact compiled reductions from source-only deletions.

## Continuation: remaining empty harness entries


Starting at d558cba17238fbe11016482e7521fd275ae066e7, remove 347 empty ignored
functions across 39 files in tidb-planner, tidb-executor, tidb-ast, tidb-expr
and tidb-vardef. The core_physical_plan_gap_catalog_source module contains
only 43 empty functions and an unused unit constant, so remove that module
entirely. Other files retain all non-comment code except the exact empty
blocks. Replace stale module-level inventory prose, but preserve contracts
beside real tests. The compact empty-test-cleanup-obligations.json ledger
records parent file hashes and original contract line ranges; Git history
retains the full original Go obligations without duplicating stale prose.

Milestone one is the exact deletion inventory and code preservation proof.
Milestone two is grouped validation of surviving tests in affected modules,
then affected checks, lint and one normal commit hook. Current Go fixtures,
native client, dependencies and production behavior remain unchanged. Move
329 now-stale candidate-index rows to recoverable historical line references.
Do not close structural findings for harness cleanup.

- [x] Inventory and remove 347 empty functions, one unused unit marker, one empty module.
- [x] Verify all remaining non-comment code is byte-identical before formatting.
- [x] Grouped surviving tests:80 passed; all removed identities absent. Root lint passed.
- [x] Affected all-target checks passed; self-review and diff check passed.
- [ ] Normal locked-build hook outcome recorded in the external final handoff.
- [ ] Normal locked-build hook, local recovery and cloud draft; no push.

Run from rust/ with CARGO_BUILD_JOBS=1 and the cloud environment activated:

    cargo test --locked -p tidb-planner -p tidb-ast -p tidb-executor --test all --no-run
    cargo test --locked -p tidb-expr -p tidb-vardef --lib --no-run
    cargo check --locked -p tidb-planner -p tidb-ast -p tidb-executor -p tidb-expr -p tidb-vardef --all-targets

Use the built harnesses with each affected module name as a filter and
--test-threads=1. Preserve all results, including failures, under
/workspace/.cloud-setup/empty-test-cleanup/. Then run root make lint and
normal commit with TERM=xterm. The actual locked server hook is mandatory.
The validation receipt owns exact commands and outcomes; completed final
hook/bundle/draft results live in that directory's final-handoff.json.

Discovery: prior strict inventories missed bare #[ignore] attributes and a
unit-valued TestMain marker. None performs Go behavior. Whole-file rustfmt
would introduce unrelated formatting changes; those edits were discarded
and the exact removal batch reapplied. No production bug fix or complete Go
package acceptance is claimed. Recover files with git show at the parent
above, preserving concurrent changes. Findings remain 29 repaired and57
unresolved (36 open,21 partial). No performance improvement is claimed until
measured; reductions in source and harness entries are reported directly.

Continuation outcome:347 empty functions and the unused TestMain marker are
removed; all367 surviving functions in touched files remain verbatim. The
built harness inventories contain none of the removed identities. Eighty
representative cases pass; other unchanged cases are not counted as executed.
233 obsolete mapping rows in17 historical receipts are also retired, retaining
parent line references. No finding status changes. Final check/hook and
recovery outcomes are recorded in the external final handoff named above.
