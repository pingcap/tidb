# Remove extra storage policies and use native client owners

This living ExecPlan follows repository `PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose and acceptance


The user explicitly requested removal of all reviewed duplicate owners and extra policies, a client-rust dependency update, and Go-compatible behavior. This authorizes replacing the integration deliberately reverted in `2471be70e9`. Start from the previously validated integration in `b1534707`, preserving all subsequent unrelated commits. The accepted boundary is Go master and its pinned client-go packages, with native Rust synchronization and typed errors. This is maintenance of existing ports, not a claim that incomplete packages have become completely transcreated.

Production transactions must have one native transaction engine, MemDB and lock resolver; caller settings and cancellation must determine behavior. Remove local mutation admission limits, SQL-category mutation rewriting/coalescing, restrictive lock decoding, fixed session lock waits, permanent full-key attempt histories, hand-written SET time_zone SQL parsing, and cancellation identified by error display text. The previously reviewed duplicate retry/cache/RPC owners must be reconciled against native APIs and migrated without deleting required DistSQL or PD behavior. All explicit retry limits, SQL error identities, snapshot metadata, shared locks, metrics, and existing performance improvements remain acceptance requirements.

## Progress


- [x] Pull TiDB integration/master and client-rust master; both working trees initially clean.
- [x] Restore native transaction/MemDB/resolver integration without committing over newer work.
- [x] Replace textual cancellation identity in native client-rust; reproduce, test, commit and push.
- [x] Synchronize TiDB to the published native revision with only required transport compatibility patches.
- [x] Remove facade mutation budgets, SQL-category rewriting, duplicate value history, and production attempt-history allocations; retain explicit test observation.
- [x] Thread session lock timeout through active SQL statements and global timeout through statistics workers; load configured native buffer size limits.
- [ ] Audit remaining contextless helpers and table-layer assertion propagation.
- [x] Remove special SQL parsing and execute supported storage SET assignments through the normal parser/session-variable path.
- [x] Publish native cancellation/lifetime fixes and synchronize TiDB to client-rust 884589f.
- [ ] Complete remaining background lifetime scopes; concrete patch awaits user approval after automatic review rejection.
- [ ] Consolidate remaining retry/cache/RPC implementations with native owners and retain TiDB-owned adapters.
- [ ] Verify affected packages, regression behavior, required lint and locked server builds; commit and push.

## Source and package inventory


TiDB baseline is `2471be70e9030f36284506f1b87baadebaf3e32d`; Go master is `51a1a4abfc192a91f98fe968ad87eced9221f663`; pinned client-go is `v2.0.8-0.20260928031501-8edb23f6c7ee`. Native baseline is `515e4aab0e4a98d891641ab726209748752eb50e`. The restored `native-transaction-integration-inventory.json` retains complete package evidence for transaction/resolver integration. Extend source inventory for the package owners touched by this follow-up; do not treat individual fixes as package-completion receipts. No deeper AGENTS.md was found in the affected Rust or native source trees.

## Milestones and implementation


First, restore the prior integrated boundary using the inverse of the exact revert, then update the native error package. Add regressions that distinguish an ordinary StringError containing context canceled from the cancellation sentinel. They must fail on the native baseline. Introduce a typed context-cancellation error with the same display spelling as Go, replace native producers and consumers, preserve remote gRPC cancellation handling, and verify full library tests and strict Clippy before native publication.

Next, run `rust/scripts/sync-tikv-client-rs.sh` against the published master. The restored source-only sync preserves build caches, regenerates protocols from source and applies only the four tonic/prost compatibility patches. Do not manually edit generated artifacts. Update root Cargo.lock through Cargo if necessary, then use locked commands. Verify MemDB size limits, flags and transaction commit behavior rather than retaining separate mutation histories or SQL-operation categories.

Session behavior is a separate milestone. Read the existing parser/SET executor and variable store, add failing regression cases for malformed SET prefixes, legitimate parsed SET forms, and nondefault lock-wait settings. Remove the special text recognizer and route normal statements to the existing owner. Keep Go timeutil's timezone-value parser and error mapping.

The remaining client boundary milestone audits every production retry/cache/RPC caller, migrates to native interfaces, and deletes only the replaced owner. Use existing native cancellation, routing, batch RPC and metric types; extend the native library where its Go contract is missing. Do not use a locally maintained protocol algorithm as the permanent adapter.

## Validation and commands


Native commands run from `/Users/qiliu/projects/client-rust`:

    cargo test --locked --lib source_cancellation_uses_identity_not_error_text
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings

TiDB commands run from `rust/`, selected according to the final change:

    cargo check --locked -p tidb-server --all-targets
    cargo test --locked -p tidb-txnkv --lib
    cargo test --locked -p tidb-txnkv --test all
    cargo test --locked -p tidb-txnkv --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-unistore --test client_transaction

Add focused SET/session regressions and whole affected retry/cache/RPC package validation as implementation proceeds. Run root `make lint`. No Bazel preparation is required unless Go files/imports, Bazel metadata, or Go module inputs change. Commit through `TERM=xterm git -c core.hooksPath=hooks commit` so the mandatory locked server build runs. Rerun `cd rust && cargo build --locked -p tidb-server` after commit and immediately before each push of Rust changes. Push native to master and TiDB to hparser-integration without force. Preserve existing regression tests and account for baseline failures explicitly.

## Surprises & Discoveries


The branch's revert restored local algorithms and an older native vendor snapshot. The latest explicit user instruction resolves the earlier pending restore decision. Prior integration validation recorded two region-cache failures and 20 DistSQL failures; those are historical baseline observations, not permission to ignore new failures or evidence of parity. The follow-up must verify any affected failures again.

## Decision Log


Restore the shared native boundary rather than patching each local algorithm independently, because Go places those algorithms in client-go. Native correctness/API changes are published upstream before vendoring. Required Rust adapters and transport-version compatibility remain. The removal list is an architectural migration: remove restrictions only together with their Go-aligned owner, never by bypassing validation or swallowing errors.

## Outcomes & Retrospective


Native cancellation identity was published as 89926da, followed by operation lifetime propagation as 884589f on client-rust master. Full native library validation passed 1,401 tests with two ignored; strict Clippy passed. The public resolver pool configuration change then passed all 42 resolver tests and Clippy. TiDB was synchronized through the maintained source/protobuf generation script to 884589f with four transport compatibility patches. Native changes make ordinary rollback, secondary commit, automatic heartbeat and region fan-out carry explicit background scopes. Injected TiDB clients disable asynchronous resolver scheduling by setting the pool size to zero instead of closing its lifetime.

The local prewrite/commit/rollback/heartbeat/resolver engines, SQL value undo log and mutation coalescer are deleted. The facade's count/byte budgets, arguments, constants, error variants and SQL sizing pass are removed. BufferMutation carries Set/Delete/Lock with independent flags/assertions. Native buffer limits load TiDB globals at construction, process configuration initializes those globals, and SQL buffer failures retain SQL identity instead of panicking. Autocommit hands over MemDB without rebuilding its entries. Full-key attempt histories are disabled by default, including injected clients, and explicitly enabled only in tests that inspect them. The SET prefix recognizer is removed from both lightweight TiKV node variants; supported storage assignments use the existing parser and SET owner, including warnings. Statement locks use native staging inspection and Go KeyNeedToLock, retaining same-value writes and native flags. Active statement waits use session values, including changes inside an open transaction; statistics workers use the global setting.

Regression evidence includes malformed SET prefixes, configured entry/transaction limits, lazy insert-delete flags, equal-value statement writes, native lock flags, session wait changes and foreground commit/rollback cancellation. Foreground cancellation failed when the previous request-type heuristic was temporarily restored, and all seven bridge ownership tests passed after restoring explicit lifetime dispatch. Logs are /private/tmp/tidb-removal-foreground-red.log and -green.log. The former stale-overlap region failures exposed cached children returned outside the requested key/end-key/ID; query-specific checks now reject them. The default batch region loader previously returned only each range's first region; walking to the range end fixed all 20 DistSQL failures. SQL PrepareTxnCtx behavior also required resetting current TSO at the beginning of the next autocommit statement. Bootstrap tests now account for the already-present metadata-lock view; the embedded ambiguity regression uses the typed cancellation sentinel.

Validated results: txnkv library 136 passed, one ignored, followed by seven bridge ownership tests after one new regression; aggregate txnkv 422 passed, ten ignored; configured limits one passed; SQL buffer 20 passed; multi-statement transactions 11 passed; server transaction tests 26 passed; SET six passed; embedded transactions 15 passed; bootstrap eight passed; DML planning 83 passed; account writes 13 passed; system variables five passed; resolver/snapshot suites 78 passed; DistSQL library 37 passed, one ignored; DistSQL integration 256 passed, two ignored. Root make lint completed successfully. macOS system-memory queries and loopback fixtures required execution outside the sandbox; their initial permission failures are not product failures. Existing compiler/linker/jemalloc warnings remain. The hook build, fresh pre-push locked build and publication are still pending.

Remaining work is explicit. The local region cache and low-level RPC owner migration is not complete. The bridge still recognizes heartbeat requests as detached until every native heartbeat path carries scope. The unapplied /private/tmp/client-rust-background-lifetimes.patch covers pipelined rollback, cleanup, secondary completion, heartbeat and transaction-file secondaries; automatic approval review rejected the multi-path write as a transaction correctness risk and a user approval question is pending. Mutation assertions still need a table-owner audit: Go lazy optimistic INSERT uses AssertUnknown, while pessimistic/eager insertion uses AssertNotExist; the generic convenience constructor currently chooses NotExist. The compatibility path for an unbound explicit-session buffer still reconstructs mutations and must be replaced without losing locked entries or flags. Contextless helper default waits require review. No complete package, full parity, benchmark improvement or RealTiKV validation is claimed by this milestone.

## Recovery and artifact ownership


Both repositories were clean before work; current changes belong to this request. Preserve other incoming changes on fetch/pull and do not reset shared branches. Regenerate vendor protocols from pinned inputs when build output is stale. Logs use `/private/tmp/native-cancellation-*.log` and `/private/tmp/tidb-removal-*.log`. Do not delete active build caches or shared worktrees to reclaim space.

Revision note: updated on 2026-09-30 with native 884589f, explicit lifetime regressions, native statement staging, region coverage repairs, current test results and unresolved ownership work.
