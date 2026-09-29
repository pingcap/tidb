# Share the Go transaction boundary through client-rust


This living ExecPlan follows `PLANS.md`. Maintain its progress and evidence as work proceeds.

## Purpose / Big Picture


TiDB Rust should wrap client-rust as Go TiDB wraps client-go. Transaction correctness fixes must live in the client repository and be tested there, so all consumers get the same behavior. TiDB should not need private transaction algorithms or behavioral vendor patches. This change upstreams the client-owned part of the completed transaction consolidation and updates TiDB's vendor dependency. It is dependency integration and regression repair, not a claim of complete transcreation of any Go package.

## Context and source boundary


The TiDB checkout is `/Users/qiliu/projects/tidb`, branch `hparser-integration`, initially `9394c7a8c575dc0e0167f06c7f5c792750d23b67`. The client checkout is `/Users/qiliu/projects/client-rust`, branch `codex/client-go-parity`, fast-forwarded to origin/master `32dec1837ee9686f1a32861a0b40f2ed880be3c7`. Push the latter to `ngaut/client-rust` master and TiDB to `pingcap/tidb` hparser-integration, without rewriting history.

Both remotes were fetched first. TiDB master is `12b639a1161cd5a60126a47277f5ad14c320fd4a`; its client-go requirement remains `v2.0.8-0.20260928031501-8edb23f6c7ee`. The Go source is available in `/Users/qiliu/go/pkg/mod/github.com/tikv/client-go/v2@v2.0.8-0.20260928031501-8edb23f6c7ee`. `txnkv/transaction` has no doc.go. Its complete source package remains the comparison unit; cross-package error, mock and snapshot changes are necessary to preserve its integration boundary. Full-package parity requires the inventories and outstanding gates recorded in `rust/docs/audits/structural-20260929/inventory.json.gz`; this change does not supersede them.

`rust/third_party/patches/tikv-client-rs` contains both dependency-version adaptations and changes belonging to the client. `src/transaction/transaction.rs` owns transaction state, `buffer.rs` owns mutation and snapshot bookkeeping, and `src/request/plan.rs` owns request retry/lock resolution. `SyncTransaction` must wrap this same engine for injected and remote clients. TiDB's driver only adapts transport, SQL errors and schema callbacks.

## Progress


- [x] Fetch both repositories and verify the current master source pin.
- [x] Read repository rules and change-review/build-gate skills; no Go or Bazel edits require bazel_prepare.
- [x] Establish upstream regression failures and review the Go ownership contracts.
- [x] Move client-owned interfaces and behavior into client-rust, with regression tests.
- [x] Validate and push the initial client integration as `54142f5df148e53b91667ab7b14bb39ed6330681`.
- [x] Validate and push the additional snapshot retry/cache repairs as `65b9a87ee39161890419627280826ada337a0caf`.
- [x] Sync TiDB to that revision and remove absorbed vendor patches; all 263 source files match the patched scratch checkout.
- [x] Run focused integration checks and make lint.
- [x] Run the commit hook build and fresh locked server build; prepare TiDB delivery to `origin/hparser-integration` with the same build enforced immediately before push.

## Milestone 1: upstream ownership and regressions


Compare client-go `txnkv/transaction/pessimistic.go`, `txn.go`, `config/retry/backoff.go`, and `txnkv/txnsnapshot/snapshot.go` against Rust. Preserve typed retry errors, independent snapshot and locking values, pessimistic wait-budget handling, and the same client for mock PD region routing. Expose the existing transport and transaction interfaces needed by TiDB instead of reimplementing their behavior. Add tests before applying the behavioral changes and observe the expected failures. Apply reviewed client-owned vendor patches to the clean client checkout, retaining its tonic/prost versions. Add an external-consumer test that constructs a synchronous transaction over the existing mock, verifies shared native buffer state and runs commit/rollback through the native engine. Check nested-runtime rejection.

## Milestone 2: dependency delivery


Validate client-rust with focused red/green regressions, its serial library suite and external-consumer test. Run formatting, workspace all-feature checks and clippy under the repository's warning policy; regenerate protocol outputs only from inputs if required. Commit and push client-rust master. Remove patches absorbed by this revision from TiDB and regenerate the vendored copy with the remaining compatibility patches. Preserve build caches during sync; copy source files, never build outputs. Verify vendor content and the recorded upstream revision.

## Milestone 3: TiDB integration delivery


Run from TiDB `rust/`: `cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture`, `cargo test --locked -p tidb-txnkv --lib transaction -- --nocapture`, and focused affected transaction/snapshot tests. Run `make lint` at TiDB root. Review the diff, commit through the pre-commit hook (which must run `cd rust && cargo build --locked -p tidb-server`), then rerun that exact build immediately before pushing `origin HEAD:hparser-integration`. Report exact commands and known unverified areas. No performance improvement is claimed without benchmarks.

## Surprises & Discoveries


The client checkout was clean but three commits behind master; it is now fast-forwarded. The transaction fixes were present only in TiDB's maintained patches, leaving direct client-rust consumers with the older behavior. Historical client parity ledgers mentioned by TiDB documents were removed in client-rust `94f30e2`; they cannot support current completion claims.

## Decision Log


Decision (2026-09-29): upstream native client fixes rather than preserve a TiDB-only behavioral fork. The Go driver delegates to the client; the Rust boundary should do the same. Retain only necessary dependency-toolchain adaptations in TiDB. Exclude the old snapshot helper-stat extension from upstream unless an actual Go contract and current consumer require it.

## Validation and recovery


Commands run in the named checkout. Logs are saved under `/private/tmp/client-rust-boundary-*` and `/private/tmp/tidb-client-boundary-*`. A failed gate must be investigated, not silently skipped. Existing unrelated failures must be identified explicitly. Changes are isolated in Git diffs, and pushes are fast-forward only. Never reset either active checkout to recover from a failed sync. Regenerate a temporary patched source tree and inspect it before replacing tracked vendor files. Keep old source and build caches until the new vendor tree has been validated.

## Outcomes & Retrospective


The transaction dependency boundary cleanup is implemented and validated. Client-rust master contains `54142f5` and `65b9a87`; TiDB now consumes that upstream implementation through public interfaces with only four dependency adaptations. Six behavioral/private-interface patches and duplicate snapshot accounting are removed. No complete Go-package parity or benchmark completion claim is made; the remaining source audit and real-cluster gates are described below.

## Validation evidence


In client-rust, the new `snapshot_retry_exhaustion_retains_the_source_error_identity` regression failed with `StringError("tikv server timeout")`; `lock_return_values_do_not_replace_snapshot_reads` failed because the locking value replaced the earlier snapshot value. The downstream `pd_routed_requests_share_the_mock_coprocessor_handler` regression failed with `Unimplemented` on the PD-routed request. All now pass.

`RUST_MIN_STACK=16777216 cargo test --locked --lib -- --test-threads=1` passed: 1369 tests, two ignored. The ordinary 2 MiB Rust test thread stack overflowed in a debug-build pessimistic-lock future; the larger-stack rerun completed all tests. `cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1` passed eight tests, including native synchronous commit/rollback, staged buffer cleanup, and nested-runtime rejection. `cargo check --locked --workspace --all-targets --all-features`, `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all`, `cargo fmt -- --check`, and `git diff --check` passed.

Decision (2026-09-29): use client-rust's existing public `tikv` and crate-root facades in TiDB. Add only the missing PD adapter type reexports. Remove patch 010 rather than make six private implementation modules public. The old optional `mock` feature in patch 020 has no consumer; the actual public `testutils` mock already works in ordinary builds. Also remove patch 120: native reads record Go's resolve-lock duration internally, and its synthetic `ResolveLock` helper-RPC bucket belongs to the removed TiDB coordinator. Tests should assert the native duration metric. The vendor sync now stages source files only and preserves both build caches instead of copying and deleting hundreds of gigabytes of Cargo output.


## Snapshot ownership follow-through


The restored standalone `snapshot_lock_wait_source` suite exposed three integration defects: mutable scans filled the point-read cache, concurrent batch workers accumulated both retry histories, and nested CheckTxnStatus retries slept outside the caller's retry owner. Go's scanner leaves the point cache alone; `batchGetKeysByRegions` selects the last completed worker's Backoffer and records its totals once; `getTxnStatusFromLock` uses that same Backoffer for txnNotFound and status RPC region errors. These fixes now live in client-rust. `ResolveLocksContext` carries a per-operation retry owner on a context clone; the store-level status cache, cleanup pool and resolving-lock registry remain shared. Detached read cleanup retains its separate lifetime.

The client exposes its existing async BatchGet selector and shared resolver injection as public integration interfaces. TiDB reads its published configuration on every read and calls native `batch_get_with_options`, preserving Go's result-map/cache semantics. Process shutdown cancels and joins the native resolver before closing the transport. TiDB's old snapshot RPC observer, transport completion hooks and synthetic ResolveLock RPC metric were deleted because the native client owns those observations.

The standalone fixtures needed the current RegionQueryLoader adapter and shared scripted-response queues/counters because the native client clones RPC adapters. Concurrent batch replies are keyed by request rather than dispatch order. Lock-hint sets are compared without order, and LockFast duration uses Go's default 10ms base with equal jitter (5–9ms), rather than the former wrapper's 1ms schedule. The tests still require exactly one completed worker's backoff and two nested txnNotFound waits.

The regression was observed before the fixes: scan cached one point value instead of zero; worker accounting reported two sleeps instead of one; nested status retries reported zero txnNotFound sleeps instead of two. All 17 standalone snapshot cases now pass. Native regression tests also cover the completed-worker history and a status sequence containing two txnNotFound errors with an intervening server-busy region error. The client all-feature library gates passed 1355 client tests (six ignored), two protocol-build tests and 46 unistore tests. Nine external-consumer tests and warning-denying workspace Clippy passed. This verifies these contracts, not every source package or every resolver path; full source-package transcreation remains subject to the existing complete inventory and gates.


## Final integration checks


The final TiDB aggregate gate initially found a fixture that expected a successful pessimistic prewrite retry but answered its blocking transaction's status RPC with Unimplemented. The fixture now explicitly supplies that transaction's rollback status and requires both the status lookup and the second prewrite. The aggregate suite then passed 419 tests with ten existing ignores and two independently established region-cache baseline failures excluded. Those excluded tests are `region_cache_source::stale_merge_parent_does_not_evict_newer_split_child` and `region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction`; baseline evidence is in `transaction-consolidation-execplan.md`. This change does not resolve those unrelated failures.

Exact final commands, run from `/Users/qiliu/projects/client-rust`:

    cargo test --locked --workspace --all-features --lib -- --test-threads=1
    cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all
    cargo fmt -- --check
    RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
    git diff --check

All passed. The earlier default-feature library run passed 1370 tests with two ignored; the two subsequent owner regressions also passed individually. Protocol regeneration with `cargo run --locked -p tikv-client-proto-build` produced no generated-source changes, and the earlier `cargo test --locked --doc` passed 51 doctests.

Exact final TiDB commands, from `/Users/qiliu/projects/tidb/rust`:

    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib read_runtime -- --nocapture
    cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture
    cargo test --locked -p tidb-txnkv --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test all -- --skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions

These passed 17, 7, 15, 135, 419 and 25 tests respectively. The library suite has one existing ignore. The aggregate result above records its exclusions. Repository-wide existing warnings remain; client-rust's all-target/all-feature Clippy passes with warnings denied. From the TiDB repository root, `bash rust/scripts/sync-tikv-client-rs.sh`, `bash -n rust/scripts/sync-tikv-client-rs.sh`, `make lint`, and `git diff --check` passed. Generated protocol outputs remain unchanged. The sync source comparison found zero mismatches across 263 files.

Correctness and compatibility evidence covers the affected retry, cache, mock transport and SQL transaction contracts. Real TiKV/multi-node fault tests, differential runs against Go, and sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. No throughput or latency improvement is claimed. Other lock-resolver paths still contain legacy retry handling and require source-shaped fork/join ownership review before any complete package parity claim. This integration removes TiDB-only behavioral patches; it does not certify the entire client-go dependency as transcreated.


## Delivery gate receipt


`git -c core.hooksPath=hooks commit` ran the required `cd rust && cargo build --locked -p tidb-server` successfully, recorded in `/private/tmp/tidb-client-boundary-commit.log`. A separate fresh run of `cd rust && cargo build --locked -p tidb-server` also passed, recorded in `/private/tmp/tidb-client-boundary-prepush-build.log`. The final delivery command repeats this locked build immediately before `git push origin HEAD:hparser-integration`; a build failure stops the push. The final documentation amendment uses the same pre-commit hook. Both remotes were fetched again before delivery, with no competing commits and no change to the recorded Go master pin.


## Review follow-through: one cache and explicit operation ownership


The 2026-09-29 complete review tested five findings, recorded in `/private/tmp/client-tidb-complete-review.md`. Continue from clean TiDB `8e5efe8304` and native client `65b9a87`; both remotes were fetched again and those branches plus the Go master pin are unchanged. The duplicate transaction phase engines are already deleted. TiDB's coprocessor read path still uses `lock/resolver.rs`, `lock/pessimistic.rs`, `lock/async_resolve.rs` and a separate resolved-status cache; these are real remaining duplicate owners, not transaction facades. Do not report all duplication removed until these call sites delegate to the native resolver.

The immediate acceptance criteria are that the four correctly attributed review reproductions pass as permanent regressions, snapshot values and lock flags have independent ownership, nested secondary checks share the caller's Go backoff fork/join lifecycle, explicit Rust retry overrides remain effective, and detached cleanup carries its own cancellation scope through TiDB's bridge. Fix client behavior in client-rust, update its master, then sync the vendor and commit/push TiDB using the existing mandatory gates. This remains integration and repair, not a whole-package transcreation claim.

Alternatives considered: retaining the lock-tagged snapshot cache and patching each accessor would preserve duplicate lock state and leave cache accounting/eviction incorrect. Remove that second lock state and use MemDB flags as the sole authority. Inferring background lifetimes from RPC type cannot distinguish foreground ResolveLock from background ResolveLock; carry the existing client's operation lifetime through an explicit context scope instead. Independent retry counters for secondary checks would continue resetting caller budgets; fork and merge the supplied owner as Go does.

Progress for this follow-through:

- [x] Fetch both remotes; confirm transaction engine deletions and identify the remaining coprocessor resolver duplication.
- [x] Reproduce four confirmed defects before fixes; retain test snippets and failure logs under `/private/tmp`. Withdraw the incorrectly attributed fifth finding as explained below.
- [x] Add seven permanent native regressions and implement cache/retry ownership repairs; push client-rust master as 8b7a726.
- [x] Carry foreground/background operation context across the native client and TiDB transport boundary, including cancellation of a running blocking RPC.
- [x] Retire the remaining TiDB lock resolver algorithms and duplicate status cache/cleanup pool once their caller contracts are covered.
- [x] Run affected native and TiDB tests, make lint and self-review.
- [x] Commit TiDB through the locked pre-commit server build; require the same fresh locked build in the final push command.

Use targeted native library tests followed by the full serial native library suite because buffer and resolver changes affect every transaction. Validate TiDB snapshot, lock-resolver and coprocessor integration surfaces, embedded transaction tests and SQL transaction tests. Preserve independent baseline failures documented above; do not weaken assertions to hide behavioral changes. Update this plan with actual results and any remaining ownership boundaries before delivery.


### Source-boundary correction


The review's mock wake-up finding was incorrectly attributed. Client-go `internal/mockstore/mocktikv/mvcc_leveldb.go` explicitly does not implement server-side lock waiting; `rpc.go:simulateServerSideWaitLock` only sleeps 5 ms and returns the original lock error. TiDB `pkg/store/mockstore/unistore/tikv/server.go` is a different backend and returns WriteConflict on normal wake-up. Client-go `integration_tests/shared_lock_test.go:TestSharedLock` explicitly skips unless real TiKV is enabled. Rust's mock has pre-existing mixed waiter behavior, and Rust source-named shared-lock tests incorrectly run against that mock. Changing every normal mock wake to WriteConflict failed RetryPushTTL and PrewriteAssertion. That proposed fix and incorrectly grounded regression were withdrawn, not hidden by altering those tests. This backend/harness mismatch remains a separate audited gap; no mock parity claim is made by this change.

The corrected full native library run passed 1378 tests with two ignored. A new saturation regression then failed because the initial cleanup scope implementation detached even rejected tasks. Go batchLiteResolveLocks calls the inline fallback with the original backoffer/context. Admission now installs the background scope only for accepted tasks, while nested accepted cleanup tasks use the same resolver owner. Saturated fallback retains the caller scope. The transport cancels blocking background RPCs on owner cancellation or task drop.


### Native delivery evidence


Client master `8b7a726` removes BufferEntry's duplicate Locked/SharedLocked state and centralizes snapshot cache access, metadata eligibility, eviction, and accounting. MemDB exclusively owns transaction lock flags. CheckSecondaryLocks uses the operation backoffer, and checkAllSecondaries forks that owner per worker, adopts the last completed worker's history on both success and error, and cancels forks on return. Explicit no-retry options no longer acquire Go-default retries through the lock-wait callback. Admitted cleanup tasks carry the resolver cancellation scope through nested workers; rejected cleanup stays in the caller scope.

From `/Users/qiliu/projects/client-rust`, these passed:

    cargo test --locked --lib ownership_regressions -- --test-threads=1
    cargo test --locked --workspace --all-features --lib -- --test-threads=1
    cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all
    cargo fmt -- --check
    git diff --check

The counts are seven focused ownership cases; 1362 client tests (six ignored), two protocol-build tests, 46 reusable engine tests; and nine external consumer tests. Logs: `/private/tmp/client-ownership-final-regressions.log`, `client-ownership-all-features.log`, `client-ownership-public.log`, and `client-ownership-clippy.log`. The earlier default-feature run passed 1378 with two ignored, before the additional saturation case. `make lint` also passed from the TiDB root (`/private/tmp/tidb-ownership-lint.log`). The native commit and master push succeeded; no private vendor fix is needed.


### TiDB integration evidence


The vendor sync regenerated protocol outputs through `sync-tikv-client-rs.sh` and produced no generated-file changes. All 263 vendored files match the patched upstream staging checkout; only the four native source files and sync receipt changed. TiDB bridge regressions run against the old bridge failed three of four tests: detached ResolveLock inherited statement cancellation, a cancelled cleanup owner still sent an RPC, and cancelling an in-flight owner did not stop the blocking transport. Restoring the fixed bridge passes all four, including a second path that explicitly drops the async task. The complete txnkv library passed 139 tests (one existing ignore), and snapshot_lock_wait_source passed all 17 cases. Logs: `/private/tmp/tidb-ownership-bridge-red.log`, `/private/tmp/tidb-ownership-library.log`, `/private/tmp/tidb-ownership-snapshot.log`.


The embedded `tidb-unistore` client transaction tests passed all 15 cases, and `tidb-server` SQL transaction tests passed all 25. Exact TiDB commands from `rust/`:

    cargo test --locked -p tidb-txnkv --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source -- --test-threads=1
    cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions

No real TiKV, multi-node faults, Go differential execution, or sysbench/TPC-C/TPC-H/YCSB benchmarks were run. No performance improvement or complete package parity is claimed. Remaining structural work includes the TiDB coprocessor resolver and its separate status cache/pool, native cleanup paths that still use legacy retry budgets, and the mock/backend test-source mismatch above. These must be addressed at their shared ownership boundary, not bypassed by returning a fixed timestamp, adding another resolver mode, or weakening the source tests.


The txnkv aggregate integration suite passed 419 tests with ten existing ignores and the same two documented region-cache baseline exclusions. Exact command from `rust/`:

    cargo test --locked -p tidb-txnkv --test all -- --skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction

Self-review verified the source cache no longer carries lock-state variants, native worker scope is applied only after admission, secondary history is merged on both result paths, the transport drop guard owns only newly detached call cancellation, and no generated outputs were edited. The required delivery uses `git -c core.hooksPath=hooks commit`, followed immediately before push by `(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration`.


The TiDB pre-commit hook successfully ran `cd rust && cargo build --locked -p tidb-server` and created the integration commit. Its log is `/private/tmp/tidb-ownership-commit.log`. This final receipt amendment uses that hook again. Delivery remains guarded by a fresh invocation of the exact same locked build immediately before the branch push; failure prevents the push. The remaining coprocessor resolver milestone above is deliberately incomplete.


## Follow Go for the five inline review comments


The user explicitly requested following Go for all five review comments. Refresh both authorized branches before edits. Comments 1, 2, 3 and 5 are implemented by native 8b7a726 / TiDB 7fd550c3ce; rerun their regressions and extend explicit finite-retry coverage. Comment 4 must follow the owning package: client-go mocktikv `MVCCLevelDB.PessimisticLock` only waits 5 ms and returns its original Locked error. The normal-wake WriteConflict rule belongs to TiDB unistore and real TiKV, not this mock. Remove the mock RPC's invented server wait/reacquisition/TTL-recomputation pipeline as one unit. Keep wake-up assertions by moving tests that require actual waiter behavior to the existing real-TiKV integration harness, matching upstream shared_lock_test.go's withTiKV gate; never change production mock behavior to make those tests pass.

The new mock regression fails on 8b7a726: after the holder rolls back, the same pending RPC returns success and acquires the lock. It must return the original Locked response in normal and ForceLock modes, leave the key unlocked, then allow acquisition by a new client request. Log: `/private/tmp/client-follow-go-mock-red.log`.

Progress:

- [x] Refresh remotes; verify correct source ownership and reproduce mock reacquisition defect.
- [x] Remove the extra mock waiter and move four server waiter cases to the real-TiKV integration target; run all four plus an exclusive normal/force comparison on an isolated local cluster.
- [x] Verify comments 1/2/3/5, extend secondary retry history to two regions, and fix the additional recently-updated-lock finite-retry gap.
- [x] Validate and publish client master; sync and validate TiDB. Deliver TiDB through the mandatory locked pre-commit and pre-push server build gates below.

This is source-boundary repair, not a whole-package parity claim. The separate coprocessor resolver milestone remains outstanding. A real-backend test that cannot run locally must remain executable under the existing integration-tests feature and be reported as unverified; do not count its compilation as a passing real-cluster test.


Comment 1's additional finite-budget regression failed on the recently-updated-lock branch (one RPC instead of three with two retries allowed). Both that branch and ordinary live locks now use ResolveLock.wait_for_lock_retry; default Go callbacks, snapshot budgets, and explicit Rust limits retain their own configured policy. The matrix covers zero/two retries, normal/recent locks, and success before exhaustion. Comment 2's completed-history regression now retries in two independent regions and requires one child's history rather than their sum. Comments 3 and 5 retain the previously passing timestamp/cache and foreground/background cancellation tests.

Four server waiter cases moved from library mock setup to `tests/integration_tests/lock_wait_source.rs`, under the existing integration-tests feature. Their normal-wake assertions now require the typed WriteConflict error, including the two/four-transaction deadlock chains. A fifth case tests an exclusive holder rolling back: normal mode returns WriteConflict, ForceLock succeeds. All five passed on local TiKV 9.0.0-beta.2 (commit 8e964719db0d2088d47a280a1dde3fefa1b31d6b, built 2026-08-21) with an isolated PD on port 22379. The playground was stopped and its task-owned data removed; no shared cluster was cleared.

The real-server command, from client-rust, was:

    PD_ADDRS=127.0.0.1:22379 cargo test --locked --features integration-tests --test integration_tests lock_wait_source -- --test-threads=1 --nocapture

The runner used `tiup playground v9.0.0-beta.2.pre-nightly --mode tikv-slim --tag <unique_task_tag> --port-offset 20000 --kv 1 --pd 1 --without-monitor`, verified PD and an Up TiKV store, and trapped teardown. Logs are `/private/tmp/client-follow-go-realtikv-tests.log`, `client-follow-go-playground.log`, and `client-follow-go-realtikv-run.log`. This verifies the reviewed wake-up contracts; it is not a full real-cluster package certification or a benchmark run.


Self-review removed the now-unused `MockEngine.transaction_was_deadlocked` accessor and the detector's separate deadlocked-transaction set. That state existed only to drive the invented waiter; client-go's mock detector owns its wait-for graph without retaining this second transaction state. Existing cycle, edge cleanup and expiration tests remain intact; assertions of the removed private state were deleted. All 46 engine tests and all-target/all-feature Clippy passed after this cleanup.

Native validation passed: 1360 client library tests (six existing ignores), two protocol-build tests, 46 engine tests, nine external consumer tests, and five real-TiKV waiter tests. The full library command and consumer commands are recorded below; the first engine-only invocation used a nonexistent package name and was corrected to `unistore`, with all 46 tests passing.

From client-rust:

    cargo test --locked --workspace --all-features --lib -- --test-threads=1
    cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1
    cargo test --locked -p unistore --lib
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all
    cargo fmt -- --check
    git diff --check

Logs: `/private/tmp/client-follow-go-all-features.log`, `client-follow-go-consumer.log`, `client-follow-go-engine-final.log`, `client-follow-go-clippy-final.log`. `make lint` passed from the TiDB root (`/private/tmp/tidb-follow-go-lint.log`). Native master `53a8db9778cba60971f0dd17b63209848eebbbe8` is committed and pushed. The maintained vendor sync succeeded with all four compatibility patches, regenerated protocol outputs without changing them, and copied all 264 source files exactly. TiDB now consumes the published upstream fix rather than a private patch.


### TiDB validation and delivery


Against the updated client, the txnkv library passed 139 tests (one existing ignore), snapshot lock waiting passed all 17, embedded client transactions passed all 15, and SQL transaction behavior passed all 25. The aggregate txnkv integration target passed 419 tests, with ten existing ignores and the same two separately documented region-cache baseline exclusions. All four bridge tests passed: foreground resolution keeps statement cancellation, accepted background cleanup outlives the statement, cancelled owners send no RPC, and cancellation or future drop stops a running blocking transport. No additional bridge implementation change was needed in this follow-up.

Commands from TiDB's `rust/` directory:

    cargo test --locked -p tidb-txnkv --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source -- --test-threads=1
    cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-txnkv --test all -- --skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction

Logs are `/private/tmp/tidb-follow-go-library.log`, `tidb-follow-go-snapshot.log`, `tidb-follow-go-embedded.log`, `tidb-follow-go-sql.log`, and `tidb-follow-go-all.log`. From the repository root, `make lint` and `git diff --check` passed. No Go source, Go modules or Bazel inputs changed, so Bazel preparation and Go failpoint toggling were unnecessary. Both repository diffs were reviewed, and client-rust master is clean and matches the published commit. The TiDB remote was fetched again with no competing branch changes.

TiDB delivery uses `TERM=xterm git -c core.hooksPath=hooks commit`, whose hook runs `cd rust && cargo build --locked -p tidb-server` and rejects a failure. Immediately before push, run the same fresh locked build from the repository root:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

This repair covers all five review comments at the correct Go package boundary, including the additional finite-budget gap. It does not certify all client-go or TiDB packages. The real-server checks used one local TiKV version; multi-node faults, Go differential execution and sysbench/TPC-C/TPC-H/YCSB throughput were not tested. The two unrelated region-cache failures and remaining TiDB coprocessor resolver ownership milestone are still open. The mock's removed deadlock-history accessor had no native or TiDB consumers after retiring the invented waiter.


## Consolidate the remaining resolver owners


The user's follow-Go instruction authorizes the five removals identified in the read-only review. Both repositories were clean and refreshed; Go master remains 12b639a1161cd5a60126a47277f5ad14c320fd4a and pins client-go 8edb23f6c7ee. The acceptance criterion is one native resolver algorithm, determined-status cache, resolving-lock registry and cleanup pool shared by TiDB transactions and coprocessor reads. TiDB retains transport/type/error adapters and per-snapshot hints, which have distinct Go counterparts. This is a shared-boundary repair, not a new whole-package transcreation claim.

### Progress


- [x] Refresh both remotes and confirm the source package pin and live duplicate callers.
- [x] Replace native cleanup's fixed-attempt/default retry resets with source-owned cumulative backoffers; preserve foreground fallback context and detach only admitted tasks, carrying request source without foreground resource attribution. Resolver and plan-builder regressions pass; broader validation is in progress.
- [x] Expose a source-shaped native resolver operation for external read callers, preserving caller budgets, exact request hints, TTL and resolved/committed transaction IDs.
- [x] Remove TiDB's second resolving-lock registry through the existing native record/update/done API; keep only the Rust lifetime/type adapter.
- [x] Route TiDB coprocessor recovery through the native owner and remove the second algorithms, status cache and cleanup pool (approved by the user on 2026-09-29).
- [x] Run red/green regressions, native library/consumer checks and affected TiDB transaction/coprocessor/SQL checks; run make lint and review the diff.
- [x] Commit and push client master, synchronize TiDB from that published revision, then commit/push hparser-integration through both locked server-build gates.

### Decision Log


Decision: repair the native resolver contract first, then migrate all callers before deleting the TiDB implementation. Patching the two caches or cleanup pools independently leaves split ownership and repeats the earlier failure mode. Keep the source tests as behavioral acceptance tests even where their implementation-specific fixture setup must move to the new boundary. Never preserve another production resolver solely to satisfy a test helper.

Native regression gates cover exhausted caller budgets in pessimistic/ordinary cleanup, cumulative background retry accounting, saturated inline fallback retaining caller context, admitted cleanup retaining only source-approved attribution, and public resolver results. TiDB acceptance covers common status-cache reuse, resolving-lock reporting, cancellation/shutdown, resource attribution, mixed optimistic/pessimistic locks, async commit and coprocessor request hints. Reuse the prior exact targeted commands and add the affected tidb-distsql tests. No Go/Bazel source changes are planned; reevaluate prerequisites if that changes.

The native cleanup fixes passed 51 resolver tests and 12 plan-builder tests. New regressions were observed failing before their fixes: exhausted foreground budgets for ordinary/lite/pessimistic cleanup; admitted versus rejected background budget/resource selection; grouped foreground cleanup incorrectly detached all four RPCs and shared one budget instead of fork/join history; rejected ordinary read cleanup swallowed its error; and request-source metadata disappeared on all cleanup paths and between request-plan wrappers. Logs are `/private/tmp/resolver-consolidation-foreground-red.log`, `resolver-consolidation-admission-red.log`, `resolver-grouped-red.log`, `resolver-error-red.log`, `resolver-source-red.log`, and `resolver-plan-source-red.log`, with corresponding green logs from the resolver and plan-builder suites.

Automatic approval review rejected the proposed public `txnkv::txnlock::ResolveLocksWithOpts` boundary as requiring narrower authorization for its retry, cancellation, hint and metadata semantics. No command from that rejected API implementation ran. A concrete API/migration proposal is saved at `/private/tmp/native-resolver-api-proposal.md`, and an asynchronous approval question is pending. Continue validating and publishing the accepted native cleanup fixes while the API and dependent TiDB resolver migration wait; do not route around that rejection. At that checkpoint the duplicate TiDB owners were still present. The independent registry removal below uses only the existing native API; the algorithm, status cache and cleanup pool remain pending.

### Surprises & Discoveries


The native GC `ResolveLocksOptions` and txnlock's operation options belong to different Go packages despite sharing a name. Preserve that distinction in the Rust public interface rather than mixing unrelated settings. Native cleanup currently rebuilds request policy from individual fields and uses 87 attempts to approximate a 40,000-ms budget. This loses the caller's backoffer on rejected admission and copies foreground resource-group/RU ownership into accepted cleanup, unlike newAsyncResolveBackoffer.

### Outcomes & Retrospective


Implementation and validation are in progress. Do not mark the earlier resolver-retirement milestone complete until the active coprocessor path delegates and the duplicate state/algorithms are deleted.


### Accepted cleanup and registry repair


Native master commit `75ca650` is published. The final native workspace library run passed 1,372 client tests (six existing ignores), two protocol-build tests and 46 engine tests; the external mock and injected consumers passed five plus four tests. Strict all-target/all-feature Clippy, formatting and diff checks passed. The engine test `TestResolveLockWithTiKVSideAsync` now covers admitted and inline cleanup separately: the foreground interceptor observes only inline cleanup, and both paths must clear the locks and expose the correct committed/rolled-back data. Grouped cleanup covers success/error under both admitted and inline workers, merging one completed fork and preserving foreground cancellation scope.

From `/Users/qiliu/projects/client-rust`:

    cargo test --locked --workspace --all-features --lib -- --test-threads=1
    cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all
    cargo fmt -- --check
    git diff --check

Logs are `/private/tmp/resolver-consolidation-workspace-final.log`, `resolver-consolidation-consumers.log`, and `resolver-consolidation-clippy-final.log`. Native implementation edits are confined to `src/transaction/lock.rs`, request-source forwarding in `src/request/plan_builder.rs` and `src/request/shard.rs`, and the engine regression in `src/transaction/integration_lock_source_tests.rs`.

A safe independent removal was possible without the proposed new public API. `rust/crates/tidb-txnkv/src/read_runtime.rs` now owns a clone of the native `ResolveLocksContext`, with no extra registry map, token counter or outer mutex. Its local guard translates observation types and invokes the existing native record/update/done methods; native state remains the single authority. The regression first failed with zero native observations despite two live TiDB entries, then passed after migration. It covers two concurrent slots for the same caller, updates retaining caller identity, and cleanup as each guard drops. All eight shared-read-runtime tests passed (`/private/tmp/tidb-resolver-registry-red.log` and `tidb-resolver-registry-green.log`).

The maintained vendor sync fetched published `75ca650`, applied all four compatibility patches and regenerated protocol artifacts without changing the generated outputs. TiDB behavioral validation is complete as recorded below; the mandatory commit and push build gates are next. The broader API/coprocessor migration remains unapproved and unapplied; no benchmark or whole-package parity claim is made.


### TiDB validation for the accepted repair


The synchronized client and unified registry passed 140 txnkv library tests (one existing ignore), 17 snapshot tests, 15 embedded-client transaction tests, 25 SQL transaction tests and 419 aggregate txnkv tests. The aggregate retains ten existing ignores and the two documented region-cache baseline exclusions from earlier delivery. The targeted coprocessor dispatch suite passed eight tests, including both shared-budget and ignored-hint regressions, but four unrelated routing fixtures failed with `scripted-pd-empty: no region` at `direct_unary_client_fixture.rs:878`. The same command against all five source files restored to unmodified HEAD reproduced exactly the same four failures and eight passes. Task changes were backed up, restored in a finally block, and verified after that comparison. These failures remain open; they are not claimed as passing or silently excluded.

From `rust/`:

    cargo test --locked -p tidb-txnkv --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source -- --test-threads=1
    cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-txnkv --test all -- --skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction
    cargo test --locked -p tidb-distsql --test all direct_unary_dispatch_contract:: -- --test-threads=1

From the repository root, `make lint` and `git diff --check` passed. Logs are `/private/tmp/tidb-resolver-library.log`, `tidb-resolver-snapshot.log`, `tidb-resolver-embedded.log`, `tidb-resolver-sql.log`, `tidb-resolver-all.log`, `tidb-resolver-coprocessor.log`, `tidb-resolver-coprocessor-baseline.log` and `tidb-resolver-lint.log`.

The four coprocessor baseline failures are `client_go_shaped_dispatch_is_lazy_address_directed_and_logically_ordered`, `ordered_regions_retain_logical_range_order`, `unordered_region_window_is_bounded_and_results_are_not_lost` and `unordered_regions_publish_first_completed_response`, all under `direct_unary_dispatch_contract`.

Remaining correctness scope: TiDB's separate recovery algorithms, determined-status cache and cleanup pool require the proposed native public resolver boundary and caller-budget migration. No new facade was applied after automatic approval review rejected it, and its approval question remains pending. Performance benchmarks (sysbench/TPC-C/TPC-H/YCSB), real-cluster faults and complete Go differential execution were not run for this repair. Existing engine and API tests cover the changed retry, cancellation, attribution, observation and cleanup behavior; they do not constitute whole-package parity certification.


### Approved native boundary and caller migration


The user explicitly approved `/private/tmp/native-resolver-api-proposal.md` with “approved, do it” on 2026-09-29. The earlier automatic-review blocker is resolved by that narrower authorization. Continue the approved `txnkv::txnlock` API, caller backoffer ownership, TiDB transport adaptation and removal of the remaining algorithm/cache/pool without requesting the same permission again. The preceding accepted repair is published as client-rust `75ca650` and TiDB `e0c327cbf9`; both TiDB locked-build gates succeeded.

Implementation will expose `LockResolver.resolve_locks_with_opts` over the existing native algorithm, with exact physical-request hints, logical key conversion, source result classification and caller-owned cumulative retry history. The Rust synchronous TiDB caller must share that history without inventing a new budget for each resolution pass. Preserve source cancellation, including accepted background tasks and saturated inline fallback, through the existing bridge. Retire obsolete production code only after the migrated behavioral tests exercise the native boundary. Keep per-reader hint sets, typed error/transport conversion and observation lifetime adapters.

Validation adds public-consumer tests for empty/hinted/mixed locks, shared cache and registry visibility, metadata/cancellation and async-commit cleanup. Reuse the native and TiDB commands above, adding targeted regressions before each behavioral fix. The known six baseline failures remain documented; do not count excluded or baseline-failing tests as passing. No whole-package parity or throughput claim is authorized by compilation alone.

- Published native boundary `448089c` to client-rust master. The empty canceled pass regression failed before its correction and passed afterward (`/private/tmp/resolver-api-empty-red.log`). The native workspace gate passed 1,372 client tests (six existing ignores), two proto-build tests, and 46 engine tests. The downstream gates passed five mock consumers and nine injected consumers, including exact hints, repeated IDs, shared cache/registry, background metadata/lifetime, and synchronous/asynchronous retry ownership. Strict all-target/all-feature Clippy and formatting passed.
- Synced the maintained vendor copy through `bash rust/scripts/sync-tikv-client-rs.sh`; all four patches applied and protobufs regenerated without hand edits.
- TiDB migration in progress: removed the duplicate resolver algorithm, pessimistic cleanup module, async worker module, and status cache. Transport and result adapters now call the native boundary. The synchronous retry adapter now delegates delay selection and completed/interrupted accounting to the same native backoffer; callers still need compilation and behavioral validation. No TiDB completion claim yet.


### Approved migration implementation and evidence


The public native boundary is published on client-rust master in `448089c`, with correctness follow-through in `1053bf6` and `8b890e2`. TiDB now calls that boundary through its existing transport and region-cache authority. `lock/async_resolve.rs` and `lock/pessimistic.rs` are deleted; `lock/resolver.rs` retains only type, error and synchronous-call adapters plus snapshot-local hints. The second determined-status cache and cleanup pool in `read_runtime.rs` are deleted. The native resolver owns cache eligibility, primary metadata, status classification, region cleanup, task admission and shutdown. TiDB's `RegionBackoffBudget` now adapts native prepare/finish/fork operations rather than maintaining another jitter schedule or accounting implementation. The coprocessor passes exact request hints to that resolver and does not charge a duplicate hint backoff. Interrupted synchronous waits record zero completed sleep. The copied metric increment wrappers are removed.

The migration exposed concrete native defects, now covered by failing-then-passing regressions: TTL calculation used the observation before the status RPC; result sets lost per-input decisions when one transaction changed state during a pass; status conversion panicked on a nonzero TTL plus commit version and discarded primary metadata; zero-TTL unconditional resolution unnecessarily consulted PD; and the wait-expired metric lived in one caller rather than the resolver. Determined status and primary metadata now occupy one native cache entry. Status eligibility uses Go's IsRolledBack/IsCommitted predicates; pessimistic rollback actions do not invent a transaction-fate hint. Completed best-effort read cleanup preserves its classification even if cancellation arrives during cleanup, while active RPCs and timestamp acquisition still observe caller cancellation.

An existing cache-availability test reproduced the native transport adapter holding the shared cache lock during EpochNotMatch metadata loading. The bridge now uses BackgroundRegionCache's shared loader outside the cache lock and publishes the hydrated replacements under the lock. The regression is in `/private/tmp/resolver-cache-lock-red.log` and passes in the complete resolver source suite. Nested native retry errors also retain their registered SQL identity through the coprocessor delegate instead of becoming strings; `/private/tmp/resolver-typed-error-red.log` records the regression without that conversion. The fixture migrations use thread-safe ownership and key-addressed secondary responses because Go's per-region workers can complete in either order. The original behavioral assertions remain, with inter-region completion order compared without imposing serialization. Snapshot timestamp-rescoping coverage remains in the adapter's unit tests.

Native commands, from `/Users/qiliu/projects/client-rust`, all pass:

    cargo test --locked --workspace --all-features --lib -- --test-threads=1
    cargo test --locked --test public_injected_client_tests --test mocktikv_transaction_tests -- --test-threads=1
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::all
    cargo fmt -- --check
    git diff --check

The final native library run passed 1,377 client tests with six existing ignores, two protocol tests and 46 embedded-engine tests. The external-consumer suites passed nine public API and five mock transaction tests. Logs are `/private/tmp/resolver-native-protocol-all.log`, `resolver-native-protocol-consumers.log` and `resolver-native-protocol-clippy.log`. Source-shaped public tests cover empty/cancelled input, request hints, exhausted and forked retry histories, mixed locks, shared status cache, source attribution and resolver-owned cleanup cancellation.

TiDB commands from `rust/`:

    cargo test --locked -p tidb-txnkv --test lock_resolver_source -- --test-threads=1
    cargo test --locked -p tidb-txnkv --lib -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source -- --test-threads=1
    cargo test --locked -p tidb-txnkv --test all -- --skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction
    cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-distsql --test all direct_unary_ -- --test-threads=1
    cargo build --locked -p tidb-server

The resolver, library, snapshot, aggregate, embedded and SQL suites passed 32, 133, 17, 419, 15 and 25 tests respectively. The library has one existing ignore; the aggregate has ten existing ignores and the two explicitly excluded baseline region-cache failures. Logs are `/private/tmp/resolver-source-verified.log`, `resolver-library-verified.log`, `resolver-snapshot-verified.log`, `resolver-txnkv-all-verified.log`, `resolver-embedded-verified.log` and `resolver-sql-verified.log`. The locked server build passed; the required pre-commit and immediate pre-push invocations remain separate delivery gates.

The expanded direct-unary suite produced 49 passes and 19 failures. A comparison against all relevant source files restored to unmodified TiDB HEAD reproduced exactly those same 49 passes and 19 failures. The temporary comparison saved and restored every migration file in a finally block. Evidence: `/private/tmp/resolver-distsql-final.log` and `/private/tmp/resolver-distsql-baseline-full.log`. The baseline failures are the four previously recorded dispatch-contract cases; seven `direct_unary_paging_and_close::concurrent` cases (`go_close_joins_rpc_and_response_channel_waiters`, `go_multiple_cop_tasks_start_on_open_and_close_cancels_pending_work`, `go_ordered_send_window_advances_but_stays_bounded_without_next`, `go_ordered_worker_pages_while_the_head_rpc_is_pending`, `go_split_switches_the_lite_reader_to_concurrent_progress`, `one_cop_worker_reuses_its_client_across_regions`, `unopened_region_tasks_do_not_fork_clients`); `direct_unary_query_seed::logical_tasks_in_one_query_share_the_bound_seed`; four `direct_unary_retry_budget` cases (`rebuild_splits_failed_task_in_place_and_keeps_future_task_order_and_attempt`, `region_evicted_after_task_build_rebuilds_ranges_before_any_rpc`, `split_child_region_gets_an_independent_budget`, `unordered_rebuild_replaces_the_completed_region_instead_of_the_first_region`); `direct_unary_store_not_match::shared_proxy_store_not_match_refreshes_only_affected_logical_target`; `direct_unary_store_selection::one_store_failure_stales_later_bound_regions_without_reordering_them`; and `direct_unary_transport_failures::return_region_error_and_non_connection_failures_close_without_future_dispatch`. These are not passing or resolved by this migration.

From the repository root, `bash rust/scripts/sync-tikv-client-rs.sh` regenerated vendor artifacts from the published native commit, and `make lint` passed (`/private/tmp/resolver-vendor-delivery-sync.log` and `resolver-lint-verified.log`). No Go, module or Bazel inputs changed, so bazel_prepare and Go failpoint setup were not applicable. Remaining validation limits are real TiKV/multi-node faults, complete Go differential execution, and sysbench/TPC-C/TPC-H/YCSB performance runs. This removes the approved duplicate owners and validates their integration; it does not certify any complete upstream Go package as transcreated or claim measured performance improvements.


Final caller checks: the direct-unary rerun with the 19 individually confirmed baseline failures excluded passed all 49 remaining tests (`/private/tmp/resolver-distsql-verified.log`; exact invocation saved in `resolver-distsql-verified-command.txt`). `cargo test --locked -p tidb-exec --lib pessimistic_lock_error::` passed 11 cases. `cargo test --locked -p tidb-distsql --test all active_cancellation_source:: -- --test-threads=1` passed two cases and failed `execution_cancellation_interrupts_dispatch_before_all_recovery_and_success_mutation` because its scripted PD loader ran out of regions. The exact command against unmodified HEAD reproduced the same two passes and one failure (`/private/tmp/resolver-cancellation-baseline.log`), so the final known baseline count is 20 coprocessor failures plus the two excluded region-cache cases. This additional result is not omitted from the receipt.

The error-boundary review also added `hinted_resolution_keeps_the_callers_pd_timeout_category`: when a caller's earlier PD wait exhausted its budget, a later lock-hint backoff must retain the PD timeout category. It failed as a generic RPC error before the adapter fix (`/private/tmp/resolver-pd-error-red.log`). Native retry configuration now also supplies category names directly; TiDB no longer duplicates their strings. The complete standalone resolver suite passed 32 tests after that fix; both retry-adapter unit tests and the coprocessor typed-error regression also passed.


### Concurrent upstream integration


Before delivery, fetching the target branch found `07cd21d778` (`perf: reduce native transaction RPC overhead`). The local implementation was rebased onto it. The overlapping bridge change retains upstream's 48-worker runtime, raw native Get publication and observation inside `dispatch_transaction`; lock-only dispatch continues through the shared resolver adapter. The native request bytes and upstream transaction behavior remain intact. Because this affects the transaction transport, repeat the core, snapshot, aggregate, embedded and SQL transaction checks on the combined tree, then amend through the locked build hook and run the immediate pre-push locked build. Baseline comparisons above were made against `e0c327cbf9`, before this concurrent commit.

The combined-tree checks found compatibility regressions in the incoming raw-Get optimization: eight snapshot cases and eleven embedded-store cases failed with `raw transaction transport unavailable`. The optional raw transport now returns `None` when unsupported, and the bridge uses the existing typed command; actual publication failures never trigger a second attempt. The production Tonic implementation retains the raw fast path. Both suites then passed all 17 and 15 cases. A new native-Get observation regression also failed because resolving a scan pair via Get lost its serving-region/value observation; the raw observer now records the same scan observation as typed Get. The encoded-request regression failed because the bridge changed cluster ID/request origin only in its side metadata. The raw encoder now stamps those two fields into the native request while retaining the other native context fields, including lock hints, and avoiding the full request/response compatibility codec round trip. Red logs are `/private/tmp/resolver-rebased-core.log`, `resolver-rebased-embedded.log`, `resolver-native-get-observation-red.log` and `resolver-raw-context-red.log`.

The combined-tree rerun passed 134 txnkv library tests (one existing ignore), all 32 resolver contracts, 17 snapshot tests, 15 embedded-store cases and 25 SQL transaction cases. The added raw-context regression then passed in the focused bridge suite. The aggregate suite also passed 419 tests after the rebase with its same two baseline exclusions and ten ignores. Logs: `/private/tmp/resolver-combined-core-verified.log`, `resolver-combined-embedded-verified.log`, `resolver-combined-sql-verified.log`, `resolver-rebased-txnkv-all.log` and `resolver-raw-contracts-green.log`.


### Completed delivery


Client-rust master contains the three approved-boundary commits through `8b890e2b0e1c40842f91dd865783431cf986a811`. TiDB implementation `dbab768b75` was pushed to `hparser-integration` on top of the concurrent `07cd21d778` performance change. `TERM=xterm git -c core.hooksPath=hooks commit --amend --no-edit` ran the required `cd rust && cargo build --locked -p tidb-server` successfully (`/private/tmp/resolver-tidb-final-commit.log`). A fresh `(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration` then succeeded (`/private/tmp/resolver-tidb-prepush-build.log`). This receipt-only follow-up uses the same commit and pre-push build gates.

The final focused command was `cargo test --locked -p tidb-txnkv --lib driver::client_bridge::ownership_regressions::`; all six cases passed, including raw request metadata, scan observations and foreground/background cancellation. The final `make lint` succeeded in `/private/tmp/resolver-delivery-lint.log`, and `git diff --check` passed. Native master and the TiDB integration are delivered; the baseline failures and unrun real-cluster/differential/performance validations above remain explicit limits. No duplicate TiDB resolver algorithm, determined-status cache, resolving registry, cleanup pool or retry scheduler remains in this approved boundary.
