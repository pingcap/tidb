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
- [ ] Retire the remaining TiDB lock resolver algorithms and duplicate status cache/cleanup pool once their caller contracts are covered.
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
