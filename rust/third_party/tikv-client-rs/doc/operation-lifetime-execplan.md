# Explicit transaction RPC ownership

## Purpose and source

TiDB should not infer operation cancellation lifetime from a wire request type. Follow pinned client-go v2.0.8-0.20260928031501-8edb23f6c7ee: txnkv/transaction/txn.go rollbackPessimisticLocks and asyncPessimisticRollback, 2pc.go secondary completion and TTL manager. This repairs the existing native package; it makes no new package-completion or full-parity claim. Existing package inventories retain source/test/build evidence.

## Progress

- [x] Reproduce missing rollback, secondary, and TTL owner scopes at the injected RPC boundary.
- [x] Scope explicit transaction rollback independently from statement rollback.
- [x] Give ordinary/async secondary completion the shared client lifetime and cancellation.
- [x] Give automatic heartbeat a background lifetime; explicit heartbeat remains caller-owned.
- [x] Carry an existing operation scope across region and store task fan-out.
- [x] Expose the existing cleanup-pool setting so workerless injection can select zero concurrency without closing its owner.
- [x] Run all library tests and strict Clippy.
- [x] Reconcile pipelined and transaction-file completion scopes, compensating cleanup, and initialization rollback; validate both injected and native transports.
- [x] Publish the operation-lifetime milestone (488bb738) and refresh TiDB (25996f29).

## Decisions and discoveries

Tokio task locals do not propagate to spawned region/store workers. Capture the existing optional scope before spawn; never detach a foreground task just because it runs concurrently. The multi-region source regression failed even after secondary completion was scoped, exposing this second boundary. Explicit statement pessimistic rollback preserves the caller scope, matching Go asyncPessimisticRollback(ctx).

A resolver closed to disable asynchronous cleanup cannot also serve as a live transaction's background owner. Workerless TiDB injection must configure zero cleanup concurrency instead of calling close. The existing pool-size setter is now public for this construction boundary; it must be called before sharing the context.

Automatic approval review rejected a broad scripted rewrite of the remaining five background paths. A concrete patch was prepared at /private/tmp/client-rust-background-lifetimes.patch. That earlier milestone did not include it. On 2026-10-01 the user renewed the instruction to remove the extra policies in both repositories; the continuation below rederives the change from Go instead of applying the old patch wholesale.

## Validation and outcome

Red evidence: cleanup ownership failed in two tests, secondary ownership failed in multi-region commit, and the automatic TTL manager lacked an owner. After the changes, all 1,401 library tests passed; two ignored. Resolver tests after the visibility change: 42 passed. Strict Clippy passed. Exact commands:

    cargo test --locked --lib source_cleanup_retries_ignore_query_kill
    cargo test --locked --lib source_standard_actions_tag_each_physical_batch_and_cleanup_primary_first
    cargo test --locked --lib source_ttl_manager_sends_live_min_commit_ts_and_txn_file_marker
    cargo test --locked --lib -- --test-threads=1
    cargo test --locked --lib transaction::lock::tests
    cargo clippy --locked --lib -- -D warnings

Logs: /private/tmp/native-lifetime-*.log, /private/tmp/native-secondary-lifetime-*.log, /private/tmp/native-heartbeat-lifetime-*.log. Real TiKV, shutdown fault injection, the complete feature matrix, and benchmarks were not run. No performance or complete-parity claim is made.

## Continuation: operation owners and transport (2026-10-01)

Baseline b2b3783 was refreshed from origin/master. TiDB master
93a01d31f6da205ae4bf376825293903a6899fdb still pins client-go 8edb23f6c7ee.
Production spawn/cleanup review covered txn.go Rollback, initialization failure
and asyncPessimisticRollback, 2pc.go cleanup/keepAlive, pipelined_flush.go
commitFlushedMutations/resolveFlushedLocks, and txn_file.go failed-primary
cleanup and asynchronous secondaries. This is a maintenance repair of existing
package ports, not a new package acceptance claim.

The native rollback algorithm now receives an explicit cancellation owner; it
no longer assigns independent background cancellation to every caller. Explicit
Rollback selects an independent lifetime. Failed-commit and pipelined rollback
select the shared store lifetime. Initialization failure retains the caller's
scope, as asyncPessimisticRollback(ctx) requires. Transaction-file primary cleanup
and both pipelined/transaction-file secondaries carry store-owned scopes. The
pipelined TTL manager carries its own cancellation. Retry owners inherit the
selected operation scope without changing explicit retry limits.

Transport review found that these scopes reached injected TiDB transports but
were not observed by native KvRpcClient. The native dispatch boundary must
observe the scope for both unary and BatchCommands calls, including cancellation
while a request is in flight. Dropping the batch submission already marks its
entry cancelled, preserving the existing shared stream. Collapsed ResolveLock
waiters must cancel independently; the shared physical request retains its Go
singleflight lifetime. Source: internal/client/client.go SendRequest and
internal/client/client_batch.go request cancellation.

Regression evidence and final validation will be recorded before publication.
TiDB can remove ClientKv's heartbeat exception once these scopes are synchronized.
The TiDB continuation also removes ClientPd timestamp classification after its
regression reproduces the wrong foreground/background lifetime. A complete
shutdown/foreground-context audit remains outside this milestone.

Validation: baseline regressions failed for store cleanup ownership, absent
secondary/TTL scopes, initialization cleanup detachment, and native queued/
in-flight RPC cancellation. The corrected fixtures then passed. Final commands:

    cargo test --locked --lib rpc_lifetimes
    cargo test --locked --lib source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedCommit
    cargo test --locked --lib source_mutation_initialization_error_rolls_back_pessimistic_prefix
    cargo test --locked --lib source_rpc_
    cargo test --locked --lib -- --test-threads=1
    cargo test --locked --lib source_cleanup_preserves_variables_and_cancellation
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The full suite passed 1,405 tests, with two ignored. The final retry-owner
assertion passed separately; strict Clippy passed. Logs are
/private/tmp/native-lifetime-{owner,ttl,init,transport}-red.log,
/private/tmp/native-lifetime-all-green.log, /private/tmp/native-lifetime-retry-green.log,
and /private/tmp/native-lifetime-clippy.log. No real cluster, benchmark, original
Go test run, complete feature matrix, or full shutdown acceptance was performed.
The per-RPC scope lookup/select adds a small cost only to explicitly scoped
background work; foreground dispatch keeps its direct future path. No throughput
claim is made. The TiDB dependency refresh follows publication of this commit.


## Retry conversion consolidation (2026-10-01)

The continuation starts at published 488bb738 with a clean tree and the same
client-go pin. Inspection of config/retry/config.go BoPDRPC and
BackoffWithCfgAndMaxSleep confirms that retry exhaustion returns the configured
NewErrPDServerTimeout(""). Native common/errors.rs incorrectly substitutes the
triggering diagnostic, while tikv.rs has a separate converter using Go's empty
message. Remove the duplicate converter and route all split/scatter terminal
errors through the corrected common conversion. Diagnostics remain in backoff
history. Cancelled/noop backoffs must keep their triggering reason; do not
replace every retry cancellation with the ContextCanceled sentinel.

Extend the existing PD terminal regression and prove it fails before the fix.
Then run cargo test --locked --lib -- --test-threads=1, cargo clippy --locked
--lib -- -D warnings, cargo fmt --all --check and git diff --check. Publish to
master, then refresh TiDB with its maintained sync script. This is a bounded
repair of existing ports, not acceptance of any complete upstream package.


The PD-terminal regression failed before the production repair (message
"PD unavailable" instead of "") and passed afterward, with the diagnostic
retained in history. The complete native library passed 1,405 tests with two
ignored. Strict Clippy, formatting and diff checks passed. Logs are
/private/tmp/native-retry-identity-{red,green,all,clippy}.log. The only production
changes are the common PD-terminal correction and removal of tikv.rs's duplicate
converter; all four split/scatter consumers now use Error::from. Real-cluster,
original Go tests, benchmarks and the complete feature matrix were not run.
