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
- [ ] Reconcile the remaining pipelined and transaction-file completion scopes, and compensating cleanup store lifetime.
- [ ] Publish this verified milestone and refresh TiDB.

## Decisions and discoveries

Tokio task locals do not propagate to spawned region/store workers. Capture the existing optional scope before spawn; never detach a foreground task just because it runs concurrently. The multi-region source regression failed even after secondary completion was scoped, exposing this second boundary. Explicit statement pessimistic rollback preserves the caller scope, matching Go asyncPessimisticRollback(ctx).

A resolver closed to disable asynchronous cleanup cannot also serve as a live transaction's background owner. Workerless TiDB injection must configure zero cleanup concurrency instead of calling close. The existing pool-size setter is now public for this construction boundary; it must be called before sharing the context.

Automatic approval review rejected a broad scripted rewrite of the remaining five background paths. A concrete patch was prepared at /private/tmp/client-rust-background-lifetimes.patch and user approval is pending in the TiDB task. Those proposed changes are not included in this milestone. The existing TiDB adapter must retain any heuristic still required by an unconverted path until that follow-up is validated.

## Validation and outcome

Red evidence: cleanup ownership failed in two tests, secondary ownership failed in multi-region commit, and the automatic TTL manager lacked an owner. After the changes, all 1,401 library tests passed; two ignored. Resolver tests after the visibility change: 42 passed. Strict Clippy passed. Exact commands:

    cargo test --locked --lib source_cleanup_retries_ignore_query_kill
    cargo test --locked --lib source_standard_actions_tag_each_physical_batch_and_cleanup_primary_first
    cargo test --locked --lib source_ttl_manager_sends_live_min_commit_ts_and_txn_file_marker
    cargo test --locked --lib -- --test-threads=1
    cargo test --locked --lib transaction::lock::tests
    cargo clippy --locked --lib -- -D warnings

Logs: /private/tmp/native-lifetime-*.log, /private/tmp/native-secondary-lifetime-*.log, /private/tmp/native-heartbeat-lifetime-*.log. Real TiKV, shutdown fault injection, the complete feature matrix, and benchmarks were not run. No performance or complete-parity claim is made.
