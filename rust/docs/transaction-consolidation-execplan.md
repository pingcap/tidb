# One transaction engine for TiDB Rust

This living ExecPlan follows `PLANS.md` and supersedes the transaction-engine
remainder of `client-rust-migration-execplan.md`. This is integration and retirement
work; it does not claim complete transcreation of any Go package or external
module. The wider source inventory remains in
`audits/structural-20260929/inventory.json.gz` and the vendor package receipts.

## Purpose and acceptance

TiDB master `pkg/store/driver/txn` wraps client-go's `KVTxn`; it does not own a
second prewrite/commit/rollback, snapshot, pessimistic-lock, or heartbeat engine.
Route Rust SQL and internal transaction callers through the vendored client's
transaction and MemDB, using the same driver for remote TiKV and embedded
unistore. Keep SQL statement retries, schema validation, error conversion and
lock-value caches at their Go ownership boundaries. Delete duplicate algorithms.

## Source anchors and preflight

- TiDB master: `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
- Pinned client-go: `v2.0.8-0.20260928031501-8edb23f6c7ee`.
- Vendored client-rust: `32dec1837ee9686f1a32861a0b40f2ed880be3c7`
  plus the maintained patch set. Patch 130 records this integration's changes.
- Starting integration branch: `1e90a167be77364693deb9cd91449eb0ed64c73c`.
  Fetched master and integration first. A final fetch found two independent
  changes; fast-forwarded to `de6802c64b` without overlapping edits.
- Read root AGENTS, PLANS, change-instruction-critic, test-guidelines and
  bazel-prepare-gate. The Go driver txn package has no `doc.go`.
- No Go, Bazel or Go module inputs changed; main checkout needs no
  `make bazel_prepare`. The isolated baseline checkout did attempt it under the
  fresh-workspace rule, but Bazel is unavailable. Cargo baseline checks ran.

## Progress

- [x] Inventory production callers, compatibility contracts and duplicate owners.
- [x] Establish failing ownership and behavioral regressions before integration.
- [x] Validate injected client adapters before switching production callers.
- [x] Move SQL and internal writers to the native engine and authoritative MemDB.
- [x] Remove the old commit, prewrite, cleanup, snapshot, region batching,
  heartbeat and transaction mutation-buffer implementations.
- [x] Preserve source assertion, cancellation, schema, retry, and lock lifetimes.
- [x] Verify the maintained vendor patch reproduces every changed vendor file.
- [x] Run native, adapter, SQL and internal-writer validation; distinguish
  unrelated baseline failures using an isolated original-HEAD checkout.
- [x] Finish focused checks and self-review.
- [x] Prepare the atomic integration change for the required delivery gates:
  commit through `hooks/pre-commit`, then run a fresh locked server build before
  pushing `hparser-integration`. Execution results are recorded in the commit
  command log and the final delivery response.

## Design and integration decisions

`driver/client_bridge.rs` adapts the process-owned transport, region cache and PD
capability to the native client's `PdClient`/`KvClient` interfaces. It translates
protobuf types and publication evidence; it does not select transaction phases,
retry requests, resolve locks, or maintain TTL. A process-wide Tokio runtime
supports the synchronous SQL driver. Detached cleanup/secondary commit/heartbeat
RPCs retain their own contexts after a foreground statement ends. Region
replacement is atomic, and store invalidation reaches the existing shared cache.

`transaction/client.rs` is the TiDB facade over `TikvTransactionDriver`, which
owns exactly one native transaction. Old public optimistic/pessimistic names are
compatibility aliases, not alternative engines. Native callbacks supply schema
checks and commit-mode evidence. SQL error conversion preserves native typed
retry sentinels rather than interpreting message strings.

`tidb-executor::MutationBuffer` is a forwarding handle to that transaction's
MemDB. Before lazy transaction creation, its native MemDB moves into the opened
transaction. SQL retains duplicate-key text and statement delta metadata; native
staging handles own value/flag rollback. Completed statement scopes are released
so a long transaction does not accumulate unused staging scopes. Savepoints keep
their outer scope. `MutationPlan` is only a transient statement-planning overlay;
it no longer coalesces or owns transaction-wide mutations.

SQL adapters explicitly forward start/retry/done/cancel fair-locking hooks.
Ordinary statement rollback retains ordinary pessimistic locks, as Go does.
Transaction rollback happens before clearing SQL's buffer, preserving the
client's acquired-lock flags until cleanup is scheduled. Point prelocks carry
the session's lock-wait budget, just like range locks.

## Discoveries and root fixes

- The previous migration had introduced a native driver but production still
  used the old coordinator. Removing only the unused driver would preserve the
  structural mismatch.
- Native locking must receive the statement's `for_update_ts`; zero selected
  optimistic bookkeeping and skipped locking RPCs.
- Lock-returned values are at `for_update_ts`, so they must populate SQL's lock
  cache, not replace cached snapshot values at `start_ts`.
- Go table assertions preserve the first assertion. An insert followed by an
  update must retain `NotExist`; generic MemDB flag updates remain unrestricted.
- Pessimistic lock resolution must reissue recently refreshed holders and check
  NOWAIT/wait budgets after resolving a positive remaining TTL. Default retry
  ownership must remain valid when promoting optimistic state to pessimistic.
- The native mock fabricated a 1 ms refresh age on every lock error, causing
  endless skip-resolution retries. Go unistore retains the observed lock
  metadata. Its normal shared/deadlock wake-ups return WriteConflict even after
  rollback (`tikv/server.go:KVPessimisticLock`), confirmed against the exact pinned
  Go `integration_tests/shared_lock_test.go` and `lock_test.go`. Existing conflict
  assertions were retained; the mock response was corrected.
- Native retry wrappers flattened typed terminal errors to strings. Preserve
  those types so SQL keeps TiKV timeout, resolve-lock timeout and region
  unavailable identities. The regression failed with
  `StringError("tikv server timeout")` before the fix and passed afterward.
- Existing test fixtures failed compilation independently of this refactor:
  seven executor table resolver fixtures omitted `clause_message`, and an analyze
  test compared the new `(rate, reason)` tuple to a scalar. Updated the fixtures
  so affected-package tests can compile; no corresponding production changes.

An early automatic approval review rejected an unverified broad replacement.
That command did not run. The bounded adapter/facade switch proceeded after
isolated compile and embedded regression evidence.

## Validation evidence

Commands below run from `rust/` unless a repository-root command is stated.

- `cargo test --locked -p tidb-txnkv --test all live_transaction_has_one_protocol_owner -- --nocapture`
  failed against the duplicate owner, then passed after retirement.
- `cargo test --locked -p tidb-txnkv --test all -- --nocapture`:
  419 passed, 10 ignored, two region-cache failures. Both also failed on original
  HEAD in the isolated baseline checkout. Final run used the same command with
  `--skip region_cache_source::stale_merge_parent_does_not_evict_newer_split_child
  --skip region_cache_source::stale_same_region_loader_result_is_rejected_without_eviction`:
  419 passed, zero failed.
- `cargo test --locked -p tidb-txnkv --lib transaction -- --nocapture`:
  10 passed, including typed SQL backoff conversion and publication contracts.
- `cargo test --locked -p tidb-unistore --test client_transaction -- --nocapture`:
  15 passed. Covers first-committer-wins, statement cleanup, fair cancellation,
  schema errors, native scans, fast commit, and ambiguous primary results.
- `cargo test --locked -p tidb-executor --lib cluster_storage::tests -- --nocapture`:
  16 passed, including native checkpoint release and rollback behavior.
- `cargo test --locked -p tidb-exec --lib multi_statement_transaction::tests -- --nocapture`:
  11 passed.
- `cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions -- --nocapture`:
  25 passed.
- `cargo test --locked -p tidb-server --lib cluster_session_node::tests::unistore_cop -- --test-threads=1`:
  105 passed, ten baseline failures below. Original HEAD had these ten plus
  `unchanged_updates_lock_only_matched_rows`; that lock-wait test now passes.
  Final rerun excluded only the ten independently reproduced baseline failures: 105 passed, zero failed.
- `cargo test --locked --manifest-path third_party/tikv-client-rs/Cargo.toml --lib -- --test-threads=1`:
  1,369 passed, two ignored. Serial execution avoids shared failpoint/configuration
  interference observed in a parallel run. Includes native buffer, retry,
  transaction, shared-lock, deadlock and mock-server tests.
- Repository root `make lint`: passed, including the final rerun.
- `git diff --check`: passed.
- Applied patch 130 to previous vendored sources in a temporary directory;
  `git apply --check` passed and all seven changed files matched byte-for-byte.

The ten existing SQL failures are:
`add_partition_statistics_follow_global_prune_mode_like_go`,
`cluster_info_reports_this_node`, `drop_partitions_statistics_match_go`,
`exchange_partition_validates_and_swaps_real_rows_atomically`,
`global_stats_drive_partition_plans_like_go`,
`partition_scoped_analyze_refreshes_global_count_and_modify_count`,
`stats_notifier_uses_a_real_internal_transaction_like_go`,
`system_table_ddl_does_not_publish_statistics_events_like_go`,
`truncate_hash_partition_statistics_match_go`, and
`truncate_partitions_refreshes_global_stats_meta_like_go`.

Logs are in `/private/tmp/tidb-{txnkv-clean,transaction-unit-final,unistore-final2,
internal-writer-final3,sql-mock-final2,sql-clean,native-client-serial3}.log`.
Baseline evidence is in `/private/tmp/tidb-baseline-sql.log`; the managed baseline
worktree was archived after tests ended to reclaim its checkout.

## Risks and boundaries

This removes competing transaction state machines and buffers; it does not
assert full Go-package parity. SQL statement semantics and async transaction
cleanup are the principal compatibility risks, covered by the focused tests.
Native client and TiDB PD/region/transport capabilities still meet through an
adapter. Broad region-cache/transport migration remains separate work.

Real TiKV, multi-node fault injection, full Go differential suites and
sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. No performance improvement is
claimed. Generated protobuf outputs were not edited. Rollback of this change
would require reverting the complete integration commit, including its vendor
patch; restoring only the removed coordinator would create duplicate owners.
