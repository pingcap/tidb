# Operation lifetime repair

This receipt records maintenance of existing package ports, not acceptance of a
new transcreated package or complete TiDB/client-go parity. TiDB baseline is
0546934c77; native baseline is b2b3783. Both branches were pulled before work.
Go master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go
v2.0.8-0.20260928031501-8edb23f6c7ee. Native repair 488bb73 is published on
ngaut/client-rust master and synchronized through the existing source-only sync
script. Generated protocols were regenerated and produced no tracked changes.

## Root cause and removal

The native rollback algorithm imposed an independent background lifetime on
different callers. Other native completion paths had no explicit scope. TiDB
compensated by guessing lifetime from TxnHeartBeatRequest and a transaction_tasks
client-mode flag. The native RPC client also ignored the scopes already provided
by operation owners. That mixture could both abandon cleanup when a statement
ended and ignore cancellation that should stop work.

The repair removes the heartbeat exception and the client-mode flag from the
TiDB bridge. Native operation owners select the lifetime; region/store task
fan-out preserves it, retry backoffers retain it, and both native and injected
transports observe it. Foreground RPCs and timestamps retain statement
cancellation. Background timestamps use their operation scope, checked before
and after invoking the synchronous timestamp provider.

| Operation | Go source/owner | Native behavior |
| --- | --- | --- |
| Explicit transaction rollback | txn.go rollbackPessimisticLocks, context.Background | Independent rollback cancellation passed explicitly to the shared algorithm |
| Start timestamp for a new transaction | tikv/kv.go KVStore.Begin, context.Background | In-process opener supplies an independent scope; its consumers use the shared opener |
| Mutation initialization failure | txn.go Commit and asyncPessimisticRollback(ctx) | Preserve caller scope; do not use explicit Rollback's background policy |
| Failed commit cleanup | 2pc.go cleanup, store.Ctx | Store-owned cancellation for rollback and retries |
| Pipelined rollback and secondary resolution | txn.go Rollback and pipelined_flush.go commitFlushedMutations/resolveFlushedLocks | Store-owned completion scope |
| Pipelined heartbeat | 2pc.go keepAlive and TTL manager | TTL-manager scope independent of the statement |
| Transaction-file primary cleanup and secondaries | txn_file.go txnFileCleanupContext and executeTxnFileAction | Store-owned primary cleanup and asynchronous secondary scope |
| Physical dispatch | internal/client/client.go SendRequest and client_batch.go | Cancel queued/in-flight work with its operation; keep the shared client usable |

ResolveLock singleflight remains shared: cancelling one logical waiter does not
cancel its shared physical request. Existing BatchCommandSubmission drop cleanup
retires cancelled entries without closing the shared stream. This repair adds no
new wire commands, retry limits, transaction protocols, or SQL behavior.

## Regression and validation evidence

Native regressions failed before production edits for cleanup/store lifetime,
missing secondary and pipelined TTL scopes, and detached initialization rollback.
Separate transport regressions failed for cancelled admission and in-flight
cancellation. TiDB regressions failed when an explicit heartbeat bypassed
statement cancellation and a foreground timestamp ignored it. Corrected test
fixtures use valid pipelined concurrency and identify the primary by region.

Commands from /Users/qiliu/projects/client-rust:

    cargo test --locked --lib rpc_lifetimes
    cargo test --locked --lib source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedCommit
    cargo test --locked --lib source_mutation_initialization_error_rolls_back_pessimistic_prefix
    cargo test --locked --lib source_rpc_
    cargo test --locked --lib -- --test-threads=1
    cargo test --locked --lib source_cleanup_preserves_variables_and_cancellation
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The full native suite passed 1,405 tests, with two ignored. The final retry-owner
assertion passed separately; strict Clippy and formatting passed. Red and green
logs are /private/tmp/native-lifetime-*.log.

Commands from TiDB rust/:

    cargo test --locked -p tidb-txnkv --lib ownership_regressions
    cargo test --locked -p tidb-txnkv --lib --test all --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    cargo test --locked -p tidb-txnkv --test all --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-unistore --test client_transaction

The bridge scope passes nine tests. The library passes 140 tests, with one ignored.
The first integration invocation hit sandbox-denied localhost listeners; rerunning
with local network access passes 423 integration tests, with ten ignored. Region
recovery passes 26 tests, final snapshot scopes pass 17 and two tests respectively,
SQL transactions pass 28, and embedded transactions pass 15. The opener regression
first failed through the real opener, then passed after the independent Begin
lifetime was supplied. Its test helper no longer constructs a parallel opener.

Two resolver cases fail identically on baseline and changed code (31 pass):
caller_cancelled_status_rpc_is_typed_and_lite_cleanup_is_best_effort and
read_lite_cleanup_errors_do_not_replace_a_determined_status. The former returns
Rpc("context canceled") rather than CallerCancelled; the latter returns a
determined status rather than the expected cancellation. These are not counted
as passes. Baseline comparison restored all changed production files with a
finally guard: /private/tmp/tidb-lifetime-resolver-baseline.log.

batch_get_split_retries_run_children_independently failed in both the first
changed run and the complete baseline snapshot suite, but passed alone on
baseline and in the final changed suite. This is recorded as an intermittent
test, not a fixed defect. Logs: /private/tmp/tidb-lifetime-snapshot-baseline.log,
/private/tmp/tidb-lifetime-snapshots-baseline-full.log and
/private/tmp/tidb-lifetime-snapshots-final.log.

Root make lint passed. No Go/import/module/Bazel inputs changed, so bazel_prepare
and Go failpoint mutation were not required. Native generated artifacts were
synchronized, not hand-edited. Logs are /private/tmp/tidb-lifetime-*.log.

Publication commands from the repository root:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: remove RPC lifetime heuristics and use native operation owners'

The hook must execute cargo build --locked -p tidb-server from rust/. After
commit, run that same command again from rust/, then git push origin
HEAD:hparser-integration. The hook and fresh pre-push logs are
/private/tmp/tidb-lifetime-commit.log and /private/tmp/tidb-lifetime-prepush.log.

Changed code is client_bridge.rs and tikv_opener.rs in tidb-txnkv,
tidb-unistore/tests/client_transaction.rs, and the synchronized native
async_util.rs, store/client.rs, transaction/transaction.rs and existing 2PC
source tests. The remaining changes are living plans, this receipt, structural
finding status and the dependency sync log.

## Limits and remaining work

T02's competing region/routing/RPC implementations remain until every DistSQL
consumer is migrated. The synchronous timestamp provider cannot be interrupted
mid-call by this adapter; it observes cancellation before dispatch and on return.
A complete foreground-context capture and shutdown audit remains open.

No real TiKV cluster, original Go test suite, complete feature matrix, full Rust
workspace, or sysbench/TPC-C/TPC-H/YCSB benchmark was run. The explicit background
scope adds a cancellation lookup/select at native dispatch; foreground requests
keep the direct path. No throughput claim is made. The completed native build's
disposable incremental cache was removed, recovering about 25 GiB of free disk.
