# Resolver and retry error identity repair

## Scope and source

This continuation refreshes TiDB hparser-integration at 25996f291eec8196f9d6fd82dd933320bddb2734 and client-rust master at 488bb73880231b0452e619817268144b5a2f4ad9. Both pulls were already current. Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb, pinning client-go v2.0.8-0.20260928031501-8edb23f6c7ee. This is maintenance of existing ports, not whole-package acceptance or a claim that all structural mismatches are resolved.

Authoritative source is the pinned module's config/retry/config.go (BoPDRPC), config/retry/backoff.go (BackoffWithCfgAndMaxSleep), txnkv/txnlock/lock_resolver.go (batchLiteResolveLocks), and internal/locate/region_request.go (caller cancellation). Retry exhaustion selects its configured terminal error; for PD this is NewErrPDServerTimeout(""). The original retry diagnostic stays in the backoff history. Already-cancelled/noop backoffs instead return the triggering error, so blanket conversion of RetryError::Cancelled into ContextCanceled would be incorrect.

Go's read cleanup suppresses errors in both asynchronous dispatch and caller-owned fallback. After status determination, cancellation may prevent cleanup dispatch but must not discard the determined read hints. Foreground status lookup errors still propagate. Remote gRPC cancellation without caller cancellation remains a retryable transport failure.

## Removed policies

ClientKv's foreground fast path stringified an already-native error. Its background worker retained that error. Removing the extra conversion preserves cancellation and transport identity for all twelve supported RPC types without changing dispatch scheduling. A string whose contents happen to be "context canceled" remains an ordinary string error.

Native tikv.rs maintained a second RetryError converter for four split/scatter paths. Its PD terminal differed from common/errors.rs. The duplicate is removed and all four callers use Error::from; the common conversion now returns Go's configured PD error. No retry budget or cancellation lifetime changes.

The existing read_lite_cleanup_errors_do_not_replace_a_determined_status test incorrectly expected cancellation after successful status lookup to replace the result. Its expectation now follows Go, while still checking that no cleanup RPC is dispatched after caller cancellation.

## Regression evidence

Before the fix, caller_cancelled_status_rpc_is_typed_and_lite_cleanup_is_best_effort returned Rpc("context canceled") instead of CallerCancelled. The added bridge regression independently failed on Get with StringError instead of ContextCanceled. It covers all twelve RPC types, foreground/background scopes, typed cancellation, identical ordinary error text, and remote gRPC cancellation code/message. Both pass after removing the conversion.

The extended native pd_timeout_remains_a_distinct_terminal_error_class test failed before the fix: the native terminal message was "PD unavailable" rather than the configured empty string. It passes after correction and additionally verifies that "PD unavailable" remains in retry history.

Red logs: /private/tmp/tidb-resolver-identity-red.log, /private/tmp/tidb-rpc-identity-red.log, /private/tmp/native-retry-identity-red.log. Targeted green logs use the corresponding -green suffix. The complete TiDB resolver suite now passes all 33 tests; the bridge scope passes ten tests.

## Validation and publication

From /Users/qiliu/projects/client-rust:

    cargo test --locked --lib pd_timeout_remains_a_distinct_terminal_error_class
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

Native b23c6d37b92312ca34bad76b0bfac7e847cd3d6f is committed and pushed to ngaut/client-rust master; remote lookup confirmed the exact revision. The full library passed 1,405 tests with two ignored; strict Clippy, formatting and diff checks passed. Logs are /private/tmp/native-retry-identity-{all,clippy}.log. TiDB was then synchronized with bash rust/scripts/sync-tikv-client-rs.sh. Its four maintained transport compatibility patches applied and protobuf outputs were regenerated with no generated-file diff. Synchronization log: /private/tmp/tidb-resolver-identity-sync.log.

From TiDB rust/:

    cargo test --locked -p tidb-txnkv --lib ownership_regressions
    cargo test --locked -p tidb-txnkv --test lock_resolver_source

After synchronization, the following commands passed from rust/:

    cargo test --locked -p tidb-txnkv --lib --test all --test lock_resolver_source --test snapshot_lock_wait_source --test snapshot_scan_page_deadline_source --test region_error_recovery_source
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    cargo test --locked -p tidb-unistore --test client_transaction

Results respectively: transaction library 141 passed/one ignored; transaction integration 423 passed/ten ignored; resolver 33, region 26, snapshot wait 17, snapshot deadline two, SQL transaction 28, embedded transaction 15 passed. Total 685 passed, 11 ignored. No failures remain in these scopes. The previously intermittent snapshot split test passes in this run; this repair does not claim to fix its scheduling sensitivity. Logs: /private/tmp/tidb-resolver-identity-{suites,sql,unistore}.log.

Root make lint passed, including protocol freshness and generated input checks; log /private/tmp/tidb-resolver-identity-lint.log. Formatting and git diff --check passed. Rust-only changes do not require bazel_prepare or Go failpoint mutation.

Publication uses the actual hook, from the TiDB root:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: preserve native RPC errors and share Go retry terminal policy'

That hook must pass cd rust && cargo build --locked -p tidb-server. After commit, rerun cargo build --locked -p tidb-server from rust/ and push only after success with git push origin HEAD:hparser-integration. Gate logs: /private/tmp/tidb-resolver-identity-commit.log and /private/tmp/tidb-resolver-identity-prepush.log. Do not substitute the earlier test compilation for either build gate.

Changed production files are tidb-txnkv/src/driver/client_bridge.rs, and synchronized native src/common/errors.rs and src/tikv.rs. Tests extend client_bridge.rs, lock_resolver_source.rs and native src/retry.rs. Remaining changes are dependency sync metadata, living plans, this receipt and the structural register.

## Limits

T02's competing region/cache/recovery/RPC ownership remains open; deleting its TiDB implementation before migrating DistSQL consumers would break required behavior. Foreground context capture, shutdown, and retry-trigger error retention beyond these converters still need package-level review. No full Rust workspace, original Go test suite, real TiKV deployment, complete feature matrix, or sysbench/TPC-C/TPC-H/YCSB benchmark is claimed. The change removes formatting/allocation on error paths; throughput is unmeasured.
