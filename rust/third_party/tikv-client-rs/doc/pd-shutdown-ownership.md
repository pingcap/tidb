# Explicit native PD shutdown ownership

This is maintenance of the existing PD/region-cache owners, not acceptance of
the complete upstream PD root, discovery, TSO or client-go locate packages.
TiDB master 93a01d31f6da205ae4bf376825293903a6899fdb selects client-go
v2.0.8-0.20260928031501-8edb23f6c7ee and PD client
v0.0.0-20260805103528-afa43111d149. The PD root has no doc.go.

Go `tikv/kv.go::KVStore.Close` joins region-cache work before closing TiKV and
PD. `internal/locate/region_cache.go::bgRunner.shutdown` cancels the task context
and waits. PD `client.go::Close` calls `inner_client.go::close`, which cancels and
joins the owner, closes the TSO client, then discovery connections.
`clients/tso/client.go::Close` waits for workers and closes the dispatcher.

Native `PdRpcClient::close` now completes the same existing-owner chain.
An async OnceCell distinguishes shutdown admission from completion: concurrent
callers wait, and cancellation of one caller permits another to finish.
RegionCache retains a shared join future, serializes task registration with
shutdown, and applies its cancellation to built-in background RPC waits. No
extra thread or shutdown service is introduced. TiKV admission closes first;
cache workers join before pooled clients are retired and RetryClient closes.

RetryClient cancels its request/retry/discovery lifetime before waiting for the
reconnect lock. Close and leader publication use that same lock. Cluster keeps
its identity but releases its metadata/keyspace connections, cancels its TSO
manager, and joins both current and retired streams. Retired stream handles
remain in Cluster until the join completes, so dropping a reconnect or close
future cannot discard the join. Every RPC constructor still returns an owned
static future; no cluster guard spans a network or stream-retirement wait.
Retained PD RPC/TSO handles fail promptly after close and cannot reconnect the owner.
Custom pool-only close adapters now require the existing RetryClientTrait;
its default close is appropriate only for adapters with no owned PD services.

Four failures were observed before their fixes: pending TSO survived close;
concurrent close returned before cache completion; interrupted close discarded
unfinished cache joins; and a cache-owned PD RPC blocked close before PD could
be closed. Six bounded local transport regressions also cover cancelled
metadata/discovery and interruption after leader publication but before retired
stream completion. Existing logical retry metrics, retry budgets, healthy-stream
reuse, replacement and request-concurrency tests remain unchanged.

Validation from /Users/qiliu/projects/client-rust:

    cargo test --locked --lib source_pd_shutdown_
    cargo test --locked --lib pd::
    cargo test --locked --lib region_cache::test
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The focused candidate passed six shutdown tests, 157 PD tests and 89 cache
tests in /private/tmp/client-rust-pd-shutdown-review before exact patch
application. After applying the reviewed patch, all 1,534 native library cases pass (two
existing cases ignored), as do strict library Clippy, all-target compilation,
formatting and diff checks. Full native results are recorded in the TiDB integration receipt
rust/docs/pd-shutdown-ownership-execplan.md. Logs use
/private/tmp/native-pd-shutdown-{red,cache-red,review*,all,clippy,check}.log.

Original Go verification copies the pinned module to /private/tmp/pd-shutdown-go,
makes only that copy writable, and runs from that directory:

    bash -c 'set -e; trap '\''/Users/qiliu/projects/tidb/tools/bin/failpoint-ctl disable . '\'' EXIT; /Users/qiliu/projects/tidb/tools/bin/failpoint-ctl enable .; GOTOOLCHAIN=go1.25.14 go test -race . -run "^(TestClientCtx|TestClientWithRetry)$" -count=1; GOTOOLCHAIN=go1.25.14 go test -race ./clients/tso -count=1'

Both source commands pass with race detection and original TestMain leak checks.
The source module cache is unchanged. /private/tmp/go-pd-shutdown.log retains
results. These are original source tests and local native mock-RPC tests, not a
live PD failover, service-mode switching, cross-platform or workload benchmark
claim. P03 and the broader P06/root/discovery/TSO obligations remain open.
