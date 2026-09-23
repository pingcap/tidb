# Pinned client-go txnkv/txnlock package inventory

Open whole-package dependency unit for the coprocessor/snapshot audit in
rust/docs/operations/store-copr-audit-execplan.md. Pin:
client-go/v2 v2.0.8-0.20260921040125-5f38569c8cc0, selected by TiDB master
bb80c86a127b579a93c2070a7f3464ef1b609e38. This is an inventory and bounded
seed evidence; the package is not transcreated.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `lock.go` | 43 | `4157ffa6a50f11540424c847e57f2922a4b112829e493a71f769f55697734694` | tidb-txnkv/src/lock/model.rs; shared/ordinary lock decoding |
| `lock_resolver.go` | 1777 | `d7018971ec92613445bbdb5a7ebad3214bcb63f6a52e7becf9a83952eea2da91` | tidb-txnkv/src/lock/resolver.rs and pessimistic.rs; partial, complete resolver lifecycle/options/cache/metrics remain open |
| `lock_resolver_test.go` | 160 | `db4a4a1786abacef8cc3965b587bd628103e3bd541e2c55e33fa634a3c054ddc` | tidb-txnkv/tests/lock_resolver_source.rs and pessimistic_lock_source.rs; original test reconciliation remains open |
| `lock_test.go` | 63 | `7061b234c5feab7f1bba1b2479ff36517702a1c7a3e65e32261bc85dc6c40bf1` | tidb-txnkv/tests/lock_model_source.rs; original test reconciliation remains open |
| `main_test.go` | 25 | `5c53aa90d1afd98d5dd5ad42eee7897264c8b9abffda6a3a6632473748d39cfd` | Cargo harness; Go testsetup/goleak gate remains separately required |
| `test_probe.go` | 129 | `380a38d0f35589f567edd2a052d702aa7d9b3b498618f919cc8f8e46844c354d` | Go test support; existing Rust scripted client seams, full correspondence open |

All six package artifacts are listed. There is no doc.go, nested fixture,
platform/build-tag variant, generated output/input or package-local Bazel file
in this module package. test_probe.go is production-compiled test support and
remains part of the inventory. Module build/license inputs are pinned below;
TiDB go.mod/go.sum and DEPS.bzl govern the consuming build.

| Module input | SHA-256 |
| --- | --- |
| `go.mod` | `4e7e04c1183bcd6ad33ec5afb44725bd93f94a92d101127a47f476c02c7bd805` |
| `go.sum` | `4037ceea026bbaccdc199f3e70c3c641f91b35247de34ecb28e50880bef0565e` |
| `LICENSE` | `c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4` |

The current Go ignored-hint contract is lock_resolver.go's
backoffOnLockHintsInRequest before resolveLocks: only ForRead checks the exact
request hints, any matching transaction charges one BoTxnLockFast backoff, and
resolution runs afterward. Repeated ignored responses exhaust the caller's
existing backoffer. The explicit lite/result-required modes, TiKV-side
async-resolve option, async-commit and secondary-check worker paths, failpoints,
original tests and all caller integrations remain open. The 2,048-entry
determined-status FIFO cache is now shared by sessions under one read
authority; cache metrics and original cache tests remain open.

The direct-unary cop response delegate now borrows the current per-region
budget through blocking-lock status/cleanup recovery, matching
coprocessor.go:2674-2728's shared Backoffer path. This is focused caller seed
evidence; complete caller and package reconciliation remains open.

The lock path now defers cleanup of small optimistic locks until all statuses
are known, then groups exact keys by transaction and region. Writers resolve
these groups synchronously. Reads schedule a detached transaction task, which
routes the keys and schedules region groups independently through a
process-wide 10,000-task admission limit. Each region task uses a 40-second
retry budget and retains only the request source. Saturated scheduling falls
back to in-place cleanup; cleanup errors do not replace a determined read
status. The shared authority cancels and drains its cleanup tasks before cache
shutdown. Source tests cover exact writer batching, read cancellation and
request-source behavior. This matches the small-lock branch of
`LockResolver.resolveLocks` and `batchLiteResolveLocks` in the pinned
`lock_resolver.go`. The remaining resolver options, complete metrics and
TiKV-side async-resolve option, async-commit/secondary-check worker
paths, failpoints, original Go support/test reconciliation and every
whole-package acceptance gate remain open. The default non-lite read cleanup
now schedules a detached region scan when the runtime has an async resolver;
the explicit Go `Lite` and `resultRequired` option combinations remain open.
The current real-TiKV and embedded unistore server openers both install the
pool; unistore authority and server compile checks cover that wiring.

The 2,048-entry transaction-status cache follows Go's FIFO eviction order and
is checked after the per-lock TSO read. Cache hits retain both the determined
status and original CheckTxnStatus response, including async-commit primary-lock
details; an expired async-commit hit reuses its fate and resolves the primary
and all secondaries without repeating CheckSecondaryLocks. Both optimistic
read and pessimistic lock recovery consume that cached fate. Direct status
responses are cached only when LockTtl is zero; async-commit recovery can cache
a determined commit with the original positive TTL. Contradictory status saves
match Go's `saveResolved` invariant. Rust cache metrics and the original Go
cache tests are still open. The three CheckTxnStatus counters now follow
client-go's cache-miss and zero-TTL boundaries. Resolver counters for nonempty
resolve batches, expired/live/wait outcomes, async-commit recovery, secondary
status checks, ResolveLock calls, and lite cleanup now follow the corresponding
client-go event boundaries. The batch-resolve API/counter, async-commit worker
fallback counters/gauges, and original Go metric tests remain open.

The async pool currently admits up to 10,000 tasks but Tokio's blocking runtime
caps worker threads at 512. Upstream's `gp.New(10000, 10*time.Second)` can run
up to 10,000 goroutines, so peak cleanup concurrency differs. This Rust-native
resource bound is not yet validated against sysbench, TPC-C, TPC-H or YCSB and
must be resolved before txnlock package acceptance.
