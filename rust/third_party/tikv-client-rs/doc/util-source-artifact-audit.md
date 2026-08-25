# `util` source-artifact audit

This is the atomic completion receipt for client-go's root `util` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust owner is the shared public `tikv_client::util` module and its native tracing, traffic, PD, and transaction integrations. Validation uses `nightly-2026-08-22`; no caller package is promoted by this receipt.

## Complete source inventory

The package contains exactly 13 Go artifacts and 3,478 lines: eight production files (2,176 lines), four ordinary test files (1,277 lines), and the 25-line package test harness.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `dns.go` | 60 | `fa440043804ddf52a2fe8b10fc749daa701b373f6fd592430b823f61a6f645bf` | `src/util/dns.rs` |
| `execdetails.go` | 1,366 | `9c4e32e4c0357c1b002f24c098e1d0317432aec4d3069be7f9ae15b37150000e` | `src/util/execdetails.rs`, `duration.rs`, `ru.rs`, plus native `trace.rs` and `traffic.rs` owners |
| `failpoint.go` | 63 | `11341b0951b5798e643da1b590fb0b9d84b67b7608d9c7310a866250cae919f0` | `src/util/failpoint.rs` |
| `misc.go` | 200 | `3548e39d38b86370ba1bf3012eb162e0d4dd35bdc0fff96a5daf38f3a3951f6f` | `src/util/misc.rs`; standard `Option` owns source `Option`/`Some`/`None` |
| `pd_interceptor.go` | 150 | `5c43afc2dcdf4199ee8f56f4826c15a5aaf39adef5c95b2f7ba4ace661f80523` | `src/util/pd_interceptor.rs` |
| `rate_limit.go` | 73 | `37f9a143a212e8cd25edd911b33d3ad960f23ab520d2efe49b3bc43bb1ec5abe` | `src/util/rate_limit.rs` |
| `request_source.go` | 190 | `584a1879d137339adad74a92b35917c844104ed3ebe61142266530347dcd4c7c` | `src/util/request_source.rs` and transaction/snapshot call sites |
| `ts_set.go` | 74 | `a3969d914b4aec481e0f8918ee48d252f9479d44f1cf02c205e1ca6d413ceb86` | `src/util/ts_set.rs` and snapshot read-lock context |
| `execdetails_test.go` | 972 | `be1f527187e1f1dee4d4e1503eb176b4718ed6b636fd9c1d99b51d41b6c87322` | 23 source scenarios in `src/util/execdetails.rs` |
| `misc_test.go` | 140 | `72b530fa37b3acfa50dec945738fd368bcb043e24999be0f627e969e7e79e52d` | GC-time, time-detail, and range-helper tests in `misc.rs`/`execdetails.rs` |
| `rate_limit_test.go` | 70 | `0cd0b82982be5680f72d63a938d7d4d6c63826c1d961c3edfa6058fd3c4e0bd7` | cancellation/blocking/redundant-token test in `rate_limit.rs` |
| `request_source_test.go` | 95 | `5eeb8d4595fb229475320cf62b60a07a31f3ef137c174e8c6071a41409704a3c` | complete source/build matrix in `request_source.rs` |
| `main_test.go` | 25 | `40d5549f5ecd71526173d7943a9808e6a168b117d3706f793aadb1ce0daf285` | all native helper tasks are scoped and joined; full library suites are the leak/lifecycle gate |

There is no `doc.go`, benchmark, example test, fixture, package metadata/`OWNERS`, generated input/output, package-local build file, Go build tag, platform variant, or non-Go artifact in this package.

## Production mapping

| client-go surface | Rust behavior and native decision |
| --- | --- |
| custom DNS dialer | `CustomDnsDialer` applies the source's exact `host:port` domain rule, sends A and AAAA packets to the explicitly configured DNS server with the same ten-second dial bound, parses compressed DNS answers, and attempts every returned address. A loopback UDP-DNS/TCP test proves that the system resolver is not substituted. |
| execution-detail context keys | Typed `TraceContext` carriers own commit, lock, execution, RU, request-source, resource-group, and session values without Go key collisions. Async task-local scopes preserve propagation across every awaited physical RPC. |
| `ContextWithTraceExecDetails`, `TraceExecDetailsEnabled` | Existing `trace::with_trace_exec_details` reconstructs and emits the source TiKV execution tree; `trace_exec_details_enabled` exposes the current-scope predicate. |
| `TiKVExecDetails`, `ReqDetailInfo` | Optional protobuf time/scan/write details merge into native aggregates and format in source order with empty components omitted. |
| `CommitTSLagDetails`, `CommitDetails` | All durations, counters, backoff vectors, slowest prewrite/primary selection, request-detail merges, no-op flush hook, resolve-lock total, and deep clone behavior are present. The source-specific omission of `PrewriteReqNum` from clone/merge is retained. |
| `LockKeysDetails` | Merge, one-per-merge retry count, deep clone, backoff order, conflict/aggressive counters, RPC totals, and strict slowest-request replacement match the source. |
| `ExecDetails`, `TrafficDetails` | Independently atomic backoff, PD/KV wait, KV/MPP, and cross-zone counters retain source concurrent observation semantics. Existing region-sender traffic integration remains authoritative. |
| `FormatDuration`, `PoolTaskDetails` | Exact precision pruning, task/sample counts, zero-as-present minimum handling, aggregate merge associativity, average divisors, fair-queue semantics, clone/empty behavior, and source strings are covered. |
| `ScanDetail`, `WriteDetail`, `TimeDetail`, `ResolveLockDetail` | Every protobuf field, IA field, duration conversion, additive merge, empty rule, and source diagnostic string is implemented. |
| `RUDetails` | Concurrent RRU/WRU/wait/TiFlash/scaled-TiKV values and all raw RU-v2 counters support update, TiFlash update, deep clone, merge, add, nondestructive peer merge, and destructive drain. |
| failpoint façade | One explicit process-wide enable gate and the exact `tikvclient/` prefix wrap the existing Rust failpoint runtime; disabled access returns the source error. |
| GC time/recovery/session/bytes/ranges | The parser accepts both persisted fractional forms and exactly one trailing legacy zone token; recovery invokes its hook on success and panic before logging the captured stack; typed session IDs, byte formatting, and unbounded range-key rules match source behavior. |
| `Option`, `Some`, `None` | Rust's standard `Option<T>` is the native zero-cost owner and is already used by union-store/locate behavior; no duplicate wrapper is introduced. |
| `RateLimit` | Atomic token admission plus `Notify` preserves fixed capacity, cancellation while blocked, wakeup, and the exact redundant-put panic. Rust's borrow/lifetime rules replace channel misuse races without changing observable admission. |
| request source | All constants, explicit-type list/order, setters, unknown/internal/external formatting, duplicate explicit-type elision, context extraction, internal predicate, resource-group context, and transaction call sites share one `util::RequestSource`. |
| `TSSet` | A lazily allocated `RwLock<HashSet<u64>>` preserves concurrent insertion, deduplication, empty output, and unspecified iteration order. Existing snapshot lock-context sets retain the same native behavior. |
| intercepted PD client | A transparent generic `PdClient` decorator delegates the complete trait and records scoped wait time around timestamp, key/end-key/ID region, batch-region, and store lookups. One async timestamp call naturally covers client-go's immediate `GetTSAsync` plus future `Wait` durations. |

## Tests and lifecycle gate

The four ordinary source test files declare exactly 30 tests: 23 execution-detail tests, four miscellaneous tests, one rate-limit test, and two request-source tests. Their complete assertion matrices are transcreated, including RU-v2 nested executor counters, pool-task zero minima, sequential-versus-aggregate equivalence, every slowest-request branch, deep-copy independence, IA scan fields, all write/time fields, GC timestamp valid/invalid cases, and blocked-token cancellation. Additional native tests cover the four production files that had no source tests: explicit DNS routing, failpoint prefix/gate, PD wait scoping/delegation, and timestamp-set concurrency semantics.

The source `TestMain` uses goleak. No Rust utility starts an unowned worker: the DNS test joins both UDP and TCP tasks, the PD decorator is taskless, token waiters are caller-owned futures, and every task-local scope ends with its future. Complete default/all-feature library suites therefore serve as the package lifecycle gate.

## Direct consumers

Exact import matching (`"github.com/tikv/client-go/v2/util"`, excluding subpackages) finds 58 files across 18 package/test directories. The production-symbol inventory is: 103 `EvalFailpoint`, 33 `Some`, 16 `SessionID`, 14 each `EnableFailpoints` and `FormatDuration`, 13 `RUDetailsCtxKey`, 11 each `ExecDetails`, `RUDetails`, and `RequestSource`, nine each `CommitDetails`, `RequestSourceFromCtx`, and `RequestSourceKey`, seven each `ResolveLockDetail` and `TSSet`, six `ResourceGroupNameFromCtx`, five `Option`, four each `ExecDetailsKey`, `IsInternalRequest`, and `None`, three each `CommitTSLagDetails`, `LockKeysDetails`, and `PoolTaskDetails`, two each `CommitDetailCtxKey`, `InternalRequestPrefix`, `InternalTxnOthers`, `IsRequestSourceInternal`, `NewInterceptedPDClient`, `NewRateLimit`, `TimeDetail`, `TraceExecDetailsEnabled`, and `WithInternalSourceType`, plus the remaining single-use surfaces.

Completed consumers are `config`/`config/retry`, `error`, `internal/client`, `internal/locate`, `internal/resourcecontrol`, `internal/unionstore`, `kv`, `oracle/oracles`, `tikvrpc`, `txnkv/transaction`, `txnkv/txnlock`, and `txnkv/txnsnapshot`; their existing native failpoint, tracing, traffic, request, transaction, and RU owners remain authoritative. The example and external integration files remain assigned to their owning public-package/live-test matrices. `internal/mockstore/mocktikv`, root `tikv`, and root `txnkv` retain their independent incomplete ledger rows. This receipt supplies their shared dependency but does not claim their algorithms or integration gates.

## Validation contract

Package completion requires both feature configurations of the focused and full library suites, all-target compilation, Clippy, rustdoc/doctests, rustfmt/diff checks, exact source identity/hashes/line counts, exact 58-importer/symbol reconciliation, and leak-free completion on `nightly-2026-08-22-aarch64-apple-darwin`. No live TiKV or PD cluster is required: DNS and PD interception use deterministic loopback/in-memory fakes, and all remaining behavior is local aggregation or context propagation.
