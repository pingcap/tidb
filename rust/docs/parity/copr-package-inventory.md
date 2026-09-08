# `pkg/store/copr` transcreation inventory

This receipt is part of `rust/docs/go-physical-plan-parity-execplan.md`. The
claim unit is the complete tracked Go package at commit
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (`origin/master`). `Partial`
means that a native Rust owner exists but full contract/test parity has not yet
been demonstrated.

| Go artifact | Rust owner or disposition | Current status / receipt |
| --- | --- | --- |
| `BUILD.bazel` | Rust Cargo targets and this receipt; build/test inventory still needs comparison | Partial |
| `batch_coprocessor.go` | `tidb-distsql/src/cop_paging`, `tidb-txnkv/src/rpc/batch` | Partial |
| `batch_coprocessor_test.go` | Rust batch coprocessor suites | Partial |
| `batch_request_sender.go` | `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs` | Partial |
| `copr_test/BUILD.bazel` | Rust integration targets | Missing receipt |
| `copr_test/coprocessor_test.go` | Rust TiKV/unistore cop suites | Partial — Go master’s same-store query/request limiter concurrency scenarios are represented by the direct-unary admission owner; live TiKV/unistore integration remains |
| `copr_test/main_test.go` | Rust test harness | Partial |
| `coprocessor.go` | `tidb-distsql/src/cop_paging.rs`, `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs`, `tidb-exec/src/cop_scan.rs`, `tidb-txnkv/src/kv_contract.rs` | Partial — query-scoped/request-local per-store attempt limiting is now enforced around every TiKV dispatch; broader task, paging, retry, and metrics parity remains |
| `coprocessor_cache.go` | `tidb-distsql/src/copr_cache.rs` | Implemented behavior; all six Go tests ported plus branch regressions for the oversized start/end key and negative `Tp` paths (`copr_cache_source::cache_key_rejects_oversized_range_keys_in_source_order`, `negative_request_type_keeps_go_uint8_low_byte`) |
| `coprocessor_cache_test.go` | `tidb-distsql/tests/copr_cache_source.rs` | Implemented; 13 focused tests, including a deterministic stand-in for ristretto's asynchronous write buffer |
| `coprocessor_test.go` | Rust paging/transport/scan tests, including `direct_unary_async_region_runtime_source::every_tikv_attempt_honors_the_query_scoped_store_limiter` | Partial — limiter admission/release is covered; complete response, failpoint, and live-store matrix remains |
| `ema.go` | `tidb-distsql/src/read_bytes_ema.rs`; `paging_response_read_bytes` in `tidb-distsql/src/cop_paging.rs` | Implemented; classic/NextGen read-byte selection and the time-decay EMA |
| `ema_test.go` | `tidb-distsql/tests/read_bytes_ema_source.rs`, `tidb-distsql/tests/cop_paging_source.rs` | Implemented; every Go case plus the zero-timestamp branch |
| `key_ranges.go` | `tidb-txnkv/src/key_ranges.rs` | Implemented; `first`/`mid`/`last` storage, `slice`, `split`, `reset`, Go `%q` display, and safe protobuf conversion |
| `key_ranges_test.go` | `tidb-txnkv/tests/key_ranges_source.rs` | Implemented; every Go split/slice case plus the unanchored methods |
| `main_test.go` | Rust package test harness | Partial |
| `metrics/BUILD.bazel` | `tidb-distsql` Cargo target and cache lifecycle tests | Partial — build inventory pending |
| `metrics/metrics.go` | `tidb-distsql/src/copr_cache_metrics.rs` | Implemented process-global `evict`/`hit`/`miss` counters; the Prometheus `/metrics` exporter is still unported in `tidb-server`, so Go's `tidb_distsql_copr_cache{type=…}` name/label is not published yet |
| `mpp.go` | `tidb-txnkv/src/mpp.rs`; TiFlash execution tier remains narrowed | Partial |
| `mpp_probe.go` | `tidb-txnkv/src/mpp_probe.rs` | Partial — failed-store prober TTL/recovery scan and server-info LRU implemented; full store integration receipt pending |
| `mpp_probe_test.go` | `tidb-txnkv::mpp_probe` focused tests | Partial |
| `range_diagnostics.go` | `tidb-txnkv/src/range_diagnostics.rs` | Implemented core monotonicity/overlap/gap diagnostics; focused unit tests |
| `region_cache.go` | `tidb-txnkv/src/region/**` | Partial |
| `region_cache_test.go` | Rust region-cache suites | Partial |
| `store.go` | distributed across `tidb-txnkv`, `tidb-distsql`, and SQL-node capabilities | Partial |

Current count: 25 tracked artifacts; 12,225 Go source/test/build lines at the
comparison commit; no complete-package claim. The previously absent MPP
probe, cache metrics, and range diagnostics now have concrete Rust owners and
focused tests. This batch closes the Go master query-scoped per-store limiter
execution boundary: metadata is admitted by store ID, a token is held through
RPC response classification, and retries release the old store token before
selecting another store. Integration, complete metrics, and the remaining
production/test rows stay explicit blockers.

Re-verified 2026-09-08 against `origin/master` `f5cf8f6337` (the package root
is unchanged in these artifacts): the four leaf owners above are now
`Implemented` rather than `Partial`. `coprocessor_cache.go`'s six Go tests,
`ema_test.go`'s five EMA cases plus `pagingResponseReadBytes`, and
`key_ranges_test.go`'s split/slice matrix all have direct Rust equivalents,
and the two Go production branches with no Go test — oversized range keys and
a negative `Tp` — now have focused Rust regressions. The package-level
blockers are the worker lifecycle, region-cache orchestration, MPP/TiFlash
tier, and the `/metrics` exporter, not these leaf owners.

## Query-scoped per-store request limiter alignment

Go `copIteratorWorker.setRequestAttemptLimiter` prefers
`QueryCopStoreLimiter.GetStoreLimiter(storeID)` over the request-local limiter,
fast-paths `TryAcquire`, waits with cancellation/deadline awareness, and
releases the token as the physical attempt completes. Rust previously carried
both limiter values through `KvRequestMetadata` but never consumed either one
in the direct-unary transport. `CoprRequestLimiter` now has a synchronous
condition-variable wait for the pull-based response owner; each prepared TiKV
dispatch owns an RAII token through synchronous, BatchCommands, and async
completion paths. A retry drops that permit before route recovery selects the
next store, and a query limiter does not fall back to the request limiter for
store ID zero, matching Go's precedence.

The complete package inventory above includes all 25 Go production, test,
fixture/harness, metrics, generated/protocol-facing, and Bazel artifacts. Rust
owners changed only in `tidb-distsql` transport/test support and
`tidb-txnkv::kv_contract`; no Go, Bazel, generated, fixture, or platform
artifact changed.

Focused fail-before/pass-after evidence is recorded in
`rust/testport/receipts/copr_query_limiter.md`.
