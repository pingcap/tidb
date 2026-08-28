# `pkg/store/copr` transcreation inventory

This receipt is part of `rust/docs/go-physical-plan-parity-execplan.md`. The
claim unit is the complete tracked Go package at commit `4f09ce1bc5ce`. `Partial`
means that a native Rust owner exists but full contract/test parity has not yet
been demonstrated.

| Go artifact | Rust owner or disposition | Current status / receipt |
| --- | --- | --- |
| `BUILD.bazel` | Rust Cargo targets and this receipt; build/test inventory still needs comparison | Partial |
| `batch_coprocessor.go` | `tidb-distsql/src/cop_paging`, `tidb-txnkv/src/rpc/batch` | Partial |
| `batch_coprocessor_test.go` | Rust batch coprocessor suites | Partial |
| `batch_request_sender.go` | `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs` | Partial |
| `copr_test/BUILD.bazel` | Rust integration targets | Missing receipt |
| `copr_test/coprocessor_test.go` | Rust TiKV/unistore cop suites | Partial |
| `copr_test/main_test.go` | Rust test harness | Partial |
| `coprocessor.go` | `tidb-distsql/src/cop_paging.rs`, `tidb-exec/src/cop_scan.rs` | Partial |
| `coprocessor_cache.go` | `tidb-distsql/src/copr_cache.rs` | Partial |
| `coprocessor_cache_test.go` | Rust coprocessor-cache tests | Partial |
| `coprocessor_test.go` | Rust paging/transport/scan tests | Partial |
| `ema.go` | `tidb-distsql/src/read_bytes_ema.rs` | Partial |
| `ema_test.go` | Rust EMA unit tests | Partial |
| `key_ranges.go` | `tidb-txnkv/src/key_ranges.rs` and `tidb-distsql` range modules | Partial |
| `key_ranges_test.go` | Rust key-range tests | Partial |
| `main_test.go` | Rust package test harness | Partial |
| `metrics/BUILD.bazel` | `tidb-distsql` Cargo target and cache lifecycle tests | Partial — build inventory pending |
| `metrics/metrics.go` | `tidb-distsql/src/copr_cache_metrics.rs` | Partial — process-global hit/miss/evict lifecycle is active; complete Go metric-vector inventory pending |
| `mpp.go` | `tidb-txnkv/src/mpp.rs`; TiFlash execution tier remains narrowed | Partial |
| `mpp_probe.go` | `tidb-txnkv/src/mpp_probe.rs` | Partial — failed-store prober TTL/recovery scan and server-info LRU implemented; full store integration receipt pending |
| `mpp_probe_test.go` | `tidb-txnkv::mpp_probe` focused tests | Partial |
| `range_diagnostics.go` | `tidb-txnkv/src/range_diagnostics.rs` | Implemented core monotonicity/overlap/gap diagnostics; focused unit tests |
| `region_cache.go` | `tidb-txnkv/src/region/**` | Partial |
| `region_cache_test.go` | Rust region-cache suites | Partial |
| `store.go` | distributed across `tidb-txnkv`, `tidb-distsql`, and SQL-node capabilities | Partial |

Current count: 25 tracked artifacts; no complete-package claim. The previously
absent MPP probe, cache metrics, and range diagnostics now have concrete Rust
owners and focused tests, while integration and the remaining production/test
rows stay explicit blockers.
