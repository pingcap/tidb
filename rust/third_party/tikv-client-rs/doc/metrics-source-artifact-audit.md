# `metrics` source-artifact audit

This is the atomic completion receipt for client-go package `metrics`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The reusable implementation is public as `tikv_client::metrics`, production call sites share its process-wide initialization through `src/stats.rs`, and validation uses `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 metrics` contains exactly two production files and 1,726 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `metrics.go` | 1,326 | `de7ba0d9d0e35b6f93290f9ec74b27716987e29c560a2b45a257ef7c4beb5a80` | `src/metrics.rs` registry, collector implementations, global lifecycle, store cleanup, commit snapshots, SLI behavior, and source-derived tests |
| `shortcuts.go` | 400 | `b3e3cbbf9b850edffe2321e923d4c8b3b90305b594c6c94fb741aabefb59ade5` | `src/metrics/shortcuts.rs`, shortcut binding in `src/metrics.rs`, and dynamic production adapters in `src/stats.rs` |

There is no package `doc.go`, colocated Go test, benchmark/example, fixture, generated source or input, build file, build-tag/platform variant, or non-Go runtime artifact. `OWNERS` is not present in the pinned package tree. Prometheus and client-model protobuf types are external dependencies, not generated artifacts owned by this package.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| Metric declarations and constructors | `CLIENT_GO_METRIC_SPECS` records all 98 declared globals in source order, including exact source type, metric name/help, ordered labels, exponential or explicit buckets, configured/fixed subsystem, and stale-store membership. `ClientGoMetrics` constructs the same 97 collectors that `initMetrics` constructs. `TiKVPessimisticLockKeysDuration` deliberately remains uninitialized and unregistered because that is the pinned source state. |
| Collector types | Scalar/vector counters use Prometheus floating-point counters, gauges retain signed floating-point values, and histograms retain every exact finite bucket. `rust-prometheus` has no Summary collector, so the seven source SummaryVec definitions use a native no-quantile collector that emits Prometheus `SUMMARY` families with exact count/sum semantics and no invented buckets or quantiles. This supersedes earlier seed notes that mapped summaries to histograms. |
| Initialization | `ClientGoMetrics::new`, `new_default`, `init_metrics`, and `init_metrics_with_const_labels` preserve custom namespace, subsystem, and constant labels. The two read-SLI metrics always use fixed subsystem `sli`, matching source. Reinitialization atomically replaces the process-wide registry; existing Rust consumer adapters resolve it per operation, so later client activity updates the replacement rather than stale collectors. In-flight bound handles continue owning their original collector, matching Prometheus child-handle ownership. |
| Registration | `ClientGoMetrics::register_metrics` and `must_register_metrics` register all 97 initialized collectors in source order. Source-shaped module-level `register_metrics` targets Prometheus's default registry, uses the current process-wide initialization, and panics on duplicate/invalid registration; `try_register_metrics` is the native fallible companion. Partial prior registration follows the source `MustRegister` boundary. |
| Shortcuts | `CLIENT_GO_SHORTCUT_SPECS` records all 151 declarations in source order and eagerly binds the same 149 handles, preserving exact parent, kind, value strings, and label order. `BatchRecvHistogramOK` and `BatchRecvHistogramError` deliberately remain uninitialized because pinned `initShortcuts` never assigns them. Histogram and summary shortcuts expose one common observer API; counters and gauges retain native child handles. |
| Commit counter | `TxnCommitCounter` reads successful 2PC, async-commit, and 1PC shortcut children, serializes exact JSON names `twoPC`, `asyncCommit`, and `onePC`, and supports component-wise subtraction. Native counter reads cannot fail, so the source `readCounter` write-error sentinel is reachable only for a missing/mistyped internal handle, where Rust returns `-1`. |
| Read SLI | `observe_read_sli` preserves the nonzero key/time gate, at-most-20-key and below-1-MiB small-read boundary, and `readSize/readTime` throughput observation. Existing snapshot execution-detail consumers now update these shared collectors. |
| Store metric vectors | The exact 15-vector source list is returned in source append order. `MetricVecHandle` supports exact partial-label deletion for counter, gauge, histogram, and native summary vectors. `find_next_stale_store_id` scans labels and ignores zero/non-numeric IDs. Region-cache GC still selects a stale ID only from `TiKVStoreLivenessGauge`, then deletes that store from all 15 vectors, matching `internal/locate/store_cache.go`. |
| Production consumers | `src/stats.rs` no longer registers duplicate approximations. Lightweight dynamic adapters obtain the current source collector for each existing client operation. This corrects prior name/help/type drift such as write-conflict naming, store-limit suffixing, and histogram substitutes for summaries while preserving native non-client-go request/PD metrics separately. |

Go package globals map to an owned `ClientGoMetrics` value plus a thread-safe process-wide `Arc`. This is the native ownership decision for mutable package globals: independent registries are directly testable and embedders can avoid process-global state, while source-shaped module functions expose initialization, registration, store-vector retrieval, commit snapshots, and read-SLI observation for normal client operations.

## Test and support mapping

The source package has no local test/support artifacts. Source-derived Rust tests therefore validate the complete production surface directly:

- exact 98/97 metric and 151/149 shortcut inventories, uniqueness, declaration order, the three intentional nil globals, and the exact 15-vector stale-store list;
- construction and registration of every initialized collector under custom namespace/subsystem/constant labels;
- every family type, name, help string, variable and constant label dimension, all histogram finite-bucket counts, and summary count/sum/no-quantile exposition;
- all shortcut parent/type/cardinality bindings, histogram and summary observations, gauge updates, commit snapshots/subtraction/JSON, and independent owned registries;
- every read-SLI boundary, native and summary partial deletion, stale-ID discovery/zero filtering, and all-vector store cleanup;
- isolated-process proof that module-level reinitialization redirects an existing production `src/stats.rs` consumer and preserves old owned handles without cross-registry value leakage.

A mechanical reconciliation parses both pinned Go files and both Rust inventory tables. It reports `go_metrics=98 initialized=97 rust_metrics=98`, `go_shortcuts=151 initialized=149 rust_shortcuts=151`, `store_vectors=15`, and zero metadata/order mismatches. This compares every collector type/name/help/label/bucket/subsystem/store flag and every shortcut kind/parent/label tuple rather than relying only on counts.

## Consumer audit

All 34 pinned direct importers were inspected and assigned:

- Completed package consumers: `config/retry/config.go`; `error/error.go`; `internal/client/{client.go,client_batch.go,conn_batch.go,conn_monitor.go}`; `internal/locate/{metrics_collector.go,metrics_collector_test.go,region_cache.go,region_request.go,region_request3_test.go,region_request_state_test.go,replica_selector.go,sorted_btree.go,store_cache.go}`; `internal/unionstore/pipelined_memdb.go`; `oracle/oracles/pd.go`; `txnkv/rangetask/range_task.go`; `txnkv/transaction/{2pc.go,cleanup.go,commit.go,pessimistic.go,prewrite.go,txn.go,txn_file.go}`; `txnkv/txnlock/lock_resolver.go`; and `txnkv/txnsnapshot/{snapshot.go,snapshot_async.go}`. Their existing Rust update sites now resolve collectors from this completed registry. A completed consumer's own receipt remains authoritative for exactly which operational branches emit each metric.
- `rawkv/rawkv.go` retains command/size update-site completion in the separate non-complete `rawkv` row. This package supplies its complete collector and shortcut definitions without promoting RawKV behavior.
- `tikv/gc.go` and `tikv/kv.go` retain safe-point/store lifecycle and high-level update-site completion in root `tikv`.
- `integration_tests/{1pc_test.go,lock_test.go,option_test.go}` retain live-cluster metric assertions and orchestration in the integration-test/final differential gates.

Completing `metrics` does not promote `rawkv`, root `tikv`, or integration packages. Conversely, those callers do not block a utility-package receipt once every definition, lifecycle operation, consumer integration decision, and source artifact is accounted for.

## Validation boundary

Final validation on `nightly-2026-08-22-aarch64-apple-darwin` passed:

- `cargo test --no-default-features --lib metrics::tests`: 7 passed and the subprocess-only probe was intentionally ignored; its parent test executed that exact probe in isolation and passed.
- `cargo test --lib --quiet`: 610 passed, with only the subprocess-only probe ignored.
- `cargo test --lib --all-features --quiet`: 610 passed, with only the subprocess-only probe ignored.
- `cargo check --all-targets --all-features`: passed with the repository's existing dead-code/deprecation warnings.
- `cargo clippy --lib --all-features --message-format short`: passed with the repository's existing 104-warning backlog and no new metrics warning.
- `cargo doc --no-deps --all-features`: passed with the existing `src/raw/client.rs` invalid-HTML warning.
- `cargo test --doc --all-features --quiet`: 50 passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- The source checkout HEAD is exactly `52c1e76cec993571493c81de442bcbef90cdc106`; `git ls-tree`, `wc -l`, and SHA-256 reproduce the two-file/1,726-line inventory above.
- The post-format mechanical reconciliation reports 98/98 metric declarations, 151/151 shortcuts, 15 store vectors, and zero type/name/help/label/bucket/subsystem/store/order mismatches.

The source package has no local test requiring UniStore. No TiKV/PD cluster is attached, so the three external integration tests and live cross-client differential checks remain repository-level gates rather than hidden package artifacts.
