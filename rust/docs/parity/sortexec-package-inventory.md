# `pkg/executor/sortexec` transcreation inventory

This receipt is part of `rust/docs/go-physical-plan-parity-execplan.md`. The
claim unit is the complete tracked Go package, refreshed at commit
`7a3dacb52efe58d28db360ae8639d8838c376544`. `Partial`
means that a Rust owner exists but the source contract has not yet been proven
complete; it is not a completion claim.

| Go artifact | Rust owner or disposition | Current status / receipt |
| --- | --- | --- |
| `BUILD.bazel` | Rust Cargo targets and this receipt | Partial — `cargo check -p tidb-executor`; package-wide test/benchmark target comparison remains |
| `OWNERS` | Repository governance, not production behavior | Not applicable |
| `benchmark_test.go` | Rust executor benchmarks are not yet source-complete | Missing |
| `multi_way_merge.go` | Active result heaps in `sort.rs` and `topn.rs` | Partial — disconnected generic merger/cursor harness removed; active multi-run behavior retained, complete shared Go ownership remains |
| `parallel_sort_spill_helper.go` | Missing active Rust owner | Missing — disabled substitute and its private tests retired; complete Go spill coordination remains required |
| `parallel_sort_spill_test.go` | Missing active parallel owner | Missing — worker/spill/error/panic/failpoint obligations remain; retired tests exercised a disconnected substitute |
| `parallel_sort_test.go` | Missing active parallel owner | Missing — parallel correctness, randomized types and worker lifecycle remain required |
| `parallel_sort_worker.go` | Missing active Rust owner | Missing — prior substitute dispatch was disabled by b00685bdd4; disconnected implementation now retired |
| `rank_topn_test.go` | `tidb-executor/src/topn.rs::RankPrefix` (one `RankPrefixColumn` per `TruncateKeyExprs` entry) | Implemented behavior; `topn::tests::rank_topn_compares_every_declared_prefix_column` ports both Go cases (`-1` whole value and `12`-character truncation), and `rank_topn_stops_after_the_boundary_prefix_group` pins the read short-circuit |
| `sort.go` | `tidb-executor/src/sort.rs` | Partial — serial partitions, result heap, spill and trackers active; default parallel lifecycle and asynchronous result channel remain missing |
| `sort_partition.go` | `tidb-executor/src/sort_partition.rs` | Partial — core in-memory/disk-run behavior and serial cancellation retained; full upstream receipt pending |
| `sort_spill.go` | Serial action in `sort_partition.rs` | Partial — serial action active; default parallel spill action and fault matrix remain missing |
| `sort_spill_test.go` | Rust serial ascending/descending and single-chunk spill tests | Partial — active spill behavior covered; complete upstream matrix remains |
| `sort_test.go` | Rust `sort::tests` | Partial — scalar/type and cancellation inventory not source-complete; previous external source-test path was stale |
| `sort_util.go` | `sort_util.rs` panic recovery; active comparator/cursor code in Sort and TopN | Partial — disconnected generic cursor/message/status adapters retired; full upstream symbol receipt pending |
| `sortexec_pkg_test.go` | package-private Rust unit tests | Partial — harness substitutes exist; global setup/teardown receipt pending |
| `topn.go` | `tidb-executor/src/topn.rs` | Partial — bounded heap, spill segments, heap K-way result merge, and the multi-column `RankInfo` prefix short-circuit are active; Go asynchronous result channel/failpoint receipts remain |
| `topn_chunk_heap.go` | `tidb-executor/src/topn_chunk_heap.rs` | Implemented core heap behavior; focused tie/sift/compaction tests |
| `topn_spill.go` | `tidb-executor/src/topn_spill.rs` | Partial — active spill action/run lifecycle; full fault matrix pending |
| `topn_spill_test.go` | Rust TopN spill and variable-output-chunk tests | Partial |
| `topn_worker.go` | persistent-pool bounded-channel workers in `tidb-executor/src/topn.rs` | Partial — active after first spill; Go random fault/panic hooks remain |

Current count: 21 tracked artifacts; no complete-package claim. Go's default
parallel sort lifecycle, benchmark parity, failpoint matrix and complete
test/build receipts remain missing. TopN's live worker and panic boundaries
remain. The prior active-parallel claims were stale after b00685bdd4 disabled
its substitute; [cleanup receipt](current-audit/sort-cleanup-validation.json)
records the dependency closure and retained checks. Retiring tests of that
substitute does not discharge their Go behavioral obligations.
