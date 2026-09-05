# `pkg/executor/sortexec` transcreation inventory

This receipt is part of `rust/docs/go-physical-plan-parity-execplan.md`. The
claim unit is the complete tracked Go package at commit `4f09ce1bc5ce`. `Partial`
means that a Rust owner exists but the source contract has not yet been proven
complete; it is not a completion claim.

| Go artifact | Rust owner or disposition | Current status / receipt |
| --- | --- | --- |
| `BUILD.bazel` | Rust Cargo targets and this receipt | Partial — `cargo check -p tidb-executor`; package-wide test/benchmark target comparison remains |
| `OWNERS` | Repository governance, not production behavior | Not applicable |
| `benchmark_test.go` | Rust executor benchmarks are not yet source-complete | Missing |
| `multi_way_merge.go` | `tidb-executor/src/multi_way_merge.rs`; Sort and TopN retain Go-heap heads across output chunks | Implemented behavior; receipts: parallel Sort test, variable-result-chunk TopN spill tests, `parallel_sort_spill_helper::tests` |
| `parallel_sort_spill_helper.go` | `tidb-executor/src/parallel_sort_spill_helper.rs`, now wired into `SortExec` | Implemented behavior; 12 focused helper tests plus active parallel-spill regression |
| `parallel_sort_spill_test.go` | `sort::tests::parallel_sort_spills_worker_rounds_and_final_batches` and helper tests | Partial — normal/multi-round/error/cleanup and worker/spill panic recovery covered; Go failpoint matrix remains |
| `parallel_sort_test.go` | `sort::tests::parallel_sort_workers_share_input_and_heap_merge_their_runs` | Partial — parallel correctness and worker-run merge covered; randomized type matrix remains |
| `parallel_sort_worker.go` | `tidb-executor/src/sort.rs::ParallelSortWorker` | Partial — bounded fetch/worker overlap, 30-chunk-size batch boundary, local merge, coordinated spill, and panic-to-error worker recovery active; Go failpoint injection remains |
| `rank_topn_test.go` | no Rust `RankInfo` prefix-key truncation owner | Missing |
| `sort.go` | `tidb-executor/src/sort.rs` | Partial — default parallel lifecycle, serial test path, heap result merge, spill and trackers active; Go asynchronous result channel/failpoint receipts remain |
| `sort_partition.go` | `tidb-executor/src/sort_partition.rs` | Implemented core in-memory/disk-run behavior; focused serial and parallel-spill tests |
| `sort_spill.go` | serial action in `sort_partition.rs`, parallel action in `sort.rs` | Partial — both active with Go-style panic-to-error spill recovery; failpoint timing matrix remains |
| `sort_spill_test.go` | Rust serial/parallel ascending/descending spill tests | Partial — normal behavior covered; complete upstream matrix remains |
| `sort_test.go` | Rust sort tests and `tidb-executor/tests/sort_execution_source.rs` | Partial — scalar/type and cancellation inventory not yet source-complete |
| `sort_util.go` | `tidb-executor/src/sort_util.rs` and `tidb-chunk/src/compare.rs` | Partial — common comparison/cursor contract covered; full upstream symbol receipt pending |
| `sortexec_pkg_test.go` | package-private Rust unit tests | Partial — harness substitutes exist; global setup/teardown receipt pending |
| `topn.go` | `tidb-executor/src/topn.rs` | Partial — bounded heap, spill segments and heap K-way result merge active; `RankInfo` missing |
| `topn_chunk_heap.go` | `tidb-executor/src/topn_chunk_heap.rs` | Implemented core heap behavior; focused tie/sift/compaction tests |
| `topn_spill.go` | `tidb-executor/src/topn_spill.rs` | Partial — active spill action/run lifecycle; full fault matrix pending |
| `topn_spill_test.go` | Rust TopN spill and variable-output-chunk tests | Partial |
| `topn_worker.go` | persistent-pool bounded-channel workers in `tidb-executor/src/topn.rs` | Partial — active after first spill; Go random fault/panic hooks remain |

Current count: 21 tracked artifacts; no complete-package claim. The remaining
blockers are explicit: RankTopN metadata, benchmark parity, the Go failpoint
matrix, and complete test/build receipts. Parallel worker and spill panic
boundaries now recover into `ExecError` and preserve the persistent pool for
later tasks; the failpoint-only fault-injection matrix remains unavailable.
