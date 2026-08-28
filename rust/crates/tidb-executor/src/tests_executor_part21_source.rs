// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-backed inventory for `pkg/executor.part21`, Go test items 661–720.
//!
//! This slice contains the slow-query SQL/parser suites, sort and TopN
//! spill/failpoint suites, region-split key generation, and statement-RU
//! session-lifecycle tests. `tidb-executor` has no slow-query file retriever,
//! TiKV/PD region-splitting executor, failpoint harness, or statement-RU owner
//! lifecycle. The crate does have lower-level sort/spill tests, but those do
//! not reproduce the Go session and failpoint contracts, so the assigned Go
//! tests remain explicit parity-gap carriers rather than being falsely marked
//! as complete ports.

/// `pkg/executor/slow_query_sql_test.go:214::TestSlowQuerySessionAlias` (item 661).
#[test]
#[ignore = "go-parity-gap: session alias state and information_schema slow-query retrieval are unported"]
fn slow_query_records_session_alias() {}

/// `pkg/executor/slow_query_sql_test.go:250::TestSlowQuery` (item 662).
#[test]
#[ignore = "go-parity-gap: slow-log file parsing, time-zone filtering, and information_schema.slow_query are unported"]
fn slow_query_retrieves_runtime_and_ru_fields() {}

/// `pkg/executor/slow_query_sql_test.go:336::TestIssue37066` (item 663).
#[test]
#[ignore = "go-parity-gap: slow-log index-name publication and statements_summary require session/domain runtime metadata"]
fn issue_37066_keeps_slow_query_index_names_consistent() {}

/// `pkg/executor/slow_query_sql_test.go:410::TestWarningsInSlowQuery` (item 664).
#[test]
#[ignore = "go-parity-gap: slow-query warning capture and the suite-backed information_schema reader are not modeled"]
fn warnings_are_recorded_in_slow_query_rows() {}

/// `pkg/executor/slow_query_sql_test.go:478::TestStorageEnginesInSlowQuery` (item 665).
#[test]
#[ignore = "go-parity-gap: TiKV/TiFlash plan selection, slow-log storage fields, and mockstore RPC metadata require session/storage integration"]
fn slow_query_records_storage_engines() {}

/// `pkg/executor/slow_query_sql_test.go:573::TestReadPoolTaskDetailsInDiagnostics` (item 666).
#[test]
#[ignore = "go-parity-gap: TiKV response exec details, failpoint hooks, slow-log diagnostics, and EXPLAIN ANALYZE are outside this crate"]
fn slow_query_records_read_pool_task_details() {}

/// `pkg/executor/slow_query_sql_test.go:710::TestSessionConnectAttrsInSlowQuery` (item 667).
#[test]
#[ignore = "go-parity-gap: slow-log Session_connect_attrs parsing and JSON information_schema projection are unported"]
fn slow_query_reads_session_connect_attrs() {}

/// `pkg/executor/slow_query_sql_test.go:756::TestSessionConnectAttrsMissingAndTruncatedInSlowQuery` (item 668).
#[test]
#[ignore = "go-parity-gap: slow-log Session_connect_attrs missing/truncated handling belongs to the unported slow-query retriever"]
fn slow_query_handles_missing_and_truncated_connect_attrs() {}

/// `pkg/executor/slow_query_test.go:92::TestParseSlowLogPanic` (item 669).
#[test]
#[ignore = "go-parity-gap: the Go test injects errorMockParseSlowLogPanic into the unported slowQueryRetriever parser"]
fn parse_slow_log_panic_is_returned_as_an_error() {}

/// `pkg/executor/slow_query_test.go:128::TestParseSlowLogFile` (item 670).
#[test]
#[ignore = "go-parity-gap: the full slow-log field parser and information_schema row factory have no Rust counterpart"]
fn parse_slow_log_file_preserves_runtime_fields() {}

/// `pkg/executor/slow_query_test.go:294::TestParseSlowLogSessionConnectAttrs` (item 671).
#[test]
#[ignore = "go-parity-gap: Session_connect_attrs parsing is implemented by the unported Go slowQueryRetriever"]
fn parse_slow_log_session_connect_attrs() {}

/// `pkg/executor/slow_query_test.go:346::TestParseSlowLogFileSerial` (item 672).
#[test]
#[ignore = "go-parity-gap: the Go test exercises the retriever's MaxOfMaxAllowedPacket session-global and scanner limit"]
fn parse_slow_log_file_enforces_the_line_limit() {}

/// `pkg/executor/slow_query_test.go:372::TestSlowLogParseTime` (item 673).
#[test]
#[ignore = "go-parity-gap: ParseTime is a private slow-query reader helper with no tidb-executor equivalent"]
fn slow_log_parse_time_accepts_tidb_formats() {}

/// `pkg/executor/slow_query_test.go:389::TestFixParseSlowLogFile` (item 674).
#[test]
#[ignore = "go-parity-gap: rotated slow-log parsing and its warning path are not modeled by tidb-executor"]
fn fix_parse_slow_log_file_accepts_legacy_time_and_warns() {}

/// `pkg/executor/slow_query_test.go:436::TestSlowQueryRetriever` (item 675).
#[test]
#[ignore = "go-parity-gap: plain/gzip slow-log file discovery, time-range filtering, and retriever iteration are unported"]
fn slow_query_retriever_filters_rotated_files() {}

/// `pkg/executor/slow_query_test.go:619::TestSplitByColon` (item 676).
#[test]
#[ignore = "go-parity-gap: splitByColon is a private helper in pkg/executor/slow_query.go and no Rust slow-log parser exists"]
fn split_slow_log_fields_by_colon() {}

/// `pkg/executor/slow_query_test.go:701::TestBatchLogForReversedScan` (item 677).
#[test]
#[ignore = "go-parity-gap: reverse scanning rotated slow logs is implemented only by the Go slowLogReverseScanner"]
fn reverse_scan_batches_slow_log_records() {}

/// `pkg/executor/slow_query_test.go:831::TestSlowQueryRetrieverReversedScanWithLimit` (item 678).
#[test]
#[ignore = "go-parity-gap: reverse slow-log block scanning, LIMIT pushdown, and file metrics are not modeled"]
fn reverse_slow_query_scan_honors_limit() {}

/// `pkg/executor/slow_query_test.go:923::TestSlowQueryRetrieverReversedScanWithTimeJitter` (item 679).
#[test]
#[ignore = "go-parity-gap: reverse slow-log scanning with time-range tolerance has no tidb-executor implementation"]
fn reverse_slow_query_scan_handles_time_jitter() {}

/// `pkg/executor/slow_query_test.go:998::TestPBPlanBuilderPushDownLimitToSlowQueryRetriever` (item 680).
#[test]
#[ignore = "go-parity-gap: protobuf plan building and the slow-query memtable reader are session/infoschema surfaces absent here"]
fn pb_plan_builder_pushes_limit_to_slow_query_retriever() {}

/// `pkg/executor/slow_query_test.go:1041::TestCancelParseSlowLog` (item 681).
#[test]
#[ignore = "go-parity-gap: cancellation of asynchronous slow-log parsing and goroutine leak checks require the Go failpoint/session harness"]
fn cancel_parse_slow_log_stops_parser_workers() {}

/// `pkg/executor/slow_query_test.go:1120::TestIssue54324` (item 682).
#[test]
#[ignore = "go-parity-gap: readLastLines is a private slow-log reverse-reader helper with no Rust counterpart"]
fn issue_54324_reads_lines_across_reverse_scan_chunks() {}

/// `pkg/executor/sortexec/benchmark_test.go:103::BenchmarkSortExec` (item 683).
#[test]
#[ignore = "skipped-reason: Go testing.B performance benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_sort_exec() {}

/// `pkg/executor/sortexec/benchmark_test.go:109::BenchmarkSortExecSpillToDisk` (item 684).
#[test]
#[ignore = "skipped-reason: Go testing.B performance benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_sort_exec_spill_to_disk() {}

/// `pkg/executor/sortexec/parallel_sort_spill_test.go:81::TestParallelSortSpillDisk` (item 685).
#[test]
#[ignore = "go-parity-gap: the Go test requires sortexec failpoints, session memory trackers, and repeated mock data-source execution; Rust core spill tests are not this session contract"]
fn parallel_sort_spill_disk_preserves_rows() {}

/// `pkg/executor/sortexec/parallel_sort_spill_test.go:119::TestParallelSortSpillDiskFailpoint` (item 686).
#[test]
#[ignore = "go-parity-gap: sort worker/random-failure and ChunkInDiskError failpoints are not available in tidb-executor"]
fn parallel_sort_spill_failures_close_cleanly() {}

/// `pkg/executor/sortexec/parallel_sort_spill_test.go:159::TestIssue59655` (item 687).
#[test]
#[ignore = "go-parity-gap: Issue59655 is driven by a sort failpoint, session memory quota, and leak-file assertions absent from this crate boundary"]
fn issue_59655_parallel_sort_does_not_hang() {}

/// `pkg/executor/sortexec/parallel_sort_spill_test.go:190::TestIssue63216` (item 688).
#[test]
#[ignore = "go-parity-gap: Issue63216 depends on the Go sort failpoint and session memory tracker lifecycle"]
fn issue_63216_parallel_sort_failure_closes_cleanly() {}

/// `pkg/executor/sortexec/parallel_sort_test.go:100::TestParallelSort` (item 689).
#[test]
#[ignore = "go-parity-gap: Go parallel-sort worker scheduling and failpoint checkpoints are not exposed as the same Rust test seam"]
fn parallel_sort_workers_return_sorted_rows() {}

/// `pkg/executor/sortexec/parallel_sort_test.go:119::TestFailpoint` (item 690).
#[test]
#[ignore = "go-parity-gap: ParallelSortRandomFail/SlowSomeWorkers/SignalCheckpointForSort failpoints are Go-only"]
fn parallel_sort_random_failure_does_not_leak() {}

/// `pkg/executor/sortexec/parallel_sort_test.go:141::TestIssue55344` (item 691).
#[test]
#[ignore = "go-parity-gap: the regression uses TiDB SQL planning, optimizer-rule blacklist state, and session TestKit"]
fn issue_55344_sort_ordering_survives_constant_keys() {}

/// `pkg/executor/sortexec/rank_topn_test.go:148::TestRankTopN` (item 692).
#[test]
#[ignore = "go-parity-gap: rank TopN prefix-key truncation/collation metadata and Go mock data-source execution are not exposed by tidb-executor"]
fn rank_topn_preserves_prefix_groups() {}

/// `pkg/executor/sortexec/sort_spill_test.go:357::TestUnparallelSortSpillDisk` (item 693).
#[test]
#[ignore = "go-parity-gap: the Go test combines serial sort mode, failpoint spill forcing, session quota trackers, and leak-file checks"]
fn unparallel_sort_spill_disk_preserves_rows() {}

/// `pkg/executor/sortexec/sort_spill_test.go:381::TestFallBackAction` (item 694).
#[test]
#[ignore = "go-parity-gap: memory Tracker ActionOnExceed fallback wiring is not the tidb-executor StatementMemory contract"]
fn sort_fallback_action_runs_after_memory_exhaustion() {}

/// `pkg/executor/sortexec/sort_test.go:34::TestSortInDisk` (item 695).
#[test]
#[ignore = "go-parity-gap: SQL sort execution, forced spill failpoint, session memory/disk trackers, and temporary-storage cleanup require the Go TestKit"]
fn sort_in_disk_preserves_rows_and_releases_trackers() {}

/// `pkg/executor/sortexec/sort_test.go:103::TestIssue16696` (item 696).
#[test]
#[ignore = "go-parity-gap: the regression requires SQL hash join plus cross-operator sort/join spill failpoints and EXPLAIN ANALYZE"]
fn issue_16696_reports_disk_usage_for_spilled_operators() {}

/// `pkg/executor/sortexec/sortexec_pkg_test.go:33::TestInterruptedDuringSort` (item 697).
#[test]
#[ignore = "go-parity-gap: SQLKiller cancellation of the package-private Go sort partition is not exposed by tidb-executor"]
fn interrupted_during_sort_returns_query_interrupted() {}

/// `pkg/executor/sortexec/sortexec_pkg_test.go:84::TestInterruptedDuringSpilling` (item 698).
#[test]
#[ignore = "go-parity-gap: SQLKiller cancellation during Go sort-partition disk spilling and leak checks are not modeled here"]
fn interrupted_during_spilling_returns_query_interrupted() {}

/// `pkg/executor/sortexec/topn_spill_test.go:383::TestGenerateTopNResultsWhenSpillOnlyOnce` (item 699).
#[test]
#[ignore = "go-parity-gap: the Go TopN helper is driven through package-private chunk DataInDiskByChunks fixtures"]
fn generate_topn_results_after_one_spill_round() {}

/// `pkg/executor/sortexec/topn_spill_test.go:400::TestTopNSpillDisk` (item 700).
#[test]
#[ignore = "go-parity-gap: Go TopN parallel spill uses failpoints, session memory trackers, and mock data-source execution not available at this boundary"]
fn topn_spill_disk_preserves_offset_and_limit() {}

/// `pkg/executor/sortexec/topn_spill_test.go:454::TestTopNSpillDiskFailpoint` (item 701).
#[test]
#[ignore = "go-parity-gap: TopNRandomFail, ParallelSortRandomFail, and ChunkInDiskError are Go failpoints absent from tidb-executor"]
fn topn_spill_failures_close_cleanly() {}

/// `pkg/executor/sortexec/topn_spill_test.go:514::TestIssue54206` (item 702).
#[test]
#[ignore = "go-parity-gap: the regression is an SQL TestKit query over a join and TopN with a session temporary-storage variable"]
fn issue_54206_topn_handles_empty_join_side() {}

/// `pkg/executor/sortexec/topn_spill_test.go:527::TestIssue54541` (item 703).
#[test]
#[ignore = "go-parity-gap: TopN kill-signal handling and temporary-storage cleanup require Go SQLKiller/session state"]
fn issue_54541_topn_kill_signal_is_handled() {}

/// `pkg/executor/sortexec/topn_spill_test.go:556::TestTopNFallBackAction` (item 704).
#[test]
#[ignore = "go-parity-gap: Go memory Tracker ActionOnExceed fallback behavior is not exposed by the Rust TopN API"]
fn topn_fallback_action_runs_after_memory_exhaustion() {}

/// `pkg/executor/split_test.go:41::TestSplitIndex` (item 705).
#[test]
#[ignore = "go-parity-gap: SplitIndexRegionExec and region-split key generation depend on Go tablecodec metadata and TiKV region integration"]
fn split_index_generates_region_keys() {}

/// `pkg/executor/split_test.go:244::TestSplitTable` (item 706).
#[test]
#[ignore = "go-parity-gap: SplitTableRegionExec and table-region key generation are not implemented in tidb-executor"]
fn split_table_generates_region_keys() {}

/// `pkg/executor/split_test.go:320::TestStepShouldLargeThanMinStep` (item 707).
#[test]
#[ignore = "go-parity-gap: the region split minimum-step validation belongs to the unported SplitTableRegionExec"]
fn split_table_rejects_a_step_below_the_minimum() {}

/// `pkg/executor/split_test.go:348::TestClusterIndexSplitTable` (item 708).
#[test]
#[ignore = "go-parity-gap: clustered-handle region split key generation and regionsplit metadata are not modeled by tidb-executor"]
fn clustered_index_split_table_generates_keys() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:31::BenchmarkStatementRUExecStmtSetup` (item 709).
#[test]
#[ignore = "skipped-reason: Go testing.B allocation benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_exec_stmt_setup() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:40::BenchmarkStatementRUNilHooks` (item 710).
#[test]
#[ignore = "skipped-reason: Go testing.B lifecycle-hook benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_nil_hooks() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:58::BenchmarkStatementRUOperatorCalculation` (item 711).
#[test]
#[ignore = "skipped-reason: Go testing.B RU calculation benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_operator_calculation() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:71::BenchmarkStatementRUTreeTraversal` (item 712).
#[test]
#[ignore = "skipped-reason: Go testing.B plan traversal benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_tree_traversal() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:94::BenchmarkStatementRUFinalizePublication` (item 713).
#[test]
#[ignore = "skipped-reason: Go testing.B RU publication benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_finalize_publication() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:118::BenchmarkStatementRUSyntheticTerminal` (item 714).
#[test]
#[ignore = "skipped-reason: Go testing.B synthetic terminal benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_synthetic_terminal() {}

/// `pkg/executor/statement_ru_plan_walk_bench_test.go:152::BenchmarkStatementRUOwnerSetup` (item 715).
#[test]
#[ignore = "skipped-reason: Go testing.B owner-install benchmark; the assigned nextest gate excludes benchmark tests"]
fn benchmark_statement_ru_owner_setup() {}

/// `pkg/executor/statement_ru_plan_walk_integration_test.go:114::TestStatementRUResultSetTerminalOutcomes` (item 716).
#[test]
#[ignore = "go-parity-gap: statement RU owner installation, flat-plan traversal, failpoint terminal paths, and session result-set lifecycle are unported"]
fn statement_ru_result_set_terminal_outcomes() {}

/// `pkg/executor/statement_ru_plan_walk_integration_test.go:424::TestStatementRUFileTransferOutcomeHandoff` (item 717).
#[test]
#[ignore = "go-parity-gap: file-transfer handoff through session ExecStmt state and statement-RU terminal ownership are Go session surfaces"]
fn statement_ru_file_transfer_outcome_handoff() {}

/// `pkg/executor/statement_ru_plan_walk_integration_test.go:570::TestStatementRUPointGetTerminalPlanHandoff` (item 718).
#[test]
#[ignore = "go-parity-gap: prepared point-get terminal plan handoff and statement-RU observation failpoints require tidb-session"]
fn statement_ru_point_get_terminal_plan_handoff() {}

/// `pkg/executor/statement_ru_plan_walk_integration_test.go:643::TestStatementRUScalarSubqueryTerminalLifecycle` (item 719).
#[test]
#[ignore = "go-parity-gap: scalar-subquery flat plans, prepared execution, and RU owner lifecycle are not modeled by tidb-executor"]
fn statement_ru_scalar_subquery_terminal_lifecycle() {}

/// `pkg/executor/statement_ru_plan_walk_integration_test.go:756::TestStatementRUCursorExclusion` (item 720).
#[test]
#[ignore = "go-parity-gap: restricted SQL, cursor result sets, and session status flags govern the Go statement-RU exclusion contract"]
fn statement_ru_cursor_exclusion() {}
