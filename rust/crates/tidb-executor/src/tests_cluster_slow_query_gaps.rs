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

//! Gap tests for Go `pkg/executor/cluster_table_test.go`: the
//! `information_schema.slow_query` / `cluster_slow_query` and statement
//! summary surfaces. Go reads real slow-log FILES through the memtable
//! readers (`pkg/executor/slow_query.go`, with
//! `DashboardSlowLogReadBlockCnt4Test` at `slow_query.go:416` counting
//! dashboard block reads) and serves the CLUSTER variant through a real
//! RPC server (`cluster_table_test.go:44 createRPCServer`). This tier has
//! neither a slow-query memtable reader nor an infoschema RPC fan-out, so
//! the four contracts are recorded as gaps.

/// Go `pkg/executor/cluster_table_test.go:85::TestClusterTableSlowQuery`:
/// slow-log files named by timestamp and the current `tidb-slow-query.log`
/// are filtered by `time` ranges and time-zone conversion (Asia/Shanghai,
/// `+00:00`, `+02:00`), with identical answers for `slow_query` and
/// `cluster_slow_query`, and the dashboard pattern
/// (`FROM_UNIXTIME` BETWEEN + ORDER BY Time DESC LIMIT 2) reads exactly
/// three log blocks.
#[test]
#[ignore = "go-parity-gap: no slow_query memtable reader over log files and no cluster RPC server; pkg/executor/slow_query.go has no Rust counterpart"]
fn cluster_table_slow_query_filters_log_files_by_time() {}

/// Go `pkg/executor/cluster_table_test.go:222::TestIssue20236`: a slow-log
/// line SPLIT across two rotated files (query text separated from its `#
/// Time` header, final line without trailing newline) must still parse into
/// one record, for both plain and gzip-compressed rotations, and the
/// time-range/order/limit queries above must agree.
#[test]
#[ignore = "go-parity-gap: no slow-log file scanning (plain or gz) on this tier; the record-splicing fix behind issue 20236 lives in pkg/executor/slow_query.go"]
fn issue_20236_splices_slow_log_records_split_across_rotated_files() {}

/// Go `pkg/executor/cluster_table_test.go:334::TestSQLDigestTextRetriever`:
/// `expression.SQLDigestTextRetriever.RetrieveLocal` fills the normalized
/// statement text for a digest present in the local statement summary
/// (`tidb_enable_stmt_summary = 1`) and leaves unknown digests empty.
#[test]
#[ignore = "go-parity-gap: no statement-summary engine for the digest -> SQL text lookup on this tier"]
fn sql_digest_text_retriever_fills_local_statements_only() {}

/// Go
/// `pkg/executor/cluster_table_test.go:396::TestClusterTableSlowQuerySessionConnectAttrs`:
/// the `Session_connect_attrs` column is served through
/// cluster_slow_query too, carrying the session's default connection
/// attributes (`pkg/executor/internal/testutil` slow-log line).
#[test]
#[ignore = "go-parity-gap: no cluster_slow_query surface; Session_connect_attrs parsing lives in the unported slow-query reader"]
fn cluster_table_slow_query_serves_session_connect_attrs() {}
