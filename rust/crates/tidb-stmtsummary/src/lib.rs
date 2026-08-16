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

//! SEED of Go `pkg/util/stmtsummary`, covering `statement_summary.go`: the
//! statement-summary LRU keyed by `StmtDigestKey`, the per-digest and
//! per-interval statistics it accumulates, and the option surface the system
//! variables drive.
//!
//! Not yet covered from the same Go package:
//!
//! - `evicted.go` (`stmtSummaryByDigestEvicted` and its element/rollup types).
//!   `AddStatement`'s eviction path reaches it through the named
//!   [`statement_summary::EvictedSink`] boundary, whose only implementation
//!   here is [`statement_summary::NoopEvictedSink`].
//! - `reader.go` (`stmtSummaryReader`, the column-value factories, and the
//!   `information_schema` row builders). The `*stmtSummaryChecker` parameter
//!   of Go's `collectHistorySummaries` is dropped until that file lands, and
//!   the reader-only helpers (`avgInt`, `avgFloat`, `avgFloat4Uint`,
//!   `avgSumFloat`, `convertEmptyToNil`, `formatBackoffTypes`) are ported as
//!   free functions with no in-crate caller yet.
//!
//! Narrowings applied to `statement_summary.go` itself:
//!
//! - `pkg/metrics` Prometheus gauges (`metrics.SetStmtSummaryWindowMetrics`)
//!   narrow to the [`statement_summary::WindowMetricsSink`] trait, defaulting
//!   to [`statement_summary::NoopWindowMetricsSink`].
//! - `go.uber.org/atomic` option cells narrow to `std::sync::atomic` types.
//! - `sync.Pool` (`StmtDigestKeyPool`) is dropped; keys are allocated per call.
//! - `github.com/pingcap/failpoint` (`mockTimeForStatementsSummary`) narrows to
//!   the [`statement_summary::StmtSummaryByDigestMap::set_mock_now`] test hook.
//! - `*stmtctx.StatementContext` narrows to
//!   [`statement_summary::StmtSummaryStmtCtx`], carrying only the fields this
//!   file reads.
//! - `execdetails.CopTasksSummary` is not yet in `tidb-exec`, so it is declared
//!   here as [`statement_summary::CopTasksSummary`].
//! - client-go `*util.RUDetails` / `*util.ExecDetails` arrive as the
//!   already-loaded snapshots `tidb_exec::slow_log_format::RuDetailsSnapshot`
//!   and `TikvExecDetailsSnapshot`, so Go's `atomic.LoadInt64` calls become
//!   plain field reads.
//! - Go's `sql[:maxSQLLength]` byte slice becomes a UTF-8 boundary-safe
//!   truncation in [`statement_summary::format_sql`].

pub mod statement_summary;
