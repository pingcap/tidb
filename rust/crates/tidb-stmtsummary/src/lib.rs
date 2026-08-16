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

//! SEED of Go `pkg/util/stmtsummary`, covering `statement_summary.go` and
//! `evicted.go`: the statement-summary LRU keyed by `StmtDigestKey`, the
//! per-digest and per-interval statistics it accumulates, the option surface
//! the system variables drive, and the rollup of the digests the LRU evicts.
//!
//! `AddStatement`'s eviction path reaches the rollup through the named
//! [`statement_summary::EvictedSink`] boundary, which
//! `Arc<Mutex<`[`evicted::StmtSummaryByDigestEvicted`]`>>` implements;
//! [`statement_summary::StmtSummaryByDigestMap::new`] wires that implementation
//! in, as Go's `newStmtSummaryByDigestMap` does.
//! [`statement_summary::NoopEvictedSink`] remains for maps built through
//! [`statement_summary::StmtSummaryByDigestMap::with_sinks`].
//!
//! Not yet covered from the same Go package:
//!
//! - `reader.go` (`stmtSummaryReader`, the column-value factories, and the
//!   `information_schema` row builders). The `*stmtSummaryChecker` parameter
//!   of Go's `collectHistorySummaries` is dropped until that file lands, and
//!   the reader-only helpers (`avgInt`, `avgFloat`, `avgFloat4Uint`,
//!   `avgSumFloat`, `convertEmptyToNil`, `formatBackoffTypes`) are ported as
//!   free functions with no in-crate caller yet.
//!   `evicted.go`'s `(*stmtSummaryByDigestEvicted).collectHistorySummaries`
//!   likewise has no in-crate caller until then.
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
//!
//! Narrowings applied to `evicted.go`:
//!
//! - Go's embedded `sync.Mutex` on `stmtSummaryByDigestEvicted` moves outside
//!   the type: the map holds it as `Arc<Mutex<..>>`, and the inherent methods
//!   take `&mut self`.
//! - Go's `container/list` history becomes a `VecDeque`, and `AddEvicted`'s
//!   carried `h` list cursor becomes an index. Go's detached-node behavior
//!   after a trim (`Prev()` nil, `InsertAfter` a no-op, a match added into an
//!   unreachable node) is observationally equal to dropping the cursor.
//! - `evictedValue.history == nil` collapses into the empty-`VecDeque` case.
//! - `types.MakeDatums` / `types.NewTime` / `mysql.TypeTimestamp` narrow to
//!   `tidb-datatype`'s `Datum`, `Time`, and `TimeType::Timestamp`; Go renders
//!   `time.Unix` in the process-local zone, this crate stays in UTC.

pub mod evicted;
pub mod statement_summary;
