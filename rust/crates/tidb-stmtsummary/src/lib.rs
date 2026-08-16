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

//! Go `pkg/util/stmtsummary`: `statement_summary.go`, `evicted.go`, and
//! `reader.go`. The statement-summary LRU keyed by `StmtDigestKey`, the
//! per-digest and per-interval statistics it accumulates, the option surface
//! the system variables drive, the rollup of the digests the LRU evicts, and
//! the reader that turns both into `information_schema` rows.
//!
//! NOT YET COMPLETE, for one reason: `reader.go`'s own upstream tests
//! (`TestToDatum`, `TestToDatumIAColumns`, `TestToDatumIAColumnsChunkRoundTrip`,
//! `TestColumnValueFactoryDoubleUintMetrics`, `TestAccessPrivilege`) are not
//! ported, so its production surface is present but unpinned. The other two
//! files carry their upstream tests in full, and the assertions those tests
//! had to weaken while the reader was absent are restored.
//!
//! `AddStatement`'s eviction path reaches the rollup through the named
//! [`statement_summary::EvictedSink`] boundary, which
//! `Arc<Mutex<`[`evicted::StmtSummaryByDigestEvicted`]`>>` implements;
//! [`statement_summary::StmtSummaryByDigestMap::new`] wires that implementation
//! in, as Go's `newStmtSummaryByDigestMap` does.
//! [`statement_summary::NoopEvictedSink`] remains for maps built through
//! [`statement_summary::StmtSummaryByDigestMap::with_sinks`].
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
//!
//! Narrowings applied to `reader.go`:
//!
//! - Go's `ssMap *stmtSummaryByDigestMap` pointer becomes the borrow
//!   [`reader::StmtSummaryReader`] carries; the constructor still points it at
//!   [`statement_summary::STMT_SUMMARY_BY_DIGEST_MAP`], and the field stays
//!   public because Go's tests reassign it.
//! - Go reads `summaryMap.Values()`, `beginTimeForCurInterval` and `other`
//!   under one `ssMap` lock; the ported map exposes each as its own locked
//!   accessor, so a concurrent `AddStatement` can interleave between them.
//! - Go drops `ssbd`'s lock before locking an element and then reads `ssbd`'s
//!   immutable-after-init fields unlocked; the port holds the `ssbd` guard
//!   across the element lock, in the order `AddStatement` already uses.
//! - `set.StringSet` narrows to `HashSet<String>` in
//!   [`reader::StmtSummaryChecker`].
//! - Go's package-level `columnValueFactoryMap` becomes the lookup function
//!   [`reader::column_value_factory`]; a factory returns the [`Datum`] that
//!   Go's `types.NewDatum(any)` would build from its `any`.
//!
//!   [`Datum`]: tidb_datatype::Datum
//! - `*time.Location` narrows to `chrono_tz::Tz`; `[]*model.ColumnInfo` and
//!   `*auth.UserIdentity` are the real `tidb_model::ColumnInfo` and
//!   `tidb_parser::auth::UserIdentity`.
//! - `logutil.BgLogger()` has no boundary here: a `plancodec::DecodePlan`
//!   failure is not logged, it only yields Go's empty plan string.

pub mod evicted;
pub mod reader;
pub mod statement_summary;
