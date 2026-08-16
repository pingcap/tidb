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

//! Go `pkg/util/topsql/stmtstats` lands as a complete package: the per-session
//! statement statistics counter (`stmtstats.go`), the Top-RU data types
//! (`rustats.go`), the once-a-second background aggregator (`aggregator.go`),
//! and the kv-dimension execution counter (`kv_exec_count.go`), with all 37 of
//! the package's test functions.
//!
//! A session holds a [`StatementStats`] and reports statement begin/finish
//! through [`StatementObserver`]. Top-SQL counters accumulate into
//! [`StatementStatsMap`]; Top-RU deltas accumulate into [`RuIncrementMap`]
//! keyed by `(user, sql digest, plan digest)`, with begin-based exec counts —
//! an execution contributes at most one count, on its first positive RU delta.
//! The [`Aggregator`] drains every registered session once a second, merges the
//! results, and pushes them to [`Collector`] and [`RuCollector`] subscribers.
//!
//! This module sits alongside `topsql_state`, the flat transcreation of Go
//! `pkg/util/topsql/state`, which is left where it is.
//!
//! # Narrowings
//!
//! Every dependency that lives above `tidb-util` (the workspace's bottom
//! crate) is recovered locally rather than dropped:
//!
//! - **client-go/v2 `util.RUDetails`** → [`RuDetails`], a local struct with the
//!   same five accessors and the same additive merge. `tidb-exec` carries the
//!   identical narrowing as `slow_log_format::RuDetailsSnapshot`.
//! - **`pkg/util/execdetails` `RUV2Metrics`/`RUV2Weights`** → [`RuV2Metrics`]
//!   and [`RuV2Weights`], local snapshots of exactly what `TotalRU` reads.
//!   `execdetails` itself lands in `tidb-exec`, which depends on this crate.
//! - **client-go/v2 `tikvrpc` + `tikvrpc/interceptor`** → [`RpcInterceptor`],
//!   which keeps client-go's wrap-a-handler shape but is generic over the
//!   request, response, and error types, so `kv_exec_count.go` ports in full
//!   instead of being dropped.
//! - **`rmclient.RUVersion`** (PD client) → [`RuVersion`], the same integer
//!   enum with the same zero-is-unspecified normalization.
//! - **`topsql/reporter/metrics`** → dropped telemetry. The two ignore-counters
//!   the RU key cap feeds become [`RuDropStats`], returned from
//!   [`Aggregator::drain_and_push_ru`]; the drop policy is unchanged.
//! - **Go's `context.Context` on `ExecBeginInfo`** exists only for one
//!   `Ctx.Value(util.RUDetailsCtxKey)` lookup at begin time, so that lookup is
//!   hoisted into [`ExecBeginInfo::ru_details`].
//! - **`time.Duration` on `ExecFinishInfo`** is signed and the finish path
//!   treats a negative duration as "no measurement", which
//!   `std::time::Duration` cannot express; [`ExecFinishInfo::exec_duration_ns`]
//!   carries the nanoseconds directly.
//!
//! # Concurrency
//!
//! Go's `sync/atomic` usage is kept where Go uses it and only there:
//! `StatementStats.finished` and the aggregator's `running`/`statsLen`/
//! `lastRUVersion` are atomics; everything Go guards with `StatementStats.mu`
//! sits behind one `Mutex` in [`StatementStatsInner`]. The aggregator's
//! `sync.Map` sets become `Mutex`-guarded vectors that are snapshotted before
//! iteration, matching `sync.Map.Range`'s tolerance of concurrent deletes —
//! `TestAggregatorDrainTailIncrementMatrix` unregisters from another thread
//! while a collector is mid-call. Go's run goroutine plus
//! `context.WithCancel` plus `sync.WaitGroup` becomes one joined thread woken
//! early through a condition variable; the tick body stays reachable as
//! [`Aggregator::aggregate_all`], [`Aggregator::drain_and_push_ru`], and
//! [`Aggregator::drain_and_push_stmt_stats`], which is how the Go tests drive
//! it.
//!
//! Not ported: the package's six Go benchmarks
//! (`BenchmarkExecCountBeginBased*`, `BenchmarkDrainAndPushRU*`), which are
//! performance harnesses rather than tests, and `TestMain`, whose `goleak`
//! goroutine-leak check has no Rust counterpart.

mod aggregator;
mod kv_exec_count;
mod ru_details;
mod rustats;
mod ruv2_metrics;
mod stmtstats;
#[cfg(test)]
mod test_support;

pub use aggregator::{
    bind_ru_version_provider, close_aggregator, register_collector, register_ru_collector,
    setup_aggregator, unregister_collector, unregister_ru_collector, Aggregator, Collector,
    RuCollector, RuDropStats, MAX_RU_KEYS_PER_AGGREGATE, MAX_STMT_STATS_SIZE,
};
pub use kv_exec_count::{KvExecCounter, RpcInterceptor, KV_EXEC_COUNTER_INTERCEPTOR_NAME};
pub use ru_details::RuDetails;
pub use rustats::{
    default_ru_version, normalize_ru_version, ExecutionContext, RuIncrement, RuIncrementMap, RuKey,
    RuVersion, RuVersionProvider,
};
pub use ruv2_metrics::{total_ru, RuV2Metrics, RuV2Weights};
pub use stmtstats::{
    create_statement_stats, new_sql_plan_digest, BinaryDigest, ExecBeginInfo, ExecFinishInfo,
    KvStatementStatsItem, SqlPlanDigest, StatementObserver, StatementStats, StatementStatsInner,
    StatementStatsItem, StatementStatsMap,
};
