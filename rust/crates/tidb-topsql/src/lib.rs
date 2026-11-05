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

//! Go `pkg/util/topsql/topsql.go`: the process-wide Top SQL pipeline.
//!
//! Go's `ctx` carries the profiler labels and `pprof.SetGoroutineLabels`
//! puts them on the running goroutine; here the attach functions take the
//! parent labels ([`tidb_cpuprofile::labels::goroutine_labels`]) and set the
//! result on the running thread.

use std::sync::{Arc, LazyLock, RwLock};

use tidb_cpuprofile::labels::{set_goroutine_labels, LabelSet};
use tidb_parser::Digest;
use tidb_proto::tipb::top_sql_pub_sub_server::TopSqlPubSubServer;
use tidb_topsql_collector::{ctx_with_process_info, ctx_with_sql_and_plan_digest, ctx_with_sql_digest, ProcessCpuTimeUpdater};
use tidb_topsql_reporter::pubsub::TopSqlPubSubService;
use tidb_topsql_reporter::reporter::{RemoteTopSqlReporter, TopSqlReporter};
use tidb_topsql_reporter::single_target::SingleTargetDataSink;
use tidb_topsql_state::top_sql_enabled;
use tidb_topsql_stmtstats::RuVersionProvider;

/// Go `MaxSQLTextSize` exports for testing.
pub const MAX_SQL_TEXT_SIZE: usize = 4 * 1024;
/// Go `MaxBinaryPlanSize` exports for testing.
pub const MAX_BINARY_PLAN_SIZE: usize = 2 * 1024;

/// Go's `init`: the remote reporter and the single-target sink over it.
struct Globals {
    reporter: RwLock<Arc<dyn TopSqlReporter>>,
    single_target_data_sink: Arc<SingleTargetDataSink>,
}

static GLOBALS: LazyLock<Globals> = LazyLock::new(|| {
    let remote_reporter = RemoteTopSqlReporter::new(
        Box::new(|plan| {
            tidb_plancodec::decode_normalized_plan(plan)
                .map(|plan| String::from_utf8_lossy(&plan).into_owned())
                .map_err(|error| error.to_string())
        }),
        Box::new(tidb_plancodec::compress),
    );
    let single_target_data_sink =
        SingleTargetDataSink::new(Arc::clone(&remote_reporter).data_sink_registerer().expect("remote reporter"));
    Globals { reporter: RwLock::new(remote_reporter), single_target_data_sink }
});

/// Go `globalTopProfilingReport`.
fn global_top_profiling_report() -> Arc<dyn TopSqlReporter> {
    Arc::clone(&GLOBALS.reporter.read().unwrap_or_else(std::sync::PoisonError::into_inner))
}

/// Go `SetupTopProfiling` sets up the Top Profiling pipeline.
///
/// NOTE: Despite the package name, this initializer wires the shared TopSQL
/// and TopRU pipeline.
pub fn setup_top_profiling(
    keyspace_name: Vec<u8>,
    updater: Arc<dyn ProcessCpuTimeUpdater>,
    ru_version_provider: Option<Arc<dyn RuVersionProvider>>,
) {
    let reporter = global_top_profiling_report();
    reporter.bind_keyspace_name(keyspace_name);
    reporter.bind_process_cpu_time_updater(updater);
    Arc::clone(&reporter).start();
    GLOBALS.single_target_data_sink.start();

    tidb_topsql_stmtstats::register_collector(Arc::clone(&reporter) as _);
    if let Some(ru_collector) = Arc::clone(&reporter).ru_collector() {
        tidb_topsql_stmtstats::register_ru_collector(ru_collector);
    }
    tidb_topsql_stmtstats::bind_ru_version_provider(ru_version_provider);
    tidb_topsql_stmtstats::setup_aggregator();
}

/// Go `SetupTopProfilingForTest` sets up the global reporter for tests.
pub fn setup_top_profiling_for_test(r: Arc<dyn TopSqlReporter>) {
    *GLOBALS.reporter.write().unwrap_or_else(std::sync::PoisonError::into_inner) = r;
}

/// Go `RegisterPubSubServer`: the TopSQLPubSubService the gRPC server
/// registers.
#[must_use]
pub fn register_pub_sub_server() -> Option<TopSqlPubSubServer<TopSqlPubSubService>> {
    global_top_profiling_report()
        .data_sink_registerer()
        .map(|register| TopSqlPubSubServer::new(TopSqlPubSubService::new(register)))
}

/// Go `Close` uses to close and release the top sql resource.
pub fn close() {
    let reporter = global_top_profiling_report();
    if let Some(ru_collector) = Arc::clone(&reporter).ru_collector() {
        tidb_topsql_stmtstats::unregister_ru_collector(&ru_collector);
    }
    GLOBALS.single_target_data_sink.close();
    reporter.close();
    tidb_topsql_stmtstats::close_aggregator();
    tidb_topsql_stmtstats::bind_ru_version_provider(None);
}

/// Go `RegisterSQL` uses to register SQL information into Top Profiling.
pub fn register_sql(normalized_sql: &str, sql_digest: Option<&Digest>, is_internal: bool) {
    if let Some(sql_digest) = sql_digest {
        link_sql_text_with_digest(sql_digest.as_bytes(), normalized_sql, is_internal);
    }
}

/// Go `RegisterPlan` uses to register plan information into Top Profiling.
pub fn register_plan(normalized_plan: &str, plan_digest: Option<&Digest>) {
    if let Some(plan_digest) = plan_digest {
        link_plan_text_with_digest(plan_digest.as_bytes(), normalized_plan);
    }
}

/// Go `AttachAndRegisterSQLInfo` attach the sql information into Top
/// Profiling and register the SQL meta information.
pub fn attach_and_register_sql_info(
    ctx: Option<LabelSet>,
    normalized_sql: &str,
    sql_digest: Option<&Digest>,
    is_internal: bool,
) -> Option<LabelSet> {
    let Some(sql_digest) = sql_digest.filter(|digest| !digest.as_str().is_empty()) else {
        return ctx;
    };
    let ctx = ctx_with_sql_digest(ctx.as_ref(), sql_digest.as_str());
    if top_sql_enabled() {
        set_goroutine_labels(Some(Arc::clone(&ctx)));
    }
    link_sql_text_with_digest(sql_digest.as_bytes(), normalized_sql, is_internal);
    Some(ctx)
}

/// Go `AttachSQLAndPlanInfo` attach the sql and plan information into Top
/// Profiling.
pub fn attach_sql_and_plan_info(
    ctx: Option<LabelSet>,
    sql_digest: Option<&Digest>,
    plan_digest: Option<&Digest>,
) -> Option<LabelSet> {
    let Some(sql_digest) = sql_digest.filter(|digest| !digest.as_str().is_empty()) else {
        return ctx;
    };
    let plan_digest_str = plan_digest.map_or("", Digest::as_str);
    let ctx = ctx_with_sql_and_plan_digest(ctx.as_ref(), sql_digest.as_str(), plan_digest_str);
    if top_sql_enabled() {
        set_goroutine_labels(Some(Arc::clone(&ctx)));
    }
    Some(ctx)
}

/// Go `AttachAndRegisterProcessInfo` attach the ProcessInfo into Goroutine
/// labels.
pub fn attach_and_register_process_info(ctx: Option<LabelSet>, conn_id: u64, sql_id: u64) -> LabelSet {
    let ctx = ctx_with_process_info(ctx.as_ref(), conn_id, sql_id);
    if top_sql_enabled() {
        set_goroutine_labels(Some(Arc::clone(&ctx)));
    }
    ctx
}

/// Go `linkSQLTextWithDigest`.
fn link_sql_text_with_digest(sql_digest: &[u8], normalized_sql: &str, is_internal: bool) {
    let mut end = normalized_sql.len().min(MAX_SQL_TEXT_SIZE);
    // Go slices bytes; a cut inside a character keeps its whole prefix here.
    while !normalized_sql.is_char_boundary(end) {
        end -= 1;
    }
    global_top_profiling_report().register_sql(sql_digest, &normalized_sql[..end], is_internal);
}

/// Go `linkPlanTextWithDigest`.
fn link_plan_text_with_digest(plan_digest: &[u8], normalized_binary_plan: &str) {
    global_top_profiling_report().register_plan(
        plan_digest,
        normalized_binary_plan,
        normalized_binary_plan.len() > MAX_BINARY_PLAN_SIZE,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tidb_topsql_collector::SqlCpuTimeRecord;
    use tidb_topsql_stmtstats::StatementStatsMap;

    /// Go `mockReporter`: keeps the registered SQL and plan texts.
    #[derive(Default)]
    struct MockReporter {
        sqls: Mutex<Vec<(Vec<u8>, String, bool)>>,
        plans: Mutex<Vec<(Vec<u8>, usize, bool)>>,
    }

    impl tidb_topsql_collector::Collector for MockReporter {
        fn collect(&self, _stats: Vec<SqlCpuTimeRecord>) {}
    }

    impl tidb_topsql_stmtstats::Collector for MockReporter {
        fn collect_stmt_stats_map(&self, _data: &StatementStatsMap) {}
    }

    impl TopSqlReporter for MockReporter {
        fn start(self: Arc<Self>) {}
        fn register_sql(&self, sql_digest: &[u8], normalized_sql: &str, is_internal: bool) {
            self.sqls.lock().unwrap().push((sql_digest.to_vec(), normalized_sql.to_owned(), is_internal));
        }
        fn register_plan(&self, plan_digest: &[u8], normalized_plan: &str, is_large: bool) {
            self.plans.lock().unwrap().push((plan_digest.to_vec(), normalized_plan.len(), is_large));
        }
        fn bind_process_cpu_time_updater(&self, _updater: Arc<dyn ProcessCpuTimeUpdater>) {}
        fn bind_keyspace_name(&self, _keyspace_name: Vec<u8>) {}
        fn close(&self) {}
    }

    /// Go `TestTopSQLReporter`'s attach path: the SQL digest, then the plan
    /// digest, label the thread while Top SQL is on, and the texts are
    /// registered, cut at Go's size limits.
    #[test]
    fn attach_labels_the_thread_and_registers_texts() {
        let reporter = Arc::new(MockReporter::default());
        setup_top_profiling_for_test(Arc::clone(&reporter) as _);
        tidb_topsql_state::enable_top_sql();

        let sql_digest = Digest::new(b"sql-digest-1".to_vec());
        let plan_digest = Digest::new(b"plan-digest-1".to_vec());
        let long_sql = "x".repeat(MAX_SQL_TEXT_SIZE + 10);
        let ctx = attach_and_register_sql_info(None, &long_sql, Some(&sql_digest), true);
        let labels = tidb_cpuprofile::labels::goroutine_labels().unwrap();
        assert_eq!(labels["sql_digest"], sql_digest.as_str());
        assert!(!labels.contains_key("plan_digest"));
        let ctx = attach_sql_and_plan_info(ctx, Some(&sql_digest), Some(&plan_digest));
        let labels = tidb_cpuprofile::labels::goroutine_labels().unwrap();
        assert_eq!(labels["plan_digest"], plan_digest.as_str());
        assert_eq!(ctx.as_ref(), Some(&labels));
        let ctx = attach_and_register_process_info(ctx, 7, 3);
        assert_eq!(ctx["sql_global_uid"], "7_3");
        assert_eq!(ctx["sql_digest"], sql_digest.as_str());

        register_plan(&"p".repeat(MAX_BINARY_PLAN_SIZE + 1), Some(&plan_digest));
        register_plan("small", Some(&plan_digest));
        register_sql("select 1", None, false);

        let sqls = reporter.sqls.lock().unwrap().clone();
        assert_eq!(sqls, [(sql_digest.as_bytes().to_vec(), "x".repeat(MAX_SQL_TEXT_SIZE), true)]);
        let plans = reporter.plans.lock().unwrap().clone();
        assert_eq!(
            plans,
            [
                (plan_digest.as_bytes().to_vec(), MAX_BINARY_PLAN_SIZE + 1, true),
                (plan_digest.as_bytes().to_vec(), 5, false)
            ]
        );

        // An empty digest attaches nothing.
        assert_eq!(attach_and_register_sql_info(None, "q", Some(&Digest::new(Vec::new())), false), None);
        // With Top SQL off the labels are built but the thread keeps its own.
        tidb_topsql_state::disable_top_sql();
        tidb_cpuprofile::labels::set_goroutine_labels(None);
        let off = attach_sql_and_plan_info(None, Some(&sql_digest), None);
        assert!(off.is_some());
        assert_eq!(tidb_cpuprofile::labels::goroutine_labels(), None);
    }
}
