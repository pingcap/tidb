// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/statistics/handle/autoanalyze/exec`.

use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use chrono::{DateTime, FixedOffset};
use prometheus::{exponential_buckets, CounterVec, Histogram, HistogramOpts, Opts};
use tidb_datatype::Datum;
use tidb_log::{Field, Value};
use tidb_resolve::ResultFieldRef;
use tidb_sqlexec::{
    analyze_snapshot_option, exec_option_analyze_ver2, exec_option_use_current_session,
    partition_prune_mode_option, sys_proc_track_option, OptionFuncAlias, SqlExecError,
    TrackSysProc, UntrackSysProc,
};
use tidb_stats_handle_util::{AutoAnalyzeProcIdGenerator, AutoAnalyzeTracker, STATS_CONTEXT};
use tidb_util::sqlescape::SqlArg;

const FULL_DAY_TIME_FORMAT: &str = "%H:%M %z";

static AUTO_ANALYZE_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "tidb_statistics_auto_analyze_duration_seconds",
            "Bucketed histogram of processing time (s) of auto analyze.",
        )
        .buckets(exponential_buckets(0.01, 2.0, 24).expect("valid auto-analyze buckets")),
    )
    .expect("valid auto-analyze histogram");
    prometheus::default_registry()
        .register(Box::new(histogram.clone()))
        .expect("register auto-analyze histogram");
    histogram
});

static AUTO_ANALYZE_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    let counter = CounterVec::new(
        Opts::new(
            "tidb_statistics_auto_analyze_total",
            "Counter of auto analyze.",
        ),
        &["type"],
    )
    .expect("valid auto-analyze counter");
    prometheus::default_registry()
        .register(Box::new(counter.clone()))
        .expect("register auto-analyze counter");
    counter
});

/// The additional session state consumed by Go `RunAnalyzeStmt`.
pub trait AutoAnalyzeSessionContext: tidb_syssession::SessionContext {
    /// Go `SessionVars.PartitionPruneMode.Load`.
    fn partition_prune_mode(&self) -> String;
    /// Go `SessionVars.EnableAnalyzeSnapshot`.
    fn enable_analyze_snapshot(&self) -> bool;
}

struct ProcessIdGuard<'a, G: AutoAnalyzeProcIdGenerator + ?Sized> {
    generator: &'a G,
    id: u64,
}

impl<G: AutoAnalyzeProcIdGenerator + ?Sized> Drop for ProcessIdGuard<'_, G> {
    fn drop(&mut self) {
        self.generator.release_auto_analyze_proc_id(self.id);
    }
}

fn exec_options<C: AutoAnalyzeSessionContext + ?Sized>(
    context: &C,
    generator: &dyn AutoAnalyzeProcIdGenerator,
    track: TrackSysProc,
    untrack: UntrackSysProc,
    stats_version: i32,
    need_version_rewrite_warn: bool,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> (u64, Vec<OptionFuncAlias>) {
    let escaped = need_version_rewrite_warn.then(|| {
        tidb_util::sqlescape::escape_sql(sql, arguments)
            .ok()
            .and_then(|sql| String::from_utf8(sql).ok())
            .unwrap_or_else(|| sql.to_owned())
    });
    record_auto_analyze_version(
        stats_version,
        need_version_rewrite_warn,
        escaped.as_deref().unwrap_or(sql),
    );
    let process_id = generator.auto_analyze_proc_id();
    let tracker = Arc::new(AutoAnalyzeTracker::new(track, untrack));
    let track = {
        let tracker = Arc::clone(&tracker);
        Arc::new(move |id, process| tracker.track(id, process)) as TrackSysProc
    };
    let untrack = Arc::new(move |id| tracker.untrack(id)) as UntrackSysProc;
    (
        process_id,
        vec![
            Arc::new(exec_option_analyze_ver2),
            analyze_snapshot_option(context.enable_analyze_snapshot()),
            partition_prune_mode_option(context.partition_prune_mode()),
            Arc::new(exec_option_use_current_session),
            sys_proc_track_option(process_id, track, untrack),
        ],
    )
}

/// Applies Go `execOptionForAnalyzeVersion` to an already rendered statement.
pub fn record_auto_analyze_version(
    stats_version: i32,
    need_version_rewrite_warn: bool,
    rendered_sql: &str,
) {
    if need_version_rewrite_warn {
        tidb_stats_handle_logutil::stats_logger().warn(
            "auto analyze rewrites legacy statistics version 1 to version 2",
            &[Field::new("sql", Value::Str(rendered_sql.to_owned()))],
        );
    }
    debug_assert_eq!(stats_version, 2, "auto analyze should use stats version 2");
}

/// Go `RunAnalyzeStmt`.
pub fn run_analyze_stmt<C: AutoAnalyzeSessionContext + ?Sized>(
    context: &C,
    generator: &dyn AutoAnalyzeProcIdGenerator,
    track: TrackSysProc,
    untrack: UntrackSysProc,
    stats_version: i32,
    need_version_rewrite_warn: bool,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
    let (process_id, options) = exec_options(
        context,
        generator,
        track,
        untrack,
        stats_version,
        need_version_rewrite_warn,
        sql,
        arguments,
    );
    let _release = ProcessIdGuard {
        generator,
        id: process_id,
    };
    match catch_unwind(AssertUnwindSafe(|| {
        context.restricted_sql_executor().exec_restricted_sql(
            &*STATS_CONTEXT,
            &options,
            sql,
            arguments,
        )
    })) {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            tidb_util::logutil::bg_logger().warn(
                "panic in execAnalyzeStmt",
                &[
                    Field::new("error", Value::Str(message.to_owned())),
                    Field::new("stack", Value::Str(Backtrace::force_capture().to_string())),
                ],
            );
            Ok((Vec::new(), Vec::new()))
        }
    }
}

/// Go `AutoAnalyze`.
pub fn auto_analyze<C: AutoAnalyzeSessionContext + ?Sized>(
    context: &C,
    generator: &dyn AutoAnalyzeProcIdGenerator,
    track: TrackSysProc,
    untrack: UntrackSysProc,
    stats_version: i32,
    need_version_rewrite_warn: bool,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> bool {
    let start = Instant::now();
    let result = run_analyze_stmt(
        context,
        generator,
        track,
        untrack,
        stats_version,
        need_version_rewrite_warn,
        sql,
        arguments,
    );
    let elapsed = start.elapsed();
    AUTO_ANALYZE_HISTOGRAM.observe(elapsed.as_secs_f64());
    match result {
        Ok(_) => {
            AUTO_ANALYZE_COUNTER.with_label_values(&["succ"]).inc();
            true
        }
        Err(error) => {
            let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)
                .ok()
                .and_then(|sql| String::from_utf8(sql).ok())
                .unwrap_or_default();
            tidb_stats_handle_logutil::stats_err_verbose_sample_logger().error(
                "auto analyze failed",
                &[
                    Field::new("sql", Value::Str(escaped)),
                    Field::new(
                        "cost_time",
                        Value::Duration(i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)),
                    ),
                    Field::new("error", Value::Str(error.to_string())),
                ],
            );
            AUTO_ANALYZE_COUNTER.with_label_values(&["failed"]).inc();
            false
        }
    }
}

/// Go `GetAutoAnalyzeParameters`.
pub fn get_auto_analyze_parameters<C: AutoAnalyzeSessionContext + ?Sized>(
    context: &C,
) -> HashMap<String, String> {
    let sql = "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)";
    let arguments = [
        SqlArg::from(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO),
        SqlArg::from(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_START_TIME),
        SqlArg::from(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_END_TIME),
    ];
    let Ok((rows, _)) = context.restricted_sql_executor().exec_restricted_sql(
        &*STATS_CONTEXT,
        &[],
        sql,
        &arguments,
    ) else {
        return HashMap::new();
    };
    rows.into_iter()
        .filter_map(|row| {
            let name = row.first()?.as_string()?.as_utf8().ok()?.to_owned();
            let value = row.get(1)?.as_string()?.as_utf8().ok()?.to_owned();
            Some((name, value))
        })
        .collect()
}

/// Go `ParseAutoAnalyzeRatio`.
pub fn parse_auto_analyze_ratio(ratio: &str) -> f64 {
    ratio
        .parse::<f64>()
        .map_or(tidb_vardef::defaults::DEF_AUTO_ANALYZE_RATIO, |ratio| {
            if ratio.is_nan() {
                ratio
            } else {
                ratio.max(0.0)
            }
        })
}

/// Go `ParseAutoAnalysisWindow`.
pub fn parse_auto_analysis_window(
    start: &str,
    end: &str,
) -> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), chrono::ParseError> {
    let start = if start.is_empty() {
        tidb_vardef::defaults::DEF_AUTO_ANALYZE_START_TIME
    } else {
        start
    };
    let end = if end.is_empty() {
        tidb_vardef::defaults::DEF_AUTO_ANALYZE_END_TIME
    } else {
        end
    };
    let parse = |value: &str| {
        let parsed = DateTime::parse_from_str(
            &format!("1970-01-01 {value}"),
            &format!("%Y-%m-%d {FULL_DAY_TIME_FORMAT}"),
        )?;
        Ok(parsed)
    };
    Ok((parse(start)?, parse(end)?))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_datatype::{Collation, StringDatum};
    use tidb_sqlexec::{
        exec_option, ExecutionContext, RecordSet, RestrictedSqlExecutor, SqlExecutor, TrackProcess,
    };
    use tidb_sqlexec_mock::MockRestrictedSqlExecutor;
    use tidb_stats_handle_util::Generator;
    use tidb_syssession::SessionContext;

    use super::*;

    struct NoopSqlExecutor;

    impl SqlExecutor for NoopSqlExecutor {
        fn execute(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
        ) -> tidb_sqlexec::Result<Vec<Box<dyn RecordSet>>> {
            unreachable!("ordinary SQL is not used by auto-analyze exec")
        }

        fn execute_internal(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
            unreachable!("ordinary SQL is not used by auto-analyze exec")
        }

        fn execute_stmt(
            &self,
            _context: &dyn ExecutionContext,
            _statement: &tidb_ast::Stmt,
        ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
            unreachable!("ordinary SQL is not used by auto-analyze exec")
        }
    }

    struct Context {
        restricted: Arc<dyn RestrictedSqlExecutor>,
        registered: AtomicBool,
        prune_mode: String,
        analyze_snapshot: bool,
    }

    impl Context {
        fn new(restricted: Arc<dyn RestrictedSqlExecutor>) -> Self {
            Self {
                restricted,
                registered: AtomicBool::new(false),
                prune_mode: "dynamic".to_owned(),
                analyze_snapshot: true,
            }
        }
    }

    impl SessionContext for Context {
        fn close(&self) {}
        fn rollback_txn(&self, _context: &dyn ExecutionContext) {}
        fn has_prepared_txn_future(&self) -> bool {
            false
        }
        fn txn_valid(&self) -> Result<bool, SqlExecError> {
            Ok(false)
        }
        fn sql_executor(&self) -> Arc<dyn SqlExecutor> {
            Arc::new(NoopSqlExecutor)
        }
        fn restricted_sql_executor(&self) -> Arc<dyn RestrictedSqlExecutor> {
            Arc::clone(&self.restricted)
        }
        fn register_internal_session(&self) {
            self.registered.store(true, Ordering::SeqCst);
        }
        fn unregister_internal_session(&self) {
            self.registered.store(false, Ordering::SeqCst);
        }
        fn contains_internal_session(&self) -> bool {
            self.registered.load(Ordering::SeqCst)
        }
        fn store_internal_session(&self) -> bool {
            self.registered.swap(true, Ordering::SeqCst)
        }
    }

    impl AutoAnalyzeSessionContext for Context {
        fn partition_prune_mode(&self) -> String {
            self.prune_mode.clone()
        }

        fn enable_analyze_snapshot(&self) -> bool {
            self.analyze_snapshot
        }
    }

    fn callbacks(
        tracked: Arc<Mutex<Vec<u64>>>,
        untracked: Arc<Mutex<Vec<u64>>>,
    ) -> (TrackSysProc, UntrackSysProc) {
        let track = Arc::new(move |id, _context: Arc<dyn TrackProcess>| {
            tracked.lock().unwrap().push(id);
            Ok(())
        }) as TrackSysProc;
        let untrack = Arc::new(move |id| untracked.lock().unwrap().push(id)) as UntrackSysProc;
        (track, untrack)
    }

    #[test]
    fn source_exec_auto_analyzes_with_the_complete_option_set() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted
            .expect()
            .exec_restricted_sql(|_, options, sql, arguments| {
                assert_eq!(sql, "analyze table %n");
                assert_eq!(arguments.len(), 1);
                let option = exec_option(options);
                assert_eq!(option.analyze_ver, 2);
                assert_eq!(option.analyze_snapshot, Some(true));
                assert_eq!(option.partition_prune_mode, "dynamic");
                assert!(option.use_cur_session);
                assert_eq!(option.track_sys_proc_id, 41);
                let track = option.track_sys_proc.expect("tracking callback");
                let untrack = option.untrack_sys_proc.expect("untracking callback");
                track(41, Arc::new(())).unwrap();
                untrack(41);
                Ok((Vec::new(), Vec::new()))
            });
        let restricted_executor: Arc<dyn RestrictedSqlExecutor> = restricted.clone();
        let context = Context::new(restricted_executor);
        let released = Arc::new(AtomicU64::new(0));
        let released_by_generator = Arc::clone(&released);
        let generator = Generator::new(
            || 41,
            move |id| released_by_generator.store(id, Ordering::SeqCst),
        );
        let tracked = Arc::new(Mutex::new(Vec::new()));
        let untracked = Arc::new(Mutex::new(Vec::new()));
        let (track, untrack) = callbacks(Arc::clone(&tracked), Arc::clone(&untracked));

        assert!(auto_analyze(
            &context,
            &generator,
            track,
            untrack,
            2,
            false,
            "analyze table %n",
            &[SqlArg::from("t")],
        ));
        assert_eq!(*tracked.lock().unwrap(), [41]);
        assert_eq!(*untracked.lock().unwrap(), [41]);
        assert_eq!(released.load(Ordering::SeqCst), 41);
    }

    #[test]
    fn source_legacy_rewrite_still_executes_as_version_two() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted.expect().exec_restricted_sql(|_, options, _, _| {
            assert_eq!(exec_option(options).analyze_ver, 2);
            Ok((Vec::new(), Vec::new()))
        });
        let context = Context::new(restricted);
        let generator = Generator::new(|| 42, |_| {});
        let (track, untrack) = callbacks(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        );
        assert!(auto_analyze(
            &context,
            &generator,
            track,
            untrack,
            2,
            true,
            "analyze table %n partition %n",
            &[SqlArg::from("pt"), SqlArg::from("p0")],
        ));
    }

    #[deny(unused_must_use)]
    #[test]
    fn source_return_values_may_be_ignored_like_go() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted
            .expect()
            .exec_restricted_sql(|_, _, _, _| Ok((Vec::new(), Vec::new())));
        let context = Context::new(restricted);
        let generator = Generator::new(|| 45, |_| {});
        let (track, untrack) = callbacks(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        );

        auto_analyze(
            &context,
            &generator,
            track,
            untrack,
            2,
            false,
            "analyze table %n",
            &[SqlArg::from("t")],
        );
        parse_auto_analyze_ratio("0.5");
    }

    #[test]
    fn source_kill_in_windows_propagates_the_interruption_and_releases_the_id() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted.expect().exec_restricted_sql(|_, _, _, _| {
            Err(Box::new(std::io::Error::other(
                "[executor:1317]Query execution was interrupted",
            )))
        });
        let context = Context::new(restricted);
        let released = Arc::new(AtomicU64::new(0));
        let released_by_generator = Arc::clone(&released);
        let generator = Generator::new(
            || 43,
            move |id| released_by_generator.store(id, Ordering::SeqCst),
        );
        let (track, untrack) = callbacks(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        );
        let error = run_analyze_stmt(
            &context,
            &generator,
            track,
            untrack,
            2,
            false,
            "analyze table %n",
            &[SqlArg::from("t1")],
        )
        .expect_err("window killer interrupts the restricted statement");
        assert!(error
            .to_string()
            .contains("Query execution was interrupted"));
        assert_eq!(released.load(Ordering::SeqCst), 43);
    }

    #[test]
    fn panic_recovery_returns_go_zero_values_and_releases_the_id() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted
            .expect()
            .exec_restricted_sql(|_, _, _, _| panic!("analyze executor panic"));
        let context = Context::new(restricted);
        let released = Arc::new(AtomicU64::new(0));
        let released_by_generator = Arc::clone(&released);
        let generator = Generator::new(
            || 44,
            move |id| released_by_generator.store(id, Ordering::SeqCst),
        );
        let (track, untrack) = callbacks(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
        );

        let (rows, fields) = run_analyze_stmt(
            &context,
            &generator,
            track,
            untrack,
            2,
            false,
            "analyze table %n",
            &[SqlArg::from("t")],
        )
        .expect("Go's deferred recovery leaves unnamed returns at zero values");
        assert!(rows.is_empty());
        assert!(fields.is_empty());
        assert_eq!(released.load(Ordering::SeqCst), 44);
    }

    #[test]
    fn parameters_ratio_and_window_match_go() {
        let restricted = Arc::new(MockRestrictedSqlExecutor::new());
        restricted
            .expect()
            .exec_restricted_sql(|_, _, _, arguments| {
                assert_eq!(arguments.len(), 3);
                let string = |value: &str| {
                    Datum::String(StringDatum::new(value.as_bytes(), Collation::Utf8Mb4Bin))
                };
                Ok((
                    vec![
                        vec![
                            string(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO),
                            string("0.75"),
                        ],
                        vec![
                            string(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_START_TIME),
                            string("01:00 +0800"),
                        ],
                        vec![
                            string(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_END_TIME),
                            string("02:00 +0800"),
                        ],
                    ],
                    Vec::new(),
                ))
            });
        let context = Context::new(restricted);
        let parameters = get_auto_analyze_parameters(&context);
        assert_eq!(
            parameters[tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO],
            "0.75"
        );
        assert_eq!(parse_auto_analyze_ratio("bad"), 0.5);
        assert_eq!(parse_auto_analyze_ratio("-1"), 0.0);
        assert!(parse_auto_analyze_ratio("NaN").is_nan());
        let (start, end) = parse_auto_analysis_window("01:00 +0800", "02:00 +0800").unwrap();
        assert_eq!(
            start.format(FULL_DAY_TIME_FORMAT).to_string(),
            "01:00 +0800"
        );
        assert_eq!(end.format(FULL_DAY_TIME_FORMAT).to_string(), "02:00 +0800");
        let (start, end) = parse_auto_analysis_window("", "").unwrap();
        assert_eq!(
            start.format(FULL_DAY_TIME_FORMAT).to_string(),
            "00:00 +0000"
        );
        assert_eq!(end.format(FULL_DAY_TIME_FORMAT).to_string(), "23:59 +0000");
    }
}
