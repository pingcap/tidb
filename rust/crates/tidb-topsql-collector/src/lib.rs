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

//! Go `pkg/util/topsql/collector` (`cpu.go`): the SQL CPU collector, which
//! consumes the global CPU profiler's one-second profiles and sums their
//! samples by the `sql_digest` / `plan_digest` labels statements set.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message as _;
use tidb_cpuprofile::labels::{self, LabelSet};
use tidb_cpuprofile::{ProfileConsumer, ProfileData};
use tidb_proto::perftools::profiles as protos;

const LABEL_SQL_DIGEST: &str = "sql_digest";
const LABEL_PLAN_DIGEST: &str = "plan_digest";
const LABEL_SQL_UID: &str = "sql_global_uid";

/// Go `ProcessCPUTimeUpdater` introduce this interface due to the dependency
/// cycle.
pub trait ProcessCpuTimeUpdater: Send + Sync {
    /// Go `UpdateProcessCPUTime`.
    fn update_process_cpu_time(&self, conn_id: u64, sql_id: u64, cpu_time: Duration);
}

/// Go `Collector` uses to collect SQL execution cpu time.
pub trait Collector: Send + Sync {
    /// Go `Collect` uses to collect the SQL execution cpu time.
    fn collect(&self, stats: Vec<SqlCpuTimeRecord>);
}

/// Go `SQLCPUTimeRecord` represents a single record of how much cpu time a
/// sql plan consumes in one second.
///
/// PlanDigest can be empty, because:
/// 1. some sql statements has no plan, like `COMMIT`
/// 2. when a sql statement is being compiled, there's no plan yet
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlCpuTimeRecord {
    /// Go `SQLDigest`.
    pub sql_digest: Vec<u8>,
    /// Go `PlanDigest`.
    pub plan_digest: Vec<u8>,
    /// Go `CPUTimeMs`.
    pub cpu_time_ms: u32,
}

/// Go `defCollectTickerInterval`.
const DEF_COLLECT_TICKER_INTERVAL: Duration = Duration::from_secs(1);

/// Go `SQLCPUCollector` uses to consume cpu profile from globalCPUProfiler,
/// then parse the SQL CPU usage from the cpu profile data.
pub struct SqlCpuCollector {
    collector: Arc<dyn Collector>,
    updater: Arc<Mutex<Option<Arc<dyn ProcessCpuTimeUpdater>>>>,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl SqlCpuCollector {
    /// Go `NewSQLCPUCollector` create a SQLCPUCollector.
    #[must_use]
    pub fn new(c: Arc<dyn Collector>) -> Self {
        Self {
            collector: c,
            updater: Arc::default(),
            cancel: Arc::default(),
            worker: None,
        }
    }

    /// Go `SetProcessCPUUpdater` sets the updater field.
    pub fn set_process_cpu_updater(&self, updater: Arc<dyn ProcessCpuTimeUpdater>) {
        *self.updater.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(updater);
    }

    /// Go `Start` uses to start to run SQLCPUCollector: it registers a
    /// consumer into globalCPUProfiler while Top SQL is enabled, then
    /// receives cpu profile data per second.
    pub fn start(&mut self) {
        if self.worker.is_some() {
            return;
        }
        self.cancel.store(false, Ordering::SeqCst);
        let collector = Arc::clone(&self.collector);
        let updater = Arc::clone(&self.updater);
        let cancel = Arc::clone(&self.cancel);
        self.worker = std::thread::Builder::new()
            .name("topsql-cpu-collector".to_owned())
            .spawn(move || collect_sql_cpu_loop(collector.as_ref(), &updater, &cancel))
            .ok();
        tidb_logutil::bg_logger().info("sql cpu collector started", &[]);
    }

    /// Go `Stop` uses to stop the SQLCPUCollector.
    pub fn stop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        self.cancel.store(true, Ordering::SeqCst);
        let _ = worker.join();
        tidb_logutil::bg_logger().info("sql cpu collector stopped", &[]);
    }
}

/// Go `collectSQLCPULoop`.
fn collect_sql_cpu_loop(
    collector: &dyn Collector,
    updater: &Mutex<Option<Arc<dyn ProcessCpuTimeUpdater>>>,
    cancel: &AtomicBool,
) {
    let (profile_consumer, receiver) = ProfileConsumer::new(1);
    let mut registered = false;
    while !cancel.load(Ordering::SeqCst) {
        if tidb_topsql_state::top_sql_enabled() {
            if !registered {
                registered = true;
                tidb_cpuprofile::register(&profile_consumer);
            }
        } else if registered {
            registered = false;
            tidb_cpuprofile::unregister(&profile_consumer);
        }
        if let Ok(data) = receiver.recv_timeout(DEF_COLLECT_TICKER_INTERVAL) {
            let updater = updater.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            handle_profile_data(collector, updater.as_deref(), &data);
        }
    }
    if registered {
        tidb_cpuprofile::unregister(&profile_consumer);
    }
}

/// Go `handleProfileData`.
fn handle_profile_data(
    collector: &dyn Collector,
    updater: Option<&dyn ProcessCpuTimeUpdater>,
    data: &ProfileData,
) {
    if data.error.is_some() {
        return;
    }
    let p = match protos::Profile::decode(data.data.as_slice()) {
        Ok(p) => p,
        Err(error) => {
            tidb_logutil::bg_logger().error("parse profile error", &[tidb_logutil::zap_error(error.to_string())]);
            return;
        }
    };
    let stats = parse_cpu_profile_by_sql_labels(&p);
    collector.collect(stats);
    if let Some(updater) = updater {
        parse_cpu_profile_for_process(&p, updater);
    }
}

/// A profile sample's labels as Go's `Sample.Label` map.
fn sample_labels(p: &protos::Profile, sample: &protos::Sample) -> HashMap<String, Vec<String>> {
    let text = |id: i64| p.string_table.get(id as usize).cloned().unwrap_or_default();
    let mut labels: HashMap<String, Vec<String>> = HashMap::new();
    for label in &sample.label {
        labels.entry(text(label.key)).or_default().push(text(label.str));
    }
    labels
}

/// Go `sqlStats`.
#[derive(Default)]
struct SqlStats {
    plans: HashMap<String, i64>,
    total: i64,
}

impl SqlStats {
    /// Go `tune` use to adjust sql stats: the sample data that only carry the
    /// sql_digest label (the optimizer generating the plan) go to the empty
    /// plan digest.
    fn tune(&mut self) {
        if self.plans.is_empty() {
            self.plans.insert(String::new(), self.total);
            return;
        }
        if self.plans.len() == 1 {
            for value in self.plans.values_mut() {
                *value = self.total;
            }
            return;
        }
        let plan_total: i64 = self.plans.values().sum();
        let optimize = self.total - plan_total;
        if optimize <= 0 {
            return;
        }
        *self.plans.entry(String::new()).or_default() += optimize;
    }
}

/// Go `parseCPUProfileBySQLLabels` uses to aggregate the cpu-profile sample
/// data by sql_digest and plan_digest labels.
fn parse_cpu_profile_by_sql_labels(p: &protos::Profile) -> Vec<SqlCpuTimeRecord> {
    let mut sql_map: HashMap<String, SqlStats> = HashMap::new();
    let idx = p.sample_type.len().saturating_sub(1);
    for s in &p.sample {
        let labels = sample_labels(p, s);
        let Some(digests) = labels.get(LABEL_SQL_DIGEST).filter(|digests| !digests.is_empty()) else {
            continue;
        };
        let value = s.value.get(idx).copied().unwrap_or_default();
        for digest in digests {
            let stmt = sql_map.entry(digest.clone()).or_default();
            stmt.total += value;
            for plan in labels.get(LABEL_PLAN_DIGEST).into_iter().flatten() {
                *stmt.plans.entry(plan.clone()).or_default() += value;
            }
        }
    }
    create_sql_stats(sql_map)
}

/// Go `createSQLStats`.
fn create_sql_stats(sql_map: HashMap<String, SqlStats>) -> Vec<SqlCpuTimeRecord> {
    let mut stats = Vec::with_capacity(sql_map.len());
    for (hex_sql_digest, mut stmt) in sql_map {
        stmt.tune();
        let sql_digest = match hex::decode(&hex_sql_digest) {
            Ok(digest) => digest,
            Err(error) => {
                tidb_logutil::bg_logger().error(
                    "decode sql digest failed",
                    &[
                        tidb_log::Field::new("sqlDigest", tidb_log::Value::Str(hex_sql_digest.clone())),
                        tidb_logutil::zap_error(error.to_string()),
                    ],
                );
                continue;
            }
        };
        for (hex_plan_digest, val) in stmt.plans {
            let plan_digest = match hex::decode(&hex_plan_digest) {
                Ok(digest) => digest,
                Err(error) => {
                    tidb_logutil::bg_logger().error(
                        "decode plan digest failed",
                        &[
                            tidb_log::Field::new("planDigest", tidb_log::Value::Str(hex_plan_digest.clone())),
                            tidb_logutil::zap_error(error.to_string()),
                        ],
                    );
                    continue;
                }
            };
            stats.push(SqlCpuTimeRecord {
                sql_digest: sql_digest.clone(),
                plan_digest,
                cpu_time_ms: u32::try_from(Duration::from_nanos(val.max(0) as u64).as_millis()).unwrap_or(u32::MAX),
            });
        }
    }
    stats
}

/// Go `processCPUTimeRecord`.
#[derive(Clone, Copy)]
struct ProcessCpuTimeRecord {
    sql_id: u64,
    total: i64,
}

/// Go `parseCPUProfileForProcess` uses to aggregate the cpu-profile sample
/// data by sql_global_uid labels: the latest statement of each connection.
fn parse_cpu_profile_for_process(p: &protos::Profile, updater: &dyn ProcessCpuTimeUpdater) {
    let mut sql_map: HashMap<u64, ProcessCpuTimeRecord> = HashMap::new();
    let idx = p.sample_type.len().saturating_sub(1);
    // Reverse traverse sample data, since only the latest sqlID for each
    // connection is usable.
    for s in p.sample.iter().rev() {
        let labels = sample_labels(p, s);
        let Some(sql_uids) = labels.get(LABEL_SQL_UID).filter(|uids| !uids.is_empty()) else {
            continue;
        };
        let value = s.value.get(idx).copied().unwrap_or_default();
        for sql_uid in sql_uids {
            let mut keys = sql_uid.split('_');
            let conn_id = keys.next().and_then(|key| key.parse::<u64>().ok()).unwrap_or_default();
            let sql_id = keys.next().and_then(|key| key.parse::<u64>().ok()).unwrap_or_default();
            match sql_map.get_mut(&conn_id) {
                Some(time_record) => {
                    if sql_id < time_record.sql_id {
                        // Ignore previous sql's cpu profile data inside the
                        // same connection.
                        continue;
                    } else if sql_id > time_record.sql_id {
                        // Resets sqlID and total value.
                        time_record.sql_id = sql_id;
                        time_record.total = value;
                    } else {
                        time_record.total += value;
                    }
                }
                None => {
                    sql_map.insert(conn_id, ProcessCpuTimeRecord { sql_id, total: value });
                }
            }
        }
    }
    for (key, val) in sql_map {
        updater.update_process_cpu_time(key, val.sql_id, Duration::from_nanos(val.total.max(0) as u64));
    }
}

/// Go `CtxWithSQLDigest` wrap the ctx with sql digest.
#[must_use]
pub fn ctx_with_sql_digest(ctx: Option<&LabelSet>, sql_digest: &str) -> LabelSet {
    labels::with_labels(ctx, &[(LABEL_SQL_DIGEST, sql_digest.to_owned())])
}

/// Go `CtxWithSQLAndPlanDigest` wrap the ctx with sql digest and plan digest.
#[must_use]
pub fn ctx_with_sql_and_plan_digest(ctx: Option<&LabelSet>, sql_digest: &str, plan_digest: &str) -> LabelSet {
    labels::with_labels(
        ctx,
        &[
            (LABEL_SQL_DIGEST, sql_digest.to_owned()),
            (LABEL_PLAN_DIGEST, plan_digest.to_owned()),
        ],
    )
}

/// Go `CtxWithProcessInfo`: the labels, also set on this thread.
#[must_use]
pub fn ctx_with_process_info(ctx: Option<&LabelSet>, conn_id: u64, sql_id: u64) -> LabelSet {
    let process_label = format!("{conn_id}_{sql_id}");
    let ctx = labels::with_labels(ctx, &[(LABEL_SQL_UID, process_label)]);
    labels::set_goroutine_labels(Some(Arc::clone(&ctx)));
    ctx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(samples: &[(&[(&str, &str)], i64)]) -> protos::Profile {
        let mut strings = vec![String::new(), "samples".to_owned(), "count".to_owned(), "cpu".to_owned(), "nanoseconds".to_owned()];
        let mut id = |text: &str| -> i64 {
            if let Some(position) = strings.iter().position(|existing| existing == text) {
                return position as i64;
            }
            strings.push(text.to_owned());
            strings.len() as i64 - 1
        };
        let sample = samples
            .iter()
            .map(|(labels, nanos)| protos::Sample {
                location_id: Vec::new(),
                value: vec![1, *nanos],
                label: labels
                    .iter()
                    .map(|(key, value)| protos::Label {
                        key: id(key),
                        str: id(value),
                        ..protos::Label::default()
                    })
                    .collect(),
            })
            .collect();
        protos::Profile {
            sample_type: vec![
                protos::ValueType { r#type: 1, unit: 2 },
                protos::ValueType { r#type: 3, unit: 4 },
            ],
            sample,
            string_table: strings,
            ..protos::Profile::default()
        }
    }

    /// Go `TestPProfCPUProfile` / `sqlStats.tune`: CPU is summed per SQL and
    /// plan digest, and SQL-only samples (planning) go to the empty plan.
    #[test]
    fn samples_sum_by_sql_and_plan_digest_like_go() {
        let ms = 1_000_000;
        let p = profile(&[
            (&[("sql_digest", "aa")], 100 * ms),
            (&[("sql_digest", "aa"), ("plan_digest", "01")], 200 * ms),
            (&[("sql_digest", "aa"), ("plan_digest", "02")], 300 * ms),
            (&[("sql_digest", "bb")], 50 * ms),
            (&[("sql_digest", "cc"), ("plan_digest", "03")], 40 * ms),
            (&[("sql_digest", "cc")], 60 * ms),
            (&[("thread", "other")], 999 * ms),
        ]);
        let mut stats = parse_cpu_profile_by_sql_labels(&p);
        stats.sort_by(|a, b| (&a.sql_digest, &a.plan_digest).cmp(&(&b.sql_digest, &b.plan_digest)));
        let record = |sql: &[u8], plan: &[u8], cpu_time_ms: u32| SqlCpuTimeRecord {
            sql_digest: sql.to_vec(),
            plan_digest: plan.to_vec(),
            cpu_time_ms,
        };
        assert_eq!(
            stats,
            [
                record(&[0xaa], &[], 100),
                record(&[0xaa], &[0x01], 200),
                record(&[0xaa], &[0x02], 300),
                record(&[0xbb], &[], 50),
                // One plan takes the SQL's whole total.
                record(&[0xcc], &[0x03], 100),
            ]
        );
    }

    struct Updates(Mutex<Vec<(u64, u64, Duration)>>);

    impl ProcessCpuTimeUpdater for Updates {
        fn update_process_cpu_time(&self, conn_id: u64, sql_id: u64, cpu_time: Duration) {
            self.0.lock().unwrap().push((conn_id, sql_id, cpu_time));
        }
    }

    /// Go `parseCPUProfileForProcess`: only each connection's latest
    /// statement counts.
    #[test]
    fn process_cpu_time_is_the_latest_statement_of_each_connection() {
        let p = profile(&[
            (&[("sql_global_uid", "7_1")], 5),
            (&[("sql_global_uid", "7_2")], 10),
            (&[("sql_global_uid", "7_2")], 20),
            (&[("sql_global_uid", "8_4")], 3),
        ]);
        let updates = Updates(Mutex::default());
        parse_cpu_profile_for_process(&p, &updates);
        let mut updates = updates.0.into_inner().unwrap();
        updates.sort();
        assert_eq!(
            updates,
            [(7, 2, Duration::from_nanos(30)), (8, 4, Duration::from_nanos(3))]
        );
    }
}
