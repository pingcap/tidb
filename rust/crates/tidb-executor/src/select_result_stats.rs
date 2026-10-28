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

//! Go `pkg/distsql/select_result.go`'s `selectResultRuntimeStats`: what a
//! select result registers on its root plan when it closes, rendered as
//! `cop_task: {...}, fetch_resp_duration: ..., rpc_info:{...}`.

use std::any::Any;
use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use tidb_distsql::LimiterWaitStats;

use tidb_util::execdetails::runtime_stats::{
    format_duration, CanGetFloat64, Duration, Int64, Percentile, RuntimeStats,
    TP_SELECT_RESULT_RUNTIME_STATS,
};

/// Go `copr.CopRuntimeStats` as `selectResultRuntimeStats.mergeCopRuntimeStats`
/// reads it.
#[derive(Clone, Default)]
pub struct CopResponseSample {
    /// Go `ScanDetail.ProcessedKeys`, `0` without a scan detail.
    pub processed_keys: i64,
    /// Go `BackoffSleep`.
    pub backoff_sleep: BTreeMap<String, StdDuration>,
    /// Go `TimeDetail.ProcessTime`.
    pub process_time: StdDuration,
    /// Go `TimeDetail.WaitTime`.
    pub wait_time: StdDuration,
    /// Go `ReqStats`.
    pub req_stats: Option<tikv_client::RegionRequestRuntimeStats>,
    /// Go `CoprCacheHit`.
    pub copr_cache_hit: bool,
}

/// Go `selectResultRuntimeStats`.
#[derive(Clone, Default)]
pub struct SelectResultRuntimeStats {
    cop_resp_time: Percentile<Duration>,
    proc_keys: Percentile<Int64>,
    backoff_sleep: BTreeMap<String, StdDuration>,
    total_process_time: StdDuration,
    total_wait_time: StdDuration,
    req_stat: Option<tikv_client::RegionRequestRuntimeStats>,
    /// Go `distSQLConcurrency`.
    pub dist_sql_concurrency: i64,
    /// Go `extraConcurrency`.
    pub extra_concurrency: i64,
    copr_cache_hit_num: i64,
    /// Go `storeBatchedNum`.
    pub store_batched_num: u64,
    /// Go `storeBatchedFallbackNum`.
    pub store_batched_fallback_num: u64,
    /// Go `buildTaskDuration`.
    pub build_task_duration: StdDuration,
    /// Go `fetchRspDuration`.
    pub fetch_rsp_duration: StdDuration,
    /// Go `limiterWait`.
    pub limiter_wait: LimiterWaitStats,
}

/// Go `%v` of a `float64` holding a whole number.
fn float_v(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e21 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Go time.Duration(float64) conversion"
)]
fn float_duration(nanos: f64) -> String {
    format_duration(StdDuration::from_nanos((nanos as i64).max(0) as u64))
}

impl SelectResultRuntimeStats {
    /// Go `&selectResultRuntimeStats{distSQLConcurrency: n}`.
    #[must_use]
    pub fn with_concurrency(dist_sql_concurrency: i64) -> Self {
        Self {
            dist_sql_concurrency,
            ..Self::default()
        }
    }

    /// Go `selectResultRuntimeStats.mergeCopRuntimeStats`.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "Go int64(time.Duration) conversion"
    )]
    pub fn merge_cop_runtime_stats(
        &mut self,
        cop_stats: &CopResponseSample,
        resp_time: StdDuration,
    ) {
        self.cop_resp_time
            .add(Duration(resp_time.as_nanos() as i64));
        self.proc_keys.add(Int64(cop_stats.processed_keys));
        for (kind, sleep) in &cop_stats.backoff_sleep {
            *self.backoff_sleep.entry(kind.clone()).or_default() += *sleep;
        }
        self.total_process_time += cop_stats.process_time;
        self.total_wait_time += cop_stats.wait_time;
        if let Some(req_stats) = &cop_stats.req_stats {
            match &self.req_stat {
                None => self.req_stat = Some(req_stats.clone()),
                Some(existing) => existing.merge(req_stats),
            }
        }
        if cop_stats.copr_cache_hit {
            self.copr_cache_hit_num += 1;
        }
    }

    /// Go `selectResultRuntimeStats.calcCacheHit`.
    #[expect(clippy::cast_precision_loss, reason = "Go float64(int64) division")]
    fn calc_cache_hit(&self) -> f64 {
        let hit = self.copr_cache_hit_num;
        let mut total = self.cop_resp_time.size();
        if self.store_batched_num > 0 {
            total += self.store_batched_num as usize;
        }
        if total == 0 {
            return 0.0;
        }
        hit as f64 / total as f64
    }
}

impl RuntimeStats for SelectResultRuntimeStats {
    /// Go `selectResultRuntimeStats.String`.
    fn string(&self) -> String {
        let mut buf = String::new();
        let mut cop_resp_time = self.cop_resp_time.clone();
        let mut proc_keys = self.proc_keys.clone();
        let size = cop_resp_time.size();
        if size > 0 {
            if size == 1 {
                buf.push_str(&format!(
                    "cop_task: {{num: 1, max: {}, proc_keys: {}",
                    float_duration(cop_resp_time.get_percentile(0.0)),
                    float_v(proc_keys.get_percentile(0.0)),
                ));
            } else {
                let v_max = cop_resp_time.get_max();
                let v_min = cop_resp_time.get_min();
                let v_p95 = cop_resp_time.get_percentile(0.95);
                let sum = cop_resp_time.sum();
                let key_max = proc_keys.get_max();
                let key_p95 = proc_keys.get_percentile(0.95);
                buf.push_str(&format!(
                    "cop_task: {{num: {size}, max: {}, min: {}, avg: {}, p95: {}",
                    float_duration(v_max.get_float64()),
                    float_duration(v_min.get_float64()),
                    float_duration(sum / size as f64),
                    float_duration(v_p95),
                ));
                if key_max.0 > 0 {
                    buf.push_str(&format!(
                        ", max_proc_keys: {}, p95_proc_keys: {}",
                        key_max.0, key_p95 as i64
                    ));
                }
            }
            if !self.total_process_time.is_zero() {
                buf.push_str(", tot_proc: ");
                buf.push_str(&format_duration(self.total_process_time));
                if !self.total_wait_time.is_zero() {
                    buf.push_str(", tot_wait: ");
                    buf.push_str(&format_duration(self.total_wait_time));
                }
            }
            if tidb_config::tikvcfg::get_global_config()
                .tikv_client
                .copr_cache
                .capacity_mb
                > 0.0
            {
                buf.push_str(&format!(
                    ", copr_cache_hit_ratio: {:.2}",
                    self.calc_cache_hit()
                ));
            } else {
                buf.push_str(", copr_cache: disabled");
            }
            if !self.build_task_duration.is_zero() {
                buf.push_str(", build_task_duration: ");
                buf.push_str(&format_duration(self.build_task_duration));
            }
            if self.limiter_wait.total_ns != 0 {
                buf.push_str(", limiter_wait:{total:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    self.limiter_wait.total_ns,
                )));
                buf.push_str(", max:");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    self.limiter_wait.max_ns,
                )));
                buf.push('}');
            }
            if self.dist_sql_concurrency > 0 {
                buf.push_str(&format!(
                    ", max_distsql_concurrency: {}",
                    self.dist_sql_concurrency
                ));
            }
            if self.extra_concurrency > 0 {
                buf.push_str(&format!(
                    ", max_extra_concurrency: {}",
                    self.extra_concurrency
                ));
            }
            if self.store_batched_num > 0 {
                buf.push_str(&format!(", store_batch_num: {}", self.store_batched_num));
            }
            if self.store_batched_fallback_num > 0 {
                buf.push_str(&format!(
                    ", store_batch_fallback_num: {}",
                    self.store_batched_fallback_num
                ));
            }
            buf.push('}');
            if !self.fetch_rsp_duration.is_zero() {
                buf.push_str(", fetch_resp_duration: ");
                buf.push_str(&format_duration(self.fetch_rsp_duration));
            }
        }
        let rpc_stats = self
            .req_stat
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !rpc_stats.is_empty() {
            buf.push_str(", rpc_info:{");
            buf.push_str(&rpc_stats);
            buf.push('}');
        }
        if !self.backoff_sleep.is_empty() {
            buf.push_str(", backoff{");
            for (index, (kind, sleep)) in self.backoff_sleep.iter().enumerate() {
                if index > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&format!("{kind}: {}", format_duration(*sleep)));
            }
            buf.push('}');
        }
        buf
    }

    /// Go `selectResultRuntimeStats.Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(other) = other.as_any().downcast_ref::<SelectResultRuntimeStats>() else {
            return;
        };
        self.cop_resp_time.merge_percentile(&other.cop_resp_time);
        self.proc_keys.merge_percentile(&other.proc_keys);
        for (kind, sleep) in &other.backoff_sleep {
            *self.backoff_sleep.entry(kind.clone()).or_default() += *sleep;
        }
        self.total_process_time += other.total_process_time;
        self.total_wait_time += other.total_wait_time;
        if let Some(other_req) = &other.req_stat {
            match &self.req_stat {
                None => self.req_stat = Some(other_req.clone()),
                Some(existing) => existing.merge(other_req),
            }
        }
        self.copr_cache_hit_num += other.copr_cache_hit_num;
        self.dist_sql_concurrency = self.dist_sql_concurrency.max(other.dist_sql_concurrency);
        self.extra_concurrency = self.extra_concurrency.max(other.extra_concurrency);
        self.store_batched_num += other.store_batched_num;
        self.store_batched_fallback_num += other.store_batched_fallback_num;
        self.build_task_duration += other.build_task_duration;
        self.fetch_rsp_duration += other.fetch_rsp_duration;
        self.limiter_wait.merge(other.limiter_wait);
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_SELECT_RESULT_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(keys: i64, process_us: u64, wait_us: u64) -> CopResponseSample {
        let req_stats = tikv_client::RegionRequestRuntimeStats::new();
        req_stats.record_rpc(
            tikv_client::CommandType::Coprocessor,
            StdDuration::from_micros(700),
        );
        CopResponseSample {
            processed_keys: keys,
            process_time: StdDuration::from_micros(process_us),
            wait_time: StdDuration::from_micros(wait_us),
            req_stats: Some(req_stats),
            ..CopResponseSample::default()
        }
    }

    /// Go's `EXPLAIN ANALYZE` of a one-task table reader:
    /// `cop_task: {num: 1, max: 749.1µs, proc_keys: 12, tot_proc: 41.3µs,
    /// tot_wait: 69µs, copr_cache_hit_ratio: 0.00, build_task_duration:
    /// 3.54µs, max_distsql_concurrency: 1}, fetch_resp_duration: 756.1µs,
    /// rpc_info:{Cop:{num_rpc:1, total_time:741.6µs}}`.
    #[test]
    fn one_task_matches_gos_string() {
        let mut stats = SelectResultRuntimeStats {
            dist_sql_concurrency: 1,
            build_task_duration: StdDuration::from_nanos(3_540),
            fetch_rsp_duration: StdDuration::from_nanos(756_100),
            ..SelectResultRuntimeStats::default()
        };
        let mut one = sample(12, 41, 69);
        one.process_time = StdDuration::from_nanos(41_300);
        one.wait_time = StdDuration::from_micros(69);
        stats.merge_cop_runtime_stats(&one, StdDuration::from_nanos(749_100));
        let text = stats.string();
        let expected_head =
            "cop_task: {num: 1, max: 749.1µs, proc_keys: 12, tot_proc: 41.3µs, tot_wait: 69µs, ";
        assert!(text.starts_with(expected_head), "{text}");
        assert!(
            text.ends_with(
                ", build_task_duration: 3.54µs, max_distsql_concurrency: 1}, \
                 fetch_resp_duration: 756.1µs, rpc_info:{Cop:{num_rpc:1, total_time:700µs}}"
            ),
            "{text}"
        );
        assert!(
            text.contains("copr_cache_hit_ratio: 0.00") || text.contains("copr_cache: disabled"),
            "{text}"
        );
    }

    /// Go's two-task form carries min/avg/p95 and the processed-key maxima
    /// (`IndexMerge`'s probe: `cop_task: {num: 2, max: 1.43ms, min: 542.8µs,
    /// avg: 987.1µs, p95: 1.43ms, max_proc_keys: 2, p95_proc_keys: 2, ...`).
    #[test]
    fn two_tasks_print_min_avg_p95_and_key_maxima() {
        let mut stats = SelectResultRuntimeStats::default();
        stats.merge_cop_runtime_stats(&sample(1, 30, 0), StdDuration::from_nanos(1_430_000));
        stats.merge_cop_runtime_stats(&sample(2, 45, 0), StdDuration::from_nanos(542_800));
        let text = stats.string();
        assert!(
            text.starts_with(
                "cop_task: {num: 2, max: 1.43ms, min: 542.8µs, avg: 986.4µs, p95: 1.43ms, \
                 max_proc_keys: 2, p95_proc_keys: 2, tot_proc: 75µs"
            ),
            "{text}"
        );
        assert!(
            text.ends_with("rpc_info:{Cop:{num_rpc:2, total_time:1.4ms}}"),
            "{text}"
        );
    }

    /// Go `Merge` adds counts and keeps the larger concurrency.
    #[test]
    fn merge_adds_samples_and_keeps_max_concurrency() {
        let mut left = SelectResultRuntimeStats {
            dist_sql_concurrency: 1,
            ..SelectResultRuntimeStats::default()
        };
        left.merge_cop_runtime_stats(&sample(1, 1, 0), StdDuration::from_micros(10));
        let mut right = SelectResultRuntimeStats {
            dist_sql_concurrency: 4,
            extra_concurrency: 2,
            ..SelectResultRuntimeStats::default()
        };
        right.merge_cop_runtime_stats(&sample(3, 1, 0), StdDuration::from_micros(20));
        left.merge(&right);
        let text = left.string();
        assert!(text.contains("cop_task: {num: 2, max: 20µs"), "{text}");
        assert!(
            text.contains("max_distsql_concurrency: 4, max_extra_concurrency: 2}"),
            "{text}"
        );
    }
}
