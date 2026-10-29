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

//! Go `pkg/executor/join/hash_join_stats.go`'s `hashJoinRuntimeStatsV2`:
//! what a hash join v2 registers on its plan when it closes
//! (`build_hash_table:{...}, probe:{...}`).

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_execdetails::{format_duration, RuntimeStats, TP_HASH_JOIN_RUNTIME_STATS_V2};

/// Go `spillStats`.
#[derive(Clone, Debug, Default)]
pub struct SpillStats {
    /// Go `round`.
    pub round: usize,
    /// Go `totalSpillBytesPerRound`.
    pub total_spill_bytes_per_round: Vec<i64>,
    /// Go `spilledPartitionNumPerRound`.
    pub spilled_partition_num_per_round: Vec<usize>,
    /// Go `spillBuildRowTableBytesPerRound`.
    pub spill_build_row_table_bytes_per_round: Vec<i64>,
    /// Go `spillBuildHashTableBytesPerRound`.
    pub spill_build_hash_table_bytes_per_round: Vec<i64>,
    /// Go `partitionNum`.
    pub partition_num: usize,
}

/// Go `hashJoinRuntimeStatsV2`. Durations are nanoseconds as in Go.
#[derive(Clone, Debug, Default)]
pub struct HashJoinRuntimeStatsV2 {
    concurrent: usize,
    probe_collision: i64,
    fetch_and_build_hash_table: i64,
    partition_data: i64,
    build_hash_table: i64,
    probe: i64,
    fetch_and_probe: i64,
    worker_fetch_and_probe: i64,
    max_partition_data: i64,
    max_build_hash_table: i64,
    max_probe: i64,
    max_worker_fetch_and_probe: i64,
    max_partition_data_for_current_round: i64,
    max_build_hash_table_for_current_round: i64,
    max_probe_for_current_round: i64,
    max_worker_fetch_and_probe_for_current_round: i64,
    spill: SpillStats,
    is_hash_join_ga: bool,
}

/// The live stats a running join's workers update.
pub type SharedHashJoinRuntimeStatsV2 = Arc<Mutex<HashJoinRuntimeStatsV2>>;

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn fmt(nanos: i64) -> String {
    format_duration(Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)))
}

/// Go `util.ByteToGiB`.
#[expect(clippy::cast_precision_loss, reason = "Go float64(bytes)")]
fn byte_to_gib(bytes: i64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

impl HashJoinRuntimeStatsV2 {
    /// Go `&hashJoinRuntimeStatsV2{concurrent: e.Concurrency}` with
    /// `Open`'s `spill.partitionNum` and `isHashJoinGA`.
    #[must_use]
    pub fn shared(
        concurrent: usize,
        partition_num: usize,
        is_hash_join_ga: bool,
    ) -> SharedHashJoinRuntimeStatsV2 {
        Arc::new(Mutex::new(Self {
            concurrent,
            spill: SpillStats {
                partition_num,
                ..SpillStats::default()
            },
            is_hash_join_ga,
            ..Self::default()
        }))
    }

    /// Go `fetchAndBuildHashTableImpl`'s deferred
    /// `stats.fetchAndBuildHashTable += time.Since(start)`.
    pub fn add_fetch_and_build_hash_table(&mut self, elapsed: Duration) {
        self.fetch_and_build_hash_table += nanos(elapsed);
    }

    /// Go `BuildWorkerV2.updatePartitionData`.
    pub fn update_partition_data(&mut self, cost: Duration) {
        let cost = nanos(cost);
        self.partition_data += cost;
        self.max_partition_data_for_current_round =
            self.max_partition_data_for_current_round.max(cost);
    }

    /// Go `BuildWorkerV2.buildHashTable`'s deferred update.
    pub fn update_build_hash_table(&mut self, cost: Duration) {
        let cost = nanos(cost);
        self.build_hash_table += cost;
        self.max_build_hash_table_for_current_round =
            self.max_build_hash_table_for_current_round.max(cost);
    }

    /// Go `ProbeWorkerV2.updateProbeStatistic`.
    pub fn update_probe_statistic(&mut self, start: Instant, probe_time: Duration) {
        let worker = nanos(start.elapsed());
        let probe = nanos(probe_time);
        self.probe += probe;
        self.worker_fetch_and_probe += worker;
        self.max_probe_for_current_round = self.max_probe_for_current_round.max(probe);
        self.max_worker_fetch_and_probe_for_current_round = self
            .max_worker_fetch_and_probe_for_current_round
            .max(worker);
    }

    /// Go `waitJoinWorkers`' `stats.fetchAndProbe` and `probeCollision`.
    pub fn finish_probe(&mut self, start: Instant, probe_collision: u64) {
        self.fetch_and_probe += nanos(start.elapsed());
        self.probe_collision += i64::try_from(probe_collision).unwrap_or(i64::MAX);
    }

    /// Go `hashJoinRuntimeStatsV2.resetCurrentRound`.
    pub fn reset_current_round(&mut self) {
        self.max_partition_data += self.max_partition_data_for_current_round;
        self.max_build_hash_table += self.max_build_hash_table_for_current_round;
        self.max_probe += self.max_probe_for_current_round;
        self.max_worker_fetch_and_probe += self.max_worker_fetch_and_probe_for_current_round;
        self.max_partition_data_for_current_round = 0;
        self.max_build_hash_table_for_current_round = 0;
        self.max_probe_for_current_round = 0;
        self.max_worker_fetch_and_probe_for_current_round = 0;
    }

    /// Go `HashJoinV2Exec.restoreAndBuild`'s `stats.spill.round`.
    pub fn set_spill_round(&mut self, round: usize) {
        self.spill.round = self.spill.round.max(round);
    }
}

impl RuntimeStats for HashJoinRuntimeStatsV2 {
    /// Go `hashJoinRuntimeStatsV2.String`.
    fn string(&self) -> String {
        let mut buf = String::with_capacity(128);
        if self.fetch_and_build_hash_table > 0 {
            let fetch = self.fetch_and_build_hash_table
                - self.max_build_hash_table
                - self.max_partition_data;
            if self.is_hash_join_ga {
                buf.push_str("build_hash_table:{concurrency:");
                buf.push_str(&self.concurrent.to_string());
                buf.push_str(", time:");
                buf.push_str(&fmt(self.fetch_and_build_hash_table));
                buf.push_str(", fetch:");
                buf.push_str(&fmt(fetch));
                buf.push_str(", max_partition:");
                buf.push_str(&fmt(self.max_partition_data));
                buf.push_str(", total_partition:");
                buf.push_str(&fmt(self.partition_data));
                buf.push_str(", max_build:");
                buf.push_str(&fmt(self.max_build_hash_table));
                buf.push_str(", total_build:");
                buf.push_str(&fmt(self.build_hash_table));
                buf.push('}');
            } else {
                buf.push_str("build_hash_table:{total:");
                buf.push_str(&fmt(self.fetch_and_build_hash_table));
                buf.push_str(", fetch:");
                buf.push_str(&fmt(fetch));
                buf.push_str(", build:");
                buf.push_str(&fmt(self.max_build_hash_table + self.max_partition_data));
                buf.push('}');
            }
        }
        if self.probe > 0 {
            buf.push_str(", probe:{concurrency:");
            buf.push_str(&self.concurrent.to_string());
            if self.is_hash_join_ga {
                buf.push_str(", time:");
                buf.push_str(&fmt(self.fetch_and_probe));
                buf.push_str(", fetch_and_wait:");
                buf.push_str(&fmt(self.fetch_and_probe - self.max_probe));
                buf.push_str(", max_worker_time:");
                buf.push_str(&fmt(self.max_worker_fetch_and_probe));
                buf.push_str(", total_worker_time:");
                buf.push_str(&fmt(self.worker_fetch_and_probe));
                buf.push_str(", max_probe:");
                buf.push_str(&fmt(self.max_probe));
                buf.push_str(", total_probe:");
                buf.push_str(&fmt(self.probe));
            } else {
                buf.push_str(", total:");
                buf.push_str(&fmt(self.fetch_and_probe));
                buf.push_str(", max:");
                buf.push_str(&fmt(self.max_worker_fetch_and_probe));
                buf.push_str(", probe:");
                buf.push_str(&fmt(self.max_probe));
                buf.push_str(", fetch_and_wait:");
                buf.push_str(&fmt(self.fetch_and_probe - self.max_probe));
            }
            if self.probe_collision > 0 {
                buf.push_str(", probe_collision:");
                buf.push_str(&self.probe_collision.to_string());
            }
            buf.push('}');
        }
        if self.spill.round > 0 {
            buf.push_str(", spill:{round:");
            buf.push_str(&self.spill.round.to_string());
            buf.push_str(", spilled_partition_num_per_round:[");
            let per_round = &self.spill.spilled_partition_num_per_round;
            if let Some(first) = per_round.first() {
                buf.push_str(&format!("{first}/{}", self.spill.partition_num));
                for index in 1..per_round.len() {
                    buf.push_str(&format!(
                        " {}/{}",
                        per_round[index],
                        per_round[index - 1] * self.spill.partition_num
                    ));
                }
            }
            buf.push(']');
            let write_bytes = |buf: &mut String, bytes: &[i64]| {
                buf.push('[');
                for (index, value) in bytes.iter().enumerate() {
                    if index > 0 {
                        buf.push(' ');
                    }
                    buf.push_str(&format!("{:.2}", byte_to_gib(*value)));
                }
                buf.push(']');
            };
            buf.push_str(", total_spill_GiB_per_round:");
            write_bytes(&mut buf, &self.spill.total_spill_bytes_per_round);
            buf.push_str(", build_spill_row_table_GiB_per_round:");
            write_bytes(&mut buf, &self.spill.spill_build_row_table_bytes_per_round);
            buf.push_str(", build_spill_hash_table_per_round:");
            write_bytes(&mut buf, &self.spill.spill_build_hash_table_bytes_per_round);
            buf.push('}');
        }
        buf
    }

    /// Go `hashJoinRuntimeStatsV2.Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<HashJoinRuntimeStatsV2>() else {
            return;
        };
        self.fetch_and_build_hash_table += tmp.fetch_and_build_hash_table;
        self.build_hash_table += tmp.build_hash_table;
        self.max_build_hash_table = self.max_build_hash_table.max(tmp.max_build_hash_table);
        self.partition_data += tmp.partition_data;
        self.max_partition_data = self.max_partition_data.max(tmp.max_partition_data);
        self.probe_collision += tmp.probe_collision;
        self.fetch_and_probe += tmp.fetch_and_probe;
        self.probe += tmp.probe;
        self.max_worker_fetch_and_probe = self
            .max_worker_fetch_and_probe
            .max(tmp.max_worker_fetch_and_probe);
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_HASH_JOIN_RUNTIME_STATS_V2
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The GA form Go prints for an inner hash join
    /// (`build_hash_table:{concurrency:5, time:.., fetch:.., max_partition:..,
    /// total_partition:.., max_build:.., total_build:..}, probe:{concurrency:5,
    /// time:.., fetch_and_wait:.., max_worker_time:.., total_worker_time:..,
    /// max_probe:.., total_probe:..}`), the maxima taken per round.
    #[test]
    fn ga_string_has_gos_keys_and_round_maxima() {
        let mut stats = HashJoinRuntimeStatsV2 {
            concurrent: 5,
            is_hash_join_ga: true,
            ..HashJoinRuntimeStatsV2::default()
        };
        stats.add_fetch_and_build_hash_table(Duration::from_micros(624));
        stats.update_partition_data(Duration::from_micros(20));
        stats.update_partition_data(Duration::from_micros(10));
        stats.update_build_hash_table(Duration::from_nanos(957));
        stats.probe = 12_600;
        stats.worker_fetch_and_probe = 3_290_000;
        stats.max_probe_for_current_round = 12_600;
        stats.max_worker_fetch_and_probe_for_current_round = 667_000;
        stats.fetch_and_probe = 668_000;
        stats.reset_current_round();
        assert_eq!(
            stats.string(),
            "build_hash_table:{concurrency:5, time:624µs, fetch:603µs, max_partition:20µs, \
             total_partition:30µs, max_build:957ns, total_build:957ns}, probe:{concurrency:5, \
             time:668µs, fetch_and_wait:655.4µs, max_worker_time:667µs, total_worker_time:3.29ms, \
             max_probe:12.6µs, total_probe:12.6µs}"
        );
    }

    /// The non-GA form (a join outside `IsGAForHashJoinV2`).
    #[test]
    fn non_ga_string() {
        let mut stats = HashJoinRuntimeStatsV2 {
            concurrent: 2,
            ..HashJoinRuntimeStatsV2::default()
        };
        stats.add_fetch_and_build_hash_table(Duration::from_micros(10));
        stats.probe = 1_000;
        stats.fetch_and_probe = 5_000;
        assert_eq!(
            stats.string(),
            "build_hash_table:{total:10µs, fetch:10µs, build:0s}, probe:{concurrency:2, \
             total:5µs, max:0s, probe:0s, fetch_and_wait:5µs}"
        );
    }
}
