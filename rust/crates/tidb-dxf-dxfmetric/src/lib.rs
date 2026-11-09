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


//! Go `pkg/dxf/framework/dxfmetric`: the distributed execution framework's
//! metrics (`metric.go`) and its task and subtask collector
//! (`collector.go`).

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use prometheus::core::{Collector as PromCollector, Desc};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{CounterVec, GaugeVec, Opts};
use tidb_dxf::{SubtaskBase, TaskBase, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING};

const NAMESPACE_TIDB: &str = "tidb";
const SUBSYSTEM_DXF: &str = "dxf";
const LBL_TYPE: &str = "type";
const LBL_EVENT: &str = "event";
const LBL_STATE: &str = "state";

/// Go `LblTaskID`.
pub const LBL_TASK_ID: &str = "task_id";

/// Go's schedule and execute event names.
pub const EVENT_SUBTASK_SCHEDULED_AWAY: &str = "subtask-scheduled-away";
/// Go `EventSubtaskRerun`.
pub const EVENT_SUBTASK_RERUN: &str = "subtask-rerun";
/// Go `EventSubtaskSlow`.
pub const EVENT_SUBTASK_SLOW: &str = "subtask-slow";
/// Go `EventRetry`.
pub const EVENT_RETRY: &str = "retry";
/// Go `EventTooManyIdx`.
pub const EVENT_TOO_MANY_IDX: &str = "too-many-idx";
/// Go `EventMergeSort`.
pub const EVENT_MERGE_SORT: &str = "merge-sort";
/// Go `EventCleanupFailed`.
pub const EVENT_CLEANUP_FAILED: &str = "cleanup-failed";
/// Go `EventMeterWriteFailed`.
pub const EVENT_METER_WRITE_FAILED: &str = "meter-write-failed";

fn register<C: PromCollector + Clone + 'static>(collector: prometheus::Result<C>) -> C {
    let collector = collector.expect("valid metric definition");
    prometheus::default_registry()
        .register(Box::new(collector.clone()))
        .expect("metric registered once");
    collector
}

/// Go `UsedSlotsGauge`, which `tidb-metrics` registers with the global-sort
/// families.
pub static USED_SLOTS_GAUGE: &LazyLock<GaugeVec> = &tidb_metrics::globalsort::USED_SLOTS;

/// Go `WorkerCount`.
pub static WORKER_COUNT: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("worker_count", "Gauge of DXF worker count.").namespace(NAMESPACE_TIDB).subsystem(SUBSYSTEM_DXF),
        &[LBL_TYPE],
    ))
});

/// Go `FinishedTaskCounter`.
pub static FINISHED_TASK_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("finished_task_total", "Counter of finished DXF tasks.")
            .namespace(NAMESPACE_TIDB)
            .subsystem(SUBSYSTEM_DXF),
        &[LBL_STATE],
    ))
});

/// Go `ScheduleEventCounter`.
pub static SCHEDULE_EVENT_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("schedule_event_total", "Counter of DXF schedule events fo tasks.")
            .namespace(NAMESPACE_TIDB)
            .subsystem(SUBSYSTEM_DXF),
        &[LBL_TASK_ID, LBL_EVENT],
    ))
});

/// Go `ExecuteEventCounter`: by task id rather than subtask id, to keep the
/// series few.
pub static EXECUTE_EVENT_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("execute_event_total", "Counter of DXF execute events fo tasks.")
            .namespace(NAMESPACE_TIDB)
            .subsystem(SUBSYSTEM_DXF),
        &[LBL_TASK_ID, LBL_EVENT],
    ))
});

/// Go `InitDistTaskMetrics` and `Register`: the statics register themselves
/// on first use; this forces them.
pub fn init_dist_task_metrics() {
    LazyLock::force(USED_SLOTS_GAUGE);
    LazyLock::force(&WORKER_COUNT);
    LazyLock::force(&FINISHED_TASK_COUNTER);
    LazyLock::force(&SCHEDULE_EVENT_COUNTER);
    LazyLock::force(&EXECUTE_EVENT_COUNTER);
}

/// Go `Collector`: a custom collector of the tasks and subtasks it was last
/// given. A subtask's `exec_id` can change and finished subtasks move to the
/// history table, which the built-in vectors could follow only by deleting
/// series; rebuilding them per collection avoids that.
pub struct Collector {
    info: Mutex<(Option<Vec<TaskBase>>, Option<Vec<SubtaskBase>>)>,
    tasks: Desc,
    subtasks: Desc,
    subtask_duration: Desc,
}

fn desc(name: &str, help: &str, labels: &[&str], const_labels: &HashMap<String, String>) -> Desc {
    Desc::new(
        name.to_owned(),
        help.to_owned(),
        labels.iter().map(|label| (*label).to_owned()).collect(),
        const_labels.clone(),
    )
    .expect("valid metric description")
}

/// Go `prometheus.MustNewConstMetric(desc, GaugeValue, value, labels...)`.
fn gauge(desc: &Desc, value: f64, label_values: &[String]) -> Metric {
    let mut labels: Vec<LabelPair> = desc
        .const_label_pairs
        .iter()
        .cloned()
        .chain(desc.variable_labels.iter().zip(label_values).map(|(name, value)| {
            let mut pair = LabelPair::default();
            pair.set_name(name.clone());
            pair.set_value(value.clone());
            pair
        }))
        .collect();
    labels.sort_by(|left, right| left.name().cmp(right.name()));
    let mut gauge = Gauge::default();
    gauge.set_value(value);
    let mut metric = Metric::default();
    metric.set_label(labels);
    metric.set_gauge(gauge);
    metric
}

fn family(desc: &Desc, metrics: Vec<Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(desc.fq_name.clone());
    family.set_help(desc.help.clone());
    family.set_field_type(MetricType::GAUGE);
    *family.mut_metric() = metrics;
    family
}

impl Collector {
    /// Go `NewCollector`. `const_labels` is Go's test-only `server_id`
    /// label, which keeps several domains in one process apart.
    #[must_use]
    pub fn new(const_labels: HashMap<String, String>) -> Self {
        Self {
            info: Mutex::new((None, None)),
            tasks: desc("tidb_disttask_task_status", "Number of tasks.", &["task_type", "status"], &const_labels),
            subtasks: desc(
                "tidb_disttask_subtasks",
                "Number of subtasks.",
                &["task_type", "task_id", "status", "exec_id"],
                &const_labels,
            ),
            subtask_duration: desc(
                "tidb_disttask_subtask_duration",
                "Duration of subtasks in different states.",
                &["task_type", "task_id", "status", "subtask_id", "exec_id"],
                &const_labels,
            ),
        }
    }

    /// Go `UpdateInfo`.
    pub fn update_info(&self, tasks: Vec<TaskBase>, subtasks: Vec<SubtaskBase>) {
        *self.info.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = (Some(tasks), Some(subtasks));
    }

    /// Go `collectTasks`: the number of tasks of each type in each state.
    fn collect_tasks(&self, tasks: &[TaskBase]) -> Vec<Metric> {
        let mut counts: HashMap<(String, String), usize> = HashMap::new();
        for task in tasks {
            *counts.entry((task.tp.to_string(), task.state.to_string())).or_default() += 1;
        }
        counts
            .into_iter()
            .map(|((tp, state), count)| gauge(&self.tasks, count as f64, &[tp, state]))
            .collect()
    }

    /// Go `collectSubtasks`: the subtasks of each task on each node in each
    /// state, and each pending or running subtask's duration.
    fn collect_subtasks(&self, subtasks: &[SubtaskBase]) -> (Vec<Metric>, Vec<Metric>) {
        let mut counts: HashMap<(i64, String, String), usize> = HashMap::new();
        let mut task_type: HashMap<i64, String> = HashMap::new();
        let mut durations = Vec::new();
        let now = chrono::Utc::now();
        for subtask in subtasks {
            *counts
                .entry((subtask.task_id, subtask.exec_id.clone(), subtask.state.to_string()))
                .or_default() += 1;
            task_type.insert(subtask.task_id, subtask.tp.to_string());
            // Go `setDistSubtaskDuration`.
            let since = if subtask.state == SUBTASK_STATE_PENDING {
                Some(subtask.create_time)
            } else if subtask.state == SUBTASK_STATE_RUNNING {
                Some(subtask.start_time)
            } else {
                None
            };
            if let Some(since) = since {
                let seconds = (now - since.with_timezone(&chrono::Utc)).num_milliseconds() as f64 / 1000.0;
                durations.push(gauge(
                    &self.subtask_duration,
                    seconds,
                    &[
                        subtask.tp.to_string(),
                        subtask.task_id.to_string(),
                        subtask.state.to_string(),
                        subtask.id.to_string(),
                        subtask.exec_id.clone(),
                    ],
                ));
            }
        }
        let metrics = counts
            .into_iter()
            .map(|((task_id, exec_id, state), count)| {
                let tp = task_type.get(&task_id).cloned().unwrap_or_default();
                gauge(&self.subtasks, count as f64, &[tp, task_id.to_string(), state, exec_id])
            })
            .collect();
        (metrics, durations)
    }
}

impl PromCollector for Collector {
    /// Go `Describe`.
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.tasks, &self.subtasks, &self.subtask_duration]
    }

    /// Go `Collect`.
    fn collect(&self) -> Vec<MetricFamily> {
        let info = self.info.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut families = Vec::new();
        if let Some(tasks) = &info.0 {
            families.push(family(&self.tasks, self.collect_tasks(tasks)));
        }
        if let Some(subtasks) = &info.1 {
            let (counts, durations) = self.collect_subtasks(subtasks);
            families.push(family(&self.subtasks, counts));
            families.push(family(&self.subtask_duration, durations));
        }
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_dxf::{TaskState, TaskType};

    /// The families Go's collector emits for its tasks and subtasks (Go has
    /// no test of its own for this package).
    #[test]
    fn collector_counts_tasks_and_subtasks_like_go() {
        let collector = Collector::new(HashMap::new());
        assert!(collector.collect().is_empty());
        let task = |state: &str| TaskBase {
            tp: TaskType::new("backfill"),
            state: TaskState::new(state),
            ..TaskBase::default()
        };
        let subtask = |id: i64, exec_id: &str, state: tidb_dxf::SubtaskState| SubtaskBase {
            id,
            task_id: 7,
            tp: TaskType::new("backfill"),
            exec_id: exec_id.to_owned(),
            state,
            ..SubtaskBase::default()
        };
        collector.update_info(
            vec![task("running"), task("running"), task("pending")],
            vec![
                subtask(1, "n1", SUBTASK_STATE_RUNNING),
                subtask(2, "n1", SUBTASK_STATE_RUNNING),
                subtask(3, "n2", SUBTASK_STATE_PENDING),
                subtask(4, "n2", tidb_dxf::SubtaskState::new("succeed")),
            ],
        );
        let families = collector.collect();
        let names: Vec<_> = families.iter().map(MetricFamily::name).collect();
        assert_eq!(names, ["tidb_disttask_task_status", "tidb_disttask_subtasks", "tidb_disttask_subtask_duration"]);
        let value = |family: &MetricFamily, labels: &[(&str, &str)]| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    labels.iter().all(|(name, value)| {
                        metric.get_label().iter().any(|pair| pair.name() == *name && pair.value() == *value)
                    })
                })
                .map(|metric| metric.get_gauge().get_value())
        };
        assert_eq!(value(&families[0], &[("status", "running")]), Some(2.0));
        assert_eq!(value(&families[0], &[("status", "pending")]), Some(1.0));
        assert_eq!(value(&families[1], &[("exec_id", "n1"), ("status", "running"), ("task_id", "7")]), Some(2.0));
        assert_eq!(families[2].get_metric().len(), 3, "only pending and running subtasks have a duration");
    }
}
