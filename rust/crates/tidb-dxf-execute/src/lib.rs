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


//! Go `pkg/dxf/framework/taskexecutor/execute`: what a task type implements
//! to run the subtasks of a step (`StepExecutor`), and the summary of a
//! running subtask the framework stores in the subtask table.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tidb_dxf::{Step, StepResource, Subtask, Task};
use tidb_dxf_metering::Recorder;
use tidb_objstore_recording::Requests;
use tidb_sqlexec::ExecutionContext;

/// A step executor's error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A step executor's result.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `StepExecutor`: the executor of a task step's subtasks. The
/// framework calls `init`; then for every subtask `run_subtask` (stopping at
/// the first failure); then `cleanup`.
pub trait StepExecutor: StepExecFrameworkInfo + Send + Sync {
    /// Go `Init`: prepares the environment. A retryable error (the task
    /// executor's `IsRetryableError`) is retried; any other fails a subtask,
    /// failing the task.
    ///
    /// # Errors
    /// The environment could not be prepared.
    fn init(&mut self, ctx: &dyn ExecutionContext) -> Result<()>;

    /// Go `RunSubtask`: runs one subtask, whose meta it may update in place
    /// (stored on success).
    ///
    /// # Errors
    /// The subtask failed.
    fn run_subtask(&mut self, ctx: &dyn ExecutionContext, subtask: &mut Subtask) -> Result<()>;

    /// Go `RealtimeSummary`: the running subtask's summary.
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>>;

    /// Go `ResetSummary`.
    fn reset_summary(&self);

    /// Go `Cleanup`: its error is only logged.
    ///
    /// # Errors
    /// Cleaning up failed.
    fn cleanup(&mut self, ctx: &dyn ExecutionContext) -> Result<()>;

    /// Go `TaskMetaModified`: the task's meta changed; on an error the
    /// framework may recreate the executor.
    ///
    /// # Errors
    /// The new meta could not be applied.
    fn task_meta_modified(&mut self, ctx: &dyn ExecutionContext, new_meta: &[u8]) -> Result<()>;

    /// Go `ResourceModified`: the allowed resource changed while a subtask
    /// runs; the executor conforms before returning (a reduction must not
    /// leave it over the new budget).
    ///
    /// # Errors
    /// The new resource could not be applied.
    fn resource_modified(&mut self, ctx: &dyn ExecutionContext, new_resource: &StepResource) -> Result<()>;
}

/// Go `UpdateSubtaskSummaryInterval`: how often the subtask summary is
/// stored in the subtask table.
pub const UPDATE_SUBTASK_SUMMARY_INTERVAL: Duration = Duration::from_secs(3);

/// Go `maxProgressInSummary`: the progress points a summary keeps.
const MAX_PROGRESS_IN_SUMMARY: usize = 5;

/// Go `SubtaskSpeedUpdateInterval`: how often subtask speeds are updated.
pub const SUBTASK_SPEED_UPDATE_INTERVAL: Duration =
    Duration::from_secs(3 * MAX_PROGRESS_IN_SUMMARY as u64);

/// Go `time.Time`'s JSON form (`RFC3339Nano`, trailing zeros of the
/// fraction trimmed).
mod go_time_json {
    use chrono::{DateTime, FixedOffset, SecondsFormat};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn format(time: &DateTime<FixedOffset>) -> String {
        let text = time.to_rfc3339_opts(SecondsFormat::Nanos, true);
        // `.000000000` -> nothing, `.500000000` -> `.5`.
        match (text.find('.'), text.rfind(['Z', '+', '-'])) {
            (Some(dot), Some(zone)) if zone > dot => {
                let fraction = text[dot + 1..zone].trim_end_matches('0');
                let separator = if fraction.is_empty() { "" } else { "." };
                format!("{}{separator}{fraction}{}", &text[..dot], &text[zone..])
            }
            _ => text,
        }
    }

    pub(crate) fn serialize<S: Serializer>(time: &DateTime<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format(time))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error> {
        let text = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&text).map_err(serde::de::Error::custom)
    }
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

/// Go `Progress`: a subtask's progress at one time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Go `RowCnt` (collected, not yet used).
    #[serde(rename = "row_count", default, skip_serializing_if = "is_zero")]
    pub row_cnt: i64,
    /// Go `Processed`: generic progress units (bytes or rows), still
    /// stored under its old `bytes` name.
    #[serde(rename = "bytes", default, skip_serializing_if = "is_zero")]
    pub processed: i64,
    /// Go `UpdateTime`: when this progress was stored.
    #[serde(rename = "update_time", with = "go_time_json")]
    pub update_time: DateTime<FixedOffset>,
}

/// Go `SubtaskSummary`: a running subtask's summary.
#[derive(Debug, Default)]
pub struct SubtaskSummary {
    /// Go `RowCnt`, updated by the collector.
    pub row_cnt: AtomicI64,
    /// Go `Processed`: units the collector reported (bytes or rows).
    pub processed: AtomicI64,
    /// Go `ReadBytes`: bytes read from the source.
    pub read_bytes: AtomicI64,
    /// Go `GetReqCnt`: GET requests to external storage.
    pub get_req_cnt: AtomicU64,
    /// Go `PutReqCnt`: PUT requests to external storage.
    pub put_req_cnt: AtomicU64,
    /// Go `Progresses`: the processed history, for a smoother speed;
    /// appended each time the summary is stored.
    pub progresses: Mutex<Vec<Progress>>,
}

/// Go's JSON form of `SubtaskSummary`: its atomics (structs, which
/// `omitempty` never drops) always, its progress history when not empty.
#[derive(Serialize, Deserialize)]
struct SubtaskSummaryJson {
    #[serde(rename = "row_count", default)]
    row_cnt: i64,
    #[serde(rename = "bytes", default)]
    processed: i64,
    #[serde(default)]
    read_bytes: i64,
    #[serde(rename = "get_request_count", default)]
    get_req_cnt: u64,
    #[serde(rename = "put_request_count", default)]
    put_req_cnt: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    progresses: Vec<Progress>,
}

impl Serialize for SubtaskSummary {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        SubtaskSummaryJson {
            row_cnt: self.row_cnt.load(Ordering::SeqCst),
            processed: self.processed.load(Ordering::SeqCst),
            read_bytes: self.read_bytes.load(Ordering::SeqCst),
            get_req_cnt: self.get_req_cnt.load(Ordering::SeqCst),
            put_req_cnt: self.put_req_cnt.load(Ordering::SeqCst),
            progresses: self.progresses().clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SubtaskSummary {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let json = SubtaskSummaryJson::deserialize(deserializer)?;
        Ok(Self {
            row_cnt: AtomicI64::new(json.row_cnt),
            processed: AtomicI64::new(json.processed),
            read_bytes: AtomicI64::new(json.read_bytes),
            get_req_cnt: AtomicU64::new(json.get_req_cnt),
            put_req_cnt: AtomicU64::new(json.put_req_cnt),
            progresses: Mutex::new(json.progresses),
        })
    }
}

impl SubtaskSummary {
    fn progresses(&self) -> std::sync::MutexGuard<'_, Vec<Progress>> {
        self.progresses.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `MergeObjStoreRequests`.
    pub fn merge_obj_store_requests(&self, reqs: &Requests) {
        self.get_req_cnt.fetch_add(reqs.get.load(Ordering::SeqCst), Ordering::SeqCst);
        self.put_req_cnt.fetch_add(reqs.put.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Go `Update`: stores the latest progress, keeping the last five.
    pub fn update(&self) {
        let mut progresses = self.progresses();
        progresses.push(Progress {
            row_cnt: self.row_cnt.load(Ordering::SeqCst),
            processed: self.processed.load(Ordering::SeqCst),
            update_time: chrono::Local::now().fixed_offset(),
        });
        if progresses.len() > MAX_PROGRESS_IN_SUMMARY {
            let excess = progresses.len() - MAX_PROGRESS_IN_SUMMARY;
            progresses.drain(..excess);
        }
    }

    /// Go `GetSpeedInTimeRange`: the processed units per second within the
    /// `duration` ending at `end_time`, interpolating the stored points.
    #[must_use]
    pub fn get_speed_in_time_range(&self, end_time: DateTime<FixedOffset>, duration: Duration) -> i64 {
        let progresses = self.progresses();
        if progresses.len() < 2 {
            return 0;
        }
        let duration = chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX);
        let start_time = end_time - duration;
        if end_time < progresses[0].update_time || start_time > progresses[progresses.len() - 1].update_time {
            return 0;
        }
        let nanos = |delta: chrono::Duration| delta.num_nanoseconds().unwrap_or(i64::MAX) as f64;
        let mut total_processed = 0.0;
        for pair in progresses.windows(2) {
            let (range_start, range_end) = (pair[0].update_time, pair[1].update_time);
            let range_processed = (pair[1].processed - pair[0].processed) as f64;
            if end_time < range_start || start_time > range_end {
                continue;
            } else if start_time < range_start && end_time > range_end {
                total_processed += range_processed;
                continue;
            }
            let i_start = if start_time > range_start { start_time } else { range_start };
            let i_end = if end_time < range_end { end_time } else { range_end };
            total_processed += range_processed * nanos(i_end - i_start) / nanos(range_end - range_start);
        }
        (total_processed / (nanos(duration) / 1e9)) as i64
    }

    /// Go `UpdateTime`: the last progress's time, Go's zero time when none.
    #[must_use]
    pub fn update_time(&self) -> DateTime<FixedOffset> {
        self.progresses().last().map_or_else(DateTime::<FixedOffset>::default, |progress| progress.update_time)
    }

    /// Go `Reset`: zeroes the summary, clears its history, and stores one
    /// point.
    pub fn reset(&self) {
        self.row_cnt.store(0, Ordering::SeqCst);
        self.processed.store(0, Ordering::SeqCst);
        self.read_bytes.store(0, Ordering::SeqCst);
        self.put_req_cnt.store(0, Ordering::SeqCst);
        self.get_req_cnt.store(0, Ordering::SeqCst);
        self.progresses().clear();
        self.update();
    }
}

/// Go `Collector`: collects a subtask's metrics.
pub trait Collector: Send + Sync {
    /// Go `Accepted`: `bytes` were accepted for processing.
    fn accepted(&self, bytes: i64);
    /// Go `Processed`: `processed_units` (bytes read, bytes merged, conflict
    /// pairs handled: it depends on the step) and `rows` were processed.
    fn processed(&self, processed_units: i64, rows: i64);
}

/// Go `NoopCollector`.
#[derive(Debug, Default)]
pub struct NoopCollector;

impl Collector for NoopCollector {
    fn accepted(&self, _bytes: i64) {}
    fn processed(&self, _processed_units: i64, _rows: i64) {}
}

/// Go `TestCollector`.
#[derive(Debug, Default)]
pub struct TestCollector {
    /// Go `ReadBytes`.
    pub read_bytes: AtomicI64,
    /// Go `ProcessedCnt`.
    pub processed_cnt: AtomicI64,
    /// Go `Rows`.
    pub rows: AtomicI64,
}

impl Collector for TestCollector {
    fn accepted(&self, bytes: i64) {
        self.read_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    fn processed(&self, processed_units: i64, rows: i64) {
        self.processed_cnt.fetch_add(processed_units, Ordering::SeqCst);
        self.rows.fetch_add(rows, Ordering::SeqCst);
    }
}

/// Go's `func(context.Context, int64, any) error` checkpoint updater: the
/// subtask id and its checkpoint, as JSON.
pub type UpdateCheckpointFunc = Arc<dyn Fn(&dyn ExecutionContext, i64, &serde_json::Value) -> Result<()> + Send + Sync>;

/// Go's `func(context.Context, int64) (string, error)` checkpoint reader.
pub type GetCheckpointFunc = Arc<dyn Fn(&dyn ExecutionContext, i64) -> Result<String> + Send + Sync>;

/// Go `frameworkInfo`: what the framework gives a step executor before its
/// `Init`.
pub struct FrameworkInfo {
    step: Step,
    meter_recorder: Arc<Recorder>,
    resource: RwLock<Arc<StepResource>>,
    update_checkpoint_func: Option<UpdateCheckpointFunc>,
    get_checkpoint_func: Option<GetCheckpointFunc>,
}

/// Go `StepExecFrameworkInfo`: the framework's information a step executor
/// reads. A step executor embeds a [`FrameworkInfoSlot`] (Go embeds the
/// interface and the framework sets it by reflection) and returns it from
/// [`Self::framework_info_slot`]; the other methods read it.
pub trait StepExecFrameworkInfo {
    /// The embedded slot (Go's `StepExecFrameworkInfo` field).
    fn framework_info_slot(&self) -> &FrameworkInfoSlot;

    /// Go `GetStep`.
    fn get_step(&self) -> Step {
        self.framework_info_slot().get().step
    }

    /// Go `GetResource`.
    fn get_resource(&self) -> Arc<StepResource> {
        Arc::clone(
            &self
                .framework_info_slot()
                .get()
                .resource
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Go `SetResource`.
    fn set_resource(&self, resource: Arc<StepResource>) {
        *self
            .framework_info_slot()
            .get()
            .resource
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = resource;
    }

    /// Go `GetMeterRecorder`.
    fn get_meter_recorder(&self) -> Arc<Recorder> {
        Arc::clone(&self.framework_info_slot().get().meter_recorder)
    }

    /// Go `GetCheckpointUpdateFunc`.
    fn get_checkpoint_update_func(&self) -> Option<UpdateCheckpointFunc> {
        self.framework_info_slot().get().update_checkpoint_func.clone()
    }

    /// Go `GetCheckpointFunc`.
    fn get_checkpoint_func(&self) -> Option<GetCheckpointFunc> {
        self.framework_info_slot().get().get_checkpoint_func.clone()
    }
}

/// The field a step executor embeds for the framework's information, set
/// once by [`set_framework_info`].
#[derive(Default)]
pub struct FrameworkInfoSlot(std::sync::OnceLock<FrameworkInfo>);

impl FrameworkInfoSlot {
    /// The framework's information; Go's methods dereference a nil field
    /// before `SetFrameworkInfo`, which this panic stands for.
    fn get(&self) -> &FrameworkInfo {
        self.0.get().expect("the framework sets its info before the step executor's Init")
    }
}

/// Go `SetFrameworkInfo`: gives `exec` its step, resource, meter recorder
/// and checkpoint functions.
pub fn set_framework_info(
    exec: &dyn StepExecutor,
    task: &Task,
    resource: Arc<StepResource>,
    update_checkpoint_func: Option<UpdateCheckpointFunc>,
    get_checkpoint_func: Option<GetCheckpointFunc>,
) {
    let info = FrameworkInfo {
        step: task.base.step,
        meter_recorder: tidb_dxf_metering::register_recorder(&task.base),
        resource: RwLock::new(resource),
        update_checkpoint_func,
        get_checkpoint_func,
    };
    // Go sets the field once per executor.
    let _ = exec.framework_info_slot().0.set(info);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn unix(seconds: i64, nanos: u32) -> DateTime<FixedOffset> {
        chrono::Utc.timestamp_opt(seconds, nanos).unwrap().fixed_offset()
    }

    /// Go `TestSubtaskSummaryGetSpeed`.
    #[test]
    fn subtask_summary_get_speed() {
        let points = |base: i64, processed: &[i64]| {
            processed
                .iter()
                .enumerate()
                .map(|(i, processed)| Progress {
                    processed: *processed,
                    update_time: unix(base + i as i64, 0),
                    ..Progress::default()
                })
                .collect::<Vec<_>>()
        };
        let cases: [(&str, Vec<Progress>, DateTime<FixedOffset>, Duration, i64); 7] = [
            ("insufficient data points", points(1000, &[100]), unix(1010, 0), Duration::from_secs(10), 0),
            ("no overlap with data range", points(1000, &[0, 100]), unix(1010, 0), Duration::from_secs(1), 0),
            ("partial time range overlap", points(1000, &[0, 50, 100, 150]), unix(1002, 500_000_000), Duration::from_secs(1), 50),
            ("partial time range overlap", points(1000, &[0, 30, 60, 90]), unix(1004, 0), Duration::from_millis(1500), 10),
            ("multiple overlapping", points(1000, &[0, 60, 120, 180, 240]), unix(1004, 500_000_000), Duration::from_secs(2), 45),
            ("exact match the range", points(1000, &[0, 60, 120, 180, 240]), unix(1004, 0), Duration::from_secs(4), 60),
            ("whole range", points(1001, &[0, 60, 120, 180, 240]), unix(1006, 500_000_000), Duration::from_secs(6), 40),
        ];
        for (name, progresses, end_time, duration, expected) in cases {
            let summary = SubtaskSummary::default();
            *summary.progresses() = progresses;
            assert_eq!(expected, summary.get_speed_in_time_range(end_time, duration), "{name}");
        }
    }

    /// Go's JSON of a summary: its counters always, its progresses when set,
    /// a progress's zero counts omitted and its time in `RFC3339Nano`.
    #[test]
    fn subtask_summary_json_is_gos() {
        let summary = SubtaskSummary::default();
        summary.row_cnt.store(3, Ordering::SeqCst);
        assert_eq!(
            serde_json::to_string(&summary).unwrap(),
            r#"{"row_count":3,"bytes":0,"read_bytes":0,"get_request_count":0,"put_request_count":0}"#
        );
        summary.progresses().push(Progress { processed: 5, update_time: unix(1000, 500_000_000), ..Progress::default() });
        assert_eq!(
            serde_json::to_string(&summary).unwrap(),
            r#"{"row_count":3,"bytes":0,"read_bytes":0,"get_request_count":0,"put_request_count":0,"progresses":[{"bytes":5,"update_time":"1970-01-01T00:16:40.5Z"}]}"#
        );
        let back: SubtaskSummary = serde_json::from_str(&serde_json::to_string(&summary).unwrap()).unwrap();
        assert_eq!(back.row_cnt.load(Ordering::SeqCst), 3);
        assert_eq!(back.progresses().len(), 1);
    }
}
