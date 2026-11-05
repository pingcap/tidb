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

//! Go `pkg/util/cpuprofile` (`cpuprofile.go`, `pprof_api.go`): one process
//! CPU profiler that hands each consumer a profile per second, over pprof-rs
//! in place of Go's `runtime/pprof`.
//!
//! [`ProfileData::data`] is Go's encoded `profile.proto`, so its consumers
//! (TopSQL's SQL CPU collector, the status port's profile routes) parse it as
//! Go's do. Each sample carries the [`labels`] its thread held when sampled.

pub mod labels;
mod pprof_api;

pub use pprof_api::{profile_http_handler, Collector};

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use prost::Message as _;
use tidb_proto::perftools::profiles as protos;

/// Go `DefProfileDuration` exports for testing.
pub static DEF_PROFILE_DURATION: Mutex<Duration> = Mutex::new(Duration::from_secs(1));

/// Go `runtime.SetCPUProfileRate`'s default: 100 samples per second.
const PROFILE_HZ: i32 = 100;

/// Go `ProfileData` contains the cpu profile data between the start and end
/// time, usually the interval between start and end is about 1 second.
#[derive(Debug, Default)]
pub struct ProfileData {
    /// The encoded `profile.proto`.
    pub data: Vec<u8>,
    /// Go `Error`.
    pub error: Option<String>,
}

/// Go `ProfileConsumer` is a channel that will receive profiling data from
/// the CPU profiler periodically. If the channel is full, then the channel
/// won't receive the latest profile data until it is not blocked.
#[derive(Clone)]
pub struct ProfileConsumer {
    id: u64,
    sender: SyncSender<Arc<ProfileData>>,
}

impl ProfileConsumer {
    /// Go `make(ProfileConsumer, capacity)`: the consumer and its receiving
    /// end.
    #[must_use]
    pub fn new(capacity: usize) -> (Self, Receiver<Arc<ProfileData>>) {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
        (
            Self {
                id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                sender,
            },
            receiver,
        )
    }
}

/// Go `StartCPUProfiler` uses to start to run the global
/// parallelCPUProfiler.
///
/// # Errors
/// Go `errProfilerAlreadyStarted`.
pub fn start_cpu_profiler() -> Result<(), String> {
    GLOBAL_CPU_PROFILER.start()
}

/// Go `StopCPUProfiler` uses to stop the global parallelCPUProfiler.
pub fn stop_cpu_profiler() {
    GLOBAL_CPU_PROFILER.stop();
}

/// Go `Register` register a ProfileConsumer into the global CPU profiler.
/// Normally, the registered ProfileConsumer will receive the cpu profile data
/// per second.
pub fn register(ch: &ProfileConsumer) {
    GLOBAL_CPU_PROFILER.register(ch);
}

/// Go `Unregister` unregister a ProfileConsumer from the global CPU
/// profiler.
pub fn unregister(ch: &ProfileConsumer) {
    GLOBAL_CPU_PROFILER.unregister(ch);
}

/// Go `globalCPUProfiler`.
static GLOBAL_CPU_PROFILER: LazyLock<ParallelCpuProfiler> = LazyLock::new(ParallelCpuProfiler::new);

#[derive(Default)]
struct ProfilerState {
    cs: HashMap<u64, SyncSender<Arc<ProfileData>>>,
    started: bool,
    notify_register: bool,
}

/// Go `parallelCPUProfiler` is a cpu profiler. With parallelCPUProfiler, it
/// is possible to have multiple profile consumer at the same time.
struct ParallelCpuProfiler {
    state: Mutex<ProfilerState>,
    wake: Condvar,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl ParallelCpuProfiler {
    fn new() -> Self {
        Self {
            state: Mutex::new(ProfilerState::default()),
            wake: Condvar::new(),
            worker: Mutex::new(None),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ProfilerState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn start(&'static self) -> Result<(), String> {
        {
            let mut state = self.lock();
            if state.started {
                return Err("parallelCPUProfiler is already started".to_owned());
            }
            state.started = true;
        }
        let worker = std::thread::Builder::new()
            .name("cpu-profiler".to_owned())
            .spawn(move || self.profiling_loop())
            .map_err(|error| error.to_string())?;
        *self.worker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        tidb_logutil::bg_logger().info("parallel cpu profiler started", &[]);
        Ok(())
    }

    fn stop(&self) {
        {
            let mut state = self.lock();
            if !state.started {
                return;
            }
            state.started = false;
        }
        self.wake.notify_all();
        if let Some(worker) = self.worker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            let _ = worker.join();
        }
        tidb_logutil::bg_logger().info("parallel cpu profiler stopped", &[]);
    }

    fn register(&self, ch: &ProfileConsumer) {
        let mut state = self.lock();
        state.cs.insert(ch.id, ch.sender.clone());
        state.notify_register = true;
        drop(state);
        self.wake.notify_all();
    }

    fn unregister(&self, ch: &ProfileConsumer) {
        self.lock().cs.remove(&ch.id);
    }

    fn consumers_count(&self) -> usize {
        self.lock().cs.len()
    }

    fn profiling_loop(&self) {
        let mut profiling: Option<Profiling> = None;
        let mut next_tick = std::time::Instant::now() + profile_duration();
        loop {
            {
                let mut state = self.lock();
                loop {
                    if !state.started {
                        // Go's deferred `pprof.StopCPUProfile`.
                        drop(profiling.take());
                        return;
                    }
                    if state.notify_register {
                        state.notify_register = false;
                        // If already in profiling, don't do anything.
                        if profiling.is_none() {
                            break;
                        }
                    }
                    let now = std::time::Instant::now();
                    if now >= next_tick {
                        next_tick = now + profile_duration();
                        break;
                    }
                    state = self
                        .wake
                        .wait_timeout(state, next_tick - now)
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0;
                }
            }
            self.do_profiling(&mut profiling);
        }
    }

    fn do_profiling(&self, profiling: &mut Option<Profiling>) {
        if let Some(running) = profiling.take() {
            self.send_to_consumers(Arc::new(running.finish()));
        }
        if self.consumers_count() == 0 {
            return;
        }
        tidb_metrics::server::CPU_PROFILE_TOTAL.inc();
        match Profiling::start() {
            Ok(running) => *profiling = Some(running),
            // notify error as soon as possible
            Err(error) => self.send_to_consumers(Arc::new(ProfileData {
                data: Vec::new(),
                error: Some(error),
            })),
        }
    }

    fn send_to_consumers(&self, data: Arc<ProfileData>) {
        let state = self.lock();
        for sender in state.cs.values() {
            match sender.try_send(Arc::clone(&data)) {
                Ok(()) | Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {}
            }
        }
    }
}

fn profile_duration() -> Duration {
    *DEF_PROFILE_DURATION.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `pprof.StartCPUProfile` .. `pprof.StopCPUProfile` into a buffer.
struct Profiling {
    guard: pprof::ProfilerGuard<'static>,
}

impl Profiling {
    fn start() -> Result<Self, String> {
        pprof::ProfilerGuardBuilder::default()
            .frequency(PROFILE_HZ)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map(|guard| Self { guard })
            .map_err(|error| error.to_string())
    }

    fn finish(self) -> ProfileData {
        match self.guard.report().build() {
            Ok(report) => ProfileData {
                data: encode_profile(&report).encode_to_vec(),
                error: None,
            },
            Err(error) => ProfileData {
                data: Vec::new(),
                error: Some(error.to_string()),
            },
        }
    }
}

/// The profile's deduplicated string table; index 0 is the empty string.
#[derive(Default)]
struct StringTable {
    strings: Vec<String>,
    ids: HashMap<String, i64>,
}

impl StringTable {
    fn new() -> Self {
        Self {
            strings: vec![String::new()],
            ids: HashMap::new(),
        }
    }

    fn id(&mut self, text: &str) -> i64 {
        if text.is_empty() {
            return 0;
        }
        if let Some(id) = self.ids.get(text) {
            return *id;
        }
        let id = self.strings.len() as i64;
        self.strings.push(text.to_owned());
        self.ids.insert(text.to_owned(), id);
        id
    }
}

/// Go `runtime/pprof`'s CPU profile: `samples/count` and `cpu/nanoseconds`
/// values per sample, period in CPU nanoseconds, and each sample's labels.
fn encode_profile(report: &pprof::Report) -> protos::Profile {
    let mut strings = StringTable::new();
    let period = 1_000_000_000 / i64::from(report.timing.frequency.max(1));
    let mut samples = Vec::with_capacity(report.data.len());
    let mut functions: Vec<protos::Function> = Vec::new();
    let mut locations: Vec<protos::Location> = Vec::new();
    let mut function_ids: HashMap<String, u64> = HashMap::new();
    for (frames, count) in &report.data {
        let mut location_ids = Vec::new();
        for frame in &frames.frames {
            for symbol in frame {
                let name = symbol.name();
                if let Some(id) = function_ids.get(&name) {
                    location_ids.push(*id);
                    continue;
                }
                let id = functions.len() as u64 + 1;
                functions.push(protos::Function {
                    id,
                    name: strings.id(&name),
                    system_name: strings.id(&symbol.sys_name()),
                    filename: strings.id(&symbol.filename()),
                    ..protos::Function::default()
                });
                locations.push(protos::Location {
                    id,
                    line: vec![protos::Line {
                        function_id: id,
                        line: i64::from(symbol.lineno()),
                        ..protos::Line::default()
                    }],
                    ..protos::Location::default()
                });
                function_ids.insert(name, id);
                location_ids.push(id);
            }
        }
        let label = labels::labels_at(frames.thread_id, frames.sample_timestamp)
            .map(|labels| {
                labels
                    .iter()
                    .map(|(key, value)| protos::Label {
                        key: strings.id(key),
                        str: strings.id(value),
                        ..protos::Label::default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let count = *count as i64;
        samples.push(protos::Sample {
            location_id: location_ids,
            value: vec![count, count * period],
            label,
        });
    }
    let sample_count = protos::ValueType {
        r#type: strings.id("samples"),
        unit: strings.id("count"),
    };
    let cpu = protos::ValueType {
        r#type: strings.id("cpu"),
        unit: strings.id("nanoseconds"),
    };
    protos::Profile {
        sample_type: vec![sample_count, cpu],
        sample: samples,
        function: functions,
        location: locations,
        time_nanos: report
            .timing
            .start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        duration_nanos: report.timing.duration.as_nanos() as i64,
        period_type: Some(cpu),
        period,
        string_table: strings.strings,
        ..protos::Profile::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn burn(duration: Duration) -> u64 {
        let start = std::time::Instant::now();
        let mut acc = 0_u64;
        while start.elapsed() < duration {
            for i in 0..10_000_u64 {
                acc = acc.wrapping_mul(31).wrapping_add(i);
            }
        }
        acc
    }

    fn label_values(profile: &protos::Profile, key: &str) -> Vec<String> {
        let text = |id: i64| profile.string_table[id as usize].clone();
        profile
            .sample
            .iter()
            .flat_map(|sample| sample.label.iter())
            .filter(|label| text(label.key) == key)
            .map(|label| text(label.str))
            .collect()
    }

    /// Go `TestParallelCPUProfiler` / `TestGetCPUProfile`: a consumer gets a
    /// profile per window whose samples carry the labels their thread held,
    /// and the HTTP collector's merged profile drops all but `sql`.
    #[test]
    fn profiles_carry_thread_labels_and_the_http_collector_strips_them() {
        let _ = start_cpu_profiler();
        let (consumer, receiver) = ProfileConsumer::new(4);
        register(&consumer);
        let worker = std::thread::spawn(|| {
            labels::set_goroutine_labels(Some(labels::with_labels(
                None,
                &[("sql_digest", "abcd".to_owned()), ("sql", "keep".to_owned())],
            )));
            burn(Duration::from_millis(2500))
        });
        let mut found = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !found && std::time::Instant::now() < deadline {
            let Ok(data) = receiver.recv_timeout(Duration::from_secs(2)) else {
                continue;
            };
            assert_eq!(data.error, None);
            let profile = protos::Profile::decode(data.data.as_slice()).unwrap();
            assert_eq!(profile.period, 10_000_000);
            found = label_values(&profile, "sql_digest").iter().any(|digest| digest == "abcd");
        }
        unregister(&consumer);
        assert!(found, "no labelled sample in any window");

        let mut collector = Collector::new();
        collector.start_cpu_profile().unwrap();
        let burner = std::thread::spawn(|| {
            labels::set_goroutine_labels(Some(labels::with_labels(
                None,
                &[("sql_digest", "ef01".to_owned()), ("sql", "keep".to_owned())],
            )));
            burn(Duration::from_millis(2200))
        });
        burner.join().unwrap();
        let gz = collector.stop_cpu_profile().unwrap();
        let mut raw = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(gz.as_slice()), &mut raw).unwrap();
        let merged = protos::Profile::decode(raw.as_slice()).unwrap();
        assert!(label_values(&merged, "sql_digest").is_empty());
        assert!(label_values(&merged, "sql").iter().any(|value| value == "keep"));
        worker.join().unwrap();
        stop_cpu_profiler();
    }
}
