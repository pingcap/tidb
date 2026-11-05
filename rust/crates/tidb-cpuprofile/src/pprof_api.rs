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

//! Go `pkg/util/cpuprofile/pprof_api.go`: a CPU profile of any length
//! collected from the global profiler's one-second profiles.

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message as _;
use tidb_proto::perftools::profiles as protos;

use super::{register, unregister, ProfileConsumer, ProfileData, StringTable};

const LABEL_SQL: &str = "sql";

/// Go `ProfileHTTPHandler` is same as `pprof.Profile`: a `seconds` (default
/// 30) CPU profile, gzipped. Errors carry Go's status and text.
///
/// # Errors
/// Go's `serveError`: 500 when profiling could not run.
pub fn profile_http_handler(seconds: &str) -> Result<Vec<u8>, (u16, String)> {
    let sec = seconds.parse::<i64>().ok().filter(|sec| *sec > 0).unwrap_or(30);
    let mut pc = Collector::new();
    pc.start_cpu_profile()
        .map_err(|error| (500, format!("Could not enable CPU profiling: {error}")))?;
    std::thread::sleep(Duration::from_secs(sec as u64));
    pc.stop_cpu_profile()
        .map_err(|error| (500, format!("Could not enable CPU profiling: {error}")))
}

/// What Go's `readProfileData` goroutine leaves for `StopCPUProfile`.
#[derive(Default)]
struct Collected {
    result: Option<protos::Profile>,
    err: Option<String>,
    first_read: bool,
}

/// Go `Collector` is a cpu profile collector, it collect cpu profile data
/// from globalCPUProfiler.
pub struct Collector {
    data_ch: ProfileConsumer,
    receiver: Option<Receiver<Arc<ProfileData>>>,
    collected: Arc<(Mutex<Collected>, Condvar)>,
    cancel: Arc<AtomicBool>,
    reader: Option<JoinHandle<()>>,
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector {
    /// Go `NewCollector`.
    #[must_use]
    pub fn new() -> Self {
        let (data_ch, receiver) = ProfileConsumer::new(1);
        Self {
            data_ch,
            receiver: Some(receiver),
            collected: Arc::default(),
            cancel: Arc::default(),
            reader: None,
        }
    }

    /// Go `StartCPUProfile` is a substitute for `pprof.StartCPUProfile`.
    ///
    /// # Errors
    /// Go's "Collector already started".
    pub fn start_cpu_profile(&mut self) -> Result<(), String> {
        let Some(receiver) = self.receiver.take() else {
            return Err("Collector already started".to_owned());
        };
        let data_ch = self.data_ch.clone();
        let collected = Arc::clone(&self.collected);
        let cancel = Arc::clone(&self.cancel);
        self.reader = Some(
            std::thread::Builder::new()
                .name("cpu-profile-collector".to_owned())
                .spawn(move || read_profile_data(&data_ch, &receiver, &collected, &cancel))
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    /// Go `StopCPUProfile` is a substitute for `pprof.StopCPUProfile`: the
    /// merged profile, gzipped (Go `profile.Write`); empty when none was
    /// collected.
    ///
    /// # Errors
    /// The profile error Go's `readProfileData` stopped on.
    pub fn stop_cpu_profile(&mut self) -> Result<Vec<u8>, String> {
        let Some(reader) = self.reader.take() else {
            return Ok(Vec::new());
        };
        // wait for reading least 1 profile data.
        {
            let (lock, first_read) = &*self.collected;
            let guard = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let _ = first_read
                .wait_timeout_while(guard, super::profile_duration() * 2, |collected| !collected.first_read)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        self.cancel.store(true, Ordering::SeqCst);
        let _ = reader.join();
        let mut collected = self.collected.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = collected.err.take() {
            return Err(error);
        }
        let Some(mut profile) = collected.result.take() else {
            return Ok(Vec::new());
        };
        remove_label(&mut profile);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&profile.encode_to_vec()).map_err(|error| error.to_string())?;
        gz.finish().map_err(|error| error.to_string())
    }
}

/// Go `Collector.readProfileData`: merges each profile the consumer receives
/// until cancelled or a profile fails.
fn read_profile_data(
    data_ch: &ProfileConsumer,
    receiver: &Receiver<Arc<ProfileData>>,
    collected: &(Mutex<Collected>, Condvar),
    cancel: &AtomicBool,
) {
    // register cpu profile consumer.
    register(data_ch);
    while !cancel.load(Ordering::SeqCst) {
        let data = match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(data) => data,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let (lock, first_read) = collected;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = handle_profile_data(&mut state, &data) {
            state.err = Some(error);
            break;
        }
        if !state.first_read {
            state.first_read = true;
            first_read.notify_all();
        }
    }
    unregister(data_ch);
}

/// Go `Collector.handleProfileData`.
fn handle_profile_data(state: &mut Collected, data: &ProfileData) -> Result<(), String> {
    if let Some(error) = &data.error {
        return Err(error.clone());
    }
    let pd = protos::Profile::decode(data.data.as_slice()).map_err(|error| error.to_string())?;
    state.result = Some(match state.result.take() {
        None => pd,
        Some(result) => merge(&[result, pd]),
    });
    Ok(())
}

/// Go `removeLabel` uses to remove the sql_digest and plan_digest labels for
/// pprof cpu profile data, since TopSQL will set them and they are strange
/// for other users.
fn remove_label(profile: &mut protos::Profile) {
    let sql = profile.string_table.iter().position(|text| text == LABEL_SQL).map(|index| index as i64);
    for sample in &mut profile.sample {
        sample.label.retain(|label| Some(label.key) == sql);
    }
}

/// One sample's identity in Go `profile.Merge`: its frames and labels.
type SampleKey = (Vec<(String, String, String, i64)>, BTreeMap<String, Vec<String>>);

/// Go `profile.Merge`: samples with the same frames and labels sum their
/// values; the earliest start time and the summed duration are kept.
pub(crate) fn merge(profiles: &[protos::Profile]) -> protos::Profile {
    let mut merged: Vec<(SampleKey, Vec<i64>)> = Vec::new();
    let mut index: HashMap<SampleKey, usize> = HashMap::new();
    for profile in profiles {
        let text = |id: i64| profile.string_table.get(id as usize).cloned().unwrap_or_default();
        let functions: HashMap<u64, &protos::Function> =
            profile.function.iter().map(|function| (function.id, function)).collect();
        let locations: HashMap<u64, &protos::Location> =
            profile.location.iter().map(|location| (location.id, location)).collect();
        for sample in &profile.sample {
            let mut frames = Vec::new();
            for location_id in &sample.location_id {
                for line in locations.get(location_id).map(|location| location.line.as_slice()).unwrap_or_default() {
                    let function = functions.get(&line.function_id);
                    frames.push((
                        function.map(|function| text(function.name)).unwrap_or_default(),
                        function.map(|function| text(function.system_name)).unwrap_or_default(),
                        function.map(|function| text(function.filename)).unwrap_or_default(),
                        line.line,
                    ));
                }
            }
            let mut labels: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for label in &sample.label {
                labels.entry(text(label.key)).or_default().push(text(label.str));
            }
            let key = (frames, labels);
            match index.get(&key) {
                Some(position) => {
                    for (total, value) in merged[*position].1.iter_mut().zip(&sample.value) {
                        *total += value;
                    }
                }
                None => {
                    index.insert(key.clone(), merged.len());
                    merged.push((key, sample.value.clone()));
                }
            }
        }
    }

    let mut strings = StringTable::new();
    let mut functions: Vec<protos::Function> = Vec::new();
    let mut locations: Vec<protos::Location> = Vec::new();
    let mut frame_ids: HashMap<(String, String, String, i64), u64> = HashMap::new();
    let mut samples = Vec::with_capacity(merged.len());
    for ((frames, labels), value) in merged {
        let mut location_id = Vec::with_capacity(frames.len());
        for frame in frames {
            let id = match frame_ids.get(&frame) {
                Some(id) => *id,
                None => {
                    let id = functions.len() as u64 + 1;
                    functions.push(protos::Function {
                        id,
                        name: strings.id(&frame.0),
                        system_name: strings.id(&frame.1),
                        filename: strings.id(&frame.2),
                        ..protos::Function::default()
                    });
                    locations.push(protos::Location {
                        id,
                        line: vec![protos::Line {
                            function_id: id,
                            line: frame.3,
                            ..protos::Line::default()
                        }],
                        ..protos::Location::default()
                    });
                    frame_ids.insert(frame, id);
                    id
                }
            };
            location_id.push(id);
        }
        let label = labels
            .iter()
            .flat_map(|(key, values)| values.iter().map(move |value| (key, value)))
            .map(|(key, value)| protos::Label {
                key: strings.id(key),
                str: strings.id(value),
                ..protos::Label::default()
            })
            .collect();
        samples.push(protos::Sample { location_id, value, label });
    }
    let first = profiles.first().cloned().unwrap_or_default();
    let value_type = |value_type: &protos::ValueType, strings: &mut StringTable| protos::ValueType {
        r#type: strings.id(first.string_table.get(value_type.r#type as usize).map_or("", String::as_str)),
        unit: strings.id(first.string_table.get(value_type.unit as usize).map_or("", String::as_str)),
    };
    let sample_type = first.sample_type.iter().map(|ty| value_type(ty, &mut strings)).collect();
    let period_type = first.period_type.as_ref().map(|ty| value_type(ty, &mut strings));
    protos::Profile {
        sample_type,
        sample: samples,
        function: functions,
        location: locations,
        time_nanos: profiles.iter().map(|profile| profile.time_nanos).min().unwrap_or_default(),
        duration_nanos: profiles.iter().map(|profile| profile.duration_nanos).sum(),
        period_type,
        period: first.period,
        string_table: strings.strings,
        ..protos::Profile::default()
    }
}
