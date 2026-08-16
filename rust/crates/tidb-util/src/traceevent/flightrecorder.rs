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

//! Go `flightrecorder.go`: the configurable flight recorder, its JSON
//! configuration, and the AND/OR dump-trigger compiler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crossbeam_channel::Sender;
use serde::{Deserialize, Serialize};
use tidb_log::{Field, Value};

use super::{log_event, Event, Sink, TraceCategory};
use crate::logutil;
use crate::tracing::TraceContext;

/// Go `Trace`: a statement's own event buffer and accumulated trigger bits.
#[derive(Debug, Default)]
pub struct Trace {
    state: RwLock<TraceState>,
}

#[derive(Debug, Default)]
struct TraceState {
    events: Vec<Event>,
    bits: u64,
    rand32: u32,
}

/// Go `maxEvents`.
const MAX_EVENTS: usize = 4096;

impl Trace {
    /// Go `NewTrace`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(TraceState {
                events: Vec::new(),
                bits: 0,
                rand32: crate::fastrand::uint32(),
            }),
        }
    }

    /// Go `Trace.rand32`, read under the trace's lock.
    #[must_use]
    pub fn rand32(&self) -> u32 {
        self.read().rand32
    }

    /// Go `Trace.bits`, read under the trace's lock.
    #[must_use]
    pub fn bits(&self) -> u64 {
        self.read().bits
    }

    /// Go `Trace.events`, cloned under the trace's lock.
    #[must_use]
    pub fn events(&self) -> Vec<Event> {
        self.read().events.clone()
    }

    /// Go `Trace.markBits`.
    pub fn mark_bits(&self, idx: usize) {
        self.write().bits |= 1 << idx;
    }

    /// Go `Trace.DiscardOrFlush`: hands the buffered events to the active
    /// recorder when the accumulated bits satisfy its truth table, then resets.
    pub fn discard_or_flush(&self, ctx: Option<&TraceContext>) {
        if let Some(sink) = get_flight_recorder() {
            // Read phase: clone the events while holding the read lock, so a
            // concurrent `record` cannot mutate the buffer under us.
            let to_flush = {
                let state = self.read();
                sink.should_keep(state.bits).then(|| state.events.clone())
            };
            if let Some(events) = to_flush {
                sink.collect(ctx, events);
            }
        }
        let new_rand = crate::fastrand::uint32();
        let mut state = self.write();
        state.bits = 0;
        if state.events.len() > MAX_EVENTS {
            // avoid using too much memory for each session.
            state.events = Vec::new();
        } else {
            state.events.clear();
        }
        state.rand32 = new_rand;
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, TraceState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, TraceState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Sink for Trace {
    fn record(&self, event: &Event) {
        self.write().events.push(event.clone());
    }
}

/// Go `UserCommandConfig`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserCommandConfig {
    /// Go `type`.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Go `sql_regexp`.
    #[serde(default)]
    pub sql_regexp: String,
    /// Go `sql_digest`.
    #[serde(default)]
    pub sql_digest: String,
    /// Go `plan_digest`.
    #[serde(default)]
    pub plan_digest: String,
    /// Go `stmt_label`.
    #[serde(default)]
    pub stmt_label: String,
    /// Go `by_user`.
    #[serde(default)]
    pub by_user: String,
    /// Go `table`.
    #[serde(default)]
    pub table: String,
}

impl UserCommandConfig {
    /// Go `UserCommandConfig.compile`.
    fn compile(
        &self,
        name: &mut String,
        mapping: &mut CompiledDumpTriggerConfig,
        conf: &DumpTriggerConfig,
    ) -> Result<u64, String> {
        name.push_str(".user_command");
        let (suffix, value) = match self.kind.as_str() {
            "sql_regexp" => (".sql_regexp", &self.sql_regexp),
            "sql_digest" => (".sql_digest", &self.sql_digest),
            "plan_digest" => (".plan_digest", &self.plan_digest),
            "stmt_label" => (".stmt_label", &self.stmt_label),
            "by_user" => (".by_user", &self.by_user),
            "table" => (".table", &self.table),
            _ => return Err("wrong dump_trigger.user_command.type".to_owned()),
        };
        if value.is_empty() {
            if suffix == ".stmt_label" {
                return Err("dump_trigger.user_command.stmt_label should not be empty, should be something in https://github.com/pingcap/tidb/blob/adf08267939416d1b989e56dba6a6544bf34a8dd/pkg/parser/ast/ast.go#L160".to_owned());
            }
            return Err(format!(
                "dump_trigger.user_command{suffix} should not be empty"
            ));
        }
        name.push_str(suffix);
        mapping.add_trigger(name.clone(), Some(conf))
    }
}

/// Go `SuspiciousEventConfig`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SuspiciousEventConfig {
    /// Go `type`.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Go `is_internal`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_internal: bool,
    /// Go `dev_debug`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev_debug: Option<DevDebugConfig>,
}

/// Go `DevDebugTypeExecuteInternalTraceMissing`.
pub const DEV_DEBUG_TYPE_EXECUTE_INTERNAL_TRACE_MISSING: &str = "execute_internal_trace_missing";
/// Go `DevDebugTypeSendRequestTraceIDMissing`.
pub const DEV_DEBUG_TYPE_SEND_REQUEST_TRACE_ID_MISSING: &str = "send_request_trace_id_missing";

/// Go `DevDebugConfig`. Go leaves the field untagged, so encoding/json matches
/// `Type` case-insensitively; both spellings are accepted here.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DevDebugConfig {
    /// Go `Type`.
    #[serde(rename = "Type", alias = "type", default)]
    pub kind: String,
}

impl DevDebugConfig {
    /// Go `DevDebugConfig.compile`.
    fn compile(
        &self,
        name: &mut String,
        mapping: &mut CompiledDumpTriggerConfig,
        conf: &DumpTriggerConfig,
    ) -> Result<u64, String> {
        name.push_str(".dev_debug");
        match self.kind.as_str() {
            DEV_DEBUG_TYPE_EXECUTE_INTERNAL_TRACE_MISSING
            | DEV_DEBUG_TYPE_SEND_REQUEST_TRACE_ID_MISSING => {
                mapping.add_trigger(name.clone(), Some(conf))
            }
            _ => Err("wrong dump_trigger.suspicious_event.dev_debug.type".to_owned()),
        }
    }
}

impl SuspiciousEventConfig {
    /// Go `SuspiciousEventConfig.compile`.
    fn compile(
        &self,
        name: &mut String,
        mapping: &mut CompiledDumpTriggerConfig,
        conf: &DumpTriggerConfig,
    ) -> Result<u64, String> {
        name.push_str(".suspicious_event");
        match self.kind.as_str() {
            "slow_query" | "query_fail" | "resolve_lock" | "region_error" => {
                mapping.add_trigger(name.clone(), Some(conf))
            }
            "is_internal" => {
                name.push_str(".is_internal");
                mapping.add_trigger(name.clone(), Some(conf))
            }
            "dev_debug" => match self.dev_debug.as_ref() {
                None => Err("dump_trigger.suspicious_event.dev_debug missing".to_owned()),
                Some(dev_debug) => dev_debug.compile(name, mapping, conf),
            },
            _ => Err("wrong dump_trigger.suspicious_event.type".to_owned()),
        }
    }
}

/// Go `DumpTriggerConfig`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DumpTriggerConfig {
    /// Go `type`.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// Go `sampling`: `sampling = n` samples one trace in every `n`.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub sampling: i64,
    /// Go `suspicious_event`.
    #[serde(
        rename = "suspicious_event",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub event: Option<SuspiciousEventConfig>,
    /// Go `user_command`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_command: Option<UserCommandConfig>,
    /// Go `and`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub and: Vec<DumpTriggerConfig>,
    /// Go `or`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub or: Vec<DumpTriggerConfig>,
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde predicate shape")]
fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

impl DumpTriggerConfig {
    /// Go `DumpTriggerConfig.Compile`: on success `name` holds the trigger's
    /// canonical name and the returned slice is its OR-of-AND truth table.
    ///
    /// # Errors
    ///
    /// Returns Go's validation message for a malformed trigger.
    pub fn compile(
        &self,
        name: &mut String,
        mapping: &mut CompiledDumpTriggerConfig,
    ) -> Result<Vec<u64>, String> {
        name.push_str("dump_trigger");
        match self.kind.as_str() {
            "sampling" => {
                if self.sampling <= 0 {
                    return Err("wrong dump_trigger.sampling".to_owned());
                }
                name.push_str(".sampling");
                Ok(vec![mapping.add_trigger(name.clone(), Some(self))?])
            }
            "suspicious_event" => match self.event.as_ref() {
                None => Err("dump_trigger.suspicious_event missing".to_owned()),
                Some(event) => Ok(vec![event.compile(name, mapping, self)?]),
            },
            "user_command" => match self.user_command.as_ref() {
                None => Err("dump_trigger.user_command missing".to_owned()),
                Some(command) => Ok(vec![command.compile(name, mapping, self)?]),
            },
            "and" => {
                if self.and.is_empty() {
                    return Err("dump_trigger.and missing".to_owned());
                }
                let mut ret = Vec::new();
                for and in &self.and {
                    let mut buf = String::new();
                    let tmp = and.compile(&mut buf, mapping)?;
                    ret = truth_table_for_and(ret, tmp);
                }
                Ok(ret)
            }
            "or" => {
                if self.or.is_empty() {
                    return Err("dump_trigger.or missing".to_owned());
                }
                let mut ret = Vec::new();
                for or in &self.or {
                    let mut buf = String::new();
                    let tmp = or.compile(&mut buf, mapping)?;
                    ret = truth_table_for_or(ret, tmp);
                }
                Ok(ret)
            }
            _ => Err("wrong dump_trigger.type".to_owned()),
        }
    }
}

/// Go `compiledDumpTriggerConfig`.
///
/// Each trigger condition owns one bit. AND is bitwise OR of the conditions'
/// bits; OR is a list of alternatives. A statement's accumulated bits satisfy
/// the trigger when they are a superset of any alternative — see
/// [`check_truth_table`].
#[derive(Clone, Debug, Default)]
pub struct CompiledDumpTriggerConfig {
    /// Go `nameMapping`: canonical trigger name to bit index.
    pub name_mapping: HashMap<String, usize>,
    /// Go `configRef`, parallel to `name_mapping` by index.
    pub config_ref: Vec<Option<DumpTriggerConfig>>,
    /// Go `truthTable`.
    pub truth_table: Vec<u64>,
}

impl CompiledDumpTriggerConfig {
    /// Go `compiledDumpTriggerConfig.addTrigger`.
    ///
    /// # Errors
    ///
    /// Rejects a duplicate name, and a 65th trigger.
    pub fn add_trigger(
        &mut self,
        canonical_name: String,
        config: Option<&DumpTriggerConfig>,
    ) -> Result<u64, String> {
        if self.name_mapping.contains_key(&canonical_name) {
            return Err(format!("duplicate trigger name: {canonical_name}"));
        }
        let idx = self.name_mapping.len();
        if idx >= 64 {
            return Err("too many triggers".to_owned());
        }
        self.name_mapping.insert(canonical_name, idx);
        self.config_ref.push(config.cloned());
        Ok(1 << idx)
    }
}

/// Go `truthTableForAnd`.
#[must_use]
pub fn truth_table_for_and(x: Vec<u64>, mut y: Vec<u64>) -> Vec<u64> {
    if x.is_empty() {
        return y;
    }
    if x.len() == 1 {
        // A && [B, C, D] => [A && B, A && C, A && D]
        truth_table_for_and1(x[0], &mut y);
        return y;
    }
    // [A || B || C] && D => [A && D || B && D || C && D]
    let mut ret = Vec::with_capacity(x.len() * y.len());
    for value in x {
        let pos = ret.len();
        ret.extend_from_slice(&y);
        truth_table_for_and1(value, &mut ret[pos..]);
    }
    ret
}

/// Go `truthTableForAnd1`.
fn truth_table_for_and1(x: u64, xs: &mut [u64]) {
    for slot in xs {
        *slot |= x;
    }
}

/// Go `truthTableForOr`.
#[must_use]
pub fn truth_table_for_or(mut x: Vec<u64>, y: Vec<u64>) -> Vec<u64> {
    // not doing any deduplication because duplicate trigger condition is not
    // allowed by compile
    x.extend(y);
    x
}

/// Go `checkTruthTable`.
#[must_use]
pub fn check_truth_table(bits: u64, table: &[u64]) -> bool {
    for &value in table {
        // The accumulated bits satisfy this alternative when they are a
        // superset of it.
        if bits & value == value {
            return true;
        }
    }
    false
}

/// Go `CheckFlightRecorderDumpTrigger`.
///
/// Go recovers the statement's `*Trace` from the context by type assertion and
/// logs a warning when the assertion fails; this port takes it explicitly (see
/// the module boundaries).
pub fn check_flight_recorder_dump_trigger(
    trace: &Trace,
    trigger_name: &str,
    check: impl Fn(Option<&DumpTriggerConfig>) -> bool,
) {
    let Some(recorder) = get_flight_recorder() else {
        return;
    };
    let Some(&idx) = recorder.compiled.name_mapping.get(trigger_name) else {
        return;
    };
    let conf = recorder.compiled.config_ref[idx].as_ref();
    if check(conf) {
        trace.mark_bits(idx);
    }
}

/// Go `FlightRecorderConfig`.
///
/// An example configuration in JSON:
///
/// ```json
/// {
///   "enabled_categories": ["general"],
///   "dump_trigger": {
///     "type": "sampling",
///     "sampling": 100
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct FlightRecorderConfig {
    /// Go `enabled_categories`.
    #[serde(default)]
    pub enabled_categories: Vec<String>,
    /// Go `dump_trigger`.
    #[serde(default)]
    pub dump_trigger: DumpTriggerConfig,
}

impl FlightRecorderConfig {
    /// Go `FlightRecorderConfig.Initialize`: dump everything except the TiKV
    /// write/read detail and developer-debug categories, whose volume would be
    /// excessive.
    pub fn initialize(&mut self) {
        self.enabled_categories = vec![
            "-".to_owned(),
            "tikv_write_details".to_owned(),
            "tikv_read_details".to_owned(),
            "dev_debug".to_owned(),
        ];
        self.dump_trigger.kind = "sampling".to_owned();
        self.dump_trigger.sampling = 1;
    }

    /// Go `FlightRecorderConfig.Compile`.
    ///
    /// # Errors
    ///
    /// Propagates the dump trigger's validation error.
    pub fn compile(&self) -> Result<CompiledDumpTriggerConfig, String> {
        let mut name = String::new();
        let mut result = CompiledDumpTriggerConfig::default();
        let truth_table = self.dump_trigger.compile(&mut name, &mut result)?;
        result.truth_table = truth_table;
        Ok(result)
    }
}

/// Go `parseCategories`.
#[must_use]
pub fn parse_categories(categories: &[String]) -> TraceCategory {
    let mut result = TraceCategory(0);
    let mut sub = false;
    for name in categories {
        if name == "*" {
            result = TraceCategory::ALL;
            break;
        }
        if name == "-" {
            result = TraceCategory::ALL;
            sub = true;
            continue;
        }
        let parsed = TraceCategory::parse(name);
        if sub {
            result = TraceCategory(result.0 & !parsed.0);
        } else {
            result = TraceCategory(result.0 | parsed.0);
        }
    }
    result
}

/// Go `HTTPFlightRecorder`: the configured recorder that keeps or drops each
/// statement's trace.
///
/// Go mutates `enabledCategories` directly from its test helpers while the
/// recorder is reachable through a global pointer; that field is atomic here so
/// the same mutation is sound behind the shared handle.
#[derive(Debug)]
pub struct HttpFlightRecorder {
    ch: Option<Sender<Vec<Event>>>,
    enabled_categories: AtomicU64,
    /// Go `counter`, used when the dump trigger is `sampling`.
    counter: AtomicI64,
    /// Go `Config`.
    pub config: FlightRecorderConfig,
    compiled: CompiledDumpTriggerConfig,
}

/// Go `globalHTTPFlightRecorder`.
static GLOBAL_HTTP_FLIGHT_RECORDER: RwLock<Option<Arc<HttpFlightRecorder>>> = RwLock::new(None);

fn store_global(recorder: Option<Arc<HttpFlightRecorder>>) {
    *GLOBAL_HTTP_FLIGHT_RECORDER
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = recorder;
}

/// Go `GetFlightRecorder`.
#[must_use]
pub fn get_flight_recorder() -> Option<Arc<HttpFlightRecorder>> {
    GLOBAL_HTTP_FLIGHT_RECORDER
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Go `newHTTPFlightRecorder`.
fn new_http_flight_recorder(
    config: FlightRecorderConfig,
) -> Result<Arc<HttpFlightRecorder>, String> {
    let compiled = config.compile()?;
    let categories = parse_categories(&config.enabled_categories);
    let recorder = Arc::new(HttpFlightRecorder {
        ch: None,
        enabled_categories: AtomicU64::new(categories.0),
        counter: AtomicI64::new(0),
        config,
        compiled,
    });
    logutil::bg_logger().info(
        "start http flight recorder",
        &[
            Field::new("category", Value::Str(categories.name())),
            Field::new(
                "mapping",
                Value::Str(format!("{:?}", recorder.compiled.name_mapping)),
            ),
            Field::new(
                "truthTable",
                Value::Array(
                    recorder
                        .compiled
                        .truth_table
                        .iter()
                        .map(|&bits| Value::U64(bits))
                        .collect(),
                ),
            ),
        ],
    );
    store_global(Some(Arc::clone(&recorder)));
    Ok(recorder)
}

/// Go `StartHTTPFlightRecorder`.
///
/// # Errors
///
/// Propagates the configuration's compile error.
pub fn start_http_flight_recorder(
    ch: Sender<Vec<Event>>,
    config: FlightRecorderConfig,
) -> Result<Arc<HttpFlightRecorder>, String> {
    // Go assigns `ret.ch` after construction; the channel is part of the
    // recorder's identity here, so it is installed before publishing.
    let compiled = config.compile()?;
    let categories = parse_categories(&config.enabled_categories);
    let recorder = Arc::new(HttpFlightRecorder {
        ch: Some(ch),
        enabled_categories: AtomicU64::new(categories.0),
        counter: AtomicI64::new(0),
        config,
        compiled,
    });
    store_global(Some(Arc::clone(&recorder)));
    Ok(recorder)
}

/// Go `StartLogFlightRecorder`: starts the recorder that sinks to the log.
///
/// # Errors
///
/// Propagates the configuration's compile error.
pub fn start_log_flight_recorder(config: FlightRecorderConfig) -> Result<(), String> {
    new_http_flight_recorder(config).map(|_| ())
}

impl HttpFlightRecorder {
    /// Go `HTTPFlightRecorder.Close`. Go's method ignores its receiver and
    /// simply clears the global pointer; so does this.
    pub fn close(&self) {
        store_global(None);
    }

    /// Go `HTTPFlightRecorder.enabledCategories`.
    #[must_use]
    pub fn enabled_categories(&self) -> TraceCategory {
        TraceCategory(self.enabled_categories.load(Ordering::SeqCst))
    }

    /// Go's test-only `HTTPFlightRecorder.SetCategories`.
    pub fn set_categories(&self, categories: TraceCategory) {
        self.enabled_categories
            .store(categories.0, Ordering::SeqCst);
    }

    /// Go's test-only `HTTPFlightRecorder.Disable`.
    pub fn disable(&self, categories: TraceCategory) {
        self.enabled_categories
            .fetch_and(!categories.0, Ordering::SeqCst);
    }

    /// Go's test-only `HTTPFlightRecorder.Enable`.
    pub fn enable(&self, categories: TraceCategory) {
        self.enabled_categories
            .fetch_or(categories.0, Ordering::SeqCst);
    }

    /// Go `HTTPFlightRecorder.truthTable`.
    #[must_use]
    pub fn truth_table(&self) -> &[u64] {
        &self.compiled.truth_table
    }

    /// Go `HTTPFlightRecorder.nameMapping`.
    #[must_use]
    pub fn name_mapping(&self) -> &HashMap<String, usize> {
        &self.compiled.name_mapping
    }

    /// Go `HTTPFlightRecorder.shouldKeep`.
    #[must_use]
    pub fn should_keep(&self, bits: u64) -> bool {
        check_truth_table(bits, &self.compiled.truth_table)
    }

    /// Go `HTTPFlightRecorder.CheckSampling`.
    #[must_use]
    pub fn check_sampling(&self, conf: &DumpTriggerConfig) -> bool {
        let value = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        if value >= conf.sampling {
            self.counter.store(0, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// Go `HTTPFlightRecorder.collect`. A recorder without a channel is the
    /// log flight recorder and logs each event instead.
    pub fn collect(&self, ctx: Option<&TraceContext>, events: Vec<Event>) {
        match self.ch.as_ref() {
            None => {
                for event in &events {
                    log_event(ctx, event);
                }
            }
            // Go's `select` with a `default` arm drops the batch when the
            // consumer is not ready.
            Some(ch) => drop(ch.try_send(events)),
        }
    }
}
