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

//! Pinned Go `pkg/ddl/serverstate`, ported as one package: the etcd-backed
//! smooth-upgrade state syncer and the process-global in-memory stand-in used
//! by a single-node unistore deployment.

use std::sync::mpsc::{self, Receiver, RecvError, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use prometheus::{exponential_buckets, CounterVec, HistogramOpts, HistogramVec, Opts};
use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, Visitor};
use serde::{Deserializer, Serialize};
use tidb_log::{Field, Value};
use tidb_pd_client::{EtcdClient, EtcdError, EtcdLeaseSession, EtcdWatcher};
pub use tidb_util::timeutil::SleepContext as Context;
use tidb_util::timeutil::{sleep, SleepError};

/// Go `util.ServerGlobalState`.
pub const SERVER_GLOBAL_STATE: &str = "/tidb/server/global_state";
/// Go `StateUpgrading`.
pub const STATE_UPGRADING: &str = "upgrading";
/// Go `StateNormalRunning`.
pub const STATE_NORMAL_RUNNING: &str = "";

const KEY_OP_DEFAULT_RETRY_COUNT: usize = 3;
const GET_KEY_TIMEOUT: Duration = Duration::from_secs(1);
const PUT_KEY_TIMEOUT: Duration = Duration::from_secs(2);
const GET_KEY_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const PUT_KEY_RETRY_INTERVAL: Duration = Duration::from_millis(30);
const NEW_SESSION_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const SESSION_TTL_SECONDS: i64 = 90;
const STATE_PROMPT: &str = "global-state-syncer";

static DEPLOY_SYNCER_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram(
        "tidb_ddl_deploy_syncer_duration_seconds",
        "Bucketed histogram of processing time (s) of deploy syncer",
        0.001,
        20,
    )
});

static OWNER_HANDLE_SYNCER_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram(
        "tidb_ddl_owner_handle_syncer_duration_seconds",
        "Bucketed histogram of processing time (s) of handle syncer",
        0.001,
        20,
    )
});

static NEW_SESSION_HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new(
            "tidb_owner_new_session_duration_seconds",
            "Bucketed histogram of processing time (s) of new session.",
        )
        .buckets(exponential_buckets(0.0005, 2.0, 22).expect("valid session buckets")),
        &["type", "result"],
    )
    .expect("valid new-session histogram");
    prometheus::default_registry()
        .register(Box::new(histogram.clone()))
        .expect("register new-session histogram");
    histogram
});

static RETRYABLE_ERROR_COUNT: LazyLock<CounterVec> = LazyLock::new(|| {
    let counter = CounterVec::new(
        Opts::new(
            "tidb_ddl_retryable_error_total",
            "Retryable error count during ddl.",
        ),
        &["type"],
    )
    .expect("valid retryable-error counter");
    prometheus::default_registry()
        .register(Box::new(counter.clone()))
        .expect("register retryable-error counter");
    counter
});

fn register_histogram(name: &str, help: &str, start: f64, count: usize) -> HistogramVec {
    let histogram = HistogramVec::new(
        HistogramOpts::new(name, help)
            .buckets(exponential_buckets(start, 2.0, count).expect("valid syncer buckets")),
        &["type", "result"],
    )
    .expect("valid syncer histogram");
    prometheus::default_registry()
        .register(Box::new(histogram.clone()))
        .expect("register syncer histogram");
    histogram
}

fn result_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "err"
    }
}

fn child_timeout(context: &Context, maximum: Duration) -> Result<Duration, Error> {
    if context.is_cancelled() {
        return Err(sleep(context, Duration::ZERO).unwrap_err().into());
    }
    Ok(context
        .remaining()
        .map_or(maximum, |remaining| remaining.min(maximum)))
}

/// Go `StateInfo`'s exact JSON payload.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct StateInfo {
    /// Empty means normal running; `upgrading` means smooth upgrade.
    pub state: String,
}

impl StateInfo {
    /// Go `NewStateInfo`.
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
        }
    }

    /// Go `StateInfo.Marshal`.
    pub fn marshal(&self) -> Result<Vec<u8>, serde_json::Error> {
        tidb_model::serde_helpers::to_go_json(self)
    }

    /// Go `StateInfo.Unmarshal`.
    pub fn unmarshal(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        // encoding/json validates the complete input before touching the
        // receiver, then applies valid fields in encounter order even when a
        // different field has a type error.
        serde_json::from_slice::<IgnoredAny>(bytes)?;
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        StateInfoSeed {
            state: &mut self.state,
        }
        .deserialize(&mut deserializer)?;
        deserializer.end()
    }
}

struct StateInfoSeed<'a> {
    state: &'a mut String,
}

impl<'de> DeserializeSeed<'de> for StateInfoSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StateInfoVisitor<'a> {
            state: &'a mut String,
        }

        impl<'de> Visitor<'de> for StateInfoVisitor<'_> {
            type Value = ();

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a server state object or null")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(())
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(())
            }

            fn visit_map<A>(self, mut fields: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut type_error = false;
                while let Some(key) = fields.next_key::<String>()? {
                    // encoding/json prefers an exact tag match, then accepts a
                    // Unicode-folded match. For the ASCII tag `state`, long-s
                    // is the only additional Unicode simple-fold spelling.
                    let matches_state = key.eq_ignore_ascii_case("state")
                        || key
                            .strip_prefix('\u{17f}')
                            .is_some_and(|suffix| suffix.eq_ignore_ascii_case("tate"));
                    if matches_state {
                        match fields.next_value::<serde_json::Value>()? {
                            serde_json::Value::Null => {}
                            serde_json::Value::String(value) => *self.state = value,
                            _ => type_error = true,
                        }
                    } else {
                        fields.next_value::<IgnoredAny>()?;
                    }
                }
                if type_error {
                    return Err(A::Error::custom(
                        "cannot unmarshal non-string JSON into StateInfo.state",
                    ));
                }
                Ok(())
            }
        }

        deserializer.deserialize_any(StateInfoVisitor { state: self.state })
    }
}

/// One event delivered by Go `WatchChan`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchEvent {
    /// Exact watched key.
    pub key: Vec<u8>,
    /// Exact value bytes (empty for deletion).
    pub value: Vec<u8>,
    /// Whether etcd deleted the key.
    pub deleted: bool,
    /// Etcd modification revision.
    pub mod_revision: i64,
}

/// One Go `clientv3.WatchResponse`, retaining its event batch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchResponse {
    /// Store revision carried by the etcd response header.
    pub header_revision: i64,
    /// Events delivered together by etcd. The memory syncer deliberately
    /// sends an empty response as a reload notification.
    pub events: Vec<WatchEvent>,
    /// Whether etcd canceled this watch.
    pub canceled: bool,
    /// Minimum available revision for a compacted watch.
    pub compact_revision: i64,
    /// Server-provided cancellation reason.
    pub cancel_reason: String,
}

/// A clonable handle onto one Go-style watch channel.
#[derive(Clone, Debug)]
pub struct WatchChannel(Arc<Mutex<Receiver<WatchResponse>>>);

impl WatchChannel {
    /// Blocks for the next watch response.
    pub fn recv(&self) -> Result<WatchResponse, RecvError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv()
    }

    /// Waits at most `timeout` for the next watch response.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<WatchResponse, RecvTimeoutError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv_timeout(timeout)
    }

    /// Receives an already-buffered response without blocking.
    pub fn try_recv(&self) -> Result<WatchResponse, TryRecvError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_recv()
    }
}

/// Failures exposed by the server-state syncer.
#[derive(Debug)]
pub enum Error {
    /// Etcd get/put/watch failure.
    Etcd(EtcdError),
    /// Stored state is not valid Go `StateInfo` JSON.
    Json(serde_json::Error),
    /// The caller's Go-style context was cancelled or its deadline elapsed.
    Context(SleepError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Etcd(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Context(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<EtcdError> for Error {
    fn from(error: EtcdError) -> Self {
        Self::Etcd(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<SleepError> for Error {
    fn from(error: SleepError) -> Self {
        Self::Context(error)
    }
}

/// Go `Syncer`'s behavior surface.
pub trait Syncer: Send + Sync {
    /// Loads the current state and starts the watch.
    fn init(&self, context: &Context) -> Result<(), Error>;
    /// Persists the global state.
    fn update_global_state(&self, context: &Context, state: &StateInfo) -> Result<(), Error>;
    /// Reloads and caches the global state.
    fn get_global_state(&self, context: &Context) -> Result<StateInfo, Error>;
    /// Whether the last loaded state is upgrading.
    fn is_upgrading_state(&self) -> bool;
    /// Returns the syncer's watch channel.
    fn watch_chan(&self) -> Option<WatchChannel>;
    /// Re-establishes the watch.
    fn rewatch(&self, context: &Context);
}

fn create_session(
    client: &Arc<EtcdClient>,
    context: &Context,
    log_prefix: &str,
) -> Result<EtcdLeaseSession, Error> {
    let mut last = None;
    for failed_count in 0..KEY_OP_DEFAULT_RETRY_COUNT {
        if context.is_cancelled() {
            return Err(sleep(context, Duration::ZERO).unwrap_err().into());
        }
        let timeout = context.remaining().unwrap_or_else(|| client.timeout());
        let session_context = context.clone();
        let started = Instant::now();
        let result = client.lease_session(SESSION_TTL_SECONDS, timeout, move || {
            session_context.is_cancelled()
        });
        NEW_SESSION_HISTOGRAM
            .with_label_values(&[log_prefix, result_label(&result)])
            .observe(started.elapsed().as_secs_f64());
        match result {
            Ok(session) => return Ok(session),
            Err(error) => {
                if failed_count % 15 == 0 {
                    tidb_ddl_logutil::ddl_logger().warn(
                        "failed to establish new session to etcd",
                        &[
                            Field::new("ownerInfo", Value::Str(log_prefix.to_owned())),
                            Field::new(
                                "error",
                                Value::Error {
                                    basic: error.to_string(),
                                    verbose: None,
                                },
                            ),
                        ],
                    );
                }
                last = Some(error);
                std::thread::sleep(NEW_SESSION_RETRY_INTERVAL);
            }
        }
    }
    Err(last
        .expect("the retry loop performs at least one session attempt")
        .into())
}

/// Go `etcdSyncer`.
pub struct EtcdSyncer {
    client: Arc<EtcdClient>,
    path: Vec<u8>,
    cluster_state: RwLock<StateInfo>,
    watcher: Arc<Mutex<Option<(EtcdWatcher, WatchChannel)>>>,
    session: Mutex<Option<EtcdLeaseSession>>,
}

impl EtcdSyncer {
    /// Go `NewEtcdSyncer`.
    pub fn new(client: Arc<EtcdClient>, path: impl Into<Vec<u8>>) -> Self {
        Self {
            client,
            path: path.into(),
            cluster_state: RwLock::new(StateInfo::new(STATE_NORMAL_RUNNING)),
            watcher: Arc::new(Mutex::new(None)),
            session: Mutex::new(None),
        }
    }

    fn make_watch(
        client: &EtcdClient,
        path: Vec<u8>,
        context: Context,
    ) -> Result<(EtcdWatcher, WatchChannel), EtcdError> {
        let (sender, receiver) = mpsc::channel();
        let event_key = path.clone();
        let cancel_context = context.clone();
        let watcher = client.watch_key_responses(
            path,
            0,
            move || cancel_context.is_cancelled(),
            move |response| {
                let _ = sender.send(WatchResponse {
                    header_revision: response.header_revision,
                    events: response
                        .events
                        .iter()
                        .map(|event| WatchEvent {
                            key: event_key.clone(),
                            value: event.value.clone(),
                            deleted: event.deleted,
                            mod_revision: event.mod_revision,
                        })
                        .collect(),
                    canceled: response.canceled,
                    compact_revision: response.compact_revision,
                    cancel_reason: response.cancel_reason.clone(),
                });
            },
        )?;
        Ok((watcher, WatchChannel(Arc::new(Mutex::new(receiver)))))
    }

    fn start_watch(&self, context: &Context) -> Result<(EtcdWatcher, WatchChannel), EtcdError> {
        Self::make_watch(&self.client, self.path.clone(), context.clone())
    }

    fn get_with_retry(&self, context: &Context) -> Result<Option<Vec<u8>>, Error> {
        let mut last = None;
        for _ in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            let timeout = child_timeout(context, GET_KEY_TIMEOUT)?;
            match self.client.get_with_timeout(&self.path, timeout) {
                Ok(value) => return Ok(value),
                Err(error) => {
                    tidb_ddl_logutil::ddl_logger().info(
                        "get key failed",
                        &[
                            Field::new(
                                "key",
                                Value::Str(String::from_utf8_lossy(&self.path).into_owned()),
                            ),
                            Field::new(
                                "error",
                                Value::Error {
                                    basic: error.to_string(),
                                    verbose: None,
                                },
                            ),
                        ],
                    );
                    last = Some(error);
                    std::thread::sleep(GET_KEY_RETRY_INTERVAL);
                }
            }
        }
        Err(last
            .expect("the retry loop performs at least one get")
            .into())
    }

    fn put_with_retry(&self, context: &Context, value: &[u8]) -> Result<(), Error> {
        let mut last = None;
        for retry_count in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            let timeout = child_timeout(context, PUT_KEY_TIMEOUT)?;
            match self.client.put_with_timeout(&self.path, value, timeout) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    RETRYABLE_ERROR_COUNT
                        .with_label_values(&[&error.to_string()])
                        .inc();
                    tidb_ddl_logutil::ddl_logger().warn(
                        "etcd-cli put kv failed",
                        &[
                            Field::new(
                                "key",
                                Value::Str(String::from_utf8_lossy(&self.path).into_owned()),
                            ),
                            Field::new(
                                "value",
                                Value::Str(String::from_utf8_lossy(value).into_owned()),
                            ),
                            Field::new(
                                "error",
                                Value::Error {
                                    basic: error.to_string(),
                                    verbose: None,
                                },
                            ),
                            Field::new("retryCnt", Value::I64(retry_count as i64)),
                        ],
                    );
                    last = Some(error);
                    std::thread::sleep(PUT_KEY_RETRY_INTERVAL);
                }
            }
        }
        Err(last
            .expect("the retry loop performs at least one put")
            .into())
    }
}

impl Syncer for EtcdSyncer {
    fn init(&self, context: &Context) -> Result<(), Error> {
        let started = Instant::now();
        let result = (|| {
            let path = String::from_utf8_lossy(&self.path);
            let session =
                create_session(&self.client, context, &format!("[{STATE_PROMPT}] {path}"))?;
            *self
                .session
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session);
            self.get_global_state(context)?;
            if let Ok(watcher) = self.start_watch(context) {
                *self
                    .watcher
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watcher);
            }
            Ok(())
        })();
        DEPLOY_SYNCER_HISTOGRAM
            .with_label_values(&["init_global_state", result_label(&result)])
            .observe(started.elapsed().as_secs_f64());
        result
    }

    fn update_global_state(&self, context: &Context, state: &StateInfo) -> Result<(), Error> {
        let started = Instant::now();
        let value = state.marshal()?;
        let result = self.put_with_retry(context, &value);
        OWNER_HANDLE_SYNCER_HISTOGRAM
            .with_label_values(&["update_global_state", result_label(&result)])
            .observe(started.elapsed().as_secs_f64());
        result
    }

    fn get_global_state(&self, context: &Context) -> Result<StateInfo, Error> {
        let started = Instant::now();
        let mut state = StateInfo::default();
        let Some(value) = self.get_with_retry(context)? else {
            *self
                .cluster_state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = state.clone();
            return Ok(state);
        };
        if let Err(error) = state.unmarshal(&value) {
            tidb_ddl_logutil::ddl_logger().warn(
                "get global state failed",
                &[
                    Field::new(
                        "key",
                        Value::Str(String::from_utf8_lossy(&self.path).into_owned()),
                    ),
                    Field::new("value", Value::ByteString(value)),
                    Field::new(
                        "error",
                        Value::Error {
                            basic: error.to_string(),
                            verbose: None,
                        },
                    ),
                ],
            );
            return Err(error.into());
        }
        *self
            .cluster_state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state.clone();
        OWNER_HANDLE_SYNCER_HISTOGRAM
            .with_label_values(&["update_global_state", "ok"])
            .observe(started.elapsed().as_secs_f64());
        Ok(state)
    }

    fn is_upgrading_state(&self) -> bool {
        self.cluster_state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
            == STATE_UPGRADING
    }

    fn watch_chan(&self) -> Option<WatchChannel> {
        self.watcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(_, receiver)| receiver.clone())
    }

    fn rewatch(&self, context: &Context) {
        let started = Instant::now();
        *self
            .watcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let client = Arc::clone(&self.client);
        let path = self.path.clone();
        let context = context.clone();
        let watcher_slot = Arc::clone(&self.watcher);
        let _ = std::thread::Builder::new()
            .name("ddl-state-rewatch".to_owned())
            .spawn(move || {
                let watcher = Self::make_watch(&client, path, context);
                if let Ok(watcher) = watcher {
                    *watcher_slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watcher);
                }
                DEPLOY_SYNCER_HISTOGRAM
                    .with_label_values(&["rewatch", "ok"])
                    .observe(started.elapsed().as_secs_f64());
                tidb_ddl_logutil::ddl_logger().info("syncer rewatch global info finished", &[]);
            });
    }
}

fn memory_cluster_state() -> &'static RwLock<Option<StateInfo>> {
    static STATE: OnceLock<RwLock<Option<StateInfo>>> = OnceLock::new();
    STATE.get_or_init(|| RwLock::new(None))
}

/// Go `memSyncer`.
pub struct MemSyncer {
    watch: Mutex<Option<(SyncSender<WatchResponse>, WatchChannel)>>,
}

impl MemSyncer {
    /// Go `NewMemSyncer` followed by `Init`'s channel construction.
    pub fn new() -> Self {
        Self {
            watch: Mutex::new(None),
        }
    }
}

impl Default for MemSyncer {
    fn default() -> Self {
        Self::new()
    }
}

impl Syncer for MemSyncer {
    fn init(&self, _context: &Context) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        *self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((sender, WatchChannel(Arc::new(Mutex::new(receiver)))));
        let mut state = memory_cluster_state()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.is_none() {
            *state = Some(StateInfo::new(STATE_NORMAL_RUNNING));
        }
        Ok(())
    }

    fn update_global_state(&self, _context: &Context, state: &StateInfo) -> Result<(), Error> {
        #[cfg(feature = "failpoints")]
        if fail::eval("mockUpgradingState", |value| {
            value.as_deref() == Some("true")
        })
        .unwrap_or(false)
        {
            *memory_cluster_state()
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(state.clone());
            return Ok(());
        }
        let sender = self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(sender, _)| sender.clone())
            .expect("memSyncer.UpdateGlobalState requires Init");
        let _ = sender.send(WatchResponse::default());
        *memory_cluster_state()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(state.clone());
        Ok(())
    }

    fn get_global_state(&self, _context: &Context) -> Result<StateInfo, Error> {
        Ok(memory_cluster_state()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("memSyncer.GetGlobalState requires Init")
            .clone())
    }

    fn is_upgrading_state(&self) -> bool {
        memory_cluster_state()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("memSyncer.IsUpgradingState requires Init")
            .state
            == STATE_UPGRADING
    }

    fn watch_chan(&self) -> Option<WatchChannel> {
        self.watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(_, receiver)| receiver.clone())
    }

    fn rewatch(&self, _context: &Context) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;

    fn memory_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: Mutex<()> = Mutex::new(());
        GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn metric_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: Mutex<()> = Mutex::new(());
        GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn counter_total(counter: &CounterVec) -> f64 {
        counter
            .collect()
            .iter()
            .flat_map(|family| family.get_metric())
            .map(|metric| metric.get_counter().get_value())
            .sum()
    }

    fn histogram_count(histogram: &HistogramVec) -> u64 {
        histogram
            .collect()
            .iter()
            .flat_map(|family| family.get_metric())
            .map(|metric| metric.get_histogram().get_sample_count())
            .sum()
    }

    #[deny(unused_must_use)]
    #[test]
    fn go_constructor_return_values_can_be_ignored() {
        StateInfo::new(STATE_NORMAL_RUNNING);
        MemSyncer::new();
        let client =
            Arc::new(EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(20)).unwrap());
        EtcdSyncer::new(client, SERVER_GLOBAL_STATE);
    }

    #[test]
    fn state_info_uses_go_json() {
        let state = StateInfo::new(STATE_UPGRADING);
        assert_eq!(state.marshal().unwrap(), br#"{"state":"upgrading"}"#);
        let mut decoded = StateInfo::default();
        decoded.unmarshal(&state.marshal().unwrap()).unwrap();
        assert_eq!(decoded, state);
        assert_eq!(StateInfo::default().marshal().unwrap(), br#"{"state":""}"#);
        assert_eq!(
            StateInfo::new("<>&\u{2028}\u{2029}").marshal().unwrap(),
            br#"{"state":"\u003c\u003e\u0026\u2028\u2029"}"#
        );

        decoded.unmarshal(br#"{"ignored":1}"#).unwrap();
        assert_eq!(
            decoded, state,
            "unknown and absent fields preserve Go state"
        );
        decoded.unmarshal(br#"{"state":null}"#).unwrap();
        assert_eq!(decoded, state, "JSON null has no effect on a Go string");
        decoded
            .unmarshal(br#"{"STATE":"first","state":null,"\u017ftate":"last"}"#)
            .unwrap();
        assert_eq!(decoded.state, "last");
        decoded.unmarshal(b"null").unwrap();
        assert_eq!(
            decoded.state, "last",
            "JSON null has no effect on a Go struct"
        );
        assert!(decoded.unmarshal(br#"{"state":"kept","state":1}"#).is_err());
        assert_eq!(decoded.state, "kept");
        assert!(decoded
            .unmarshal(br#"{"state":1,"state":"later"}"#)
            .is_err());
        assert_eq!(decoded.state, "later");
        assert!(decoded.unmarshal(br#"{"state":"invalid"#).is_err());
        assert_eq!(
            decoded.state, "later",
            "Go validates malformed JSON before mutating the receiver"
        );
    }

    #[test]
    fn child_operations_use_the_earlier_context_deadline() {
        assert_eq!(
            child_timeout(&Context::background(), GET_KEY_TIMEOUT).unwrap(),
            GET_KEY_TIMEOUT
        );
        let context = Context::with_timeout(Duration::from_millis(100));
        assert!(child_timeout(&context, GET_KEY_TIMEOUT).unwrap() <= Duration::from_millis(100));
        context.cancel();
        assert!(matches!(
            child_timeout(&context, GET_KEY_TIMEOUT),
            Err(Error::Context(SleepError::Cancelled))
        ));
    }

    #[test]
    fn failed_key_operations_keep_go_final_retry_delay() {
        let _guard = metric_test_guard();
        let client =
            Arc::new(EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(20)).unwrap());
        let syncer = EtcdSyncer::new(client, SERVER_GLOBAL_STATE);
        let context = Context::background();

        let get_started = std::time::Instant::now();
        let retries_before_get = counter_total(&RETRYABLE_ERROR_COUNT);
        assert!(syncer.get_with_retry(&context).is_err());
        assert_eq!(counter_total(&RETRYABLE_ERROR_COUNT), retries_before_get);
        assert!(
            get_started.elapsed() >= GET_KEY_RETRY_INTERVAL * 3,
            "Go sleeps 200ms after every failed get, including the last"
        );

        let put_started = std::time::Instant::now();
        let retries_before_put = counter_total(&RETRYABLE_ERROR_COUNT);
        assert!(syncer.put_with_retry(&context, b"{}").is_err());
        assert_eq!(
            counter_total(&RETRYABLE_ERROR_COUNT),
            retries_before_put + 3.0
        );
        assert!(
            put_started.elapsed() >= PUT_KEY_RETRY_INTERVAL * 3,
            "PutKVToEtcd sleeps 30ms after every failed put, including the last"
        );
    }

    #[test]
    fn new_session_metric_records_each_go_retry_attempt() {
        let _guard = metric_test_guard();
        let client =
            Arc::new(EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(20)).unwrap());
        let before = histogram_count(&NEW_SESSION_HISTOGRAM);
        assert!(create_session(&client, &Context::background(), "[test] /state").is_err());
        assert_eq!(histogram_count(&NEW_SESSION_HISTOGRAM), before + 3);
    }

    #[test]
    fn memory_syncer_keeps_process_global_state_and_delivers_update_notification() {
        let _guard = memory_test_guard();
        let first = MemSyncer::new();
        let context = Context::background();
        assert!(first.watch_chan().is_none());
        first.init(&context).unwrap();
        first
            .update_global_state(&context, &StateInfo::new(STATE_UPGRADING))
            .unwrap();
        let response = first
            .watch_chan()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(response.events.is_empty());
        assert!(first.is_upgrading_state());

        let second = MemSyncer::new();
        second.init(&context).unwrap();
        assert_eq!(
            second.get_global_state(&context).unwrap(),
            StateInfo::new(STATE_UPGRADING)
        );
    }

    #[test]
    fn etcd_watch_channel_closes_with_its_context() {
        let client =
            Arc::new(EtcdClient::connect(["127.0.0.1:1"], Duration::from_secs(1)).unwrap());
        let syncer = EtcdSyncer::new(client, SERVER_GLOBAL_STATE);
        let context = Context::background();
        let (watcher, channel) = syncer.start_watch(&context).unwrap();
        context.cancel();
        assert_eq!(
            channel.recv_timeout(Duration::from_secs(1)),
            Err(RecvTimeoutError::Disconnected)
        );
        drop(watcher);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn mock_upgrading_state_stores_without_notifying() {
        let _guard = memory_test_guard();
        let context = Context::background();
        let syncer = MemSyncer::new();
        syncer.init(&context).unwrap();
        fail::cfg("mockUpgradingState", "return(true)").unwrap();
        syncer
            .update_global_state(&context, &StateInfo::new(STATE_UPGRADING))
            .unwrap();
        fail::remove("mockUpgradingState");
        assert!(syncer.is_upgrading_state());
        assert_eq!(
            syncer.watch_chan().unwrap().try_recv(),
            Err(TryRecvError::Empty)
        );
    }

    /// Pinned Go `TestStateSyncerSimple`, against a caller-provided PD's
    /// embedded etcd. Run with `TIDB_ETCD_PROBE_PD=127.0.0.1:2379`.
    #[test]
    #[ignore = "requires a live PD; set TIDB_ETCD_PROBE_PD"]
    fn etcd_syncer_watches_and_then_reloads_the_global_state() {
        let endpoint = std::env::var("TIDB_ETCD_PROBE_PD")
            .expect("set TIDB_ETCD_PROBE_PD to a PD client address");
        let client = Arc::new(
            EtcdClient::connect([endpoint], Duration::from_secs(5))
                .expect("connect the etcd client"),
        );
        let original = client
            .get(SERVER_GLOBAL_STATE.as_bytes())
            .expect("read the original state");
        let context = Context::background();
        let syncer = EtcdSyncer::new(Arc::clone(&client), SERVER_GLOBAL_STATE);
        syncer.init(&context).expect("initialize the state syncer");
        assert_eq!(
            syncer.get_global_state(&context).unwrap(),
            original
                .as_deref()
                .map(|bytes| {
                    let mut state = StateInfo::default();
                    state.unmarshal(bytes).map(|()| state)
                })
                .transpose()
                .unwrap()
                .unwrap_or_default()
        );

        // The watch is established asynchronously, as in the PD-client
        // transport integration test.
        std::thread::sleep(Duration::from_millis(500));
        syncer
            .update_global_state(&context, &StateInfo::new(STATE_UPGRADING))
            .unwrap();
        let response = syncer
            .watch_chan()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("the state PUT reaches the watch");
        assert_eq!(response.events.len(), 1);
        let event = &response.events[0];
        assert_eq!(event.key, SERVER_GLOBAL_STATE.as_bytes());
        assert_eq!(event.value, br#"{"state":"upgrading"}"#);
        assert!(!syncer.is_upgrading_state());
        assert_eq!(
            syncer.get_global_state(&context).unwrap(),
            StateInfo::new(STATE_UPGRADING)
        );
        assert!(syncer.is_upgrading_state());

        syncer
            .update_global_state(&context, &StateInfo::new(STATE_NORMAL_RUNNING))
            .unwrap();
        syncer
            .watch_chan()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("the normal-state PUT reaches the watch");
        assert!(syncer.is_upgrading_state());
        assert_eq!(
            syncer.get_global_state(&context).unwrap(),
            StateInfo::new(STATE_NORMAL_RUNNING)
        );
        assert!(!syncer.is_upgrading_state());

        match original {
            Some(value) => client
                .put(SERVER_GLOBAL_STATE.as_bytes(), &value)
                .expect("restore the original state"),
            None => client
                .delete(SERVER_GLOBAL_STATE.as_bytes())
                .expect("remove the test state"),
        }
    }
}
