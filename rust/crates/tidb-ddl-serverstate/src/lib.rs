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

use std::sync::mpsc::{
    self, Receiver, RecvError, RecvTimeoutError, Sender, SyncSender, TryRecvError,
};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use tidb_pd_client::{EtcdClient, EtcdError, EtcdWatchEvent, EtcdWatcher};
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
    #[must_use]
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
        if let Some(state) = serde_json::from_slice::<StateInfoUpdate>(bytes)?.0 {
            self.state = state;
        }
        Ok(())
    }
}

struct StateInfoUpdate(Option<String>);

impl<'de> Deserialize<'de> for StateInfoUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StateInfoVisitor;

        impl<'de> Visitor<'de> for StateInfoVisitor {
            type Value = StateInfoUpdate;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a server state object or null")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(StateInfoUpdate(None))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(StateInfoUpdate(None))
            }

            fn visit_map<A>(self, mut fields: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut state = None;
                while let Some(key) = fields.next_key::<String>()? {
                    // encoding/json prefers an exact tag match, then accepts a
                    // Unicode-folded match. For the ASCII tag `state`, long-s
                    // is the only additional Unicode simple-fold spelling.
                    let matches_state = key.eq_ignore_ascii_case("state")
                        || key
                            .strip_prefix('\u{17f}')
                            .is_some_and(|suffix| suffix.eq_ignore_ascii_case("tate"));
                    if matches_state {
                        if let Some(value) = fields.next_value::<Option<String>>()? {
                            state = Some(value);
                        }
                    } else {
                        fields.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(StateInfoUpdate(state))
            }
        }

        deserializer.deserialize_any(StateInfoVisitor)
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

/// A clonable handle onto one Go-style watch channel.
#[derive(Clone, Debug)]
pub struct WatchChannel(Arc<Mutex<Receiver<WatchEvent>>>);

impl WatchChannel {
    /// Blocks for the next watch response.
    pub fn recv(&self) -> Result<WatchEvent, RecvError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv()
    }

    /// Waits at most `timeout` for the next watch response.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<WatchEvent, RecvTimeoutError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv_timeout(timeout)
    }

    /// Receives an already-buffered response without blocking.
    pub fn try_recv(&self) -> Result<WatchEvent, TryRecvError> {
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

struct EtcdSession {
    client: Arc<EtcdClient>,
    lease: i64,
    stop: Sender<()>,
    keeper: Option<JoinHandle<()>>,
}

impl EtcdSession {
    fn create(client: Arc<EtcdClient>, context: &Context) -> Result<Self, Error> {
        let mut last = None;
        for _ in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            if context.is_cancelled() {
                return Err(sleep(context, Duration::ZERO).unwrap_err().into());
            }
            let result = context.remaining().map_or_else(
                || client.lease_grant(SESSION_TTL_SECONDS),
                |timeout| client.lease_grant_with_timeout(SESSION_TTL_SECONDS, timeout),
            );
            match result {
                Ok((lease, _)) => {
                    let (stop, keeper_stop) = mpsc::channel();
                    let keeper_client = Arc::clone(&client);
                    let keeper = std::thread::Builder::new()
                        .name("ddl-state-session".to_owned())
                        .spawn(move || {
                            let cadence = Duration::from_secs(
                                u64::try_from(SESSION_TTL_SECONDS).unwrap_or(1).div_ceil(3),
                            );
                            loop {
                                match keeper_stop.recv_timeout(cadence) {
                                    Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
                                    Err(RecvTimeoutError::Timeout) => {}
                                }
                                if keeper_client.lease_keep_alive_once(lease).is_err() {
                                    return;
                                }
                            }
                        })
                        .map_err(|error| Error::Etcd(EtcdError::Runtime(error.to_string())))?;
                    return Ok(Self {
                        client,
                        lease,
                        stop,
                        keeper: Some(keeper),
                    });
                }
                Err(error) => {
                    last = Some(error);
                    std::thread::sleep(NEW_SESSION_RETRY_INTERVAL);
                }
            }
        }
        Err(last
            .expect("the retry loop performs at least one lease grant")
            .into())
    }
}

impl Drop for EtcdSession {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(keeper) = self.keeper.take() {
            let _ = keeper.join();
        }
        let _ = self.client.lease_revoke(self.lease);
    }
}

/// Go `etcdSyncer`.
pub struct EtcdSyncer {
    client: Arc<EtcdClient>,
    path: Vec<u8>,
    cluster_state: RwLock<StateInfo>,
    watch_sender: Sender<WatchEvent>,
    watch_receiver: WatchChannel,
    watcher: Arc<Mutex<Option<EtcdWatcher>>>,
    session: Mutex<Option<EtcdSession>>,
}

impl EtcdSyncer {
    /// Go `NewEtcdSyncer`.
    #[must_use]
    pub fn new(client: Arc<EtcdClient>, path: impl Into<Vec<u8>>) -> Self {
        let (watch_sender, watch_receiver) = mpsc::channel();
        Self {
            client,
            path: path.into(),
            cluster_state: RwLock::new(StateInfo::new(STATE_NORMAL_RUNNING)),
            watch_sender,
            watch_receiver: WatchChannel(Arc::new(Mutex::new(watch_receiver))),
            watcher: Arc::new(Mutex::new(None)),
            session: Mutex::new(None),
        }
    }

    fn start_watch(&self) -> Result<EtcdWatcher, EtcdError> {
        let path = self.path.clone();
        let event_key = path.clone();
        let sender = self.watch_sender.clone();
        self.client
            .watch_key(path, 0, move |event: &EtcdWatchEvent| {
                let _ = sender.send(WatchEvent {
                    key: event_key.clone(),
                    value: event.value.clone(),
                    deleted: event.deleted,
                    mod_revision: event.mod_revision,
                });
            })
    }

    fn get_with_retry(&self, context: &Context) -> Result<Option<Vec<u8>>, Error> {
        let mut last = None;
        for _ in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            let timeout = child_timeout(context, GET_KEY_TIMEOUT)?;
            match self.client.get_with_timeout(&self.path, timeout) {
                Ok(value) => return Ok(value),
                Err(error) => {
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
        for _ in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            let timeout = child_timeout(context, PUT_KEY_TIMEOUT)?;
            match self.client.put_with_timeout(&self.path, value, timeout) {
                Ok(()) => return Ok(()),
                Err(error) => {
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
        let session = EtcdSession::create(Arc::clone(&self.client), context)?;
        *self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session);
        self.get_global_state(context)?;
        if let Ok(watcher) = self.start_watch() {
            *self
                .watcher
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watcher);
        }
        Ok(())
    }

    fn update_global_state(&self, context: &Context, state: &StateInfo) -> Result<(), Error> {
        self.put_with_retry(context, &state.marshal()?)?;
        Ok(())
    }

    fn get_global_state(&self, context: &Context) -> Result<StateInfo, Error> {
        let mut state = StateInfo::default();
        if let Some(value) = self.get_with_retry(context)? {
            state.unmarshal(&value)?;
        }
        *self
            .cluster_state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state.clone();
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
            .map(|_| self.watch_receiver.clone())
    }

    fn rewatch(&self, context: &Context) {
        *self
            .watcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let _ = context;
        let client = Arc::clone(&self.client);
        let path = self.path.clone();
        let event_key = path.clone();
        let sender = self.watch_sender.clone();
        let watcher_slot = Arc::clone(&self.watcher);
        let _ = std::thread::Builder::new()
            .name("ddl-state-rewatch".to_owned())
            .spawn(move || {
                let watcher = client.watch_key(path, 0, move |event: &EtcdWatchEvent| {
                    let _ = sender.send(WatchEvent {
                        key: event_key.clone(),
                        value: event.value.clone(),
                        deleted: event.deleted,
                        mod_revision: event.mod_revision,
                    });
                });
                if let Ok(watcher) = watcher {
                    *watcher_slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watcher);
                }
            });
    }
}

fn memory_cluster_state() -> &'static RwLock<StateInfo> {
    static STATE: OnceLock<RwLock<StateInfo>> = OnceLock::new();
    STATE.get_or_init(|| RwLock::new(StateInfo::new(STATE_NORMAL_RUNNING)))
}

/// Go `memSyncer`.
pub struct MemSyncer {
    watch: Mutex<Option<(SyncSender<WatchEvent>, WatchChannel)>>,
}

impl MemSyncer {
    /// Go `NewMemSyncer` followed by `Init`'s channel construction.
    #[must_use]
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
        let _ = memory_cluster_state();
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
                .unwrap_or_else(std::sync::PoisonError::into_inner) = state.clone();
            return Ok(());
        }
        let sender = self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(sender, _)| sender.clone())
            .expect("memSyncer.UpdateGlobalState requires Init");
        let _ = sender.send(WatchEvent {
            key: Vec::new(),
            value: Vec::new(),
            deleted: false,
            mod_revision: 0,
        });
        *memory_cluster_state()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state.clone();
        Ok(())
    }

    fn get_global_state(&self, _context: &Context) -> Result<StateInfo, Error> {
        Ok(memory_cluster_state()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }

    fn is_upgrading_state(&self) -> bool {
        memory_cluster_state()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    fn memory_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: Mutex<()> = Mutex::new(());
        GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        assert!(decoded.unmarshal(br#"{"state":1}"#).is_err());
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
        let client =
            Arc::new(EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(20)).unwrap());
        let syncer = EtcdSyncer::new(client, SERVER_GLOBAL_STATE);
        let context = Context::background();

        let get_started = std::time::Instant::now();
        assert!(syncer.get_with_retry(&context).is_err());
        assert!(
            get_started.elapsed() >= GET_KEY_RETRY_INTERVAL * 3,
            "Go sleeps 200ms after every failed get, including the last"
        );

        let put_started = std::time::Instant::now();
        assert!(syncer.put_with_retry(&context, b"{}").is_err());
        assert!(
            put_started.elapsed() >= PUT_KEY_RETRY_INTERVAL * 3,
            "PutKVToEtcd sleeps 30ms after every failed put, including the last"
        );
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
        first
            .watch_chan()
            .unwrap()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(first.is_upgrading_state());

        let second = MemSyncer::new();
        second.init(&context).unwrap();
        assert_eq!(
            second.get_global_state(&context).unwrap(),
            StateInfo::new(STATE_UPGRADING)
        );
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
        let event = syncer
            .watch_chan()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("the state PUT reaches the watch");
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
