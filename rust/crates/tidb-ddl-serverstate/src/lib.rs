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

use serde::{Deserialize, Serialize};
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
const GET_KEY_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const PUT_KEY_RETRY_INTERVAL: Duration = Duration::from_millis(30);
const NEW_SESSION_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const SESSION_TTL_SECONDS: i64 = 90;

/// Go `StateInfo`'s exact JSON payload.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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
        serde_json::to_vec(self)
    }

    /// Go `StateInfo.Unmarshal`.
    pub fn unmarshal(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
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
        for attempt in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            if context.is_cancelled() {
                return Err(sleep(context, Duration::ZERO).unwrap_err().into());
            }
            match client.lease_grant(SESSION_TTL_SECONDS) {
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
                Err(error) => last = Some(error),
            }
            if attempt + 1 != KEY_OP_DEFAULT_RETRY_COUNT {
                sleep(context, NEW_SESSION_RETRY_INTERVAL)?;
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
    watcher: Mutex<Option<EtcdWatcher>>,
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
            watcher: Mutex::new(None),
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
        for attempt in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            if context.is_cancelled() {
                return Err(sleep(context, Duration::ZERO).unwrap_err().into());
            }
            match self.client.get(&self.path) {
                Ok(value) => return Ok(value),
                Err(error) => last = Some(error),
            }
            if attempt + 1 != KEY_OP_DEFAULT_RETRY_COUNT {
                sleep(context, GET_KEY_RETRY_INTERVAL)?;
            }
        }
        Err(last
            .expect("the retry loop performs at least one get")
            .into())
    }

    fn put_with_retry(&self, context: &Context, value: &[u8]) -> Result<(), Error> {
        let mut last = None;
        for attempt in 0..KEY_OP_DEFAULT_RETRY_COUNT {
            if context.is_cancelled() {
                return Err(sleep(context, Duration::ZERO).unwrap_err().into());
            }
            match self.client.put(&self.path, value) {
                Ok(()) => return Ok(()),
                Err(error) => last = Some(error),
            }
            if attempt + 1 != KEY_OP_DEFAULT_RETRY_COUNT {
                sleep(context, PUT_KEY_RETRY_INTERVAL)?;
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
        self.rewatch(context);
        Ok(())
    }

    fn update_global_state(&self, context: &Context, state: &StateInfo) -> Result<(), Error> {
        self.put_with_retry(context, &state.marshal()?)?;
        Ok(())
    }

    fn get_global_state(&self, context: &Context) -> Result<StateInfo, Error> {
        let state = self
            .get_with_retry(context)?
            .map(|value| StateInfo::unmarshal(&value))
            .transpose()?
            .unwrap_or_default();
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

    fn rewatch(&self, _context: &Context) {
        *self
            .watcher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        if let Ok(watcher) = self.start_watch() {
            *self
                .watcher
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watcher);
        }
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

    #[test]
    fn state_info_uses_go_json() {
        let state = StateInfo::new(STATE_UPGRADING);
        assert_eq!(state.marshal().unwrap(), br#"{"state":"upgrading"}"#);
        assert_eq!(
            StateInfo::unmarshal(&state.marshal().unwrap()).unwrap(),
            state
        );
        assert_eq!(StateInfo::default().marshal().unwrap(), br#"{"state":""}"#);
    }

    #[test]
    fn memory_syncer_keeps_process_global_state_and_delivers_update_notification() {
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
                .map(StateInfo::unmarshal)
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
