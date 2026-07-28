// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The etcd v3 KV/Watch surface PD serves on its own client port.
//!
//! TiDB reaches etcd through exactly these addresses: `pkg/store/etcd.go`
//! `NewEtcdCli` builds its `clientv3` from `store.EtcdAddrs()`, which are the
//! PD endpoints this crate already dials for `pdpb.PD`. PD embeds a real etcd
//! server, so `etcdserverpb.KV` and `etcdserverpb.Watch` answer on the same
//! channel — no second port, no second discovery.
//!
//! Two shapes live here because the two uses have opposite lifetimes:
//!
//! * [`EtcdClient`] is the bounded foreground client. Like [`crate::PdClient`]
//!   it owns one worker thread with one current-thread Tokio runtime, so the
//!   synchronous callers (a DDL commit path) never nest a runtime inside one
//!   they already own. Each call tries the configured endpoints in order and
//!   drops a channel that failed, so a restarted PD is picked up on the next
//!   call rather than poisoning the client.
//! * [`EtcdWatcher`] is a long-lived stream. It gets its own thread and
//!   runtime rather than sharing the foreground worker, because a bidi stream
//!   that blocks would otherwise stall every unary call queued behind it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_proto::etcdserverpb::kv_client::KvClient;
use tidb_proto::etcdserverpb::watch_client::WatchClient;
use tidb_proto::etcdserverpb::{
    watch_request::RequestUnion, PutRequest, RangeRequest, WatchCreateRequest, WatchRequest,
};
use tidb_proto::mvccpb::event::EventType;
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

use crate::client::normalize_endpoints;
use crate::PdClientError;

/// Exact generated method path for the schema-version PUT.
pub const ETCD_PUT_PATH: &str = "/etcdserverpb.KV/Put";
/// Exact generated method path for reading one key back.
pub const ETCD_RANGE_PATH: &str = "/etcdserverpb.KV/Range";
/// Exact generated method path for the watch stream.
pub const ETCD_WATCH_PATH: &str = "/etcdserverpb.Watch/Watch";

/// The etcd key TiDB publishes the cluster's schema version under.
///
/// Source of truth: `pkg/ddl/util/util.go` `DDLGlobalSchemaVersion`.
pub const DDL_GLOBAL_SCHEMA_VERSION_KEY: &str = "/tidb/ddl/global_schema_version";

/// The etcd key TiDB notifies privilege changes on.
///
/// Source of truth: `pkg/domain/domain.go` `privilegeKey`, watched by
/// `Domain.LoadPrivilegeLoop` and written by `Domain.notifyUpdatePrivilege`.
/// Unlike the schema-version key this one carries no state -- the value is a
/// `PrivilegeEvent` message and every reader reloads from `mysql.*` itself --
/// so a node that misses an event loses nothing but time.
pub const PRIVILEGE_UPDATE_KEY: &str = "/tidb/privilege";

/// The `PrivilegeEvent` body that asks every reader to reload every account.
///
/// Go's `PrivilegeEvent` is `{All bool, ServerID uint64, UserList []string}`
/// (`pkg/domain/domain.go`). A reader skips an event whose `ServerID` equals
/// its own, so `0` -- the ID no running TiDB reports, and a value its check
/// ignores anyway -- is what keeps a real TiDB from mistaking this node's
/// announcement for its own. `All` rather than a `UserList` because every
/// reader of this key reloads its whole account table regardless, and a
/// partial list that missed a row would be a silently stale grant.
const PRIVILEGE_UPDATE_ALL_EVENT: &str = r#"{"All":true,"ServerID":0,"UserList":null}"#;

/// Why an etcd call or watch could not be completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EtcdError {
    /// A configured endpoint is not a plaintext URI this client can dial.
    InvalidEndpoint {
        /// The endpoint as configured.
        endpoint: String,
        /// Why it was refused.
        message: String,
    },
    /// No endpoint was configured at all.
    NoEndpoint,
    /// The dedicated runtime or thread could not be created.
    Runtime(String),
    /// The worker is gone; the client was shut down.
    Closed,
    /// Every configured endpoint failed, with the last failure retained.
    Unreachable {
        /// The endpoint whose failure is reported.
        endpoint: String,
        /// The gRPC status code identity, or `timeout`.
        code: String,
        /// The failure detail.
        message: String,
    },
    /// etcd answered, but not with the shape this key's contract requires.
    UnexpectedResponse(String),
}

impl std::fmt::Display for EtcdError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint { endpoint, message } => {
                write!(formatter, "invalid etcd endpoint {endpoint}: {message}")
            }
            Self::NoEndpoint => formatter.write_str("no etcd endpoint was configured"),
            Self::Runtime(message) => write!(formatter, "etcd client runtime failed: {message}"),
            Self::Closed => formatter.write_str("the etcd client is closed"),
            Self::Unreachable {
                endpoint,
                code,
                message,
            } => write!(formatter, "etcd {endpoint} unreachable ({code}): {message}"),
            Self::UnexpectedResponse(message) => {
                write!(formatter, "unexpected etcd response: {message}")
            }
        }
    }
}

impl std::error::Error for EtcdError {}

impl From<PdClientError> for EtcdError {
    fn from(error: PdClientError) -> Self {
        match error {
            PdClientError::InvalidEndpoint { endpoint, message } => {
                Self::InvalidEndpoint { endpoint, message }
            }
            other => Self::Runtime(other.to_string()),
        }
    }
}

enum EtcdCommand {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        reply: mpsc::Sender<Result<(), EtcdError>>,
    },
    Get {
        key: Vec<u8>,
        reply: mpsc::Sender<Result<Option<Vec<u8>>, EtcdError>>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

struct EtcdClientShared {
    endpoints: Vec<String>,
    timeout: Duration,
    commands: mpsc::Sender<EtcdCommand>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// A bounded synchronous etcd KV client over the PD endpoints.
///
/// Cloning shares the worker; the last handle dropped stops it. Unlike
/// [`crate::PdClient`] there is no owner/handle distinction, because nothing
/// here holds a stream whose ownership has to be resolved at shutdown.
#[derive(Clone)]
pub struct EtcdClient {
    shared: Arc<EtcdClientShared>,
}

impl std::fmt::Debug for EtcdClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdClient")
            .field("endpoints", &self.shared.endpoints)
            .field("timeout", &self.shared.timeout)
            .finish_non_exhaustive()
    }
}

impl EtcdClient {
    /// Starts the worker over the PD endpoints, in the caller's order.
    ///
    /// Connecting is lazy: this does not prove etcd is reachable, because the
    /// notification path must not make a node's startup depend on a surface
    /// whose failure it deliberately tolerates.
    pub fn connect<I, S>(endpoints: I, timeout: Duration) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints = normalize_endpoints(endpoints, false)?;
        if endpoints.is_empty() {
            return Err(EtcdError::NoEndpoint);
        }
        let (commands, receiver) = mpsc::channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker_endpoints = endpoints.clone();
        let worker = std::thread::Builder::new()
            .name("etcd-kv".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    // Every queued and future command answers `Closed` when the
                    // receiver drops with the thread, which is the same
                    // observable outcome as a shut-down worker.
                    return;
                };
                run_kv_worker(
                    &runtime,
                    &worker_endpoints,
                    timeout,
                    &receiver,
                    &shutdown_rx,
                );
            })
            .map_err(|error| EtcdError::Runtime(error.to_string()))?;
        Ok(Self {
            shared: Arc::new(EtcdClientShared {
                endpoints,
                timeout,
                commands,
                shutdown,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    /// The endpoints this client dials, normalized.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.shared.endpoints
    }

    /// Puts one key with no lease attached, exactly as
    /// `OwnerUpdateGlobalVersion` does.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Reads one key. `None` means the key is absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EtcdError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(EtcdCommand::Get {
                key: key.to_vec(),
                reply,
            })
            .map_err(|_| EtcdError::Closed)?;
        response.recv().unwrap_or(Err(EtcdError::Closed))
    }

    /// Publishes a schema version under the key TiDB's own owner writes.
    ///
    /// The value is the decimal ASCII text of the `int64`
    /// (`strconv.FormatInt(version, 10)` in `OwnerUpdateGlobalVersion`), and
    /// the PUT carries no lease, so the key outlives the writer's session.
    pub fn put_global_schema_version(&self, version: i64) -> Result<(), EtcdError> {
        self.put(
            DDL_GLOBAL_SCHEMA_VERSION_KEY.as_bytes(),
            version.to_string().as_bytes(),
        )
    }

    /// Announces that this node changed the cluster's accounts, so every
    /// peer's privilege watch reloads instead of waiting out its own tick.
    pub fn notify_privilege_update(&self) -> Result<(), EtcdError> {
        self.put(
            PRIVILEGE_UPDATE_KEY.as_bytes(),
            PRIVILEGE_UPDATE_ALL_EVENT.as_bytes(),
        )
    }

    /// Reads back whatever schema version the cluster last published.
    pub fn global_schema_version(&self) -> Result<Option<i64>, EtcdError> {
        let Some(value) = self.get(DDL_GLOBAL_SCHEMA_VERSION_KEY.as_bytes())? else {
            return Ok(None);
        };
        parse_global_schema_version(&value).map(Some)
    }
}

impl Drop for EtcdClient {
    fn drop(&mut self) {
        // Only the last handle stops the worker: an in-flight call on another
        // clone must not lose its runtime under it.
        if Arc::strong_count(&self.shared) > 1 {
            return;
        }
        let _ = self.shared.shutdown.send(true);
        let (reply, response) = mpsc::channel();
        if self
            .shared
            .commands
            .send(EtcdCommand::Close { reply })
            .is_ok()
        {
            let _ = response.recv();
        }
        let worker = match self.shared.worker.lock() {
            Ok(mut worker) => worker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

/// The decimal ASCII int64 contract of the global schema version value.
fn parse_global_schema_version(value: &[u8]) -> Result<i64, EtcdError> {
    let text = std::str::from_utf8(value)
        .map_err(|_| EtcdError::UnexpectedResponse(format!("non-UTF-8 version value {value:?}")))?;
    text.trim().parse::<i64>().map_err(|error| {
        EtcdError::UnexpectedResponse(format!("version value {text:?} is not an int64: {error}"))
    })
}

fn run_kv_worker(
    runtime: &tokio::runtime::Runtime,
    endpoints: &[String],
    timeout: Duration,
    receiver: &mpsc::Receiver<EtcdCommand>,
    shutdown: &watch::Receiver<bool>,
) {
    let mut clients: HashMap<String, KvClient<Channel>> = HashMap::new();
    while let Ok(command) = receiver.recv() {
        match command {
            EtcdCommand::Close { reply } => {
                let _ = reply.send(());
                return;
            }
            _ if *shutdown.borrow() => match command {
                EtcdCommand::Put { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::Get { reply, .. } => {
                    let _ = reply.send(Err(EtcdError::Closed));
                }
                EtcdCommand::Close { .. } => unreachable!("handled above"),
            },
            EtcdCommand::Put { key, value, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    |runtime, client| {
                        let request = PutRequest {
                            key: key.clone(),
                            value: value.clone(),
                            ..Default::default()
                        };
                        runtime
                            .block_on(client.put(with_deadline(request, timeout)))
                            .map(|_| ())
                    },
                );
                let _ = reply.send(result);
            }
            EtcdCommand::Get { key, reply } => {
                let result = across_endpoints(
                    runtime,
                    endpoints,
                    &mut clients,
                    timeout,
                    |runtime, client| {
                        let request = RangeRequest {
                            key: key.clone(),
                            limit: 1,
                            ..Default::default()
                        };
                        runtime
                            .block_on(client.range(with_deadline(request, timeout)))
                            .map(|response| {
                                response
                                    .into_inner()
                                    .kvs
                                    .into_iter()
                                    .next()
                                    .map(|kv| kv.value)
                            })
                    },
                );
                let _ = reply.send(result);
            }
        }
    }
}

fn with_deadline<T>(message: T, timeout: Duration) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(timeout);
    request
}

/// Runs one call against the first endpoint that answers.
///
/// A channel that failed is dropped rather than reused: etcd inside a
/// restarted PD gets a fresh connection on the next call instead of a
/// permanently broken one.
fn across_endpoints<T>(
    runtime: &tokio::runtime::Runtime,
    endpoints: &[String],
    clients: &mut HashMap<String, KvClient<Channel>>,
    timeout: Duration,
    mut call: impl FnMut(&tokio::runtime::Runtime, &mut KvClient<Channel>) -> Result<T, tonic::Status>,
) -> Result<T, EtcdError> {
    let mut last = None;
    for endpoint in endpoints {
        if !clients.contains_key(endpoint) {
            match connect_channel(runtime, endpoint, timeout) {
                Ok(channel) => {
                    clients.insert(endpoint.clone(), KvClient::new(channel));
                }
                Err(error) => {
                    last = Some(error);
                    continue;
                }
            }
        }
        let client = clients
            .get_mut(endpoint)
            .expect("the channel was just inserted");
        match call(runtime, client) {
            Ok(value) => return Ok(value),
            Err(status) => {
                clients.remove(endpoint);
                last = Some(EtcdError::Unreachable {
                    endpoint: endpoint.clone(),
                    code: format!("{:?}", status.code()),
                    message: status.message().to_owned(),
                });
            }
        }
    }
    Err(last.unwrap_or(EtcdError::NoEndpoint))
}

fn connect_channel(
    runtime: &tokio::runtime::Runtime,
    endpoint: &str,
    timeout: Duration,
) -> Result<Channel, EtcdError> {
    let channel = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| EtcdError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: error.to_string(),
        })?
        .connect_timeout(timeout)
        .timeout(timeout);
    runtime
        .block_on(channel.connect())
        .map_err(|error| EtcdError::Unreachable {
            endpoint: endpoint.to_owned(),
            code: "connect".to_owned(),
            message: error.to_string(),
        })
}

/// What one watched key change was.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EtcdWatchEvent {
    /// Whether the key was written or deleted.
    pub deleted: bool,
    /// The value written, empty for a delete.
    pub value: Vec<u8>,
    /// The store revision the change was applied at.
    pub mod_revision: i64,
}

/// What the watch thread has observed, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EtcdWatchStats {
    /// Watch streams successfully created, including reconnections.
    pub streams: u64,
    /// Key changes delivered to the callback.
    pub events: u64,
    /// Streams that ended or failed and had to be re-established.
    pub reconnects: u64,
}

#[derive(Debug, Default)]
struct WatchCounters {
    streams: AtomicU64,
    events: AtomicU64,
    reconnects: AtomicU64,
}

impl WatchCounters {
    fn snapshot(&self) -> EtcdWatchStats {
        EtcdWatchStats {
            streams: self.streams.load(Ordering::Acquire),
            events: self.events.load(Ordering::Acquire),
            reconnects: self.reconnects.load(Ordering::Acquire),
        }
    }
}

/// A single-key etcd watch, running until dropped.
///
/// Go's `Syncer.SyncLoop` treats a closed watch channel as "need rewatch" and
/// re-establishes it while the `lease/2` ticker keeps reloading meanwhile
/// (`pkg/infoschema/issyncer/syncer.go`). The same division holds here: this
/// thread reconnects on its own, and the caller's tick is what guarantees
/// progress while it is disconnected.
#[derive(Debug)]
pub struct EtcdWatcher {
    shutdown: watch::Sender<bool>,
    stats: Arc<WatchCounters>,
    worker: Option<JoinHandle<()>>,
}

impl EtcdWatcher {
    /// Starts watching one key, calling `on_event` for every change.
    ///
    /// The callback runs on the watch thread and must not block for long; the
    /// intended use is nudging a reload thread, not reloading inline.
    pub fn spawn<I, S>(
        endpoints: I,
        timeout: Duration,
        key: impl Into<Vec<u8>>,
        on_event: impl Fn(&EtcdWatchEvent) + Send + 'static,
    ) -> Result<Self, EtcdError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let endpoints = normalize_endpoints(endpoints, false)?;
        if endpoints.is_empty() {
            return Err(EtcdError::NoEndpoint);
        }
        let key = key.into();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let stats = Arc::new(WatchCounters::default());
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("etcd-watch".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(watch_forever(
                    &endpoints,
                    timeout,
                    &key,
                    &on_event,
                    &worker_stats,
                    shutdown_rx,
                ));
            })
            .map_err(|error| EtcdError::Runtime(error.to_string()))?;
        Ok(Self {
            shutdown,
            stats,
            worker: Some(worker),
        })
    }

    /// What the watch thread has observed so far.
    #[must_use]
    pub fn stats(&self) -> EtcdWatchStats {
        self.stats.snapshot()
    }

    /// Stops the watch thread and waits for it. Idempotent; [`Drop`] calls it.
    pub fn shutdown(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for EtcdWatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The reconnect delay: short enough that a PD restart is invisible next to
/// the reload tick that backs this path up, long enough not to spin.
const WATCH_RECONNECT_DELAY: Duration = Duration::from_secs(1);

async fn watch_forever(
    endpoints: &[String],
    timeout: Duration,
    key: &[u8],
    on_event: &(impl Fn(&EtcdWatchEvent) + Send + 'static),
    stats: &WatchCounters,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut established = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        for endpoint in endpoints {
            if *shutdown.borrow() {
                return;
            }
            if established {
                stats.reconnects.fetch_add(1, Ordering::AcqRel);
                established = false;
            }
            if watch_one_stream(endpoint, timeout, key, on_event, stats, &mut shutdown).await {
                established = true;
                break;
            }
        }
        // Either the stream ended or no endpoint accepted one. Waiting before
        // the next attempt is what keeps a down PD from becoming a spin loop;
        // the caller's lease tick is still reloading throughout.
        tokio::select! {
            () = tokio::time::sleep(WATCH_RECONNECT_DELAY) => {}
            _ = shutdown.changed() => return,
        }
    }
}

/// Runs one watch stream to its end. Returns whether the stream was created.
async fn watch_one_stream(
    endpoint: &str,
    timeout: Duration,
    key: &[u8],
    on_event: &(impl Fn(&EtcdWatchEvent) + Send + 'static),
    stats: &WatchCounters,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let Ok(channel) = Endpoint::from_shared(endpoint.to_owned()) else {
        return false;
    };
    let Ok(channel) = channel.connect_timeout(timeout).connect().await else {
        return false;
    };
    let mut client = WatchClient::new(channel);
    // The request sender is held for the life of the stream: dropping it would
    // half-close the bidi call and end the watch.
    let (requests, request_rx) = tokio::sync::mpsc::channel(1);
    if requests
        .send(WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                key: key.to_vec(),
                ..Default::default()
            })),
        })
        .await
        .is_err()
    {
        return false;
    }
    let Ok(response) = client
        .watch(tokio_stream::wrappers::ReceiverStream::new(request_rx))
        .await
    else {
        return false;
    };
    let mut stream = response.into_inner();
    stats.streams.fetch_add(1, Ordering::AcqRel);
    loop {
        let message = tokio::select! {
            message = stream.message() => message,
            _ = shutdown.changed() => return true,
        };
        let Ok(Some(response)) = message else {
            return true;
        };
        if response.canceled {
            return true;
        }
        for event in response.events {
            let deleted = event.r#type == EventType::Delete as i32;
            let (value, mod_revision) = event
                .kv
                .map_or_else(|| (Vec::new(), 0), |kv| (kv.value, kv.mod_revision));
            stats.events.fetch_add(1, Ordering::AcqRel);
            on_event(&EtcdWatchEvent {
                deleted,
                value,
                mod_revision,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_global_schema_version_value_is_decimal_ascii() {
        // `OwnerUpdateGlobalVersion` writes `strconv.FormatInt(version, 10)`:
        // text, not a fixed-width or varint encoding.
        assert_eq!(parse_global_schema_version(b"127").unwrap(), 127);
        assert_eq!(parse_global_schema_version(b"-1").unwrap(), -1);
        assert_eq!(
            parse_global_schema_version(i64::MAX.to_string().as_bytes()).unwrap(),
            i64::MAX
        );
        assert!(matches!(
            parse_global_schema_version(&[0, 0, 0, 7]),
            Err(EtcdError::UnexpectedResponse(_))
        ));
        assert!(matches!(
            parse_global_schema_version(b"1.5"),
            Err(EtcdError::UnexpectedResponse(_))
        ));
    }

    #[test]
    fn the_watched_key_is_the_one_the_ddl_owner_writes() {
        // A typo here would make the watch silently never fire, which is
        // exactly the failure the lease tick would hide.
        assert_eq!(
            DDL_GLOBAL_SCHEMA_VERSION_KEY,
            "/tidb/ddl/global_schema_version"
        );
    }

    #[test]
    fn a_client_without_endpoints_is_refused_rather_than_started() {
        assert_eq!(
            EtcdClient::connect(Vec::<String>::new(), Duration::from_secs(1)).unwrap_err(),
            EtcdError::NoEndpoint
        );
        assert_eq!(
            EtcdWatcher::spawn(Vec::<String>::new(), Duration::from_secs(1), "/k", |_| {})
                .unwrap_err(),
            EtcdError::NoEndpoint
        );
    }

    #[test]
    fn endpoints_are_normalized_to_the_plaintext_form_pd_is_dialed_with() {
        let client = EtcdClient::connect(["127.0.0.1:2379"], Duration::from_millis(50)).unwrap();
        assert_eq!(client.endpoints(), ["http://127.0.0.1:2379".to_owned()]);
    }

    #[test]
    fn a_put_to_an_unreachable_endpoint_fails_without_hanging() {
        // Port 1 has no listener; the call must come back as Unreachable
        // rather than block the DDL path that is best-effort calling it.
        let client = EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(200)).unwrap();
        assert!(matches!(
            client.put_global_schema_version(9),
            Err(EtcdError::Unreachable { .. })
        ));
    }

    #[test]
    fn a_watcher_on_an_unreachable_endpoint_stops_promptly() {
        let mut watcher = EtcdWatcher::spawn(
            ["127.0.0.1:1"],
            Duration::from_millis(100),
            DDL_GLOBAL_SCHEMA_VERSION_KEY,
            |_| unreachable!("no event can arrive from a closed port"),
        )
        .unwrap();
        let stopping = std::time::Instant::now();
        watcher.shutdown();
        assert!(stopping.elapsed() < Duration::from_secs(5));
        assert_eq!(watcher.stats().events, 0);
    }

    /// End-to-end against a real PD, opt in with
    /// `TIDB_ETCD_PROBE_PD=127.0.0.1:2379 cargo test -p tidb-pd-client -- --ignored`.
    ///
    /// Nothing else in this file can prove the watch stream actually works:
    /// PD's embedded etcd is the only implementation of the contract, and a
    /// projection that compiles is not a projection that is understood.
    #[test]
    #[ignore = "requires a live PD; set TIDB_ETCD_PROBE_PD"]
    fn a_put_wakes_a_watch_on_a_real_pd() {
        let Ok(endpoint) = std::env::var("TIDB_ETCD_PROBE_PD") else {
            panic!("set TIDB_ETCD_PROBE_PD to a PD client address");
        };
        let timeout = Duration::from_secs(5);
        let key = "/tidb/ddl/global_schema_version";
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut watcher = EtcdWatcher::spawn([endpoint.as_str()], timeout, key, move |event| {
            let _ = sender.send(event.clone());
        })
        .unwrap();
        // The stream is created asynchronously; a PUT that races its creation
        // would be missed by etcd itself, not by this code.
        std::thread::sleep(Duration::from_millis(500));

        let client = EtcdClient::connect([endpoint.as_str()], timeout).unwrap();
        client.put_global_schema_version(4242).unwrap();
        let event = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("the watch must deliver the PUT");
        assert_eq!(event.value, b"4242");
        assert!(!event.deleted);
        assert_eq!(client.global_schema_version().unwrap(), Some(4242));
        assert!(watcher.stats().streams >= 1);
        watcher.shutdown();
    }

    #[test]
    fn a_cloned_client_keeps_the_worker_alive_until_the_last_handle_drops() {
        let client = EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(100)).unwrap();
        let clone = client.clone();
        drop(client);
        // The worker is still there: this answers with a transport failure,
        // not `Closed`.
        assert!(matches!(
            clone.get(b"/k"),
            Err(EtcdError::Unreachable { .. })
        ));
    }
}
