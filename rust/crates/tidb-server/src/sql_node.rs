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

//! Bounded concurrent listener and worker-local query-session ownership.

use tidb_domain_core::connection_ids::new_connection_id_allocator;
use tidb_worker_stack::SQL_WORKER_STACK_BYTES;
use std::fmt;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_globalconn::Allocator;

use crate::configured_user_store::ConfiguredUserStore;
use crate::client_socket::ClientSocket;
use tidb_session::domain::peer_address::PeerAddress;
use crate::mysql_connection::{
    serve_mysql_connection_with_runtime, MysqlConnectionError, MysqlConnectionRuntime,
};
use crate::mysql_tls::{MysqlTlsManager, TlsRotation};
use crate::node_config::{NodeConfig, MAX_CONNECTION_WORKERS};
pub use crate::session_api::*;
use crate::driver::QuerySessionFactory;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
/// Wakes the accept loop out of its blocking `accept` by connecting to the
/// listener once: Go closes its listener so `Accept` returns, and this is the
/// same wake for a listener the loop keeps owning.
#[derive(Clone, Debug, Default)]
struct AcceptWaker {
    address: Arc<std::sync::Mutex<Option<SocketAddr>>>,
}

impl AcceptWaker {
    fn bind_to(&self, listener: &TcpListener) {
        let Ok(mut address) = listener.local_addr() else {
            return;
        };
        if address.ip().is_unspecified() {
            address.set_ip(match address {
                SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
                SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
            });
        }
        *self
            .address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(address);
    }

    fn wake(&self) {
        let address = *self
            .address
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(address) = address {
            let _ = TcpStream::connect_timeout(&address, Duration::from_millis(500));
        }
    }
}

/// Cloneable process shutdown signal for the sole production accept loop.
#[derive(Clone, Debug, Default)]
pub struct ShutdownHandle {
    requested: Arc<AtomicBool>,
    waker: AcceptWaker,
}

impl ShutdownHandle {
    /// Requests shutdown. Repeated requests are idempotent.
    pub fn shutdown(&self) {
        self.requested.store(true, Ordering::Release);
        self.waker.wake();
    }

    /// Whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// Shared per-server connection accounting and command admission. Every socket
/// for this server retains this authority, including dedicated worker threads.
pub struct ConnectionTracker {
    pub(crate) command_limiter: crate::mysql_connection::CommandLimiter,
    connection_ids: Arc<dyn Allocator + Send + Sync>,
    identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}


impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::with_token_limit(tidb_config::config_tree::Config::default().token_limit)
    }
}

impl ConnectionTracker {
    fn with_token_limit(limit: usize) -> Self {
        let config = tidb_config::config_tree::Config {
            token_limit: limit,
            ..Default::default()
        };
        Self::with_config(&config, None, None)
    }

    /// `connection_ids` is the node's shared allocator when its session
    /// factory owns one (Go's `Domain.connIDAllocator`, which `newClientConn`
    /// reads through `dom.NextConnID`); otherwise the tracker builds the same
    /// allocator for itself.
    fn with_config(
        config: &tidb_config::config_tree::Config,
        identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
        connection_ids: Option<Arc<dyn Allocator + Send + Sync>>,
    ) -> Self {
        let identity = identity.filter(|_| config.enable_global_kill);
        let connection_ids =
            connection_ids.unwrap_or_else(|| new_connection_id_allocator(config, identity.clone()));
        Self {
            command_limiter: crate::mysql_connection::CommandLimiter::new(config.token_limit),
            connection_ids,
            identity,
            active: AtomicUsize::default(),
            max_active: AtomicUsize::default(),
            accepted: AtomicU64::default(),
            completed: AtomicU64::default(),
            failed: AtomicU64::default(),
        }
    }

    pub(crate) fn identity_lost(&self) -> bool {
        self.identity
            .as_ref()
            .is_some_and(|identity| identity.is_lost())
    }
}

impl fmt::Debug for ConnectionTracker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionTracker")
            .field("active", &self.active())
            .field("max_active", &self.max_active())
            .field("accepted", &self.accepted())
            .field("completed", &self.completed())
            .field("failed", &self.failed())
            .finish()
    }
}

impl ConnectionTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> ConnectionLease {
        let id = self.connection_ids.next_id();
        self.accepted.fetch_add(1, Ordering::AcqRel);
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        ConnectionLease {
            tracker: Arc::clone(self),
            id,
            failed: false,
        }
    }

    /// Number of accepted connections currently inside their lifecycle.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Maximum simultaneously active connection count observed.
    #[must_use]
    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Acquire)
    }

    /// Total accepted connections.
    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Acquire)
    }

    /// Total lifecycles released exactly once.
    #[must_use]
    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Total lifecycles that ended on a connection error.
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Acquire)
    }
}

pub(crate) struct ConnectionLease {
    tracker: Arc<ConnectionTracker>,
    id: u64,
    failed: bool,
}

impl ConnectionLease {
    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) const fn mark_failed(&mut self) {
        self.failed = true;
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        // `active` falls last: a drain that waits for it to reach zero must
        // find this connection's outcome and released id already recorded.
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        if self.failed {
            self.tracker.failed.fetch_add(1, Ordering::AcqRel);
        }
        self.tracker.connection_ids.release(self.id);
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "connection count underflow");
    }
}

struct IdentitySocketRegistration(Arc<tidb_domain::server_id::ServerIdAuthority>);
impl Drop for IdentitySocketRegistration {
    fn drop(&mut self) {
        self.0.set_connection_killer(None);
    }
}

struct ConnectionWork {
    stream: ClientSocket,
    peer_addr: PeerAddress,
    cancellation: ConnectionCancellation,
    registration: ActiveSocketRegistration,
}

struct WorkerPool {
    workers: Vec<WorkerHandle>,
    work_senders: Vec<mpsc::Sender<ConnectionWork>>,
    available_workers: mpsc::Receiver<usize>,
    available_sender: mpsc::SyncSender<usize>,
    /// A live clone so the accept loop can hand dedicated (per-connection)
    /// threads the same terminal guard the warm pool uses to report panics.
    terminal_sender: TerminalSender,
    terminal_workers: mpsc::Receiver<WorkerTerminal>,
}

#[derive(Clone)]
struct WorkerConnectionConfig {
    max_allowed_packet: usize,
    tls: Option<Arc<MysqlTlsManager>>,
}

struct WorkerHandle {
    index: usize,
    join: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerTerminalKind {
    Returned,
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerTerminal {
    index: usize,
    kind: WorkerTerminalKind,
}

/// The terminal channel's sender: a PANICKED serving thread also wakes the
/// accept loop, so admission stops at once rather than at the next client.
/// A thread that merely returned needs no wake; the loop reaps it on its next
/// pass.
#[derive(Clone)]
struct TerminalSender {
    sender: mpsc::Sender<WorkerTerminal>,
    waker: AcceptWaker,
}

impl TerminalSender {
    fn send(&self, terminal: WorkerTerminal) -> Result<(), mpsc::SendError<WorkerTerminal>> {
        let sent = self.sender.send(terminal);
        if terminal.kind == WorkerTerminalKind::Panicked {
            self.waker.wake();
        }
        sent
    }
}

impl From<mpsc::Sender<WorkerTerminal>> for TerminalSender {
    fn from(sender: mpsc::Sender<WorkerTerminal>) -> Self {
        Self {
            sender,
            waker: AcceptWaker::default(),
        }
    }
}

struct WorkerTerminalGuard {
    index: usize,
    terminal: TerminalSender,
}

impl Drop for WorkerTerminalGuard {
    fn drop(&mut self) {
        let kind = if std::thread::panicking() {
            WorkerTerminalKind::Panicked
        } else {
            WorkerTerminalKind::Returned
        };
        let _ = self.terminal.send(WorkerTerminal {
            index: self.index,
            kind,
        });
    }
}

/// Surfaces a PANICKED serving thread to the accept loop so admission stops,
/// draining everything else. A dedicated thread reporting `Returned` is just
/// its one client disconnecting -- exactly how Go's per-connection goroutines
/// end, thousands of times over a server's life -- so only an actual panic
/// stops admission here.
fn poll_terminal(terminal: &mpsc::Receiver<WorkerTerminal>) -> Result<(), SqlNodeError> {
    while let Ok(worker) = terminal.try_recv() {
        if worker.kind == WorkerTerminalKind::Panicked {
            return Err(SqlNodeError::WorkerTerminated {
                index: worker.index,
                panicked: true,
            });
        }
    }
    Ok(())
}

/// Go's `ErrConCount` reply: a client arriving when every worker is busy is
/// told `1040 Too many connections` and closed, rather than left waiting in
/// the listen backlog.
///
/// `server.go` returns `servererr.ErrConCount` from its capacity check and
/// `onConn` answers it with `conn.writeError(ctx, err)`, so the packet goes
/// out before any handshake -- which is why the sequence number is 0 and the
/// packet carries NO SQLSTATE marker. Go writes it through the same
/// `writeError`, whose 4.1 form is conditioned on
/// `cc.capability&mysql.ClientProtocol41`, and at this point the client has
/// sent no capability flags at all, so that bit is clear. Sending the marker
/// anyway is not cosmetic: the client parses the packet in the old form and
/// the marker lands INSIDE the message, which reads
/// `ERROR 1040 (HY000): #08004Too many connections`.
///
/// Failures are ignored on purpose: the connection is being refused, and a
/// peer that has already gone away is the ordinary case.
fn refuse_over_capacity(stream: &ClientSocket) {
    let Ok(socket) = stream.try_clone() else {
        return;
    };
    let Ok(mut output) =
        tidb_protocol::PacketIoWriter::new(socket, tidb_protocol::CompressionAlgorithm::None)
    else {
        return;
    };
    let _ = crate::connection_writers::write_error(
        &mut output,
        0,
        tidb_mysql::errors::errcode::ErrConCount,
        *b"08004",
        "Too many connections",
        false,
    );
    let _ = stream.shutdown(Shutdown::Both);
}

#[derive(Default)]
struct ActiveSockets {
    streams: Mutex<Vec<ActiveSocket>>,
    poisoned: AtomicBool,
}

struct ActiveSocket {
    key: u64,
    stream: ClientSocket,
    cancellation: ConnectionCancellation,
}

struct ActiveSocketRegistration {
    key: u64,
    sockets: Arc<ActiveSockets>,
}

impl ActiveSocketRegistration {
    fn new(key: u64, sockets: Arc<ActiveSockets>) -> Self {
        Self { key, sockets }
    }
}

impl Drop for ActiveSocketRegistration {
    fn drop(&mut self) {
        self.sockets.remove(self.key);
    }
}

impl ActiveSockets {
    fn lock_streams(&self) -> (std::sync::MutexGuard<'_, Vec<ActiveSocket>>, bool) {
        match self.streams.lock() {
            Ok(streams) => (streams, false),
            Err(poisoned) => {
                self.poisoned.store(true, Ordering::Release);
                self.streams.clear_poison();
                (poisoned.into_inner(), true)
            }
        }
    }

    fn take_poisoned(&self, observed: bool) -> bool {
        let recorded = self.poisoned.swap(false, Ordering::AcqRel);
        observed || recorded
    }

    fn register(
        &self,
        key: u64,
        stream: ClientSocket,
        cancellation: ConnectionCancellation,
    ) -> Result<(), SqlNodeError> {
        self.streams
            .lock()
            .map_err(|_| SqlNodeError::WorkerStatePoisoned)?
            .push(ActiveSocket {
                key,
                stream,
                cancellation,
            });
        Ok(())
    }

    fn remove(&self, key: u64) {
        let (mut streams, _) = self.lock_streams();
        streams.retain(|socket| socket.key != key);
    }

    fn len(&self) -> Result<usize, SqlNodeError> {
        let (streams, observed) = self.lock_streams();
        let len = streams.len();
        drop(streams);
        if self.take_poisoned(observed) {
            Err(SqlNodeError::WorkerStatePoisoned)
        } else {
            Ok(len)
        }
    }

    fn shutdown_all(&self) -> (usize, Option<SqlNodeError>) {
        let (streams, observed) = self.lock_streams();
        let forced = streams.len();
        for socket in streams.iter() {
            socket.cancellation.cancel();
            let _ = socket.stream.shutdown(Shutdown::Both);
        }
        drop(streams);
        let error = if self.take_poisoned(observed) {
            Some(SqlNodeError::WorkerStatePoisoned)
        } else {
            None
        };
        (forced, error)
    }

    fn cancel_queries(&self) -> Result<(), SqlNodeError> {
        let (streams, observed) = self.lock_streams();
        for socket in streams.iter() {
            socket.cancellation.cancel();
        }
        drop(streams);
        if self.take_poisoned(observed) {
            Err(SqlNodeError::WorkerStatePoisoned)
        } else {
            Ok(())
        }
    }
}

/// A loopback SQL node serving accepted connections on a warm worker pool
/// that grows with one dedicated thread per connection past its size --
/// Go's `go s.onConn(clientConn)` per accept -- under a bounded
/// accepted-socket queue.
pub struct ConcurrentSqlNode<F: QuerySessionFactory> {
    listener: TcpListener,
    /// Go's `socket` listener, when `socket` is configured.
    socket: Option<SocketListener>,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    /// Shared TLS owner; each accepted connection uses its current material
    /// to advertise `CLIENT_SSL` and retains that snapshot through handshake.
    tls: Arc<MysqlTlsManager>,
    /// This server's `SHOW STATUS` provider, unregistered when it drops.
    _statistics: crate::stat::RegisteredStatistics,
    _tls_rotation: Option<TlsRotation>,
    /// Go's `Instance.MaxConnections`: the simultaneous-connection limit,
    /// where zero means unlimited (`server.go`'s `checkConnectionCount`).
    /// It also bounds the warm pool; demand past it spawns one dedicated
    /// thread per connection.
    worker_count: usize,
    shutdown: ShutdownHandle,
    shutdown_grace: Duration,
    connection_timeout: Duration,
    _server_memory_limit: Option<ServerMemoryLimitRunner>,
    _memory_usage_alarm: Option<MemoryUsageAlarmRunner>,
}

/// Go `go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()`: the
/// handle runs until its domain closes the exit it was built over.
pub(crate) fn run_expensive_query_handle(
    handle: Arc<tidb_expensivequery::Handle>,
    manager: Arc<dyn tidb_sessmgr::processinfo::SessionManager>,
) {
    handle.set_session_manager(manager);
    std::thread::spawn(move || handle.run());
}

struct ServerMemoryLimitRunner {
    exit: mpsc::Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl ServerMemoryLimitRunner {
    fn start(manager: Arc<dyn tidb_sessmgr::processinfo::SessionManager>) -> Self {
        let (exit, receiver) = mpsc::channel();
        let mut handle = tidb_servermemorylimit::new_server_memory_limit_handle(receiver);
        handle.set_session_manager(manager);
        let thread = std::thread::spawn(move || handle.run());
        Self {
            exit,
            thread: Some(thread),
        }
    }
}

impl Drop for ServerMemoryLimitRunner {
    fn drop(&mut self) {
        let _ = self.exit.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct MemoryUsageAlarmRunner {
    exit: crossbeam_channel::Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl MemoryUsageAlarmRunner {
    fn start(manager: Arc<dyn tidb_sessmgr::processinfo::SessionManager>) -> Self {
        let (exit, receiver) = crossbeam_channel::unbounded();
        let handle = tidb_memoryusagealarm::Handle::new(
            receiver,
            Arc::new(tidb_memoryusagealarm::TiDBConfigProvider),
        );
        handle.set_session_manager(Some(manager));
        let thread = std::thread::spawn(move || handle.run());
        Self {
            exit,
            thread: Some(thread),
        }
    }
}

impl Drop for MemoryUsageAlarmRunner {
    fn drop(&mut self) {
        let _ = self.exit.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl<F: QuerySessionFactory> ConcurrentSqlNode<F> {
    /// Binds the configured loopback endpoint and retains process authorities.
    pub fn bind(
        config: &NodeConfig,
        factory: Arc<F>,
        users: Arc<ConfiguredUserStore>,
    ) -> Result<Self, SqlNodeError> {
        let tls =
            MysqlTlsManager::new(config).map_err(|error| SqlNodeError::Tls(error.to_string()))?;
        // Go's listener blocks in `Accept`; shutdown and a panicked serving
        // thread wake it (`AcceptWaker`).
        let listener = TcpListener::bind((config.host, config.port)).map_err(SqlNodeError::Bind)?;
        let tls_variables = tls.variables();
        if tls.current().is_some() {
            // Go `NewServer`, once a TLS config is in place.
            let client_verification = tls_variables
                .iter()
                .any(|(name, value)| *name == "ssl_ca" && !value.is_empty());
            tidb_logutil::bg_logger().info(
                "mysql protocol server secure connection is enabled",
                &[tidb_log::Field::new(
                    "client verification enabled",
                    tidb_log::Value::Bool(client_verification),
                )],
            );
        }
        crate::log_mysql_listening(&config.host.to_string(), config.port);
        let socket = (!config.socket.is_empty())
            .then(|| SocketListener::bind(&config.socket))
            .transpose()?;
        factory.install_tls_manager(tls.clone(), tls_variables);
        // Go `NewServer`: `variable.RegisterStatistics(s)`.
        let statistics = crate::stat::RegisteredStatistics::register(Arc::new(
            crate::stat::ServerStatistics {
                tls: Arc::clone(&tls),
            },
        ));
        let tls_rotation = TlsRotation::start(Arc::clone(&tls));
        let session_manager = factory.session_manager();
        let server_memory_limit = session_manager
            .as_ref()
            .map(|manager| ServerMemoryLimitRunner::start(Arc::clone(manager)));
        if let (Some(handle), Some(manager)) = (factory.expensive_query_handle(), &session_manager) {
            run_expensive_query_handle(handle, Arc::clone(manager));
        }
        let memory_usage_alarm = session_manager.map(MemoryUsageAlarmRunner::start);
        let shutdown = ShutdownHandle::default();
        shutdown.waker.bind_to(&listener);
        let tracker = Arc::new(ConnectionTracker::with_config(
            &config.global_config,
            factory.server_identity(),
            factory.connection_ids(),
        ));
        Ok(Self {
            listener,
            socket,
            factory,
            users,
            tracker,
            max_allowed_packet: config.max_allowed_packet,
            tls,
            _statistics: statistics,
            _tls_rotation: tls_rotation,
            worker_count: config.max_connections,
            shutdown,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            connection_timeout: config.connection_timeout,
            _server_memory_limit: server_memory_limit,
            _memory_usage_alarm: memory_usage_alarm,
        })
    }

    /// Returns the operating-system-selected listener address.
    pub fn local_addr(&self) -> Result<SocketAddr, SqlNodeError> {
        self.listener.local_addr().map_err(SqlNodeError::Listener)
    }

    /// Returns shared exact connection lifecycle counters.
    #[must_use]
    pub fn tracker(&self) -> Arc<ConnectionTracker> {
        Arc::clone(&self.tracker)
    }

    /// Returns the signal that stops acceptance and starts worker drain.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    /// Returns the configured bound on graceful connection drain.
    #[must_use]
    pub fn shutdown_grace_ms(&self) -> u128 {
        self.shutdown_grace.as_millis()
    }

    /// Overrides the graceful drain interval for deterministic lifecycle tests.
    #[must_use]
    pub fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Runs the production accept loop indefinitely.
    pub fn run(self) -> Result<(), SqlNodeError> {
        self.run_accept_loop(None)
    }

    /// Accepts exactly `connections` sockets, then drains all fixed workers.
    ///
    /// This bounded entry point exists for lifecycle and concurrency proofs;
    /// production uses [`Self::run`].
    pub fn serve_connections(self, connections: usize) -> Result<(), SqlNodeError> {
        self.run_accept_loop(Some(connections))
    }

    fn run_accept_loop(self, limit: Option<usize>) -> Result<(), SqlNodeError> {
        self.run_accept_loop_with(limit, prepare_accepted_stream)
    }

    fn run_accept_loop_with<P>(
        self,
        limit: Option<usize>,
        mut prepare_stream: P,
    ) -> Result<(), SqlNodeError>
    where
        P: FnMut(&ClientSocket, Duration) -> Result<(), SqlNodeError>,
    {
        let active_sockets = Arc::new(ActiveSockets::default());
        // Includes sockets still in authentication, before ProcessRegistry has
        // a SQL session. A lost lease must retire those IDs as well.
        let _identity_registration = self.tracker.identity.as_ref().map(|identity| {
            let sockets = Arc::clone(&active_sockets);
            identity.set_connection_killer(Some(Arc::new(move || {
                let _ = sockets.cancel_queries();
                let _ = sockets.shutdown_all();
            })));
            IdentitySocketRegistration(Arc::clone(identity))
        });
        // Go pre-spawns nothing: `go s.onConn(clientConn)` runs one goroutine
        // per accepted connection. The warm pool keeps a bounded set of
        // threads for the common small fan-out (and for the deterministic
        // lifecycle proofs); demand past it is served by spawning exactly
        // what Go would -- a thread per connection.
        let warm_workers = self.worker_count.min(MAX_CONNECTION_WORKERS);
        let WorkerPool {
            mut workers,
            work_senders: worker_senders,
            available_workers,
            available_sender,
            terminal_sender,
            terminal_workers,
        } = spawn_workers(
            warm_workers,
            &self.factory,
            &self.users,
            &self.tracker,
            WorkerConnectionConfig {
                max_allowed_packet: self.max_allowed_packet,
                tls: Some(Arc::clone(&self.tls)),
            },
            self.shutdown.waker.clone(),
        )?;

        let (acceptors, incoming) = Acceptors::start(&self.listener, self.socket.as_ref())?;
        let mut accepted = 0_usize;
        let mut dedicated_workers: Vec<WorkerHandle> = Vec::new();
        let mut next_worker_index = warm_workers;
        let connection_config = WorkerConnectionConfig {
            max_allowed_packet: self.max_allowed_packet,
            tls: Some(Arc::clone(&self.tls)),
        };
        let accept_result = (|| loop {
            if limit == Some(accepted) || self.shutdown.is_shutdown_requested() {
                break Ok(());
            }
            // A panicked or returned worker must stop admission promptly, so
            // the terminal channel is polled on every iteration -- not only
            // while the loop idles inside accept.
            poll_terminal(&terminal_workers)?;
            // Reap dedicated threads whose client has gone: a joinable
            // thread's stack stays MAPPED until `join`, so deferring every
            // join to the drain would hold one dead stack per finished
            // connection -- tens of thousands of VMAs under connection
            // churn, exactly what Go never retains when a goroutine ends.
            dedicated_workers = dedicated_workers
                .drain(..)
                .filter_map(|handle| {
                    if handle.join.is_finished() {
                        // A panic already surfaced through the terminal guard,
                        // so the join outcome itself needs no handling: the
                        // point is releasing the dead thread's stack.
                        let _ = handle.join.join();
                        None
                    } else {
                        Some(handle)
                    }
                })
                .collect();
            // Go ACCEPTS first and checks capacity after
            // (`server.go`'s `onConn` -> `checkConnectionCount`):
            //
            //     if conns >= int(s.cfg.Instance.MaxConnections) {
            //         return servererr.ErrConCount
            //     }
            //     ...
            //     case servererr.ErrConCount:
            //         if err := conn.writeError(ctx, err); ...
            //
            // so a client over the limit is TOLD `1040 Too many connections`
            // and closed. Acquiring a worker BEFORE accepting left that client
            // in the listen backlog instead, where it waited out its own
            // connect timeout and reported "Lost connection ... waiting for
            // initial communication packet" -- a pooler cannot tell that from
            // a dead server, and 1040 is what tells it to back off.
            let (stream, peer_addr) = match incoming.recv() {
                Ok(Ok(connection)) => connection,
                Ok(Err(error)) => break Err(SqlNodeError::Listener(error)),
                Err(mpsc::RecvError) => {
                    break Err(SqlNodeError::Listener(std::io::Error::other(
                        "every listener stopped",
                    )))
                }
            };
            // A wake (shutdown, or a panicked serving thread) is a connection
            // the loop drops unserved, as Go's closed listener ends `Accept`.
            if self.shutdown.is_shutdown_requested() {
                drop(stream);
                break Ok(());
            }
            if let Err(error) = poll_terminal(&terminal_workers) {
                drop(stream);
                break Err(error);
            }
            if self.tracker.identity_lost() {
                drop(stream);
                continue;
            }
            // Go's capacity test is a COUNT against the configured limit, and
            // a zero limit is UNLIMITED (`server.go`'s `checkConnectionCount`):
            //
            //     // When the value of Instance.MaxConnections is 0, the number
            //     // of connections is unlimited.
            //     if int(s.cfg.Instance.MaxConnections) == 0 { return nil }
            //     conns := s.ConnectionCount()
            //     if conns >= int(s.cfg.Instance.MaxConnections) { ... }
            //
            // The count says exactly what Go's says, and every admitted client
            // under the limit is guaranteed a serving thread because the
            // dispatch below grows past the warm pool on demand.
            if self.worker_count != 0 && self.tracker.active() >= self.worker_count {
                refuse_over_capacity(&stream);
                continue;
            }
            // Go `newConn` logs a socket option it could not set and serves
            // the connection anyway: one client's socket never ends the
            // listener (a peer that already closed fails its handshake).
            if let Err(error) = prepare_stream(&stream, self.connection_timeout) {
                tidb_logutil::bg_logger()
                    .error("failed to set connection socket options", &[crate::zap_error(error)]);
            }
            let connection_key = u64::try_from(accepted)
                .ok()
                .and_then(|value| value.checked_add(1))
                .expect("accepted connection count fits u64");
            let cancellation = ConnectionCancellation::default();
            active_sockets.register(
                connection_key,
                stream.try_clone().map_err(SqlNodeError::Listener)?,
                cancellation.clone(),
            )?;
            let registration =
                ActiveSocketRegistration::new(connection_key, Arc::clone(&active_sockets));
            // Fast path: an idle warm-pool thread. An empty channel also means
            // "momentarily between hands", but that no longer refuses (or
            // waits for) anything: Go serves each accepted connection on its
            // own goroutine (`go s.onConn(clientConn)`), so a pool-less moment
            // simply grows by one dedicated thread for this socket.
            match available_workers.try_recv() {
                Ok(worker_index) => {
                    if worker_senders[worker_index]
                        .send(ConnectionWork {
                            stream,
                            peer_addr,
                            cancellation,
                            registration,
                        })
                        .is_err()
                    {
                        break Err(SqlNodeError::WorkerQueueClosed);
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {
                    let index = next_worker_index;
                    next_worker_index += 1;
                    let job = dedicated_connection_job(
                        index,
                        stream,
                        peer_addr,
                        cancellation,
                        registration,
                        Arc::clone(&self.factory),
                        Arc::clone(&self.users),
                        Arc::clone(&self.tracker),
                        connection_config.clone(),
                        terminal_sender.clone(),
                    );
                    match std::thread::Builder::new()
                        .name(format!("tidb-sql-connection-{index}"))
                        .stack_size(SQL_WORKER_STACK_BYTES)
                        .spawn(job)
                    {
                        Ok(join) => dedicated_workers.push(WorkerHandle { index, join }),
                        Err(error) => break Err(SqlNodeError::WorkerSpawn(error)),
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    break Err(SqlNodeError::WorkerQueueClosed)
                }
            }
            accepted += 1;
        })();

        acceptors.stop(&self.shutdown.waker);
        drop(worker_senders);
        drop(available_sender);
        drop(terminal_sender);
        workers.append(&mut dedicated_workers);
        let drain_result = drain_workers(workers, &active_sockets, self.shutdown_grace);
        combine_node_results(accept_result, drain_result)
    }
}

/// Go's `socket` listener (`net.Listen("unix", cfg.Socket)`). Go's
/// `UnixListener` unlinks its socket file on `Close`; dropping this does.
struct SocketListener {
    #[cfg(unix)]
    listener: std::os::unix::net::UnixListener,
    path: std::path::PathBuf,
}

impl SocketListener {
    #[cfg(unix)]
    fn bind(socket: &str) -> Result<Self, SqlNodeError> {
        cleanup_stale_socket(socket)?;
        let listener =
            std::os::unix::net::UnixListener::bind(socket).map_err(SqlNodeError::Bind)?;
        tidb_logutil::bg_logger().info(
            "server is running MySQL protocol",
            &[tidb_log::Field::new("socket", tidb_log::Value::Str(socket.to_owned()))],
        );
        Ok(Self {
            listener,
            path: socket.into(),
        })
    }

    #[cfg(not(unix))]
    fn bind(_socket: &str) -> Result<Self, SqlNodeError> {
        Err(SqlNodeError::Bind(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Unix-domain sockets are not supported on this platform",
        )))
    }
}

impl Drop for SocketListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Go `cleanupStaleSocket`: a socket file a previous server left behind is
/// removed, but a live socket or a file that is no socket is an error.
#[cfg(unix)]
fn cleanup_stale_socket(socket: &str) -> Result<(), SqlNodeError> {
    use std::os::unix::fs::FileTypeExt;
    let refuse = |message: String| SqlNodeError::Bind(std::io::Error::other(message));
    let metadata = match std::fs::metadata(socket) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(SqlNodeError::Bind(error)),
    };
    if !metadata.file_type().is_socket() {
        return Err(refuse(format!(
            "the specified socket file {socket} is a {:?} instead of a socket file",
            metadata.file_type()
        )));
    }
    if std::os::unix::net::UnixStream::connect(socket).is_ok() {
        return Err(refuse(format!(
            "unix socket {socket} exists and is functional, not removing it"
        )));
    }
    std::fs::remove_file(socket).map_err(|error| {
        refuse(format!("failed to cleanup stale Unix socket file {socket}: {error}"))
    })
}

type Accepted = std::io::Result<(ClientSocket, PeerAddress)>;

/// Go's `startNetworkListener` per listener: each accepts on its own thread
/// and hands connections to the one accept loop.
struct Acceptors {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    socket_path: Option<std::path::PathBuf>,
}

impl Acceptors {
    fn start(
        listener: &TcpListener,
        socket: Option<&SocketListener>,
    ) -> Result<(Self, mpsc::Receiver<Accepted>), SqlNodeError> {
        let stop = Arc::new(AtomicBool::new(false));
        let (sender, incoming) = mpsc::channel::<Accepted>();
        let mut threads = Vec::new();
        let tcp = listener.try_clone().map_err(SqlNodeError::Listener)?;
        threads.push(Self::spawn(&stop, &sender, move || {
            tcp.accept()
                .map(|(stream, address)| (ClientSocket::from(stream), PeerAddress::Tcp(address)))
        })?);
        #[cfg(unix)]
        if let Some(socket) = socket {
            let unix = socket.listener.try_clone().map_err(SqlNodeError::Listener)?;
            threads.push(Self::spawn(&stop, &sender, move || {
                // The peer's address is unused (Go's `PeerHost` is
                // `localhost`), and std's `accept` fails on macOS for an
                // unnamed client whose address it cannot parse.
                rustix::net::accept(&unix)
                    .map(|fd| {
                        (
                            ClientSocket::from(std::os::unix::net::UnixStream::from(fd)),
                            PeerAddress::Unix,
                        )
                    })
                    .map_err(std::io::Error::from)
            })?);
        }
        Ok((
            Self {
                stop,
                threads,
                socket_path: socket.map(|socket| socket.path.clone()),
            },
            incoming,
        ))
    }

    fn spawn(
        stop: &Arc<AtomicBool>,
        sender: &mpsc::Sender<Accepted>,
        mut accept: impl FnMut() -> Accepted + Send + 'static,
    ) -> Result<JoinHandle<()>, SqlNodeError> {
        let stop = Arc::clone(stop);
        let sender = sender.clone();
        std::thread::Builder::new()
            .name("tidb-sql-accept".to_owned())
            .spawn(move || loop {
                let accepted = accept();
                if stop.load(Ordering::Acquire) {
                    return;
                }
                match accepted {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        let _ = sender.send(Err(error));
                        return;
                    }
                    Ok(connection) => {
                        if sender.send(Ok(connection)).is_err() {
                            return;
                        }
                    }
                }
            })
            .map_err(SqlNodeError::WorkerSpawn)
    }

    /// Ends every acceptor: each is woken out of its `accept` by one
    /// connection it then drops, as Go's closed listener ends `Accept`.
    fn stop(self, waker: &AcceptWaker) {
        self.stop.store(true, Ordering::Release);
        waker.wake();
        #[cfg(unix)]
        let socket_woken = self
            .socket_path
            .as_ref()
            .is_none_or(|path| std::os::unix::net::UnixStream::connect(path).is_ok());
        #[cfg(not(unix))]
        let socket_woken = true;
        for thread in self.threads {
            if socket_woken || thread.is_finished() {
                let _ = thread.join();
            }
        }
    }
}

type WorkerJob = Box<dyn FnOnce() + Send + 'static>;

fn run_worker<S>(
    index: usize,
    work_receiver: mpsc::Receiver<ConnectionWork>,
    available: mpsc::SyncSender<usize>,
    terminal: TerminalSender,
    mut serve: S,
) where
    S: FnMut(ConnectionWork),
{
    let _terminal = WorkerTerminalGuard { index, terminal };
    if available.send(index).is_err() {
        return;
    }
    loop {
        let Ok(work) = work_receiver.recv() else {
            return;
        };
        serve(work);
        if available.send(index).is_err() {
            return;
        }
    }
}

/// Serves one accepted connection to completion. Shared verbatim by the warm
/// pool and the dedicated per-connection threads so both paths run the same
/// handshake/query loop Go runs inside every `onConn` goroutine.
fn serve_connection_work<F: QuerySessionFactory>(
    work: ConnectionWork,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: &WorkerConnectionConfig,
) {
    let ConnectionWork {
        stream,
        peer_addr,
        cancellation,
        registration: _registration,
    } = work;
    let tls = connection
        .tls
        .as_ref()
        .and_then(|manager| manager.current());
    if let Err(error) = serve_mysql_connection_with_runtime(
        stream,
        peer_addr,
        cancellation,
        factory.as_ref(),
        users.as_ref(),
        tracker,
        MysqlConnectionRuntime {
            max_allowed_packet: connection.max_allowed_packet,
            tls: tls.as_ref(),
        },
    ) {
        log_connection_error(&error, peer_addr);
    }
}

/// Go's log line for a connection that ended in `error`: `onConn`'s
/// handshake and lost-PD rejections, or `clientConn.Run`'s close reasons.
/// A clean end of stream and a recovered panic (already logged where it was
/// caught) log nothing more, like Go.
fn log_connection_error(error: &MysqlConnectionError, peer_addr: PeerAddress) {
    use tidb_protocol::PacketError;
    let logger = tidb_logutil::bg_logger();
    match error {
        MysqlConnectionError::Handshake(_) => logger.warn(
            "Server.onConn handshake",
            &[
                crate::zap_error(error),
                tidb_log::Field::new("remote addr", tidb_log::Value::Str(peer_addr.remote_addr())),
            ],
        ),
        MysqlConnectionError::ResultUndetermined(message) => logger.warn(
            "result undetermined, close this connection",
            &[crate::zap_error(message)],
        ),
        MysqlConnectionError::Io(io) if io.kind() == std::io::ErrorKind::ConnectionAborted => {
            logger.warn("reject connection due to lost connection to PD", &[]);
        }
        MysqlConnectionError::Packet(PacketError::EndOfStream)
        | MysqlConnectionError::Panicked(_)
        | MysqlConnectionError::PartialResult(_) => {}
        MysqlConnectionError::Io(_) | MysqlConnectionError::Packet(_) => logger.warn(
            "read packet failed, close this connection",
            &[crate::zap_error(error)],
        ),
    }
}

/// Builds the job for a dedicated thread that serves exactly ONE accepted
/// socket, the direct analogue of Go's per-connection goroutine
/// (`server.go`: `go s.onConn(clientConn)`). The terminal guard reports a
/// panic through the same channel the warm pool uses, so admission still
/// stops when a serving thread dies unexpectedly.
#[allow(clippy::too_many_arguments)]
fn dedicated_connection_job<F: QuerySessionFactory>(
    index: usize,
    stream: ClientSocket,
    peer_addr: PeerAddress,
    cancellation: ConnectionCancellation,
    registration: ActiveSocketRegistration,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
    terminal: TerminalSender,
) -> WorkerJob {
    Box::new(move || {
        let _terminal = WorkerTerminalGuard { index, terminal };
        serve_connection_work(
            ConnectionWork {
                stream,
                peer_addr,
                cancellation,
                registration,
            },
            &factory,
            &users,
            &tracker,
            &connection,
        );
    })
}

fn spawn_workers<F: QuerySessionFactory>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
    waker: AcceptWaker,
) -> Result<WorkerPool, SqlNodeError> {
    spawn_workers_with(count, factory, users, tracker, connection, waker, |index, job| {
        std::thread::Builder::new()
            .name(format!("tidb-sql-connection-{index}"))
            .stack_size(SQL_WORKER_STACK_BYTES)
            .spawn(job)
    })
}

fn spawn_workers_with<F, S>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
    waker: AcceptWaker,
    mut spawn: S,
) -> Result<WorkerPool, SqlNodeError>
where
    F: QuerySessionFactory,
    S: FnMut(usize, WorkerJob) -> std::io::Result<JoinHandle<()>>,
{
    let mut workers = Vec::with_capacity(count);
    let mut work_senders = Vec::with_capacity(count);
    let (available_sender, available_receiver) = mpsc::sync_channel(count);
    let (terminal_channel, terminal_receiver) = mpsc::channel();
    let terminal_sender = TerminalSender {
        sender: terminal_channel,
        waker,
    };
    for index in 0..count {
        let (work_sender, work_receiver) = mpsc::channel::<ConnectionWork>();
        let factory = Arc::clone(factory);
        let users = Arc::clone(users);
        let worker_tracker = Arc::clone(tracker);
        let worker_available = available_sender.clone();
        let worker_terminal = terminal_sender.clone();
        let worker_connection = connection.clone();
        let job: WorkerJob = Box::new(move || {
            run_worker(
                index,
                work_receiver,
                worker_available,
                worker_terminal,
                move |work| {
                    serve_connection_work(
                        work,
                        &factory,
                        &users,
                        &worker_tracker,
                        &worker_connection,
                    );
                },
            );
        });
        let worker = match spawn(index, job) {
            Ok(worker) => worker,
            Err(error) => {
                drop(work_sender);
                drop(work_senders);
                drop(available_receiver);
                drop(available_sender);
                drop(terminal_sender);
                let mut failures = vec![SqlNodeError::WorkerSpawn(error)];
                failures.extend(join_worker_failures(workers));
                return Err(collapse_node_failures(failures));
            }
        };
        work_senders.push(work_sender);
        workers.push(WorkerHandle {
            index,
            join: worker,
        });
    }
    // The original terminal sender moves into the returned pool: the accept
    // loop clones it for every dedicated (per-connection) thread so those
    // report panics through the same channel as the warm pool.
    Ok(WorkerPool {
        workers,
        work_senders,
        available_workers: available_receiver,
        available_sender,
        terminal_sender,
        terminal_workers: terminal_receiver,
    })
}

fn join_worker_failures(workers: Vec<WorkerHandle>) -> Vec<SqlNodeError> {
    let mut panicked = Vec::new();
    for worker in workers {
        if worker.join.join().is_err() {
            panicked.push(worker.index);
        }
    }
    if panicked.is_empty() {
        Vec::new()
    } else {
        vec![SqlNodeError::WorkersPanicked { indexes: panicked }]
    }
}

fn drain_workers(
    workers: Vec<WorkerHandle>,
    active_sockets: &ActiveSockets,
    grace: Duration,
) -> Result<(), SqlNodeError> {
    // Go `Server.DrainClients`' log lines mark the same phases.
    let logger = tidb_logutil::bg_logger();
    logger.info("start drain clients", &[]);
    let mut failures = Vec::new();
    if let Err(error) = active_sockets.cancel_queries() {
        failures.push(error);
    }
    let deadline = Instant::now() + grace;
    loop {
        match active_sockets.len() {
            Ok(0) => {
                logger.info("all sessions quit in drain wait time", &[]);
                break;
            }
            Ok(_) if Instant::now() < deadline => std::thread::sleep(ACCEPT_POLL_INTERVAL),
            Ok(_) => {
                logger.info("timeout waiting all sessions quit", &[]);
                break;
            }
            Err(error) => {
                failures.push(error);
                break;
            }
        }
    }
    // Go `KillAllConnections`.
    logger.info(
        "kill all connections.",
        &[tidb_log::Field::new(
            "category",
            tidb_log::Value::Str("server".to_owned()),
        )],
    );
    match active_sockets.len() {
        Ok(0) => {}
        Ok(_) => {
            let (_, error) = active_sockets.shutdown_all();
            failures.extend(error);
        }
        Err(error) => {
            failures.push(error);
            let (_, error) = active_sockets.shutdown_all();
            failures.extend(error);
        }
    }
    failures.extend(join_worker_failures(workers));
    if failures.is_empty() {
        Ok(())
    } else {
        Err(collapse_node_failures(failures))
    }
}

fn combine_node_results(
    first: Result<(), SqlNodeError>,
    second: Result<(), SqlNodeError>,
) -> Result<(), SqlNodeError> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(first), Err(second)) => Err(collapse_node_failures(vec![first, second])),
    }
}

fn collapse_node_failures(mut failures: Vec<SqlNodeError>) -> SqlNodeError {
    if failures.len() == 1 {
        failures.pop().expect("one SQL node failure")
    } else {
        SqlNodeError::Multiple(failures)
    }
}

/// Startup or runtime failure from [`ConcurrentSqlNode`].
#[derive(Debug)]
pub enum SqlNodeError {
    /// The configured address could not be bound.
    Bind(std::io::Error),
    /// The active listener failed.
    Listener(std::io::Error),
    /// A fixed connection worker could not be created.
    WorkerSpawn(std::io::Error),
    /// Every fixed worker exited before the accepted socket was handed off.
    WorkerQueueClosed,
    /// A worker exited while the accept loop still expected it to serve.
    WorkerTerminated {
        /// Fixed worker index.
        index: usize,
        /// Whether unwind caused the terminal observation.
        panicked: bool,
    },
    /// Every worker was joined; these indexes panicked.
    WorkersPanicked {
        /// Worker indexes whose join handles carried panics.
        indexes: Vec<usize>,
    },
    /// Shared admission or active-socket state was poisoned.
    WorkerStatePoisoned,
    /// One accepted connection failed in a direct lifecycle proof.
    Connection(MysqlConnectionError),
    /// Server TLS material for the MySQL port could not be obtained.
    Tls(String),
    /// Independent node or drain failures retained after all cleanup attempts.
    Multiple(Vec<SqlNodeError>),
}

impl fmt::Display for SqlNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(error) => write!(formatter, "failed to bind SQL listener: {error}"),
            Self::Listener(error) => write!(formatter, "SQL listener failed: {error}"),
            Self::WorkerSpawn(error) => write!(formatter, "failed to spawn SQL worker: {error}"),
            Self::WorkerQueueClosed => formatter.write_str("SQL worker queue closed"),
            Self::WorkerTerminated { index, panicked } => write!(
                formatter,
                "SQL worker {index} terminated while admission was active (panicked={panicked})"
            ),
            Self::WorkersPanicked { indexes } => {
                write!(formatter, "SQL workers panicked during join: {indexes:?}")
            }
            Self::WorkerStatePoisoned => formatter.write_str("SQL worker state is poisoned"),
            Self::Connection(error) => write!(formatter, "MySQL connection failed: {error}"),
            Self::Tls(detail) => write!(formatter, "MySQL port TLS is unusable: {detail}"),
            Self::Multiple(failures) => {
                formatter.write_str("multiple SQL node failures")?;
                for failure in failures {
                    write!(formatter, "; {failure}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for SqlNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) | Self::Listener(error) | Self::WorkerSpawn(error) => Some(error),
            Self::Connection(error) => Some(error),
            Self::WorkerQueueClosed
            | Self::WorkerTerminated { .. }
            | Self::WorkersPanicked { .. }
            | Self::WorkerStatePoisoned
            | Self::Tls(_)
            | Self::Multiple(_) => None,
        }
    }
}

/// readies one accepted MySQL socket for serving, exactly as the production
/// accept loop does.
///
/// TCP_NODELAY comes first because Go puts it on every accepted connection:
/// `net.setDefaultListenerSockopts` runs `setNoDelayConn` on each fd out of
/// `Accept`, so a Go TiDB never lets Nagle hold a response segment back. A
/// server that skips it pays ~40ms per statement whenever a response leaves in
/// more than one small write and the client's delayed ACK loses that race --
/// which is precisely what an OLTP pooler's request/response pattern produces
/// through any proxy or tunnel hop between the peers.
pub(crate) fn prepare_accepted_stream(
    stream: &ClientSocket,
    timeout: Duration,
) -> Result<(), SqlNodeError> {
    stream.set_nodelay(true).map_err(SqlNodeError::Listener)?;
    stream
        .set_nonblocking(false)
        .map_err(SqlNodeError::Listener)?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(SqlNodeError::Listener)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(SqlNodeError::Listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::QuerySession;
    use crate::node_config::MemoryArbitratorConfig;
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::PathBuf;
    use tidb_store_driver_error::sql_query_error::SqlQueryError;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn cluster_lifecycle_batch_disabled_global_kill_uses_simple_ids() {
        let mut config = test_config();
        config.global_config.enable_global_kill = false;
        let users =
            Arc::new(ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n").unwrap());
        let node = ConcurrentSqlNode::bind(&config, Arc::new(UnusedFactory), users).unwrap();
        assert_eq!(node.tracker.connection_ids.next_id(), 1);
        assert_eq!(node.tracker.connection_ids.next_id(), 2);
    }

    #[test]
    fn cluster_lifecycle_batch_64bit_configuration_reaches_allocator() {
        let mut config = test_config();
        config.global_config.enable_32bits_connection_id = false;
        let users =
            Arc::new(ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n").unwrap());
        let node = ConcurrentSqlNode::bind(&config, Arc::new(UnusedFactory), users).unwrap();
        let (id, truncated) =
            tidb_globalconn::parse_conn_id(node.tracker.connection_ids.next_id()).unwrap();
        assert!(id.is_64bits && !truncated);
        assert_eq!(id.server_id, 1);
    }

    #[test]
    fn cluster_lifecycle_batch_standalone_publishes_its_allocator_identity() {
        let mut info = tidb_session::domain::serverinfo_etcd::node_server_info(&test_config());
        let encoded = info.marshal().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["server_id"], 1);
    }

    #[test]
    fn cluster_lifecycle_batch_unclaimed_identity_closes_before_handshake() {
        struct LostFactory(Arc<tidb_domain::server_id::ServerIdAuthority>);
        impl QuerySessionFactory for LostFactory {
            type Session = UnusedSession;
            fn open_session(&self, _: SessionContext) -> Result<Self::Session, SqlQueryError> {
                panic!("unclaimed identity must not authenticate a session")
            }
            fn server_identity(&self) -> Option<Arc<tidb_domain::server_id::ServerIdAuthority>> {
                Some(self.0.clone())
            }
        }
        let client =
            tidb_pd_client::EtcdClient::connect(["127.0.0.1:1"], Duration::from_millis(100))
                .unwrap();
        let identity = tidb_domain::server_id::ServerIdAuthority::new(
            Some(Arc::new(tidb_ddl::etcd_ops::EtcdClientOps::new(
                Arc::new(client),
            ))),
            true,
        );
        let users =
            Arc::new(ConfiguredUserStore::parse("root\t%\tmysql_native_password\t\n").unwrap());
        let node = ConcurrentSqlNode::bind(&test_config(), Arc::new(LostFactory(identity)), users)
            .unwrap();
        let address = node.local_addr().unwrap();
        let shutdown = node.shutdown_handle();
        let tracker = node.tracker();
        let serving = std::thread::spawn(move || node.run());
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let observed = stream.read(&mut [0u8; 1]);
        shutdown.shutdown();
        serving.join().unwrap().unwrap();
        assert!(
            matches!(observed, Ok(0)),
            "unexpected handshake/read: {observed:?}"
        );
        assert_eq!(tracker.accepted(), 0);
    }

    struct UnusedSession;

    impl QuerySession for UnusedSession {
        fn parse_statement(&mut self, sql: &str) -> Result<tidb_ast::Stmt, SqlQueryError> {
            // A fixture session takes any command text; one that is not SQL
        // stands as a plain statement.
        Ok(tidb_parser::parse(sql)
            .unwrap_or_else(|_| tidb_parser::parse("SELECT 1").expect("a statement")))
        }

        fn execute_stmt<'a>(
            &'a mut self,
            _sql: &str,
            _stmt: &tidb_ast::Stmt,
        ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
            panic!("the drain regression never completes authentication")
        }
    }

    struct UnusedFactory;

    impl QuerySessionFactory for UnusedFactory {
        type Session = UnusedSession;

        fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
            panic!("the drain regression never completes authentication")
        }
    }

    fn test_config() -> NodeConfig {
        NodeConfig {
            report_status: false,
            advertise_address: String::new(),
            status_host: "0.0.0.0".to_owned(),
            status_port: 0,
            socket: String::new(),
            isolation_read_engines: Vec::new(),
            store_kind: crate::node_config::StoreKind::TiKv,
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            affinity_cpus: Vec::new(),
            pd_endpoints: vec!["127.0.0.1:2379".to_owned()],
            max_allowed_packet: tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
            auth_file: PathBuf::from("unused"),
            load_privileges: false,
            ssl_cert: None,
            ssl_key: None,
            ssl_ca: None,
            min_tls_version: String::new(),
            // The unit tests here exercise worker lifecycle, not the wire, so
            // they take the plaintext port rather than pay for key generation.
            auto_tls: false,
            disconnect_on_expired_password: true,
            sem_enabled: false,
            sem_config: String::new(),
            skip_grant_table: false,
            max_connections: 2,
            connection_timeout: Duration::from_secs(5),
            deadlock_history_capacity: 10,
            deadlock_history_collect_retryable: false,
            schema_lease: Duration::from_millis(45_000),
            run_ddl: true,
            stats_lease: crate::node_config::StatsLease::Positive(Duration::from_secs(3)),
            cluster_security: tidb_pd_client::ClusterSecurity::plaintext(),
            spill_storage: tidb_spill_storage::SpillStorageSpec {
                path: std::env::temp_dir().join("tidb-sql-node-unit-spill"),
                quota_bytes: -1,
                encryption: tidb_spill_storage::SpillEncryptionMethod::Plaintext,
            },
            memory_arbitrator: MemoryArbitratorConfig {
                server_memory_limit: "80%".to_owned(),
                mode: "disable".to_owned(),
                soft_limit: "0".to_owned(),
            },
            global_config: tidb_config::config_tree::Config::default(),
        }
    }

    /// Go `newConn` only logs a socket option it could not set: the
    /// connection is still served and the listener keeps accepting.
    #[test]
    fn accepted_socket_setup_error_is_logged_and_the_connection_still_served() {
        let users = ConfiguredUserStore::parse(
            "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
        )
        .unwrap();
        let node =
            ConcurrentSqlNode::bind(&test_config(), Arc::new(UnusedFactory), Arc::new(users))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(20));
        assert_eq!(node.shutdown_grace_ms(), 20);
        let address = node.local_addr().unwrap();
        let tracker = node.tracker();
        let server = std::thread::spawn(move || {
            let mut prepared = 0;
            node.run_accept_loop_with(Some(2), move |stream, timeout| {
                prepared += 1;
                if prepared == 2 {
                    return Err(SqlNodeError::Listener(std::io::Error::other(
                        "injected accepted-socket setup failure",
                    )));
                }
                stream
                    .set_nonblocking(false)
                    .map_err(SqlNodeError::Listener)?;
                stream
                    .set_read_timeout(Some(timeout))
                    .map_err(SqlNodeError::Listener)?;
                stream
                    .set_write_timeout(Some(timeout))
                    .map_err(SqlNodeError::Listener)
            })
        });

        let stalled_client = TcpStream::connect(address).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while tracker.active() != 1 {
            assert!(Instant::now() < deadline, "first worker did not start");
            std::thread::sleep(Duration::from_millis(1));
        }
        let failed_setup_client = TcpStream::connect(address).unwrap();
        server.join().unwrap().unwrap();

        assert_eq!(tracker.accepted(), 2, "the failed setup is still served");
        assert_eq!(tracker.completed(), 2);
        assert_eq!(
            tracker.active(),
            0,
            "all workers must be joined before return"
        );
        drop(failed_setup_client);
        drop(stalled_client);
    }

    #[test]
    fn partial_worker_spawn_failure_joins_every_spawned_prefix() {
        let users = Arc::new(
            ConfiguredUserStore::parse(
                "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            )
            .unwrap(),
        );
        let factory = Arc::new(UnusedFactory);
        let tracker = Arc::new(ConnectionTracker::default());
        let joined = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&joined);

        let result = spawn_workers_with(
            3,
            &factory,
            &users,
            &tracker,
            WorkerConnectionConfig {
                max_allowed_packet: tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
                tls: None,
            },
            AcceptWaker::default(),
            move |index, job: WorkerJob| {
                if index == 1 {
                    return Err(std::io::Error::other("injected second spawn failure"));
                }
                let observed = Arc::clone(&observed);
                std::thread::Builder::new().spawn(move || {
                    job();
                    observed.fetch_add(1, Ordering::AcqRel);
                })
            },
        );
        let error = match result {
            Ok(_) => panic!("injected worker spawn failure unexpectedly succeeded"),
            Err(error) => error,
        };

        assert!(matches!(error, SqlNodeError::WorkerSpawn(_)));
        assert_eq!(joined.load(Ordering::Acquire), 1);
    }

    #[test]
    fn worker_join_collects_every_panic_index() {
        let workers = vec![
            WorkerHandle {
                index: 0,
                join: std::thread::spawn(|| panic!("first worker panic")),
            },
            WorkerHandle {
                index: 1,
                join: std::thread::spawn(|| {}),
            },
            WorkerHandle {
                index: 2,
                join: std::thread::spawn(|| panic!("last worker panic")),
            },
        ];

        let failures = join_worker_failures(workers);
        let [SqlNodeError::WorkersPanicked { indexes }] = failures.as_slice() else {
            panic!("one aggregate worker panic failure");
        };
        assert_eq!(indexes, &[0, 2]);
    }

    #[test]
    fn worker_panic_surfaces_through_terminal_poll_and_stops_admission() {
        let (_available_tx, _available_rx) = mpsc::sync_channel::<usize>(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _terminal = WorkerTerminalGuard {
                index: 3,
                terminal: terminal_tx.into(),
            };
            panic!("injected pre-availability worker panic");
        });
        // The panicked guard's terminal surfaces through the same poll the
        // accept loop runs every iteration; poll with a deadline because the
        // guard only sends while the panic unwinds.
        let deadline = Instant::now() + Duration::from_secs(2);
        let surfaced = loop {
            match poll_terminal(&terminal_rx) {
                Err(error @ SqlNodeError::WorkerTerminated { .. }) => break error,
                _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
                _ => panic!("the panicked worker never surfaced through the terminal poll"),
            }
        };
        assert!(matches!(
            surfaced,
            SqlNodeError::WorkerTerminated {
                index: 3,
                panicked: true,
            }
        ));
        // A dedicated thread whose client simply disconnected reports
        // `Returned`; that is normal goroutine completion and must not stop
        // admission.
        let (returned_tx, returned_rx) = mpsc::channel();
        returned_tx
            .send(WorkerTerminal {
                index: 4,
                kind: WorkerTerminalKind::Returned,
            })
            .unwrap();
        drop(returned_tx);
        assert!(poll_terminal(&returned_rx).is_ok());
        // An empty channel keeps the accept loop admitting.
        assert!(poll_terminal(&terminal_rx).is_ok());
        assert!(worker.join().is_err());
    }

    #[test]
    fn worker_panic_after_admission_releases_socket_and_stops_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let _client = TcpStream::connect(address).unwrap();
        let (server, peer_addr) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        let connection_key = 11;
        sockets
            .register(
                connection_key,
                server.try_clone().unwrap().into(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (work_tx, work_rx) = mpsc::channel();
        let (available_tx, available_rx) = mpsc::sync_channel(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            run_worker(2, work_rx, available_tx, terminal_tx.into(), |_work| {
                panic!("injected post-admission worker panic");
            });
        });
        assert_eq!(available_rx.recv().unwrap(), 2);
        work_tx
            .send(ConnectionWork {
                stream: server.into(),
                peer_addr: peer_addr.into(),
                cancellation,
                registration,
            })
            .unwrap();

        let terminal = terminal_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert_eq!(
            terminal,
            WorkerTerminal {
                index: 2,
                kind: WorkerTerminalKind::Panicked,
            }
        );
        assert_eq!(sockets.len().unwrap(), 0);
        assert!(worker.join().is_err());
    }

    #[test]
    fn simultaneous_accept_and_drain_failures_are_both_retained() {
        let error = combine_node_results(
            Err(SqlNodeError::Listener(std::io::Error::other(
                "injected accept failure",
            ))),
            Err(SqlNodeError::WorkersPanicked {
                indexes: vec![1, 4],
            }),
        )
        .unwrap_err();

        let SqlNodeError::Multiple(failures) = error else {
            panic!("independent accept and drain failures must be aggregated");
        };
        assert!(matches!(failures[0], SqlNodeError::Listener(_)));
        assert!(matches!(
            &failures[1],
            SqlNodeError::WorkersPanicked { indexes } if indexes == &[1, 4]
        ));
    }

    #[test]
    fn repeated_shutdown_signals_are_idempotent_and_stop_admission() {
        let users = ConfiguredUserStore::parse(
            "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
        )
        .unwrap();
        let node =
            ConcurrentSqlNode::bind(&test_config(), Arc::new(UnusedFactory), Arc::new(users))
                .unwrap();
        let shutdown = node.shutdown_handle();
        shutdown.shutdown();
        shutdown.shutdown();
        shutdown.shutdown();
        assert!(shutdown.is_shutdown_requested());

        // Admission stops before any accept: the bounded loop drains and
        // returns without ever handing a socket to a worker.
        let tracker = node.tracker();
        let server = std::thread::spawn(move || node.serve_connections(1).unwrap());
        let deadline = Instant::now() + Duration::from_secs(2);
        while !server.is_finished() {
            assert!(Instant::now() < deadline, "shutdown did not stop admission");
            std::thread::sleep(Duration::from_millis(5));
        }
        server.join().unwrap();
        assert_eq!(tracker.accepted(), 0);
    }

    #[test]
    fn queued_connection_receiver_death_releases_active_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let _client = TcpStream::connect(address).unwrap();
        let (server, peer_addr) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        let connection_key = 17;
        sockets
            .register(
                connection_key,
                server.try_clone().unwrap().into(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (sender, receiver) = mpsc::channel();
        sender
            .send(ConnectionWork {
                stream: server.into(),
                peer_addr: peer_addr.into(),
                cancellation,
                registration,
            })
            .unwrap();

        drop(receiver);

        assert_eq!(sockets.len().unwrap(), 0);
    }

    #[test]
    fn poisoned_active_socket_state_still_cancels_and_forces_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        sockets.register(23, server.into(), cancellation.clone()).unwrap();
        let poisoned_sockets = Arc::clone(&sockets);
        assert!(std::thread::spawn(move || {
            let _guard = poisoned_sockets.streams.lock().unwrap();
            panic!("injected active-socket state poison");
        })
        .join()
        .is_err());
        let workers = vec![WorkerHandle {
            index: 0,
            join: std::thread::spawn(|| {}),
        }];

        let error = drain_workers(workers, sockets.as_ref(), Duration::ZERO).unwrap_err();

        assert!(matches!(error, SqlNodeError::WorkerStatePoisoned));
        assert!(cancellation.is_cancelled());
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).unwrap(), 0);
    }

    #[test]
    fn an_accepted_mysql_socket_leaves_preparation_with_nodelay_set() {
        let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        prepare_accepted_stream(
            &ClientSocket::from(server.try_clone().unwrap()),
            Duration::from_secs(30),
        )
        .unwrap();

        // Go's `setDefaultListenerSockopts` puts TCP_NODELAY on every accepted
        // connection; the Rust accept loop must match it or a Nagle-enabled
        // hop between server and client stalls small responses behind the
        // client's delayed ACK.
        assert_eq!(server.nodelay().unwrap(), true);
        assert_eq!(
            server.read_timeout().unwrap(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            server.write_timeout().unwrap(),
            Some(Duration::from_secs(30))
        );
        drop(client);
    }
}
