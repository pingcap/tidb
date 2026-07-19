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

use std::fmt;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::configured_user_store::{AuthenticatedIdentity, ConfiguredUserStore};
use crate::mysql_connection::{serve_mysql_connection, MysqlConnectionError};
use crate::node_config::NodeConfig;
use crate::resultset_source::ResultSetSource;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Cloneable process shutdown signal for the sole production accept loop.
#[derive(Clone, Debug, Default)]
pub struct ShutdownHandle {
    requested: Arc<AtomicBool>,
}

/// A backend query that can be interrupted during forced connection drain.
pub trait ActiveQueryCancellation: Send + Sync {
    /// Fires the query's canonical cancellation carrier.
    fn cancel(&self);
}

#[derive(Default)]
struct ConnectionCancellationState {
    requested: bool,
    generation: u64,
    active: Option<Arc<dyn ActiveQueryCancellation>>,
}

/// Cloneable connection-local registry for the currently active query.
#[derive(Clone, Default)]
pub struct ConnectionCancellation {
    state: Arc<Mutex<ConnectionCancellationState>>,
}

impl std::fmt::Debug for ConnectionCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionCancellation")
            .finish_non_exhaustive()
    }
}

impl ConnectionCancellation {
    /// Installs one query cancellation and returns its generation lease.
    pub fn install(
        &self,
        cancellation: Arc<dyn ActiveQueryCancellation>,
    ) -> QueryCancellationLease {
        let (generation, requested) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.generation = state.generation.wrapping_add(1);
            let generation = state.generation;
            let requested = state.requested;
            state.active = Some(Arc::clone(&cancellation));
            (generation, requested)
        };
        if requested {
            cancellation.cancel();
        }
        QueryCancellationLease {
            connection: self.clone(),
            generation,
        }
    }

    fn cancel(&self) {
        let cancellation = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.requested = true;
            state.active.clone()
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .requested
    }

    fn clear(&self, generation: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.generation == generation {
            state.active = None;
        }
    }
}

/// RAII registration for one active query cancellation generation.
pub struct QueryCancellationLease {
    connection: ConnectionCancellation,
    generation: u64,
}

impl Drop for QueryCancellationLease {
    fn drop(&mut self) {
        self.connection.clear(self.generation);
    }
}

impl ShutdownHandle {
    /// Requests shutdown. Repeated requests are idempotent.
    pub fn shutdown(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// A source-rendered error returned before a query result escapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlQueryError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// Caller-rendered error text.
    pub message: String,
}

impl SqlQueryError {
    /// Creates one explicit client-visible query failure.
    #[must_use]
    pub fn new(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
        }
    }

    /// Creates the fail-closed generic boundary for an injected engine error.
    #[must_use]
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }
}

/// A lazy query result owned by one worker-local session.
pub struct QueryResult<'a> {
    source: BoxedResultSetSource<'a>,
}

impl<'a> QueryResult<'a> {
    /// Transfers one session-owned result source to the connection writer.
    #[must_use]
    pub fn new(source: Box<dyn ResultSetSource + 'a>) -> Self {
        Self {
            source: BoxedResultSetSource { inner: source },
        }
    }

    /// Returns the sole mutable result-set owner.
    pub fn source(&mut self) -> &mut BoxedResultSetSource<'a> {
        &mut self.source
    }
}

/// Sized adapter for a boxed worker-local result source.
pub struct BoxedResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
}

impl ResultSetSource for BoxedResultSetSource<'_> {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner.next_batch(max_rows)
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}

/// One authenticated connection's immutable session context.
#[derive(Clone, Debug)]
pub struct SessionContext {
    /// Server connection identity.
    pub connection_id: u64,
    /// Accepted peer address.
    pub peer_addr: SocketAddr,
    /// Canonical configured identity established by password verification.
    pub identity: AuthenticatedIdentity,
    /// Forced-drain carrier on which the session registers each active query.
    pub cancellation: ConnectionCancellation,
}

/// Query capability retained entirely inside one fixed worker thread.
pub trait QuerySession {
    /// Starts one sequential query and returns its lazy result owner.
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError>;
}

/// Process-owned factory invoked only after authentication, inside a worker.
pub trait QuerySessionFactory: Send + Sync + 'static {
    /// Worker-local session type. It deliberately has no `Send` bound.
    type Session: QuerySession;

    /// Opens a session from already-running process authorities.
    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError>;
}

/// Process-wide connection accounting with exactly-once owned-lease cleanup.
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    active: AtomicUsize,
    max_active: AtomicUsize,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl ConnectionTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> ConnectionLease {
        let id = self.accepted.fetch_add(1, Ordering::AcqRel) + 1;
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
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "connection count underflow");
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        if self.failed {
            self.tracker.failed.fetch_add(1, Ordering::AcqRel);
        }
    }
}

struct ConnectionWork {
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    registration: ActiveSocketRegistration,
}

struct WorkerPool {
    workers: Vec<WorkerHandle>,
    work_senders: Vec<mpsc::Sender<ConnectionWork>>,
    available_workers: mpsc::Receiver<usize>,
    available_sender: mpsc::SyncSender<usize>,
    terminal_workers: mpsc::Receiver<WorkerTerminal>,
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

struct WorkerTerminalGuard {
    index: usize,
    terminal: mpsc::Sender<WorkerTerminal>,
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

fn acquire_worker(
    available: &mpsc::Receiver<usize>,
    terminal: &mpsc::Receiver<WorkerTerminal>,
    shutdown: &ShutdownHandle,
) -> Result<Option<usize>, SqlNodeError> {
    loop {
        if shutdown.is_shutdown_requested() {
            return Ok(None);
        }
        match terminal.try_recv() {
            Ok(worker) => {
                return Err(SqlNodeError::WorkerTerminated {
                    index: worker.index,
                    panicked: worker.kind == WorkerTerminalKind::Panicked,
                })
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => return Err(SqlNodeError::WorkerQueueClosed),
        }
        match available.recv_timeout(ACCEPT_POLL_INTERVAL) {
            Ok(worker) => return Ok(Some(worker)),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(SqlNodeError::WorkerQueueClosed);
            }
        }
    }
}

#[derive(Default)]
struct ActiveSockets {
    streams: Mutex<Vec<ActiveSocket>>,
    poisoned: AtomicBool,
}

struct ActiveSocket {
    key: u64,
    stream: TcpStream,
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
        stream: TcpStream,
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

/// A loopback SQL node with fixed workers and a bounded accepted-socket queue.
pub struct ConcurrentSqlNode<F: QuerySessionFactory> {
    listener: TcpListener,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    worker_count: usize,
    shutdown: ShutdownHandle,
    shutdown_grace: Duration,
    connection_timeout: Duration,
}

impl<F: QuerySessionFactory> ConcurrentSqlNode<F> {
    /// Binds the configured loopback endpoint and retains process authorities.
    pub fn bind(
        config: &NodeConfig,
        factory: Arc<F>,
        users: Arc<ConfiguredUserStore>,
    ) -> Result<Self, SqlNodeError> {
        let listener = TcpListener::bind((config.host, config.port)).map_err(SqlNodeError::Bind)?;
        listener
            .set_nonblocking(true)
            .map_err(SqlNodeError::Listener)?;
        Ok(Self {
            listener,
            factory,
            users,
            tracker: Arc::new(ConnectionTracker::default()),
            max_allowed_packet: config.max_allowed_packet,
            worker_count: config.max_connections,
            shutdown: ShutdownHandle::default(),
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            connection_timeout: config.connection_timeout,
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
        self.run_accept_loop_with(limit, |stream, timeout| {
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
    }

    fn run_accept_loop_with<P>(
        self,
        limit: Option<usize>,
        mut prepare_stream: P,
    ) -> Result<(), SqlNodeError>
    where
        P: FnMut(&TcpStream, Duration) -> Result<(), SqlNodeError>,
    {
        let active_sockets = Arc::new(ActiveSockets::default());
        let WorkerPool {
            workers,
            work_senders: worker_senders,
            available_workers,
            available_sender,
            terminal_workers,
        } = spawn_workers(
            self.worker_count,
            &self.factory,
            &self.users,
            &self.tracker,
            self.max_allowed_packet,
        )?;

        let mut accepted = 0_usize;
        let mut next_connection_key = 1_u64;
        let accept_result = (|| loop {
            if limit == Some(accepted) || self.shutdown.is_shutdown_requested() {
                break Ok(());
            }
            let Some(worker_index) =
                acquire_worker(&available_workers, &terminal_workers, &self.shutdown)?
            else {
                break Ok(());
            };
            let (stream, peer_addr) = match self.listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    available_sender
                        .send(worker_index)
                        .map_err(|_| SqlNodeError::WorkerQueueClosed)?;
                    std::thread::sleep(ACCEPT_POLL_INTERVAL);
                    continue;
                }
                Err(error) => {
                    let _ = available_sender.send(worker_index);
                    break Err(SqlNodeError::Listener(error));
                }
            };
            prepare_stream(&stream, self.connection_timeout)?;
            let connection_key = next_connection_key;
            next_connection_key = next_connection_key.wrapping_add(1);
            if next_connection_key == 0 {
                break Err(SqlNodeError::ConnectionIdentityExhausted);
            }
            let cancellation = ConnectionCancellation::default();
            eprintln!(
                    "{{\"event\":\"connection_dispatch\",\"connection_key\":{connection_key},\"worker_index\":{worker_index}}}"
                );
            active_sockets.register(
                connection_key,
                stream.try_clone().map_err(SqlNodeError::Listener)?,
                cancellation.clone(),
            )?;
            let registration =
                ActiveSocketRegistration::new(connection_key, Arc::clone(&active_sockets));
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
            accepted += 1;
        })();

        drop(worker_senders);
        drop(available_sender);
        let drain_result =
            drain_workers(workers, &active_sockets, self.shutdown_grace, &self.tracker);
        combine_node_results(accept_result, drain_result)
    }
}

type WorkerJob = Box<dyn FnOnce() + Send + 'static>;

fn run_worker<S>(
    index: usize,
    work_receiver: mpsc::Receiver<ConnectionWork>,
    available: mpsc::SyncSender<usize>,
    terminal: mpsc::Sender<WorkerTerminal>,
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

fn spawn_workers<F: QuerySessionFactory>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
) -> Result<WorkerPool, SqlNodeError> {
    spawn_workers_with(
        count,
        factory,
        users,
        tracker,
        max_allowed_packet,
        |index, job| {
            std::thread::Builder::new()
                .name(format!("tidb-sql-connection-{index}"))
                .spawn(job)
        },
    )
}

fn spawn_workers_with<F, S>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    mut spawn: S,
) -> Result<WorkerPool, SqlNodeError>
where
    F: QuerySessionFactory,
    S: FnMut(usize, WorkerJob) -> std::io::Result<JoinHandle<()>>,
{
    let mut workers = Vec::with_capacity(count);
    let mut work_senders = Vec::with_capacity(count);
    let (available_sender, available_receiver) = mpsc::sync_channel(count);
    let (terminal_sender, terminal_receiver) = mpsc::channel();
    for index in 0..count {
        let (work_sender, work_receiver) = mpsc::channel::<ConnectionWork>();
        let factory = Arc::clone(factory);
        let users = Arc::clone(users);
        let worker_tracker = Arc::clone(tracker);
        let worker_available = available_sender.clone();
        let worker_terminal = terminal_sender.clone();
        let job: WorkerJob = Box::new(move || {
            run_worker(
                index,
                work_receiver,
                worker_available,
                worker_terminal,
                move |work| {
                    let ConnectionWork {
                        stream,
                        peer_addr,
                        cancellation,
                        registration: _registration,
                    } = work;
                    if let Err(error) = serve_mysql_connection(
                        stream,
                        peer_addr,
                        cancellation,
                        factory.as_ref(),
                        users.as_ref(),
                        &worker_tracker,
                        max_allowed_packet,
                    ) {
                        let message = error.to_string();
                        eprintln!("{{\"event\":\"connection_error\",\"error\":{message:?}}}");
                    }
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
                eprintln!(
                    "{{\"event\":\"process_shutdown_stage\",\"stage\":\"connections\",\"outcome\":\"error\",\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{},\"forced_connections\":0}}",
                    tracker.active(), tracker.accepted(), tracker.completed(), tracker.failed(),
                );
                return Err(collapse_node_failures(failures));
            }
        };
        work_senders.push(work_sender);
        workers.push(WorkerHandle {
            index,
            join: worker,
        });
    }
    drop(terminal_sender);
    Ok(WorkerPool {
        workers,
        work_senders,
        available_workers: available_receiver,
        available_sender,
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
    tracker: &ConnectionTracker,
) -> Result<(), SqlNodeError> {
    let mut failures = Vec::new();
    if let Err(error) = active_sockets.cancel_queries() {
        failures.push(error);
    }
    let deadline = Instant::now() + grace;
    loop {
        match active_sockets.len() {
            Ok(0) => break,
            Ok(_) if Instant::now() < deadline => std::thread::sleep(ACCEPT_POLL_INTERVAL),
            Ok(_) => break,
            Err(error) => {
                failures.push(error);
                break;
            }
        }
    }
    let forced = match active_sockets.len() {
        Ok(0) => 0,
        Ok(_) => {
            let (forced, error) = active_sockets.shutdown_all();
            failures.extend(error);
            forced
        }
        Err(error) => {
            failures.push(error);
            let (forced, error) = active_sockets.shutdown_all();
            failures.extend(error);
            forced
        }
    };
    failures.extend(join_worker_failures(workers));
    let outcome = if failures.is_empty() {
        "success"
    } else {
        "error"
    };
    eprintln!(
        "{{\"event\":\"process_shutdown_stage\",\"stage\":\"connections\",\"outcome\":\"{outcome}\",\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{},\"forced_connections\":{forced}}}",
        tracker.active(), tracker.accepted(), tracker.completed(), tracker.failed(),
    );
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
    /// The bounded connection identity counter wrapped.
    ConnectionIdentityExhausted,
    /// One accepted connection failed in a direct lifecycle proof.
    Connection(MysqlConnectionError),
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
            Self::ConnectionIdentityExhausted => {
                formatter.write_str("SQL connection identity space exhausted")
            }
            Self::Connection(error) => write!(formatter, "MySQL connection failed: {error}"),
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
            | Self::ConnectionIdentityExhausted
            | Self::Multiple(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_config::{ConfiguredReadColumn, ConfiguredReadColumnKind, ConfiguredReadTable};
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    struct UnusedSession;

    impl QuerySession for UnusedSession {
        fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
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
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            pd_endpoints: vec!["127.0.0.1:2379".to_owned()],
            read_tables: vec![ConfiguredReadTable {
                database: "test".to_owned(),
                table: "rows".to_owned(),
                table_id: 42,
                columns: vec![ConfiguredReadColumn {
                    name: "id".to_owned(),
                    id: 1,
                    kind: ConfiguredReadColumnKind::ClusteredPrimaryKey,
                }],
            }],
            max_allowed_packet: tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
            auth_file: PathBuf::from("unused"),
            max_connections: 2,
            connection_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn accepted_socket_setup_error_still_drains_and_joins_workers() {
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
        let error = server.join().unwrap().unwrap_err();

        assert!(matches!(error, SqlNodeError::Listener(_)));
        assert_eq!(tracker.accepted(), 1);
        assert_eq!(tracker.completed(), 1);
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
            tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
            move |index, job| {
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
    fn worker_panic_before_availability_stops_admission() {
        let (_available_tx, available_rx) = mpsc::sync_channel(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _terminal = WorkerTerminalGuard {
                index: 3,
                terminal: terminal_tx,
            };
            panic!("injected pre-availability worker panic");
        });

        assert!(matches!(
            acquire_worker(&available_rx, &terminal_rx, &ShutdownHandle::default()),
            Err(SqlNodeError::WorkerTerminated {
                index: 3,
                panicked: true,
            })
        ));
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
                server.try_clone().unwrap(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (work_tx, work_rx) = mpsc::channel();
        let (available_tx, available_rx) = mpsc::sync_channel(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            run_worker(2, work_rx, available_tx, terminal_tx, |_work| {
                panic!("injected post-admission worker panic");
            });
        });
        assert_eq!(available_rx.recv().unwrap(), 2);
        work_tx
            .send(ConnectionWork {
                stream: server,
                peer_addr,
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
        let shutdown = ShutdownHandle::default();
        shutdown.shutdown();
        shutdown.shutdown();
        shutdown.shutdown();
        let (available_tx, available_rx) = mpsc::sync_channel(1);
        available_tx.send(0).unwrap();
        let (_terminal_tx, terminal_rx) = mpsc::channel();

        assert!(matches!(
            acquire_worker(&available_rx, &terminal_rx, &shutdown),
            Ok(None)
        ));
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
                server.try_clone().unwrap(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (sender, receiver) = mpsc::channel();
        sender
            .send(ConnectionWork {
                stream: server,
                peer_addr,
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
        sockets.register(23, server, cancellation.clone()).unwrap();
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

        let error = drain_workers(
            workers,
            sockets.as_ref(),
            Duration::ZERO,
            &ConnectionTracker::default(),
        )
        .unwrap_err();

        assert!(matches!(error, SqlNodeError::WorkerStatePoisoned));
        assert!(cancellation.is_cancelled());
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).unwrap(), 0);
    }
}
