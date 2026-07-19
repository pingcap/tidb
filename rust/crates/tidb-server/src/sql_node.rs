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
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

use crate::configured_user_store::{AuthenticatedIdentity, ConfiguredUserStore};
use crate::mysql_connection::{serve_mysql_connection, MysqlConnectionError};
use crate::node_config::NodeConfig;
use crate::resultset_source::ResultSetSource;

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionContext {
    /// Server connection identity.
    pub connection_id: u64,
    /// Accepted peer address.
    pub peer_addr: SocketAddr,
    /// Canonical configured identity established by password verification.
    pub identity: AuthenticatedIdentity,
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
}

/// A loopback SQL node with fixed workers and a bounded accepted-socket queue.
pub struct ConcurrentSqlNode<F: QuerySessionFactory> {
    listener: TcpListener,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    worker_count: usize,
}

impl<F: QuerySessionFactory> ConcurrentSqlNode<F> {
    /// Binds the configured loopback endpoint and retains process authorities.
    pub fn bind(
        config: &NodeConfig,
        factory: Arc<F>,
        users: Arc<ConfiguredUserStore>,
    ) -> Result<Self, SqlNodeError> {
        let listener = TcpListener::bind((config.host, config.port)).map_err(SqlNodeError::Bind)?;
        Ok(Self {
            listener,
            factory,
            users,
            tracker: Arc::new(ConnectionTracker::default()),
            max_allowed_packet: config.max_allowed_packet,
            worker_count: config.max_connections,
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
        let (sender, receiver) = mpsc::sync_channel::<ConnectionWork>(self.worker_count);
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = spawn_workers(
            self.worker_count,
            receiver,
            &self.factory,
            &self.users,
            &self.tracker,
            self.max_allowed_packet,
        )?;

        let mut accepted = 0_usize;
        let accept_result = loop {
            if limit == Some(accepted) {
                break Ok(());
            }
            let (stream, peer_addr) = match self.listener.accept() {
                Ok(connection) => connection,
                Err(error) => break Err(SqlNodeError::Listener(error)),
            };
            if sender.send(ConnectionWork { stream, peer_addr }).is_err() {
                break Err(SqlNodeError::WorkerQueueClosed);
            }
            accepted += 1;
        };

        drop(sender);
        let join_result = join_workers(workers);
        accept_result.and(join_result)
    }
}

fn spawn_workers<F: QuerySessionFactory>(
    count: usize,
    receiver: Arc<Mutex<mpsc::Receiver<ConnectionWork>>>,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    max_allowed_packet: usize,
) -> Result<Vec<JoinHandle<()>>, SqlNodeError> {
    let mut workers = Vec::with_capacity(count);
    for index in 0..count {
        let receiver = Arc::clone(&receiver);
        let factory = Arc::clone(factory);
        let users = Arc::clone(users);
        let tracker = Arc::clone(tracker);
        let worker = std::thread::Builder::new()
            .name(format!("tidb-sql-connection-{index}"))
            .spawn(move || loop {
                let work = {
                    let Ok(receiver) = receiver.lock() else {
                        return;
                    };
                    receiver.recv()
                };
                let Ok(work) = work else {
                    return;
                };
                if let Err(error) = serve_mysql_connection(
                    work.stream,
                    work.peer_addr,
                    factory.as_ref(),
                    users.as_ref(),
                    &tracker,
                    max_allowed_packet,
                ) {
                    eprintln!("{{\"event\":\"connection_error\",\"error\":{error:?}}}");
                }
            })
            .map_err(SqlNodeError::WorkerSpawn)?;
        workers.push(worker);
    }
    Ok(workers)
}

fn join_workers(workers: Vec<JoinHandle<()>>) -> Result<(), SqlNodeError> {
    for worker in workers {
        worker.join().map_err(|_| SqlNodeError::WorkerPanicked)?;
    }
    Ok(())
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
    /// A fixed worker panicked during an orderly test drain.
    WorkerPanicked,
    /// One accepted connection failed in a direct lifecycle proof.
    Connection(MysqlConnectionError),
}

impl fmt::Display for SqlNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(error) => write!(formatter, "failed to bind SQL listener: {error}"),
            Self::Listener(error) => write!(formatter, "SQL listener failed: {error}"),
            Self::WorkerSpawn(error) => write!(formatter, "failed to spawn SQL worker: {error}"),
            Self::WorkerQueueClosed => formatter.write_str("SQL worker queue closed"),
            Self::WorkerPanicked => formatter.write_str("SQL worker panicked"),
            Self::Connection(error) => write!(formatter, "MySQL connection failed: {error}"),
        }
    }
}

impl std::error::Error for SqlNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) | Self::Listener(error) | Self::WorkerSpawn(error) => Some(error),
            Self::Connection(error) => Some(error),
            Self::WorkerQueueClosed | Self::WorkerPanicked => None,
        }
    }
}
