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

//! Serial listener and injected query-engine boundary for the first SQL node.
//!
//! Campaign 18's production read transport is deliberately single-threaded.
//! This owner therefore serves one accepted connection at a time instead of
//! hiding `Rc`-owned routing state behind an unsafe or duplicate concurrency
//! layer. The listener can become concurrent only after the transport itself
//! has one source-backed shared ownership model.

use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::mysql_connection::{serve_mysql_connection, ConnectionReport, MysqlConnectionError};
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

/// A query result whose lazy source remains borrowed from the serial engine.
pub struct SerialQueryResult<'a> {
    source: BoxedResultSetSource<'a>,
}

impl<'a> SerialQueryResult<'a> {
    /// Transfers one engine-owned result source to the connection writer.
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

/// Sized adapter allowing the generic incremental writer to consume a boxed
/// engine-owned source without weakening its compile-time source contract.
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

/// Injected serial query capability consumed by the MySQL connection owner.
///
/// The real adapter is implemented in `tidb-server`, where this local trait
/// may wrap `tidb_exec::RealTiKvReadEngine` without reversing the dependency
/// from execution into server protocol code.
pub trait SerialQueryEngine {
    /// Parses, lowers, and starts one read-only query without materializing all
    /// rows. The returned source must finish and close before another query is
    /// admitted on this engine.
    fn execute<'a>(&'a mut self, sql: &str) -> Result<SerialQueryResult<'a>, SqlQueryError>;
}

/// Process-wide connection accounting with exactly-once Drop cleanup.
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    active: AtomicUsize,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl ConnectionTracker {
    pub(crate) fn begin(&self) -> ConnectionLease<'_> {
        let id = self.accepted.fetch_add(1, Ordering::AcqRel) + 1;
        self.active.fetch_add(1, Ordering::AcqRel);
        ConnectionLease {
            tracker: self,
            id,
            failed: false,
        }
    }

    /// Number of accepted connections currently inside their lifecycle.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
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

pub(crate) struct ConnectionLease<'a> {
    tracker: &'a ConnectionTracker,
    id: u64,
    failed: bool,
}

impl ConnectionLease<'_> {
    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) const fn mark_failed(&mut self) {
        self.failed = true;
    }
}

impl Drop for ConnectionLease<'_> {
    fn drop(&mut self) {
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "connection count underflow");
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        if self.failed {
            self.tracker.failed.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// A bound serial node retaining one production query engine.
pub struct SerialSqlNode<E> {
    listener: TcpListener,
    engine: E,
    tracker: ConnectionTracker,
    max_allowed_packet: usize,
}

impl<E: SerialQueryEngine> SerialSqlNode<E> {
    /// Binds the configured loopback endpoint and retains the injected engine.
    pub fn bind(config: &NodeConfig, engine: E) -> Result<Self, SqlNodeError> {
        let listener = TcpListener::bind((config.host, config.port)).map_err(SqlNodeError::Bind)?;
        Ok(Self {
            listener,
            engine,
            tracker: ConnectionTracker::default(),
            max_allowed_packet: config.max_allowed_packet,
        })
    }

    /// Returns the operating-system-selected listener address.
    pub fn local_addr(&self) -> Result<SocketAddr, SqlNodeError> {
        self.listener.local_addr().map_err(SqlNodeError::Listener)
    }

    /// Borrows exact connection lifecycle counters.
    #[must_use]
    pub const fn tracker(&self) -> &ConnectionTracker {
        &self.tracker
    }

    /// Accepts and completely serves one connection before accepting another.
    pub fn serve_next(&mut self) -> Result<ConnectionReport, SqlNodeError> {
        let (stream, _) = self.listener.accept().map_err(SqlNodeError::Listener)?;
        serve_mysql_connection(
            stream,
            &mut self.engine,
            &self.tracker,
            self.max_allowed_packet,
        )
        .map_err(SqlNodeError::Connection)
    }

    /// Runs the serial accept loop. A malformed client cannot terminate the
    /// listener; only an accept failure ends the process loop.
    pub fn run(&mut self) -> Result<(), SqlNodeError> {
        loop {
            match self.serve_next() {
                Ok(_) | Err(SqlNodeError::Connection(_)) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

/// Startup or runtime failure from [`SerialSqlNode`].
#[derive(Debug)]
pub enum SqlNodeError {
    /// The configured address could not be bound.
    Bind(std::io::Error),
    /// The active listener failed.
    Listener(std::io::Error),
    /// One accepted connection failed; the process loop isolates this case.
    Connection(MysqlConnectionError),
}

impl fmt::Display for SqlNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(error) => write!(formatter, "failed to bind SQL listener: {error}"),
            Self::Listener(error) => write!(formatter, "SQL listener failed: {error}"),
            Self::Connection(error) => write!(formatter, "MySQL connection failed: {error}"),
        }
    }
}

impl std::error::Error for SqlNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) | Self::Listener(error) => Some(error),
            Self::Connection(error) => Some(error),
        }
    }
}
