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

//! The standalone listener lifecycle for the Rust SQL node.
//!
//! This module is intentionally a small ownership boundary.  It mirrors the
//! useful part of TiDB's `InitTiDBListener`, `startShutdown`, and
//! `closeListener` flow: bind a configured TCP endpoint exactly once, publish
//! the address selected by the operating system (including an ephemeral test
//! port), mark the node unhealthy before shutdown, and close the socket
//! idempotently.  Authentication, TLS, compression, PROXY protocol handling,
//! Unix-socket credential lookup, and connection/session acceptance belong to
//! later server layers; keeping them out here prevents this lifecycle leaf
//! from silently becoming a second protocol implementation.

use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};

/// Configuration for the TCP endpoint owned by [`ListenerLifecycle`].
///
/// `None` for either endpoint component means that this instance has no TCP
/// endpoint.  A port of zero is valid when a host is supplied: the operating
/// system chooses an ephemeral port, which is the source-shaped equivalent of
/// TiDB's Go-test listener configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListenerConfig {
    /// Hostname or IP address to bind.
    pub host: Option<String>,
    /// TCP port to bind.  Zero requests an ephemeral port.
    pub port: Option<u16>,
    /// Restrict address resolution to IPv4 addresses.
    pub tcp4_only: bool,
}

impl ListenerConfig {
    /// Creates a TCP configuration.
    #[must_use]
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: Some(host.into()),
            port: Some(port),
            tcp4_only: false,
        }
    }

    /// Creates a TCP configuration with IPv4-only resolution.
    #[must_use]
    pub fn tcp4(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: Some(host.into()),
            port: Some(port),
            tcp4_only: true,
        }
    }

    /// Creates an intentionally unconfigured listener.
    #[must_use]
    pub const fn no_endpoint() -> Self {
        Self {
            host: None,
            port: None,
            tcp4_only: false,
        }
    }
}

/// Lifecycle states corresponding to the server's bind, activation, and
/// shutdown phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerState {
    /// No socket has been bound yet.
    New,
    /// A socket is bound but the server has not reported itself healthy.
    Bound,
    /// The listener is bound and the server has entered its active phase.
    Active,
    /// Shutdown has started; the listener is no longer healthy.
    ShuttingDown,
    /// The listener has been closed permanently.
    Closed,
}

/// Errors returned by listener lifecycle transitions.
#[derive(Debug)]
pub enum ListenerError {
    /// No TCP endpoint was configured.
    NoEndpoint,
    /// The operating system rejected address resolution or socket binding.
    Bind(io::Error),
    /// The requested operation is not valid in the current state.
    InvalidTransition {
        /// Operation requested by the caller.
        operation: &'static str,
        /// Current lifecycle state.
        state: ListenerState,
    },
}

impl fmt::Display for ListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEndpoint => formatter
                .write_str("Server not configured to listen on either -socket or -host and -port"),
            Self::Bind(error) => write!(formatter, "failed to bind TCP listener: {error}"),
            Self::InvalidTransition { operation, state } => {
                write!(formatter, "cannot {operation} listener in {state:?} state")
            }
        }
    }
}

impl std::error::Error for ListenerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) => Some(error),
            Self::NoEndpoint | Self::InvalidTransition { .. } => None,
        }
    }
}

/// Owns one TCP listener and its shutdown flags.
///
/// The type is deliberately single-threaded (`&mut self` transitions).  The
/// eventual server can place it behind its connection/lifecycle coordinator;
/// this leaf does not make assumptions about authentication, accept loops, or
/// a runtime executor.
#[derive(Debug)]
pub struct ListenerLifecycle {
    config: ListenerConfig,
    listener: Option<TcpListener>,
    state: ListenerState,
    healthy: bool,
    in_shutdown: bool,
    force_shutdown: bool,
    need_request_manager_free: bool,
}

impl ListenerLifecycle {
    /// Creates an unbound lifecycle with the supplied endpoint configuration.
    #[must_use]
    pub fn new(config: ListenerConfig) -> Self {
        Self {
            config,
            listener: None,
            state: ListenerState::New,
            healthy: false,
            in_shutdown: false,
            force_shutdown: false,
            need_request_manager_free: false,
        }
    }

    /// Returns the configured endpoint.
    #[must_use]
    pub const fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> ListenerState {
        self.state
    }

    /// Binds the configured TCP endpoint.
    ///
    /// Calling this more than once after a successful bind is idempotent and
    /// returns the same operating-system-selected address.  A listener that
    /// has begun shutdown cannot be rebound; constructing a new lifecycle is
    /// the explicit restart boundary.
    pub fn bind(&mut self) -> Result<SocketAddr, ListenerError> {
        match self.state {
            ListenerState::Bound | ListenerState::Active => {
                return self.bound_address().ok_or_else(|| {
                    ListenerError::Bind(io::Error::other("bound listener lost its local address"))
                });
            }
            ListenerState::New => {}
            ListenerState::ShuttingDown | ListenerState::Closed => {
                return Err(ListenerError::InvalidTransition {
                    operation: "bind",
                    state: self.state,
                });
            }
        }

        let listener = self.bind_tcp()?;
        let address = listener.local_addr().map_err(ListenerError::Bind)?;
        self.listener = Some(listener);
        self.state = ListenerState::Bound;
        Ok(address)
    }

    /// Marks a successfully bound listener active and healthy.
    ///
    /// Activation is idempotent.  It is separate from [`Self::bind`] so a
    /// server can finish listener setup before advertising health, matching
    /// TiDB's `Run` ordering.
    pub fn activate(&mut self) -> Result<(), ListenerError> {
        match self.state {
            ListenerState::Bound => {
                self.state = ListenerState::Active;
                self.healthy = true;
                Ok(())
            }
            ListenerState::Active => Ok(()),
            state => Err(ListenerError::InvalidTransition {
                operation: "activate",
                state,
            }),
        }
    }

    /// Begins shutdown and marks the server unhealthy before closing.
    pub fn begin_shutdown(&mut self) {
        self.healthy = false;
        self.in_shutdown = true;
        if matches!(
            self.state,
            ListenerState::New | ListenerState::Bound | ListenerState::Active
        ) {
            self.state = ListenerState::ShuttingDown;
        }
    }

    /// Closes the owned socket and completes shutdown.  Repeated calls are
    /// harmless, just like TiDB's `closeListener` path.
    pub fn close(&mut self) {
        self.begin_shutdown();
        self.listener.take();
        self.state = ListenerState::Closed;
    }

    /// Returns the bound address, if a socket is currently owned.
    #[must_use]
    pub fn bound_address(&self) -> Option<SocketAddr> {
        self.listener
            .as_ref()
            .and_then(|listener| listener.local_addr().ok())
    }

    /// Borrows the underlying TCP listener for an outer accept loop.
    ///
    /// This is intentionally only a borrow: protocol framing, authentication,
    /// and session dispatch remain outside the lifecycle leaf.
    #[must_use]
    pub const fn tcp_listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// Returns whether the node is currently reporting healthy.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }

    /// Returns whether shutdown has started.
    #[must_use]
    pub const fn is_in_shutdown(&self) -> bool {
        self.in_shutdown
    }

    /// Sets the force-shutdown flag.
    pub const fn set_force_shutdown(&mut self) {
        self.force_shutdown = true;
    }

    /// Returns the force-shutdown flag.
    #[must_use]
    pub const fn force_shutdown(&self) -> bool {
        self.force_shutdown
    }

    /// Sets the request-manager cleanup flag.
    pub const fn set_need_request_manager_free(&mut self) {
        self.need_request_manager_free = true;
    }

    /// Returns the request-manager cleanup flag.
    #[must_use]
    pub const fn need_request_manager_free(&self) -> bool {
        self.need_request_manager_free
    }

    fn bind_tcp(&self) -> Result<TcpListener, ListenerError> {
        let host = self
            .config
            .host
            .as_deref()
            .filter(|host| !host.trim().is_empty())
            .ok_or(ListenerError::NoEndpoint)?;
        let port = self.config.port.ok_or(ListenerError::NoEndpoint)?;
        let addresses = (host, port)
            .to_socket_addrs()
            .map_err(ListenerError::Bind)?
            .filter(|address| !self.config.tcp4_only || address.is_ipv4())
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(ListenerError::Bind(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "configured host did not resolve to a usable TCP address",
            )));
        }

        let mut last_error = None;
        for address in addresses {
            match TcpListener::bind(address) {
                Ok(listener) => return Ok(listener),
                Err(error) => last_error = Some(error),
            }
        }
        Err(ListenerError::Bind(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "unable to bind TCP listener",
            )
        })))
    }
}

impl Drop for ListenerLifecycle {
    fn drop(&mut self) {
        self.listener.take();
    }
}
