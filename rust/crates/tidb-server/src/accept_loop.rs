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

//! Dependency-closed ownership of TiDB's network accept loop.
//!
//! Go's `Server.startNetworkListener` owns a small but important boundary:
//! repeatedly accept a connection, hand it to the connection owner, and
//! report either listener or connection-handler failure to the server.  A
//! closed listener is only a normal exit after shutdown has been requested;
//! an unexpected listener failure must remain visible to the caller.
//!
//! This leaf deliberately does not parse MySQL packets or implement
//! authentication, TLS, compression, PROXY protocol, Unix-socket credentials,
//! or connection admission checks.  Those behaviors have separate owners in
//! the rewrite.  The injected listener and handler make those boundaries
//! explicit while keeping this loop usable with `std::net::TcpListener` and
//! deterministic test doubles.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// An object that can accept one owned connection at a time.
///
/// The associated connection type intentionally remains generic.  The TCP
/// implementation returns the same `(TcpStream, SocketAddr)` pair as the
/// standard library, while tests and future transports can inject a smaller
/// source-shaped value without coupling this loop to protocol/session code.
pub trait AcceptListener {
    /// Accepted connection value handed to the connection handler.
    type Connection;
    /// Failure returned by the underlying listener.
    type Error;

    /// Accepts one connection.
    fn accept(&mut self) -> Result<Self::Connection, Self::Error>;
}

impl AcceptListener for std::net::TcpListener {
    type Connection = (std::net::TcpStream, std::net::SocketAddr);
    type Error = std::io::Error;

    fn accept(&mut self) -> Result<Self::Connection, Self::Error> {
        std::net::TcpListener::accept(self)
    }
}

/// A cloneable shutdown signal shared by an accept loop and its server owner.
///
/// Requesting shutdown does not fabricate a listener error.  The loop checks
/// the signal before each accept and treats an error observed after the signal
/// as the normal [`AcceptLoopExit::Shutdown`] path, matching Go's
/// `inShutdownMode` handling for a closed network connection.
#[derive(Clone, Debug, Default)]
pub struct ShutdownHandle {
    requested: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Requests graceful loop shutdown.  Repeated requests are idempotent.
    pub fn shutdown(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Returns whether shutdown was requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// Why an accept loop stopped without an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptLoopExit {
    /// The server was configured without a listener (the source nil-listener
    /// branch reports completion rather than an error).
    NoListener,
    /// Shutdown was requested before the next accept or while handling a
    /// listener error.
    Shutdown,
}

/// An error returned by the accept loop.
#[derive(Debug)]
pub enum AcceptLoopError<ListenerError, HandlerError> {
    /// The listener failed unexpectedly.  This is never hidden as a clean
    /// shutdown unless the shutdown signal had already been requested.
    Listener(ListenerError),
    /// The connection handler rejected an accepted connection.
    Handler(HandlerError),
}

/// Owns one injected listener and the shutdown state for its accept loop.
///
/// `AcceptLoop` does not spawn a thread.  The eventual server decides whether
/// to run this method on its runtime, and can retain a clone of
/// [`Self::shutdown_handle`] for an external shutdown coordinator.  A handler
/// may also request shutdown after processing a connection, which is useful
/// for bounded tests and orderly server drains.
#[derive(Debug)]
pub struct AcceptLoop<L> {
    listener: Option<L>,
    shutdown: ShutdownHandle,
}

impl<L> AcceptLoop<L> {
    /// Creates an accept loop around an optional listener.
    #[must_use]
    pub fn new(listener: Option<L>) -> Self {
        Self {
            listener,
            shutdown: ShutdownHandle::default(),
        }
    }

    /// Returns a cloneable handle that can request graceful shutdown.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    /// Requests graceful shutdown for this loop.
    pub fn shutdown(&self) {
        self.shutdown.shutdown();
    }

    /// Returns whether this loop has been asked to stop.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown.is_shutdown_requested()
    }

    /// Borrows the injected listener, if one was configured.
    #[must_use]
    pub const fn listener(&self) -> Option<&L> {
        self.listener.as_ref()
    }
}

impl<L: AcceptListener> AcceptLoop<L> {
    /// Runs the accept/dispatch loop until shutdown or an error.
    ///
    /// The handler is invoked once for each accepted connection.  Returning a
    /// handler error stops the loop and preserves that error for the caller;
    /// no connection error is silently logged or converted to success.  A
    /// listener error is likewise propagated unless shutdown has already been
    /// requested, in which case it is the normal shutdown exit.
    pub fn run<H, HandlerError>(
        &mut self,
        mut handler: H,
    ) -> Result<AcceptLoopExit, AcceptLoopError<L::Error, HandlerError>>
    where
        H: FnMut(L::Connection) -> Result<(), HandlerError>,
    {
        let Some(listener) = self.listener.as_mut() else {
            return Ok(AcceptLoopExit::NoListener);
        };

        loop {
            if self.shutdown.is_shutdown_requested() {
                return Ok(AcceptLoopExit::Shutdown);
            }

            let connection = match listener.accept() {
                Ok(connection) => connection,
                Err(error) => {
                    if self.shutdown.is_shutdown_requested() {
                        return Ok(AcceptLoopExit::Shutdown);
                    }
                    return Err(AcceptLoopError::Listener(error));
                }
            };

            handler(connection).map_err(AcceptLoopError::Handler)?;
        }
    }
}
