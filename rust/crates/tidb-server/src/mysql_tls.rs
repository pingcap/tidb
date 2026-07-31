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

//! Server-side TLS material for the MySQL port, and the client socket that can
//! be upgraded in place.
//!
//! Go builds this in `pkg/server/server.go`: `util.LoadTLSCertificates(
//! Security.SSLCA, SSLKey, SSLCert, Security.AutoTLS, RSAKeySize)` returns a
//! `*tls.Config` or nil, `s.capability |= mysql.ClientSSL` happens *only* when
//! that config is non-nil, and `clientConn.upgradeToTLS` wraps the same
//! connection with `tls.Server(...)` after the client's SSLRequest. Two
//! consequences are load-bearing and are preserved here:
//!
//! * `CLIENT_SSL` is advertised only when material exists. Advertising the bit
//!   without being able to complete a handshake hangs every client that asks.
//! * The upgrade happens on the *same* socket, mid-handshake, and the client
//!   then repeats a full `HandshakeResponse41` over the encrypted stream.
//!
//! Go's `LoadTLSCertificates` auto-generates a self-signed cert into the temp
//! storage path when no `ssl-cert`/`ssl-key` is configured and `auto-tls` is
//! on. `security.auto-tls` defaults to false in `pkg/config`, but the TiUP
//! playground server this node is measured against runs with it enabled --
//! which is why the Go server on the shared cluster advertises `CLIENT_SSL`
//! out of the box, and why this node's `--auto-tls` defaults to on to match
//! the server it is compared against.

use std::fs;
use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

/// Why the MySQL port could not obtain server TLS material.
#[derive(Debug)]
pub enum MysqlTlsError {
    /// A configured certificate or key file could not be read or parsed.
    Material(String),
    /// Self-signed generation failed.
    Generation(String),
}

impl std::fmt::Display for MysqlTlsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Material(detail) => write!(formatter, "TLS certificate material: {detail}"),
            Self::Generation(detail) => {
                write!(formatter, "self-signed certificate generation: {detail}")
            }
        }
    }
}

impl std::error::Error for MysqlTlsError {}

/// Accepted server TLS material for the MySQL port.
///
/// Holding one of these is what entitles the connection path to advertise
/// `CLIENT_SSL`; there is deliberately no way to advertise the bit without it.
#[derive(Clone)]
pub struct MysqlServerTls {
    config: Arc<ServerConfig>,
    /// How the material was obtained, for the startup line.
    origin: &'static str,
}

impl std::fmt::Debug for MysqlServerTls {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlServerTls")
            .field("origin", &self.origin)
            .finish()
    }
}

impl MysqlServerTls {
    /// Loads a PEM certificate chain and private key, as Go's
    /// `tls.LoadX509KeyPair(cert, key)` does.
    pub fn from_pem_files(cert: &Path, key: &Path) -> Result<Self, MysqlTlsError> {
        let certs = read_certificates(cert)?;
        let key = read_private_key(key)?;
        Self::from_material(certs, key, "configured --ssl-cert/--ssl-key")
    }

    /// Generates an in-memory self-signed certificate, mirroring Go's
    /// `createTLSCertificates` fallback under `auto-tls`.
    ///
    /// Go writes the pair to `TempStoragePath`; this node keeps it in memory
    /// because nothing else in this process re-reads it, and a file would be
    /// one more private key on disk for a certificate that lives exactly as
    /// long as the process.
    pub fn self_signed() -> Result<Self, MysqlTlsError> {
        let certified = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
        ])
        .map_err(|error| MysqlTlsError::Generation(error.to_string()))?;
        let key = PrivateKeyDer::try_from(certified.signing_key.serialize_der())
            .map_err(|error| MysqlTlsError::Generation(error.to_string()))?;
        let certificate = certified.cert.der().clone();
        Self::from_material(vec![certificate], key, "auto-generated self-signed")
    }

    fn from_material(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        origin: &'static str,
    ) -> Result<Self, MysqlTlsError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| MysqlTlsError::Material(error.to_string()))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|error| MysqlTlsError::Material(error.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            origin,
        })
    }

    /// Names how the material was obtained, for the node's startup line.
    #[must_use]
    pub const fn origin(&self) -> &'static str {
        self.origin
    }

    fn accept(&self, stream: TcpStream) -> io::Result<StreamOwned<ServerConnection, TcpStream>> {
        let connection = ServerConnection::new(Arc::clone(&self.config))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut stream = StreamOwned::new(connection, stream);
        // Drive the handshake to completion here so a TLS failure is reported
        // as a connection error rather than surfacing later as a malformed
        // MySQL packet.
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        Ok(stream)
    }
}

/// Resolves the MySQL port's TLS material from the node's options.
///
/// A configured cert/key pair wins; otherwise `auto_tls` decides between an
/// in-memory self-signed pair and no TLS at all. `Ok(None)` means the port
/// stays plaintext and `CLIENT_SSL` must not be advertised.
pub fn resolve_server_tls(
    cert: Option<&Path>,
    key: Option<&Path>,
    auto_tls: bool,
) -> Result<Option<MysqlServerTls>, MysqlTlsError> {
    match (cert, key) {
        (Some(cert), Some(key)) => MysqlServerTls::from_pem_files(cert, key).map(Some),
        (None, None) => {
            if auto_tls {
                MysqlServerTls::self_signed().map(Some)
            } else {
                Ok(None)
            }
        }
        // Go's `LoadTLSCertificates` treats a lone cert or key as "no
        // material" and silently falls through to the auto-TLS branch.
        // Refusing is the better shape: a half-configured pair is an operator
        // mistake, and serving a self-signed certificate in place of the
        // operator's own would hide it.
        (Some(_), None) => Err(MysqlTlsError::Material(
            "--ssl-cert requires --ssl-key".to_owned(),
        )),
        (None, Some(_)) => Err(MysqlTlsError::Material(
            "--ssl-key requires --ssl-cert".to_owned(),
        )),
    }
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, MysqlTlsError> {
    let file = fs::File::open(path)
        .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", path.display())))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", path.display())))?;
    if certs.is_empty() {
        return Err(MysqlTlsError::Material(format!(
            "{}: no PEM certificate found",
            path.display()
        )));
    }
    Ok(certs)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, MysqlTlsError> {
    let file = fs::File::open(path)
        .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", path.display())))?
        .ok_or_else(|| MysqlTlsError::Material(format!("{}: no private key found", path.display())))
}

/// The client socket, before or after the in-place TLS upgrade.
///
/// The MySQL handshake reads and writes the *same* connection on both sides of
/// the upgrade, so the reader and the writer must share one object once TLS is
/// established -- a `TcpStream::try_clone` pair cannot carry a TLS session.
/// `ClientStream` is therefore a cheap handle: clones share the connection, and
/// [`ClientStream::upgrade_to_tls`] swaps what is underneath for every handle
/// at once.
#[derive(Clone)]
pub struct ClientStream {
    inner: Arc<Mutex<ClientStreamInner>>,
}

enum ClientStreamInner {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
    /// Momentary state while the socket is moved out for the upgrade.
    Upgrading,
}

impl ClientStream {
    /// Wraps an accepted plaintext socket.
    #[must_use]
    pub fn plain(stream: TcpStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClientStreamInner::Plain(stream))),
        }
    }

    /// Whether TLS has been established on this connection.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        matches!(
            &*self.inner.lock().expect("client stream lock"),
            ClientStreamInner::Tls(_)
        )
    }

    /// Performs Go's `upgradeToTLS`: the same socket becomes a TLS server
    /// connection, and every later read and write goes through it.
    pub fn upgrade_to_tls(&self, tls: &MysqlServerTls) -> io::Result<()> {
        let mut guard = self.inner.lock().expect("client stream lock");
        let socket = match std::mem::replace(&mut *guard, ClientStreamInner::Upgrading) {
            ClientStreamInner::Plain(socket) => socket,
            other => {
                *guard = other;
                return Err(io::Error::other("TLS is already established"));
            }
        };
        let stream = tls.accept(socket)?;
        *guard = ClientStreamInner::Tls(Box::new(stream));
        Ok(())
    }
}

impl Read for ClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.read(buffer),
            ClientStreamInner::Tls(stream) => stream.read(buffer),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.write(buffer),
            ClientStreamInner::Tls(stream) => stream.write(buffer),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.flush(),
            ClientStreamInner::Tls(stream) => stream.flush(),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }
}
