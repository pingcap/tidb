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
//! on. `security.auto-tls` defaults to false, as in Go's shared configuration.
//! A deployment that wants generated certificates enables it explicitly.

use std::fs;
use std::io::{self, BufReader, IoSlice, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    certificates: Vec<CertificateDer<'static>>,
    key: Arc<PrivateKeyDer<'static>>,
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
        Self::from_material_with_policy(certs, key, origin, None, "")
    }

    fn from_material_with_policy(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        origin: &'static str,
        ca: Option<&Path>,
        min_version: &str,
    ) -> Result<Self, MysqlTlsError> {
        if !matches!(min_version, "" | "TLSv1.2" | "TLSv1.3") {
            eprintln!("Invalid TLS version {min_version:?}, using TLSv1.2 minimum");
        }
        let versions: &[&'static rustls::SupportedProtocolVersion] = if min_version == "TLSv1.3" {
            &[&rustls::version::TLS13]
        } else {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        };
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(versions)
            .map_err(|error| MysqlTlsError::Material(error.to_string()))?;
        let builder = if let Some(ca) = ca {
            let file = fs::File::open(ca)
                .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", ca.display())))?;
            let mut roots = rustls::RootCertStore::empty();
            // Go AppendCertsFromPEM keeps valid certificates and ignores invalid
            // blocks; an empty pool leaves client verification disabled.
            for cert in rustls_pemfile::certs(&mut BufReader::new(file)).flatten() {
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                builder.with_no_client_auth()
            } else {
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                );
                let verifier = if tidb_util::tls::REQUIRE_SECURE_TRANSPORT
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    verifier
                } else {
                    verifier.allow_unauthenticated()
                };
                builder.with_client_cert_verifier(
                    verifier
                        .build()
                        .map_err(|error| MysqlTlsError::Material(error.to_string()))?,
                )
            }
        } else {
            builder.with_no_client_auth()
        };
        let config = builder
            .with_single_cert(certs.clone(), key.clone_key())
            .map_err(|error| MysqlTlsError::Material(error.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            certificates: certs,
            key: Arc::new(key),
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
        (Some(_), None) | (None, Some(_)) => {
            if auto_tls {
                MysqlServerTls::self_signed().map(Some)
            } else {
                Ok(None)
            }
        }
    }
}

/// Resolves configured inbound CA and minimum protocol together with material.
pub fn resolve_server_tls_with_policy(
    cert: Option<&Path>,
    key: Option<&Path>,
    auto_tls: bool,
    ca: Option<&Path>,
    min_version: &str,
) -> Result<Option<MysqlServerTls>, MysqlTlsError> {
    let Some(tls) = resolve_server_tls(cert, key, auto_tls)? else {
        return Ok(None);
    };
    MysqlServerTls::from_material_with_policy(
        tls.certificates,
        tls.key.clone_key(),
        tls.origin,
        ca,
        min_version,
    )
    .map(Some)
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

// Go crypto/x509 accepts Latin-1 T61 and UCS-2 BMP names as well as UTF-8.
fn x509_name_value(attr: &x509_parser::x509::AttributeTypeAndValue<'_>) -> Option<String> {
    let value = attr.as_slice();
    match attr.attr_value().tag().0 {
        20 => Some(value.iter().map(|b| char::from(*b)).collect()),
        30 => {
            if value.len() % 2 != 0 {
                return None;
            }
            let value = value.strip_suffix(&[0, 0]).unwrap_or(value);
            let mut result = String::new();
            for pair in value.chunks_exact(2) {
                let point = u16::from_be_bytes([pair[0], pair[1]]);
                if matches!(point, 0xfffe | 0xffff | 0xfdd0..=0xfdef | 0xd800..=0xdfff) {
                    return None;
                }
                result.push(char::from_u32(u32::from(point))?);
            }
            Some(result)
        }
        _ => attr.as_str().ok().map(str::to_owned),
    }
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

    /// A completed handshake with peer certs under a configured verifier.
    /// rustls exposes peers only after its verifier accepted their chain.
    pub fn has_verified_client_certificate(&self) -> bool {
        match &*self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Tls(stream) => stream
                .conn
                .peer_certificates()
                .is_some_and(|chain| !chain.is_empty()),
            _ => false,
        }
    }

    /// Leaf properties are usable only after the configured verifier accepted
    /// the handshake. Certificate parsing never substitutes for chain verification.
    pub(crate) fn verified_tls_peer(&self) -> Option<tidb_session::privilege::TlsPeerIdentity> {
        use x509_parser::extensions::GeneralName;
        let guard = self.inner.lock().expect("client stream lock");
        let ClientStreamInner::Tls(stream) = &*guard else {
            return None;
        };
        if stream.conn.is_handshaking() {
            return None;
        }
        let cert = stream.conn.peer_certificates()?.first()?;
        let (_, cert) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
        let name = |name: &x509_parser::x509::X509Name<'_>| {
            let mut result = String::new();
            for attr in name.iter_attributes() {
                let key = match attr.attr_type().to_id_string().as_str() {
                    "2.5.4.6" => "C",
                    "2.5.4.10" => "O",
                    "2.5.4.11" => "OU",
                    "2.5.4.3" => "CN",
                    "2.5.4.5" => "SERIALNUMBER",
                    "2.5.4.7" => "L",
                    "2.5.4.8" => "ST",
                    "2.5.4.9" => "STREET",
                    "2.5.4.17" => "POSTALCODE",
                    "1.2.840.113549.1.9.1" => "emailAddress",
                    _ => continue,
                };
                let value = x509_name_value(attr)?;
                result.push('/');
                result.push_str(key);
                result.push('=');
                result.push_str(&value);
            }
            Some(result)
        };
        let mut sans = std::collections::BTreeMap::<String, Vec<String>>::new();
        if let Some(extension) = cert.subject_alternative_name().ok()? {
            for value in &extension.value.general_names {
                let (kind, value) = match value {
                    GeneralName::URI(value) => {
                        ("URI", tidb_session::privilege::certificate_uri(value)?)
                    }
                    GeneralName::DNSName(value) => ("DNS", (*value).to_owned()),
                    GeneralName::IPAddress(value) if value.len() == 4 => (
                        "IP",
                        std::net::Ipv4Addr::from(<[u8; 4]>::try_from(*value).ok()?).to_string(),
                    ),
                    GeneralName::IPAddress(value) if value.len() == 16 => ("IP", {
                        let address = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(*value).ok()?);
                        address
                            .to_ipv4_mapped()
                            .map_or_else(|| address.to_string(), |ip| ip.to_string())
                    }),
                    _ => continue,
                };
                sans.entry(kind.into()).or_default().push(value);
            }
        }
        Some(tidb_session::privilege::TlsPeerIdentity {
            cipher: tidb_util::tls::cipher_suite_name(u16::from(
                stream.conn.negotiated_cipher_suite()?.suite(),
            ))
            .into(),
            issuer: name(cert.issuer())?,
            subject: name(cert.subject())?,
            sans,
        })
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

    /// The negotiated `(cipher_suite, protocol_version)` wire identifiers of
    /// this connection's TLS session, `None` before an upgrade. Go reads the
    /// same pair off `tls.ConnectionState` for the `Ssl_cipher` and
    /// `Ssl_version` status variables (`server.go:1329`).
    pub fn negotiated_tls(&self) -> Option<(u16, u16)> {
        match &*self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Tls(stream) => {
                let cipher = stream
                    .conn
                    .negotiated_cipher_suite()
                    .map(|suite| u16::from(suite.suite()))?;
                let version = stream.conn.protocol_version().map(u16::from)?;
                Some((cipher, version))
            }
            _ => None,
        }
    }

    /// Applies the session's command read timeout to the underlying socket.
    ///
    /// TLS changes the byte codec, not the socket authority. Updating the
    /// `TcpStream` inside `StreamOwned` keeps `SET wait_timeout` effective on
    /// the next command in both transport modes.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match &*self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.set_read_timeout(timeout),
            ClientStreamInner::Tls(stream) => stream.sock.set_read_timeout(timeout),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
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

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.write_vectored(buffers),
            ClientStreamInner::Tls(stream) => {
                // StreamOwned does not forward vectored writes. Borrow its
                // existing connection/socket so TLS accepts both frame parts
                // before completing I/O, without introducing another owner.
                let stream = stream.as_mut();
                rustls::Stream::new(&mut stream.conn, &mut stream.sock).write_vectored(buffers)
            }
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

#[cfg(test)]
mod tiflash_cluster_http_tests {
    use super::*;
    use std::io::BufRead;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use tidb_exec::tiflash_replica_manager::{TiFlashReplicaControl, TiFlashReplicaManager};

    struct Owner;
    impl TiFlashReplicaControl for Owner {
        fn is_owner(&self) -> bool {
            true
        }
        fn update_replica_status(&self, _: i64, _: bool) -> Result<(), String> {
            Ok(())
        }
    }

    fn exercise_tls_discovery(trust_server: bool) {
        let certificate = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
        ])
        .unwrap();
        let wrong = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let ca_path = std::env::temp_dir().join(format!(
            "tidb-tiflash-ca-{}-{}-{}.pem",
            std::process::id(),
            trust_server,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &ca_path,
            if trust_server {
                certificate.cert.pem()
            } else {
                wrong.cert.pem()
            },
        )
        .unwrap();
        let key = PrivateKeyDer::try_from(certificate.signing_key.serialize_der()).unwrap();
        let tls = MysqlServerTls::from_material(
            vec![certificate.cert.der().clone()],
            key,
            "test cluster server",
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, received) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            match tls.accept(socket) {
                Ok(mut stream) => {
                    let mut request = String::new();
                    let mut reader = BufReader::new(&mut stream);
                    reader.read_line(&mut request).unwrap();
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                            break;
                        }
                    }
                    let body = "{\"stores\":[]}";
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                    stream.flush().unwrap();
                    sent.send(Some(request)).unwrap();
                }
                Err(_) => {
                    sent.send(None).unwrap();
                }
            }
        });
        let security = tidb_pd_client::ClusterSecurity::new(
            ca_path.to_string_lossy().into_owned(),
            String::new(),
            String::new(),
            vec![],
        );
        let catalog = Arc::new(tidb_exec::catalog_watch::SharedCatalog::new(
            tidb_exec::cluster_catalog::ClusterCatalog {
                schema_version: 1,
                databases: vec![],
            },
        ));
        let manager = TiFlashReplicaManager::with_pd_http_security(
            catalog,
            Arc::new(Owner),
            format!("http://{address}"),
            &security,
        )
        .unwrap();
        let mut poller = manager.spawn();
        let request = received
            .recv_timeout(Duration::from_secs(10))
            .expect("discovery reaches the TLS peer");
        poller.shutdown();
        worker.join().unwrap();
        fs::remove_file(ca_path).unwrap();
        if trust_server {
            assert!(request.unwrap().starts_with("GET /pd/api/v1/stores "));
        } else {
            assert!(
                request.is_none(),
                "untrusted certificates must never receive an HTTP request"
            );
        }
    }

    #[test]
    fn tiflash_batch_cluster_http_uses_configured_ca_and_https() {
        exercise_tls_discovery(true);
    }

    #[test]
    fn tiflash_batch_cluster_http_rejects_an_untrusted_peer() {
        exercise_tls_discovery(false);
    }
}

#[cfg(test)]
mod account_tls_batch_tests {
    use super::*;

    #[test]
    fn account_tls_batch_partial_pair_uses_go_auto_tls_fallback() {
        assert!(
            resolve_server_tls(Some(Path::new("unused.pem")), None, false)
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_server_tls(None, Some(Path::new("unused.pem")), true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn account_tls_batch_configured_ca_is_loaded_before_serving() {
        let result = resolve_server_tls_with_policy(
            None,
            None,
            true,
            Some(Path::new("/nonexistent-tidb-ca.pem")),
            "TLSv1.3",
        );
        assert!(
            result.is_err(),
            "CA read failure must prevent configured TLS startup"
        );
    }
}

#[cfg(test)]
mod account_tls_batch_handshake_tests {
    use super::*;
    use crate::configured_user_store::ConfiguredUserStore;
    use crate::secure_transport::TransportKind;
    use std::net::TcpListener;
    use tidb_session::privilege::{PrivilegeRegistry, SslType};

    // Real TLS plus the exact transport-to-account handoff used by wire auth.
    fn handshake(client_kind: u8, tls12_only: bool, minimum: &str) -> (bool, bool, bool) {
        handshake_policy(client_kind, tls12_only, minimum, None)
    }

    fn handshake_policy(
        client_kind: u8,
        tls12_only: bool,
        minimum: &str,
        policy: Option<&str>,
    ) -> (bool, bool, bool) {
        let policy = policy.map(str::to_owned);
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tidb-pd-client/testdata/tls");
        let tls = resolve_server_tls_with_policy(
            Some(&dir.join("server.crt")),
            Some(&dir.join("server.key")),
            false,
            Some(&dir.join("ca.crt")),
            minimum,
        )
        .unwrap()
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut stream = ClientStream::plain(socket);
            if stream.upgrade_to_tls(&tls).is_err() {
                return (false, false, false);
            }
            let verified = stream.has_verified_client_certificate();
            let registry = PrivilegeRegistry::default();
            registry.set_ssl_type("root", "%", SslType::X509);
            if let Some(policy) = policy {
                registry.load_global_priv(vec![
                    tidb_exec::cluster_privilege_load::LoadedGlobalPriv {
                        user: "root".into(),
                        host: "%".into(),
                        priv_json: policy,
                    },
                ]);
            }
            let users = ConfiguredUserStore::from_accounts(registry);
            let admission = users
                .admit_transport(TransportKind::DirectTls)
                .unwrap()
                .with_verified_client_certificate(verified)
                .with_tls_peer(stream.verified_tls_peer());
            let authenticated = users
                .authenticate_admitted("root", "127.0.0.1", &[0; 20], &[], admission)
                .is_ok();
            stream.write_all(&[42]).unwrap();
            stream.flush().unwrap();
            (true, verified, authenticated)
        });
        let mut roots = rustls::RootCertStore::empty();
        for cert in read_certificates(&dir.join("ca.crt")).unwrap() {
            roots.add(cert).unwrap();
        }
        let versions: &[&'static rustls::SupportedProtocolVersion] = if tls12_only {
            &[&rustls::version::TLS12]
        } else {
            &[&rustls::version::TLS13, &rustls::version::TLS12]
        };
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(versions)
        .unwrap()
        .with_root_certificates(roots);
        let config = match client_kind {
            0 => builder.with_no_client_auth(),
            1 => builder
                .with_client_auth_cert(
                    read_certificates(&dir.join("client.crt")).unwrap(),
                    read_private_key(&dir.join("client.key")).unwrap(),
                )
                .unwrap(),
            3 => builder
                .with_client_auth_cert(
                    read_certificates(
                        &Path::new(env!("CARGO_MANIFEST_DIR"))
                            .join("testdata/tls/admission-client-san.crt"),
                    )
                    .unwrap(),
                    read_private_key(&dir.join("client.key")).unwrap(),
                )
                .unwrap(),
            _ => {
                let wrong =
                    rcgen::generate_simple_self_signed(vec!["wrong-client".into()]).unwrap();
                builder
                    .with_client_auth_cert(
                        vec![wrong.cert.der().clone()],
                        PrivateKeyDer::try_from(wrong.signing_key.serialize_der()).unwrap(),
                    )
                    .unwrap()
            }
        };
        let connection = rustls::ClientConnection::new(
            Arc::new(config),
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut client = StreamOwned::new(connection, socket);
        let mut byte = [0];
        let received = client.read_exact(&mut byte).is_ok();
        let result = worker.join().unwrap();
        assert_eq!(received, result.0);
        result
    }

    #[test]
    fn admission_policy_batch_san_alternatives_types_and_verified_origin() {
        let matching = r#"{"ssl_type":3,"san":"DNS:wrong,DNS:client.example,IP:127.0.0.1,URI:spiffe://domain/ns/*/sa/client"}"#;
        assert_eq!(
            handshake_policy(3, false, "TLSv1.3", Some(matching)),
            (true, true, true)
        );
        for policy in [
            r#"{"ssl_type":3,"san":"DNS:CLIENT.EXAMPLE"}"#,
            r#"{"ssl_type":3,"san":"IP:192.0.2.2"}"#,
            r#"{"ssl_type":3,"san":"URI:spiffe://domain/ns/def*/sa/client"}"#,
            r#"{"ssl_type":3,"san":"URI:spiffe://*/ns/default/sa/client"}"#,
            r#"{"ssl_type":3,"san":"DNS:client.example,IP:192.0.2.2"}"#,
        ] {
            assert_eq!(
                handshake_policy(3, false, "TLSv1.3", Some(policy)),
                (true, true, false),
                "{policy}"
            );
        }
        assert_eq!(
            handshake_policy(
                3,
                false,
                "TLSv1.3",
                Some(r#"{"ssl_type":3,"san":"IP:192.0.2.1"}"#)
            ),
            (true, true, true)
        );
        assert_eq!(
            handshake_policy(1, false, "TLSv1.3", Some(matching)),
            (true, true, false)
        );
        assert_eq!(
            handshake_policy(0, false, "TLSv1.3", Some(matching)),
            (true, false, false)
        );
        assert_eq!(
            handshake_policy(2, false, "TLSv1.3", Some(matching)),
            (false, false, false)
        );
    }

    #[test]
    fn admission_policy_batch_specified_mismatches_and_cipher_requires_certificate() {
        for policy in [
            r#"{"ssl_type":3,"x509_issuer":"/CN=wrong"}"#,
            r#"{"ssl_type":3,"x509_subject":"/CN=wrong"}"#,
            r#"{"ssl_type":3,"ssl_cipher":"TLS_AES_128_GCM_SHA256"}"#,
        ] {
            assert_eq!(
                handshake_policy(1, false, "TLSv1.3", Some(policy)),
                (true, true, false)
            );
        }
        let cipher = r#"{"ssl_type":3,"ssl_cipher":"TLS_AES_256_GCM_SHA384"}"#;
        assert_eq!(
            handshake_policy(0, false, "TLSv1.3", Some(cipher)),
            (true, false, false)
        );
        assert_eq!(
            handshake_policy(1, false, "TLSv1.3", Some(r#"{"ssl_type":3}"#)),
            (true, true, true)
        );
        assert_eq!(
            handshake_policy(0, false, "TLSv1.3", Some(r#"{"ssl_type":3}"#)),
            (true, false, false)
        );
    }

    #[test]
    fn admission_policy_batch_matching_issuer_succeeds() {
        assert_eq!(
            handshake_policy(
                1,
                false,
                "TLSv1.3",
                Some(r#"{"ssl_type":3,"x509_issuer":"/CN=tidb-test-ca"}"#)
            ),
            (true, true, true)
        );
    }

    #[test]
    fn admission_policy_batch_matching_subject_succeeds() {
        assert_eq!(
            handshake_policy(
                1,
                false,
                "TLSv1.3",
                Some(r#"{"ssl_type":3,"x509_subject":"/CN=tidb-test-client"}"#)
            ),
            (true, true, true)
        );
    }

    #[test]
    fn admission_policy_batch_matching_cipher_succeeds() {
        assert_eq!(
            handshake_policy(
                1,
                false,
                "TLSv1.3",
                Some(r#"{"ssl_type":3,"ssl_cipher":"TLS_AES_256_GCM_SHA384"}"#)
            ),
            (true, true, true)
        );
    }

    #[test]
    fn account_tls_batch_verified_client_satisfies_x509_authentication() {
        assert_eq!(handshake(1, false, "TLSv1.3"), (true, true, true));
    }

    #[test]
    fn account_tls_batch_optional_certificate_tls_does_not_satisfy_x509() {
        assert_eq!(handshake(0, false, ""), (true, false, false));
    }

    #[test]
    fn account_tls_batch_untrusted_client_is_rejected_by_transport() {
        assert_eq!(handshake(2, false, ""), (false, false, false));
    }

    #[test]
    fn account_tls_batch_minimum_tls13_rejects_tls12_only_peer() {
        assert_eq!(handshake(0, true, "TLSv1.3"), (false, false, false));
        assert_eq!(
            handshake(0, true, "invalid-defaults-to-tls12"),
            (true, false, false)
        );
    }
}
