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
use std::path::{Path, PathBuf};
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
    verifies_client_certificates: bool,
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
        let paths = (cert.to_owned(), key.to_owned());
        let certs = read_certificates(cert)?;
        let key = read_private_key(key)?;
        let mut tls = Self::from_material(certs, key, "configured --ssl-cert/--ssl-key")?;
        tls.watch_certificate_files(paths)?;
        Ok(tls)
    }

    /// Generates an in-memory self-signed certificate, mirroring Go's
    /// `createTLSCertificates` fallback under `auto-tls`.
    ///
    /// Go writes the pair to `TempStoragePath`; this node keeps it in memory
    /// under the shared reload/rotation owner. RSA-key-size and temporary
    /// file publication remain separate Go parity obligations.
    pub fn self_signed() -> Result<Self, MysqlTlsError> {
        let now = std::time::SystemTime::now();
        let mut params =
            rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .map_err(|error| MysqlTlsError::Generation(error.to_string()))?;
        // Go CreateCertificates makes a 90-day certificate, renewed at 30 days.
        params.not_before = now.into();
        params.not_after = (now + Duration::from_secs(90 * 24 * 60 * 60)).into();
        let pair = rcgen::KeyPair::generate()
            .map_err(|error| MysqlTlsError::Generation(error.to_string()))?;
        let certificate = params
            .self_signed(&pair)
            .map_err(|error| MysqlTlsError::Generation(error.to_string()))?
            .der()
            .clone();
        let key = PrivateKeyDer::try_from(pair.serialize_der())
            .map_err(|error| MysqlTlsError::Generation(error.to_string()))?;
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
        Self::from_material_with_client_auth(
            certs,
            key,
            origin,
            ca,
            min_version,
            tidb_util::tls::REQUIRE_SECURE_TRANSPORT.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    fn from_material_with_client_auth(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        origin: &'static str,
        ca: Option<&Path>,
        min_version: &str,
        require_secure: bool,
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
        let mut verifies_client_certificates = false;
        let request_only = || {
            Arc::new(RequestClientCert {
                provider: Arc::clone(&provider),
            })
        };
        let builder = if let Some(ca) = ca {
            let file = fs::File::open(ca)
                .map_err(|error| MysqlTlsError::Material(format!("{}: {error}", ca.display())))?;
            let mut roots = rustls::RootCertStore::empty();
            // Go AppendCertsFromPEM keeps valid certificates and ignores invalid
            // blocks; an empty pool retains the no-CA request policy.
            for cert in rustls_pemfile::certs(&mut BufReader::new(file)).flatten() {
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                if require_secure {
                    builder.with_client_cert_verifier(request_only())
                } else {
                    builder.with_no_client_auth()
                }
            } else {
                verifies_client_certificates = true;
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                );
                let verifier = if require_secure {
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
        } else if require_secure {
            builder.with_client_cert_verifier(request_only())
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
            verifies_client_certificates,
        })
    }

    fn watch_certificate_files(&mut self, paths: (PathBuf, PathBuf)) -> Result<(), MysqlTlsError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let key = rustls::sign::CertifiedKey::from_der(
            self.certificates.clone(),
            self.key.clone_key(),
            &provider,
        )
        .map_err(|error| MysqlTlsError::Material(error.to_string()))?;
        Arc::make_mut(&mut self.config).cert_resolver = Arc::new(PemResolver {
            paths,
            provider,
            current: Mutex::new(Arc::new(key)),
        });
        Ok(())
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
    let mut tls = MysqlServerTls::from_material_with_policy(
        tls.certificates,
        tls.key.clone_key(),
        tls.origin,
        ca,
        min_version,
    )?;
    if let (Some(cert), Some(key)) = (cert, key) {
        tls.watch_certificate_files((cert.to_owned(), key.to_owned()))?;
    }
    Ok(Some(tls))
}

/// Go GetCertificate reloads a key pair per handshake and retains the last
/// valid pair when files disappear, mismatch or are temporarily malformed.
#[derive(Debug)]
struct PemResolver {
    paths: (PathBuf, PathBuf),
    provider: Arc<rustls::crypto::CryptoProvider>,
    current: Mutex<Arc<rustls::sign::CertifiedKey>>,
}

impl rustls::server::ResolvesServerCert for PemResolver {
    fn resolve(
        &self,
        _: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        let load = || {
            rustls::sign::CertifiedKey::from_der(
                read_certificates(&self.paths.0)?,
                read_private_key(&self.paths.1)?,
                &self.provider,
            )
            .map_err(|error| MysqlTlsError::Material(error.to_string()))
        };
        let next = load();
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match next {
            Ok(key) => *current = Arc::new(key),
            Err(error) => {
                eprintln!("Could not reload server certificate, using the old one: {error}")
            }
        }
        Some(Arc::clone(&current))
    }
}

/// Go RequestClientCert parses certificates and verifies handshake signatures,
/// but does not verify trust/expiry. This policy NEVER supplies account X509
/// evidence: the accepted socket separately retains CA-verification provenance.
#[derive(Debug)]
struct RequestClientCert {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl rustls::server::danger::ClientCertVerifier for RequestClientCert {
    fn client_auth_mandatory(&self) -> bool {
        false
    }
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        for cert in std::iter::once(end_entity).chain(intermediates) {
            let (remaining, _) =
                x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|_| {
                    rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
                })?;
            if !remaining.is_empty() {
                return Err(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::BadEncoding,
                ));
            }
        }
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// One process configuration, shared by SQL reload and each new connection.
pub(crate) struct MysqlTlsManager {
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    ca: Option<PathBuf>,
    auto_tls: bool,
    min_version: String,
    current: std::sync::RwLock<Option<MysqlServerTls>>,
}

impl MysqlTlsManager {
    pub(crate) fn new(config: &crate::NodeConfig) -> Result<Arc<Self>, MysqlTlsError> {
        let current = resolve_server_tls_with_policy(
            config.ssl_cert.as_deref(),
            config.ssl_key.as_deref(),
            config.auto_tls,
            config.ssl_ca.as_deref(),
            &config.min_tls_version,
        )?;
        Ok(Arc::new(Self {
            cert: config.ssl_cert.clone(),
            key: config.ssl_key.clone(),
            ca: config.ssl_ca.clone(),
            auto_tls: config.auto_tls,
            min_version: config.min_tls_version.clone(),
            current: std::sync::RwLock::new(current),
        }))
    }

    pub(crate) fn current(&self) -> Option<MysqlServerTls> {
        self.current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn variables(&self) -> Vec<(&'static str, String)> {
        if self.current().is_none() {
            return Vec::new();
        }
        vec![
            (
                "ssl_ca",
                self.ca
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string()),
            ),
            (
                "ssl_cert",
                self.cert
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string()),
            ),
            (
                "ssl_key",
                self.key
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string()),
            ),
            ("have_ssl", "YES".into()),
            ("have_openssl", "YES".into()),
        ]
    }

    fn load(&self) -> Result<Option<MysqlServerTls>, MysqlTlsError> {
        resolve_server_tls_with_policy(
            self.cert.as_deref(),
            self.key.as_deref(),
            self.auto_tls,
            self.ca.as_deref(),
            &self.min_version,
        )
    }

    pub(crate) fn automatic(&self) -> bool {
        self.auto_tls && (self.cert.is_none() || self.key.is_none())
    }

    fn rotate(&self) {
        // Go's automatic rotation stores the loader result, including nil on
        // failure. SQL reload has its separate rollback policy below.
        let next = self.load().unwrap_or_else(|error| {
            eprintln!("TLS Certificate rotation failed: {error}");
            None
        });
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    }
}

impl tidb_session::process::TlsManager for MysqlTlsManager {
    fn reload_tls(&self, no_rollback_on_error: bool) -> Result<(), String> {
        let next = match self.load() {
            Ok(next) => next,
            Err(error)
                if !no_rollback_on_error
                    || tidb_util::tls::REQUIRE_SECURE_TRANSPORT
                        .load(std::sync::atomic::Ordering::SeqCst) =>
            {
                return Err(error.to_string());
            }
            Err(error) => {
                eprintln!("Reload TLS failed; NO ROLLBACK ON ERROR disables new TLS: {error}");
                None
            }
        };
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        Ok(())
    }
}

/// Go renews auto-generated material every 30 days. The Rust process owner
/// interrupts the wait and joins the worker on every shutdown/drop path.
pub(crate) struct TlsRotation {
    exit: std::sync::mpsc::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl TlsRotation {
    pub(crate) fn start(manager: Arc<MysqlTlsManager>) -> Option<Self> {
        manager
            .automatic()
            .then(|| Self::with_interval(manager, Duration::from_secs(30 * 24 * 60 * 60)))
    }

    fn with_interval(manager: Arc<MysqlTlsManager>, interval: Duration) -> Self {
        let (exit, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            while matches!(
                receive.recv_timeout(interval),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                manager.rotate();
            }
        });
        Self {
            exit,
            worker: Some(worker),
        }
    }
}

impl Drop for TlsRotation {
    fn drop(&mut self) {
        let _ = self.exit.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
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
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>, bool),
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
            ClientStreamInner::Tls(..)
        )
    }

    /// Only a configured CA verifier establishes trusted chain evidence.
    /// Go RequestClientCert supplies parsed peers without VerifiedChains.
    pub fn has_verified_client_certificate(&self) -> bool {
        match &*self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Tls(stream, true) => {
                !stream.conn.is_handshaking()
                    && stream
                        .conn
                        .peer_certificates()
                        .is_some_and(|chain| !chain.is_empty())
            }
            _ => false,
        }
    }

    /// Leaf properties are usable only after the configured verifier accepted
    /// the handshake. Certificate parsing never substitutes for chain verification.
    pub(crate) fn verified_tls_peer(&self) -> Option<tidb_session::privilege::TlsPeerIdentity> {
        use x509_parser::extensions::GeneralName;
        let guard = self.inner.lock().expect("client stream lock");
        let ClientStreamInner::Tls(stream, true) = &*guard else {
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
        *guard = ClientStreamInner::Tls(Box::new(stream), tls.verifies_client_certificates);
        Ok(())
    }

    /// The negotiated `(cipher_suite, protocol_version)` wire identifiers of
    /// this connection's TLS session, `None` before an upgrade. Go reads the
    /// same pair off `tls.ConnectionState` for the `Ssl_cipher` and
    /// `Ssl_version` status variables (`server.go:1329`).
    pub fn negotiated_tls(&self) -> Option<(u16, u16)> {
        match &*self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Tls(stream, _) => {
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
            ClientStreamInner::Tls(stream, _) => stream.sock.set_read_timeout(timeout),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }
}

impl Read for ClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.read(buffer),
            ClientStreamInner::Tls(stream, _) => stream.read(buffer),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.write(buffer),
            ClientStreamInner::Tls(stream, _) => stream.write(buffer),
            ClientStreamInner::Upgrading => Err(io::Error::other("connection is mid-upgrade")),
        }
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        match &mut *self.inner.lock().expect("client stream lock") {
            ClientStreamInner::Plain(stream) => stream.write_vectored(buffers),
            ClientStreamInner::Tls(stream, _) => {
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
            ClientStreamInner::Tls(stream, _) => stream.flush(),
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
    fn tls_owner_sql_reload_uses_shared_process_and_preserves_failed_configuration() {
        let ca = std::env::temp_dir().join(format!(
            "tidb-reload-ca-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::write(&ca, "").unwrap();
        let mut config =
            crate::NodeConfig::parse(["tidb-server", "--store", "unistore", "--load-privileges"])
                .unwrap();
        config.auto_tls = true;
        config.ssl_ca = Some(ca.clone());
        let manager = MysqlTlsManager::new(&config).unwrap();
        let processes = tidb_session::process::ProcessRegistry::default();
        processes.set_tls_manager(manager.clone());
        let mut session = tidb_session::Session::new();
        session.attach_process(
            1,
            processes.register(1, "root".into(), "localhost".into(), String::new(), None),
        );
        let original = manager.current().unwrap().certificates[0].clone();
        session.run("ALTER INSTANCE RELOAD TLS").unwrap();
        let reloaded = manager.current().unwrap().certificates[0].clone();
        assert_ne!(original, reloaded);
        fs::remove_file(&ca).unwrap();
        assert!(session.run("ALTER INSTANCE RELOAD TLS").is_err());
        assert_eq!(manager.current().unwrap().certificates[0], reloaded);
        session.attach_privileges(tidb_session::privilege::PrivilegeRegistry::default());
        session.set_user("limited@%".into(), "limited@localhost".into());
        let error = session
            .run("ALTER INSTANCE RELOAD TLS")
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1227);
        assert!(error.message.contains("SUPER"));
    }

    #[test]
    fn tls_owner_automatic_certificates_renew_and_worker_retires() {
        let initial = MysqlServerTls::self_signed().unwrap();
        let (_, cert) =
            x509_parser::parse_x509_certificate(initial.certificates[0].as_ref()).unwrap();
        assert_eq!(
            cert.validity().not_after.timestamp() - cert.validity().not_before.timestamp(),
            90 * 24 * 60 * 60
        );
        let old = initial.certificates[0].clone();
        let manager = Arc::new(MysqlTlsManager {
            cert: None,
            key: None,
            ca: None,
            auto_tls: true,
            min_version: String::new(),
            current: std::sync::RwLock::new(Some(initial)),
        });
        let runner = TlsRotation::with_interval(manager.clone(), Duration::from_millis(10));
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while manager.current().unwrap().certificates[0] == old {
            assert!(
                std::time::Instant::now() < deadline,
                "automatic renewal did not publish"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        drop(runner);
        let retired = manager.current().unwrap().certificates[0].clone();
        std::thread::sleep(Duration::from_millis(25));
        assert_eq!(manager.current().unwrap().certificates[0], retired);
    }

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
        handshake_client_policy(client_kind, tls12_only, minimum, policy, true)
    }

    fn handshake_client_policy(
        client_kind: u8,
        tls12_only: bool,
        minimum: &str,
        policy: Option<&str>,
        trust_clients: bool,
    ) -> (bool, bool, bool) {
        let policy = policy.map(str::to_owned);
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tidb-pd-client/testdata/tls");
        let ca = dir.join("ca.crt");
        let tls = MysqlServerTls::from_material_with_client_auth(
            read_certificates(&dir.join("server.crt")).unwrap(),
            read_private_key(&dir.join("server.key")).unwrap(),
            "test",
            trust_clients.then_some(ca.as_path()),
            minimum,
            !trust_clients,
        )
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
            if !trust_clients {
                let guard = stream.inner.lock().unwrap();
                let ClientStreamInner::Tls(peer, _) = &*guard else {
                    unreachable!()
                };
                assert_eq!(
                    peer.conn
                        .peer_certificates()
                        .is_some_and(|certs| !certs.is_empty()),
                    client_kind != 0,
                    "RequestClientCert must actually request the peer certificate"
                );
                drop(guard);
                assert!(stream.verified_tls_peer().is_none());
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
    fn tls_owner_request_without_ca_never_creates_verified_account_evidence() {
        // Go util.LoadTLSCertificates RequestClientCert; privileges checks
        // VerifiedChains, not merely PeerCertificates (TLS1.2 and TLS1.3).
        for tls12 in [true, false] {
            for client in [0, 1, 2] {
                assert_eq!(
                    handshake_client_policy(client, tls12, "", None, false),
                    (true, false, false)
                );
            }
        }
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
