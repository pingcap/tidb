// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! The two TLS admission rules, over a real socket with real TLS.
//!
//! Go applies them in two different places and this pins both:
//!
//! * `require_secure_transport` (`server/conn.go` line 669) is process-wide
//!   and fires BEFORE the account is looked up, with errno 3159.
//! * an account's own `REQUIRE SSL` (`privileges.go`'s `checkSSL`, line 795)
//!   is per-account, fires inside `ConnectionVerification` before the
//!   password comparison, and reports the SAME generic 1045 a wrong password
//!   does -- so a client learns nothing about why.
//!
//! Measured against a real TiDB in this checkout:
//!
//! ```text
//! CREATE USER 'ssl'@'%' REQUIRE SSL   mysql.global_priv.PRIV {"ssl_type":1}
//! CREATE USER 'plain'@'%'             {}
//! ALTER USER 'plain'@'%' REQUIRE SSL  {"ssl_type":1}
//! ALTER USER 'plain'@'%' REQUIRE NONE {}
//! SHOW CREATE USER 'ssl'@'%'          ... REQUIRE SSL ...
//! ```
//!
//! Every certificate here is generated into the test's own temporary
//! directory and removed with it.

#![allow(missing_docs)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha1::{Digest, Sha1};
use tidb_protocol::{PacketReader, PacketWriter, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection_with_tls, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker, MysqlServerTls, QueryResult, QuerySession, QuerySessionFactory,
    SessionContext, SqlQueryError,
};
use tidb_session::privilege::{encode_password, PrivilegeRegistry, SslType};
use tidb_session::GlobalSysvars;

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
const CLIENT_SSL: u32 = 1 << 11;
const NATIVE: &str = "mysql_native_password";

struct Session;

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        unreachable!("no statement is issued by these tests")
    }
}

struct Factory;

impl QuerySessionFactory for Factory {
    type Session = Session;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(Session)
    }
}

/// Two accounts on one registry: one that requires TLS and one that does
/// not, so every assertion below has its own control on the same server.
fn store(global_vars: GlobalSysvars) -> ConfiguredUserStore {
    let accounts = PrivilegeRegistry::default();
    accounts.create_user("secure", "%", &encode_password("pw"));
    accounts.set_ssl_type("secure", "%", SslType::Any);
    accounts.create_user("open", "%", &encode_password("pw"));
    ConfiguredUserStore::from_accounts(accounts).with_global_vars(global_vars)
}

fn native_response(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut challenge = Sha1::new();
    challenge.update(salt);
    challenge.update(stage_two);
    let challenge = challenge.finalize();
    let mut response = [0; 20];
    for index in 0..response.len() {
        response[index] = stage_one[index] ^ challenge[index];
    }
    response
}

fn handshake_salt(initial: &[u8]) -> [u8; 20] {
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let first = version_end + 1 + 4;
    let second = first + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    let mut salt = [0; 20];
    salt[..8].copy_from_slice(&initial[first..first + 8]);
    salt[8..].copy_from_slice(&initial[second..second + 12]);
    salt
}

fn base_capabilities(with_ssl: bool) -> u32 {
    let base = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_DEPRECATE_EOF;
    if with_ssl {
        base | CLIENT_SSL
    } else {
        base
    }
}

fn handshake_response(user: &str, auth: &[u8], with_ssl: bool) -> Vec<u8> {
    let mut response = Vec::new();
    response.extend_from_slice(&base_capabilities(with_ssl).to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(auth);
    response.extend_from_slice(NATIVE.as_bytes());
    response.push(0);
    response
}

/// The truncated `SSLRequest` a client sends to ask for the upgrade: the
/// handshake-response header and nothing after it.
fn ssl_request() -> Vec<u8> {
    let mut request = Vec::new();
    request.extend_from_slice(&base_capabilities(true).to_le_bytes());
    request.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    request.push(46);
    request.extend_from_slice(&[0; 23]);
    request
}

struct Server {
    address: SocketAddr,
    directory: PathBuf,
    worker: std::thread::JoinHandle<tidb_server::ConnectionReport>,
    cert_path: PathBuf,
}

fn serve(label: &str, users: ConfiguredUserStore) -> Server {
    let directory = temporary_directory(label);
    let (cert_path, key_path) = write_self_signed_pem(&directory);
    let tls = MysqlServerTls::from_pem_files(&cert_path, &key_path).expect("load material");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection_with_tls(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &Factory,
            &users,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
            Some(&tls),
        )
        .unwrap()
    });
    Server {
        address,
        directory,
        worker,
        cert_path,
    }
}

/// Runs a plaintext login and returns the server's reply packet verbatim.
fn login_plaintext(address: SocketAddr, user: &str, password: &[u8]) -> Vec<u8> {
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let salt = handshake_salt(&reader.read_packet().unwrap());
    let mut writer = PacketWriter::with_sequence(&mut client, 1);
    writer
        .write_packet(&handshake_response(
            user,
            &native_response(password, &salt),
            false,
        ))
        .unwrap();
    writer.flush().unwrap();
    reader.set_sequence(2);
    reader.read_packet().unwrap()
}

/// Runs the real TLS upgrade and then the login over the encrypted stream,
/// returning the server's reply packet.
fn login_over_tls(address: SocketAddr, cert_path: &Path, user: &str, password: &[u8]) -> Vec<u8> {
    let mut socket = TcpStream::connect(address).unwrap();
    let mut plain_reader = PacketReader::new(socket.try_clone().unwrap());
    plain_reader.set_sequence(0);
    let salt = handshake_salt(&plain_reader.read_packet().unwrap());
    // The SSLRequest goes out in the clear; everything after it is encrypted.
    let mut writer = PacketWriter::with_sequence(&mut socket, 1);
    writer.write_packet(&ssl_request()).unwrap();
    writer.flush().unwrap();

    let mut connection = client_session(cert_path);
    let mut stream = rustls::Stream::new(&mut connection, &mut socket);
    // The full HandshakeResponse41 is repeated over TLS, and the sequence
    // continues across the upgrade -- which is why the reply is sequence 3.
    let mut framed = Vec::new();
    {
        let mut writer = PacketWriter::with_sequence(&mut framed, 2);
        writer
            .write_packet(&handshake_response(
                user,
                &native_response(password, &salt),
                true,
            ))
            .unwrap();
        writer.flush().unwrap();
    }
    stream.write_all(&framed).unwrap();
    stream.flush().unwrap();

    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).unwrap();
    if payload[0] == 0 {
        // A successful login: close politely so the server reports Quit.
        let mut framed = Vec::new();
        let mut writer = PacketWriter::with_sequence(&mut framed, 0);
        writer.write_packet(&[COM_QUIT]).unwrap();
        writer.flush().unwrap();
        let _ = stream.write_all(&framed);
        let _ = stream.flush();
    }
    payload
}

/// An account with `REQUIRE SSL` is refused over plaintext and admitted over
/// TLS, with the same password both times -- which is what makes this a test
/// of the transport rule and not of the password.
#[test]
fn a_require_ssl_account_is_refused_in_the_clear_and_admitted_over_tls() {
    let server = serve("require-ssl", store(GlobalSysvars::default()));

    let refused = login_plaintext(server.address, "secure", b"pw");
    assert_eq!(refused[0], 0xff, "an error packet: {refused:?}");
    assert_eq!(
        u16::from_le_bytes([refused[1], refused[2]]),
        1045,
        "Go's checkSSL failure reports the GENERIC access-denied, not its own errno",
    );
    assert_eq!(server.worker.join().unwrap().exit, ConnectionExit::AuthenticationRejected);

    // The same account, the same password, over TLS.
    let server = serve("require-ssl-tls", store(GlobalSysvars::default()));
    let ok = login_over_tls(server.address, &server.cert_path, "secure", b"pw");
    assert_eq!(ok[0], 0, "TLS satisfies REQUIRE SSL: {ok:?}");
    assert_eq!(server.worker.join().unwrap().exit, ConnectionExit::Quit);
    std::fs::remove_dir_all(&server.directory).ok();
}

/// The control: an account WITHOUT `REQUIRE SSL` still logs in over
/// plaintext, so the gate above is the account's requirement and not a
/// server-wide refusal that happened to fire.
#[test]
fn an_ordinary_account_still_logs_in_over_plaintext() {
    let server = serve("require-ssl-control", store(GlobalSysvars::default()));
    let ok = login_plaintext(server.address, "open", b"pw");
    assert_eq!(ok[0], 0, "no REQUIRE clause, no TLS needed: {ok:?}");
    std::fs::remove_dir_all(&server.directory).ok();
    // The connection is left open; the worker ends when the client drops.
    drop(server.worker);
}

/// `require_secure_transport = ON` refuses EVERY plaintext connection, with
/// Go's own errno 3159 -- and, unlike the per-account rule, before the
/// account is looked up at all.
#[test]
fn require_secure_transport_refuses_every_plaintext_login() {
    let global_vars = GlobalSysvars::default();
    global_vars
        .set("require_secure_transport", "ON".to_owned())
        .expect("the sysvar exists and takes ON");
    let server = serve("secure-transport", store(global_vars.clone()));

    // `open` has no REQUIRE clause at all, so only the process-wide policy
    // can refuse it.
    let refused = login_plaintext(server.address, "open", b"pw");
    assert_eq!(refused[0], 0xff, "an error packet: {refused:?}");
    assert_eq!(u16::from_le_bytes([refused[1], refused[2]]), 3159);
    assert!(
        String::from_utf8_lossy(&refused[9..]).contains("--require_secure_transport=ON"),
        "Go's own message: {:?}",
        String::from_utf8_lossy(&refused[9..]),
    );
    assert_eq!(server.worker.join().unwrap().exit, ConnectionExit::AuthenticationRejected);
    std::fs::remove_dir_all(&server.directory).ok();

    // Over TLS the same account is admitted, which is what makes this a
    // TRANSPORT policy and not a shutdown.
    let server = serve("secure-transport-tls", store(global_vars));
    let ok = login_over_tls(server.address, &server.cert_path, "open", b"pw");
    assert_eq!(ok[0], 0, "TLS satisfies require_secure_transport: {ok:?}");
    assert_eq!(server.worker.join().unwrap().exit, ConnectionExit::Quit);
    std::fs::remove_dir_all(&server.directory).ok();
}

/// A client that trusts exactly the certificate the server was given, so the
/// handshake proves the server's material rather than a disabled verifier.
fn client_session(cert_path: &Path) -> rustls::ClientConnection {
    let mut roots = rustls::RootCertStore::empty();
    let file = std::fs::File::open(cert_path).expect("open certificate");
    for certificate in rustls_pemfile::certs(&mut std::io::BufReader::new(file)) {
        roots
            .add(certificate.expect("PEM certificate"))
            .expect("trust the server certificate");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    rustls::ClientConnection::new(
        Arc::new(config),
        "localhost".try_into().expect("server name"),
    )
    .expect("client session")
}

fn write_self_signed_pem(directory: &Path) -> (PathBuf, PathBuf) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate certificate");
    let cert_path = directory.join("cert.pem");
    let key_path = directory.join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write certificate");
    std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("write key");
    (cert_path, key_path)
}

fn temporary_directory(label: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "tidb-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).expect("create temporary directory");
    directory
}
