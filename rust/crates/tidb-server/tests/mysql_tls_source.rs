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

//! Server TLS material policy, and the in-place upgrade of a live socket.
//!
//! The MySQL-level sequence (advertise `CLIENT_SSL`, read an `SSLRequest`,
//! upgrade, read the real `HandshakeResponse41` off the encrypted stream) is
//! proven end to end by `scripts/run-sysbench-ladder.sh` against the stock
//! MySQL client and sysbench. What is proven here is the mechanism underneath
//! it: the same socket carries plaintext and then TLS, and both handles on the
//! connection see the swap.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tidb_server::{resolve_server_tls, ClientStream, MysqlServerTls};

/// `pkg/server/server.go` advertises `CLIENT_SSL` only when
/// `LoadTLSCertificates` returned a config; the resolution below is what
/// decides that, so each branch is named.
#[test]
fn no_material_and_no_auto_tls_leaves_the_port_plaintext() {
    let resolved = resolve_server_tls(None, None, false).expect("plaintext is a valid outcome");
    assert!(
        resolved.is_none(),
        "without material and without auto-tls the port must not offer TLS"
    );
}

#[test]
fn auto_tls_generates_material_when_none_is_configured() {
    let resolved = resolve_server_tls(None, None, true).expect("self-signed generation");
    let tls = resolved.expect("auto-tls must produce material");
    assert_eq!(tls.origin(), "auto-generated self-signed");
}

#[test]
fn a_half_configured_pair_is_refused_rather_than_silently_self_signed() {
    let cert = PathBuf::from("/nonexistent/cert.pem");
    let key = PathBuf::from("/nonexistent/key.pem");
    let cert_only = resolve_server_tls(Some(&cert), None, true);
    assert!(
        cert_only.is_err(),
        "--ssl-cert without --ssl-key must be refused, not replaced by a generated pair"
    );
    let key_only = resolve_server_tls(None, Some(&key), true);
    assert!(
        key_only.is_err(),
        "--ssl-key without --ssl-cert must be refused"
    );
}

#[test]
fn configured_pem_material_is_loaded_and_wins_over_auto_tls() {
    let directory = temporary_directory("tls-material");
    let (cert_path, key_path) = write_self_signed_pem(&directory);
    let tls = resolve_server_tls(Some(&cert_path), Some(&key_path), true)
        .expect("configured PEM material loads")
        .expect("configured material is always TLS");
    assert_eq!(tls.origin(), "configured --ssl-cert/--ssl-key");
    std::fs::remove_dir_all(&directory).expect("remove temporary directory");
}

#[test]
fn a_missing_certificate_file_is_reported_rather_than_ignored() {
    let directory = temporary_directory("tls-missing");
    let (_cert_path, key_path) = write_self_signed_pem(&directory);
    let missing = directory.join("absent.pem");
    let error = resolve_server_tls(Some(&missing), Some(&key_path), true)
        .expect_err("a configured certificate that does not exist is a startup failure");
    assert!(
        error.to_string().contains("absent.pem"),
        "the failure must name the file: {error}"
    );
    std::fs::remove_dir_all(&directory).expect("remove temporary directory");
}

/// The mechanism Go's `upgradeToTLS` needs: the socket that carried the
/// initial handshake in the clear keeps carrying the session once TLS is
/// established, and the reader's handle sees it as much as the writer's does.
#[test]
fn the_same_socket_carries_plaintext_and_then_tls() {
    let directory = temporary_directory("tls-upgrade");
    let (cert_path, key_path) = write_self_signed_pem(&directory);
    let tls = MysqlServerTls::from_pem_files(&cert_path, &key_path).expect("load material");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let address = listener.local_addr().expect("listener address");
    let server = std::thread::spawn(move || {
        let (socket, _peer) = listener.accept().expect("accept");
        let stream = ClientStream::plain(socket);
        // Two handles, as the connection path has: one writes, one reads.
        let mut writer = stream.clone();
        let mut reader = stream.clone();
        writer.write_all(b"plaintext-greeting").expect("greeting");
        writer.flush().expect("flush greeting");
        assert!(!stream.is_tls(), "the greeting is sent in the clear");
        stream.upgrade_to_tls(&tls).expect("upgrade");
        assert!(stream.is_tls(), "the upgrade is visible on every handle");
        let mut received = [0_u8; 9];
        reader.read_exact(&mut received).expect("encrypted read");
        assert_eq!(&received, b"encrypted");
        writer.write_all(b"server-after-tls").expect("encrypted write");
        writer.flush().expect("flush");
    });

    let mut socket = TcpStream::connect(address).expect("connect");
    let mut greeting = [0_u8; 18];
    socket.read_exact(&mut greeting).expect("plaintext greeting");
    assert_eq!(&greeting, b"plaintext-greeting");

    let mut client = client_session(&cert_path);
    let mut client_stream = rustls::Stream::new(&mut client, &mut socket);
    client_stream.write_all(b"encrypted").expect("client write");
    client_stream.flush().expect("client flush");
    let mut answer = [0_u8; 16];
    client_stream.read_exact(&mut answer).expect("client read");
    assert_eq!(&answer, b"server-after-tls");

    server.join().expect("server thread");
    std::fs::remove_dir_all(&directory).expect("remove temporary directory");
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
