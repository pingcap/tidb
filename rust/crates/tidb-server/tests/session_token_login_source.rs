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

//! Go `TestAuthSessionTokenPlugin` (`pkg/server/conn_test.go`): a migrated
//! session logs in with the token `SHOW SESSION_STATES` signed, over a real
//! socket. `checkAuthPlugin` always permits `tidb_session_token` without an
//! auth switch, and `ConnectionVerification` checks the token instead of the
//! account's password, skipping the password-expiry check.

#![allow(missing_docs)]

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use tidb_protocol::{PacketReader, PacketWriter, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker, QueryResult, QuerySession, QuerySessionFactory, SessionContext,
    SqlQueryError,
};
use tidb_session::privilege::{PasswordExpireSetting, PrivilegeRegistry};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
const SESSION_TOKEN: &str = "tidb_session_token";
const NATIVE: &str = "mysql_native_password";

struct Session;

impl QuerySession for Session {
    fn parse_statement(&mut self, _sql: &str) -> Result<tidb_ast::Stmt, SqlQueryError> {
        unreachable!("no statement is issued by these tests")
    }

    fn execute_stmt<'a>(
        &'a mut self,
        _sql: &str,
        _stmt: &tidb_ast::Stmt,
    ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
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

/// Go `util.CreateCertificates(..., x509.RSA, x509.UnknownSignatureAlgorithm)`.
fn create_rsa_cert(cert_path: &std::path::Path, key_path: &std::path::Path) {
    use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa, x509::X509Builder};
    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut builder = X509Builder::new().unwrap();
    builder.set_version(2).unwrap();
    builder.set_pubkey(&key).unwrap();
    builder
        .set_not_before(&openssl::asn1::Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&openssl::asn1::Asn1Time::days_from_now(90).unwrap())
        .unwrap();
    builder.sign(&key, MessageDigest::sha256()).unwrap();
    std::fs::write(cert_path, builder.build().to_pem().unwrap()).unwrap();
    std::fs::write(key_path, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
}

/// Serves one connection authenticating against `users`.
fn serve(
    users: &ConfiguredUserStore,
) -> (
    std::net::SocketAddr,
    std::thread::JoinHandle<tidb_server::ConnectionReport>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let users = users.clone();
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &Factory,
            &users,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    (address, worker)
}

/// A `HandshakeResponse41` whose auth data is length-encoded, as a token is
/// longer than the one-byte length allows.
fn handshake_response(user: &str, plugin: &str, auth: &[u8]) -> Vec<u8> {
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0xfc);
    response.extend_from_slice(&u16::try_from(auth.len()).unwrap().to_le_bytes());
    response.extend_from_slice(auth);
    response.extend_from_slice(plugin.as_bytes());
    response.push(0);
    response
}

/// Logs in once and returns the first reply packet and how the connection
/// ended.
fn login(users: &ConfiguredUserStore, user: &str, plugin: &str, auth: &[u8]) -> (Vec<u8>, ConnectionExit) {
    let (address, worker) = serve(users);
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    reader.read_packet().unwrap();
    let mut writer = PacketWriter::with_sequence(&mut client, 1);
    writer.write_packet(&handshake_response(user, plugin, auth)).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(2);
    let reply = reader.read_packet().unwrap();
    // Both handles share the socket; closing it ends a logged-in
    // connection's command loop.
    drop(reader);
    drop(client);
    (reply, worker.join().unwrap().exit)
}

fn error_code(reply: &[u8]) -> u16 {
    assert_eq!(reply[0], 0xff, "an error packet: {reply:?}");
    u16::from_le_bytes([reply[1], reply[2]])
}

#[test]
fn a_migrated_session_logs_in_with_its_session_token_like_go() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("test1_cert.pem");
    let key_path = dir.path().join("test1_key.pem");
    create_rsa_cert(&cert_path, &key_path);
    tidb_sessionstates::set_cert_path(cert_path.to_str().unwrap());
    tidb_sessionstates::set_key_path(key_path.to_str().unwrap());
    tidb_sessionstates::reload_signing_cert();

    let accounts = PrivilegeRegistry::default();
    accounts.create_user_with_plugin("auth_session_token", "%", "", NATIVE);
    accounts.create_user_with_plugin("another_user", "%", "", NATIVE);
    let users = ConfiguredUserStore::from_accounts(accounts);
    let token = tidb_util::stringutil::go_json_marshal(
        &tidb_sessionstates::create_session_token("auth_session_token").unwrap(),
    )
    .into_bytes();

    // auth with the token: no auth switch, straight to OK
    let (reply, _) = login(&users, "auth_session_token", SESSION_TOKEN, &token);
    assert_eq!(reply[0], 0, "the token logs in: {reply:?}");

    // login succeeds even if the password expires now
    assert!(users
        .accounts()
        .set_password_expire("auth_session_token", "%", PasswordExpireSetting::Now));
    let (reply, exit) = login(&users, "auth_session_token", NATIVE, &[]);
    assert_eq!(error_code(&reply), 1862, "Your password has expired");
    assert_eq!(exit, ConnectionExit::AuthenticationRejected);
    let (reply, _) = login(&users, "auth_session_token", SESSION_TOKEN, &token);
    assert_eq!(reply[0], 0, "the token skips the expiry check: {reply:?}");

    // wrong token should fail
    let mut wrong = token.clone();
    wrong[0] ^= 0xff;
    let (reply, _) = login(&users, "auth_session_token", SESSION_TOKEN, &wrong);
    assert_eq!(error_code(&reply), 1045);

    // using the token to auth with another user should fail
    let (reply, _) = login(&users, "another_user", SESSION_TOKEN, &token);
    assert_eq!(error_code(&reply), 1045);
}
