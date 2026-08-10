// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! `caching_sha2_password` and `tidb_sm3_password` logins, over a real
//! socket.
//!
//! Go's flow (`pkg/server/conn.go`'s `checkAuthPlugin`, `authSha` and
//! `authSM3`, then `pkg/privilege/privileges`'s `checkPasswordForPlugin`) is
//! SHORTER than MySQL's, and that shortness is the whole reason this is
//! portable:
//!
//! 1. the server advertises `mysql_native_password`, then switches the
//!    connection to the ACCOUNT's `mysql.user.plugin`;
//! 2. TiDB implements NEITHER the cached fast path NOR the RSA public-key
//!    exchange, so it always answers `fastAuthFail` (`0x01 0x04`);
//! 3. the client sends the CLEARTEXT password, NUL-terminated;
//! 4. the server re-derives the stored SHA-crypt envelope from that
//!    cleartext, using the salt and iteration count the STORED HASH carries,
//!    and compares the whole string.
//!
//! Step 2 is why no RSA key is needed and nothing here is faked. The one
//! carve-out is Go's own (issue 40831): an EMPTY response skips steps 2-3
//! entirely, because asking a passwordless client for a full authentication
//! confuses it.

#![allow(missing_docs)]

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use tidb_protocol::{PacketReader, PacketWriter, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_parser::auth::hash_password_with_salt_bytes;
use tidb_server::{
    serve_mysql_connection, AuthSwitchRequest, ConfiguredUserStore, ConnectionCancellation,
    ConnectionExit, ConnectionTracker, QueryResult, QuerySession, QuerySessionFactory,
    SessionContext, SqlQueryError,
};
use tidb_session::privilege::{encode_password_for_plugin, PluginCredential, PrivilegeRegistry};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
const CACHING_SHA2: &str = "caching_sha2_password";
const TIDB_SM3: &str = "tidb_sm3_password";
const NATIVE: &str = "mysql_native_password";

struct Session;

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        unreachable!("no statement is issued by these tests")
    }
}

#[derive(Default)]
struct RecordingFactory {
    contexts: Mutex<Vec<SessionContext>>,
}

impl QuerySessionFactory for RecordingFactory {
    type Session = Session;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        self.contexts.lock().unwrap().push(context);
        Ok(Session)
    }
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_response(user: &str, plugin: &str, auth: &[u8]) -> Vec<u8> {
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(auth);
    response.extend_from_slice(plugin.as_bytes());
    response.push(0);
    response
}

/// One account on `plugin`, whose stored `authentication_string` is the one
/// `CREATE USER ... IDENTIFIED WITH <plugin> BY '<password>'` writes.
fn store_with(user: &str, plugin: &str, password: Option<&str>) -> ConfiguredUserStore {
    let accounts = PrivilegeRegistry::default();
    let credential = password.map_or(PluginCredential::None, PluginCredential::By);
    let stored = encode_password_for_plugin(plugin, &credential).expect("a valid credential");
    accounts.create_user_with_plugin(user, "%", &stored, plugin);
    ConfiguredUserStore::from_accounts(accounts)
}

fn store_with_password_bytes(user: &str, plugin: &str, password: &[u8]) -> ConfiguredUserStore {
    let accounts = PrivilegeRegistry::default();
    let stored = hash_password_with_salt_bytes(password, b"source-compatible-20", plugin);
    let stored = String::from_utf8(stored).expect("authentication strings are ASCII");
    accounts.create_user_with_plugin(user, "%", &stored, plugin);
    ConfiguredUserStore::from_accounts(accounts)
}

struct Wire {
    client: TcpStream,
    reader: PacketReader<TcpStream>,
}

/// Connects, reads the greeting, and sends a `HandshakeResponse41` naming
/// `client_plugin` -- the client's OWN preference, which Go ignores in favour
/// of the account's.
fn open(address: std::net::SocketAddr, user: &str, client_plugin: &str, auth: &[u8]) -> Wire {
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let greeting = reader.read_packet().unwrap();
    assert_eq!(greeting[0], 10, "protocol 10 greeting");
    write_packet(&mut client, 1, &handshake_response(user, client_plugin, auth));
    reader.set_sequence(2);
    Wire { client, reader }
}

fn serve(
    users: ConfiguredUserStore,
) -> (
    std::net::SocketAddr,
    Arc<RecordingFactory>,
    std::thread::JoinHandle<tidb_server::ConnectionReport>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let factory = Arc::new(RecordingFactory::default());
    let worker_factory = Arc::clone(&factory);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_factory.as_ref(),
            &users,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    (address, factory, worker)
}

/// The whole `caching_sha2_password` exchange, end to end over TCP: the
/// switch to the ACCOUNT's plugin, the unconditional `fastAuthFail`, the
/// NUL-terminated cleartext, and the OK packet.
#[test]
fn a_caching_sha2_account_authenticates_over_the_full_auth_exchange() {
    let (address, factory, worker) = serve(store_with("sha", CACHING_SHA2, Some("s3cret")));
    // The client offers native; the ACCOUNT's plugin is what wins.
    let mut wire = open(address, "sha", NATIVE, b"ignored-native-scramble");

    let switch = AuthSwitchRequest::parse_payload(&wire.reader.read_packet().unwrap()).unwrap();
    assert_eq!(
        switch.client_plugin, CACHING_SHA2,
        "the switch names the account's plugin, not the client's"
    );

    // The client would answer the switch with a caching_sha2 scramble; Go
    // never looks at it, so any nonempty response drives the same next step.
    write_packet(&mut wire.client, 3, b"scramble-go-discards");
    wire.reader.set_sequence(4);
    assert_eq!(
        wire.reader.read_packet().unwrap(),
        vec![1, 4],
        "TiDB always answers fastAuthFail: it caches nothing and serves no RSA key",
    );

    write_packet(&mut wire.client, 5, b"s3cret\0");
    wire.reader.set_sequence(6);
    assert_eq!(
        wire.reader.read_packet().unwrap()[0],
        0,
        "the cleartext password matches the stored SHA-crypt envelope"
    );

    write_packet(&mut wire.client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    let contexts = factory.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].identity.username(), "sha");
}

/// The same exchange with the WRONG password is 1045, which is what proves
/// the comparison actually happens rather than the arm admitting anything.
#[test]
fn a_caching_sha2_account_refuses_the_wrong_cleartext() {
    let (address, factory, worker) = serve(store_with("sha", CACHING_SHA2, Some("s3cret")));
    let mut wire = open(address, "sha", CACHING_SHA2, b"scramble");
    AuthSwitchRequest::parse_payload(&wire.reader.read_packet().unwrap()).unwrap();
    write_packet(&mut wire.client, 3, b"scramble");
    wire.reader.set_sequence(4);
    assert_eq!(wire.reader.read_packet().unwrap(), vec![1, 4]);
    write_packet(&mut wire.client, 5, b"wrong\0");
    wire.reader.set_sequence(6);
    let error = wire.reader.read_packet().unwrap();
    assert_eq!(error[0], 0xff, "an error packet");
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1045);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::AuthenticationRejected);
    assert!(factory.contexts.lock().unwrap().is_empty());
}

/// `tidb_sm3_password` is the same protocol driven by SM3 instead of
/// SHA-256, which is exactly how Go writes it (`authSM3` is `authSha` with
/// the comment changed, and `CheckHashingPassword` switches only the digest).
#[test]
fn an_sm3_account_authenticates_through_the_same_exchange() {
    let (address, _factory, worker) = serve(store_with("sm3", TIDB_SM3, Some("sm3-pass")));
    let mut wire = open(address, "sm3", NATIVE, b"ignored");
    let switch = AuthSwitchRequest::parse_payload(&wire.reader.read_packet().unwrap()).unwrap();
    assert_eq!(switch.client_plugin, TIDB_SM3);
    write_packet(&mut wire.client, 3, b"scramble");
    wire.reader.set_sequence(4);
    assert_eq!(wire.reader.read_packet().unwrap(), vec![1, 4]);
    write_packet(&mut wire.client, 5, b"sm3-pass\0");
    wire.reader.set_sequence(6);
    assert_eq!(wire.reader.read_packet().unwrap()[0], 0);
    write_packet(&mut wire.client, 0, &[COM_QUIT]);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

#[test]
fn an_arbitrary_byte_password_authenticates_without_utf8_narrowing() {
    let password = b"not-utf8-\xff";
    let (address, _factory, worker) = serve(store_with_password_bytes(
        "bytes",
        CACHING_SHA2,
        password,
    ));
    let mut wire = open(address, "bytes", NATIVE, b"ignored");
    let switch = AuthSwitchRequest::parse_payload(&wire.reader.read_packet().unwrap()).unwrap();
    assert_eq!(switch.client_plugin, CACHING_SHA2);
    write_packet(&mut wire.client, 3, b"scramble");
    wire.reader.set_sequence(4);
    assert_eq!(wire.reader.read_packet().unwrap(), vec![1, 4]);
    let mut cleartext = password.to_vec();
    cleartext.push(0);
    write_packet(&mut wire.client, 5, &cleartext);
    wire.reader.set_sequence(6);
    assert_eq!(wire.reader.read_packet().unwrap()[0], 0);
    write_packet(&mut wire.client, 0, &[COM_QUIT]);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// An SM3 password does not open a caching_sha2 account and vice versa: the
/// two envelopes are the same SHAPE and a different digest, so nothing but
/// the digest choice distinguishes them.
#[test]
fn the_two_hashing_plugins_do_not_accept_each_other_s_hashes() {
    use tidb_session::privilege::check_hashing_password;
    let sha2 = encode_password_for_plugin(CACHING_SHA2, &PluginCredential::By("shared")).unwrap();
    let sm3 = encode_password_for_plugin(TIDB_SM3, &PluginCredential::By("shared")).unwrap();
    assert!(check_hashing_password(&sha2, b"shared", CACHING_SHA2));
    assert!(check_hashing_password(&sm3, b"shared", TIDB_SM3));
    assert!(!check_hashing_password(&sha2, b"shared", TIDB_SM3));
    assert!(!check_hashing_password(&sm3, b"shared", CACHING_SHA2));
    assert!(!check_hashing_password(&sha2, b"other", CACHING_SHA2));
    // Go's three malformed-hash error returns all reach a caller that logs
    // and treats the check as failed.
    for malformed in ["", "$A$005", "$B$005$saltsaltsaltsaltsalt", "$A$zz$salt"] {
        assert!(
            !check_hashing_password(malformed, b"shared", CACHING_SHA2),
            "{malformed} must not verify",
        );
    }
}

/// Go's issue-40831 carve-out: a PASSWORDLESS hashing account gets no
/// `fastAuthFail` at all, because the exchange makes no sense without a
/// password and confuses real clients. The login succeeds directly.
#[test]
fn a_passwordless_hashing_account_skips_the_full_auth_exchange() {
    let (address, factory, worker) = serve(store_with("bare", CACHING_SHA2, None));
    let mut wire = open(address, "bare", NATIVE, b"");
    let switch = AuthSwitchRequest::parse_payload(&wire.reader.read_packet().unwrap()).unwrap();
    assert_eq!(switch.client_plugin, CACHING_SHA2);
    // An empty answer to the switch: no password.
    write_packet(&mut wire.client, 3, b"");
    wire.reader.set_sequence(4);
    assert_eq!(
        wire.reader.read_packet().unwrap()[0],
        0,
        "an empty response must reach the OK packet, never a fastAuthFail",
    );
    write_packet(&mut wire.client, 0, &[COM_QUIT]);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
    assert_eq!(factory.contexts.lock().unwrap().len(), 1);
}
