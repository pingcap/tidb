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

//! `auth_socket` logins over a real Unix-domain socket: Go's
//! `checkAuthPlugin` (`server/conn.go:988`) answers for the client with the
//! peer process's OS user (`GetSockUID` + `user.LookupId`), and
//! `ConnectionVerification` accepts it when it is the account's user or its
//! authentication string.

#![cfg(unix)]
#![allow(missing_docs)]

use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use tidb_protocol::{PacketReader, PacketWriter, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit, ConnectionReport, ConnectionTracker, QueryResult, QuerySession, QuerySessionFactory, SessionContext};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_session::domain::peer_address::PeerAddress;
use tidb_privilege::PrivilegeRegistry;

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

struct Session;

impl QuerySession for Session {
    fn parse_statement(&mut self, sql: &str) -> Result<tidb_ast::Stmt, SqlQueryError> {
        Ok(tidb_parser::parse(sql)
            .unwrap_or_else(|_| tidb_parser::parse("SELECT 1").expect("a statement")))
    }

    fn execute_stmt<'a>(
        &'a mut self,
        _sql: &str,
        _stmt: &tidb_ast::Stmt,
    ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
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

/// This process's OS user, which is the peer of both socket ends.
fn os_user() -> String {
    let output = std::process::Command::new("id")
        .arg("-un")
        .output()
        .unwrap();
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn write_packet(stream: &mut UnixStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

/// Logs `user` in over a Unix socket pair with a native-password client
/// that sends no password, and returns the server's reply after the
/// handshake response with the connection's report.
fn login(users: ConfiguredUserStore, user: &str) -> (Vec<u8>, ConnectionReport, usize) {
    let (mut client, server) = UnixStream::pair().unwrap();
    let factory = Arc::new(RecordingFactory::default());
    let worker_factory = Arc::clone(&factory);
    let worker = std::thread::spawn(move || {
        serve_mysql_connection(
            server,
            PeerAddress::Unix,
            ConnectionCancellation::default(),
            worker_factory.as_ref(),
            &users,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    assert_eq!(reader.read_packet().unwrap()[0], 10, "protocol 10 greeting");
    let capabilities =
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(0);
    response.extend_from_slice(b"mysql_native_password\0");
    write_packet(&mut client, 1, &response);
    reader.set_sequence(2);
    let reply = reader.read_packet().unwrap();
    if reply[0] == 0 {
        write_packet(&mut client, 0, &[COM_QUIT]);
    }
    let report = worker.join().unwrap();
    let sessions = factory.contexts.lock().unwrap().len();
    (reply, report, sessions)
}

#[test]
fn the_peer_os_user_logs_in_as_its_own_account() {
    let accounts = PrivilegeRegistry::default();
    assert!(accounts.create_user_with_plugin(&os_user(), "%", "", "auth_socket"));
    let (reply, report, sessions) = login(ConfiguredUserStore::from_accounts(accounts), &os_user());
    assert_eq!(reply[0], 0, "no auth switch, straight to OK: {reply:?}");
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(sessions, 1);
}

#[test]
fn the_authentication_string_maps_an_account_to_an_os_user() {
    let accounts = PrivilegeRegistry::default();
    assert!(accounts.create_user_with_plugin("mapped", "%", &os_user(), "auth_socket"));
    assert!(accounts.create_user_with_plugin("other", "%", "nobody-here", "auth_socket"));
    let (reply, report, _) = login(
        ConfiguredUserStore::from_accounts(accounts.clone()),
        "mapped",
    );
    assert_eq!(reply[0], 0, "{reply:?}");
    assert_eq!(report.exit, ConnectionExit::Quit);

    let (reply, report, sessions) = login(ConfiguredUserStore::from_accounts(accounts), "other");
    assert_eq!(reply[0], 0xff, "{reply:?}");
    assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 1045);
    assert_eq!(report.exit, ConnectionExit::AuthenticationRejected);
    assert_eq!(sessions, 0);
}
