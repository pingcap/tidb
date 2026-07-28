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

//! End-to-end over TCP: `CREATE USER` + `GRANT` from a root connection, a
//! SECOND real connection authenticating as the newly granted user, and
//! `SHOW GRANTS` / `SHOW PROCESSLIST` / `information_schema.PROCESSLIST` /
//! `KILL` proved through the real handshake/auth/COM_QUERY wire path rather
//! than one in-process `Session`.
//!
//! This closes a specific gap: privilege-registry unit tests
//! (`tidb_session::lib`) run everything through ONE `Session` object sharing
//! Rust references directly, which can never catch a wire-level seam -- for
//! example two independently authenticated connections not actually sharing
//! the same `PrivilegeRegistry`/`ProcessRegistry`, or a privilege grant not
//! reaching an already-open peer connection because the wire layer cached
//! something at login time.
//!
//! Auth-store/privilege-registry reconciliation (documented, not a bug):
//! `ConfiguredUserStore` (this crate's stand-in for Go's `mysql.user` auth
//! columns) is a static, file-loaded, immutable-at-runtime catalog -- see
//! `configured_user_store.rs`. `CREATE USER` only ever touches the SEPARATE
//! `PrivilegeRegistry` (`tidb_session::privilege`), and `CREATE USER ...
//! IDENTIFIED BY` is explicitly unsupported
//! (`DriverError::Unsupported("CREATE USER ... IDENTIFIED BY/WITH is not
//! supported yet")`), so this tier has no live API that lets a freshly
//! `CREATE USER`'d account authenticate with a chosen password. The two
//! stores are reconciled the same way Go's single `mysql.user` table
//! naturally is: an account must already have a row in `ConfiguredUserStore`
//! (pre-provisioned, exactly as an operator would provision a real account)
//! before it can log in at all, and `PipelineSessionFactory::open_session`
//! auto-seeds a matching `PrivilegeRegistry` entry on that account's first
//! login (`pipeline_session.rs`, "seed the account on first login"), which is
//! a no-op once `CREATE USER` already ran. Because `bob` here is
//! `CREATE USER`'d (and `GRANT`ed) BEFORE its first login, the auto-seed
//! never overwrites the grants -- this is the same ordering constraint real
//! MySQL/TiDB has (you cannot `CREATE USER` an account that can already log
//! in, because login implies a `mysql.user` row already exists).
use sha1::{Digest, Sha1};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tidb_protocol::{PacketReader, PacketWriter, COM_QUERY, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker, PipelineSessionFactory,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

/// `root`/`rootpw` and `bob`/`bobpw`, both pre-provisioned so either can
/// authenticate over the real wire -- see the module doc for why `bob` must
/// be listed here even though `bob` is created by SQL, not by this file.
fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "root\t%\tmysql_native_password\t*79D0CF9A6A052105DA1E1181406C34FC87AAC89D\n\
         bob\t%\tmysql_native_password\t*6793F32F5FAF66A40EFA6B5E9887765E983829BC\n",
    )
    .unwrap()
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_salt(initial: &[u8]) -> [u8; 20] {
    assert_eq!(initial[0], 10);
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

fn authenticate(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, user: &str, password: &[u8]) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    let auth = native_response(password, &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    write_packet(client, 1, &response);
    reader.set_sequence(2);
    let ok = reader.read_packet().unwrap();
    assert_eq!(ok[0], 0, "auth OK for {user}: {ok:?}");
}

fn read_length_encoded_string(packet: &mut &[u8]) -> Vec<u8> {
    let first = packet[0];
    let (length, header) = match first {
        0xfb => (0, 1),
        0xfc => (
            usize::from(u16::from_le_bytes([packet[1], packet[2]])),
            3,
        ),
        other => (usize::from(other), 1),
    };
    let value = packet[header..header + length].to_vec();
    *packet = &packet[header + length..];
    value
}

fn read_text_value(packet: &mut &[u8]) -> String {
    String::from_utf8_lossy(&read_length_encoded_string(packet)).into_owned()
}

/// Sends one write/DDL COM_QUERY and returns the OK packet's affected-row
/// count, failing loudly (with the ERR payload) if the server answered an
/// error instead.
fn run_write(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str) -> u64 {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(
        packet[0], 0x00,
        "{sql} expected an OK packet, got: {packet:?}"
    );
    u64::from(packet[1])
}

/// Sends one COM_QUERY expected to fail, and returns `(error_code, message)`
/// read straight off the ERR packet's wire bytes.
fn run_query_expect_error(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> (u16, String) {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(packet[0], 0xff, "{sql} expected an ERR packet: {packet:?}");
    let code = u16::from_le_bytes([packet[1], packet[2]]);
    // Byte 3 is the '#' SQLSTATE marker, bytes 4..9 the 5-byte SQLSTATE, the
    // rest is the human-readable message.
    let message = String::from_utf8_lossy(&packet[9..]).into_owned();
    (code, message)
}

/// `CREATE USER`/`GRANT`/`REVOKE` are Go `AdminStmt`s that answer over the
/// wire as a one-column, one-row `affected_rows` result set rather than an
/// OK packet (`tidb_session::Session::statement_kind` classifies every
/// `AdminStmt` other than `KILL` as `StmtKind::Query`,
/// `pipeline_session.rs`'s `affected_rows_source` shapes the reply) -- unlike
/// `KILL`, which Go answers with a real OK packet and this tier special-cases
/// the same way.
fn run_admin(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str) -> u64 {
    let rows = run_query(client, reader, sql);
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    assert_eq!(rows[0].len(), 1, "{sql}: {rows:?}");
    rows[0][0].parse().unwrap()
}

/// Sends one COM_QUERY and reads its (deprecate-EOF) text result set.
fn run_query(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str) -> Vec<Vec<String>> {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);

    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "{sql} errored: {first:?}");
    let column_count = usize::from(first[0]);
    assert!(column_count > 0, "the pipeline answers with result sets");
    for _ in 0..column_count {
        let _column_definition = reader.read_packet().unwrap();
    }
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            break;
        }
        let mut remaining = packet.as_slice();
        let mut row = Vec::new();
        for _ in 0..column_count {
            row.push(read_text_value(&mut remaining));
        }
        rows.push(row);
    }
    rows
}

/// The full grants lifecycle over TWO real, independently authenticated TCP
/// connections sharing one server (one `PipelineSessionFactory`, exactly as
/// one TiDB instance shares one `Domain`):
///
/// root `CREATE USER` + a global `GRANT PROCESS` + a scoped `GRANT SELECT ON
/// test.*` -> bob connects and authenticates for the FIRST time (proving the
/// auth-store/privilege-registry seam is reconciled, see the module doc) ->
/// `SHOW GRANTS` reports bob's own lines -> `SHOW PROCESSLIST` /
/// `information_schema.PROCESSLIST` are gated live by `PROCESS`, INCLUDING
/// changes root makes to an already-open peer session (`GRANT`/`REVOKE`
/// after bob's login already changes what bob's NEXT query sees -- there is
/// no session-scoped privilege cache to invalidate) -> `KILL` of root's
/// connection is refused with 1227 until root grants bob `SUPER`, after
/// which the same `KILL` succeeds and really ends root's connection.
#[test]
fn grant_process_and_scoped_select_are_visible_and_live_across_real_connections() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    // One factory serves both connections: that is what gives them one
    // shared `PrivilegeRegistry` and one shared `ProcessRegistry`, as one
    // TiDB instance has one `Domain`.
    let factory = Arc::new(PipelineSessionFactory::default());
    let store = Arc::new(users());

    let acceptor_factory = Arc::clone(&factory);
    let acceptor_store = Arc::clone(&store);
    let acceptor_tracker = Arc::clone(&tracker);
    let acceptor = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..2 {
            let (stream, peer_addr) = listener.accept().unwrap();
            let factory = Arc::clone(&acceptor_factory);
            let store = Arc::clone(&acceptor_store);
            let tracker = Arc::clone(&acceptor_tracker);
            workers.push(std::thread::spawn(move || {
                serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    factory.as_ref(),
                    store.as_ref(),
                    &tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                )
                .unwrap()
            }));
        }
        workers
    });

    // root connects first and provisions bob BEFORE bob ever logs in --
    // see the module doc for why that ordering matters.
    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");

    assert_eq!(
        run_write(&mut root, &mut root_reader, "CREATE USER 'bob'@'%'"),
        0
    );
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT SELECT ON test.* TO 'bob'@'%'"),
        0
    );

    // bob's FIRST connection: authenticates against the pre-provisioned
    // `ConfiguredUserStore` entry, and lands in a `PrivilegeRegistry` account
    // that root's `CREATE USER`/`GRANT` already populated.
    let mut bob = TcpStream::connect(address).unwrap();
    let bob_read = bob.try_clone().unwrap();
    let mut bob_reader = PacketReader::new(bob_read);
    authenticate(&mut bob, &mut bob_reader, "bob", b"bobpw");

    let grants = run_query(&mut bob, &mut bob_reader, "SHOW GRANTS");
    assert_eq!(
        grants,
        vec![
            vec!["GRANT USAGE ON *.* TO 'bob'@'%'".to_owned()],
            vec!["GRANT SELECT ON `test`.* TO 'bob'@'%'".to_owned()],
        ],
        "bob's own scoped grant, over the real wire: {grants:?}"
    );

    // Without PROCESS, bob's own SHOW PROCESSLIST / information_schema query
    // sees only bob's own connection -- root's row is hidden.
    let before = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(before.len(), 1, "bob has no PROCESS yet: {before:?}");
    assert_eq!(before[0][1], "bob");
    let before_schema = run_query(
        &mut bob,
        &mut bob_reader,
        "SELECT id, user FROM information_schema.processlist",
    );
    assert_eq!(before_schema.len(), 1, "{before_schema:?}");

    // root grants PROCESS to the ALREADY-OPEN bob connection -- no
    // reconnect, no cache to invalidate: the very next query on bob's
    // existing socket sees the change.
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT PROCESS ON *.* TO 'bob'@'%'"),
        0
    );
    let after = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(
        after.len(),
        2,
        "PROCESS makes root's connection visible too, live: {after:?}"
    );
    let root_row = after
        .iter()
        .find(|row| row[1] == "root")
        .unwrap_or_else(|| panic!("root's row should now be visible: {after:?}"));
    let root_connection_id: u64 = root_row[0].parse().unwrap();
    let after_schema = run_query(
        &mut bob,
        &mut bob_reader,
        "SELECT id, user FROM information_schema.processlist",
    );
    assert_eq!(after_schema.len(), 2, "{after_schema:?}");

    // root REVOKEs PROCESS: bob's visibility shrinks back on the same
    // already-open connection.
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "REVOKE PROCESS ON *.* FROM 'bob'@'%'"),
        0
    );
    let revoked = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(revoked.len(), 1, "PROCESS revoked, live: {revoked:?}");

    // Without SUPER (or CONNECTION_ADMIN, not modelled), bob cannot KILL
    // root's connection -- Go's `planbuilder.go` `*ast.KillStmt` case reports
    // `ErrSpecificAccessDenied` (1227), not the unused 1095 `ErrKillDenied`.
    let (code, message) = run_query_expect_error(
        &mut bob,
        &mut bob_reader,
        &format!("KILL {root_connection_id}"),
    );
    assert_eq!(code, 1227, "{message}");
    assert!(
        message.contains("SUPER or CONNECTION_ADMIN"),
        "{message}"
    );

    // Once root grants SUPER, the SAME kill from the SAME connection
    // succeeds and really ends root's connection.
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT SUPER ON *.* TO 'bob'@'%'"),
        0
    );
    assert_eq!(
        run_write(&mut bob, &mut bob_reader, &format!("KILL {root_connection_id}")),
        0
    );

    write_packet(&mut bob, 0, &[0x01]);
    drop(bob);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert!(
        exits.contains(&ConnectionExit::Killed),
        "root's connection reports its kill: {exits:?}"
    );
    assert!(exits.contains(&ConnectionExit::Quit), "{exits:?}");
}
