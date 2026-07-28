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
//! Auth store and privilege registry are ONE table (this used to be a
//! documented ordering constraint, and no longer is): `ConfiguredUserStore`
//! now holds the shared `tidb_session::privilege::PrivilegeRegistry` rather
//! than an immutable file snapshot, so its strict TSV is only the operator's
//! INITIAL provisioning and `CREATE USER ... IDENTIFIED BY` / `DROP USER`
//! write the very rows a login is verified against -- exactly as Go has one
//! `mysql.user` carrying both `authentication_string` and the privilege
//! columns. `PipelineSessionFactory::with_accounts(store.accounts())` is
//! what ties the wire authenticator and the SQL executor to that one table;
//! nothing seeds accounts at login time any more, because an identity the
//! handshake matched IS a registry row by construction.
//!
//! What that buys, and what the second test below proves over TCP: `bob` is
//! NOT in the provisioning file at all. Root creates it with a password at
//! runtime, and a brand-new connection then authenticates as `bob` through a
//! real handshake.
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

/// The operator's initial provisioning: `root`/`rootpw` and nothing else.
/// Every other account in this file is created at runtime through SQL.
fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "root\t%\tmysql_native_password\t*79D0CF9A6A052105DA1E1181406C34FC87AAC89D\n",
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
    let ok = try_authenticate(client, reader, user, password);
    assert_eq!(ok[0], 0, "auth OK for {user}: {ok:?}");
}

/// Runs the real handshake and returns the server's reply packet verbatim,
/// so a REJECTED login can be inspected instead of panicking.
fn try_authenticate(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &str,
    password: &[u8],
) -> Vec<u8> {
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
    reader.read_packet().unwrap()
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
/// root `CREATE USER ... IDENTIFIED BY` + a scoped `GRANT SELECT ON test.*`
/// -> bob connects and authenticates for the FIRST time with that password
/// (proving auth store and privilege registry are one table, see the module
/// doc) ->
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
    let store = Arc::new(users());
    // One table: the accounts this factory's `CREATE USER` writes are the
    // accounts `store` authenticates logins against.
    let factory = Arc::new(PipelineSessionFactory::with_accounts(store.accounts()));

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

    // root connects first and creates bob, password and all; bob exists in
    // no provisioning file.
    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");

    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "CREATE USER 'bob'@'%' IDENTIFIED BY 'bobpw'"
        ),
        0
    );
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT SELECT ON test.* TO 'bob'@'%'"),
        0
    );

    // bob's FIRST connection: authenticates against the row root's
    // `CREATE USER ... IDENTIFIED BY` just wrote, with the grants root's
    // `GRANT` put on that same row.
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

    // The SAME gate reached through a ROLE, over the same two live
    // connections. `PROCESS` lives only on the role, so what bob may see
    // follows ACTIVATION, not the grant: holding the role changes nothing
    // until bob runs `SET ROLE`, and `SET ROLE NONE` takes it away again --
    // all without reconnecting, because there is no per-session privilege
    // cache on either side.
    assert_eq!(run_write(&mut root, &mut root_reader, "CREATE ROLE watcher"), 0);
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT PROCESS ON *.* TO watcher"),
        0
    );
    assert_eq!(
        run_admin(&mut root, &mut root_reader, "GRANT watcher TO 'bob'@'%'"),
        0
    );
    let granted_not_active = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(
        granted_not_active.len(),
        1,
        "a granted but inactive role confers nothing: {granted_not_active:?}"
    );
    assert_eq!(
        run_query(&mut bob, &mut bob_reader, "SELECT CURRENT_ROLE()"),
        vec![vec!["NONE".to_owned()]]
    );
    assert_eq!(run_write(&mut bob, &mut bob_reader, "SET ROLE watcher"), 0);
    assert_eq!(
        run_query(&mut bob, &mut bob_reader, "SELECT CURRENT_ROLE()"),
        vec![vec!["`watcher`@`%`".to_owned()]]
    );
    let via_role = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(
        via_role.len(),
        2,
        "the activated role's PROCESS reaches the process list: {via_role:?}"
    );
    // The role line reaches the wire too, and the role's own privileges are
    // merged under bob's name while it is active.
    assert_eq!(
        run_query(&mut bob, &mut bob_reader, "SHOW GRANTS"),
        vec![
            vec!["GRANT PROCESS ON *.* TO 'bob'@'%'".to_owned()],
            vec!["GRANT SELECT ON `test`.* TO 'bob'@'%'".to_owned()],
            vec!["GRANT 'watcher'@'%' TO 'bob'@'%'".to_owned()],
        ]
    );
    assert_eq!(run_write(&mut bob, &mut bob_reader, "SET ROLE NONE"), 0);
    let deactivated = run_query(&mut bob, &mut bob_reader, "SHOW PROCESSLIST");
    assert_eq!(deactivated.len(), 1, "deactivated: {deactivated:?}");
    // Restore the state the rest of this test continues from.
    assert_eq!(run_write(&mut root, &mut root_reader, "DROP ROLE watcher"), 0);

    // Holding neither SUPER nor CONNECTION_ADMIN, bob cannot KILL root's
    // connection -- Go's `planbuilder.go` `*ast.KillStmt` case reports
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

    // Once root grants the DYNAMIC `CONNECTION_ADMIN` -- no SUPER anywhere --
    // the SAME kill from the SAME connection succeeds and really ends root's
    // connection. That is Go's actual gate; SUPER only passes it as the
    // dynamic-privilege fallback.
    assert_eq!(
        run_admin(
            &mut root,
            &mut root_reader,
            "GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'"
        ),
        0
    );
    // The dynamic grant crosses the wire as a trailing `SHOW GRANTS` line
    // AFTER every static scope, leaving the static global line at bare
    // `USAGE` (the earlier `PROCESS` having been revoked above).
    assert_eq!(
        run_query(&mut bob, &mut bob_reader, "SHOW GRANTS"),
        vec![
            vec!["GRANT USAGE ON *.* TO 'bob'@'%'".to_owned()],
            vec!["GRANT SELECT ON `test`.* TO 'bob'@'%'".to_owned()],
            vec!["GRANT CONNECTION_ADMIN ON *.* TO 'bob'@'%'".to_owned()],
        ]
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

/// The account LIFECYCLE over TCP, which only a real handshake can prove:
/// root creates an account with a password at runtime; a brand-new
/// connection authenticates as it; the WRONG password on an otherwise
/// identical handshake is rejected with 1045; `ALTER USER ... IDENTIFIED BY`
/// changes which password the NEXT login accepts; and after `DROP USER` the
/// account can no longer log in at all.
///
/// None of this is reachable in-process: `tidb_session::Session` never
/// verifies a password, so a registry unit test cannot tell a stored
/// `authentication_string` that actually gates logins from one that is
/// merely written down.
#[test]
fn a_runtime_created_account_can_log_in_over_tcp_until_it_is_dropped() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = Arc::new(users());
    let factory = Arc::new(PipelineSessionFactory::with_accounts(store.accounts()));

    let acceptor_factory = Arc::clone(&factory);
    let acceptor_store = Arc::clone(&store);
    let acceptor_tracker = Arc::clone(&tracker);
    // root's connection plus the five login attempts below, each of which
    // reaches the server whether or not it authenticates.
    let acceptor = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..6 {
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

    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");

    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "CREATE USER 'carol'@'%' IDENTIFIED BY 'carolpw'"
        ),
        0
    );

    // The right password authenticates: the account created by SQL a moment
    // ago is a real login.
    {
        let mut carol = TcpStream::connect(address).unwrap();
        let carol_read = carol.try_clone().unwrap();
        let mut carol_reader = PacketReader::new(carol_read);
        authenticate(&mut carol, &mut carol_reader, "carol", b"carolpw");
        let grants = run_query(&mut carol, &mut carol_reader, "SHOW GRANTS");
        assert_eq!(
            grants,
            vec![vec!["GRANT USAGE ON *.* TO 'carol'@'%'".to_owned()]],
            "carol's own row, over the real wire: {grants:?}"
        );
        write_packet(&mut carol, 0, &[0x01]);
    }

    // The wrong password on the same account is refused with 1045/28000.
    {
        let mut wrong = TcpStream::connect(address).unwrap();
        let wrong_read = wrong.try_clone().unwrap();
        let mut wrong_reader = PacketReader::new(wrong_read);
        let reply = try_authenticate(&mut wrong, &mut wrong_reader, "carol", b"wrongpw");
        assert_eq!(reply[0], 0xff, "wrong password must be an ERR: {reply:?}");
        assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 1045);
        assert_eq!(&reply[4..9], b"28000");
    }

    // `ALTER USER ... IDENTIFIED BY` moves the account to a new password:
    // the old one now fails and the new one succeeds.
    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "ALTER USER 'carol'@'%' IDENTIFIED BY 'carolpw2'"
        ),
        0
    );
    {
        let mut stale = TcpStream::connect(address).unwrap();
        let stale_read = stale.try_clone().unwrap();
        let mut stale_reader = PacketReader::new(stale_read);
        let reply = try_authenticate(&mut stale, &mut stale_reader, "carol", b"carolpw");
        assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 1045, "{reply:?}");
    }
    {
        let mut carol = TcpStream::connect(address).unwrap();
        let carol_read = carol.try_clone().unwrap();
        let mut carol_reader = PacketReader::new(carol_read);
        authenticate(&mut carol, &mut carol_reader, "carol", b"carolpw2");
        write_packet(&mut carol, 0, &[0x01]);
    }

    // `DROP USER` removes the login itself, not just the grants.
    assert_eq!(
        run_write(&mut root, &mut root_reader, "DROP USER 'carol'@'%'"),
        0
    );
    let mut dropped = TcpStream::connect(address).unwrap();
    let dropped_read = dropped.try_clone().unwrap();
    let mut dropped_reader = PacketReader::new(dropped_read);
    let reply = try_authenticate(&mut dropped, &mut dropped_reader, "carol", b"carolpw2");
    assert_eq!(
        u16::from_le_bytes([reply[1], reply[2]]),
        1045,
        "a dropped account cannot log in: {reply:?}"
    );

    write_packet(&mut root, 0, &[0x01]);
    drop(root);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert_eq!(
        exits
            .iter()
            .filter(|exit| **exit == ConnectionExit::AuthenticationRejected)
            .count(),
        3,
        "wrong password, stale password, and dropped account: {exits:?}"
    );
}

/// `CREATE USER ... IDENTIFIED WITH caching_sha2_password BY '<password>'`
/// really does create the account (it shows up in `SHOW GRANTS`), but a
/// client that authenticates the way every other login in this file does --
/// `mysql_native_password`, the only plugin the wire front end's login path
/// (`ConfiguredUserStore::authenticate_native`) actually verifies -- gets a
/// clean, ordinary 1045/28000 access-denied over the real wire. No panic, no
/// hang: the account's `authentication_string` is a `caching_sha2_password`
/// shape (`$A$...`, 70 bytes), which never parses as a native stage-two
/// hash, so the native verifier honestly fails it exactly like a wrong
/// password would.
///
/// DEFERRED (documented): this crate's wire front end always auth-switches
/// every client to `mysql_native_password` (see `mysql_connection.rs`), so
/// there is no `caching_sha2_password` SCRAMBLE exchange to attempt in the
/// first place -- the account's plugin is stored and displayed honestly, but
/// only a native login is ever tried, by construction.
#[test]
fn a_caching_sha2_password_account_creates_but_native_login_fails_cleanly() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = Arc::new(users());
    let factory = Arc::new(PipelineSessionFactory::with_accounts(store.accounts()));

    let acceptor_factory = Arc::clone(&factory);
    let acceptor_store = Arc::clone(&store);
    let acceptor_tracker = Arc::clone(&tracker);
    // root's connection plus the one native login attempt below.
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

    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");

    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "CREATE USER 'dana'@'%' IDENTIFIED WITH caching_sha2_password BY 'danapw'"
        ),
        0
    );
    // The account exists and prints like any other -- `IDENTIFIED WITH`
    // does not stop it from being a real, grantable account.
    let grants = run_query(&mut root, &mut root_reader, "SHOW GRANTS FOR 'dana'@'%'");
    assert_eq!(
        grants,
        vec![vec!["GRANT USAGE ON *.* TO 'dana'@'%'".to_owned()]],
        "a caching_sha2_password account is a real account: {grants:?}"
    );

    // A native login attempt -- with either the real password or the wrong
    // one -- is refused the same clean way, because `dana`'s stored
    // authentication_string is not native-shaped at all.
    let mut dana = TcpStream::connect(address).unwrap();
    let dana_read = dana.try_clone().unwrap();
    let mut dana_reader = PacketReader::new(dana_read);
    let reply = try_authenticate(&mut dana, &mut dana_reader, "dana", b"danapw");
    assert_eq!(reply[0], 0xff, "must be a clean ERR, not a panic: {reply:?}");
    assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 1045);
    assert_eq!(&reply[4..9], b"28000");

    write_packet(&mut root, 0, &[0x01]);
    drop(root);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert_eq!(
        exits
            .iter()
            .filter(|exit| **exit == ConnectionExit::AuthenticationRejected)
            .count(),
        1,
        "dana's native login attempt: {exits:?}"
    );
}

/// `ALTER USER ... ACCOUNT LOCK` on a LIVE, already-logged-in-once account
/// rejects the NEXT login with a distinct errno -- 3118, not the generic
/// 1045 a bad password or unknown user gets -- matching Go's
/// `pkg/privilege/privileges.ConnectionVerification`, which checks
/// `record.AccountLocked` before ever comparing the password
/// (`mysql.ErrAccountHasBeenLocked`: `"Access denied for user '%s'@'%s'.
/// Account is locked."`). `ACCOUNT UNLOCK` restores the same login
/// immediately, over the same live registry a new connection reads.
///
/// None of this is reachable in-process: only the wire front end's
/// `ConfiguredUserStore::authenticate_native` runs the lock check at all.
#[test]
fn an_account_lock_rejects_the_next_login_with_3118_and_unlock_restores_it() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = Arc::new(users());
    let factory = Arc::new(PipelineSessionFactory::with_accounts(store.accounts()));

    let acceptor_factory = Arc::clone(&factory);
    let acceptor_store = Arc::clone(&store);
    let acceptor_tracker = Arc::clone(&tracker);
    // root's connection plus: eve's first (successful) login, the locked
    // login attempt, and the post-unlock login.
    let acceptor = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..4 {
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

    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");

    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "CREATE USER 'eve'@'%' IDENTIFIED BY 'evepw'"
        ),
        0
    );

    // Before the lock, eve logs in normally.
    {
        let mut eve = TcpStream::connect(address).unwrap();
        let eve_read = eve.try_clone().unwrap();
        let mut eve_reader = PacketReader::new(eve_read);
        authenticate(&mut eve, &mut eve_reader, "eve", b"evepw");
        write_packet(&mut eve, 0, &[0x01]);
    }

    assert_eq!(
        run_write(&mut root, &mut root_reader, "ALTER USER 'eve'@'%' ACCOUNT LOCK"),
        0
    );

    // Locked: the SAME correct password is refused, with 3118/HY000, not
    // the generic 1045/28000 access-denied.
    {
        let mut eve = TcpStream::connect(address).unwrap();
        let eve_read = eve.try_clone().unwrap();
        let mut eve_reader = PacketReader::new(eve_read);
        let reply = try_authenticate(&mut eve, &mut eve_reader, "eve", b"evepw");
        assert_eq!(reply[0], 0xff, "a locked account must be an ERR: {reply:?}");
        assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 3118, "{reply:?}");
        assert_eq!(&reply[4..9], b"HY000");
        let message = String::from_utf8_lossy(&reply[9..]);
        assert_eq!(
            message, "Access denied for user 'eve'@'127.0.0.1'. Account is locked.",
            "{reply:?}"
        );
    }

    assert_eq!(
        run_write(&mut root, &mut root_reader, "ALTER USER 'eve'@'%' ACCOUNT UNLOCK"),
        0
    );

    // Unlocked: the same correct password authenticates again.
    {
        let mut eve = TcpStream::connect(address).unwrap();
        let eve_read = eve.try_clone().unwrap();
        let mut eve_reader = PacketReader::new(eve_read);
        authenticate(&mut eve, &mut eve_reader, "eve", b"evepw");
        write_packet(&mut eve, 0, &[0x01]);
    }

    write_packet(&mut root, 0, &[0x01]);
    drop(root);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert_eq!(
        exits
            .iter()
            .filter(|exit| **exit == ConnectionExit::AuthenticationRejected)
            .count(),
        1,
        "only the locked-account login attempt is rejected: {exits:?}"
    );
}

/// `FAILED_LOGIN_ATTEMPTS` / `PASSWORD_LOCK_TIME` auto-lock, proven over real
/// TCP connections because that is the only place it exists: the counter is
/// bumped by the wire front end's authenticator, not by any statement.
///
/// Two wrong passwords against a `FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME
/// 3` account lock it -- the FIRST reporting the ordinary 1045 and the SECOND
/// 3955 -- after which even the RIGHT password gets 3955 with Go's captured
/// sentence, and `ALTER USER ... ACCOUNT UNLOCK` from another connection
/// restores the login over the same live registry.
#[test]
fn failed_login_attempts_auto_lock_reports_3955_over_the_wire_until_unlocked() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = Arc::new(users());
    let factory = Arc::new(PipelineSessionFactory::with_accounts(store.accounts()));

    let acceptor_factory = Arc::clone(&factory);
    let acceptor_store = Arc::clone(&store);
    let acceptor_tracker = Arc::clone(&tracker);
    // root's connection plus mallory's four attempts: two wrong passwords,
    // the right password while locked, and the right password after unlock.
    let acceptor = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..5 {
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

    let mut root = TcpStream::connect(address).unwrap();
    let root_read = root.try_clone().unwrap();
    let mut root_reader = PacketReader::new(root_read);
    authenticate(&mut root, &mut root_reader, "root", b"rootpw");
    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "CREATE USER 'mallory'@'%' IDENTIFIED BY 'mallorypw' \
             FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 3"
        ),
        0
    );
    // The policy is visible where a DBA would look for it.
    assert_eq!(
        run_query(
            &mut root,
            &mut root_reader,
            "SHOW CREATE USER 'mallory'@'%'"
        )[0][0],
        "CREATE USER 'mallory'@'%' IDENTIFIED WITH 'mysql_native_password' \
         AS '*9C07CAE3178C7DCA67D1A409B633937A6ABE6125' REQUIRE NONE \
         PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT \
         PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 3"
    );

    let attempt = |password: &[u8]| {
        let mut client = TcpStream::connect(address).unwrap();
        let client_read = client.try_clone().unwrap();
        let mut reader = PacketReader::new(client_read);
        try_authenticate(&mut client, &mut reader, "mallory", password)
    };

    // Attempt 1 -- below the limit, so the ordinary access-denied.
    let reply = attempt(b"wrong");
    assert_eq!(reply[0], 0xff, "{reply:?}");
    assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 1045, "{reply:?}");

    // Attempt 2 -- reaches FAILED_LOGIN_ATTEMPTS, so it both locks the
    // account and reports 3955 with the full lock time remaining.
    let locked_message = "Access denied for user 'mallory'@'%'. Account is blocked for 3 day(s) \
                          (3 day(s) remaining) due to 2 consecutive failed logins.";
    let reply = attempt(b"wrong");
    assert_eq!(reply[0], 0xff, "{reply:?}");
    assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 3955, "{reply:?}");
    assert_eq!(&reply[4..9], b"HY000");
    assert_eq!(String::from_utf8_lossy(&reply[9..]), locked_message);

    // Attempt 3 -- the CORRECT password, refused all the same. This is the
    // whole point of the feature and the one thing a password check alone
    // could never do.
    let reply = attempt(b"mallorypw");
    assert_eq!(u16::from_le_bytes([reply[1], reply[2]]), 3955, "{reply:?}");
    assert_eq!(String::from_utf8_lossy(&reply[9..]), locked_message);

    assert_eq!(
        run_write(
            &mut root,
            &mut root_reader,
            "ALTER USER 'mallory'@'%' ACCOUNT UNLOCK"
        ),
        0
    );

    // Attempt 4 -- unlocked, so the same correct password gets in.
    {
        let mut client = TcpStream::connect(address).unwrap();
        let client_read = client.try_clone().unwrap();
        let mut reader = PacketReader::new(client_read);
        authenticate(&mut client, &mut reader, "mallory", b"mallorypw");
        write_packet(&mut client, 0, &[0x01]);
    }

    write_packet(&mut root, 0, &[0x01]);
    drop(root);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert_eq!(
        exits
            .iter()
            .filter(|exit| **exit == ConnectionExit::AuthenticationRejected)
            .count(),
        3,
        "the three refused attempts, and only those: {exits:?}"
    );
}
