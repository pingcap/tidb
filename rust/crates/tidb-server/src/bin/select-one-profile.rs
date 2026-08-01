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

//! Cluster-free profile of the per-statement floor.
//!
//! `SELECT 1` touches no table, no snapshot and no storage, so everything it
//! costs is charged to every statement of every workload. This measures that
//! floor two ways against the same process:
//!
//! * over a real TCP MySQL connection served by `serve_mysql_connection`, so
//!   packet read, command dispatch, result encode and packet write are all in
//!   the number a client would see; and
//! * inside the driver session, one stage at a time, so the wire half and the
//!   statement half can be told apart by subtraction rather than by guess.
//!
//! Run it with no playground: `cargo run --release --bin select-one-profile`.

use sha1::{Digest, Sha1};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Instant;
use tidb_exec::pinned_thread_pool::PinnedThreadPool;
use tidb_protocol::{PacketReader, PacketWriter, COM_QUERY, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionTracker,
    PipelineSessionFactory,
};
use tidb_session::Session;

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

const SQL: &str = "SELECT 1";

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap()
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

fn authenticate(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
    let capabilities =
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"alice");
    response.push(0);
    let auth = native_response(b"secret", &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    let mut writer = PacketWriter::with_sequence(&mut *client, 1);
    writer.write_packet(&response).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "auth OK");
}

/// Sends one `SELECT 1` and drains its whole result set.
fn round_trip(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) -> usize {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(SQL.as_bytes());
    let mut writer = PacketWriter::with_sequence(&mut *client, 0);
    writer.write_packet(&command).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    let mut packets = 0;
    // Column count, one column definition, one row, then the EOF/OK
    // terminator (DEPRECATE_EOF: an 0xfe packet shorter than nine bytes).
    let header = reader.read_packet().unwrap();
    assert_eq!(header[0], 1, "one column: {header:?}");
    packets += 1;
    loop {
        let packet = reader.read_packet().unwrap();
        packets += 1;
        if packet[0] == 0xfe && packet.len() < 9 {
            return packets;
        }
        assert_ne!(packet[0], 0xff, "SELECT 1 errored: {packet:?}");
    }
}

fn percentile(sorted: &[u128], fraction: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index] as f64 / 1000.0
}

fn report(label: &str, mut samples: Vec<u128>) {
    let total: u128 = samples.iter().sum();
    let mean = total as f64 / samples.len() as f64 / 1000.0;
    samples.sort_unstable();
    println!(
        "{label:38} mean {mean:8.3} us   p50 {:8.3}   p90 {:8.3}   p99 {:8.3}",
        percentile(&samples, 0.50),
        percentile(&samples, 0.90),
        percentile(&samples, 0.99),
    );
}

/// The wire half: a real client, a real server thread, one `SELECT 1` per
/// iteration over loopback TCP.
fn wire_floor(iterations: usize) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PipelineSessionFactory::with_accounts(store.accounts()),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    client.set_nodelay(true).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    for _ in 0..iterations / 10 {
        round_trip(&mut client, &mut reader);
    }
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        round_trip(&mut client, &mut reader);
        samples.push(start.elapsed().as_nanos());
    }
    report("TCP round trip (client-observed)", samples);

    let mut writer = PacketWriter::with_sequence(&mut client, 0);
    writer.write_packet(&[COM_QUIT]).unwrap();
    writer.flush().unwrap();
    drop(client);
    worker.join().unwrap();
}

/// The statement half: the same driver calls the text `COM_QUERY` arm makes,
/// timed one at a time inside the process.
fn session_stages(iterations: usize) {
    let mut session = Session::new();
    for _ in 0..iterations / 10 {
        let _ = session.control_transaction(SQL);
        let stmt = session.parse_statement(SQL).unwrap();
        let _ = session.apply_set_stmt(&stmt);
        let _ = Session::statement_kind_parsed(&stmt);
        let _ = session.run_with_columns(SQL).unwrap();
    }
    let mut control = Vec::with_capacity(iterations);
    let mut set = Vec::with_capacity(iterations);
    let mut kind = Vec::with_capacity(iterations);
    let mut stored = Vec::with_capacity(iterations);
    let mut run = Vec::with_capacity(iterations);
    let mut whole = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let outer = Instant::now();
        let start = Instant::now();
        session.control_transaction(SQL).unwrap();
        control.push(start.elapsed().as_nanos());
        // What `PipelineServerSession::execute_write` really does: one parse,
        // then both of its questions asked of that tree.
        let start = Instant::now();
        let stmt = session.parse_statement(SQL).unwrap();
        session.apply_set_stmt(&stmt).unwrap();
        set.push(start.elapsed().as_nanos());
        let start = Instant::now();
        let _ = Session::statement_kind_parsed(&stmt);
        kind.push(start.elapsed().as_nanos());
        let start = Instant::now();
        session.run_with_columns(SQL).unwrap();
        run.push(start.elapsed().as_nanos());
        whole.push(outer.elapsed().as_nanos());
        // Not on the pipeline path, but the convergence node calls it twice
        // per text statement, so its cost is reported next to the others.
        let start = Instant::now();
        session.statement_stored_state_change(SQL).unwrap();
        stored.push(start.elapsed().as_nanos());
    }
    report("session.control_transaction (parse)", control);
    report("session.execute_write pre-pass (one parse)", set);
    report("session.statement_kind (off that parse)", kind);
    report("session.run_with_columns (parse+plan+exec)", run);
    report("[pipeline] four stages together", whole);
    report("(cluster only) stored_state_change (parse)", stored);
}

/// The per-statement thread the cluster path pays for its read snapshot.
///
/// `StatementSnapshot::open` spawns a named thread, waits for it to report the
/// timestamp it opened at, and `finish` sends a request and joins it. PD is not
/// involved here: this measures only the thread and channel scaffolding, which
/// is the part of that cost a cluster-free run can pin.
fn statement_thread_scaffolding(iterations: usize) {
    use std::sync::mpsc;
    let mut samples = Vec::with_capacity(iterations);
    for index in 0..iterations + iterations / 10 {
        let start = Instant::now();
        let (requests, incoming) = mpsc::channel::<mpsc::Sender<u64>>();
        let (opened, opened_reply) = mpsc::channel::<u64>();
        let worker = std::thread::Builder::new()
            .name("cluster-statement-snapshot".to_owned())
            .spawn(move || {
                if opened.send(7).is_err() {
                    return;
                }
                while let Ok(reply) = incoming.recv() {
                    let _ = reply.send(0);
                }
            })
            .unwrap();
        let _start_ts = opened_reply.recv().unwrap();
        let (reply, answer) = mpsc::channel();
        requests.send(reply).unwrap();
        answer.recv().unwrap();
        drop(requests);
        worker.join().unwrap();
        if index >= iterations / 10 {
            samples.push(start.elapsed().as_nanos());
        }
    }
    report("per-statement snapshot thread (no PD)", samples);
}

/// The same open/serve/finish handshake on the real pool the cluster path now
/// uses.
///
/// This runs `PinnedThreadPool::run` itself, with a job that stands in for the
/// transaction: it reports a start timestamp, serves requests until its channel
/// closes, and returns -- which is exactly the job
/// `StatementSnapshot::open` submits, minus PD and TiKV. What it measures is
/// therefore the scaffolding the production path actually pays, not a model of
/// it.
fn pooled_thread_scaffolding(iterations: usize) {
    use std::sync::mpsc;
    let pool = PinnedThreadPool::shared();
    let mut samples = Vec::with_capacity(iterations);
    for index in 0..iterations + iterations / 10 {
        let start = Instant::now();
        let (requests, incoming) = mpsc::channel::<mpsc::Sender<u64>>();
        let (opened, opened_reply) = mpsc::channel::<u64>();
        pool.run(
            "cluster-statement-snapshot",
            Box::new(move || {
                if opened.send(7).is_err() {
                    return;
                }
                while let Ok(reply) = incoming.recv() {
                    let _ = reply.send(0);
                }
            }),
        )
        .unwrap();
        let _start_ts = opened_reply.recv().unwrap();
        let (reply, answer) = mpsc::channel();
        requests.send(reply).unwrap();
        answer.recv().unwrap();
        drop(requests);
        if index >= iterations / 10 {
            samples.push(start.elapsed().as_nanos());
        }
    }
    report("the same work on the pinned pool", samples);
}

/// What the same scaffolding costs on a thread that outlives the statement.
///
/// The difference against [`statement_thread_scaffolding`] is what a
/// connection-lifetime transaction worker would save per statement; it is the
/// ceiling on that change, since the PD timestamp would still be spent.
fn persistent_thread_scaffolding(iterations: usize) {
    use std::sync::mpsc;
    let (requests, incoming) = mpsc::channel::<mpsc::Sender<u64>>();
    let worker = std::thread::Builder::new()
        .name("persistent-statement-worker".to_owned())
        .spawn(move || {
            while let Ok(reply) = incoming.recv() {
                let _ = reply.send(0);
            }
        })
        .unwrap();
    let mut samples = Vec::with_capacity(iterations);
    for index in 0..iterations + iterations / 10 {
        let start = Instant::now();
        let (reply, answer) = mpsc::channel();
        requests.send(reply).unwrap();
        answer.recv().unwrap();
        if index >= iterations / 10 {
            samples.push(start.elapsed().as_nanos());
        }
    }
    drop(requests);
    worker.join().unwrap();
    report("same work on a persistent thread", samples);
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000);
    println!("SELECT 1 floor, {iterations} iterations, no cluster\n");
    session_stages(iterations);
    println!();
    statement_thread_scaffolding(iterations);
    pooled_thread_scaffolding(iterations);
    persistent_thread_scaffolding(iterations);
    println!();
    wire_floor(iterations);
}
