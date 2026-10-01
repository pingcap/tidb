// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Diagnostic only. Uses the existing pipeline wire fixture's public owners.
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use tidb_protocol::{PacketReader, PacketWriter, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionTracker,
    NodeConfig, PipelineSessionFactory,
};

fn write(stream: &mut TcpStream, sequence: u8, bytes: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(bytes).unwrap();
    writer.flush().unwrap();
}

fn main() {
    let args = [
        "audit",
        "--path",
        "127.0.0.1:2379",
        "--cluster-session",
        "--load-privileges",
    ];
    println!(
        "node default auto_tls: {:?}",
        NodeConfig::parse(args).map(|c| c.auto_tls)
    );
    let mut token_args = args.to_vec();
    token_args.push("--token-limit=1");
    println!(
        "token-limit flag: {:?}",
        NodeConfig::parse(token_args).map(|_| ())
    );
    let path = std::env::temp_dir().join(format!("tidb-parity-config-{}.toml", std::process::id()));
    for text in [
        "token-limit = 1\n",
        "[performance]\nstats-lease = '5s'\n",
        "[performance]\nrun-auto-analyze = false\n",
        "[security]\nssl-ca = 'ca.pem'\n",
    ] {
        std::fs::write(&path, text).unwrap();
        let mut configured: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        configured.extend(["--config".to_owned(), path.to_string_lossy().into_owned()]);
        let outcome = format!("{:?}", NodeConfig::parse(configured).map(|_| ()));
        println!(
            "config {text:?}: {}",
            outcome.replace(path.to_str().unwrap(), "<temporary TOML>")
        );
    }
    std::fs::remove_file(path).unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, peer) = listener.accept().unwrap();
        let store = ConfiguredUserStore::from_accounts(
            tidb_session::privilege::PrivilegeRegistry::default(),
        );
        serve_mysql_connection(
            stream,
            peer,
            ConnectionCancellation::default(),
            &PipelineSessionFactory::with_configured_store(&store),
            &store,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.read_packet().unwrap();
    let capabilities: u32 = (1 << 9) | (1 << 15); // protocol 4.1, secure auth framing
    let mut handshake = capabilities.to_le_bytes().to_vec();
    handshake.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    handshake.push(8); // latin1_swedish_ci
    handshake.extend_from_slice(&[0; 23]);
    handshake.extend_from_slice(b"root\0\0");
    write(&mut client, 1, &handshake);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "authentication setup");
    let mut query = vec![3]; // COM_QUERY; preserve the raw non-UTF8 byte
    query.extend_from_slice(b"SELECT HEX('\xe9')");
    write(&mut client, 0, &query);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), vec![1], "one result column");
    reader.read_packet().unwrap(); // definition
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe, "metadata EOF");
    let row = reader.read_packet().unwrap();
    println!(
        "latin1 COM_QUERY SELECT HEX(raw E9): {}",
        String::from_utf8_lossy(&row[1..])
    );
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe, "rows EOF");
    write(&mut client, 0, &[1]); // COM_QUIT
    worker.join().unwrap();
}
