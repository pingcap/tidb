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

//! Go TestTiDBClusterConfig's endpoint, row, filtering and warning contracts.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};
use tidb_domain::cluster_topology::ClusterServer;
use tidb_exec::cluster_config::ClusterConfigClient;

// Accept every expected concurrent request before responding in reverse order.
// The deadline makes an accidentally serial fetch fail instead of hanging.
fn http_batch(
    count: usize,
    reply: fn(&str) -> (&'static str, &'static str),
) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();
    let task = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut streams = Vec::new();
        let mut requests = Vec::new();
        while streams.len() < count && Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert!(request
                        .to_ascii_lowercase()
                        .contains("pd-allow-follower-handle: true"));
                    requests.push(request.clone());
                    streams.push((stream, request));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(error) => panic!("{error}"),
            }
        }
        assert_eq!(
            streams.len(),
            count,
            "all per-node requests run concurrently"
        );
        for (mut stream, request) in streams.into_iter().rev() {
            let (status, body) = reply(&request);
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
        requests
    });
    (address, task)
}

fn server(kind: &str, status: &str) -> ClusterServer {
    ClusterServer {
        server_type: kind.into(),
        address: format!("{kind}:4000"),
        status_address: status.into(),
        ..Default::default()
    }
}

#[test]
fn cluster_config_routes_all_go_components_and_keeps_ordered_live_rows() {
    let (address, task) = http_batch(8, |_| {
        (
            "200 OK",
            r#"{"z":"plain<>&","a":{"b":true},"array":[1.0,"<>&"],"large":9007199254740993,"null":null,"empty":{},"performance":{"INDEX-USAGE-SYNC-LEASE":"0s"},"prepared-plan-cache":{"enabled":true}} trailing bytes"#,
        )
    });
    let kinds = [
        "tidb",
        "tikv",
        "tiflash",
        "tiproxy",
        "pd",
        "ticdc",
        "tso",
        "scheduling",
    ];
    let servers: Vec<_> = kinds.iter().map(|kind| server(kind, &address)).collect();
    let client = ClusterConfigClient::new(&Default::default()).unwrap();
    let mut warnings = Vec::new();
    let rows = client.fetch(&servers, &mut warnings);
    assert!(warnings.is_empty(), "{warnings:?}");
    let text: Vec<Vec<String>> = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| String::from_utf8(value.to_bytes().unwrap()).unwrap())
                .collect()
        })
        .collect();
    assert_eq!(text.len(), 8 * 5);
    for (kind, rows) in kinds.into_iter().zip(text.chunks(5)) {
        assert!(rows
            .iter()
            .all(|row| row[0] == kind && row[1] == format!("{kind}:4000")));
        assert_eq!(
            rows.iter()
                .map(|row| (&*row[2], &*row[3]))
                .collect::<Vec<_>>(),
            [
                ("a.b", "true"),
                ("array", r#"[1,"\u003c\u003e\u0026"]"#),
                ("large", "9007199254740992"),
                ("null", "null"),
                ("z", "plain<>&")
            ]
        );
    }
    let requests = task.join().unwrap();
    for path in [
        "/config",
        "/pd/api/v1/config",
        "/api/admin/config?format=json",
        "/tso/api/v1/config",
        "/scheduling/api/v1/config",
    ] {
        assert!(requests
            .iter()
            .any(|request| request.starts_with(&format!("GET {path} HTTP/1.1"))));
    }
}

#[test]
fn cluster_config_partial_failures_warn_without_discarding_other_nodes() {
    let (address, task) = http_batch(3, |request| {
        if request.starts_with("GET /pd/") {
            ("503 Service Unavailable", "unavailable")
        } else if request.starts_with("GET /tso/") {
            ("200 OK", "[1,2]")
        } else {
            ("200 OK", r#"{"live":"yes"}"#)
        }
    });
    let client = ClusterConfigClient::new(&Default::default()).unwrap();
    let mut warnings = Vec::new();
    let rows = client.fetch(
        &[
            server("pd", &address),
            server("tso", &address),
            server("tidb", &address),
            server("tikv", ""),
            server("tiflash_compute", &address),
        ],
        &mut warnings,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(warnings.len(), 4, "{warnings:?}");
    assert!(warnings
        .iter()
        .any(|w| w.contains("503 Service Unavailable")));
    assert!(warnings
        .iter()
        .any(|w| w.contains("does not contain status address")));
    assert!(warnings
        .iter()
        .any(|w| w.contains("do not support get config from node type: tiflash_compute")));
    task.join().unwrap();
}
