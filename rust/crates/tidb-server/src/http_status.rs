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

//! The status HTTP listener: the `/status` slice of Go's
//! `pkg/server/http_status.go`.
//!
//! Go's status server mounts dozens of handlers; the one every client and
//! `cmd/tidb-server`'s own `main_test.go` reach first is `/status`
//! (`http_status.go:689`), whose body is the `Status` struct
//! (`:675`): `{connections, version, git_hash, status:{init_stats_percentage}}`
//! in exactly that field order. This module serves that endpoint over a
//! hand-rolled HTTP/1.1 loop and answers 404 for every other path.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `/metrics` (the prometheus registry) and the rest of the router —
//!   `/settings`, `/schema`, pprof — are unported handlers; they answer 404
//!   here where Go serves them.
//! * `s.health.Load()` — the 500-during-shutdown arm — narrows with the
//!   graceful-shutdown integration; this listener lives for the process.
//! * `initstats.InitStatsPercentage` reads 100 here: this node loads its
//!   statistics before the SQL listener opens, which is the state Go's
//!   gauge reports as 100 once init completes.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use crate::sql_node::ConnectionTracker;

/// A running status listener; the accept thread lives for the process,
/// as Go's does until shutdown.
pub struct StatusServer {
    local_addr: std::net::SocketAddr,
}

impl StatusServer {
    /// The bound address, for logs and tests.
    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }
}

/// Binds and serves `GET /status` in a background thread.
pub fn start_status_listener(
    host: &str,
    port: u16,
    tracker: Arc<ConnectionTracker>,
    version: String,
    git_hash: String,
) -> std::io::Result<StatusServer> {
    let listener = TcpListener::bind((host, port))?;
    let local_addr = listener.local_addr()?;
    std::thread::Builder::new()
        .name("tidb-status-http".to_owned())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let tracker = Arc::clone(&tracker);
                let version = version.clone();
                let git_hash = git_hash.clone();
                // One short-lived thread per request keeps the accept loop
                // from stalling on a slow client without an executor.
                let _ = std::thread::Builder::new()
                    .name("tidb-status-conn".to_owned())
                    .spawn(move || {
                        let mut buffer = [0_u8; 4096];
                        let Ok(read) = stream.read(&mut buffer) else {
                            return;
                        };
                        let request = String::from_utf8_lossy(&buffer[..read]);
                        let path = request
                            .lines()
                            .next()
                            .and_then(|line| line.split_whitespace().nth(1))
                            .unwrap_or("");
                        let response = if path == "/status" {
                            // Go `handleStatus`: the Status struct's field
                            // order, `Content-Type: application/json`.
                            let body = format!(
                                "{{\"connections\":{},\"version\":\"{}\",\"git_hash\":\"{}\",\
                                 \"status\":{{\"init_stats_percentage\":100}}}}",
                                tracker.active(),
                                version,
                                git_hash,
                            );
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len(),
                            )
                        } else {
                            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\
                             Connection: close\r\n\r\n"
                                .to_owned()
                        };
                        let _ = stream.write_all(response.as_bytes());
                    });
            }
        })?;
    Ok(StatusServer { local_addr })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_answers_gos_shape_and_other_paths_answer_404() {
        let tracker = Arc::new(ConnectionTracker::default());
        let server = start_status_listener(
            "127.0.0.1",
            0,
            Arc::clone(&tracker),
            "8.0.11-TiDB-test".to_owned(),
            "abc123".to_owned(),
        )
        .expect("binds");
        let addr = server.local_addr();

        let fetch = |path: &str| {
            let mut stream = std::net::TcpStream::connect(addr).expect("connects");
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                .expect("writes");
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("reads");
            response
        };

        let status = fetch("/status");
        assert!(status.starts_with("HTTP/1.1 200 OK"), "{status}");
        assert!(
            status.contains(
                "{\"connections\":0,\"version\":\"8.0.11-TiDB-test\",\"git_hash\":\"abc123\",\
                 \"status\":{\"init_stats_percentage\":100}}"
            ),
            "Go's field order, exactly: {status}"
        );
        assert!(fetch("/nosuch").starts_with("HTTP/1.1 404"));
    }
}
