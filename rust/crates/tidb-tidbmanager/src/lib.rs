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

//! Go `pkg/tidbmanager`: the client to communicate with the TiDB manager
//! server.

use std::time::Duration;

const FREE_REQ_PATH: &str = "/api/tidb/free";

/// Go `DefaultTimeout`: the default timeout for manager requests.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Go `Client`: the client to communicate with the TiDB manager server.
pub trait Client: Send + Sync {
    /// Go `Free`: notifies the manager that the pod is ready to be reused.
    /// `timeout` is the caller context's remaining time.
    ///
    /// # Errors
    ///
    /// The request failed or the manager answered a status other than 200.
    fn free(&self, timeout: Duration, exit_reason: &str) -> Result<(), String>;
}

struct ManagerClient {
    http_client: reqwest::blocking::Client,
    manager_addr: String,
    pod_name: String,
    pod_ip: String,
    namespace: String,
}

/// Go `NewClient`: `tls` is the client configured with Go's `tlsConfig`
/// (`None`: plain HTTP).
#[must_use]
pub fn new_client(
    addr: &str,
    tls: Option<reqwest::blocking::ClientBuilder>,
    pod_name: &str,
    pod_ip: &str,
    namespace: &str,
) -> Box<dyn Client> {
    let (scheme, builder) = match tls {
        Some(builder) => ("https://", builder),
        None => ("http://", reqwest::blocking::Client::builder()),
    };
    Box::new(ManagerClient {
        http_client: builder.timeout(DEFAULT_TIMEOUT).build().expect("the manager HTTP client"),
        manager_addr: format!("{scheme}{}", addr.trim_end_matches('/')),
        pod_name: pod_name.to_owned(),
        pod_ip: pod_ip.to_owned(),
        namespace: namespace.to_owned(),
    })
}

impl Client for ManagerClient {
    fn free(&self, timeout: Duration, exit_reason: &str) -> Result<(), String> {
        // Go `url.Values.Encode`: keys sorted, values `QueryEscape`d.
        let query = [
            ("normal_restart_log", exit_reason),
            ("ns", self.namespace.as_str()),
            ("pod_ip", self.pod_ip.as_str()),
            ("pod_name", self.pod_name.as_str()),
        ]
        .iter()
        .map(|(key, value)| format!("{key}={}", query_escape(value)))
        .collect::<Vec<_>>()
        .join("&");
        let response = self
            .http_client
            .put(format!("{}{FREE_REQ_PATH}?{query}", self.manager_addr))
            .timeout(timeout.min(DEFAULT_TIMEOUT))
            .send()
            .map_err(|error| format!("free request failed: {error}"))?;
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            let status = format!("{} {}", status.as_u16(), status.canonical_reason().unwrap_or(""));
            return match response.bytes() {
                Ok(body) => {
                    Err(format!("free response failed: {status}, err: {}", String::from_utf8_lossy(body.as_ref())))
                }
                Err(error) => {
                    Err(format!("free response failed: {status}, read body failed: {error}, partial body: "))
                }
            };
        }
        Ok(())
    }
}

use tidb_go_url::query_escape_string as query_escape;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// One request answered with `response`; answers the request's first line.
    fn serve_once(response: &'static str) -> (String, std::thread::JoinHandle<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let n = stream.read(&mut buf).unwrap();
            stream.write_all(response.as_bytes()).unwrap();
            String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or_default().to_owned()
        });
        (addr, handle)
    }

    /// Go `TestFree`.
    #[test]
    fn test_free() {
        let (addr, server) = serve_once("HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        let client = new_client(&format!("{addr}/"), None, "tidb-0", "10.0.0.1", "ns");
        client.free(DEFAULT_TIMEOUT, "received exit request").unwrap();
        let line = server.join().unwrap();
        assert!(line.starts_with("PUT /api/tidb/free?"), "{line}");
        for part in ["pod_name=tidb-0", "pod_ip=10.0.0.1", "ns=ns", "normal_restart_log=received+exit+request"] {
            assert!(line.contains(part), "{line}");
        }
    }

    /// Go `TestFreeReturnsErrorOnNonOK`.
    #[test]
    fn test_free_returns_error_on_non_ok() {
        let (addr, server) = serve_once(
            "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 4\r\nconnection: close\r\n\r\nboom",
        );
        let client = new_client(&addr, None, "tidb-0", "10.0.0.1", "ns");
        let error = client.free(DEFAULT_TIMEOUT, "").unwrap_err();
        server.join().unwrap();
        assert_eq!(error, "free response failed: 500 Internal Server Error, err: boom");
    }

    /// Go `TestFreeReturnsBodyReadError`.
    #[test]
    fn test_free_returns_body_read_error() {
        let (addr, server) = serve_once(
            "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 10\r\nconnection: close\r\n\r\npartial",
        );
        let client = new_client(&addr, None, "tidb-0", "10.0.0.1", "ns");
        let error = client.free(DEFAULT_TIMEOUT, "").unwrap_err();
        server.join().unwrap();
        assert!(error.starts_with("free response failed: 500 Internal Server Error, read body failed: "), "{error}");
    }
}
