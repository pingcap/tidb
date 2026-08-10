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

//! TiProxy discovery and the TiDB HTTP endpoints it consumes.
//!
//! Go TiDB publishes `/topology/tidb/<sql-address>/info` without a lease and
//! `/topology/tidb/<sql-address>/ttl` under a 45-second etcd lease. TiProxy
//! admits an address only while both keys exist, then checks `/status` before
//! opening the MySQL port. This module owns that exact process-lifetime
//! contract for the cluster-session server.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_pd_client::EtcdClient;

use crate::node_config::NodeConfig;
use crate::sql_node::ConnectionTracker;

const TOPOLOGY_ROOT: &str = "/topology/tidb";
const TOPOLOGY_LEASE_TTL_SECONDS: i64 = 45;
const TOPOLOGY_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const TOPOLOGY_REPUBLISH_EVERY_KEEP_ALIVES: u8 = 2;
const TOPOLOGY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STATUS_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_STATUS_REQUEST_BYTES: usize = 4096;
const STATUS_VERSION: &str = "5.7.25-TiDB-Rust";

/// Process-owned TiDB topology publication.
pub(crate) struct TopologyPublisher {
    shutdown: mpsc::Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl TopologyPublisher {
    /// Publishes this SQL node before returning and then retains its lease.
    pub(crate) fn start(
        config: &NodeConfig,
        sql_port: u16,
        status_port: u16,
    ) -> Result<Self, String> {
        let advertise_ip = resolve_advertise_ip(config)?;
        let sql_address = SocketAddr::new(advertise_ip, sql_port).to_string();
        let prefix = format!("{TOPOLOGY_ROOT}/{sql_address}");
        let info_key = format!("{prefix}/info");
        let ttl_key = format!("{prefix}/ttl");
        let info = topology_info(advertise_ip, status_port)?;
        let client = EtcdClient::connect_with_security(
            config.pd_endpoints.iter(),
            super::CONTROL_PLANE_TIMEOUT,
            Arc::new(config.cluster_security.clone()),
        )
        .map_err(|error| format!("connect topology etcd client: {error}"))?;
        let lease_id = establish_topology_lease(&client, &info_key, &info, &ttl_key)
            .map_err(|error| format!("publish TiDB topology for {sql_address}: {error}"))?;
        let (shutdown, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("tidb-topology".to_owned())
            .spawn(move || {
                refresh_topology_loop(client, receiver, prefix, info_key, info, ttl_key, lease_id);
            })
            .map_err(|error| format!("start topology refresh thread: {error}"))?;
        Ok(Self {
            shutdown,
            worker: Some(worker),
        })
    }

    fn shutdown(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for TopologyPublisher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn establish_topology_lease(
    client: &EtcdClient,
    info_key: &str,
    info: &[u8],
    ttl_key: &str,
) -> Result<i64, tidb_pd_client::EtcdError> {
    let lease_id = client.grant_lease(TOPOLOGY_LEASE_TTL_SECONDS)?;
    client.put(info_key.as_bytes(), info)?;
    client.put_with_lease(
        ttl_key.as_bytes(),
        topology_timestamp().as_bytes(),
        lease_id,
    )?;
    Ok(lease_id)
}

fn refresh_topology_loop(
    client: EtcdClient,
    receiver: mpsc::Receiver<()>,
    prefix: String,
    info_key: String,
    info: Vec<u8>,
    ttl_key: String,
    initial_lease_id: i64,
) {
    let mut lease_id = Some(initial_lease_id);
    let mut successful_keep_alives = 0_u8;
    let mut delay = TOPOLOGY_KEEP_ALIVE_INTERVAL;
    loop {
        match receiver.recv_timeout(delay) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        let result = match lease_id {
            Some(id) => client.keep_alive_lease(id).and_then(|_| {
                successful_keep_alives = successful_keep_alives.saturating_add(1);
                if successful_keep_alives < TOPOLOGY_REPUBLISH_EVERY_KEEP_ALIVES {
                    return Ok(id);
                }
                successful_keep_alives = 0;
                client.put(info_key.as_bytes(), &info)?;
                client.put_with_lease(ttl_key.as_bytes(), topology_timestamp().as_bytes(), id)?;
                Ok(id)
            }),
            None => establish_topology_lease(&client, &info_key, &info, &ttl_key),
        };
        match result {
            Ok(id) => {
                lease_id = Some(id);
                delay = TOPOLOGY_KEEP_ALIVE_INTERVAL;
            }
            Err(error) => {
                eprintln!(
                    "{{\"event\":\"topology_refresh_failed\",\"error\":{:?}}}",
                    error.to_string()
                );
                lease_id = None;
                successful_keep_alives = 0;
                delay = TOPOLOGY_RETRY_INTERVAL;
            }
        }
    }

    if let Some(id) = lease_id {
        let _ = client.revoke_lease(id);
    }
    let _ = client.delete_prefix(prefix.as_bytes());
}

fn topology_info(advertise_ip: IpAddr, status_port: u16) -> Result<Vec<u8>, String> {
    let deploy_path = std::env::current_exe()
        .ok()
        .as_deref()
        .and_then(Path::parent)
        .map_or_else(String::new, |path| path.to_string_lossy().into_owned());
    serde_json::to_vec(&serde_json::json!({
        "version": STATUS_VERSION,
        "git_hash": tidb_util::versioninfo::TIDB_GIT_HASH,
        "ip": advertise_ip.to_string(),
        "status_port": status_port,
        "deploy_path": deploy_path,
        "start_timestamp": unix_seconds(),
        "labels": {},
    }))
    .map_err(|error| format!("encode topology info: {error}"))
}

fn topology_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn resolve_advertise_ip(config: &NodeConfig) -> Result<IpAddr, String> {
    if let Some(address) = config.advertise_address {
        return Ok(address);
    }
    if !config.host.is_unspecified() {
        return Ok(config.host);
    }
    let endpoint = config
        .pd_endpoints
        .first()
        .ok_or_else(|| "cannot resolve advertise address without a PD endpoint".to_owned())?;
    let destination = endpoint
        .to_socket_addrs()
        .map_err(|error| format!("resolve PD endpoint {endpoint}: {error}"))?
        .next()
        .ok_or_else(|| format!("PD endpoint {endpoint} resolved no address"))?;
    let bind_address = if destination.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_address)
        .map_err(|error| format!("bind advertise route probe: {error}"))?;
    socket
        .connect(destination)
        .map_err(|error| format!("resolve route to PD {destination}: {error}"))?;
    let address = socket
        .local_addr()
        .map_err(|error| format!("read advertise route address: {error}"))?
        .ip();
    if address.is_unspecified() {
        return Err("route to PD resolved an unspecified advertise address".to_owned());
    }
    Ok(address)
}

/// Minimal TiDB-compatible status service used by TiProxy health and metric checks.
pub(crate) struct StatusServer {
    address: SocketAddr,
    shutdown: mpsc::Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl StatusServer {
    /// Binds the configured status listener and starts serving `/status`,
    /// `/config`, and `/metrics` before returning.
    pub(crate) fn start(
        host: IpAddr,
        port: u16,
        tracker: Arc<ConnectionTracker>,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind((host, port))
            .map_err(|error| format!("bind status server {host}:{port}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("configure status listener: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("read status listener address: {error}"))?;
        let (shutdown, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("tidb-status".to_owned())
            .spawn(move || serve_status(listener, receiver, tracker))
            .map_err(|error| format!("start status server thread: {error}"))?;
        Ok(Self {
            address,
            shutdown,
            worker: Some(worker),
        })
    }

    /// Operating-system-selected address, including an ephemeral test port.
    pub(crate) const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    fn shutdown(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for StatusServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn serve_status(
    listener: TcpListener,
    shutdown: mpsc::Receiver<()>,
    tracker: Arc<ConnectionTracker>,
) {
    loop {
        if shutdown.try_recv().is_ok() {
            return;
        }
        match listener.accept() {
            Ok((mut stream, _)) => serve_status_connection(&mut stream, &tracker),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                match shutdown.recv_timeout(STATUS_POLL_INTERVAL) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            Err(error) => {
                eprintln!(
                    "{{\"event\":\"status_accept_failed\",\"error\":{:?}}}",
                    error.to_string()
                );
                return;
            }
        }
    }
}

fn serve_status_connection(stream: &mut TcpStream, tracker: &ConnectionTracker) {
    let _ = stream.set_read_timeout(Some(STATUS_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(STATUS_IO_TIMEOUT));
    let mut request = [0_u8; MAX_STATUS_REQUEST_BYTES];
    let Ok(length) = stream.read(&mut request) else {
        return;
    };
    let first_line = String::from_utf8_lossy(&request[..length])
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    let path = first_line.split_ascii_whitespace().nth(1).unwrap_or("");
    let (status, content_type, body) = match path {
        "/status" => (
            "200 OK",
            "application/json",
            serde_json::json!({
                "connections": tracker.active(),
                "version": STATUS_VERSION,
                "git_hash": tidb_util::versioninfo::TIDB_GIT_HASH,
            })
            .to_string(),
        ),
        "/config" => (
            "200 OK",
            "application/json",
            serde_json::json!({
                "security": {"session-token-signing-cert": ""}
            })
            .to_string(),
        ),
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            format!(
                "# HELP tidb_server_connections Number of client connections.\n\
                 # TYPE tidb_server_connections gauge\n\
                 tidb_server_connections {}\n",
                tracker.active()
            ),
        ),
        _ => ("404 Not Found", "application/json", "{}".to_owned()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fetch(address: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    #[test]
    fn status_server_answers_the_tiproxy_health_and_metrics_paths() {
        let tracker = Arc::new(ConnectionTracker::default());
        let server = StatusServer::start(IpAddr::from([127, 0, 0, 1]), 0, tracker).unwrap();

        let status = fetch(server.local_addr(), "/status");
        assert!(status.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(status.contains("\"connections\":0"));
        assert!(status.contains("\"version\":\"5.7.25-TiDB-Rust\""));

        let config = fetch(server.local_addr(), "/config");
        assert!(config.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(config.contains("\"session-token-signing-cert\":\"\""));

        let metrics = fetch(server.local_addr(), "/metrics");
        assert!(metrics.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(metrics.contains("Content-Type: text/plain; version=0.0.4\r\n"));
        assert!(metrics.contains("tidb_server_connections 0\n"));
        assert!(fetch(server.local_addr(), "/missing").starts_with("HTTP/1.1 404 Not Found\r\n"));
    }

    #[test]
    fn topology_key_and_info_match_go_and_tiproxy_shapes() {
        let ip = IpAddr::from([10, 0, 0, 8]);
        let address = SocketAddr::new(ip, 4000).to_string();
        assert_eq!(
            format!("{TOPOLOGY_ROOT}/{address}/info"),
            "/topology/tidb/10.0.0.8:4000/info"
        );
        let info: serde_json::Value =
            serde_json::from_slice(&topology_info(ip, 10080).unwrap()).unwrap();
        assert_eq!(info["ip"], "10.0.0.8");
        assert_eq!(info["status_port"], 10080);
        assert!(info["start_timestamp"].as_u64().unwrap() > 0);
        assert!(info["labels"].as_object().unwrap().is_empty());
    }
}
