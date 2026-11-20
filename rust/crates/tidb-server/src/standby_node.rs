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

//! Go `cmd/tidb-server/main.go`'s standby flow: the kernel-type checks, the
//! manager client, `WaitForActivate` over a temporary listener on the status
//! address, and the `/tidb-pool/` API on the node's status server.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use tidb_standby::{LoadKeyspaceController, Request, Response, StandbyServer};

/// Go `main`'s `standbyController`.
static CONTROLLER: OnceLock<Arc<LoadKeyspaceController>> = OnceLock::new();

/// Go `svr.StandbyController`, when the node started in standby mode.
#[must_use]
pub fn standby_controller() -> Option<&'static Arc<LoadKeyspaceController>> {
    CONTROLLER.get()
}

/// Go `main`'s check after the configuration is loaded ("we cannot add this
/// check inside config.Valid()").
///
/// # Errors
///
/// The message Go prints before exiting.
pub fn check_kernel_config() -> Result<(), &'static str> {
    let config = tidb_config::config_tree::config::get_global_config();
    if tidb_config_kerneltype::is_next_gen() && config.keyspace_name.is_empty() && !config.standby.standby_mode {
        return Err("invalid config: keyspace name or standby mode is required for nextgen TiDB");
    }
    if tidb_config_kerneltype::is_classic()
        && (!config.keyspace_name.is_empty() || config.standby.standby_mode || config.keyspace_activate_mode)
    {
        return Err(
            "invalid config: keyspace name, standby mode or keyspace-activate mode is not supported for classic TiDB",
        );
    }
    Ok(())
}

/// Go `starterParams`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StarterParams {
    manager_namespace: String,
    pod_name: String,
    pod_ip: String,
    pod_namespace: String,
}

/// Go `parseStarterAdditionalParams`.
pub(crate) fn parse_starter_additional_params(raw: &str) -> Result<StarterParams, String> {
    let mut params = StarterParams::default();
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(params);
    }
    let mut seen = std::collections::HashSet::new();
    for item in raw.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err("starter additional params contains an empty item".to_owned());
        }
        let Some((key, value)) = item.split_once('=') else {
            return Err(format!("starter additional param {item:?} must be in k=v format"));
        };
        let (key, value) = (key.trim(), value.trim());
        if key.is_empty() {
            return Err(format!("starter additional param {item:?} has an empty key"));
        }
        if value.is_empty() {
            return Err(format!("starter additional param {key:?} has an empty value"));
        }
        if !seen.insert(key.to_owned()) {
            return Err(format!("starter additional param {key:?} is duplicated"));
        }
        match key {
            "manager-namespace" => params.manager_namespace = value.to_owned(),
            "pod-name" => params.pod_name = value.to_owned(),
            "pod-ip" => params.pod_ip = value.to_owned(),
            "pod-namespace" => params.pod_namespace = value.to_owned(),
            _ => return Err(format!("unknown starter additional param {key:?}")),
        }
    }
    Ok(params)
}

/// Go `createMgrClientForStarter`.
fn create_mgr_client_for_starter(
    starter_additional_params: &str,
) -> Result<Option<Box<dyn tidb_tidbmanager::Client>>, String> {
    if !tidb_config_deploymode::is_starter() {
        return Ok(None);
    }
    let cfg = tidb_config::config_tree::config::get_global_config();
    if !cfg.starter_params.enable_manager_notifier {
        return Ok(None);
    }
    let params = parse_starter_additional_params(starter_additional_params)?;
    let mut manager_addr = cfg.starter_params.manager_addr.clone();
    if manager_addr.is_empty() {
        if params.manager_namespace.is_empty() {
            return Err(
                "manager notifier requires manager-addr config or manager-namespace in --starter-additional-params"
                    .to_owned(),
            );
        }
        manager_addr = format!("manager-server.{}.svc:8000", params.manager_namespace);
    }
    if params.pod_name.is_empty() || params.pod_ip.is_empty() || params.pod_namespace.is_empty() {
        return Err(format!(
            "manager notifier requires --starter-additional-params with pod-name, pod-ip and pod-namespace: pod-name={:?}, pod-ip={:?}, pod-namespace={:?}",
            params.pod_name, params.pod_ip, params.pod_namespace
        ));
    }
    // Go passes the cluster TLS configuration; this port's manager client
    // speaks plain HTTP until the cluster security reaches reqwest.
    Ok(Some(tidb_tidbmanager::new_client(&manager_addr, None, &params.pod_name, &params.pod_ip, &params.pod_namespace)))
}

/// Go `main`: in standby mode, creates the controller and waits for the
/// activate request, which rewrites the global configuration. Answers whether
/// the node was activated (its `NodeConfig` must then be rebuilt).
///
/// # Errors
///
/// The manager client could not be created.
pub fn wait_for_activate_if_standby(starter_additional_params: &str) -> Result<bool, String> {
    if !tidb_config::config_tree::config::get_global_config().standby.standby_mode {
        return Ok(false);
    }
    let mgr_cli = create_mgr_client_for_starter(starter_additional_params)?;
    let controller = Arc::new(LoadKeyspaceController::new(mgr_cli));
    let _ = CONTROLLER.set(Arc::clone(&controller));
    let serving = Arc::clone(&controller);
    controller.wait_for_activate(move || listen_standby(serving));
    Ok(true)
}

/// Go `main` after `WaitForActivate`: "need to validate config again in case
/// of config change via standby", then the node's configuration from the
/// activated global one.
///
/// # Errors
///
/// The activated configuration is invalid.
pub fn activated_node_config(
    previous: &tidb_domain_core::node_config::NodeConfig,
) -> Result<tidb_domain_core::node_config::NodeConfig, tidb_domain_core::node_config::NodeConfigError> {
    let global = tidb_config::config_tree::config::get_global_config();
    tidb_domain_core::node_config::NodeConfig::from_config(
        (*global).clone(),
        tidb_domain_core::node_config::NodeOptions { affinity_cpus: previous.affinity_cpus.clone() },
    )
}

/// Go `WaitForActivate`'s temporary HTTP server on the status address.
fn listen_standby(controller: Arc<LoadKeyspaceController>) -> Box<dyn FnOnce() + Send> {
    let config = tidb_config::config_tree::config::get_global_config();
    let addr = (config.status.status_host.clone(), config.status.status_port as u16);
    let listener = match std::net::TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(error) => {
            tidb_logutil::bg_logger().warn("failed to listen", &[tidb_logutil::zap_error(error.to_string())]);
            std::process::exit(1);
        }
    };
    let local = listener.local_addr().ok();
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = Arc::clone(&stop);
    let _ = std::thread::Builder::new().name("standby-http".to_owned()).spawn(move || {
        for stream in listener.incoming() {
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            let Ok(stream) = stream else { continue };
            let controller = Arc::clone(&controller);
            let _ = std::thread::Builder::new()
                .name("standby-http-conn".to_owned())
                .spawn(move || serve_standby_connection(&controller, stream));
        }
    });
    Box::new(move || {
        stop.store(true, Ordering::SeqCst);
        // Wake the accept loop so it sees the stop.
        if let Some(local) = local {
            let _ = std::net::TcpStream::connect(local);
        }
    })
}

/// One HTTP/1.1 request of the temporary standby server.
fn serve_standby_connection(controller: &LoadKeyspaceController, mut stream: std::net::TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let (head_end, content_length) = loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..end]);
            let length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (end + 4, length);
        }
        if buffer.len() > 1 << 20 {
            return;
        }
    };
    while buffer.len() < head_end + content_length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
    }
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let target = head.lines().next().and_then(|line| line.split_whitespace().nth(1)).unwrap_or("/").to_owned();
    let (path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
    let query = decode_query(raw_query);
    let _ = stream.set_nonblocking(true);
    let peer = stream.try_clone().ok();
    let client_closed = move || {
        let mut probe = [0_u8; 1];
        peer.as_ref().is_some_and(|peer| matches!(peer.peek(&mut probe), Ok(0)))
    };
    let response = tidb_standby::standby_probe(path)
        .or_else(|| {
            controller.serve(
                None,
                &Request { path, query: &query, body: &buffer[head_end.min(buffer.len())..], client_closed: &client_closed },
            )
        })
        .unwrap_or(Response { status: 404, content_type: None, body: b"404 page not found\n".to_vec() });
    let _ = stream.set_nonblocking(false);
    let mut out = format!("HTTP/1.1 {} {}\r\n", response.status, reason(response.status));
    if let Some(content_type) = response.content_type {
        out.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    out.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", response.body.len()));
    let _ = stream.write_all(out.as_bytes());
    let _ = stream.write_all(&response.body);
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        304 => "Not Modified",
        400 => "Bad Request",
        404 => "Not Found",
        408 => "Request Timeout",
        412 => "Precondition Failed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// Go `url.Values`: the decoded pairs of a raw query.
pub(crate) fn decode_query(raw: &str) -> Vec<(String, String)> {
    fn unescape(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'+' => out.push(b' '),
                b'%' => match bytes
                    .get(i + 1..i + 3)
                    .and_then(|hex| u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok())
                {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                },
                byte => out.push(byte),
            }
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (unescape(key), unescape(value))
        })
        .collect()
}

/// The server as the `/tidb-pool/` handlers reach it. This port's server has
/// no `health` / force-shutdown / manager-free flags yet (register N08), so
/// it answers as a healthy server that owns no AutoID service.
struct NodeServer;

impl StandbyServer for NodeServer {
    fn health(&self) -> bool {
        true
    }
    fn is_auto_id_owner(&self) -> bool {
        false
    }
    fn set_force_shutdown(&self) {}
    fn set_need_request_mgr_free(&self) {}
    fn get_normal_closed_conn(&self, _: &str, _: &str) -> String {
        String::new()
    }
}

/// Go `svr.StandbyController.Handler(svr)` on the status server: the
/// `/tidb-pool/` API of an activated node.
pub(crate) fn serve(path: &str, body: &str) -> Option<axum::response::Response> {
    let controller = standby_controller()?;
    let (route, raw_query) = path.split_once('?').unwrap_or((path, ""));
    let query = decode_query(raw_query);
    let response = controller.serve(
        Some(&NodeServer),
        &Request { path: route, query: &query, body: body.as_bytes(), client_closed: &|| false },
    )?;
    let mut builder = axum::http::Response::builder().status(response.status);
    if let Some(content_type) = response.content_type {
        builder = builder.header("content-type", content_type);
    }
    Some(builder.body(axum::body::Body::from(response.body)).expect("standby response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestParseStarterAdditionalParams`.
    #[test]
    fn test_parse_starter_additional_params() {
        assert_eq!(parse_starter_additional_params("  ").unwrap(), StarterParams::default());
        assert_eq!(
            parse_starter_additional_params("pod-name=pod-1, pod-ip=10.0.0.1,pod-namespace=ns-1,manager-namespace=m")
                .unwrap(),
            StarterParams {
                manager_namespace: "m".to_owned(),
                pod_name: "pod-1".to_owned(),
                pod_ip: "10.0.0.1".to_owned(),
                pod_namespace: "ns-1".to_owned(),
            }
        );
        for (raw, want) in [
            ("pod-name=a,,pod-ip=b", "starter additional params contains an empty item"),
            ("pod-name", "starter additional param \"pod-name\" must be in k=v format"),
            ("=x", "starter additional param \"=x\" has an empty key"),
            ("pod-name=", "starter additional param \"pod-name\" has an empty value"),
            ("pod-name=a,pod-name=b", "starter additional param \"pod-name\" is duplicated"),
        ] {
            assert_eq!(parse_starter_additional_params(raw).unwrap_err(), want, "{raw}");
        }
    }

    #[test]
    fn decode_query_unescapes_like_go() {
        assert_eq!(
            decode_query("keyspace=ks+1&wait=1h&x=%2Fa"),
            [
                ("keyspace".to_owned(), "ks 1".to_owned()),
                ("wait".to_owned(), "1h".to_owned()),
                ("x".to_owned(), "/a".to_owned())
            ]
        );
    }
}
