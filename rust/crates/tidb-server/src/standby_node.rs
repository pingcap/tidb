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
use std::sync::{Arc, LazyLock, Mutex, OnceLock, RwLock};

use tidb_standby::{
    IdleServer, LoadKeyspaceController, Request, Response, StandbyServer, StandbyShutdownServer,
};

/// Go `Server.health`: false until the server runs, true while it accepts
/// connections, false again once its shutdown starts.
pub(crate) static SERVER_HEALTH: AtomicBool = AtomicBool::new(false);
/// Go `Server.forceShutdown`.
static FORCE_SHUTDOWN: AtomicBool = AtomicBool::new(false);
/// Go `Server.needRequestMgrFree`.
static NEED_REQUEST_MGR_FREE: AtomicBool = AtomicBool::new(false);

/// Go `tidbGatewayAttrsConnKey`.
pub(crate) const TIDB_GATEWAY_ATTRS_CONN_KEY: &str = "TiDB-Gateway-ConnID";
/// Go `normalClosedConnsCapacity`.
const NORMAL_CLOSED_CONNS_CAPACITY: usize = 1000;
/// Go `Server.normalClosedConns`, keyed as `normalCloseConnKey.Hash`.
static NORMAL_CLOSED_CONNS: LazyLock<Mutex<tidb_kvcache::SimpleLruCache<String, String>>> =
    LazyLock::new(|| Mutex::new(tidb_kvcache::SimpleLruCache::new(NORMAL_CLOSED_CONNS_CAPACITY)));

/// Go `normalCloseConnKey.Hash`.
fn normal_close_conn_key(keyspace_name: &str, conn_id: &str) -> String {
    format!("{keyspace_name}-{conn_id}")
}

/// Go `Server.SetNormalClosedConn`: sets the normal closed connection
/// message by the gateway's connection id.
pub(crate) fn set_normal_closed_conn(keyspace_name: &str, conn_id: &str, msg: &str) {
    if conn_id.is_empty() {
        return;
    }
    NORMAL_CLOSED_CONNS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put(normal_close_conn_key(keyspace_name, conn_id), msg.to_owned());
}

/// Go `Server.GetNormalClosedConn`.
pub(crate) fn get_normal_closed_conn(keyspace_name: &str, conn_id: &str) -> String {
    if conn_id.is_empty() {
        return String::new();
    }
    NORMAL_CLOSED_CONNS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(normal_close_conn_key(keyspace_name, conn_id).as_str())
        .cloned()
        .unwrap_or_default()
}

/// Go `Server.kill`: a killed connection's `normalCloseMsg` is recorded
/// under its gateway connection id when the server has a standby controller.
pub(crate) fn record_killed_conn_normal_close(gateway_conn_id: &str, normal_close_msg: &str) {
    if !normal_close_msg.is_empty() && server_standby_controller().is_some() {
        set_normal_closed_conn(
            &tidb_keyspace::get_keyspace_name_by_settings(),
            gateway_conn_id,
            normal_close_msg,
        );
    }
}

/// Go `Server.SetForceShutdown`.
pub(crate) fn set_force_shutdown() {
    FORCE_SHUTDOWN.store(true, Ordering::Release);
}

/// Go `Server.GetForceShutdown`.
pub(crate) fn get_force_shutdown() -> bool {
    FORCE_SHUTDOWN.load(Ordering::Acquire)
}

/// Go `Server.SetNeedRequestMgrFree`.
pub(crate) fn set_need_request_mgr_free() {
    NEED_REQUEST_MGR_FREE.store(true, Ordering::Release);
}

/// Go `Server.GetNeedRequestMgrFree`.
pub(crate) fn get_need_request_mgr_free() -> bool {
    NEED_REQUEST_MGR_FREE.load(Ordering::Acquire)
}

/// Go `server.StandbyController`, the part a running server calls.
pub trait StandbyController: Send + Sync {
    /// Go `OnConnActive`: called when a new connection is established or a
    /// connection finished a command.
    fn on_conn_active(&self);
    /// Go `OnServerCreated`.
    fn on_server_created(&self, svr: Arc<dyn IdleServer>);
    /// Go `OnServerShutdown`: called when the server is going to shut down.
    fn on_server_shutdown(&self, svr: &Arc<dyn StandbyShutdownServer>);
}

impl StandbyController for Arc<LoadKeyspaceController> {
    fn on_conn_active(&self) {
        LoadKeyspaceController::on_conn_active(self);
    }
    fn on_server_created(&self, svr: Arc<dyn IdleServer>) {
        LoadKeyspaceController::on_server_created(self, svr);
    }
    fn on_server_shutdown(&self, svr: &Arc<dyn StandbyShutdownServer>) {
        LoadKeyspaceController::on_server_shutdown(self, svr);
    }
}

/// Go `svr.StandbyController`.
static SERVER_CONTROLLER: RwLock<Option<Arc<dyn StandbyController>>> = RwLock::new(None);

/// Go `svr.StandbyController`, nil unless the node started in standby mode.
pub(crate) fn server_standby_controller() -> Option<Arc<dyn StandbyController>> {
    SERVER_CONTROLLER
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Go `svr.StandbyController = standbyController`.
pub(crate) fn set_server_standby_controller(controller: Option<Arc<dyn StandbyController>>) {
    *SERVER_CONTROLLER
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = controller;
}

/// Go `mysql.ComSleep` / `mysql.ComQuery`.
const COM_SLEEP: u8 = 0;
const COM_QUERY: u8 = 3;
/// Go `mysql.ServerStatusInTrans`.
const SERVER_STATUS_IN_TRANS: u16 = 0x0001;

/// The node as the standby controller's shutdown and idle paths read it (Go
/// `*server.Server` behind `StandbyShutdownServer` and `OnServerCreated`).
pub(crate) struct NodeStandbyServer {
    /// Go `Server.clients`, as a count.
    pub(crate) tracker: Arc<crate::sql_node::ConnectionTracker>,
    pub(crate) session_manager: Option<Arc<dyn tidb_sessmgr::processinfo::SessionManager>>,
    /// Go `Server.autoIDService`.
    pub(crate) auto_id: Option<Arc<tidb_autoid_service::Service>>,
}

impl StandbyShutdownServer for NodeStandbyServer {
    fn auto_id_service_close(&self) {
        if let Some(service) = &self.auto_id {
            service.close();
        }
    }
    fn get_force_shutdown(&self) -> bool {
        get_force_shutdown()
    }
    /// Go `Server.WaitZeroConn`: waits until all client connections are
    /// closed.
    fn wait_zero_conn(&self) {
        while self.tracker.active() > 0 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    fn get_need_request_mgr_free(&self) -> bool {
        get_need_request_mgr_free()
    }
}

impl IdleServer for NodeStandbyServer {
    fn connection_count(&self) -> usize {
        self.tracker.active()
    }
    /// Go `Server.GetUserProcessList`. The process list publishes a running
    /// statement's text and the rendered status (`serverStatus2Str`), which
    /// carry the two facts the idle watcher reads: whether a command runs
    /// and whether the session is in a transaction.
    fn get_user_process_list(&self) -> Vec<(u8, u16)> {
        self.session_manager.as_ref().map_or_else(Vec::new, |manager| {
            manager
                .show_process_list()
                .iter()
                .map(|process| {
                    let command = if process.info.is_empty() { COM_SLEEP } else { COM_QUERY };
                    let state = if process.state.contains("in transaction") {
                        SERVER_STATUS_IN_TRANS
                    } else {
                        0
                    };
                    (command, state)
                })
                .collect()
        })
    }
    fn get_client_capability_list(&self) -> Vec<u32> {
        self.session_manager
            .as_ref()
            .map_or_else(Vec::new, |manager| manager.get_client_capability_list())
    }
    fn set_need_request_mgr_free(&self) {
        set_need_request_mgr_free();
    }
}

/// Go `Server.startShutdown`'s starter branch: the standby controller sees
/// the shutdown once the server reports unhealthy and admits no connection.
pub(crate) fn on_server_shutdown(svr: Arc<dyn StandbyShutdownServer>) {
    if !tidb_config_deploymode::is_starter() {
        return;
    }
    if let Some(controller) = server_standby_controller() {
        controller.on_server_shutdown(&svr);
    }
}

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
    // Go `clusterSecurity.ToTLSConfig()`, read before the parameters.
    let tls = cluster_tls_client_builder(&cluster_security())?;
    Ok(Some(tidb_tidbmanager::new_client(&manager_addr, tls, &params.pod_name, &params.pod_ip, &params.pod_namespace)))
}

/// Go `cfg.Security.ClusterSecurity()`.
fn cluster_security() -> tidb_pd_client::ClusterSecurity {
    let security = tidb_config::config_tree::config::get_global_config().security.clone();
    tidb_pd_client::ClusterSecurity::new(
        security.cluster_ssl_ca,
        security.cluster_ssl_cert,
        security.cluster_ssl_key,
        security.cluster_verify_cn,
    )
}

/// Go `Security.ToTLSConfig` for the manager's HTTP client: `None` without a
/// cluster CA.
fn cluster_tls_client_builder(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<Option<reqwest::blocking::ClientBuilder>, String> {
    if !security.is_tls_enabled() {
        return Ok(None);
    }
    tidb_store_driver_txn::cluster_http::with_cluster_tls(reqwest::blocking::Client::builder(), security)
        .map(Some)
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
    set_server_standby_controller(Some(Arc::new(Arc::clone(&controller))));
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
    // Go: `tls.NewListener(l, tlsConfig)` under the cluster security.
    let tls = match crate::status_transport::server_tls_config(&cluster_security()) {
        Ok(tls) => tls,
        Err(error) => {
            tidb_logutil::bg_logger()
                .warn("failed to get tls config", &[tidb_logutil::zap_error(error.to_string())]);
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
            let tls = tls.clone();
            let _ = std::thread::Builder::new().name("standby-http-conn".to_owned()).spawn(move || {
                let peer = stream.try_clone().ok();
                match tls {
                    Some(config) => {
                        if let Ok(session) = rustls::ServerConnection::new(config) {
                            serve_standby_connection(
                                &controller,
                                rustls::StreamOwned::new(session, stream),
                                peer,
                            );
                        }
                    }
                    None => serve_standby_connection(&controller, stream, peer),
                }
            });
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
fn serve_standby_connection<S: Read + Write>(
    controller: &LoadKeyspaceController,
    mut stream: S,
    peer: Option<std::net::TcpStream>,
) {
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
    // Go `r.Context().Done()`: the client's socket reached its end.
    if let Some(peer) = &peer {
        let _ = peer.set_nonblocking(true);
    }
    let client_closed = || {
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
    if let Some(peer) = &peer {
        let _ = peer.set_nonblocking(false);
    }
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

/// Go `r.URL.Query()`: `url.ParseQuery`'s valid pairs, a malformed one
/// silently dropped.
pub(crate) fn decode_query(raw: &str) -> Vec<(String, String)> {
    let (values, _) = tidb_go_url::parse_query(raw);
    values
        .0
        .iter()
        .flat_map(|(key, values)| {
            values.iter().map(move |value| {
                (
                    String::from_utf8_lossy(key).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                )
            })
        })
        .collect()
}

/// Go `initExternalWorkloadManager`: the manager of an enabled
/// `[external-workload]` section. Its init is non-fatal; TiDB can continue
/// without external workload coordination.
pub(crate) fn init_external_workload_manager(
    meta: Option<&tikv_client::proto::keyspacepb::KeyspaceMeta>,
) -> Option<Box<dyn tidb_extworkload::Manager>> {
    let cfg = tidb_config::config_tree::config::get_global_config().external_workload.clone();
    if !cfg.enable {
        return None;
    }
    if meta.is_none() {
        tidb_logutil::bg_logger().warn(
            "external workload controller enabled but keyspace meta is unavailable; TiDB will continue without external workload coordination",
            &[],
        );
        return None;
    }
    // Go `dialClient`: the cluster TLS configuration when the cluster CA is
    // set ("build external workload TLS config" under "init external
    // workload client").
    let tls_config = match cluster_security().client_tls_config() {
        Ok(tls_config) => tls_config,
        Err(error) => {
            tidb_logutil::bg_logger().warn(
                "failed to initialize external workload manager; TiDB will continue without external workload coordination",
                &[tidb_logutil::zap_error(format!(
                    "init external workload client: build external workload TLS config: {error}"
                ))],
            );
            return None;
        }
    };
    match tidb_extworkload::new_manager(meta, &cfg, tls_config) {
        Ok(manager) => manager,
        Err(error) => {
            tidb_logutil::bg_logger().warn(
                "failed to initialize external workload manager; TiDB will continue without external workload coordination",
                &[tidb_logutil::zap_error(error.to_string())],
            );
            None
        }
    }
}

/// Go `defer closeExternalWorkloadManager(mgr)`.
pub(crate) struct ExternalWorkloadManagerGuard(pub(crate) Box<dyn tidb_extworkload::Manager>);

impl Drop for ExternalWorkloadManagerGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.close() {
            tidb_logutil::bg_logger().warn(
                "failed to close external workload manager",
                &[tidb_logutil::zap_error(error.to_string())],
            );
        }
    }
}

/// The server as the `/tidb-pool/` handlers reach it; the field is Go
/// `Server.IsAutoIDOwner`.
struct NodeServer(bool);

impl StandbyServer for NodeServer {
    fn health(&self) -> bool {
        SERVER_HEALTH.load(Ordering::Acquire)
    }
    fn is_auto_id_owner(&self) -> bool {
        self.0
    }
    fn set_force_shutdown(&self) {
        set_force_shutdown();
    }
    fn set_need_request_mgr_free(&self) {
        set_need_request_mgr_free();
    }
    fn get_normal_closed_conn(&self, keyspace_name: &str, conn_id: &str) -> String {
        get_normal_closed_conn(keyspace_name, conn_id)
    }
}

/// Go `svr.StandbyController.Handler(svr)` on the status server: the
/// `/tidb-pool/` API of an activated node.
pub(crate) fn serve(path: &str, body: &str, is_auto_id_owner: bool) -> Option<axum::response::Response> {
    let controller = standby_controller()?;
    let (route, raw_query) = path.split_once('?').unwrap_or((path, ""));
    let query = decode_query(raw_query);
    let response = controller.serve(
        Some(&NodeServer(is_auto_id_owner)),
        &Request { path: route, query: &query, body: body.as_bytes(), client_closed: &|| false },
    )?;
    let mut builder = axum::http::Response::builder().status(response.status);
    if let Some(content_type) = response.content_type {
        builder = builder.header("content-type", content_type);
    }
    Some(builder.body(axum::body::Body::from(response.body)).expect("standby response"))
}

/// Go `AutoIDOwnerHandler.ServeHTTP` (`/owner_manager/auto_id_service`, a
/// starter deployment's route): 500 unless the server is healthy, else
/// whether this TiDB owns the auto ID service.
pub(crate) fn auto_id_owner_status(is_auto_id_owner: bool) -> Result<String, ()> {
    if !SERVER_HEALTH.load(Ordering::Acquire) {
        return Err(());
    }
    Ok(format!("{{\"is_owner\":{is_auto_id_owner}}}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `AutoIDOwnerHandler`.
    #[test]
    fn auto_id_owner_status_answers_as_go() {
        // An unhealthy server answers 500.
        SERVER_HEALTH.store(false, Ordering::Release);
        assert_eq!(auto_id_owner_status(true), Err(()));
        SERVER_HEALTH.store(true, Ordering::Release);
        assert_eq!(auto_id_owner_status(true), Ok("{\"is_owner\":true}".to_owned()));
        assert_eq!(auto_id_owner_status(false), Ok("{\"is_owner\":false}".to_owned()));
    }

    /// Go `TestServerShutdownFlags`.
    #[test]
    fn test_server_shutdown_flags() {
        let svr = NodeServer(false);
        assert!(!get_force_shutdown());
        assert!(!get_need_request_mgr_free());

        svr.set_force_shutdown();
        svr.set_need_request_mgr_free();
        assert!(get_force_shutdown());
        assert!(get_need_request_mgr_free());
    }

    struct TestStandbyController;

    impl StandbyController for TestStandbyController {
        fn on_conn_active(&self) {}
        fn on_server_created(&self, _: Arc<dyn IdleServer>) {}
        fn on_server_shutdown(&self, _: &Arc<dyn StandbyShutdownServer>) {}
    }

    /// Go `TestGetConAttrs`, the normal-closed-connection half: a killed
    /// connection's message is kept under its gateway id only when the
    /// server has a standby controller, and never for `KILL QUERY`.
    #[test]
    fn test_get_con_attrs() {
        use tidb_session::domain::session_api::{
            ConnectionCancellation, ConnectionClose, ConnectionKillTarget,
        };
        use tidb_sessmgr::{ProcessRegistry, NORMAL_CLOSE_MSG_KILL_STMT};

        let registry = ProcessRegistry::default();
        // The connection's kill handle as `serve_connection_inner` binds it.
        let new_conn = |conn_id: u64, gw_conn_id: &'static str| {
            let close = ConnectionClose::with_wake(move |normal_close_msg| {
                record_killed_conn_normal_close(gw_conn_id, normal_close_msg);
            });
            let guard = registry.register(
                conn_id,
                "root".to_owned(),
                "localhost".to_owned(),
                String::new(),
                Some(Arc::new(ConnectionKillTarget::new(
                    ConnectionCancellation::default(),
                    close.clone(),
                ))),
            );
            (close, guard)
        };

        let normal_close_msg = NORMAL_CLOSE_MSG_KILL_STMT;
        let keyspace_name = tidb_keyspace::get_keyspace_name_by_settings();
        let (_no_standby_conn, _guard) = new_conn(100, "gw-no-standby");
        registry.kill_with_normal_close_msg(100, false, false, false, normal_close_msg);
        assert!(get_normal_closed_conn(&keyspace_name, "gw-no-standby").is_empty());

        set_server_standby_controller(Some(Arc::new(TestStandbyController)));
        let (kill_conn, _guard) = new_conn(101, "gw-kill-connection");
        assert!(get_normal_closed_conn(&keyspace_name, "gw-kill-connection").is_empty());
        registry.kill_with_normal_close_msg(101, false, false, false, normal_close_msg);
        assert_eq!(get_normal_closed_conn(&keyspace_name, "gw-kill-connection"), normal_close_msg);
        // Go `connStatusWaitShutdown`.
        assert!(kill_conn.is_closed());

        let (query_conn, _guard) = new_conn(102, "gw-kill-query");
        registry.kill_with_normal_close_msg(102, true, false, false, normal_close_msg);
        assert!(get_normal_closed_conn(&keyspace_name, "gw-kill-query").is_empty());
        assert!(!query_conn.is_closed());
        set_server_standby_controller(None);
    }

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
        // Go drops a pair it cannot unescape.
        assert_eq!(decode_query("a=%zz&b=1"), [("b".to_owned(), "1".to_owned())]);
    }
}
