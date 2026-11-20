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

//! Go `pkg/standby`: `LoadKeyspaceController` keeps a tidb-server in standby
//! until the manager activates it for a keyspace, serves the `/tidb-pool/`
//! API, and watches for idleness.
//!
//! Go's handlers are `net/http` handlers over a mux; here [`LoadKeyspaceController::serve`]
//! answers one request, and the node's status listener mounts it under
//! [`HTTP_PATH_PREFIX`].

mod idle_watcher;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;

pub use idle_watcher::IdleServer;

const STANDBY_STATE: &str = "standby";
const ACTIVATED_STATE: &str = "activated";
const TERMINATING_STATE: &str = "terminating";

const CONN_NORMAL_CLOSED: &str = "normal closed";
const TIDB_NORMAL_RESTART_LOG_PATH: &str = "/tmp/tidb-normal-restart.log";

/// Go `httpPathPrefix`.
pub const HTTP_PATH_PREFIX: &str = "/tidb-pool/";

const DEFAULT_CLOSE_CONN_WAIT: Duration = Duration::from_secs(8 * 3600);
const MAX_CLOSE_CONN_WAIT: Duration = Duration::from_secs(24 * 3600);

const MANAGER_FREE_MAX_ATTEMPTS: usize = 3;
const MANAGER_FREE_RETRY_INTERVAL: Duration = Duration::from_millis(200);

/// Go `syscall.SIGTERM` / `SIGINT` as `signal.TiDBExit` takes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitSignal {
    /// Go `Term`.
    Term,
    /// Go `Int`.
    Int,
}

/// Go `var tidbExit = signal.TiDBExit`: raises the signal on this process.
static TIDB_EXIT: RwLock<fn(ExitSignal)> = RwLock::new(default_tidb_exit);

fn default_tidb_exit(signal: ExitSignal) {
    let signal = match signal {
        ExitSignal::Term => signal_hook::consts::SIGTERM,
        ExitSignal::Int => signal_hook::consts::SIGINT,
    };
    let _ = signal_hook::low_level::raise(signal);
}

pub(crate) fn tidb_exit(signal: ExitSignal) {
    let exit = *TIDB_EXIT.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    exit(signal);
}

/// Replaces Go's `tidbExit` (a test's hook); answers the previous one.
pub fn set_tidb_exit(exit: fn(ExitSignal)) -> fn(ExitSignal) {
    std::mem::replace(&mut *TIDB_EXIT.write().unwrap_or_else(std::sync::PoisonError::into_inner), exit)
}

/// Go `ActivateRequest`: the request body for activating the tidb server.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
pub struct ActivateRequest {
    #[serde(default)]
    /// Go `KeyspaceName`.
    pub keyspace_name: String,
    #[serde(default)]
    /// Go `ExportId`.
    pub export_id: String,
    #[serde(default)]
    /// Go `MaxIdleSeconds`.
    pub max_idle_seconds: usize,
    /// Keyspace metadata sent by the manager during activation, such as
    /// tenant, project, and cluster identifiers.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    /// Go `RunAutoAnalyze`.
    pub run_auto_analyze: bool,
    #[serde(default)]
    /// Go `TidbEnableDdl`.
    pub tidb_enable_ddl: bool,
}

/// Go's package state (`mu`, `state`, `activateRequest`, `activationTimeout`,
/// the normal-restart info and `activateCh`).
struct Shared {
    state: &'static str,
    activate_request: ActivateRequest,
    activation_timeout: usize,
    pre_tidb_normal_restart_keyspace_name: String,
    pre_tidb_normal_restart_msg: String,
    /// Go `activateCh` (capacity one).
    activate_signalled: bool,
}

static SHARED: Mutex<Shared> = Mutex::new(Shared {
    state: STANDBY_STATE,
    activate_request: ActivateRequest {
        keyspace_name: String::new(),
        export_id: String::new(),
        max_idle_seconds: 0,
        metadata: BTreeMap::new(),
        run_auto_analyze: false,
        tidb_enable_ddl: false,
    },
    activation_timeout: 0,
    pre_tidb_normal_restart_keyspace_name: String::new(),
    pre_tidb_normal_restart_msg: String::new(),
    activate_signalled: false,
});
static ACTIVATED: Condvar = Condvar::new();

fn shared() -> std::sync::MutexGuard<'static, Shared> {
    SHARED.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The server as the `/tidb-pool/` handlers reach it (Go `*server.Server`).
pub trait StandbyServer: Send + Sync {
    /// Go `Server.Health`.
    fn health(&self) -> bool;
    /// Go `Server.IsAutoIDOwner`.
    fn is_auto_id_owner(&self) -> bool;
    /// Go `Server.SetForceShutdown`.
    fn set_force_shutdown(&self);
    /// Go `Server.SetNeedRequestMgrFree`.
    fn set_need_request_mgr_free(&self);
    /// Go `Server.GetNormalClosedConn`.
    fn get_normal_closed_conn(&self, keyspace_name: &str, conn_id: &str) -> String;
}

/// Go `server.StandbyReadyServer`.
pub trait StandbyReadyServer {
    /// Go `InitTiDBListener`.
    ///
    /// # Errors
    ///
    /// The listener could not be bound.
    fn init_tidb_listener(&mut self) -> Result<(), String>;
}

/// Go `server.StandbyShutdownServer`.
pub trait StandbyShutdownServer: Send + Sync {
    /// Go `AutoIDServiceClose`.
    fn auto_id_service_close(&self);
    /// Go `GetForceShutdown`.
    fn get_force_shutdown(&self) -> bool;
    /// Go `WaitZeroConn`.
    fn wait_zero_conn(&self);
    /// Go `GetNeedRequestMgrFree`.
    fn get_need_request_mgr_free(&self) -> bool;
}

/// One request of the `/tidb-pool/` API.
pub struct Request<'a> {
    /// The path with its prefix, without the query.
    pub path: &'a str,
    /// The decoded query pairs.
    pub query: &'a [(String, String)],
    /// Go `Body`.
    pub body: &'a [u8],
    /// Go `r.Context().Done()`: whether the client closed the connection.
    pub client_closed: &'a (dyn Fn() -> bool + Sync),
}

impl Request<'_> {
    fn query_get(&self, key: &str) -> &str {
        self.query.iter().find(|(name, _)| name == key).map_or("", |(_, value)| value.as_str())
    }
}

/// A handler's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// Go `Status`.
    pub status: u16,
    /// Go `ContentType`.
    pub content_type: Option<&'static str>,
    /// Go `Body`.
    pub body: Vec<u8>,
}

impl Response {
    fn status(status: u16) -> Self {
        Self { status, content_type: None, body: Vec::new() }
    }

    fn text(status: u16, body: &str) -> Self {
        Self { status, content_type: None, body: body.as_bytes().to_vec() }
    }
}

/// Go `KeyspaceMismatch`: the response body when the keyspace name in the
/// request does not match the local keyspace name.
#[derive(serde::Serialize)]
struct KeyspaceMismatch<'a> {
    remote: &'a str,
    local: &'a str,
}

#[derive(serde::Serialize)]
struct StatusResponse<'a> {
    state: &'a str,
    keyspace_name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    export_id: &'a str,
}

/// Go `exitOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExitOptions {
    graceful: bool,
    wait: Duration,
    skip_auto_id_owner: bool,
    need_mgr_free: bool,
}

/// Go `serverStartCh` with `startServerErr`.
#[derive(Default)]
struct ServerStart {
    done: Mutex<Option<Option<String>>>,
    cond: Condvar,
}

/// Go `LoadKeyspaceController`: controls the tidb server to be in standby
/// mode or activated.
pub struct LoadKeyspaceController {
    server_start: Arc<ServerStart>,
    mgr_cli: Option<Box<dyn tidb_tidbmanager::Client>>,
    /// Go `closeConnWait`, in nanoseconds.
    close_conn_wait: AtomicI64,
    pub(crate) last_active: AtomicI64,
    /// Go's package `httpServer`: stops the standby listener.
    http_server_shutdown: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl LoadKeyspaceController {
    /// Go `NewLoadKeyspaceController`: `mgr_cli` is `None` when manager
    /// notification is disabled.
    #[must_use]
    pub fn new(mgr_cli: Option<Box<dyn tidb_tidbmanager::Client>>) -> Self {
        Self {
            server_start: Arc::default(),
            mgr_cli,
            close_conn_wait: AtomicI64::new(0),
            last_active: AtomicI64::new(0),
            http_server_shutdown: Mutex::new(None),
        }
    }

    /// Go `ActivationMetadata`: a copy of the metadata carried by the
    /// activate request.
    #[must_use]
    pub fn activation_metadata(&self) -> Option<BTreeMap<String, String>> {
        let shared = shared();
        (!shared.activate_request.metadata.is_empty()).then(|| shared.activate_request.metadata.clone())
    }

    /// Go `Handler`'s mux: answers one request to query the pool status, or
    /// to activate or exit the tidb server. `None` for a path it does not
    /// serve.
    pub fn serve(&self, svr: Option<&dyn StandbyServer>, request: &Request<'_>) -> Option<Response> {
        match request.path.strip_prefix(HTTP_PATH_PREFIX)? {
            "status" => Some(status_handler()),
            "activate" => Some(self.activate(svr, request)),
            "exit" => Some(keyspace_validate(request).unwrap_or_else(|| self.exit(svr, request))),
            "checkconn" => Some(checkconn(svr, request)),
            _ => None,
        }
    }

    fn activate(&self, svr: Option<&dyn StandbyServer>, request: &Request<'_>) -> Response {
        let Ok(req) = serde_json::from_slice::<ActivateRequest>(request.body) else {
            return Response::status(400);
        };
        if req.keyspace_name.is_empty() {
            return Response::status(400);
        }
        let activation_timeout = {
            let mut shared = shared();
            if shared.state == STANDBY_STATE {
                shared.state = ACTIVATED_STATE;
                shared.activate_request = req;
                shared.activate_signalled = true;
                ACTIVATED.notify_all();
            } else if tidb_config_deploymode::is_starter() && shared.state == TERMINATING_STATE {
                return Response::text(503, "server is going to shutdown");
            } else if svr.is_some_and(|svr| !svr.health()) {
                return Response::text(503, "server is going to shutdown");
            } else if shared.activate_request.keyspace_name != req.keyspace_name {
                return Response::text(412, "server is not in standby mode");
            }
            // if client tries to activate with same keyspace name, wait for
            // ready signal and return 200.
            shared.activation_timeout
        };
        let deadline =
            (activation_timeout > 0).then(|| Instant::now() + Duration::from_secs(activation_timeout as u64));
        let mut done = self.server_start.done.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(start_server_err) = done.as_ref() {
                return match start_server_err {
                    Some(error) => Response::text(500, error),
                    None => status_handler(),
                };
            }
            if (request.client_closed)() {
                drop(done);
                // client closed connection.
                self.end_standby_and_exit("client closed connection");
                return Response::status(499);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                drop(done);
                // reach hardlimit timeout from config.
                bg_logger().warn("timeout waiting for activation", &[]);
                self.end_standby_and_exit("timeout waiting for activation");
                return Response::text(408, "timeout waiting for activation");
            }
            (done, _) = self
                .server_start
                .cond
                .wait_timeout(done, Duration::from_millis(10))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Go's `go func() { c.EndStandby(err); tidbExit(syscall.SIGTERM) }()`.
    fn end_standby_and_exit(&self, error: &str) {
        self.end_standby(Some(error.to_owned()));
        tidb_exit(ExitSignal::Term);
    }

    // Terminate the tidb server by sending a request. For example, in a
    // cloud environment, we may need to delete pod to free up resources.
    fn exit(&self, svr: Option<&dyn StandbyServer>, request: &Request<'_>) -> Response {
        let options = match parse_exit_options(request) {
            Ok(options) => options,
            Err(option) => return Response::text(400, &format!("invalid {option}\n")),
        };
        bg_logger().info(
            "receiving exit request",
            &[
                Field::new("graceful", Value::Bool(options.graceful)),
                Field::new("wait", Value::Duration(i64::try_from(options.wait.as_nanos()).unwrap_or(i64::MAX))),
                Field::new("skip_auto_id_owner", Value::Bool(options.skip_auto_id_owner)),
                Field::new("need_mgr_free", Value::Bool(options.need_mgr_free)),
            ],
        );
        if let Some(svr) = svr {
            if tidb_config_deploymode::is_starter() {
                if options.need_mgr_free && self.mgr_cli.is_none() {
                    return Response::text(503, "manager notifier is unavailable\n");
                }
                if options.skip_auto_id_owner && svr.is_auto_id_owner() {
                    bg_logger().info("auto id service is owner, skip exit", &[]);
                    return Response::text(304, "auto id service is owner");
                }
                if !options.graceful {
                    svr.set_force_shutdown();
                    save_tidb_normal_restart_info("received force exit request");
                    // Consider the server is going to force shutdown, send a
                    // high priority signal to kill tidb.
                    tidb_exit(ExitSignal::Int);
                    return Response::status(200);
                }
                let wait = if options.wait.is_zero() { DEFAULT_CLOSE_CONN_WAIT } else { options.wait };
                self.set_close_conn_wait(wait);
                if options.need_mgr_free {
                    svr.set_need_request_mgr_free();
                }
            }
            save_tidb_normal_restart_info("received exit request");
        }
        if tidb_config_deploymode::is_starter() {
            tidb_exit(ExitSignal::Term);
        } else {
            // Consider the server is going to force shutdown, send a high
            // priority signal to kill tidb.
            tidb_exit(ExitSignal::Int);
        }
        Response::status(200)
    }

    fn set_close_conn_wait(&self, wait: Duration) {
        self.close_conn_wait.store(i64::try_from(wait.as_nanos()).unwrap_or(i64::MAX), Ordering::SeqCst);
    }

    fn get_close_conn_wait(&self) -> Duration {
        Duration::from_nanos(u64::try_from(self.close_conn_wait.load(Ordering::SeqCst)).unwrap_or(0))
    }

    /// Go `WaitForActivate`: `listen` starts the temporary HTTP server over
    /// this controller's [`Self::serve`] (with Go's `/status` liveness probe
    /// and `/health` answer, [`standby_probe`]) and answers its shutdown;
    /// then waits for the activation signal and applies the request to the
    /// global configuration.
    pub fn wait_for_activate(&self, listen: impl FnOnce() -> Box<dyn FnOnce() + Send>) {
        let timeout = tidb_config::config_tree::config::get_global_config().standby.activation_timeout;
        shared().activation_timeout = timeout;
        load_tidb_normal_restart_info_and_remove();
        bg_logger().info("tidb-server is now running as standby, waiting for activation...", &[]);
        *self.http_server_shutdown.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(listen());

        let request = {
            let mut shared = shared();
            while !shared.activate_signalled {
                shared = ACTIVATED.wait(shared).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            shared.activate_signalled = false;
            shared.activate_request.clone()
        };
        bg_logger().info(
            "standby receive activate request",
            &[
                Field::new("keyspace-name", Value::Str(request.keyspace_name.clone())),
                Field::new("max-idle-seconds", Value::U64(request.max_idle_seconds as u64)),
                Field::new("run-auto-analyze", Value::Bool(request.run_auto_analyze)),
                Field::new("tidb-enable-ddl", Value::Bool(request.tidb_enable_ddl)),
                Field::new("metadata-count", Value::I64(request.metadata.len() as i64)),
            ],
        );
        tidb_config::config_tree::config::update_global(|c| {
            c.keyspace_name.clone_from(&request.keyspace_name);
            if tidb_config_deploymode::is_starter() && !request.export_id.is_empty() {
                c.starter_params.export_id.clone_from(&request.export_id);
            }
            if request.max_idle_seconds > 0 {
                c.standby.max_idle_seconds = request.max_idle_seconds;
            }
            // DDL config
            if request.tidb_enable_ddl {
                c.instance.tidb_enable_ddl.0 = true;
            }
            // analyze table
            if request.run_auto_analyze {
                c.performance.run_auto_analyze = true;
            }
        });
    }

    /// Go `PrepareForActivation`: binds the server listener and only then
    /// ends standby, so the activation API reports success after the server
    /// is ready to accept connections.
    ///
    /// # Errors
    ///
    /// The listener could not be bound.
    pub fn prepare_for_activation(&self, svr: &mut dyn StandbyReadyServer) -> Result<(), String> {
        let result = svr.init_tidb_listener();
        self.end_standby(result.clone().err());
        result
    }

    /// Go `EndStandby`: notifies the temp http server that the tidb server
    /// is ready or failed to init.
    pub fn end_standby(&self, error: Option<String>) {
        {
            let mut done = self.server_start.done.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if done.is_some() {
                return;
            }
            *done = Some(error);
            self.server_start.cond.notify_all();
        }
        if let Some(shutdown) =
            self.http_server_shutdown.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
        {
            shutdown();
        }
    }

    /// Go `OnServerShutdown`: called when the server is going to shut down.
    pub fn on_server_shutdown(&self, svr: &Arc<dyn StandbyShutdownServer>) {
        if !tidb_config_deploymode::is_starter() {
            return;
        }
        shared().state = TERMINATING_STATE;

        // Give up auto ID ownership before waiting for TiProxy to migrate traffic.
        svr.auto_id_service_close();

        if svr.get_force_shutdown() {
            return;
        }
        if !self.wait_zero_conn(svr) {
            return;
        }
        if svr.get_need_request_mgr_free() {
            let exit_reason = match std::fs::read(TIDB_NORMAL_RESTART_LOG_PATH) {
                Ok(data) => String::from_utf8_lossy(&data).into_owned(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(error) => {
                    let exit_reason = format!("failed to load normal restart log: {error}");
                    bg_logger().warn(
                        "failed to load tidb normal restart log",
                        &[Field::new("exitReason", Value::Str(exit_reason.clone()))],
                    );
                    exit_reason
                }
            };
            self.report_manager_free(&exit_reason);
        }
    }

    fn wait_zero_conn(&self, svr: &Arc<dyn StandbyShutdownServer>) -> bool {
        let max_wait_time = self.get_close_conn_wait();
        if max_wait_time.is_zero() {
            return true;
        }
        bg_logger().info(
            "waiting for tiproxy to migrate and close all connections",
            &[Field::new("maxWaitTime", Value::Duration(i64::try_from(max_wait_time.as_nanos()).unwrap_or(i64::MAX)))],
        );
        let (done, finished) = std::sync::mpsc::sync_channel(1);
        let waiting = Arc::clone(svr);
        std::thread::spawn(move || {
            waiting.wait_zero_conn();
            let _ = done.send(());
        });
        if finished.recv_timeout(max_wait_time).is_ok() {
            bg_logger().info("tiproxy has closed all connections", &[]);
            true
        } else {
            bg_logger().info("tiproxy connection close timed out", &[]);
            false
        }
    }

    pub(crate) fn report_manager_free(&self, exit_reason: &str) -> bool {
        let Some(mgr_cli) = &self.mgr_cli else {
            bg_logger().warn("manager notifier is unavailable", &[]);
            return false;
        };
        let deadline = Instant::now() + tidb_tidbmanager::DEFAULT_TIMEOUT;
        let mut last_err = String::new();
        for attempt in 1..=MANAGER_FREE_MAX_ATTEMPTS {
            match mgr_cli.free(deadline.saturating_duration_since(Instant::now()), exit_reason) {
                Ok(()) => {
                    if attempt > 1 {
                        bg_logger().info("reported free after retry", &[Field::new("attempt", Value::I64(attempt as i64))]);
                    }
                    return true;
                }
                Err(error) => {
                    bg_logger().warn(
                        "failed to report free",
                        &[
                            Field::new("attempt", Value::I64(attempt as i64)),
                            Field::new("maxAttempts", Value::I64(MANAGER_FREE_MAX_ATTEMPTS as i64)),
                            tidb_logutil::zap_error(error.clone()),
                        ],
                    );
                    last_err = error;
                    if attempt < MANAGER_FREE_MAX_ATTEMPTS {
                        if Instant::now() + MANAGER_FREE_RETRY_INTERVAL >= deadline {
                            bg_logger().warn("manager free report timed out", &[]);
                            return false;
                        }
                        std::thread::sleep(MANAGER_FREE_RETRY_INTERVAL);
                    }
                }
            }
        }
        bg_logger().warn("failed to report free after retries", &[tidb_logutil::zap_error(last_err)]);
        false
    }
}

/// Go `WaitForActivate`'s `/status` liveness probe and `/health` answer of
/// the temporary standby server.
#[must_use]
pub fn standby_probe(path: &str) -> Option<Response> {
    match path {
        "/status" => Some(Response::status(200)),
        "/health" => Some(Response::text(200, r#"{"status":"standby"}"#)),
        _ => None,
    }
}

/// Go `keyspaceValidateMiddleware`: 412 with both names when the request's
/// `keyspace` is not this node's.
fn keyspace_validate(request: &Request<'_>) -> Option<Response> {
    let remote = request.query_get("keyspace");
    let local = tidb_config::config_tree::config::get_global_keyspace_name();
    (remote != local).then(|| Response {
        status: 412,
        content_type: None,
        body: serde_json::to_vec(&KeyspaceMismatch { remote, local: &local }).unwrap_or_default(),
    })
}

fn checkconn(svr: Option<&dyn StandbyServer>, request: &Request<'_>) -> Response {
    let (keyspace_name, conn_id) = (request.query_get("keyspace_name"), request.query_get("conn_id"));
    if keyspace_name.is_empty() || conn_id.is_empty() {
        return Response::text(400, "keyspace_name or conn_id is empty");
    }
    let fields = || {
        vec![
            Field::new("keyspace_name", Value::Str(keyspace_name.to_owned())),
            Field::new("conn_id", Value::Str(conn_id.to_owned())),
        ]
    };
    bg_logger().info("check connection", &fields());
    let closed = |msg: String| {
        let mut fields = fields();
        fields.push(Field::new("msg", Value::Str(msg)));
        bg_logger().info("connection is normal closed", &fields);
        Response::text(200, CONN_NORMAL_CLOSED)
    };
    if let Some(msg) = svr.map(|svr| svr.get_normal_closed_conn(keyspace_name, conn_id)).filter(|msg| !msg.is_empty()) {
        return closed(msg);
    }
    if let (true, msg) = is_pre_tidb_normal_restart(keyspace_name) {
        return closed(msg);
    }
    bg_logger().info("connection is unconfirmed", &fields());
    Response::text(200, "unconfirmed")
}

/// Go `parseExitOptions`; the error names the invalid option.
fn parse_exit_options(request: &Request<'_>) -> Result<ExitOptions, &'static str> {
    Ok(ExitOptions {
        graceful: parse_exit_bool(request.query_get("graceful"), "graceful")?,
        skip_auto_id_owner: parse_exit_bool(request.query_get("skip_auto_id_owner"), "skip_auto_id_owner")?,
        need_mgr_free: parse_exit_bool(request.query_get("need_mgr_free"), "need_mgr_free")?,
        wait: parse_exit_wait(request.query_get("wait"))?,
    })
}

/// Go `parseExitBool` (`strconv.ParseBool`).
fn parse_exit_bool(value: &str, name: &'static str) -> Result<bool, &'static str> {
    match value {
        "" => Ok(false),
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(name),
    }
}

/// Go `parseExitWait`: a Go duration, else whole seconds, within a day.
pub(crate) fn parse_exit_wait(value: &str) -> Result<Duration, &'static str> {
    if value.is_empty() {
        return Ok(Duration::ZERO);
    }
    let nanos = match tidb_config_configtypes::parse_duration(value) {
        Ok(nanos) => nanos,
        Err(_) => {
            let seconds = value.parse::<i64>().map_err(|_| "wait")?;
            if seconds < 0 || seconds > MAX_CLOSE_CONN_WAIT.as_secs() as i64 {
                return Err("wait");
            }
            seconds * 1_000_000_000
        }
    };
    let wait = Duration::from_nanos(u64::try_from(nanos).map_err(|_| "wait")?);
    if wait > MAX_CLOSE_CONN_WAIT {
        return Err("wait");
    }
    Ok(wait)
}

/// Go `statusHandler`.
fn status_handler() -> Response {
    let shared = shared();
    let export_id = if tidb_config_deploymode::is_starter() { shared.activate_request.export_id.as_str() } else { "" };
    let mut body = serde_json::to_vec(&StatusResponse {
        state: shared.state,
        keyspace_name: &shared.activate_request.keyspace_name,
        export_id,
    })
    .unwrap_or_default();
    // Go `json.Encoder.Encode` ends the value with a newline.
    body.push(b'\n');
    Response { status: 200, content_type: Some("application/json"), body }
}

fn load_tidb_normal_restart_info_and_remove() {
    let data = match std::fs::read(TIDB_NORMAL_RESTART_LOG_PATH) {
        Ok(data) => String::from_utf8_lossy(&data).into_owned(),
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                bg_logger().error(
                    "failed to read tidb normal restart log file",
                    &[tidb_logutil::zap_error(error.to_string())],
                );
            }
            return;
        }
    };
    let Some((keyspace_name, msg)) = data.split_once(':') else {
        bg_logger().error("invalid tidb normal restart log file", &[]);
        return;
    };
    {
        let mut shared = shared();
        shared.pre_tidb_normal_restart_keyspace_name = keyspace_name.to_owned();
        shared.pre_tidb_normal_restart_msg = msg.to_owned();
    }
    bg_logger().info(
        "load tidb normal restart log file",
        &[
            Field::new("preTidbNormalRestartKeyspaceName", Value::Str(keyspace_name.to_owned())),
            Field::new("preTidbNormalRestartMsg", Value::Str(msg.to_owned())),
        ],
    );
    if let Err(error) = std::fs::remove_file(TIDB_NORMAL_RESTART_LOG_PATH) {
        bg_logger().error("failed to remove tidb normal restart log file", &[tidb_logutil::zap_error(error.to_string())]);
    }
}

/// Go `SaveTidbNormalRestartInfo`: saves tidb normal restart info to file.
pub fn save_tidb_normal_restart_info(msg: &str) {
    let keyspace_name = tidb_keyspace::get_keyspace_name_by_settings();
    if keyspace_name.is_empty() {
        return;
    }
    if let Err(error) = std::fs::write(TIDB_NORMAL_RESTART_LOG_PATH, format!("{keyspace_name}:{msg}")) {
        bg_logger().error("failed to write tidb normal restart log file", &[tidb_logutil::zap_error(error.to_string())]);
    }
}

/// Go `IsPreTidbNormalRestart`: whether tidb was restarted normally before.
#[must_use]
pub fn is_pre_tidb_normal_restart(keyspace_name: &str) -> (bool, String) {
    let shared = shared();
    if keyspace_name.is_empty() || shared.pre_tidb_normal_restart_keyspace_name != keyspace_name {
        return (false, String::new());
    }
    (true, shared.pre_tidb_normal_restart_msg.clone())
}

#[cfg(test)]
mod tests;
