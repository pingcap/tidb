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

//! Go `pkg/standby/standby_test.go` and `standby_nextgen_test.go` (the
//! starter cases run on a next-gen kernel build only, as Go's build tag).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use super::*;

/// Go's tests share the package state; one at a time.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Go `resetStandbyTestState`.
fn reset_standby_test_state() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut shared = shared();
    shared.state = STANDBY_STATE;
    shared.activate_request = ActivateRequest::default();
    shared.activation_timeout = 0;
    shared.activate_signalled = false;
    guard
}

#[derive(Default)]
struct MockManagerClient {
    calls: AtomicUsize,
    failures: usize,
    got_reason: Mutex<String>,
}

struct Mgr(Arc<MockManagerClient>);

impl tidb_tidbmanager::Client for Mgr {
    fn free(&self, _: Duration, exit_reason: &str) -> Result<(), String> {
        let calls = self.0.calls.fetch_add(1, Ordering::SeqCst) + 1;
        *self.0.got_reason.lock().unwrap() = exit_reason.to_owned();
        if calls <= self.0.failures {
            return Err("temporary error".to_owned());
        }
        Ok(())
    }
}

#[derive(Default)]
struct MockReadyServer {
    called: bool,
    err: Option<String>,
}

impl StandbyReadyServer for MockReadyServer {
    fn init_tidb_listener(&mut self) -> Result<(), String> {
        self.called = true;
        self.err.clone().map_or(Ok(()), Err)
    }
}

fn never_closed() -> bool {
    false
}

fn request<'a>(path: &'a str, query: &'a [(String, String)], body: &'a [u8]) -> Request<'a> {
    Request { path, query, body, client_closed: &never_closed }
}

fn query(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(key, value)| ((*key).to_owned(), (*value).to_owned())).collect()
}

#[test]
fn test_activate_request_metadata() {
    let _guard = reset_standby_test_state();
    let req: ActivateRequest = serde_json::from_str(
        r#"{
		"keyspace_name": "ks",
		"metadata": {
			"meta_a": "value_a"
		}
	}"#,
    )
    .unwrap();
    assert_eq!(req.metadata, BTreeMap::from([("meta_a".to_owned(), "value_a".to_owned())]));
    shared().activate_request = req.clone();

    let controller = LoadKeyspaceController::new(None);
    let mut metadata = controller.activation_metadata().unwrap();
    assert_eq!(req.metadata, metadata);
    metadata.insert("meta_a".to_owned(), "changed".to_owned());
    assert_eq!("value_a", controller.activation_metadata().unwrap()["meta_a"]);
}

#[test]
fn test_activate_requires_keyspace_name() {
    let _guard = reset_standby_test_state();
    let controller = LoadKeyspaceController::new(None);
    let resp = controller.serve(None, &request("/tidb-pool/activate", &[], b"{}")).unwrap();
    assert_eq!(400, resp.status);
}

#[test]
fn test_activate_waits_until_server_ready() {
    let _guard = reset_standby_test_state();
    let controller = Arc::new(LoadKeyspaceController::new(None));
    let serving = Arc::clone(&controller);
    let handle = std::thread::spawn(move || {
        serving.serve(None, &request("/tidb-pool/activate", &[], br#"{"keyspace_name":"ks"}"#)).unwrap()
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while shared().state != ACTIVATED_STATE {
        assert!(Instant::now() < deadline, "the request did not activate");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!handle.is_finished(), "activate request returned before server ready");

    let mut ready_server = MockReadyServer::default();
    controller.prepare_for_activation(&mut ready_server).unwrap();
    let resp = handle.join().unwrap();

    assert!(ready_server.called);
    assert_eq!(200, resp.status);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&resp.body).unwrap(),
        serde_json::json!({"state": "activated", "keyspace_name": "ks"})
    );
}

struct TestServer {
    need_mgr_free: bool,
}

impl StandbyShutdownServer for TestServer {
    fn auto_id_service_close(&self) {}
    fn get_force_shutdown(&self) -> bool {
        false
    }
    fn wait_zero_conn(&self) {}
    fn get_need_request_mgr_free(&self) -> bool {
        self.need_mgr_free
    }
}

#[test]
fn test_on_server_shutdown_noop_outside_starter() {
    let _guard = reset_standby_test_state();
    if tidb_config_deploymode::is_starter() {
        return;
    }
    let mgr_cli = Arc::new(MockManagerClient::default());
    let controller = LoadKeyspaceController::new(Some(Box::new(Mgr(Arc::clone(&mgr_cli)))));
    let svr: Arc<dyn StandbyShutdownServer> = Arc::new(TestServer { need_mgr_free: true });
    controller.on_server_shutdown(&svr);
    assert_eq!(0, mgr_cli.calls.load(Ordering::SeqCst));
}

#[test]
fn test_status_does_not_return_export_id_outside_starter() {
    let _guard = reset_standby_test_state();
    if tidb_config_deploymode::is_starter() {
        return;
    }
    shared().activate_request =
        ActivateRequest { keyspace_name: "ks1".to_owned(), export_id: "export-1".to_owned(), ..Default::default() };
    let resp = status_handler();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&resp.body).unwrap(),
        serde_json::json!({"state": "standby", "keyspace_name": "ks1"})
    );
}

/// Go `TestOnServerShutdownStarterRetriesManagerFree`'s `reportManagerFree`
/// halves, which need no deploy mode.
#[test]
fn test_report_manager_free_retries() {
    let _guard = reset_standby_test_state();
    let mgr_cli = Arc::new(MockManagerClient { failures: 2, ..Default::default() });
    let controller = LoadKeyspaceController::new(Some(Box::new(Mgr(Arc::clone(&mgr_cli)))));
    assert!(controller.report_manager_free("idle"));
    assert_eq!(3, mgr_cli.calls.load(Ordering::SeqCst));
    assert_eq!("idle", *mgr_cli.got_reason.lock().unwrap());

    // "returns false after exhausting retries"
    let mgr_cli = Arc::new(MockManagerClient { failures: MANAGER_FREE_MAX_ATTEMPTS, ..Default::default() });
    let controller = LoadKeyspaceController::new(Some(Box::new(Mgr(Arc::clone(&mgr_cli)))));
    assert!(!controller.report_manager_free("manager free failed"));
    assert_eq!(MANAGER_FREE_MAX_ATTEMPTS, mgr_cli.calls.load(Ordering::SeqCst));
    assert!(!LoadKeyspaceController::new(None).report_manager_free(""));
}

#[test]
fn test_exit_rejects_invalid_query_params() {
    let _guard = reset_standby_test_state();
    let controller = LoadKeyspaceController::new(None);
    let keyspace = tidb_config::config_tree::config::get_global_keyspace_name();
    for (pairs, want) in [
        (vec![("graceful", "maybe")], "invalid graceful\n"),
        (vec![("graceful", "true"), ("wait", "-1")], "invalid wait\n"),
        (vec![("graceful", "true"), ("wait", "9223372036854775807")], "invalid wait\n"),
        (vec![("graceful", "true"), ("wait", "24h1s")], "invalid wait\n"),
        (vec![("skip_auto_id_owner", "maybe")], "invalid skip_auto_id_owner\n"),
        (vec![("need_mgr_free", "maybe")], "invalid need_mgr_free\n"),
    ] {
        let mut pairs = pairs;
        pairs.push(("keyspace", keyspace.as_str()));
        let resp = controller.serve(None, &request("/tidb-pool/exit", &query(&pairs), b"")).unwrap();
        assert_eq!(400, resp.status, "{want}");
        assert_eq!(want, String::from_utf8(resp.body).unwrap());
    }
    // "close connection wait is instance scoped"
    controller.set_close_conn_wait(Duration::from_secs(3));
    assert_eq!(Duration::from_secs(3), controller.get_close_conn_wait());
    assert_eq!(Duration::ZERO, LoadKeyspaceController::new(None).get_close_conn_wait());
}

/// Go `keyspaceValidateMiddleware`.
#[test]
fn exit_refuses_another_keyspace() {
    let _guard = reset_standby_test_state();
    let controller = LoadKeyspaceController::new(None);
    let resp = controller
        .serve(None, &request("/tidb-pool/exit", &query(&[("keyspace", "not-this-node")]), b""))
        .unwrap();
    assert_eq!(412, resp.status);
    let local = tidb_config::config_tree::config::get_global_keyspace_name();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&resp.body).unwrap(),
        serde_json::json!({"remote": "not-this-node", "local": local})
    );
}

#[test]
fn test_parse_exit_wait() {
    for (value, want) in [
        ("", Some(Duration::ZERO)),
        ("0", Some(Duration::ZERO)),
        ("0s", Some(Duration::ZERO)),
        ("1h30m", Some(Duration::from_secs(5400))),
        ("3600", Some(Duration::from_secs(3600))),
        ("24h", Some(Duration::from_secs(86400))),
        ("24h1s", None),
        ("9223372036854775807", None),
    ] {
        assert_eq!(want, parse_exit_wait(value).ok(), "{value}");
    }
}

/// Go `checkconn`: both names are required, and an unknown connection is
/// unconfirmed.
#[test]
fn checkconn_answers_as_go() {
    let _guard = reset_standby_test_state();
    let controller = LoadKeyspaceController::new(None);
    let resp = controller.serve(None, &request("/tidb-pool/checkconn", &[], b"")).unwrap();
    assert_eq!((400, "keyspace_name or conn_id is empty"), (resp.status, std::str::from_utf8(&resp.body).unwrap()));
    let pairs = query(&[("keyspace_name", "ks"), ("conn_id", "1")]);
    let resp = controller.serve(None, &request("/tidb-pool/checkconn", &pairs, b"")).unwrap();
    assert_eq!("unconfirmed", std::str::from_utf8(&resp.body).unwrap());
    assert!(controller.serve(None, &request("/tidb-pool/nope", &[], b"")).is_none());
    assert_eq!(standby_probe("/health").unwrap().body, br#"{"status":"standby"}"#);
}

struct IdleMock {
    conns: usize,
    processes: Vec<(u8, u16)>,
    capabilities: Vec<u32>,
}

impl IdleServer for IdleMock {
    fn connection_count(&self) -> usize {
        self.conns
    }
    fn get_user_process_list(&self) -> Vec<(u8, u16)> {
        self.processes.clone()
    }
    fn get_client_capability_list(&self) -> Vec<u32> {
        self.capabilities.clone()
    }
    fn set_need_request_mgr_free(&self) {}
}

static EXITS: Mutex<Vec<ExitSignal>> = Mutex::new(Vec::new());

fn record_exit(signal: ExitSignal) {
    EXITS.lock().unwrap().push(signal);
}

/// Go `OnServerCreated`'s watcher: an idle node exits with SIGINT unless a
/// transaction or an interactive client holds it.
#[test]
fn idle_watcher_exits_only_an_idle_node() {
    let _guard = reset_standby_test_state();
    let previous = set_tidb_exit(record_exit);
    EXITS.lock().unwrap().clear();
    let controller = LoadKeyspaceController::new(None);
    controller.on_conn_active();
    let idle = IdleMock { conns: 0, processes: Vec::new(), capabilities: Vec::new() };
    // Active within the window: nothing happens.
    controller.check_idle(&idle, 3600);
    assert!(EXITS.lock().unwrap().is_empty());
    controller.last_active.store(0, std::sync::atomic::Ordering::SeqCst);
    // In a transaction, or an interactive client: stays.
    controller.check_idle(&IdleMock { conns: 1, processes: vec![(0, 1)], capabilities: Vec::new() }, 1);
    controller.check_idle(&IdleMock { conns: 1, processes: vec![(0, 0)], capabilities: vec![1 << 10] }, 1);
    assert!(EXITS.lock().unwrap().is_empty());
    if !tidb_config_deploymode::is_starter() {
        controller.check_idle(&idle, 1);
        assert_eq!(*EXITS.lock().unwrap(), [ExitSignal::Int]);
    }
    set_tidb_exit(previous);
}
