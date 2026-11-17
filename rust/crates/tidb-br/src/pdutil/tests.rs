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

//! Go `pd_serial_test.go`, and the PD HTTP requests of `RemoveSchedulers`
//! / `RemoveAllPDSchedulers` and their undo against a Go master capture
//! (Go's controller over an `httptest` PD answering the same JSON).

use std::sync::Mutex;

use tikv_client::pd_http::{HttpClient, HttpRequest, HttpResponse, RoundTripper, ServiceClient, ServiceDiscovery};

use super::*;
use crate::test_util::{go_records, MockPdClient};

const SCHEDULE_CFG: &str = r#"{"merge-schedule-limit":8,"leader-schedule-limit":4,"region-schedule-limit":2048,"max-snapshot-count":64,"enable-location-replacement":"true","max-pending-peer-count":64,"hot-region-schedule-limit":4,"replica-schedule-limit":64,"enable-tikv-split-region":"true","max-merge-region-size":20}"#;
const SCHEDULERS_JSON: &str =
    r#"["balance-leader-scheduler","balance-hot-region-scheduler","evict-slow-store-scheduler","grant-leader-scheduler"]"#;

/// A PD HTTP server: records each request, answers the schedule config
/// and schedulers, and fails every request while `fail` is set.
#[derive(Default)]
struct MockPd {
    calls: Mutex<Vec<String>>,
    fail: Mutex<bool>,
}

#[async_trait::async_trait]
impl RoundTripper for MockPd {
    async fn round_trip(&self, req: HttpRequest) -> tikv_client::Result<HttpResponse> {
        let uri = req.url.trim_start_matches("http://pd");
        let body = String::from_utf8_lossy(&req.body).into_owned();
        self.calls.lock().unwrap().push(format!("{} {uri} {body}", req.method));
        let (status, body) = if *self.fail.lock().unwrap() {
            (500, b"failed".to_vec())
        } else if req.method == "GET" && uri == "/pd/api/v1/config/schedule" {
            (200, SCHEDULE_CFG.as_bytes().to_vec())
        } else if req.method == "GET" && uri == "/pd/api/v1/schedulers" {
            (200, SCHEDULERS_JSON.as_bytes().to_vec())
        } else {
            (200, Vec::new())
        };
        Ok(HttpResponse {
            status_code: status,
            status: format!("{status}"),
            headers: Default::default(),
            body,
            request: req,
        })
    }
}

struct OneMember;

#[async_trait::async_trait]
impl ServiceDiscovery for OneMember {
    async fn get_all_service_clients(&self) -> Vec<ServiceClient> {
        vec![ServiceClient::new("http://pd", true)]
    }
    fn schedule_check_member_changed(&self) {}
    async fn close(&self) {}
}

fn controller(pd: &Arc<MockPd>) -> Arc<PdController> {
    let http = pd_http::new_client_with_service_discovery(
        "test",
        Arc::new(OneMember),
        vec![pd_http::with_http_client(HttpClient::with_transport(Arc::clone(pd) as Arc<dyn RoundTripper>))],
    )
    .unwrap();
    let stores = (1..=3).map(|id| tidb_proto::metapb::Store { id, ..Default::default() }).collect();
    let runtime = Arc::new(tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap());
    Arc::new(PdController::with_pd_client(
        Arc::new(MockPdClient::with_stores(stores)),
        http,
        tidb_go_semver::new("6.5.0"),
        runtime,
    ))
}

/// Go's `RemoveSchedulers` / `RemoveAllPDSchedulers` and their undo send
/// PD the same requests, in the same order, with the same JSON bodies.
#[test]
fn remove_schedulers_requests_match_go() {
    for r in go_records("pd_controller") {
        let pd = Arc::new(MockPd::default());
        let ctrl = controller(&pd);
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let (undo, res) = if r["mode"] == "default" { ctrl.remove_schedulers(&ctx) } else { ctrl.remove_all_pd_schedulers(&ctx) };
        res.unwrap();
        undo(&ctx).unwrap();
        cancel.cancel();
        let want: Vec<String> = r["calls"].as_array().unwrap().iter().map(|c| c.as_str().unwrap().to_owned()).collect();
        assert_eq!(*pd.calls.lock().unwrap(), want, "{}", r["mode"]);
    }
}

/// Go `TestScheduler`: a failed pause or config update is the error; a
/// resume always succeeds.
#[test]
fn test_scheduler() {
    let pd = Arc::new(MockPd::default());
    *pd.fail.lock().unwrap() = true;
    let ctrl = controller(&pd);
    let ctx = Context::background();
    let scheduler = vec!["balance-leader-scheduler".to_owned()];
    assert!(ctrl.pause_schedulers_and_config_with(&ctx, scheduler.clone(), None).is_err());
    ctrl.resume_scheduler_with(&scheduler).unwrap();
    let cfg = BTreeMap::from([
        ("max-merge-region-keys".to_owned(), Json::from(0)),
        ("max-snapshot".to_owned(), Json::from(1)),
        ("enable-location-replacement".to_owned(), Json::Bool(false)),
        ("max-pending-peer-count".to_owned(), Json::from(16_u64)),
    ]);
    let err = ctrl.pause_schedulers_and_config_with(&ctx, Vec::new(), Some(cfg.clone())).unwrap_err();
    assert!(err.to_string().starts_with("failed to update PD"), "{err}");
    ctrl.resume_scheduler_with(&scheduler).unwrap();
    *pd.fail.lock().unwrap() = false;
    ctrl.pause_schedulers_and_config_with(&ctx, scheduler.clone(), Some(cfg)).unwrap();
    ctrl.resume_scheduler_with(&scheduler).unwrap();
}

/// Go `TestPDVersion`.
#[test]
fn test_pd_version() {
    let r = parse_version("\"v4.1.0-alpha1\"\n");
    let want = tidb_go_semver::new("4.1.0-alpha1");
    assert_eq!((r.major, r.minor, r.pre_release), (want.major, want.minor, want.pre_release));
    assert_eq!(parse_version("bogus"), Version::default());
}

/// Go `TestPauseSchedulersByKeyRange`: the rule is refreshed with its TTL
/// and deleted once the pause ends.
#[test]
fn test_pause_schedulers_by_key_range() {
    let pd = Arc::new(MockPd::default());
    let ctrl = controller(&pd);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let (done, rule_id) = pause_scheduler_by_key_range_with_ttl(
        &ctx,
        Arc::new(ctrl.pd_http.clone()),
        &[(vec![0, 0, 0, 0], vec![0xff, 0xff, 0xff, 0xff])],
        Duration::from_secs(1),
    )
    .unwrap();
    std::thread::sleep(Duration::from_secs(3));
    cancel.cancel();
    done.unwrap().join().unwrap();
    let calls = pd.calls.lock().unwrap().clone();
    let set = format!(
        r#"POST /pd/api/v1/config/region-label/rule {{"id":"{rule_id}","index":0,"labels":[{{"key":"schedule","value":"deny","ttl":"1s"}}],"rule_type":"key-range","data":[{{"end_key":"ffffffff","start_key":"00000000"}}]}}"#
    );
    assert!(calls.len() >= 4, "{calls:?}");
    assert!(calls[..calls.len() - 1].iter().all(|c| c == &set), "{calls:?}");
    // Go marshals the nil `SetRules` as `null`; the vendored PD HTTP
    // client's `LabelRulePatch` writes `[]`. PD reads both as no rules.
    assert_eq!(calls.last().unwrap(), &format!(r#"PATCH /pd/api/v1/config/region-label/rules {{"sets":[],"deletes":["{rule_id}"]}}"#));
}
